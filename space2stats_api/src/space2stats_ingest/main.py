import tempfile
from typing import Set

import adbc_driver_postgresql.dbapi as pg
import boto3
import pyarrow.parquet as pq
from pystac import Item, STACValidationError
from tqdm import tqdm

TABLE_NAME = "space2stats"


def read_parquet_file(file_path: str):
    """Reads a Parquet file either from a local path or an S3 path."""
    if file_path.startswith("s3://"):
        # Read from S3
        s3 = boto3.client("s3")
        bucket, key = file_path[5:].split("/", 1)
        with tempfile.NamedTemporaryFile() as tmp_file:
            s3.download_file(bucket, key, tmp_file.name)
            table = pq.read_table(tmp_file.name)
    else:
        # Read from local path
        table = pq.read_table(file_path)
    return table


def get_stac_fields_from_item(stac_item_path: str) -> Set[str]:
    item = Item.from_file(stac_item_path)
    columns = [c["name"] for c in item.properties.get("table:columns")]
    return set(columns)


def validate_stac_item(stac_item_path: str) -> bool:
    item = Item.from_file(stac_item_path)
    try:
        item.validate()
        return True
    except STACValidationError as e:
        raise STACValidationError(f"Expected valid STAC item, error: {e}")


def verify_columns(parquet_file: str, stac_item_path: str) -> bool:
    """Verifies that the Parquet file columns match the STAC item metadata columns."""
    parquet_table = read_parquet_file(parquet_file)
    parquet_columns = set(parquet_table.column_names)
    stac_fields = get_stac_fields_from_item(stac_item_path)

    if parquet_columns != stac_fields:
        extra_in_parquet = parquet_columns - stac_fields
        extra_in_stac = stac_fields - parquet_columns
        raise ValueError(
            f"Column mismatch: Extra in Parquet: {extra_in_parquet}, Extra in STAC: {extra_in_stac}"
        )
    return True


def load_parquet_to_db(
    parquet_file: str,
    connection_string: str,
    stac_item_path: str,
    chunksize: int = 64_000,
):
    """Loads and updates Parquet data in PostgreSQL, merging columns if the table exists or creating it if it does not."""
    validate_stac_item(stac_item_path)
    verify_columns(parquet_file, stac_item_path)

    table = read_parquet_file(parquet_file)

    with (
        pg.connect(connection_string) as conn,
        conn.cursor() as cur,
        tqdm(total=table.num_rows, desc="Ingesting Data", unit="rows") as pbar,
    ):
        # Check if the main table exists
        cur.execute(f"SELECT to_regclass('{TABLE_NAME}');")
        table_exists = cur.fetchone()[0] is not None

        if not table_exists:
            # If the main table doesn't exist, create it and load data directly
            cur.adbc_ingest(TABLE_NAME, table.slice(0, 0), mode="replace")
            for batch in table.to_batches(max_chunksize=chunksize):
                cur.adbc_ingest(TABLE_NAME, batch, mode="append")
                pbar.update(batch.num_rows)

            # Create an index on hex_id for faster future joins
            cur.execute(
                f"CREATE INDEX idx_{TABLE_NAME}_hex_id ON {TABLE_NAME} (hex_id)"
            )

        else:
            # Use a temporary table for staging the new data
            temp_table_name = f"{TABLE_NAME}_temp"
            cur.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            cur.adbc_ingest(temp_table_name, table.slice(0, 0), mode="replace")

            for batch in table.to_batches(max_chunksize=chunksize):
                cur.adbc_ingest(temp_table_name, batch, mode="append")
                pbar.update(batch.num_rows)

            # Optimize join by indexing hex_id in the temp table
            cur.execute(
                f"CREATE INDEX idx_{temp_table_name}_hex_id ON {temp_table_name} (hex_id)"
            )

            # Fetch the columns from temp excluding 'hex_id'
            cur.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{temp_table_name}' AND column_name != 'hex_id'"
            )
            temp_columns = [row[0] for row in cur.fetchall()]

            # Construct the SELECT clause excluding 'hex_id' from temp
            temp_columns_select = ", ".join([f"temp.{col}" for col in temp_columns])
            select_query = (
                f"main.*, {temp_columns_select}" if temp_columns_select else "main.*"
            )

            # Create the new joined table with columns from both main and temp
            new_table_name = f"{TABLE_NAME}_new"

            print("Joining new columns to space2stats table.")

            # SQL command to create a new joined table
            cur.execute(
                f"""
                CREATE TABLE {new_table_name} AS
                SELECT {select_query}
                FROM {TABLE_NAME} AS main
                LEFT JOIN {temp_table_name} AS temp ON main.hex_id = temp.hex_id
                """
            )

            # Drop the old table and rename the new table to the original name
            cur.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
            cur.execute(f"ALTER TABLE {new_table_name} RENAME TO {TABLE_NAME}")

            # Recreate index on hex_id
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_hex_id ON {TABLE_NAME} (hex_id)"
            )

            # Clean up by dropping the temporary tables
            cur.execute(f"DROP TABLE IF EXISTS {temp_table_name}")

        conn.commit()
