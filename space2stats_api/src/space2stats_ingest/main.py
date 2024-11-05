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
    """Loads and updates Parquet data in PostgreSQL, creating the table if it does not exist."""
    validate_stac_item(stac_item_path)
    verify_columns(parquet_file, stac_item_path)

    table = read_parquet_file(parquet_file)

    # Connect to the database
    with (
        pg.connect(connection_string) as conn,
        conn.cursor() as cur,
        tqdm(total=table.num_rows, desc="Ingesting Data", unit="rows") as pbar,
    ):
        # Check if the main table exists
        cur.execute(f"SELECT to_regclass('{TABLE_NAME}');")
        table_exists = cur.fetchone()[0] is not None

        if not table_exists:
            # If the main table doesn't exist, create it directly
            cur.adbc_ingest(TABLE_NAME, table, mode="replace")
            for batch in table.to_batches(max_chunksize=chunksize):
                cur.adbc_ingest(TABLE_NAME, batch, mode="append")
                pbar.update(batch.num_rows)
        else:
            # If the main table exists, create a temporary table for the new data
            temp_table_name = f"{TABLE_NAME}_temp"
            cur.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            cur.adbc_ingest(temp_table_name, table, mode="replace")

            for batch in table.to_batches(max_chunksize=chunksize):
                cur.adbc_ingest(temp_table_name, batch, mode="append")
                pbar.update(batch.num_rows)

            # Add all columns from the temporary table to the main table
            cur.execute(
                f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{temp_table_name}'
                """
            )
            temp_columns = cur.fetchall()
            temp_columns = [c for c in temp_columns if c[0] != "hex_id"]

            for column, column_type in temp_columns:
                try:
                    cur.execute(
                        f"ALTER TABLE {TABLE_NAME} ADD COLUMN {column} {column_type}"
                    )
                except Exception as e:
                    print(f"Could not add column '{column}': {e}")

            # Update the main table using the temporary table
            update_columns = ", ".join(
                [f"{col} = {temp_table_name}.{col}" for col, _ in temp_columns]
            )
            cur.execute(
                f"""
                UPDATE {TABLE_NAME}
                SET {update_columns}
                FROM {temp_table_name}
                WHERE {TABLE_NAME}.hex_id = {temp_table_name}.hex_id
                """
            )

            # Clean up by dropping the temporary table
            cur.execute(f"DROP TABLE IF EXISTS {temp_table_name}")

        conn.commit()
