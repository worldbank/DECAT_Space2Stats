import tempfile
from typing import Set

import adbc_driver_postgresql.dbapi as pg
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from pystac import Item, STACValidationError
from tqdm import tqdm

TABLE_NAME = "space2stats_temp"


def read_parquet_file(file_path: str) -> pa.Table:
    """Reads a Parquet file either from a local path or an S3 path."""
    if file_path.startswith("s3://"):
        s3 = boto3.client("s3")
        bucket, key = file_path[5:].split("/", 1)
        with tempfile.NamedTemporaryFile() as tmp_file:
            s3.download_file(bucket, key, tmp_file.name)
            table = pq.read_table(tmp_file.name)
    else:
        table = pq.read_table(file_path)
    return table.rename_columns([col.lower() for col in table.column_names])


def get_stac_fields_from_item(stac_item_path: str) -> Set[str]:
    item = Item.from_file(stac_item_path)
    columns = [c["name"].lower() for c in item.properties.get("table:columns")]
    return set(columns)


def validate_stac_item(stac_item_path: str) -> bool:
    item = Item.from_file(stac_item_path)
    try:
        item.validate()
        return True
    except STACValidationError as e:
        raise STACValidationError(f"Expected valid STAC item, error: {e}")


def verify_columns(
    parquet_file: str, stac_item_path: str, connection_string: str
) -> bool:
    """Verifies that the Parquet file columns match the STAC item metadata columns,
    ensures that 'hex_id' column is present, and checks that new columns don't already exist in the database."""

    # Read Parquet columns and STAC fields
    parquet_table = read_parquet_file(parquet_file)
    parquet_columns = set(parquet_table.column_names)
    stac_fields = get_stac_fields_from_item(stac_item_path)

    # Check if 'hex_id' is present in the Parquet columns
    if "hex_id" not in parquet_columns:
        raise ValueError("The 'hex_id' column is missing from the Parquet file.")

    # Verify Parquet columns match the STAC fields
    if parquet_columns != stac_fields:
        extra_in_parquet = parquet_columns - stac_fields
        extra_in_stac = stac_fields - parquet_columns
        raise ValueError(
            f"Column mismatch: Extra in Parquet: {extra_in_parquet}, Extra in STAC: {extra_in_stac}"
        )

    # Retrieve columns already present in the main table in the database
    with pg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{TABLE_NAME}'
            """)
            existing_columns = set(row[0].lower() for row in cur.fetchall())

    # Check for overlap in columns (excluding 'hex_id')
    overlapping_columns = parquet_columns.intersection(existing_columns) - {"hex_id"}
    if overlapping_columns:
        raise ValueError(
            f"Columns already exist in the database: {overlapping_columns}"
        )

    return True


def merge_tables(db_table: pa.Table, parquet_table: pa.Table) -> pa.Table:
    """Adds columns from the Parquet table to the database table in memory."""
    for column in parquet_table.column_names:
        if column != "hex_id":  # Exclude hex_id to prevent duplicates
            db_table = db_table.append_column(column, parquet_table[column])
    return db_table


def load_parquet_to_db(
    parquet_file: str,
    connection_string: str,
    stac_item_path: str,
    chunksize: int = 64_000,
):
    """Main function to load and update data in PostgreSQL using Arrow in replace mode."""
    validate_stac_item(stac_item_path)
    verify_columns(parquet_file, stac_item_path, connection_string)

    # Check if the table already exists in the database
    with pg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT to_regclass('{TABLE_NAME}');")
            table_exists = cur.fetchone()[0] is not None

    if not table_exists:
        # If the table does not exist, directly ingest the Parquet file in batches
        parquet_table = read_parquet_file(parquet_file)

        with pg.connect(connection_string) as conn, tqdm(
            total=parquet_table.num_rows, desc="Ingesting Data", unit="rows"
        ) as pbar:
            with conn.cursor() as cur:
                # Create an empty table with the same schema
                cur.adbc_ingest(TABLE_NAME, parquet_table.slice(0, 0), mode="replace")

                for batch in parquet_table.to_batches(max_chunksize=chunksize):
                    cur.adbc_ingest(TABLE_NAME, batch, mode="append")
                    pbar.update(batch.num_rows)

                # Create an index on hex_id for future joins
                print("Creating index")
                cur.execute(
                    f"CREATE INDEX idx_{TABLE_NAME}_hex_id ON {TABLE_NAME} (hex_id)"
                )
            conn.commit()
        return

    # Load Parquet file into a temporary table
    parquet_table = read_parquet_file(parquet_file)
    temp_table = f"{TABLE_NAME}_temp"
    with pg.connect(connection_string) as conn, tqdm(
        total=parquet_table.num_rows, desc="Ingesting Temporary Table", unit="rows"
    ) as pbar:
        with conn.cursor() as cur:
            cur.adbc_ingest(temp_table, parquet_table.slice(0, 0), mode="replace")

            for batch in parquet_table.to_batches(max_chunksize=chunksize):
                cur.adbc_ingest(temp_table, batch, mode="append")
                pbar.update(batch.num_rows)

            conn.commit()

    # Fetch columns to add to the main table
    with pg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{temp_table}'
                AND column_name NOT IN (
                    SELECT LOWER(column_name) FROM information_schema.columns WHERE table_name = '{TABLE_NAME}'
                )
            """)
            new_columns = cur.fetchall()

    # Add new columns and attempt to update in a transaction
    try:
        with pg.connect(connection_string) as conn:
            with conn.cursor() as cur:
                # Add new columns to the main table
                for column, column_type in new_columns:
                    cur.execute(
                        f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS {column.lower()} {column_type}"
                    )

                print(f"Adding new columns: {[c[0] for c in new_columns]}...")

                # Construct the SET clause for the update query
                update_columns = [
                    f"{column.lower()} = temp.{column.lower()}"
                    for column, _ in new_columns
                ]
                set_clause = ", ".join(update_columns)

                # Update TABLE_NAME with data from temp_table based on matching hex_id
                print(
                    "Adding columns to dataset... All or nothing operation may take some time."
                )
                cur.execute(f"""
                    UPDATE {TABLE_NAME} AS main
                    SET {set_clause}
                    FROM {temp_table} AS temp
                    WHERE main.hex_id = temp.hex_id
                """)

            conn.commit()  # Commit transaction if all operations succeed
    except Exception as e:
        # Rollback if any error occurs during the update
        print("An error occurred during update. Rolling back changes.")
        conn.rollback()
        raise e  # Re-raise the exception to alert calling code

    # Drop the temporary table
    with pg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        conn.commit()


def load_parquet_to_db_ts(
    parquet_file: str,
    connection_string: str,
    stac_item_path: str,
    table_name_ts: str,
    chunksize: int = 64_000,
):
    """Main function to load TS data in PostgreSQL."""
    validate_stac_item(stac_item_path)
    verify_columns(parquet_file, stac_item_path, connection_string)

    # Check if the table already exists in the database
    with pg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT to_regclass('{table_name_ts}');")
            table_exists = cur.fetchone()[0] is not None

    if not table_exists:
        # If the table does not exist, directly ingest the Parquet file in batches
        parquet_table = read_parquet_file(parquet_file)

        with pg.connect(connection_string) as conn, tqdm(
            total=parquet_table.num_rows, desc="Ingesting Data", unit="rows"
        ) as pbar:
            with conn.cursor() as cur:
                # Create an empty table with the same schema
                cur.adbc_ingest(
                    table_name_ts, parquet_table.slice(0, 0), mode="replace"
                )

                for batch in parquet_table.to_batches(max_chunksize=chunksize):
                    cur.adbc_ingest(table_name_ts, batch, mode="append")
                    pbar.update(batch.num_rows)

                # Create an index on hex_id for future joins
                print("Creating index")
                cur.execute(
                    f"CREATE INDEX idx_{table_name_ts}_hex_id ON {table_name_ts} (hex_id)"
                )
            conn.commit()
        return
