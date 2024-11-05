import tempfile
from typing import Set

import adbc_driver_postgresql.dbapi as pg
import boto3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pystac import Item, STACValidationError
from tqdm import tqdm

TABLE_NAME = "space2stats"


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


def read_table_from_db(connection_string: str, table_name: str) -> pa.Table:
    """Reads a PostgreSQL table into an Arrow table, ordered by hex_id."""
    with pg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            # Check if the table exists
            cur.execute(f"SELECT to_regclass('{table_name}');")
            if cur.fetchone()[0] is None:
                raise ValueError(
                    f"Table '{table_name}' does not exist in the database."
                )

            # Fetch the table data ordered by hex_id
            query = f"SELECT * FROM {table_name} ORDER BY hex_id"
            cur.execute(query)

            return cur.fetch_arrow_table()


def validate_table_alignment(
    db_table: pa.Table, parquet_table: pa.Table, sample_size: int = 1000
):
    """Ensures both tables have similar 'hex_id' values based on a random sample."""
    if db_table.num_rows != parquet_table.num_rows:
        raise ValueError(
            "Row counts do not match between the database and Parquet table."
        )

    # Determine the sample indices
    total_rows = db_table.num_rows
    sample_size = min(sample_size, total_rows)  # Ensure sample size is within bounds
    sample_indices = np.random.choice(total_rows, size=sample_size, replace=False)

    # Compare hex_id values at the sampled indices
    db_sample = db_table["hex_id"].take(sample_indices)
    parquet_sample = parquet_table["hex_id"].take(sample_indices)

    if not pa.compute.all(pa.compute.equal(db_sample, parquet_sample)).as_py():
        raise ValueError(
            "hex_id columns do not match between database and Parquet tables for the sampled rows."
        )


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
    verify_columns(parquet_file, stac_item_path)

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

    # Load the existing table and new table if the table already exists
    db_table = read_table_from_db(connection_string, TABLE_NAME)
    print("Read db table")
    parquet_table = read_parquet_file(parquet_file).sort_by("hex_id")
    print("read parquet table")

    # Validate alignment of the two tables using a sample
    print("Validating alignment")
    validate_table_alignment(db_table, parquet_table)

    # Merge tables in memory
    print("Merge tables")
    merged_table = merge_tables(db_table, parquet_table)

    # Write merged data back to the database in batches
    with pg.connect(connection_string) as conn, tqdm(
        total=merged_table.num_rows, desc="Ingesting Merged Data", unit="rows"
    ) as pbar:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
            cur.adbc_ingest(TABLE_NAME, merged_table.slice(0, 0), mode="replace")

            for batch in merged_table.to_batches(max_chunksize=chunksize):
                cur.adbc_ingest(TABLE_NAME, batch, mode="append")
                pbar.update(batch.num_rows)

            # Recreate index on hex_id
            cur.execute(
                f"CREATE INDEX idx_{TABLE_NAME}_hex_id ON {TABLE_NAME} (hex_id)"
            )
        conn.commit()
