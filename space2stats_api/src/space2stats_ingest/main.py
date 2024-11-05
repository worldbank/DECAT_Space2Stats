import tempfile
from typing import Set

import adbc_driver_postgresql.dbapi as pg
import boto3
import pyarrow.parquet as pq
from pystac import Item, STACValidationError
from tqdm import tqdm

TABLE_NAME = "space2stats"


def read_parquet_file(file_path: str):
    """
    Reads a Parquet file either from a local path or an S3 path.

    Args:
        file_path (str): Path to the Parquet file, either local or S3.

    Returns:
        pyarrow.Table: Parquet table object.
    """
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


def validate_stac_item(stac_item_path) -> bool:
    item = Item.from_file(stac_item_path)
    try:
        item.validate()
        return True
    except STACValidationError as e:
        raise STACValidationError(f"Expected valid STAC item, error: {e}")


def verify_columns(parquet_file: str, stac_item_path: str) -> bool:
    """
    Verifies that the Parquet file columns match the STAC item metadata columns.

    Args:
        parquet_file (str): Path to the Parquet file.
        stac_metadata_file (str): Path to the STAC item metadata JSON file.

    Returns:
        bool: True if the columns match, False otherwise.
    """
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
    validate_stac_item(stac_item_path)
    verify_columns(parquet_file, stac_item_path)

    table = pq.read_table(parquet_file)
    with (
        pg.connect(connection_string) as conn,
        conn.cursor() as cur,
        tqdm(total=table.num_rows, desc="Loading to PostgreSQL", unit="rows") as pbar,
    ):
        cur.adbc_ingest(TABLE_NAME, table.slice(0, 0), mode="replace")
        for batch in table.to_batches(max_chunksize=chunksize):
            count = cur.adbc_ingest(TABLE_NAME, batch, mode="append")
            pbar.update(count)
        cur.execute("CREATE INDEX ON space2stats (hex_id);")
        conn.commit()
