import json
import tempfile

import adbc_driver_postgresql.dbapi as pg
import boto3
import pyarrow.parquet as pq
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


def read_stac_metadata_file(file_path: str):
    """
    Reads a STAC metadata file either from a local path or an S3 path.

    Args:
        file_path (str): Path to the STAC metadata file, either local or S3.

    Returns:
        dict: Parsed JSON content of the STAC metadata.
    """
    if file_path.startswith("s3://"):
        s3 = boto3.client("s3")
        bucket, key = file_path[5:].split("/", 1)
        with tempfile.NamedTemporaryFile() as tmp_file:
            s3.download_file(bucket, key, tmp_file.name)
            with open(tmp_file.name, "r") as f:
                stac_metadata = json.load(f)
    else:
        with open(file_path, "r") as f:
            stac_metadata = json.load(f)

    return stac_metadata


def verify_columns(parquet_file: str, stac_metadata_file: str) -> bool:
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

    stac_metadata = read_stac_metadata_file(stac_metadata_file)
    stac_columns = {
        column["name"] for column in stac_metadata["properties"]["table:columns"]
    }

    if parquet_columns != stac_columns:
        extra_in_parquet = parquet_columns - stac_columns
        extra_in_stac = stac_columns - parquet_columns
        raise ValueError(
            f"Column mismatch: Extra in Parquet: {extra_in_parquet}, Extra in STAC: {extra_in_stac}"
        )

    return True


def download_parquet_from_s3(s3_path: str, local_path: str):
    """
    Downloads a Parquet file from an S3 bucket and saves it locally.
    """
    s3 = boto3.client("s3")

    # Split the S3 path into bucket and key
    if s3_path.startswith("s3://"):
        s3_path = s3_path[5:]

    bucket, key = s3_path.split("/", 1)
    s3.download_file(bucket, key, local_path)


def load_parquet_to_db(
    parquet_file: str,
    connection_string: str,
    stac_metadata_file: str,
    chunksize: int = 64_000,
):
    # Verify column consistency between Parquet file and STAC metadata
    if not verify_columns(parquet_file, stac_metadata_file):
        raise ValueError("Column mismatch between Parquet file and STAC metadata")

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
