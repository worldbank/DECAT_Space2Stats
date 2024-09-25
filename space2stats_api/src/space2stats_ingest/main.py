import adbc_driver_postgresql.dbapi as pg
import boto3
import pyarrow.parquet as pq
from tqdm import tqdm

TABLE_NAME = "space2stats"


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
    parquet_file: str, connection_string: str, chunksize: int = 64_000
):
    """
    Loads a local Parquet file into a PostgreSQL database in chunks with a progress bar.

    Args:
        parquet_file (str): Path to the Parquet file.
        connection_string (str): SQLAlchemy-compatible connection string to the PostgreSQL database.
        chunksize (int): Number of rows to process in each chunk.
    """
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
        conn.execute("CREATE INDEX ON space2stats (hex_id);")
        conn.commit()
