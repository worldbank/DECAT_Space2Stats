import boto3
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from tqdm import tqdm


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
    parquet_file: str, connection_string: str, chunksize: int = 10000
):
    """
    Loads a local Parquet file into a PostgreSQL database in chunks with a progress bar.

    Args:
        parquet_file (str): Path to the Parquet file.
        connection_string (str): SQLAlchemy-compatible connection string to the PostgreSQL database.
        chunksize (int): Number of rows to process in each chunk.
    """
    engine = create_engine(connection_string)
    with open(parquet_file, "rb") as f:
        table = pq.read_table(f)
    df = table.to_pandas()

    total_rows = len(df)
    with tqdm(total=total_rows, desc="Loading to PostgreSQL", unit="rows") as pbar:
        for start in range(0, total_rows, chunksize):
            end = min(start + chunksize, total_rows)
            chunk = df.iloc[start:end]
            chunk.to_sql("space2stats", engine, if_exists="append", index=False)
            pbar.update(len(chunk))
