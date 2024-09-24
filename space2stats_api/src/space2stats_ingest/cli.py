import typer
from space2stats_ingest.main import download_parquet_from_s3, load_parquet_to_db

app = typer.Typer()


def handle_errors(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            typer.echo(f"An error occurred: {e}", err=True)

    return wrapper


@app.command()
@handle_errors
def download(s3_path: str, local_path: str = "local.parquet"):
    """
    Download a Parquet file from an S3 bucket.
    """
    typer.echo(f"Starting download from S3: {s3_path}")
    download_parquet_from_s3(s3_path, local_path)
    typer.echo(f"Download complete: {local_path}")


@app.command()
@handle_errors
def load(
    connection_string: str, parquet_file: str = "local.parquet", chunksize: int = 10000
):
    """
    Load a Parquet file into a PostgreSQL database.
    """
    typer.echo(f"Loading data into PostgreSQL database from {parquet_file}")
    load_parquet_to_db(parquet_file, connection_string, chunksize)
    typer.echo("Data loaded successfully to PostgreSQL!")


@app.command()
@handle_errors
def download_and_load(
    s3_path: str,
    connection_string: str,
    local_parquet: str = "local.parquet",
    chunksize: int = 10000,
):
    """
    Download a Parquet file from S3 and load it into a PostgreSQL database.
    """
    typer.echo(f"Starting download from S3: {s3_path}")
    download_parquet_from_s3(s3_path, local_parquet)
    typer.echo(f"Download complete: {local_parquet}")

    typer.echo(f"Loading data into PostgreSQL database from {local_parquet}")
    load_parquet_to_db(local_parquet, connection_string, chunksize)
    typer.echo("Data loaded successfully to PostgreSQL!")


if __name__ == "__main__":
    app()
