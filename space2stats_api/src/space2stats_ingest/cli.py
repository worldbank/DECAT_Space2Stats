from functools import wraps

import typer

from .main import download_parquet_from_s3, load_parquet_to_db

app = typer.Typer()


def handle_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ValueError as e:
            typer.echo(f"An error occurred: {e}", err=True)
            raise typer.Exit(code=1)  # Ensure non-zero exit code on error
        except Exception as e:
            typer.echo(f"An unexpected error occurred: {e}", err=True)
            raise typer.Exit(code=1)  # General non-zero exit for any exception

    return wrapper


@app.command()
@handle_errors
def download(s3_path: str, local_path: str = typer.Option("local.parquet")):
    """
    Download a Parquet file from an S3 bucket.
    """
    typer.echo(f"Starting download from S3: {s3_path}")
    download_parquet_from_s3(s3_path, local_path)
    typer.echo(f"Download complete: {local_path}")


@app.command()
@handle_errors
def load(
    connection_string: str,
    stac_catalog_path: str,  # Add the STAC metadata file path as an argument
    item_name: str,
    parquet_file: str = typer.Option("local.parquet"),
    chunksize: int = 64_000,
):
    """
    Load a Parquet file into a PostgreSQL database after verifying columns with the STAC metadata.
    """
    typer.echo(f"Loading data into PostgreSQL database from {parquet_file}")
    load_parquet_to_db(parquet_file, connection_string, stac_catalog_path, item_name, chunksize)
    typer.echo("Data loaded successfully to PostgreSQL!")


@app.command()
@handle_errors
def download_and_load(
    s3_path: str,
    connection_string: str,
    stac_catalog_path: str,
    parquet_file: str = typer.Option("local.parquet"),
    chunksize: int = 64_000,
):
    """
    Download a Parquet file from S3, verify columns with the STAC metadata, and load it into a PostgreSQL database.
    """
    download(
        s3_path=s3_path,
        local_path=parquet_file,
    )
    load(
        parquet_file=parquet_file,
        connection_string=connection_string,
        stac_catalog_path=stac_catalog_path,  # Ensure this is passed along
        chunksize=chunksize,
    )
