from functools import wraps

import typer

from .main import load_parquet_to_db, load_parquet_to_db_ts

app = typer.Typer()
app_ts = typer.Typer()


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
def load(
    connection_string: str,
    stac_item_path: str,  # Add the STAC metadata file path as an argument
    parquet_file: str,
    chunksize: int = 64_000,
):
    """
    Load a Parquet file into a PostgreSQL database after verifying columns with the STAC metadata.
    """
    typer.echo(f"Loading data into PostgreSQL database from {parquet_file}")
    load_parquet_to_db(parquet_file, connection_string, stac_item_path, chunksize)
    typer.echo("Data loaded successfully to PostgreSQL!")


@app_ts.command()
@handle_errors
def load_ts(
    connection_string: str,
    stac_item_path: str,  # Add the STAC metadata file path as an argument
    table_name: str,
    parquet_file: str,
    chunksize: int = 64_000,
):
    """
    Load a Parquet file into a PostgreSQL database after verifying columns with the STAC metadata.
    """
    typer.echo(f"Loading data into PostgreSQL database from {parquet_file}")
    load_parquet_to_db_ts(
        parquet_file, connection_string, stac_item_path, table_name, chunksize
    )
    typer.echo("Data loaded successfully to PostgreSQL!")
