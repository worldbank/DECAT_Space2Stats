from functools import wraps

import psycopg2 as pg
import typer

from .main import load_parquet_to_db

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
def load(
    connection_string: str,
    stac_item_path: str,
    parquet_file: str,
    table_name: str,
    chunksize: int = 64_000,
):
    """
    Load a Parquet file into a PostgreSQL database after verifying columns with the STAC metadata.
    """
    typer.echo(
        f"Loading data into PostgreSQL database table '{table_name}' from {parquet_file}"
    )

    # Check if the table exists
    with pg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table_name}'
                );
            """)
            table_exists = cur.fetchone()[0]

    # If table doesn't exist, ask for confirmation
    if not table_exists:
        should_create = typer.confirm(
            f"Table '{table_name}' does not exist. Would you like to create it?",
            default=False,
        )
        if not should_create:
            typer.echo("Operation cancelled.")
            raise typer.Exit(code=1)

    # Load the data
    load_parquet_to_db(
        parquet_file=parquet_file,
        connection_string=connection_string,
        stac_item_path=stac_item_path,
        table_name=table_name,
        chunksize=chunksize,
    )

    typer.echo(f"Data loaded successfully to PostgreSQL table '{table_name}'!")
