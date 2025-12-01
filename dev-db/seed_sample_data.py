#!/usr/bin/env python3
"""
Sample data seeder for the Space2Stats development environment.

This script loads the cross-sectional Space2Stats sample dataset together with
the climate time series sample dataset into the local PostgreSQL instance.
"""

import os
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
import psycopg

# Default database configuration values can be overridden via environment variables.
DEFAULT_DB_CONFIG = {
    "host": "localhost",
    "port": 5439,
    "dbname": "postgres",
    "user": "username",
    "password": "password",
}

DEFAULT_MAIN_TABLE = "space2stats"
DEFAULT_TS_TABLE = "climate"
DEFAULT_CHUNK_SIZE = 1000


def get_db_config() -> dict:
    """Return database connection parameters with environment overrides."""
    return {
        "host": os.getenv("PGHOST", DEFAULT_DB_CONFIG["host"]),
        "port": int(os.getenv("PGPORT", DEFAULT_DB_CONFIG["port"])),
        "dbname": os.getenv("PGDATABASE", DEFAULT_DB_CONFIG["dbname"]),
        "user": os.getenv("PGUSER", DEFAULT_DB_CONFIG["user"]),
        "password": os.getenv("PGPASSWORD", DEFAULT_DB_CONFIG["password"]),
    }


def get_table_name(env_var: str, default: str) -> str:
    """Return a table name, allowing overrides from the environment."""
    return os.getenv(env_var, default)


def load_parquet_data(
    parquet_file_path: Path, expected_columns: Sequence[str] | None = None
) -> pd.DataFrame:
    """Load data from a parquet file and optionally validate required columns."""
    if not parquet_file_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_file_path}")

    df = pd.read_parquet(parquet_file_path)
    print(f"Loaded {len(df)} rows from {parquet_file_path.name}")
    print(f"Columns ({len(df.columns)} total): {list(df.columns[:10])}")

    if expected_columns:
        missing = set(expected_columns) - set(df.columns)
        if missing:
            raise ValueError(
                f"Missing expected columns {missing} in {parquet_file_path.name}"
            )

    return df


def build_insert_query(
    table_name: str, columns: Sequence[str], conflict_columns: Sequence[str]
) -> str:
    """Construct an INSERT ... ON CONFLICT statement for the provided metadata."""
    placeholders = ", ".join(["%s"] * len(columns))
    columns_str = ", ".join(columns)

    if conflict_columns:
        conflict_target = ", ".join(conflict_columns)
        update_columns = [col for col in columns if col not in conflict_columns]
        if update_columns:
            conflict_clause = ", ".join(
                [f"{col} = EXCLUDED.{col}" for col in update_columns]
            )
            return (
                f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders}) "
                f"ON CONFLICT ({conflict_target}) DO UPDATE SET {conflict_clause}"
            )
        return (
            f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_target}) DO NOTHING"
        )

    return f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"


def chunked(iterable: Sequence[tuple], size: int) -> Iterable[Sequence[tuple]]:
    """Yield fixed-size chunks from a sequence."""
    for start in range(0, len(iterable), size):
        yield iterable[start : start + size]


def insert_data_to_db(
    df: pd.DataFrame,
    table_name: str,
    conflict_columns: Sequence[str],
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> None:
    """Insert DataFrame rows into PostgreSQL, respecting the provided conflict columns."""
    if df.empty:
        print(f"No rows to insert into {table_name}. Skipping.")
        return

    df = df.copy()
    df.columns = df.columns.str.lower()

    # Normalise known column types.
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date

    df_clean = df.where(pd.notnull(df), None)
    data_tuples = [tuple(row) for row in df_clean.itertuples(index=False, name=None)]

    query = build_insert_query(table_name, df.columns.tolist(), conflict_columns)

    print(f"Inserting {len(data_tuples)} rows into {table_name}...")
    config = get_db_config()

    with psycopg.connect(**config) as conn:
        with conn.cursor() as cur:
            for chunk in chunked(data_tuples, chunk_size):
                cur.executemany(query, chunk)
        conn.commit()

    print(f"Successfully inserted {len(data_tuples)} rows into {table_name}.")


def main() -> None:
    """Load the Space2Stats sample datasets into the development database."""
    script_dir = Path(__file__).parent
    data_dir = script_dir / "init-scripts" / "data"

    main_table = get_table_name("PGTABLENAME", DEFAULT_MAIN_TABLE)
    ts_table = get_table_name("TIMESERIES_TABLE_NAME", DEFAULT_TS_TABLE)

    cs_file = data_dir / "space2stats_sample_cs.parquet"
    ts_file = data_dir / "space2stats_sample_ts.parquet"

    print(f"Preparing to seed cross-sectional data into '{main_table}'.")
    cs_df = load_parquet_data(cs_file)
    insert_data_to_db(cs_df, main_table, conflict_columns=["hex_id"])

    print(f"Preparing to seed climate time series data into '{ts_table}'.")
    ts_df = load_parquet_data(ts_file, expected_columns=("hex_id", "date", "spi"))
    insert_data_to_db(ts_df, ts_table, conflict_columns=["hex_id", "date"])

    print("Sample data seeding completed successfully.")


if __name__ == "__main__":
    main()
