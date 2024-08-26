"""Database connection handling."""

from typing import Any, Dict, Optional

from fastapi import FastAPI
from psycopg_pool import ConnectionPool

from .settings import Settings


async def connect_to_db(
    app: FastAPI,
    settings: Optional[Settings] = None,
    pool_kwargs: Optional[Dict[str, Any]] = None,
) -> None:
    """Connect to Database."""
    if not settings:
        settings = Settings()

    pool_kwargs = pool_kwargs or {}

    app.state.pool = ConnectionPool(
        conninfo=settings.DB_CONNECTION_STRING,
        min_size=settings.DB_MIN_CONN_SIZE,
        max_size=settings.DB_MAX_CONN_SIZE,
        max_waiting=settings.DB_MAX_QUERIES,
        max_idle=settings.DB_MAX_IDLE,
        num_workers=settings.DB_NUM_WORKERS,
        kwargs=pool_kwargs,
    )

    # Make sure the pool is ready
    # ref: https://www.psycopg.org/psycopg3/docs/advanced/pool.html#pool-startup-check
    app.state.pool.wait()


async def close_db_connection(app: FastAPI) -> None:
    """Close Pool."""
    app.state.pool.close()
