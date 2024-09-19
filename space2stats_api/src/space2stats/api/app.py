from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import boto3
from asgi_s3_response_middleware import S3ResponseMiddleware
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from starlette.requests import Request
from starlette_cramjam.middleware import CompressionMiddleware

from ..lib import StatsTable
from ..settings import Settings as BaseSettings
from .db import close_db_connection, connect_to_db
from .errors import add_exception_handlers
from .schemas import SummaryRequest

s3_client = boto3.client("s3")


class Settings(BaseSettings):
    # Bucket for large responses
    S3_BUCKET_NAME: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect_to_db(app)
    yield
    await close_db_connection(app)


def build_app(settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or Settings()

    app = FastAPI(
        default_response_class=ORJSONResponse,
        lifespan=lifespan,
        settings=settings,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(CompressionMiddleware)
    app.add_middleware(
        S3ResponseMiddleware,
        s3_bucket_name=settings.S3_BUCKET_NAME,
        s3_client=s3_client,
    )

    add_exception_handlers(app)

    def stats_table(request: Request):
        """Dependency to generate a per-request connection to stats table"""
        with request.app.state.pool.connection() as conn:
            yield StatsTable(conn=conn, table_name=settings.PGTABLENAME)

    @app.post("/summary", response_model=List[Dict[str, Any]])
    def get_summary(body: SummaryRequest, table: StatsTable = Depends(stats_table)):
        return table.summaries(
            body.aoi,
            body.spatial_join_method,
            body.fields,
            body.geometry,
        )

    @app.get("/fields", response_model=List[str])
    def fields(table: StatsTable = Depends(stats_table)):
        return table.fields()

    @app.get("/")
    def read_root():
        return {"message": "Welcome to Space2Stats!"}

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app
