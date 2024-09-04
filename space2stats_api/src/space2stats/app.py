from contextlib import asynccontextmanager
from typing import Any, Dict, List

import boto3
from asgi_s3_response_middleware import S3ResponseMiddleware
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from starlette.requests import Request
from starlette_cramjam.middleware import CompressionMiddleware

from .db import close_db_connection, connect_to_db
from .main import (
    SummaryRequest,
    get_available_fields,
    get_summaries_from_geom,
    settings,
)

s3_client = boto3.client("s3")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI Lifespan."""
    # Create Connection Pool
    await connect_to_db(app)
    yield
    # Close the Connection Pool
    await close_db_connection(app)


app = FastAPI(
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
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


@app.post("/summary", response_model=List[Dict[str, Any]])
def get_summary(request: Request, body: SummaryRequest):
    with request.app.state.pool.connection() as conn:
        return get_summaries_from_geom(
            body.aoi,
            body.spatial_join_method,
            body.fields,
            conn,
            geometry=body.geometry,
        )


@app.get("/fields", response_model=List[str])
def fields(request: Request):
    with request.app.state.pool.connection() as conn:
        return get_available_fields(conn)


@app.get("/")
def read_root():
    return {"message": "Welcome to Space2Stats!"}
