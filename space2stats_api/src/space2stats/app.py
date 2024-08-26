from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from typing import Any, Dict, List

from starlette.requests import Request

from .db import connect_to_db, close_db_connection
from .main import get_summaries_from_geom, get_available_fields, SummaryRequest

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI Lifespan."""
    # Create Connection Pool
    await connect_to_db(app)
    yield
    # Close the Connection Pool
    await close_db_connection(app)


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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
