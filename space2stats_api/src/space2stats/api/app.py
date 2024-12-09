from contextlib import asynccontextmanager
from textwrap import dedent
from typing import Any, Dict, List, Optional

import boto3
import psycopg as pg
from asgi_s3_response_middleware import S3ResponseMiddleware
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse, RedirectResponse
from starlette.requests import Request
from starlette_cramjam.middleware import CompressionMiddleware

from .. import __version__
from ..lib import StatsTable
from .db import close_db_connection, connect_to_db
from .errors import add_exception_handlers
from .schemas import AggregateRequest, SummaryRequest
from .settings import Settings

s3_client = boto3.client("s3")


def build_app(settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await connect_to_db(app, settings=settings)
        yield
        await close_db_connection(app)

    app = FastAPI(
        default_response_class=ORJSONResponse,
        lifespan=lifespan,
        title="World Bank Space2Stats API",
        version=__version__,
        summary="API for Space2Stats",
        description=dedent(
            """
            The Space2Stats program is designed to provide academics, statisticians, and data 
            scientists with easier access to regularly requested geospatial aggregate data.

            For more information, see the [Space2Stats docs](https://worldbank.github.io/DECAT_Space2Stats/readme.html).
            """
        ),
        contact={
            "name": "Ben Stewart (Task Leader), Development Data Group (DECDG), Worldbank",
            "url": "https://data.worldbank.org",
            "email": "bstewart@worldbankgroup.org",
        },
        license_info={
            "name": "MIT",
            "url": "https://opensource.org/licenses/MIT",
        },
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
        with request.app.state.pool.connection() as conn:
            yield StatsTable(conn=conn, table_name=settings.PGTABLENAME)

    @app.post("/summary", response_model=List[Dict[str, Any]])
    def get_summary(body: SummaryRequest, table: StatsTable = Depends(stats_table)):
        """Retrieve Statistics from a GeoJSON feature.

        Parameters
        ----------

        <dl>
        <dt>aoi</dt>
        <dd>

        `GeoJSON Feature`

        The Area of Interest, either as a `Feature` or an instance of `AoiModel`
        </dd>

        <dt>spatial_join_method</dt>
        <dd>

        `["touches", "centroid", "within"]`

        The method to use for performing the spatial join between the AOI and H3 cells

        - `touches`: Includes H3 cells that touch the AOI
        - `centroid`: Includes H3 cells where the centroid falls within the AOI
        - `within`: Includes H3 cells entirely within the AOI

        </dd>

        <dt>fields</dt>
        <dd>

        `List[str]`

        A list of field names to retrieve from the statistics table.
        </dd>

        <dt>geometry</dt>
        <dd>

        `Optional["polygon", "point"]`

        Specifies if the H3 geometries should be included in the response. It can be either "polygon" or "point". If None, geometries are not included
        </dd>
        </dl>

        Returns
        -------
        `List[Dict]`

        A list of dictionaries containing statistical summaries for each H3 cell. Each dictionary contains:

        - `hex_id`: The H3 cell identifier
        - `geometry` (optional): The geometry of the H3 cell, if geometry is specified.
        - Other fields from the statistics table, based on the specified `fields`
        """
        try:
            return table.summaries(
                body.aoi,
                body.spatial_join_method,
                body.fields,
                body.geometry,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    @app.post("/aggregate", response_model=Dict[str, float])
    def get_aggregate(body: AggregateRequest, table: StatsTable = Depends(stats_table)):
        """Aggregate Statistics from a GeoJSON feature.

         Parameters
        ----------

        <dl>
        <dt>aoi</dt>
        <dd>

        `GeoJSON Feature`

        The Area of Interest, either as a `Feature` or an instance of `AoiModel`
        </dd>

        <dt>spatial_join_method</dt>
        <dd>

        `["touches", "centroid", "within"]`

        The method to use for performing the spatial join between the AOI and H3 cells

        - `touches`: Includes H3 cells that touch the AOI
        - `centroid`: Includes H3 cells where the centroid falls within the AOI
        - `within`: Includes H3 cells entirely within the AOI

        </dd>

        <dt>fields</dt>
        <dd>

        `List[str]`

        A list of field names to retrieve from the statistics table.
        </dd>

        <dt>aggregation_type</dt>
        <dd>

        `["sum", "avg", "count", "max", "min"]`

        The manner in which to aggregate the statistics.
        </dd>
        </dl>

        Returns
        -------
        `Dict[str, float]`
        """
        try:
            return table.aggregate(
                aoi=body.aoi,
                spatial_join_method=body.spatial_join_method,
                fields=body.fields,
                aggregation_type=body.aggregation_type,
            )
        except pg.errors.UndefinedColumn as e:
            raise HTTPException(status_code=400, detail=e.diag.message_primary) from e

    @app.get("/fields", response_model=List[str])
    def fields(table: StatsTable = Depends(stats_table)):
        """Fields available in the statistics table"""
        return table.fields()

    @app.get("/metadata")
    def metadata_redirect():
        """Redirect to project STAC Browser."""
        return RedirectResponse(
            "https://radiantearth.github.io/stac-browser/#/external/raw.githubusercontent.com/worldbank/DECAT_Space2Stats/refs/heads/main/space2stats_api/src/space2stats_ingest/METADATA/stac/catalog.json"
        )

    @app.get("/")
    def docs_redirect():
        """Redirect to project documentation."""
        return RedirectResponse("https://worldbank.github.io/DECAT_Space2Stats")

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app
