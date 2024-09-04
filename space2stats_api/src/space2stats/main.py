


from typing import Dict, List, Literal, Optional

import psycopg as pg
from psycopg import Connection
from pydantic import BaseModel
from geojson_pydantic import Feature, Polygon
from typing_extensions import TypeAlias

from .h3_utils import generate_h3_ids, generate_h3_geometries
from .settings import Settings

settings = Settings()

AoiModel: TypeAlias = Feature[Polygon, Dict]

class SummaryRequest(BaseModel):
    aoi: AoiModel
    spatial_join_method: Literal["touches", "centroid", "within"]
    fields: List[str]
    geometry: Optional[Literal["polygon", "point"]] = None


def _get_summaries(fields: List[str], h3_ids: List[str], conn: Connection):
    colnames = ["hex_id"] + fields
    cols = [pg.sql.Identifier(c) for c in colnames]
    sql_query = pg.sql.SQL(
        """
            SELECT {0}
            FROM {1}
            WHERE hex_id = ANY (%s)
        """
    ).format(pg.sql.SQL(", ").join(cols), pg.sql.Identifier(settings.PGTABLENAME))

    # Convert h3_ids to a list to ensure compatibility with psycopg
    h3_ids = list(h3_ids)
    with conn.cursor() as cur:
        cur.execute(
            sql_query,
            [
                h3_ids,
            ],
        )
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

    return rows, colnames


def get_summaries_from_geom(
    aoi: AoiModel,
    spatial_join_method: Literal["touches", "centroid", "within"],
    fields: List[str],
    conn: Connection,
    geometry: Optional[Literal["polygon", "point"]] = None,
):
    # Get H3 ids from geometry
    resolution = 6
    h3_ids = generate_h3_ids(
        aoi.geometry.model_dump(exclude_none=True),
        resolution,
        spatial_join_method,
    )

    if not h3_ids:
        return []

    # Get Summaries from H3 ids
    rows, colnames = _get_summaries(fields, h3_ids, conn)
    if not rows:
        return []

    # Format Summaries
    summaries: List[Dict] = []
    geometries = (
        generate_h3_geometries(h3_ids, geometry) if geometry else None
    )

    for idx, row in enumerate(rows):
        summary = {"hex_id": row[0]}
        if geometry and geometries:
            summary["geometry"] = geometries[idx]

        summary.update(
            {
                col: row[idx]
                for idx, col in enumerate(colnames[1:], start=1)
                if col in fields
            }
        )
        summaries.append(summary)

    return summaries


def get_available_fields(conn: Connection) -> List[str]:
    sql_query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(
            sql_query,
            [
                settings.PGTABLENAME,
            ],
        )
        columns = [row[0] for row in cur.fetchall() if row[0] != "hex_id"]

    return columns


def summaries(
    aoi: AoiModel,
    spatial_join_method: Literal["touches", "centroid", "within"],
    fields: List[str],
    geometry: Optional[Literal["polygon", "point"]] = None,
) -> List[Dict]:
    """Retrieve Statistics from a GeoJSON feature."""
    if not isinstance(aoi, Feature):
        aoi = AoiModel.model_validate(aoi)

    with pg.connect(settings.DB_CONNECTION_STRING) as conn:
        return get_summaries_from_geom(
            aoi,
            spatial_join_method,
            fields,
            conn,
            geometry=geometry,
        )


def fields() -> List[str]:
    """List Available Fields in the Table."""
    with pg.connect(settings.DB_CONNECTION_STRING) as conn:
        return get_available_fields(conn)
