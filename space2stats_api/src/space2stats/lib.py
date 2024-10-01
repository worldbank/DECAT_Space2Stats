from dataclasses import dataclass
from typing import Dict, List, Literal, Optional

import psycopg as pg
from geojson_pydantic import Feature
from psycopg import Connection

from .h3_utils import generate_h3_geometries, generate_h3_ids
from .settings import Settings
from .types import AoiModel


@dataclass
class StatsTable:
    conn: Connection
    table_name: str

    @classmethod
    def connect(cls, settings: Optional[Settings] = None, **kwargs) -> "StatsTable":
        """
        Helper method to connect to the database and return a StatsTable instance.

        ```py
        with StatsTable.connect() as stats_table:
            stats_table.fields()
        ```
        """
        settings = settings or Settings(**kwargs, _extra="forbid")
        conn = pg.connect(settings.DB_CONNECTION_STRING)
        return cls(conn=conn, table_name=settings.PGTABLENAME)

    def __enter__(self) -> "StatsTable":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self.conn:
            self.conn.close()

    def _get_summaries(self, fields: List[str], h3_ids: List[str]):
        colnames = ["hex_id"] + fields
        cols = [pg.sql.Identifier(c) for c in colnames]
        sql_query = pg.sql.SQL(
            """
                SELECT {0}
                FROM {1}
                WHERE hex_id = ANY (%s)
            """
        ).format(pg.sql.SQL(", ").join(cols), pg.sql.Identifier(self.table_name))

        # Convert h3_ids to a list to ensure compatibility with psycopg
        h3_ids = list(h3_ids)
        with self.conn.cursor() as cur:
            cur.execute(
                sql_query,
                [
                    h3_ids,
                ],
            )
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

        return rows, colnames

    def summaries(
        self,
        aoi: AoiModel,
        spatial_join_method: Literal["touches", "centroid", "within"],
        fields: List[str],
        geometry: Optional[Literal["polygon", "point"]] = None,
    ):
        """Retrieve Statistics from a GeoJSON feature."""
        if not isinstance(aoi, Feature):
            aoi = AoiModel.model_validate(aoi)

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
        rows, colnames = self._get_summaries(fields=fields, h3_ids=list(h3_ids))
        if not rows:
            return []

        # Format Summaries
        summaries: List[Dict] = []
        geometries = (
            generate_h3_geometries(list(h3_ids), geometry) if geometry else None
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

    def fields(self) -> List[str]:
        sql_query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """

        with self.conn.cursor() as cur:
            cur.execute(
                sql_query,
                [self.table_name],
            )
            columns = [row[0] for row in cur.fetchall() if row[0] != "hex_id"]

        return columns
