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

        .. code-block:: python

            with StatsTable.connect() as stats_table:
                stats_table.fields()
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
        """Retrieve Statistics from a GeoJSON feature.

        Parameters
        ----------
        aoi : GeoJSON Feature
            The Area of Interest, either as a `Feature` or an instance of `AoiModel`

        spatial_join_method : ["touches", "centroid", "within"]
            The method to use for performing the spatial join between the AOI and H3 cells
                - "touches": Includes H3 cells that touch the AOI
                - "centroid": Includes H3 cells where the centroid falls within the AOI
                - "within": Includes H3 cells entirely within the AOI

        fields : List[str]
            A list of field names to retrieve from the statistics table.

        geometry : Optional["polygon", "point"]
            Specifies if the H3 geometries should be included in the response. It can be either "polygon" or "point". If None, geometries are not included

        Returns
        -------
        List[Dict]
            A list of dictionaries containing statistical summaries for each H3 cell. Each dictionary contains:
                - "hex_id": The H3 cell identifier
                - "geometry" (optional): The geometry of the H3 cell, if geometry is specified.
                - Other fields from the statistics table, based on the specified `fields`
        """
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

    def aggregate(
        self,
        aoi: AoiModel,
        spatial_join_method: Literal["touches", "centroid", "within"],
        fields: List[str],
        aggregation_type: Literal["sum", "avg", "count", "max", "min"],
    ) -> Dict[str, float]:
        """Aggregate Statistics from a GeoJSON feature."""
        if not isinstance(aoi, Feature):
            aoi = AoiModel.model_validate(aoi)

        # Get H3 ids from geometry
        resolution = 6
        h3_ids = list(
            generate_h3_ids(
                aoi.geometry.model_dump(exclude_none=True),
                resolution,
                spatial_join_method,
            )
        )

        if not h3_ids:
            return {}

        # Prepare SQL aggregation query
        aggregations = [f"{aggregation_type}({field}) AS {field}" for field in fields]
        sql_query = pg.sql.SQL(
            """
                SELECT {0}
                FROM {1}
                WHERE hex_id = ANY (%s)
            """
        ).format(
            pg.sql.SQL(", ").join(pg.sql.SQL(a) for a in aggregations),
            pg.sql.Identifier(self.table_name),
        )

        # Convert h3_ids to a list to ensure compatibility with psycopg
        h3_ids = list(h3_ids)
        with self.conn.cursor() as cur:
            cur.execute(
                sql_query,
                [h3_ids],
            )
            row = cur.fetchone()  # Get a single row of results
            colnames = [desc[0] for desc in cur.description]

        # Create a dictionary to hold the aggregation results
        aggregated_results: Dict[str, float] = {}
        for idx, col in enumerate(colnames):
            aggregated_results[col] = row[idx]

        return aggregated_results
