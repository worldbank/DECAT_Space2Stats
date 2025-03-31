from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

import psycopg as pg
from arro3.core import Array
from geojson_pydantic import Feature
from h3ronpy import cells_to_string
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

    def fields(self) -> List[str]:
        """Get available fields from the statistics table."""
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

    def summaries(
        self,
        aoi: AoiModel,
        spatial_join_method: Literal["touches", "centroid", "within"],
        fields: List[str],
        geometry: Optional[Literal["polygon", "point"]] = None,
    ) -> List[Dict]:
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
            A list of field names to retrieve from the statistics table
        geometry : Optional["polygon", "point"]
            Specifies if the H3 geometries should be included in the response

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

        self._validate_fields(fields)

        h3_ids = self._get_h3_ids_for_aoi(aoi, spatial_join_method)

        if not h3_ids:
            return []

        # Get Summaries from H3 ids
        rows, colnames = self._get_summaries(fields=fields, h3_ids=h3_ids)
        if not rows:
            return []

        return self._format_summaries(rows, colnames, fields, h3_ids, geometry)

    def summaries_by_hexids(
        self,
        hex_ids: List[str],
        fields: List[str],
        geometry: Optional[Literal["polygon", "point"]] = None,
    ) -> List[Dict]:
        """Retrieve statistics for specific hex IDs.

        Parameters
        ----------
        hex_ids : List[str]
            List of H3 hexagon IDs to query
        fields : List[str]
            List of fields to retrieve
        geometry : Optional[Literal["polygon", "point"]]
            If specified, includes H3 cell geometries in the response

        Returns
        -------
        List[Dict]
            List of dictionaries containing statistics for each hex ID
        """
        self._validate_fields(fields)

        # Convert hex_ids to integers
        h3_ids = [int(h, 16) for h in hex_ids]

        # Get summaries from H3 ids
        rows, colnames = self._get_summaries(fields=fields, h3_ids=h3_ids)
        if not rows:
            return []

        return self._format_summaries(rows, colnames, fields, h3_ids, geometry)

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

        self._validate_fields(fields)

        h3_ids = self._get_h3_ids_for_aoi(aoi, spatial_join_method)

        if not h3_ids:
            return {}

        return self._aggregate_by_h3_ids(h3_ids, fields, aggregation_type)

    def aggregate_by_hexids(
        self,
        hex_ids: List[str],
        fields: List[str],
        aggregation_type: Literal["sum", "avg", "count", "max", "min"],
    ) -> Dict[str, float]:
        """Aggregate statistics for specific hex IDs.

        Parameters
        ----------
        hex_ids : List[str]
            List of H3 hexagon IDs to aggregate
        fields : List[str]
            List of fields to aggregate
        aggregation_type : Literal["sum", "avg", "count", "max", "min"]
            Type of aggregation to perform

        Returns
        -------
        Dict[str, float]
            Dictionary containing aggregated statistics
        """
        self._validate_fields(fields)

        # Convert hex_ids to integers
        h3_ids = [int(h, 16) for h in hex_ids]

        return self._aggregate_by_h3_ids(h3_ids, fields, aggregation_type)

    def _validate_fields(self, fields: List[str]) -> None:
        """Validate that requested fields exist in the database."""
        invalid_fields = [field for field in fields if field not in self.fields()]
        if invalid_fields:
            raise ValueError(f"Invalid fields: {invalid_fields}")

    def _get_summaries(self, fields: List[str], h3_ids: List[int]):
        """Internal method to fetch summaries from database."""
        colnames = ["hex_id"] + fields
        cols = [pg.sql.Identifier(c) for c in colnames]
        sql_query = pg.sql.SQL(
            """
                SELECT {0}
                FROM {1}
                WHERE hex_id = ANY (%s)
                ORDER BY array_position(%s, hex_id)
            """
        ).format(pg.sql.SQL(", ").join(cols), pg.sql.Identifier(self.table_name))

        # Convert h3_ids to strings
        h3_id_strings = cells_to_string(h3_ids).to_pylist()

        with self.conn.cursor() as cur:
            cur.execute(
                sql_query,
                [h3_id_strings, h3_id_strings],
            )
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

        return rows, colnames

    def _format_summaries(
        self,
        rows: List[tuple],
        colnames: List[str],
        fields: List[str],
        h3_ids: List[int],
        geometry: Optional[Literal["polygon", "point"]],
    ) -> List[Dict]:
        """Internal method to format summary results."""
        summaries: List[Dict] = []
        geometries = generate_h3_geometries(h3_ids, geometry) if geometry else None

        for idx, row in enumerate(rows):
            summary = {"hex_id": row[0]}
            if geometry and geometries is not None:
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

    def _aggregate_by_h3_ids(
        self,
        h3_ids: List[int],
        fields: List[str],
        aggregation_type: Literal["sum", "avg", "count", "max", "min"],
    ) -> Dict[str, float]:
        """Internal method to perform aggregation on H3 IDs."""
        # Convert H3 scalar objects to integers
        h3_ids = [
            scalar.as_py() if hasattr(scalar, "as_py") else scalar for scalar in h3_ids
        ]

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

        with self.conn.cursor() as cur:
            cur.execute(
                sql_query,
                [cells_to_string(h3_ids).to_pylist()],
            )
            row = cur.fetchone()
            colnames = [desc[0] for desc in cur.description]

        # Create a dictionary to hold the aggregation results
        aggregated_results: Dict[str, float] = {}
        for idx, col in enumerate(colnames):
            aggregated_results[col] = row[idx]

        return aggregated_results

    def timeseries_fields(self) -> List[str]:
        """Get available fields from the timeseries table.

        Returns
        -------
        List[str]
            List of field names available in the timeseries table
        """
        sql_query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'spi_test'
        """

        with self.conn.cursor() as cur:
            cur.execute(sql_query)
            columns = [
                row[0] for row in cur.fetchall() if row[0] not in ["hex_id", "date"]
            ]

        return columns

    def timeseries_data(
        self,
        aoi: AoiModel,
        spatial_join_method: Literal["touches", "centroid", "within"],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        fields: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieve timeseries data for an area of interest.

        Parameters
        ----------
        aoi : GeoJSON Feature
            The Area of Interest, either as a `Feature` or an instance of `AoiModel`
        spatial_join_method : Literal["touches", "centroid", "within"]
            The method to use for performing the spatial join
        start_date : Optional[str]
            Start date for filtering data (format: 'YYYY-MM-DD')
        end_date : Optional[str]
            End date for filtering data (format: 'YYYY-MM-DD')
        fields : Optional[List[str]]
            List of fields to retrieve. If None, all available fields will be returned.

        Returns
        -------
        List[Dict[str, Any]]
            List of dictionaries containing timeseries data for each hex ID and date
        """
        # Get H3 IDs for the AOI
        h3_ids = self._get_h3_ids_for_aoi(aoi, spatial_join_method)

        # Convert H3 IDs to strings
        hex_ids = cells_to_string(h3_ids).to_pylist()

        # Use the existing method to get timeseries data for these hex IDs
        return self.timeseries_data_by_hexids(
            hex_ids=hex_ids,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
        )

    def timeseries_data_by_hexids(
        self,
        hex_ids: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        fields: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieve timeseries data from the timeseries data table for specific hex IDs.

        Parameters
        ----------
        hex_ids : List[str]
            List of H3 hexagon IDs to query
        start_date : Optional[str]
            Start date for filtering data (format: 'YYYY-MM-DD')
        end_date : Optional[str]
            End date for filtering data (format: 'YYYY-MM-DD')
        fields : Optional[List[str]]
            List of fields to retrieve. If None, all available fields will be returned.

        Returns
        -------
        List[Dict[str, Any]]
            List of dictionaries containing timeseries data for each hex ID and date
        """
        # Get available fields if not specified
        if fields is None:
            fields = self.timeseries_fields()
        else:
            # Validate requested fields
            available_fields = self.timeseries_fields()
            invalid_fields = [
                field for field in fields if field not in available_fields
            ]
            if invalid_fields:
                raise ValueError(f"Invalid fields: {invalid_fields}")

        # Convert hex_ids to proper format if needed
        h3_id_strings = [h for h in hex_ids]

        # Build the query
        field_identifiers = [pg.sql.Identifier(field) for field in fields]
        select_fields = [
            pg.sql.Identifier("hex_id"),
            pg.sql.Identifier("date"),
        ] + field_identifiers

        sql_query = pg.sql.SQL("""
            SELECT {0}
            FROM spi_test
            WHERE hex_id = ANY (%s)
            {1}
            ORDER BY hex_id, date
        """).format(pg.sql.SQL(", ").join(select_fields), pg.sql.SQL(""))

        params: List[Any] = [h3_id_strings]

        # Add date filters if specified
        where_clauses = []
        if start_date:
            where_clauses.append(pg.sql.SQL("AND date >= %s"))
            params.append(start_date)
        if end_date:
            where_clauses.append(pg.sql.SQL("AND date <= %s"))
            params.append(end_date)

        # Add where clauses to query if they exist
        if where_clauses:
            sql_query = pg.sql.SQL("""
                SELECT {0}
                FROM spi_test
                WHERE hex_id = ANY (%s)
                {1}
                ORDER BY hex_id, date
            """).format(
                pg.sql.SQL(", ").join(select_fields),
                pg.sql.SQL(" ").join(where_clauses),
            )

        # Execute the query
        with self.conn.cursor() as cur:
            cur.execute(sql_query, params)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

        # Format the results
        results = []
        for row in rows:
            result = {}
            for i, col in enumerate(colnames):
                # Convert date objects to ISO format strings
                if col == "date" and row[i]:
                    result[col] = row[i].isoformat()
                else:
                    result[col] = row[i]
            results.append(result)

        return results

    def _get_h3_ids_for_aoi(
        self,
        aoi: AoiModel,
        spatial_join_method: Literal["touches", "centroid", "within"],
    ) -> Array:
        """Get H3 IDs for an area of interest.

        Parameters
        ----------
        aoi : Union[Feature, AoiModel]
            The Area of Interest, either as a Feature or an instance of AoiModel
        spatial_join_method : Literal["touches", "centroid", "within"]
            The method to use for performing the spatial join

        Returns
        -------
        Array
            Array of H3 IDs
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

        return h3_ids
