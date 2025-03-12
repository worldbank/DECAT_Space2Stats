import atexit
from dataclasses import dataclass
from typing import Dict, List, Literal, Optional, Set, Tuple

import psycopg as pg
from geojson_pydantic import Feature
from h3ronpy import cells_to_string
from psycopg import Connection
from psycopg_pool import ConnectionPool

from .h3_utils import generate_h3_geometries, generate_h3_ids
from .settings import Settings
from .types import AoiModel

# Module-level connection pool
_CONNECTION_POOL = None


def get_connection_pool(settings=None):
    """Get or create a connection pool."""
    global _CONNECTION_POOL

    if _CONNECTION_POOL is None:
        settings = settings or Settings()
        _CONNECTION_POOL = ConnectionPool(
            settings.DB_CONNECTION_STRING, min_size=1, max_size=10
        )

    return _CONNECTION_POOL


@dataclass
class StatsTable:
    conn: Connection
    table_name: str
    _owns_connection: bool = True  # Flag to track if we should close the connection

    # Cache for field to table mapping
    _field_table_map: Optional[Dict[str, str]] = None
    _available_fields: Optional[Set[str]] = None

    @classmethod
    def connect(cls, settings: Optional[Settings] = None, **kwargs) -> "StatsTable":
        """
        Helper method to connect to the database and return a StatsTable instance.

        .. code-block:: python

            with StatsTable.connect() as stats_table:
                stats_table.fields()
        """
        settings = settings or Settings(**kwargs, _extra="forbid")
        pool = get_connection_pool(settings)
        conn = pool.getconn()
        return cls(conn=conn, table_name=settings.PGTABLENAME, _owns_connection=True)

    def __enter__(self) -> "StatsTable":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self.conn and self._owns_connection:
            # Return connection to pool instead of closing
            get_connection_pool().putconn(self.conn)

    def _load_field_table_mapping(self) -> Dict[str, str]:
        """
        Load mapping of fields to their source tables.
        Uses database introspection to determine which tables contain which fields.

        Returns:
            Dict[str, str]: Mapping of field names to table names
        """
        if self._field_table_map is not None:
            return self._field_table_map

        field_table_map = {}

        # Use database introspection to find tables and their columns
        with self.conn.cursor() as cur:
            # Get all tables in the database
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = [row[0] for row in cur.fetchall()]

            # For each table, get its columns
            for table in tables:
                cur.execute(
                    """
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' AND table_name = %s
                """,
                    (table,),
                )
                columns = [row[0] for row in cur.fetchall()]

                # Map each column to its table
                for column in columns:
                    if column != "hex_id":  # Skip the primary key/join column
                        field_table_map[column] = table

        self._field_table_map = field_table_map
        return field_table_map

    def fields(self) -> List[str]:
        """Get available fields from all statistics tables."""
        if self._available_fields is not None:
            return list(self._available_fields)

        field_table_map = self._load_field_table_mapping()
        self._available_fields = set(field_table_map.keys())
        return list(self._available_fields)

    def _get_tables_for_fields(self, fields: List[str]) -> Dict[str, List[str]]:
        """
        Group requested fields by their source tables.

        Returns:
            Dict mapping table names to lists of fields from that table
        """
        field_table_map = self._load_field_table_mapping()
        tables_to_fields: Dict[str, List[str]] = {}

        for field in fields:
            if field not in field_table_map:
                raise ValueError(f"Field '{field}' not found in any table")

            table = field_table_map[field]
            if table not in tables_to_fields:
                tables_to_fields[table] = []
            tables_to_fields[table].append(field)

        return tables_to_fields

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
        rows, colnames = self._get_summaries_multi_table(fields=fields, h3_ids=h3_ids)
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
        rows, colnames = self._get_summaries_multi_table(fields=fields, h3_ids=h3_ids)
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

        return self._aggregate_by_h3_ids_multi_table(h3_ids, fields, aggregation_type)

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

        return self._aggregate_by_h3_ids_multi_table(h3_ids, fields, aggregation_type)

    def _validate_fields(self, fields: List[str]) -> None:
        """Validate that requested fields exist in the database."""
        available_fields = set(self.fields())
        invalid_fields = [field for field in fields if field not in available_fields]
        if invalid_fields:
            raise ValueError(f"Invalid fields: {invalid_fields}")

    def _get_summaries_multi_table(
        self, fields: List[str], h3_ids: List[int]
    ) -> Tuple[List[tuple], List[str]]:
        """
        Fetch summaries from multiple tables based on field mapping.

        Returns:
            Tuple of (rows, column_names)
        """
        tables_to_fields = self._get_tables_for_fields(fields)

        if len(tables_to_fields) == 1:
            # If all fields are from the same table, use the simpler query
            table_name = list(tables_to_fields.keys())[0]
            return self._get_summaries_single_table(table_name, fields, h3_ids)

        # Build a query that joins multiple tables
        select_clauses = ["t0.hex_id"]
        join_clauses = []
        table_aliases = {}

        # Convert h3_ids to strings for the query
        h3_id_strings = cells_to_string(h3_ids).to_pylist()

        # Build the query parts
        for i, (table, table_fields) in enumerate(tables_to_fields.items()):
            alias = f"t{i}"
            table_aliases[table] = alias

            # Add fields from this table to the SELECT clause
            for field in table_fields:
                select_clauses.append(f"{alias}.{field}")

            # Add JOIN clause if this isn't the first table
            if i > 0:
                join_clauses.append(
                    f"LEFT JOIN {pg.sql.Identifier(table).as_string(self.conn)} {alias} ON t0.hex_id = {alias}.hex_id"
                )

        # Construct the full query
        base_table = list(tables_to_fields.keys())[0]
        base_alias = table_aliases[base_table]

        query = f"""
            SELECT {', '.join(select_clauses)}
            FROM {pg.sql.Identifier(base_table).as_string(self.conn)} {base_alias}
            {' '.join(join_clauses)}
            WHERE {base_alias}.hex_id = ANY (%s)
            ORDER BY array_position(%s, {base_alias}.hex_id)
        """

        with self.conn.cursor() as cur:
            cur.execute(query, [h3_id_strings, h3_id_strings])
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

        return rows, colnames

    def _get_summaries_single_table(
        self, table_name: str, fields: List[str], h3_ids: List[int]
    ) -> Tuple[List[tuple], List[str]]:
        """Internal method to fetch summaries from a single database table."""
        colnames = ["hex_id"] + fields
        cols = [pg.sql.Identifier(c) for c in colnames]
        sql_query = pg.sql.SQL(
            """
                SELECT {0}
                FROM {1}
                WHERE hex_id = ANY (%s)
                ORDER BY array_position(%s, hex_id)
            """
        ).format(pg.sql.SQL(", ").join(cols), pg.sql.Identifier(table_name))

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

    def _aggregate_by_h3_ids_multi_table(
        self,
        h3_ids: List[int],
        fields: List[str],
        aggregation_type: Literal["sum", "avg", "count", "max", "min"],
    ) -> Dict[str, float]:
        """
        Perform aggregation on H3 IDs across multiple tables.

        Returns:
            Dictionary with aggregated values for each field
        """
        tables_to_fields = self._get_tables_for_fields(fields)

        # Convert H3 scalar objects to integers
        h3_ids = [
            scalar.as_py() if hasattr(scalar, "as_py") else scalar for scalar in h3_ids
        ]

        # Convert to strings for the query
        h3_id_strings = cells_to_string(h3_ids).to_pylist()

        # If all fields are from the same table, use simpler query
        if len(tables_to_fields) == 1:
            table_name = list(tables_to_fields.keys())[0]
            return self._aggregate_by_h3_ids_single_table(
                table_name, h3_id_strings, fields, aggregation_type
            )

        # Otherwise, query each table separately and combine results
        aggregated_results: Dict[str, float] = {}

        for table_name, table_fields in tables_to_fields.items():
            table_results = self._aggregate_by_h3_ids_single_table(
                table_name, h3_id_strings, table_fields, aggregation_type
            )
            aggregated_results.update(table_results)

        return aggregated_results

    def _aggregate_by_h3_ids_single_table(
        self,
        table_name: str,
        h3_id_strings: List[str],
        fields: List[str],
        aggregation_type: Literal["sum", "avg", "count", "max", "min"],
    ) -> Dict[str, float]:
        """Internal method to perform aggregation on H3 IDs for a single table."""
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
            pg.sql.Identifier(table_name),
        )

        with self.conn.cursor() as cur:
            cur.execute(
                sql_query,
                [h3_id_strings],
            )
            row = cur.fetchone()
            colnames = [desc[0] for desc in cur.description]

        # Create a dictionary to hold the aggregation results
        aggregated_results: Dict[str, float] = {}
        if row:  # Check if we got results
            for idx, col in enumerate(colnames):
                aggregated_results[col] = row[idx]

        return aggregated_results


# Register cleanup function
def close_pool():
    global _CONNECTION_POOL
    if _CONNECTION_POOL is not None:
        _CONNECTION_POOL.close()
        _CONNECTION_POOL = None


atexit.register(close_pool)
