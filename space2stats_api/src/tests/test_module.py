import pytest
from geojson_pydantic import Feature
from h3ronpy import cells_parse
from space2stats.lib import Settings, StatsTable


def test_stats_table(mock_env):
    with StatsTable.connect() as stats_table:
        assert stats_table.table_name == "space2stats"
        assert stats_table.conn.closed == 0
        stats_table.conn.execute("SELECT 1")


def test_stats_table_connect(mock_env, database):
    with StatsTable.connect(
        PGHOST=database.host,
        PGPORT=database.port,
        PGDATABASE=database.dbname,
        PGUSER=database.user,
        PGPASSWORD=database.password,
        PGTABLENAME="XYZ",
    ) as stats_table:
        assert stats_table.table_name == "XYZ"
        assert stats_table.conn.closed == 0
        stats_table.conn.execute("SELECT 1")


def test_stats_table_settings(mock_env, database):
    settings = Settings(
        PGHOST=database.host,
        PGPORT=database.port,
        PGDATABASE=database.dbname,
        PGUSER=database.user,
        PGPASSWORD=database.password,
        PGTABLENAME="ABC",
    )
    with StatsTable.connect(settings) as stats_table:
        assert stats_table.table_name == "ABC"
        assert stats_table.conn.closed == 0
        stats_table.conn.execute("SELECT 1")


def test_aggregate_sum_touches(mock_env, database, aoi_example):
    """Test successful aggregation."""
    settings = Settings(
        PGHOST=database.host,
        PGPORT=database.port,
        PGDATABASE=database.dbname,
        PGUSER=database.user,
        PGPASSWORD=database.password,
        PGTABLENAME="space2stats",
    )

    with StatsTable.connect(settings) as stats_table:
        result = stats_table.aggregate(
            aoi=aoi_example,
            spatial_join_method="touches",
            fields=["sum_pop_2020", "sum_pop_f_10_2020"],
            aggregation_type="sum",
        )

        assert "sum_pop_2020" in result
        assert "sum_pop_f_10_2020" in result
        assert result["sum_pop_2020"] == 250
        assert result["sum_pop_f_10_2020"] == 450


@pytest.mark.parametrize(
    "aggregation_type",
    [
        "sum",
        "avg",
        "count",
        "max",
        "min",
    ],
)
def test_aggregate_methods_no_errors(mock_env, database, aoi_example, aggregation_type):
    """Test different aggregation methods to ensure no errors are thrown."""
    settings = Settings(
        PGHOST=database.host,
        PGPORT=database.port,
        PGDATABASE=database.dbname,
        PGUSER=database.user,
        PGPASSWORD=database.password,
        PGTABLENAME="space2stats",
    )

    with StatsTable.connect(settings) as stats_table:
        result = stats_table.aggregate(
            aoi=aoi_example,
            spatial_join_method="touches",
            fields=["sum_pop_2020", "sum_pop_f_10_2020"],
            aggregation_type=aggregation_type,
        )

        assert "sum_pop_2020" in result
        assert "sum_pop_f_10_2020" in result


def test_aggregate_empty_aoi(mock_env, database):
    """Test aggregation with an empty AOI."""
    empty_aoi = Feature(
        type="Feature",
        geometry={
            "type": "Polygon",
            "coordinates": [
                [
                    [-124.000, 50.000],
                    [-124.000, 51.000],
                    [-125.000, 51.000],
                    [-125.000, 50.000],
                    [-124.000, 50.000],
                ]
            ],
        },
        properties={},
    )

    settings = Settings(
        PGHOST=database.host,
        PGPORT=database.port,
        PGDATABASE=database.dbname,
        PGUSER=database.user,
        PGPASSWORD=database.password,
        PGTABLENAME="space2stats",
    )

    with StatsTable.connect(settings) as stats_table:
        result = stats_table.aggregate(
            aoi=empty_aoi,
            spatial_join_method="centroid",
            fields=["sum_pop_2020", "sum_pop_f_10_2020"],
            aggregation_type="sum",
        )

        assert result["sum_pop_2020"] is None
        assert result["sum_pop_f_10_2020"] is None
        assert len(result) == 2


def test_aggregate_invalid_field(mock_env, database, aoi_example):
    """Test aggregation with an invalid field name."""
    settings = Settings(
        PGHOST=database.host,
        PGPORT=database.port,
        PGDATABASE=database.dbname,
        PGUSER=database.user,
        PGPASSWORD=database.password,
        PGTABLENAME="space2stats",
    )

    with StatsTable.connect(settings) as stats_table:
        with pytest.raises(ValueError):
            stats_table.aggregate(
                aoi=aoi_example,
                spatial_join_method="centroid",
                fields=["invalid_field"],
                aggregation_type="sum",
            )


def test_get_summaries_ordering(mock_env, database):
    """Test that _get_summaries preserves the order of input h3_ids."""
    settings = {
        "PGHOST": database.host,
        "PGPORT": database.port,
        "PGDATABASE": database.dbname,
        "PGUSER": database.user,
        "PGPASSWORD": database.password,
        "PGTABLENAME": "space2stats",
    }

    # Use 3 IDs that exist in the database
    input_h3_ids_str = [
        "862a1070fffffff",
        "867a74817ffffff",
        "862a10767ffffff",
    ]
    input_h3_ids = cells_parse(input_h3_ids_str).to_pylist()

    fields = ["sum_pop_2020"]

    with StatsTable.connect(**settings) as stats_table:
        rows, colnames = stats_table._get_summaries(fields=fields, h3_ids=input_h3_ids)

        returned_h3_ids_str = [row[0] for row in rows]

        # Assert that the order matches the input
        assert (
            returned_h3_ids_str == input_h3_ids_str
        ), f"Mismatch in order: input={input_h3_ids_str}, returned={returned_h3_ids_str}"
