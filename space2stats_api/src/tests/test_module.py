import psycopg as pg
import pytest
from geojson_pydantic import Feature
from space2stats.lib import StatsTable


def test_aggregate_success(mock_env, database, aoi_example):
    """Test successful aggregation."""
    with StatsTable.connect() as stats_table:
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

    with StatsTable.connect() as stats_table:
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
    with StatsTable.connect() as stats_table:
        with pytest.raises(pg.errors.UndefinedColumn):
            stats_table.aggregate(
                aoi=aoi_example,
                spatial_join_method="centroid",
                fields=["invalid_field"],
                aggregation_type="sum",
            )
