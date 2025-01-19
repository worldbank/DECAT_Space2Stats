import geopandas as gpd
import pandas as pd
import pytest

from space2stats_client import Space2StatsClient


def test_client_initialization():
    """Test that the client initializes with correct endpoints."""
    client = Space2StatsClient()
    assert client.base_url == "https://space2stats.ds.io"
    assert client.summary_endpoint == f"{client.base_url}/summary"
    assert client.aggregation_endpoint == f"{client.base_url}/aggregate"
    assert client.fields_endpoint == f"{client.base_url}/fields"


def test_get_topics(mock_api_response):
    """Test that get_topics returns expected DataFrame."""
    client = Space2StatsClient()
    topics = client.get_topics()
    assert isinstance(topics, pd.DataFrame)
    assert "name" in topics.columns
    assert "description" in topics.columns
    assert "source_data" in topics.columns


def test_get_fields(mock_api_response):
    """Test that get_fields returns list of available fields."""
    client = Space2StatsClient()
    fields = client.get_fields()
    assert isinstance(fields, list)
    assert "sum_pop_2020" in fields
    assert "sum_pop_f_10_2020" in fields


def test_get_properties(mock_api_response):
    """Test that get_properties returns DataFrame with variable descriptions."""
    client = Space2StatsClient()
    properties = client.get_properties("test_dataset")
    assert "name" in properties
    assert "description" in properties
    assert "sum_pop_2020" in properties["name"].values


def test_fetch_admin_boundaries(mock_api_response):
    """Test fetching admin boundaries."""
    client = Space2StatsClient()
    boundaries = client.fetch_admin_boundaries("USA", "ADM1")
    assert isinstance(boundaries, gpd.GeoDataFrame)
    assert "geometry" in boundaries.columns


def test_get_summary(mock_api_response, sample_geodataframe):
    """Test get_summary with sample data."""
    client = Space2StatsClient()
    result = client.get_summary(
        gdf=sample_geodataframe, spatial_join_method="centroid", fields=["sum_pop_2020"]
    )
    assert isinstance(result, pd.DataFrame)
    assert "hex_id" in result.columns
    assert "sum_pop_2020" in result.columns


def test_get_aggregate(mock_api_response, sample_geodataframe):
    """Test get_aggregate with sample data."""
    client = Space2StatsClient()
    result = client.get_aggregate(
        gdf=sample_geodataframe,
        spatial_join_method="centroid",
        fields=["sum_pop_2020"],
        aggregation_type="sum",
    )
    assert isinstance(result, pd.DataFrame)
    assert "sum_pop_2020" in result


def test_invalid_spatial_join_method(sample_geodataframe):
    """Test that invalid spatial join method raises ValueError."""
    client = Space2StatsClient()
    with pytest.raises(
        Exception, match="Input should be 'touches', 'centroid' or 'within'"
    ):
        client.get_summary(
            gdf=sample_geodataframe,
            spatial_join_method="invalid",
            fields=["population"],
        )


def test_invalid_aggregation_type(sample_geodataframe):
    """Test that invalid aggregation type raises ValueError."""
    client = Space2StatsClient()
    with pytest.raises(
        Exception, match="Input should be 'sum', 'avg', 'count', 'max' or 'min'"
    ):
        client.get_aggregate(
            gdf=sample_geodataframe,
            spatial_join_method="centroid",
            fields=["population"],
            aggregation_type="invalid",
        )
