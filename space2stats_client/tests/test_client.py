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
    assert "geometry" in boundaries.columns


def test_get_summary(mock_api_response, sample_geodataframe):
    """Test get_summary with sample data."""
    client = Space2StatsClient()
    result = client.get_summary(
        gdf=sample_geodataframe, spatial_join_method="centroid", fields=["sum_pop_2020"]
    )
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


def test_get_summary_by_hexids(mock_api_response):
    """Test get_summary_by_hexids with sample data."""
    client = Space2StatsClient()
    hex_ids = ["862a1070fffffff", "862a10767ffffff"]
    fields = ["sum_pop_2020", "sum_pop_f_10_2020"]
    result = client.get_summary_by_hexids(hex_ids=hex_ids, fields=fields)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert "hex_id" in result.columns
    for field in fields:
        assert field in result.columns


def test_get_summary_by_hexids_with_geometry(mock_api_response):
    """Test get_summary_by_hexids with geometry."""
    client = Space2StatsClient()
    hex_ids = ["862a1070fffffff", "862a10767ffffff"]
    fields = ["sum_pop_2020", "sum_pop_f_10_2020"]
    result = client.get_summary_by_hexids(
        hex_ids=hex_ids, fields=fields, geometry="polygon"
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert "hex_id" in result.columns
    assert "geometry" in result.columns
    for field in fields:
        assert field in result.columns


def test_get_aggregate_by_hexids(mock_api_response):
    """Test get_aggregate_by_hexids with sample data."""
    client = Space2StatsClient()
    hex_ids = ["862a1070fffffff", "862a10767ffffff"]
    fields = ["sum_pop_2020", "sum_pop_f_10_2020"]
    aggregation_type = "sum"
    result = client.get_aggregate_by_hexids(
        hex_ids=hex_ids, fields=fields, aggregation_type=aggregation_type
    )

    assert isinstance(result, pd.DataFrame)
    assert "sum_pop_2020" in result.columns
    assert "sum_pop_f_10_2020" in result.columns


def test_get_timeseries_fields(mock_api_response):
    """Test that get_timeseries_fields returns list of available fields."""
    client = Space2StatsClient()
    fields = client.get_timeseries_fields()
    assert isinstance(fields, list)
    assert "value" in fields


def test_get_timeseries(mock_api_response, sample_geodataframe):
    """Test get_timeseries with sample data."""
    client = Space2StatsClient()
    result = client.get_timeseries(
        gdf=sample_geodataframe, spatial_join_method="centroid", fields=["value"]
    )
    assert isinstance(result, pd.DataFrame)
    assert "hex_id" in result.columns
    assert "date" in result.columns
    assert "value" in result.columns
    assert "area_id" in result.columns


def test_get_timeseries_by_hexids(mock_api_response):
    """Test get_timeseries_by_hexids with sample data."""
    client = Space2StatsClient()
    hex_ids = ["8611822e7ffffff", "8611823e3ffffff"]
    fields = ["value"]
    result = client.get_timeseries_by_hexids(hex_ids=hex_ids, fields=fields)

    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0
    assert "hex_id" in result.columns
    assert "date" in result.columns
    assert "value" in result.columns


def test_get_timeseries_by_hexids_with_date_range(mock_api_response):
    """Test get_timeseries_by_hexids with date filtering."""
    client = Space2StatsClient()
    hex_ids = ["8611822e7ffffff"]
    fields = ["value"]
    start_date = "2024-01-02"
    end_date = "2024-01-03"

    result = client.get_timeseries_by_hexids(
        hex_ids=hex_ids, fields=fields, start_date=start_date, end_date=end_date
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2  # Should have 2 records in this date range
    assert all(pd.to_datetime(result["date"]) >= pd.to_datetime(start_date))
    assert all(pd.to_datetime(result["date"]) <= pd.to_datetime(end_date))


def test_get_timeseries_by_hexids_values(mock_api_response):
    """Test that get_timeseries_by_hexids returns expected values."""
    client = Space2StatsClient()
    hex_ids = ["8611822e7ffffff", "8611823e3ffffff"]
    fields = ["value"]

    # Use the existing mock data from the fixture
    result = client.get_timeseries_by_hexids(hex_ids=hex_ids, fields=fields)

    # Verify specific values based on the mock data in conftest.py
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 6  # Should have 6 records total from fixture

    # Check values for the first hex_id
    first_hex_data = result[result["hex_id"] == "8611822e7ffffff"]
    assert len(first_hex_data) == 3

    # Sort by date to ensure consistent order
    first_hex_data = first_hex_data.sort_values("date").reset_index(drop=True)

    # Check specific values from the first hex_id
    assert first_hex_data.loc[0, "date"] == "2024-01-01"
    assert first_hex_data.loc[0, "value"] == 0.5
    assert first_hex_data.loc[1, "date"] == "2024-01-02"
    assert first_hex_data.loc[1, "value"] == 0.75
    assert first_hex_data.loc[2, "date"] == "2024-01-03"
    assert first_hex_data.loc[2, "value"] == 1.25

    # Check values for the second hex_id
    second_hex_data = result[result["hex_id"] == "8611823e3ffffff"]
    assert len(second_hex_data) == 3

    # Check a specific value from the second hex_id
    assert "2024-01-03" in second_hex_data["date"].values
    assert 1.5 in second_hex_data["value"].values


def test_handle_api_error_413(mock_error_response_413):
    """Test handling of 413 Request Entity Too Large error."""
    client = Space2StatsClient()

    with pytest.raises(Exception) as exc_info:
        client._handle_api_error(mock_error_response_413)

    expected_message = (
        "Failed to test_handle_api_error_413 (HTTP 413): Request Entity Too Large: The request payload exceeds the API limits\n\n"
        "Hint: Try again with a smaller request or making multiple requests with smaller payloads. "
        "The factors to consider are the number of hexIds (ie. AOI), the number of fields requested, "
        "and the date range (if timeseries data is requested)."
    ).strip()
    assert str(exc_info.value).strip() == expected_message


def test_handle_api_error_503(mock_error_response_503):
    """Test handling of 503 Service Unavailable error."""
    client = Space2StatsClient()

    with pytest.raises(Exception) as exc_info:
        client._handle_api_error(mock_error_response_503)

    expected_message = (
        "Failed to test_handle_api_error_503 (HTTP 503): Service Unavailable - "
        "Request timed out due to API Gateway timeout limit (30 seconds). "
        "Try reducing the request size:\n"
        "  • Use fewer hexagon IDs or a smaller geographic area\n"
        "  • Request fewer fields at a time\n"
        "  • For polygon AOI requests, use a smaller area or simpler geometry\n"
        "  • Consider breaking large requests into smaller chunks"
    ).strip()
    assert str(exc_info.value).strip() == expected_message
