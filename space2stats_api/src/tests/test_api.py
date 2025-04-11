import pytest
from shapely import from_geojson

aoi = {
    "type": "Feature",
    "geometry": {
        "type": "Polygon",
        "coordinates": [
            [
                [-74.1, 40.6],
                [-73.9, 40.6],
                [-73.9, 40.8],
                [-74.1, 40.8],
                [-74.1, 40.6],
            ]
        ],
    },
    "properties": {},
}


def test_read_root(client):
    response = client.get("/", follow_redirects=False)
    assert response.status_code in [
        302,
        307,
    ], f"Unexpected status code: {response.status_code}"
    assert (
        response.headers["Location"] == "https://worldbank.github.io/DECAT_Space2Stats"
    )


def test_metadata_redirect(client):
    response = client.get("/metadata", follow_redirects=False)
    assert response.status_code in [
        302,
        307,
    ], f"Unexpected status code: {response.status_code}"
    assert response.headers["Location"] == (
        "https://radiantearth.github.io/stac-browser/#/external/raw.githubusercontent.com/"
        "worldbank/DECAT_Space2Stats/refs/heads/main/space2stats_api/src/space2stats_ingest/"
        "METADATA/stac/catalog.json"
    )


def test_get_summary(client):
    request_payload = {
        "aoi": aoi,
        "spatial_join_method": "touches",
        "fields": ["sum_pop_2020", "sum_pop_f_10_2020"],
    }

    response = client.post("/summary", json=request_payload)
    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, list)

    assert len(response_json) > 0, "Test query failed to return any summaries"
    for summary in response_json:
        assert "hex_id" in summary
        for field in request_payload["fields"]:
            assert field in summary
        assert len(summary) == len(request_payload["fields"]) + 1


def test_bad_fields_validated(client):
    request_payload = {
        "aoi": aoi,
        "spatial_join_method": "touches",
        "fields": ["sum_pop_2020", "sum_pop_f_10_2020", "a_non_existent_field"],
    }

    response = client.post("/summary", json=request_payload)
    assert response.status_code == 400
    assert response.json() == {"error": "Invalid fields: ['a_non_existent_field']"}


@pytest.mark.parametrize("aggregation_type", ["sum", "avg", "count", "max", "min"])
def test_aggregate_methods(client, aggregation_type):
    request_payload = {
        "aoi": aoi,
        "spatial_join_method": "touches",
        "fields": ["sum_pop_2020", "sum_pop_f_10_2020"],
        "aggregation_type": aggregation_type,
    }

    response = client.post("/aggregate", json=request_payload)
    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, dict)
    assert "sum_pop_2020" in response_json
    assert "sum_pop_f_10_2020" in response_json


def test_get_summary_with_geometry_multipolygon(client):
    request_payload = {
        "aoi": {
            **aoi,
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": [
                    aoi["geometry"]["coordinates"],
                    [
                        [
                            [100.0, 0.0],
                            [101.0, 0.0],
                            [101.0, 1.0],
                            [100.0, 1.0],
                            [100.0, 0.0],
                        ],
                        [
                            [100.2, 0.2],
                            [100.8, 0.2],
                            [100.8, 0.8],
                            [100.2, 0.8],
                            [100.2, 0.2],
                        ],
                    ],
                ],
            },
        },
        "spatial_join_method": "touches",
        "fields": ["sum_pop_2020", "sum_pop_f_10_2020"],
        "geometry": "polygon",
    }

    response = client.post("/summary", json=request_payload)
    assert response.status_code == 200
    response_json = response.json()
    assert len(response_json) > 0, "Test query failed to return any summaries"
    assert isinstance(response_json, list)

    for summary in response_json:
        assert "hex_id" in summary
        assert "geometry" in summary
        geometry = from_geojson(summary["geometry"])
        assert geometry.geom_type == "Polygon"
        assert len(summary) == len(request_payload["fields"]) + 2


def test_get_summary_with_geometry_polygon(client):
    request_payload = {
        "aoi": aoi,
        "spatial_join_method": "touches",
        "fields": ["sum_pop_2020", "sum_pop_f_10_2020"],
        "geometry": "polygon",
    }

    response = client.post("/summary", json=request_payload)
    assert response.status_code == 200
    response_json = response.json()
    assert len(response_json) > 0, "Test query failed to return any summaries"
    assert isinstance(response_json, list)

    for summary in response_json:
        assert "hex_id" in summary
        assert "geometry" in summary
        geometry = from_geojson(summary["geometry"])
        assert geometry.geom_type == "Polygon"
        assert len(summary) == len(request_payload["fields"]) + 2


def test_get_summary_with_geometry_point(client):
    request_payload = {
        "aoi": aoi,
        "spatial_join_method": "touches",
        "fields": ["sum_pop_2020", "sum_pop_f_10_2020"],
        "geometry": "point",
    }

    response = client.post("/summary", json=request_payload)
    print(response.json())
    assert response.status_code == 200
    response_json = response.json()
    assert len(response_json) > 0, "Test query failed to return any summaries"
    assert isinstance(response_json, list)

    for summary in response_json:
        assert "hex_id" in summary
        assert "geometry" in summary
        geometry = from_geojson(summary["geometry"])
        assert geometry.geom_type == "Point"
        assert len(summary) == len(request_payload["fields"]) + 2


def test_get_fields(client):
    response = client.get("/fields")
    assert response.status_code == 200
    response_json = response.json()
    assert len(response_json) > 0, "Test query failed to return any summaries"

    expected_fields = ["sum_pop_2020", "sum_pop_f_10_2020"]
    for field in expected_fields:
        assert field in response_json


def test_get_summary_by_hexids(client):
    request_payload = {
        "hex_ids": ["862a1070fffffff", "862a10767ffffff"],
        "fields": ["sum_pop_2020", "sum_pop_f_10_2020"],
    }

    response = client.post("/summary_by_hexids", json=request_payload)
    assert response.status_code == 200
    response_json = response.json()
    assert isinstance(response_json, list)
    assert len(response_json) == 2

    # Check structure of returned data
    for summary in response_json:
        assert "hex_id" in summary
        for field in request_payload["fields"]:
            assert field in summary
        assert len(summary) == len(request_payload["fields"]) + 1


def test_get_summary_by_hexids_with_geometry(client):
    request_payload = {
        "hex_ids": ["862a1070fffffff", "862a10767ffffff"],
        "fields": ["sum_pop_2020", "sum_pop_f_10_2020"],
        "geometry": "polygon",
    }

    response = client.post("/summary_by_hexids", json=request_payload)
    assert response.status_code == 200
    response_json = response.json()

    for summary in response_json:
        assert "hex_id" in summary
        assert "geometry" in summary
        geometry = from_geojson(summary["geometry"])
        assert geometry.geom_type == "Polygon"
        assert len(summary) == len(request_payload["fields"]) + 2


def test_get_summary_by_hexids_invalid_fields(client):
    request_payload = {
        "hex_ids": ["862a1070fffffff"],
        "fields": ["sum_pop_2020", "invalid_field"],
    }

    response = client.post("/summary_by_hexids", json=request_payload)
    assert response.status_code == 400
    assert response.json() == {"error": "Invalid fields: ['invalid_field']"}


def test_aggregate_by_hexids(client):
    request_payload = {
        "hex_ids": ["862a1070fffffff", "862a10767ffffff"],
        "fields": ["sum_pop_2020", "sum_pop_f_10_2020"],
        "aggregation_type": "sum",
    }

    response = client.post("/aggregate_by_hexids", json=request_payload)
    assert response.status_code == 200
    response_json = response.json()

    assert isinstance(response_json, dict)
    assert "sum_pop_2020" in response_json
    assert "sum_pop_f_10_2020" in response_json
    assert response_json["sum_pop_2020"] == 250  # 100 + 150 from test data
    assert response_json["sum_pop_f_10_2020"] == 450  # 200 + 250 from test data


def test_aggregate_by_hexids_invalid_fields(client):
    request_payload = {
        "hex_ids": ["862a1070fffffff"],
        "fields": ["sum_pop_2020", "invalid_field"],
        "aggregation_type": "sum",
    }

    response = client.post("/aggregate_by_hexids", json=request_payload)
    assert response.status_code == 400
    assert response.json() == {"error": "Invalid fields: ['invalid_field']"}


def test_get_timeseries_fields(setup_timeseries_data, client):
    """Test retrieving available timeseries fields."""
    response = client.get("/timeseries/fields")
    assert response.status_code == 200

    assert set(response.json()) == {"field1", "field2"}


def test_get_timeseries(setup_timeseries_data, timeseries_data, client):
    """Test retrieving timeseries data for an AOI."""
    response = client.post(
        "/timeseries",
        json={
            "aoi": {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [41.14, -2.10],
                            [41.15, -2.10],
                            [41.15, -2.11],
                            [41.14, -2.11],
                            [41.14, -2.10],
                        ]
                    ],
                },
                "properties": {},
            },
            "spatial_join_method": "touches",
            "start_date": "2023-01-01",
            "end_date": "2023-01-03",
            "fields": ["field1", "field2"],
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)

    if data:  # If any results matched our AOI
        assert "hex_id" in data[0]
        assert "date" in data[0]
        assert "field1" in data[0]
        assert "field2" in data[0]


def test_get_timeseries_by_hexids(setup_timeseries_data, timeseries_data, client):
    """Test retrieving timeseries data by specific hex IDs."""
    response = client.post(
        "/timeseries_by_hexids",
        json={
            "hex_ids": ["8611822e7ffffff"],
            "start_date": "2023-01-01",
            "end_date": "2023-01-03",
            "fields": ["field1", "field2"],
        },
    )

    assert response.status_code == 200
    assert response.json() == timeseries_data


def test_get_timeseries_date_filtering(setup_timeseries_data, client):
    """Test date filtering in timeseries endpoint."""
    # Test with only start_date
    response = client.post(
        "/timeseries_by_hexids",
        json={
            "hex_ids": ["8611822e7ffffff"],
            "start_date": "2023-01-02",
            "fields": ["field1", "field2"],
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2  # Only 2023-01-02 and 2023-01-03 records
    assert all(record["date"] >= "2023-01-02" for record in data)

    # Test with only end_date
    response = client.post(
        "/timeseries_by_hexids",
        json={
            "hex_ids": ["8611822e7ffffff"],
            "end_date": "2023-01-02",
            "fields": ["field1", "field2"],
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2  # Only 2023-01-01 and 2023-01-02 records
    assert all(record["date"] <= "2023-01-02" for record in data)


def test_get_timeseries_field_filtering(setup_timeseries_data, client):
    """Test field filtering in timeseries endpoint."""
    response = client.post(
        "/timeseries_by_hexids",
        json={
            "hex_ids": ["8611822e7ffffff"],
            "fields": ["field1"],  # Only request field1
        },
    )

    assert response.status_code == 200
    data = response.json()

    # Check that only requested field is present
    for record in data:
        assert "field1" in record
        assert "field2" not in record
        assert "hex_id" in record
        assert "date" in record


def test_get_timeseries_invalid_field(setup_timeseries_data, client):
    """Test error handling for invalid field names."""
    response = client.post(
        "/timeseries_by_hexids",
        json={"hex_ids": ["8611822e7ffffff"], "fields": ["invalid_field"]},
    )

    assert response.status_code == 400
    assert response.json()  # Just ensure there's a JSON response


def test_get_timeseries_invalid_hexid(setup_timeseries_data, client):
    """Test behavior with non-existent hex IDs."""
    response = client.post(
        "/timeseries_by_hexids",
        json={"hex_ids": ["nonexistent_hex_id"], "fields": ["field1"]},
    )

    assert response.status_code == 200
    assert response.json() == []


def test_get_timeseries_multiple_hexids(setup_timeseries_data, client):
    """Test retrieving data for multiple hex IDs."""
    response = client.post(
        "/timeseries_by_hexids",
        json={
            "hex_ids": ["8611822e7ffffff", "8611823e3ffffff"],
            "fields": ["field1", "field2"],
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 6  # 3 dates × 2 hex IDs = 6 records

    # Check that both hex IDs are represented
    hex_ids = {record["hex_id"] for record in data}
    assert hex_ids == {"8611822e7ffffff", "8611823e3ffffff"}


def test_get_timeseries_with_geometry_polygon(setup_timeseries_data, client):
    """Test retrieving timeseries data with polygon geometry."""
    response = client.post(
        "/timeseries",
        json={
            "aoi": {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [41.14, -2.10],
                            [41.15, -2.10],
                            [41.15, -2.11],
                            [41.14, -2.11],
                            [41.14, -2.10],
                        ]
                    ],
                },
                "properties": {},
            },
            "spatial_join_method": "touches",
            "fields": ["field1", "field2"],
            "geometry": "polygon",
        },
    )

    assert response.status_code == 200
    data = response.json()

    if data:
        for record in data:
            assert "hex_id" in record
            assert "date" in record
            assert "geometry" in record
            geometry = from_geojson(record["geometry"])
            assert geometry.geom_type == "Polygon"


def test_get_timeseries_with_geometry_point(setup_timeseries_data, client):
    """Test retrieving timeseries data with point geometry."""
    response = client.post(
        "/timeseries",
        json={
            "aoi": {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [41.14, -2.10],
                            [41.15, -2.10],
                            [41.15, -2.11],
                            [41.14, -2.11],
                            [41.14, -2.10],
                        ]
                    ],
                },
                "properties": {},
            },
            "spatial_join_method": "touches",
            "fields": ["field1", "field2"],
            "geometry": "point",
        },
    )

    assert response.status_code == 200
    data = response.json()

    if data:
        for record in data:
            assert "hex_id" in record
            assert "date" in record
            assert "geometry" in record
            geometry = from_geojson(record["geometry"])
            assert geometry.geom_type == "Point"


def test_get_timeseries_by_hexids_with_geometry_polygon(setup_timeseries_data, client):
    """Test retrieving timeseries data by hex IDs with polygon geometry."""
    response = client.post(
        "/timeseries_by_hexids",
        json={
            "hex_ids": ["8611822e7ffffff"],
            "fields": ["field1", "field2"],
            "geometry": "polygon",
        },
    )

    assert response.status_code == 200
    data = response.json()

    if data:
        for record in data:
            assert "hex_id" in record
            assert "date" in record
            assert "geometry" in record
            geometry = from_geojson(record["geometry"])
            assert geometry.geom_type == "Polygon"


def test_get_timeseries_by_hexids_with_geometry_point(setup_timeseries_data, client):
    """Test retrieving timeseries data by hex IDs with point geometry."""
    response = client.post(
        "/timeseries_by_hexids",
        json={
            "hex_ids": ["8611822e7ffffff"],
            "fields": ["field1", "field2"],
            "geometry": "point",
        },
    )

    assert response.status_code == 200
    data = response.json()

    if data:
        for record in data:
            assert "hex_id" in record
            assert "date" in record
            assert "geometry" in record
            geometry = from_geojson(record["geometry"])
            assert geometry.geom_type == "Point"


def test_timeseries_by_hexids_with_date_and_geometry(setup_timeseries_data, client):
    """Test combining date filtering and geometry in a single request."""
    response = client.post(
        "/timeseries_by_hexids",
        json={
            "hex_ids": ["8611822e7ffffff"],
            "start_date": "2023-01-01",
            "end_date": "2023-01-02",
            "fields": ["field1", "field2"],
            "geometry": "polygon",
        },
    )

    assert response.status_code == 200
    data = response.json()

    if data:
        for record in data:
            assert "geometry" in record
            geometry = from_geojson(record["geometry"])
            assert geometry.geom_type == "Polygon"

            date = record["date"]
            assert date >= "2023-01-01"
            assert date <= "2023-01-02"


def test_timeseries_date_edge_cases(setup_timeseries_data, client):
    """Test edge cases for date validation in timeseries requests."""
    # Test with February 29 on a leap year
    response = client.post(
        "/timeseries_by_hexids",
        json={
            "hex_ids": ["8611822e7ffffff"],
            "start_date": "2020-02-29",
            "end_date": "2020-03-01",
            "fields": ["field1"],
        },
    )
    assert response.status_code == 200

    # Test with date at millennium boundary
    response = client.post(
        "/timeseries_by_hexids",
        json={
            "hex_ids": ["8611822e7ffffff"],
            "start_date": "2000-01-01",
            "fields": ["field1"],
        },
    )
    assert response.status_code == 200

    # Test with very old date
    response = client.post(
        "/timeseries_by_hexids",
        json={
            "hex_ids": ["8611822e7ffffff"],
            "start_date": "1900-01-01",
            "fields": ["field1"],
        },
    )
    assert response.status_code == 200

    # Test with future date
    response = client.post(
        "/timeseries_by_hexids",
        json={
            "hex_ids": ["8611822e7ffffff"],
            "start_date": "2050-01-01",
            "fields": ["field1"],
        },
    )
    assert response.status_code == 200


def test_timeseries_invalid_date_formats(setup_timeseries_data, client):
    """Test that invalid date formats are rejected with appropriate error messages."""
    invalid_dates = [
        "01-01-2023",  # MM-DD-YYYY format (should be YYYY-MM-DD)
        "2023/01/01",  # Wrong separator (/ instead of -)
        "2023-1-1",  # Missing leading zeros
        "20230101",  # No separators
        "not-a-date",  # Not a date at all
        "2023-02-30",  # Non-existent date (February 30)
        "2023-13-01",  # Invalid month (13)
        "2023-04-31",  # Invalid day (April 31)
        "2023-02-29",  # Not a leap year
    ]

    # Test each invalid date as start_date
    for invalid_date in invalid_dates:
        response = client.post(
            "/timeseries_by_hexids",
            json={
                "hex_ids": ["8611822e7ffffff"],
                "start_date": invalid_date,
                "fields": ["field1"],
            },
        )

        assert (
            response.status_code == 400
        ), f"Failed to reject invalid start_date: {invalid_date}"
        error_detail = str(response.json())
        assert (
            "Invalid" in error_detail
            or "does not exist" in error_detail
            or "format" in error_detail.lower()
        ), f"Unexpected error message for {invalid_date}: {error_detail}"

    # Test each invalid date as end_date
    for invalid_date in invalid_dates:
        response = client.post(
            "/timeseries_by_hexids",
            json={
                "hex_ids": ["8611822e7ffffff"],
                "end_date": invalid_date,
                "fields": ["field1"],
            },
        )

        assert (
            response.status_code == 400
        ), f"Failed to reject invalid end_date: {invalid_date}"
        error_detail = str(response.json())
        assert (
            "Invalid" in error_detail
            or "does not exist" in error_detail
            or "format" in error_detail.lower()
        ), f"Unexpected error message for {invalid_date}: {error_detail}"

    # Test with both start_date and end_date being invalid
    response = client.post(
        "/timeseries_by_hexids",
        json={
            "hex_ids": ["8611822e7ffffff"],
            "start_date": "01/01/2023",
            "end_date": "12/31/2023",
            "fields": ["field1"],
        },
    )

    assert response.status_code == 400
    error_detail = str(response.json())
    assert (
        "Invalid" in error_detail
        or "does not exist" in error_detail
        or "format" in error_detail.lower()
    ), f"Unexpected error message for {invalid_date}: {error_detail}"


def test_timeseries_end_date_before_start_date(setup_timeseries_data, client):
    """Test validation when end_date is before start_date."""
    response = client.post(
        "/timeseries_by_hexids",
        json={
            "hex_ids": ["8611822e7ffffff"],
            "start_date": "2023-05-15",
            "end_date": "2023-03-01",
            "fields": ["field1"],
        },
    )

    assert (
        response.status_code == 400
    ), "API should reject when end_date is before start_date"
    error_detail = str(response.json())
    assert (
        "start_date" in error_detail.lower() or "end_date" in error_detail.lower()
    ), f"Error message should mention date range issue: {error_detail}"
