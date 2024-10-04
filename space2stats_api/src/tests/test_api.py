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
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to Space2Stats!"}


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
