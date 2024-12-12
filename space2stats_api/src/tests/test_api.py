import h3
import pytest
from shapely import from_geojson
from shapely.geometry import Polygon, shape

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


def test_summary_geometry_mismatch_with_h3(client):
    aoi = {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [33.78593974945852, 5.115816884114494],
                    [33.78593974945852, -4.725410543134203],
                    [41.94362577283266, -4.725410543134203],
                    [41.94362577283266, 5.115816884114494],
                    [33.78593974945852, 5.115816884114494],
                ]
            ],
        },
        "properties": {"name": "Updated AOI"},
    }

    # Define a request payload to match the reported issue
    request_payload = {
        "aoi": aoi,
        "spatial_join_method": "touches",
        "fields": ["sum_pop_2020"],
        "geometry": "polygon",
    }

    response = client.post("/summary", json=request_payload)
    assert (
        response.status_code == 200
    ), f"Unexpected status code: {response.status_code}"

    response_json = response.json()
    assert len(response_json) > 0, "No summaries returned from the API"

    for summary in response_json:
        assert "hex_id" in summary, "Missing 'hex_id' in summary"
        assert "geometry" in summary, "Missing 'geometry' in summary"

        # Extract hex_id and geometry from the API response
        hex_id = summary["hex_id"]
        api_geometry = shape(from_geojson(summary["geometry"]))

        # Generate the geometry independently using h3
        boundary = h3.cell_to_boundary(hex_id)
        independent_geometry = Polygon([(lon, lat) for lat, lon in boundary])

        # Compare the API geometry with the independently generated geometry
        assert api_geometry.equals_exact(independent_geometry, tolerance=0.0001), (
            f"Geometry mismatch for hex_id {hex_id}. "
            f"API geometry: {api_geometry.wkt}, "
            f"Independent geometry: {independent_geometry.wkt}"
        )
