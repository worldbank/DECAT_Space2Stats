import os

import pytest
import requests
from shapely.geometry import Point, Polygon


def get_field_counts():
    fields_endpoint = f"{os.getenv('BASE_URL')}{os.getenv('FIELDS_ENDPOINT')}"
    response = requests.get(fields_endpoint)
    num_fields = len(response.json())

    # Generate field counts in steps of 5
    return list(range(5, num_fields + 1, 5))


def generate_aoi_params(area, centroid_latitude, centroid_longitude):
    half_side = (area**0.5) / 2
    center = Point(centroid_longitude, centroid_latitude)

    square = Polygon(
        [
            (center.x - half_side, center.y - half_side),
            (center.x + half_side, center.y - half_side),
            (center.x + half_side, center.y + half_side),
            (center.x - half_side, center.y + half_side),
            (center.x - half_side, center.y - half_side),
        ]
    )

    geojson = {
        "type": "Feature",
        "geometry": square.__geo_interface__,
        "properties": {},
    }

    return geojson


def make_summary_request(area, centroid_latitude, centroid_longitude, fields):
    summary_endpoint = f"{os.getenv('BASE_URL')}{os.getenv('SUMMARY_ENDPOINT')}"
    geojson = generate_aoi_params(area, centroid_latitude, centroid_longitude)
    request_payload = {
        "spatial_join_method": "centroid",
        "fields": fields,
        "aoi": geojson,
    }
    response = requests.post(summary_endpoint, json=request_payload)
    assert response.status_code == 200


area_sizes = [1, 3, 5, 10, 15]  # degrees


@pytest.mark.parametrize("area", area_sizes)
def test_benchmark_summary_varying_aoi(benchmark, area, setup_benchmark_env):
    centroid_latitude = 0.0236
    centroid_longitude = 37.9062

    benchmark(
        make_summary_request,
        area=area,
        centroid_latitude=centroid_latitude,
        centroid_longitude=centroid_longitude,
        fields=[os.getenv("FIELD")],
    )


@pytest.mark.parametrize(
    "field_count", [5, 10, 15, 20, 25]
)  # Adjust the range manually for now
def test_benchmark_summary_aoi_10_varying_fields(
    benchmark, field_count, setup_benchmark_env
):
    # Fetch all available fields from the API once the environment is set
    fields_endpoint = f"{os.getenv('BASE_URL')}{os.getenv('FIELDS_ENDPOINT')}"
    response = requests.get(fields_endpoint)
    fields = response.json()

    assert field_count <= len(fields)
    field_subset = fields[:field_count]

    benchmark(
        make_summary_request,
        area=10,
        centroid_latitude=0.0236,
        centroid_longitude=37.9062,
        fields=field_subset,
    )
