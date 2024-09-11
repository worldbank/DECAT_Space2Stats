import pytest
import requests
from shapely.geometry import Point, Polygon

BASE_URL = "https://space2stats.ds.io"
FIELDS_ENDPOINT = f"{BASE_URL}/fields"
SUMMARY_ENDPOINT = f"{BASE_URL}/summary"
FIELD = "sum_pop_2020"


def get_field_counts():
    # Fetch the fields from the API
    response = requests.get(FIELDS_ENDPOINT)
    num_fields = len(response.json())

    # Generate field counts in steps of 5
    return list(range(5, num_fields + 1, 5))


field_counts = get_field_counts()
area_sizes = [1, 3, 5, 10, 15]  # degrees


def generate_aoi_params(area, centroid_latitude, centroid_longitude):
    # The area will be used to determine half the side length of the square
    half_side = (area**0.5) / 2
    center = Point(centroid_longitude, centroid_latitude)

    # Define the square's corners (coordinates are shifted around the center point)
    square = Polygon(
        [
            (center.x - half_side, center.y - half_side),  # bottom-left
            (center.x + half_side, center.y - half_side),  # bottom-right
            (center.x + half_side, center.y + half_side),  # top-right
            (center.x - half_side, center.y + half_side),  # top-left
            (center.x - half_side, center.y - half_side),  # close the square
        ]
    )

    geojson = {
        "type": "Feature",
        "geometry": square.__geo_interface__,
        "properties": {},
    }

    return geojson


def make_summary_request(area, centroid_latitude, centroid_longitude, fields):
    geojson = generate_aoi_params(area, centroid_latitude, centroid_longitude)
    request_payload = {
        "spatial_join_method": "centroid",
        "fields": fields,
        "aoi": geojson,
    }
    response = requests.post(SUMMARY_ENDPOINT, json=request_payload)
    assert response.status_code == 200


@pytest.mark.parametrize("area", area_sizes)
def test_benchmark_summary_varying_aoi(benchmark, area):
    # Set the center point for Kenya, region used for examples
    centroid_latitude = 0.0236
    centroid_longitude = 37.9062

    benchmark(
        make_summary_request,
        area=area,
        centroid_latitude=centroid_latitude,
        centroid_longitude=centroid_longitude,
        fields=[FIELD],  # Use a constant field for this benchmark
    )


@pytest.mark.parametrize("field_count", field_counts)
def test_benchmark_summary_aoi_10_varying_fields(benchmark, field_count):
    # Fetch all available fields from the API once
    response = requests.get(FIELDS_ENDPOINT)
    fields = response.json()

    # Ensure we don't exceed available fields
    assert field_count <= len(fields)
    field_subset = fields[:field_count]

    benchmark(
        make_summary_request,
        area=10,  # 10 degrees covers Kenya and more
        centroid_latitude=0.0236,
        centroid_longitude=37.9062,
        fields=field_subset,
    )
