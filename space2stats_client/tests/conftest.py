import json

import geopandas as gpd
import pandas as pd
import pytest
from pystac import Catalog, Collection, Item
from shapely.geometry import Polygon

MOCK_GEOMETRY = Polygon([[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]])
MOCK_GDF = gpd.GeoDataFrame(
    {"name": ["Test Area"]}, geometry=[MOCK_GEOMETRY], crs="EPSG:4326"
)


@pytest.fixture
def sample_geodataframe():
    """Create a sample GeoDataFrame for testing."""
    geometry = Polygon([[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]])
    return gpd.GeoDataFrame({"id": [1], "geometry": [geometry]}, crs="EPSG:4326")


@pytest.fixture
def mock_catalog(mocker):
    """Mock STAC catalog responses."""
    mock_item = mocker.Mock(spec=Item)
    mock_item.id = "test_dataset"
    mock_item.properties = {
        "name": "Test Dataset",
        "description": "Test Description",
        "source_data": "Test Source",
        "table:columns": [
            {"name": "sum_pop_2020", "description": "Population count 2020"},
            {
                "name": "sum_pop_f_10_2020",
                "description": "Female population count 2020",
            },
        ],
    }

    mock_collection = mocker.Mock(spec=Collection)
    mock_collection.id = "test_dataset"
    mock_collection.get_item.return_value = mock_item

    mock_catalog = mocker.Mock(spec=Catalog)
    mock_catalog.get_all_items.return_value = iter([mock_item])
    mock_catalog.get_collections.return_value = iter([mock_collection])

    return mock_catalog


@pytest.fixture
def mock_api_response(mocker, mock_catalog):
    """Mock API responses for testing."""

    def mock_response(*args, **kwargs):
        mock = mocker.Mock()
        mock.status_code = 200

        if "geoboundaries.org" in str(args[0]):
            mock.json.return_value = {
                "gjDownloadURL": "https://example.com/boundary.geojson"
            }
        elif "topics" in str(args[0]):
            mock.json.return_value = [
                {
                    "id": "test_dataset",
                    "name": "Test Dataset",
                    "description": "Test Description",
                    "source_data": "Test Source",
                    "variables": {"sum_pop_2020": "Population count 2020"},
                }
            ]
        elif "properties" in str(args[0]):
            mock.json.return_value = {
                "name": "Test Dataset",
                "description": "Test Description",
                "variables": {
                    "sum_pop_2020": "Population count 2020",
                    "sum_pop_f_10_2020": "Female population count 2020",
                },
            }
        elif "fields" in str(args[0]) and "timeseries" not in str(args[0]):
            mock.json.return_value = ["sum_pop_2020", "sum_pop_f_10_2020"]
        elif "summary_by_hexids" in str(args[0]):
            if "geometry" in kwargs.get("json", {}):
                mock.json.return_value = [
                    {
                        "hex_id": "862a1070fffffff",
                        "sum_pop_2020": 1000,
                        "sum_pop_f_10_2020": 500,
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                        },
                    },
                    {
                        "hex_id": "862a10767ffffff",
                        "sum_pop_2020": 1500,
                        "sum_pop_f_10_2020": 750,
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                        },
                    },
                ]
            else:
                mock.json.return_value = [
                    {
                        "hex_id": "862a1070fffffff",
                        "sum_pop_2020": 1000,
                        "sum_pop_f_10_2020": 500,
                    },
                    {
                        "hex_id": "862a10767ffffff",
                        "sum_pop_2020": 1500,
                        "sum_pop_f_10_2020": 750,
                    },
                ]
        elif "aggregate_by_hexids" in str(args[0]):
            mock.json.return_value = {"sum_pop_2020": 2500, "sum_pop_f_10_2020": 1250}
        elif "summary" in str(args[0]):
            mock.json.return_value = [
                {
                    "hex_id": "862a1070fffffff",
                    "sum_pop_2020": 1000,
                    "sum_pop_f_10_2020": 500,
                }
            ]
        elif "aggregate" in str(args[0]):
            mock.json.return_value = {"sum_pop_2020": 5000, "sum_pop_f_10_2020": 2500}
        elif "timeseries/fields" in str(args[0]):
            mock.json.return_value = ["value"]
        elif "timeseries_by_hexids" in str(args[0]):
            if "start_date" in kwargs.get("json", {}) and "end_date" in kwargs.get(
                "json", {}
            ):
                mock.json.return_value = [
                    {"hex_id": "8611822e7ffffff", "date": "2024-01-02", "value": 0.75},
                    {"hex_id": "8611822e7ffffff", "date": "2024-01-03", "value": 1.25},
                ]
            elif "start_date" in kwargs.get("json", {}):
                mock.json.return_value = [
                    {"hex_id": "8611822e7ffffff", "date": "2024-01-02", "value": 0.75},
                    {"hex_id": "8611822e7ffffff", "date": "2024-01-03", "value": 1.25},
                ]
            elif "end_date" in kwargs.get("json", {}):
                mock.json.return_value = [
                    {"hex_id": "8611822e7ffffff", "date": "2024-01-01", "value": 0.5},
                    {"hex_id": "8611822e7ffffff", "date": "2024-01-02", "value": 0.75},
                ]
            else:
                mock.json.return_value = [
                    {"hex_id": "8611822e7ffffff", "date": "2024-01-01", "value": 0.5},
                    {"hex_id": "8611822e7ffffff", "date": "2024-01-02", "value": 0.75},
                    {"hex_id": "8611822e7ffffff", "date": "2024-01-03", "value": 1.25},
                    {"hex_id": "8611823e3ffffff", "date": "2024-01-01", "value": 0.8},
                    {"hex_id": "8611823e3ffffff", "date": "2024-01-02", "value": 1.0},
                    {"hex_id": "8611823e3ffffff", "date": "2024-01-03", "value": 1.5},
                ]
        elif "timeseries" in str(args[0]):
            mock.json.return_value = [
                {"hex_id": "8611822e7ffffff", "date": "2024-01-01", "value": 0.5},
                {"hex_id": "8611822e7ffffff", "date": "2024-01-02", "value": 0.75},
                {"hex_id": "8611822e7ffffff", "date": "2024-01-03", "value": 1.25},
            ]

        return mock

    # Mock requests
    mocker.patch("requests.get", side_effect=mock_response)
    mocker.patch("requests.post", side_effect=mock_response)

    # Use pre-created GeoDataFrame
    mocker.patch("geopandas.read_file", return_value=MOCK_GDF)

    # Mock STAC catalog
    mocker.patch("pystac.Catalog.from_file", return_value=mock_catalog)

    return mock_response
