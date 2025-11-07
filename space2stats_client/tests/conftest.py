import json
import urllib.error

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


@pytest.fixture
def mock_error_response_413(mocker):
    """Mock response for 413 Request Entity Too Large error."""
    mock_response = mocker.Mock()
    mock_response.status_code = 413
    mock_response.json.return_value = {
        "error": "Request Entity Too Large",
        "detail": "The request payload exceeds the API limits",
        "hint": "Try again with a smaller request or making multiple requests with smaller payloads. The factors to consider are the number of hexIds (ie. AOI), the number of fields requested, and the date range (if timeseries data is requested).",
    }
    return mock_response


@pytest.fixture
def mock_error_response_400(mocker):
    """Mock response for 400 Bad Request error with JSON response."""
    mock_response = mocker.Mock()
    mock_response.status_code = 400
    mock_response.json.return_value = {
        "error": "Bad Request",
        "detail": "Invalid request parameters",
    }
    return mock_response


@pytest.fixture
def mock_error_response_500(mocker):
    """Mock response for 500 Internal Server Error with plain text."""
    mock_response = mocker.Mock()
    mock_response.status_code = 500
    mock_response.json.side_effect = ValueError("Not JSON")
    mock_response.text = "Internal Server Error"
    return mock_response


@pytest.fixture
def mock_error_response_503(mocker):
    """Mock response for 503 Service Unavailable error."""
    mock_response = mocker.Mock()
    mock_response.status_code = 503
    mock_response.json.return_value = {
        "error": "Service Unavailable",
        "detail": "The request likely timed out due to processing complexity or high server load",
        "hint": "Try a smaller request by reducing the area of interest (AOI), number of fields requested, or date range (for timeseries). You can also break large requests into multiple smaller requests.",
        "suggestions": [
            "Reduce the number of hexagon IDs in your request",
            "Request fewer fields at a time",
            "Use a smaller geographic area",
            "For timeseries requests, use a shorter date range",
            "Try the request again in a few moments",
        ],
    }
    return mock_response


@pytest.fixture
def mock_adm2_response(mocker):
    """Mock ADM2 API responses for testing."""

    def mock_ddh_response(*args, **kwargs):
        mock = mocker.Mock()
        mock.status_code = 200
        mock.raise_for_status.return_value = None

        # Determine dataset type from URL
        url = str(args[0])

        if "DR0095354" in url:  # population
            mock_data = {
                "value": [
                    {
                        "ISO3": "USA",
                        "ADM2_NAME": "Los Angeles County",
                        "population_total": 10000000,
                        "population_male": 5000000,
                        "population_female": 5000000,
                    },
                    {
                        "ISO3": "USA",
                        "ADM2_NAME": "Cook County",
                        "population_total": 5000000,
                        "population_male": 2500000,
                        "population_female": 2500000,
                    },
                    {
                        "ISO3": "BRA",
                        "ADM2_NAME": "São Paulo",
                        "population_total": 12000000,
                        "population_male": 6000000,
                        "population_female": 6000000,
                    },
                ],
                "count": 3,
            }
        elif "DR0095357" in url:  # urbanization
            mock_data = {
                "value": [
                    {
                        "ISO3": "USA",
                        "ADM2_NAME": "Los Angeles County",
                        "urban_extent_km2": 1500.5,
                        "rural_extent_km2": 500.2,
                        "total_built_up_km2": 800.3,
                    },
                    {
                        "ISO3": "USA",
                        "ADM2_NAME": "Cook County",
                        "urban_extent_km2": 1200.1,
                        "rural_extent_km2": 300.8,
                        "total_built_up_km2": 600.5,
                    },
                ],
                "count": 2,
            }
        elif "DR0095356" in url:  # nighttimelights
            mock_data = {
                "value": [
                    {
                        "ISO3": "USA",
                        "ADM2_NAME": "Los Angeles County",
                        "mean_luminosity": 15.5,
                        "total_luminosity": 25000.8,
                        "max_luminosity": 63.2,
                    },
                    {
                        "ISO3": "USA",
                        "ADM2_NAME": "Cook County",
                        "mean_luminosity": 12.3,
                        "total_luminosity": 18500.4,
                        "max_luminosity": 55.1,
                    },
                ],
                "count": 2,
            }
        elif "DR0095355" in url:  # flood_exposure
            mock_data = {
                "value": [
                    {
                        "ISO3": "USA",
                        "ADM2_NAME": "Los Angeles County",
                        "flood_risk_high": 250.5,
                        "flood_risk_medium": 500.2,
                        "flood_risk_low": 1000.8,
                        "population_exposed": 50000,
                    },
                    {
                        "ISO3": "USA",
                        "ADM2_NAME": "Cook County",
                        "flood_risk_high": 180.3,
                        "flood_risk_medium": 350.7,
                        "flood_risk_low": 800.1,
                        "population_exposed": 35000,
                    },
                ],
                "count": 2,
            }
        else:
            mock_data = {"value": [], "count": 0}

        # Apply ISO3 filter if present
        params = kwargs.get("params", {})
        if "filter" in params and "ISO3" in params["filter"]:
            # Extract ISO3 code from filter string like "ISO3='USA'"
            iso3_code = params["filter"].split("'")[1]
            filtered_records = [
                record
                for record in mock_data["value"]
                if record.get("ISO3") == iso3_code
            ]
            mock_data = {"value": filtered_records, "count": len(filtered_records)}

        mock.json.return_value = mock_data
        return mock

    mocker.patch("requests.get", side_effect=mock_ddh_response)
    return mock_ddh_response


@pytest.fixture
def mock_adm2_error_response(mocker):
    """Mock ADM2 API error response for testing."""

    def mock_error_response(*args, **kwargs):
        mock = mocker.Mock()
        mock.status_code = 404

        # Create a proper HTTPError
        import requests

        http_error = requests.HTTPError("404 Client Error: Not Found for url")
        mock.raise_for_status.side_effect = http_error

        return mock

    mocker.patch("requests.get", side_effect=mock_error_response)
    return mock_error_response


@pytest.fixture
def mock_esri_service(mocker):
    """Simple mock for ESRI service that covers basic functionality."""

    def mock_urlopen(url, **kwargs):  # Accept **kwargs to handle timeout
        mock_response = mocker.Mock()
        url_str = str(url)

        # Service metadata - always queryable
        if "?f=pjson" in url_str:
            service_metadata = {
                "capabilities": "Query,Create,Update,Delete",
                "maxRecordCount": 1000,
            }
            mock_response.read.return_value.decode.return_value = json.dumps(
                service_metadata
            )
        # Count query - always return 1 record
        elif "returnCountOnly=True" in url_str:
            count_response = {"count": 1}
            mock_response.read.return_value.decode.return_value = json.dumps(
                count_response
            )
        # Data query - return simple geojson
        else:
            data_response = {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                        },
                        "properties": {"ISO_A3": "USA", "NAME": "Test Area"},
                    }
                ],
            }
            mock_response.read.return_value.decode.return_value = json.dumps(
                data_response
            )

        mock_response.__enter__ = lambda self: mock_response
        mock_response.__exit__ = lambda self, *args: None

        return mock_response

    mocker.patch("urllib.request.urlopen", side_effect=mock_urlopen)
    mocker.patch("geopandas.read_file", return_value=MOCK_GDF)

    return mock_urlopen


@pytest.fixture
def mock_esri_non_queryable(mocker):
    """Mock ESRI service that is not queryable - tests error handling."""

    def mock_urlopen(url, **kwargs):  # Accept **kwargs to handle timeout
        mock_response = mocker.Mock()
        # Only respond to metadata requests with non-queryable service
        service_metadata = {
            "capabilities": "Create,Update,Delete",  # No Query capability
            "maxRecordCount": 1000,
        }
        mock_response.read.return_value.decode.return_value = json.dumps(
            service_metadata
        )
        mock_response.__enter__ = lambda self: mock_response
        mock_response.__exit__ = lambda self, *args: None
        return mock_response

    mocker.patch("urllib.request.urlopen", side_effect=mock_urlopen)
    return mock_urlopen


@pytest.fixture
def mock_esri_metadata_http_error(mocker):
    """Mock ESRI service that returns HTTP error for metadata request."""

    def mock_urlopen(url, **kwargs):
        if "?f=pjson" in str(url):
            error = urllib.error.HTTPError(url, 404, "Not Found", {}, None)
            raise error
        # Shouldn't reach other requests
        return mocker.Mock()

    mocker.patch("urllib.request.urlopen", side_effect=mock_urlopen)
    return mock_urlopen


@pytest.fixture
def mock_esri_metadata_url_error(mocker):
    """Mock ESRI service that returns URL error for metadata request."""

    def mock_urlopen(url, **kwargs):
        if "?f=pjson" in str(url):
            error = urllib.error.URLError("Connection refused")
            raise error
        # Shouldn't reach other requests
        return mocker.Mock()

    mocker.patch("urllib.request.urlopen", side_effect=mock_urlopen)
    return mock_urlopen


@pytest.fixture
def mock_esri_metadata_json_error(mocker):
    """Mock ESRI service that returns invalid JSON for metadata."""

    def mock_urlopen(url, **kwargs):
        mock_response = mocker.Mock()
        if "?f=pjson" in str(url):
            mock_response.read.return_value.decode.return_value = "invalid json{"
        mock_response.__enter__ = lambda self: mock_response
        mock_response.__exit__ = lambda self, *args: None
        return mock_response

    mocker.patch("urllib.request.urlopen", side_effect=mock_urlopen)
    return mock_urlopen


@pytest.fixture
def mock_esri_count_http_error(mocker):
    """Mock ESRI service that returns HTTP error for count request."""

    def mock_urlopen(url, **kwargs):
        mock_response = mocker.Mock()
        url_str = str(url)

        if "?f=pjson" in url_str:
            # Return valid metadata
            service_metadata = {
                "capabilities": "Query,Create,Update,Delete",
                "maxRecordCount": 1000,
            }
            mock_response.read.return_value.decode.return_value = json.dumps(
                service_metadata
            )
            mock_response.__enter__ = lambda self: mock_response
            mock_response.__exit__ = lambda self, *args: None
            return mock_response
        elif "returnCountOnly=True" in url_str:
            # Raise HTTP error for count request
            error = urllib.error.HTTPError(url, 500, "Internal Server Error", {}, None)
            raise error

        return mock_response

    mocker.patch("urllib.request.urlopen", side_effect=mock_urlopen)
    return mock_urlopen


@pytest.fixture
def mock_esri_count_url_error(mocker):
    """Mock ESRI service that returns URL error for count request."""

    def mock_urlopen(url, **kwargs):
        mock_response = mocker.Mock()
        url_str = str(url)

        if "?f=pjson" in url_str:
            # Return valid metadata
            service_metadata = {
                "capabilities": "Query,Create,Update,Delete",
                "maxRecordCount": 1000,
            }
            mock_response.read.return_value.decode.return_value = json.dumps(
                service_metadata
            )
            mock_response.__enter__ = lambda self: mock_response
            mock_response.__exit__ = lambda self, *args: None
            return mock_response
        elif "returnCountOnly=True" in url_str:
            # Raise URL error for count request
            error = urllib.error.URLError("Network unreachable")
            raise error

        return mock_response

    mocker.patch("urllib.request.urlopen", side_effect=mock_urlopen)
    return mock_urlopen


@pytest.fixture
def mock_esri_count_json_error(mocker):
    """Mock ESRI service that returns invalid JSON for count request."""

    def mock_urlopen(url, **kwargs):
        mock_response = mocker.Mock()
        url_str = str(url)

        if "?f=pjson" in url_str:
            # Return valid metadata
            service_metadata = {
                "capabilities": "Query,Create,Update,Delete",
                "maxRecordCount": 1000,
            }
            mock_response.read.return_value.decode.return_value = json.dumps(
                service_metadata
            )
        elif "returnCountOnly=True" in url_str:
            # Return invalid JSON for count request
            mock_response.read.return_value.decode.return_value = "invalid json{"

        mock_response.__enter__ = lambda self: mock_response
        mock_response.__exit__ = lambda self, *args: None
        return mock_response

    mocker.patch("urllib.request.urlopen", side_effect=mock_urlopen)
    return mock_urlopen


@pytest.fixture
def mock_esri_zero_count(mocker):
    """Mock ESRI service that returns zero count (no features found)."""

    def mock_urlopen(url, **kwargs):
        mock_response = mocker.Mock()
        url_str = str(url)

        if "?f=pjson" in url_str:
            service_metadata = {
                "capabilities": "Query,Create,Update,Delete",
                "maxRecordCount": 1000,
            }
            mock_response.read.return_value.decode.return_value = json.dumps(
                service_metadata
            )
        elif "returnCountOnly=True" in url_str:
            count_response = {"count": 0}  # No features found
            mock_response.read.return_value.decode.return_value = json.dumps(
                count_response
            )

        mock_response.__enter__ = lambda self: mock_response
        mock_response.__exit__ = lambda self, *args: None
        return mock_response

    mocker.patch("urllib.request.urlopen", side_effect=mock_urlopen)
    return mock_urlopen


@pytest.fixture
def mock_esri_geojson_error(mocker):
    """Mock ESRI service that fails when reading GeoJSON."""

    def mock_urlopen(url, **kwargs):
        mock_response = mocker.Mock()
        url_str = str(url)

        if "?f=pjson" in url_str:
            service_metadata = {
                "capabilities": "Query,Create,Update,Delete",
                "maxRecordCount": 1000,
            }
            mock_response.read.return_value.decode.return_value = json.dumps(
                service_metadata
            )
        elif "returnCountOnly=True" in url_str:
            count_response = {"count": 1}
            mock_response.read.return_value.decode.return_value = json.dumps(
                count_response
            )

        mock_response.__enter__ = lambda self: mock_response
        mock_response.__exit__ = lambda self, *args: None
        return mock_response

    # Mock geopandas.read_file to raise an exception
    def mock_read_file_error(url):
        raise Exception("Failed to read GeoJSON")

    mocker.patch("urllib.request.urlopen", side_effect=mock_urlopen)
    mocker.patch("geopandas.read_file", side_effect=mock_read_file_error)
    return mock_urlopen


@pytest.fixture
def mock_esri_pagination_needed(mocker):
    """Mock ESRI service that requires pagination (more records than max)."""

    def mock_urlopen(url, **kwargs):
        mock_response = mocker.Mock()
        url_str = str(url)

        if "?f=pjson" in url_str:
            service_metadata = {
                "capabilities": "Query,Create,Update,Delete",
                "maxRecordCount": 1,  # Force pagination
            }
            mock_response.read.return_value.decode.return_value = json.dumps(
                service_metadata
            )
        elif "returnCountOnly=True" in url_str:
            count_response = {"count": 2}  # More than maxRecordCount
            mock_response.read.return_value.decode.return_value = json.dumps(
                count_response
            )

        mock_response.__enter__ = lambda self: mock_response
        mock_response.__exit__ = lambda self, *args: None
        return mock_response

    # Mock geopandas.read_file to return different data for each page
    page_count = 0

    def mock_read_file_pagination(url):
        nonlocal page_count
        page_count += 1
        return gpd.GeoDataFrame(
            {"name": [f"Test Area {page_count}"]},
            geometry=[MOCK_GEOMETRY],
            crs="EPSG:4326",
        )

    mocker.patch("urllib.request.urlopen", side_effect=mock_urlopen)
    mocker.patch("geopandas.read_file", side_effect=mock_read_file_pagination)
    return mock_urlopen


@pytest.fixture
def mock_esri_pagination_error(mocker):
    """Mock ESRI service that fails during pagination."""

    def mock_urlopen(url, **kwargs):
        mock_response = mocker.Mock()
        url_str = str(url)

        if "?f=pjson" in url_str:
            service_metadata = {
                "capabilities": "Query,Create,Update,Delete",
                "maxRecordCount": 1,  # Force pagination
            }
            mock_response.read.return_value.decode.return_value = json.dumps(
                service_metadata
            )
        elif "returnCountOnly=True" in url_str:
            count_response = {"count": 2}  # More than maxRecordCount
            mock_response.read.return_value.decode.return_value = json.dumps(
                count_response
            )

        mock_response.__enter__ = lambda self: mock_response
        mock_response.__exit__ = lambda self, *args: None
        return mock_response

    # Mock geopandas.read_file to fail on second page
    page_count = 0

    def mock_read_file_fail_on_second(url):
        nonlocal page_count
        page_count += 1
        if page_count == 1:
            return gpd.GeoDataFrame(
                {"name": ["Test Area 1"]}, geometry=[MOCK_GEOMETRY], crs="EPSG:4326"
            )
        else:
            raise Exception("Failed to read second page")

    mocker.patch("urllib.request.urlopen", side_effect=mock_urlopen)
    mocker.patch("geopandas.read_file", side_effect=mock_read_file_fail_on_second)
    return mock_urlopen
