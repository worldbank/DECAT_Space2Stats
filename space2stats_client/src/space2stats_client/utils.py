import json
import urllib.parse
import urllib.request
from typing import Optional

import geopandas as gpd
import pandas as pd


def download_esri_boundaries(url, layer, iso3) -> gpd.GeoDataFrame:
    """Download admin boundaries from ESRI REST API

    Parameters
    ----------
    url : str
        Base FeatureServer URL (e.g., "https://services.arcgis.com/.../FeatureServer").
    layer : int
        Layer index within the FeatureServer.
    iso3 : str
        ISO3 country code filter (e.g., "USA").

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame containing the requested boundaries.
    """

    # 1) Read layer-level metadata (capabilities and maxRecordCount are per-layer)
    query_url = f"{url}/{layer}?f=pjson"
    try:
        with urllib.request.urlopen(query_url, timeout=30) as resp:
            service_data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        raise RuntimeError(
            f"Failed to fetch layer metadata ({query_url}): HTTP {e.code}"
        ) from e
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"Failed to reach ESRI service for metadata ({query_url}): {e.reason}"
        ) from e
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in layer metadata ({query_url})") from e

    # 2) Ensure the layer is queryable
    capabilities = str(service_data.get("capabilities"))
    queryable = "Query" in capabilities
    if not queryable:
        raise ValueError("Service is not queryable")

    # 3) Determine server page size
    max_records = int(service_data.get("maxRecordCount", 1000))

    # 4) Count matching records for the ISO3 filter
    query_url = f"{url}/{layer}/query"
    where = f"ISO_A3='{iso3}'"
    count_params = {
        "outFields": "*",
        "where": where,
        "returnCountOnly": True,
        "f": "json",
    }
    count_qs = urllib.parse.urlencode(count_params)
    try:
        with urllib.request.urlopen(f"{query_url}?{count_qs}", timeout=60) as resp:
            count_json = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        raise RuntimeError(
            f"Failed to fetch feature count ({query_url}): HTTP {e.code}"
        ) from e
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"Failed to reach ESRI service for count ({query_url}): {e.reason}"
        ) from e
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in feature count response ({query_url})") from e

    n_records = int(count_json.get("count", 0))
    if n_records == 0:
        raise ValueError(
            f"No features found for ISO3 code '{iso3}' in the specified layer"
        )

    # 5) If everything fits in one request, pull as GeoJSON in a single read
    if n_records <= max_records:
        all_params = {
            "outFields": "*",
            "where": where,
            "returnGeometry": True,
            "f": "geojson",
        }
        all_qs = urllib.parse.urlencode(all_params)
        all_url = f"{query_url}?{all_qs}"
        try:
            return gpd.read_file(all_url)
        except Exception as e:
            raise RuntimeError(
                f"Failed to read GeoJSON for single-page download ({all_url}): {e}"
            ) from e

    # 6) Otherwise, page through results and concatenate
    gdf = None
    for offset in range(0, n_records, max_records):
        step_params = {
            "outFields": "*",
            "where": where,
            "returnGeometry": True,
            "f": "geojson",
            "resultRecordCount": max_records,
            "resultOffset": offset,
        }
        step_qs = urllib.parse.urlencode(step_params)
        step_url = f"{query_url}?{step_qs}"
        try:
            page_gdf = gpd.read_file(step_url)
            if gdf is None:
                gdf = page_gdf
            else:
                gdf = pd.concat([gdf, page_gdf], ignore_index=True)
        except Exception as e:
            raise RuntimeError(
                f"Failed to read GeoJSON page at offset {offset} ({step_url}): {e}"
            ) from e

    return gdf
