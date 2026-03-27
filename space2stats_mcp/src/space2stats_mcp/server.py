"""MCP server for World Bank Space2Stats spatial statistics."""

import json
import os
import urllib.parse
import urllib.request
from typing import Literal, Optional

import requests
from mcp.server.fastmcp import FastMCP
from pystac import Catalog

mcp = FastMCP(
    "space2stats",
    instructions=(
        "Space2Stats provides sub-national geospatial statistics from the World Bank. "
        "Data is indexed by H3 hexagons (level 6). Use `list_fields` or `list_topics` "
        "to discover available data, then query with an AOI (GeoJSON) or specific hex IDs."
    ),
)

BASE_URL = os.environ.get("SPACE2STATS_BASE_URL", "https://space2stats.ds.io")
CATALOG_URL = "https://raw.githubusercontent.com/worldbank/DECAT_Space2Stats/refs/heads/main/space2stats_api/src/space2stats_ingest/METADATA/stac/catalog.json"

_catalog_cache: Optional[Catalog] = None


def _get_catalog() -> Catalog:
    """Return a cached STAC catalog instance."""
    global _catalog_cache
    if _catalog_cache is None:
        _catalog_cache = Catalog.from_file(CATALOG_URL)
    return _catalog_cache


def _get_field_metadata(fields: list[str]) -> dict:
    """Look up STAC metadata (description, topic, source) for queried fields."""
    catalog = _get_catalog()
    items = list(catalog.get_all_items())

    field_meta = {}
    for field in fields:
        for item in items:
            columns = item.properties.get("table:columns", [])
            for col in columns:
                if col["name"] == field:
                    field_meta[field] = {
                        "description": col.get("description", ""),
                        "topic": item.properties.get("name", item.id),
                        "source": item.properties.get("source_data", ""),
                    }
                    break
            if field in field_meta:
                break

    return field_meta


def _api_url(path: str) -> str:
    return f"{BASE_URL}/{path.lstrip('/')}"


def _post(endpoint: str, payload: dict) -> dict | list:
    resp = requests.post(_api_url(endpoint), json=payload, timeout=60)
    if resp.status_code != 200:
        try:
            detail = resp.json()
        except Exception:
            detail = resp.text
        raise RuntimeError(f"API error {resp.status_code}: {detail}")
    return resp.json()


def _get(endpoint: str) -> dict | list:
    resp = requests.get(_api_url(endpoint), timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"API error {resp.status_code}: {resp.text}")
    return resp.json()


# ── Discovery tools ──────────────────────────────────────────────────────────


@mcp.tool()
def list_fields() -> list[str]:
    """List all available field names that can be queried from Space2Stats."""
    return _get("fields")


@mcp.tool()
def list_timeseries_fields() -> list[str]:
    """List available fields from the Space2Stats timeseries table."""
    return _get("timeseries/fields")


@mcp.tool()
def list_topics() -> list[dict]:
    """List dataset topics/themes with descriptions from the STAC catalog.

    Returns a list of items with their name, description, and source data info.
    Use this to understand what data is available before querying.
    """
    catalog = _get_catalog()
    items = list(catalog.get_all_items())
    return [
        {
            "id": item.id,
            **{
                k: v
                for k, v in item.properties.items()
                if k in ["name", "description", "source_data"]
            },
        }
        for item in items
    ]


@mcp.tool()
def get_topic_fields(item_id: str) -> list[dict]:
    """Get detailed field descriptions for a specific dataset topic.

    Parameters
    ----------
    item_id : str
        The topic/item ID from list_topics (e.g., "world_pop", "nighttime_lights").
    """
    catalog = _get_catalog()
    collection = next(catalog.get_collections())
    item = collection.get_item(item_id)
    if item is None:
        raise ValueError(f"Item '{item_id}' not found in catalog.")
    return item.properties.get("table:columns", [])


# ── Boundary tools ───────────────────────────────────────────────────────────


ESRI_BASE_URL = "https://services.arcgis.com/iQ1dY19aHwbSDYIF/arcgis/rest/services/World_Bank_Global_Administrative_Divisions/FeatureServer"
ESRI_LAYER_MAP = {"ADM0": 1, "ADM1": 2, "ADM2": 3}


def _fetch_esri_boundaries(iso3: str, adm: str) -> dict:
    """Fetch boundaries from World Bank ESRI FeatureServer as GeoJSON."""
    layer = ESRI_LAYER_MAP[adm]
    query_url = f"{ESRI_BASE_URL}/{layer}/query"
    where = f"ISO_A3='{iso3}'"

    # Check record count
    count_params = urllib.parse.urlencode(
        {"where": where, "returnCountOnly": True, "f": "json"}
    )
    with urllib.request.urlopen(f"{query_url}?{count_params}", timeout=60) as resp:
        count_data = json.loads(resp.read().decode())

    n_records = int(count_data.get("count", 0))
    if n_records == 0:
        raise ValueError(f"No features found for ISO3 code '{iso3}' at {adm}")

    # Get max page size from layer metadata
    meta_url = f"{ESRI_BASE_URL}/{layer}?f=pjson"
    with urllib.request.urlopen(meta_url, timeout=30) as resp:
        meta = json.loads(resp.read().decode())
    max_records = int(meta.get("maxRecordCount", 1000))

    # Fetch features (with paging if needed)
    all_features = []
    for offset in range(0, n_records, max_records):
        params = urllib.parse.urlencode(
            {
                "outFields": "*",
                "where": where,
                "returnGeometry": True,
                "f": "geojson",
                "resultRecordCount": max_records,
                "resultOffset": offset,
            }
        )
        with urllib.request.urlopen(f"{query_url}?{params}", timeout=60) as resp:
            page = json.loads(resp.read().decode())
        all_features.extend(page.get("features", []))

    return {"type": "FeatureCollection", "features": all_features}


def _fetch_geoboundaries(iso3: str, adm: str) -> dict:
    """Fetch boundaries from GeoBoundaries API as GeoJSON."""
    url = f"https://www.geoboundaries.org/api/current/gbOpen/{iso3}/{adm}/"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    geojson_resp = requests.get(data["gjDownloadURL"], timeout=60)
    geojson_resp.raise_for_status()
    return geojson_resp.json()


@mcp.tool()
def fetch_admin_boundaries(
    iso3: str,
    adm: Literal["ADM0", "ADM1", "ADM2"],
    source: Literal["WB", "GB"] = "WB",
) -> dict:
    """Fetch administrative boundaries for a country as GeoJSON.

    Use this to get an AOI that can be passed to the query tools.

    Parameters
    ----------
    iso3 : str
        ISO3 country code (e.g., "KEN" for Kenya, "BRA" for Brazil).
    adm : str
        Administrative level: "ADM0" (country), "ADM1" (region/province), "ADM2" (district).
    source : str
        Boundary source: "WB" for World Bank (default) or "GB" for GeoBoundaries.
    """
    if source == "WB":
        return _fetch_esri_boundaries(iso3, adm)
    elif source == "GB":
        return _fetch_geoboundaries(iso3, adm)
    else:
        raise ValueError("Source must be 'WB' or 'GB'")


# ── Query tools ──────────────────────────────────────────────────────────────


@mcp.tool()
def get_summary(
    aoi: dict,
    spatial_join_method: Literal["touches", "centroid", "within"],
    fields: list[str],
    geometry: Optional[Literal["polygon", "point"]] = None,
) -> list[dict]:
    """Get H3 hex-level statistics for an area of interest.

    Parameters
    ----------
    aoi : dict
        A GeoJSON Feature with a geometry (Polygon or MultiPolygon).
        Example: {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [...]}, "properties": {}}
    spatial_join_method : str
        How to match H3 cells to the AOI:
        - "touches": cells that intersect the AOI boundary
        - "centroid": cells whose centroid falls within the AOI
        - "within": cells entirely contained within the AOI
    fields : list[str]
        Field names to retrieve (use list_fields to see options).
    geometry : str, optional
        Include H3 cell geometry in response: "polygon" or "point".
    """
    payload = {
        "aoi": aoi,
        "spatial_join_method": spatial_join_method,
        "fields": fields,
        "geometry": geometry,
    }
    return _post("summary", payload)


@mcp.tool()
def get_summary_by_hexids(
    hex_ids: list[str],
    fields: list[str],
    geometry: Optional[Literal["polygon", "point"]] = None,
) -> list[dict]:
    """Get statistics for specific H3 hex IDs.

    Parameters
    ----------
    hex_ids : list[str]
        H3 level 6 hexagon IDs to query.
    fields : list[str]
        Field names to retrieve.
    geometry : str, optional
        Include H3 cell geometry: "polygon" or "point".
    """
    payload = {"hex_ids": hex_ids, "fields": fields, "geometry": geometry}
    return _post("summary_by_hexids", payload)


@mcp.tool()
def get_aggregate(
    aoi: dict,
    spatial_join_method: Literal["touches", "centroid", "within"],
    fields: list[str],
    aggregation_type: Literal["sum", "avg", "count", "max", "min"],
) -> dict:
    """Aggregate statistics across H3 cells for an area of interest.

    Returns a single aggregated result (e.g., total population for a region).

    Parameters
    ----------
    aoi : dict
        A GeoJSON Feature with a geometry.
    spatial_join_method : str
        How to match H3 cells: "touches", "centroid", or "within".
    fields : list[str]
        Field names to aggregate.
    aggregation_type : str
        Aggregation function: "sum", "avg", "count", "max", or "min".
    """
    payload = {
        "aoi": aoi,
        "spatial_join_method": spatial_join_method,
        "fields": fields,
        "aggregation_type": aggregation_type,
    }
    result = _post("aggregate", payload)
    field_metadata = _get_field_metadata(fields)
    return {
        "results": result,
        "metadata": {
            "fields": field_metadata,
            "spatial_join_method": spatial_join_method,
            "aggregation_type": aggregation_type,
        },
    }


@mcp.tool()
def get_aggregate_by_hexids(
    hex_ids: list[str],
    fields: list[str],
    aggregation_type: Literal["sum", "avg", "count", "max", "min"],
) -> dict:
    """Aggregate statistics for specific H3 hex IDs.

    Parameters
    ----------
    hex_ids : list[str]
        H3 hexagon IDs to aggregate.
    fields : list[str]
        Field names to aggregate.
    aggregation_type : str
        Aggregation function: "sum", "avg", "count", "max", or "min".
    """
    payload = {
        "hex_ids": hex_ids,
        "fields": fields,
        "aggregation_type": aggregation_type,
    }
    result = _post("aggregate_by_hexids", payload)
    field_metadata = _get_field_metadata(fields)
    return {
        "results": result,
        "metadata": {
            "fields": field_metadata,
            "aggregation_type": aggregation_type,
        },
    }


@mcp.tool()
def get_timeseries(
    aoi: dict,
    spatial_join_method: Literal["touches", "centroid", "within"],
    fields: list[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    geometry: Optional[Literal["polygon", "point"]] = None,
) -> list[dict]:
    """Get timeseries data for an area of interest.

    Parameters
    ----------
    aoi : dict
        A GeoJSON Feature with a geometry.
    spatial_join_method : str
        How to match H3 cells: "touches", "centroid", or "within".
    fields : list[str]
        Field names to retrieve (use list_timeseries_fields).
    start_date : str, optional
        Start date filter (YYYY-MM-DD).
    end_date : str, optional
        End date filter (YYYY-MM-DD).
    geometry : str, optional
        Include H3 cell geometry: "polygon" or "point".
    """
    payload = {
        "aoi": aoi,
        "spatial_join_method": spatial_join_method,
        "fields": fields,
    }
    if start_date:
        payload["start_date"] = start_date
    if end_date:
        payload["end_date"] = end_date
    if geometry:
        payload["geometry"] = geometry
    return _post("timeseries", payload)


@mcp.tool()
def get_timeseries_by_hexids(
    hex_ids: list[str],
    fields: list[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    geometry: Optional[Literal["polygon", "point"]] = None,
) -> list[dict]:
    """Get timeseries data for specific H3 hex IDs.

    Parameters
    ----------
    hex_ids : list[str]
        H3 hexagon IDs to query.
    fields : list[str]
        Field names to retrieve.
    start_date : str, optional
        Start date filter (YYYY-MM-DD).
    end_date : str, optional
        End date filter (YYYY-MM-DD).
    geometry : str, optional
        Include H3 cell geometry: "polygon" or "point".
    """
    payload = {"hex_ids": hex_ids, "fields": fields}
    if start_date:
        payload["start_date"] = start_date
    if end_date:
        payload["end_date"] = end_date
    if geometry:
        payload["geometry"] = geometry
    return _post("timeseries_by_hexids", payload)


def main():
    mcp.run()


if __name__ == "__main__":
    main()
