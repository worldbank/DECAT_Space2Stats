"""Space2Stats client for accessing the World Bank's spatial statistics API."""

import inspect
import json
import urllib
from typing import Dict, List, Literal, Optional

import geopandas as gpd
import pandas as pd
import requests
from pystac import Catalog


def download_esri_boundaries(url, layer, iso3) -> gpd.GeoDataFrame:
    """_summary_

    Parameters
    ----------
    url : _type_
        _description_
    layer : _type_
        _description_
    iso3 : _type_
        _description_

    Returns
    -------
    gpd.GeoDataFrame
        _description_
    """
    # Look at metadata of url
    with urllib.request.urlopen(f"{url}/?f=pjson") as service_url:
        service_data = json.loads(service_url.read().decode())

    queryable = ["Query" in service_data["capabilities"]]

    if queryable:
        query_url = f"{url}/{layer}/query"
        # get total number of records in complete service
        n_queries = service_data["maxRecordCount"]
        count_query = {
            "outFields": "*",
            "where": f"ISO_A3='{iso3}'",
            "returnCountOnly": True,
            "f": "json",
        }
        count_str = urllib.parse.urlencode(count_query)
        with urllib.request.urlopen(f"{query_url}?{count_str}") as count_url:
            count_json = json.loads(count_url.read().decode())
            n_records = count_json["count"]
        if n_records < n_queries:  # We can download all the data in a single query
            all_records_query = {
                "outFields": "*",
                "where": f"ISO_A3='{iso3}'",
                "returnGeometry": True,
                "f": "geojson",
            }
            query_str = urllib.parse.urlencode(all_records_query)
            all_query_url = f"{query_url}?{query_str}"
            return gpd.read_file(all_query_url)
        else:
            step_query = {
                "outFields": "*",
                "where": f"ISO_A3='{iso3}'",
                "returnGeometry": True,
                "f": "geojson",
                "resultRecordCount": n_queries,
                "resultOffset": 0,
            }
            for offset in range(0, n_records, n_queries):
                step_query["resultOffset"] = offset
                query_str = urllib.parse.urlencode(step_query)
                step_query_url = f"{query_url}?{query_str}"
                cur_res = gpd.read_file(step_query_url)
                if offset == 0:
                    gdf = cur_res
                else:
                    gdf = pd.concat([gdf, cur_res])
            return gdf
    else:
        raise ValueError("Service is not queryable :(")


class Space2StatsClient:
    """Client for interacting with the Space2Stats API.

    This client is provided by the World Bank to access spatial statistics data.
    The World Bank makes no warranties regarding the accuracy, reliability or
    completeness of the results and content.
    """

    def __init__(
        self, base_url: str = "https://space2stats.ds.io", verify_ssl: bool = True
    ):
        """Initialize the Space2Stats client.

        Parameters
        ----------
        base_url : str
            Base URL for the Space2Stats API
        verify_ssl : bool
            Whether to verify SSL certificates in requests (default: True)
        """
        if not isinstance(verify_ssl, bool):
            raise TypeError("verify_ssl must be a boolean value (True or False)")

        self.base_url = base_url
        self.verify_ssl = verify_ssl
        self.summary_endpoint = f"{base_url}/summary"
        self.aggregation_endpoint = f"{base_url}/aggregate"
        self.fields_endpoint = f"{base_url}/fields"
        self.timeseries_endpoint = f"{base_url}/timeseries"
        self.timeseries_by_hexids_endpoint = f"{base_url}/timeseries_by_hexids"
        self.timeseries_fields_endpoint = f"{base_url}/timeseries/fields"
        self.catalog = Catalog.from_file(
            "https://raw.githubusercontent.com/worldbank/DECAT_Space2Stats/refs/heads/main/space2stats_api/src/space2stats_ingest/METADATA/stac/catalog.json"
        )

    def _handle_api_error(self, response: requests.Response) -> None:
        """Handle API error responses with specific handling for different status codes."""
        caller = inspect.currentframe().f_back.f_code.co_name

        # Special handling for 503 Service Unavailable from API Gateway
        if response.status_code == 503:
            try:
                error_data = response.json()
                error_message = error_data.get("message", "Service Unavailable")
            except Exception:
                error_message = "Service Unavailable"

            # Check if this is the basic API Gateway timeout message
            if error_message == "Service Unavailable":
                raise Exception(
                    f"Failed to {caller} (HTTP 503): Service Unavailable - "
                    f"Request timed out due to API Gateway timeout limit (30 seconds). "
                    f"Try reducing the request size:\n"
                    f"  • Use fewer hexagon IDs or a smaller geographic area\n"
                    f"  • Request fewer fields at a time\n"
                    f"  • For polygon AOI requests, use a smaller area or simpler geometry\n"
                    f"  • Consider breaking large requests into smaller chunks"
                )
            else:
                raise Exception(f"Failed to {caller} (HTTP 503): {error_message}")

        try:
            error_data = response.json()

            # Handle both API Gateway format and application format
            if "message" in error_data and "error" not in error_data:
                # This is API Gateway's format (e.g., 413 from API Gateway limits)
                if response.status_code == 413:
                    error_message = (
                        "Request Entity Too Large: The request payload exceeds API Gateway limits of 10MB\n\n"
                        "Hint: Try again with a smaller request or making multiple requests "
                        "with smaller payloads. The factors to consider are the number of "
                        "hexIds (ie. AOI), the number of fields requested, and the date range (if timeseries is requested)."
                    )
                else:
                    error_message = error_data.get("message", response.text)
            else:
                # This is your application's format
                error_title = error_data.get("error", "API Error")
                error_detail = error_data.get("detail", response.text)
                error_message = f"{error_title}: {error_detail}"

                # Add hint if available
                hint = error_data.get("hint", "")
                if hint:
                    error_message += f"\n\nHint: {hint}"

                # Add suggestions if available
                suggestions = error_data.get("suggestions", [])
                if suggestions:
                    error_message += "\n\nSuggestions:"
                    for suggestion in suggestions:
                        error_message += f"\n  • {suggestion}"

        except Exception:
            error_message = response.text

        raise Exception(
            f"Failed to {caller} (HTTP {response.status_code}): {error_message}"
        )

    def get_topics(self) -> pd.DataFrame:
        """Get a table of items (dataset themes/topics) from the STAC catalog."""
        items = self.catalog.get_all_items()
        items = list(items)
        topics = [
            {
                i.id: {
                    k: v
                    for k, v in i.properties.items()
                    if k in ["name", "description", "source_data"]
                }
            }
            for i in items
        ]
        topics = [pd.DataFrame(t) for t in topics]
        topics = pd.concat(topics, axis=1)
        topics.index.name = "Item ID"
        return topics.transpose()

    def get_properties(self, item_id: str) -> Dict:
        """Get a table with a description of variables for a given dataset (item)."""
        collection = next(self.catalog.get_collections())
        item = collection.get_item(item_id)
        properties = item.properties["table:columns"]
        return pd.DataFrame(properties)

    def get_fields(self) -> List[str]:
        """Get a list of all available fields from the Space2Stats API.

        Returns
        -------
        List[str]
            A list of field names that can be used with the API.

        Raises
        ------
        Exception
            If the API request fails.
        """
        response = requests.get(self.fields_endpoint, verify=self.verify_ssl)
        if response.status_code != 200:
            raise Exception(f"Failed to get fields: {response.text}")

        return response.json()

    def fetch_admin_boundaries(self, iso3, adm, source="WB") -> gpd.GeoDataFrame:
        """Fetch administrative boundaries as geopandas GeoDataFrame.

        Parameters
        ----------
        iso3 : str
            Country code
        adm : str
            Administrative level (e.g., "ADM0", "ADM1", "ADM2")
        source : str
            Data source options are "WB" for World Bank or "GB" for GeoBoundaries (default: "WB")

        Returns
        -------
        gpd.GeoDataFrame
            A GeoDataFrame containing the administrative boundaries.
        """
        if source == "WB":
            esri_url = "https://services.arcgis.com/iQ1dY19aHwbSDYIF/arcgis/rest/services/World_Bank_Global_Administrative_Divisions/FeatureServer"
            layer = 1
            if adm == "ADM1":
                layer = 2
            elif adm == "ADM2":
                layer = 3
            return download_esri_boundaries(esri_url, layer, iso3)
        elif source == "GB":
            url = f"https://www.geoboundaries.org/api/current/gbOpen/{iso3}/{adm}/"
            res = requests.get(url, verify=self.verify_ssl).json()
            return gpd.read_file(res["gjDownloadURL"])
        else:
            raise ValueError("Source must be 'WB' or 'GB'")

    def get_summary(
        self,
        gdf: gpd.GeoDataFrame,
        spatial_join_method: Literal["touches", "centroid", "within"],
        fields: List[str],
        geometry: Optional[Literal["polygon", "point"]] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """Extract h3 level data from Space2Stats for a GeoDataFrame.

        Parameters
        ----------
        gdf : GeoDataFrame
            The Areas of Interest

        spatial_join_method : ["touches", "centroid", "within"]
            The method to use for performing the spatial join between the AOI and H3 cells
                - "touches": Includes H3 cells that touch the AOI
                - "centroid": Includes H3 cells where the centroid falls within the AOI
                - "within": Includes H3 cells entirely within the AOI

        fields : List[str]
            A list of field names to retrieve from the statistics table.

        geometry : Optional["polygon", "point"]
            Specifies if the H3 geometries should be included in the response.

        verbose : bool
            Whether to display progress messages (default: True)

        Returns
        -------
        DataFrame
            A DataFrame with the requested fields for each H3 cell.
        """
        if spatial_join_method not in ["touches", "centroid", "within"]:
            raise ValueError("Input should be 'touches', 'centroid' or 'within'")

        total_boundaries = len(gdf)
        res_all = {}

        for boundary_num, (idx, row) in enumerate(gdf.iterrows(), 1):
            if verbose:
                print(
                    f"Fetching data for boundary {boundary_num} of {total_boundaries}..."
                )

            request_payload = {
                "aoi": {
                    "type": "Feature",
                    "geometry": row.geometry.__geo_interface__,
                    "properties": {},
                },
                "spatial_join_method": spatial_join_method,
                "fields": fields,
                "geometry": geometry,
            }
            response = requests.post(
                self.summary_endpoint, json=request_payload, verify=self.verify_ssl
            )

            if response.status_code != 200:
                self._handle_api_error(response)

            summary_data = response.json()
            if not summary_data:
                print(f"Failed to get summary for {idx}")
                summary_data = pd.DataFrame()

            df = pd.DataFrame(summary_data)
            res_all[idx] = df

        res_all = pd.concat(res_all, names=["index_gdf", "index_h3"])
        res_all = res_all.reset_index()
        gdf_copy = gdf.copy()
        gdf_copy.drop(columns=["geometry"], inplace=True)
        res_all = gdf_copy.merge(res_all, left_index=True, right_on="index_gdf")
        return res_all

    def get_aggregate(
        self,
        gdf: gpd.GeoDataFrame,
        spatial_join_method: Literal["touches", "centroid", "within"],
        fields: list,
        aggregation_type: Literal["sum", "avg", "count", "max", "min"],
        verbose: bool = True,
    ) -> pd.DataFrame:
        """Extract summary statistic from underlying H3 Space2Stats data.

        Parameters
        ----------
        gdf : GeoDataFrame
            The Areas of Interest

        spatial_join_method : ["touches", "centroid", "within"]
            The method to use for performing the spatial join

        fields : List[str]
            A list of field names to retrieve

        aggregation_type : ["sum", "avg", "count", "max", "min"]
            Statistical function to apply to each field per AOI.

        verbose : bool
            Whether to display progress messages (default: True)

        Returns
        -------
        DataFrame
            A DataFrame with the aggregated statistics.
        """
        if aggregation_type not in ["sum", "avg", "count", "max", "min"]:
            raise ValueError("Input should be 'sum', 'avg', 'count', 'max' or 'min'")

        total_boundaries = len(gdf)
        res_all = []

        for boundary_num, (idx, row) in enumerate(gdf.iterrows(), 1):
            if verbose:
                print(
                    f"Fetching data for boundary {boundary_num} of {total_boundaries}..."
                )

            request_payload = {
                "aoi": {
                    "type": "Feature",
                    "geometry": row.geometry.__geo_interface__,
                    "properties": {},
                },
                "spatial_join_method": spatial_join_method,
                "fields": fields,
                "aggregation_type": aggregation_type,
            }
            response = requests.post(
                self.aggregation_endpoint, json=request_payload, verify=self.verify_ssl
            )

            if response.status_code != 200:
                self._handle_api_error(response)

            aggregate_data = response.json()
            if not aggregate_data:
                print(f"Failed to get summary for {idx}")
                aggregate_data = pd.DataFrame()

            df = pd.DataFrame(aggregate_data, index=[idx])
            res_all.append(df)

        res_all = pd.concat(res_all)
        gdf_copy = gdf.copy()
        res_all = gdf_copy.join(res_all)
        return res_all

    def get_summary_by_hexids(
        self,
        hex_ids: List[str],
        fields: List[str],
        geometry: Optional[Literal["polygon", "point"]] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """Retrieve statistics for specific hex IDs.

        Parameters
        ----------
        hex_ids : List[str]
            List of H3 hexagon IDs to query
        fields : List[str]
            List of field names to retrieve from the statistics table
        geometry : Optional[Literal["polygon", "point"]]
            Specifies if the H3 geometries should be included in the response.
        verbose : bool
            Whether to display progress messages (default: True)

        Returns
        -------
        DataFrame
            A DataFrame with the requested fields for each H3 cell.
        """
        if verbose:
            print(f"Fetching data for {len(hex_ids)} hex IDs...")

        request_payload = {
            "hex_ids": hex_ids,
            "fields": fields,
            "geometry": geometry,
        }
        response = requests.post(
            f"{self.base_url}/summary_by_hexids",
            json=request_payload,
            verify=self.verify_ssl,
        )

        if response.status_code != 200:
            self._handle_api_error(response)

        summary_data = response.json()
        return pd.DataFrame(summary_data)

    def get_aggregate_by_hexids(
        self,
        hex_ids: List[str],
        fields: List[str],
        aggregation_type: Literal["sum", "avg", "count", "max", "min"],
        verbose: bool = True,
    ) -> pd.DataFrame:
        """Aggregate statistics for specific hex IDs.

        Parameters
        ----------
        hex_ids : List[str]
            List of H3 hexagon IDs to aggregate
        fields : List[str]
            List of field names to aggregate
        aggregation_type : Literal["sum", "avg", "count", "max", "min"]
            Type of aggregation to perform
        verbose : bool
            Whether to display progress messages (default: True)

        Returns
        -------
        DataFrame
            A DataFrame with the aggregated statistics.
        """
        if verbose:
            print(f"Aggregating data for {len(hex_ids)} hex IDs...")

        request_payload = {
            "hex_ids": hex_ids,
            "fields": fields,
            "aggregation_type": aggregation_type,
        }
        response = requests.post(
            f"{self.base_url}/aggregate_by_hexids",
            json=request_payload,
            verify=self.verify_ssl,
        )

        if response.status_code != 200:
            self._handle_api_error(response)

        aggregate_data = response.json()
        return pd.DataFrame([aggregate_data])

    def get_timeseries_fields(self) -> List[str]:
        """Get available fields from the timeseries table.

        Returns
        -------
        List[str]
            List of field names available in the timeseries table

        Raises
        ------
        Exception
            If the API request fails
        """
        response = requests.get(self.timeseries_fields_endpoint)
        if response.status_code != 200:
            self._handle_api_error(response)
        return response.json()

    def get_timeseries(
        self,
        gdf: gpd.GeoDataFrame,
        spatial_join_method: Literal["touches", "centroid", "within"],
        fields: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        geometry: Optional[Literal["polygon", "point"]] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """Get timeseries data for areas of interest.

        Parameters
        ----------
        gdf : GeoDataFrame
            The Areas of Interest
        spatial_join_method : ["touches", "centroid", "within"]
            The method to use for performing the spatial join between the AOI and H3 cells
                - "touches": Includes H3 cells that touch the AOI
                - "centroid": Includes H3 cells where the centroid falls within the AOI
                - "within": Includes H3 cells entirely within the AOI
        fields : List[str]
            List of fields to retrieve.
        start_date : Optional[str]
            Start date for filtering data (format: 'YYYY-MM-DD')
        end_date : Optional[str]
            End date for filtering data (format: 'YYYY-MM-DD')
        verbose : bool
            Whether to display progress messages (default: True)

        Returns
        -------
        DataFrame
            A DataFrame containing timeseries data for each hex ID and date
        """
        total_boundaries = len(gdf)
        res_all = []

        for boundary_num, (idx, row) in enumerate(gdf.iterrows(), 1):
            if verbose:
                print(
                    f"Fetching data for boundary {boundary_num} of {total_boundaries}..."
                )

            request_payload = {
                "aoi": {
                    "type": "Feature",
                    "geometry": row.geometry.__geo_interface__,
                    "properties": {},
                },
                "spatial_join_method": spatial_join_method,
                "start_date": start_date,
                "end_date": end_date,
                "fields": fields,
                "geometry": geometry,
            }

            response = requests.post(self.timeseries_endpoint, json=request_payload)
            if response.status_code != 200:
                self._handle_api_error(response)

            timeseries_data = response.json()
            if timeseries_data:
                df = pd.DataFrame(timeseries_data)
                df["area_id"] = idx
                res_all.append(df)

        if not res_all:
            return pd.DataFrame()

        result = pd.concat(res_all, ignore_index=True)

        gdf_copy = gdf.copy()
        gdf_copy.drop(columns=["geometry"], inplace=True)
        result = gdf_copy.merge(result, left_index=True, right_on="area_id")

        return result

    def get_timeseries_by_hexids(
        self,
        hex_ids: List[str],
        fields: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        geometry: Optional[Literal["polygon", "point"]] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """Get timeseries data for specific hex IDs.

        Parameters
        ----------
        hex_ids : List[str]
            List of H3 hexagon IDs to query
        fields : List[str]
            List of fields to retrieve from the timeseries table
        start_date : Optional[str]
            Start date for filtering data (format: 'YYYY-MM-DD')
        end_date : Optional[str]
            End date for filtering data (format: 'YYYY-MM-DD')
        geometry : Optional[Literal["polygon", "point"]]
            Specifies if the H3 geometries should be included in the response.
        verbose : bool
            Whether to display progress messages (default: True)

        Returns
        -------
        DataFrame
            A DataFrame containing timeseries data for each hex ID and date
        """
        if verbose:
            print(f"Fetching timeseries data for {len(hex_ids)} hex IDs...")

        request_payload = {
            "hex_ids": hex_ids,
            "fields": fields,
            "start_date": start_date,
            "end_date": end_date,
            "geometry": geometry,
        }

        # Remove None values from payload
        request_payload = {k: v for k, v in request_payload.items() if v is not None}

        response = requests.post(
            self.timeseries_by_hexids_endpoint, json=request_payload
        )
        if response.status_code != 200:
            self._handle_api_error(response)

        timeseries_data = response.json()
        return pd.DataFrame(timeseries_data)

    # ADM2 Summaries functionality for World Bank DDH API
    WORLD_BANK_DDH_DATASETS = {
        "urbanization": "DR0095357",
        "nighttimelights": "DR0095356",
        "population": "DR0095354",
        "flood_exposure": "DR0095355",
    }

    WORLD_BANK_DDH_BASE_URL = (
        "https://datacatalogapi.worldbank.org/ddhxext/v3/resources"
    )

    def get_adm2_summaries(
        self,
        dataset: Literal[
            "urbanization", "nighttimelights", "population", "flood_exposure"
        ],
        iso3_filter: Optional[str] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """Retrieve ADM2 summaries from World Bank DDH API.

        Parameters
        ----------
        dataset : Literal["urbanization", "nighttimelights", "population", "flood_exposure"]
            The dataset to retrieve:
            - "urbanization": Urban and rural settlement data
            - "nighttimelights": Nighttime lights intensity data
            - "population": Population statistics
            - "flood_exposure": Flood exposure risk data
        iso3_filter : Optional[str]
            ISO3 country code to filter by (e.g., 'AND' for Andorra, 'USA' for United States)
        verbose : bool
            Whether to display progress messages (default: True)

        Returns
        -------
        DataFrame
            A DataFrame containing ADM2-level statistics records

        Raises
        ------
        ValueError
            If an invalid dataset is specified
        Exception
            If the API request fails
        """
        if dataset not in self.WORLD_BANK_DDH_DATASETS:
            raise ValueError(
                f"Invalid dataset. Must be one of: {list(self.WORLD_BANK_DDH_DATASETS.keys())}"
            )

        if verbose:
            print(f"Fetching {dataset} data from World Bank DDH API...")
            if iso3_filter:
                print(f"Filtering by ISO3: {iso3_filter}")

        resource_id = self.WORLD_BANK_DDH_DATASETS[dataset]
        url = f"{self.WORLD_BANK_DDH_BASE_URL}/{resource_id}/data"

        # Build query parameters
        params = {}
        if iso3_filter:
            params["filter"] = f"ISO3='{iso3_filter}'"

        try:
            response = requests.get(
                url, params=params, verify=self.verify_ssl, timeout=30.0
            )
            response.raise_for_status()

            data = response.json()
            records = data.get("value", [])

            if verbose:
                total_count = data.get("count", len(records))
                print(
                    f"Retrieved {len(records)} records (total available: {total_count})"
                )

            return pd.DataFrame(records)

        except requests.HTTPError as e:
            raise Exception(f"Failed to fetch data from World Bank DDH API: {e}")
        except Exception as e:
            raise Exception(f"Error processing World Bank DDH API response: {e}")

    def get_adm2_dataset_info(self) -> pd.DataFrame:
        """Get information about available ADM2 datasets.

        Returns
        -------
        DataFrame
            A DataFrame with information about each available ADM2 dataset
        """
        datasets_info = [
            {
                "dataset": "urbanization",
                "resource_id": "DR0095357",
                "description": "Urban and rural settlement data - GHS settlement model data",
                "url": f"{self.WORLD_BANK_DDH_BASE_URL}/DR0095357/data",
            },
            {
                "dataset": "nighttimelights",
                "resource_id": "DR0095356",
                "description": "Nighttime lights intensity data - satellite-derived luminosity measurements",
                "url": f"{self.WORLD_BANK_DDH_BASE_URL}/DR0095356/data",
            },
            {
                "dataset": "population",
                "resource_id": "DR0095354",
                "description": "Population statistics - demographic data",
                "url": f"{self.WORLD_BANK_DDH_BASE_URL}/DR0095354/data",
            },
            {
                "dataset": "flood_exposure",
                "resource_id": "DR0095355",
                "description": "Flood exposure risk data - flood hazard and exposure metrics",
                "url": f"{self.WORLD_BANK_DDH_BASE_URL}/DR0095355/data",
            },
        ]

        return pd.DataFrame(datasets_info)
