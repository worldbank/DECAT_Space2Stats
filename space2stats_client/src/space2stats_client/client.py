"""Space2Stats client for accessing the World Bank's spatial statistics API."""

from typing import Dict, List, Literal, Optional

import geopandas as gpd
import pandas as pd
import requests
from pystac import Catalog


class Space2StatsClient:
    """Client for interacting with the Space2Stats API.

    This client is provided by the World Bank to access spatial statistics data.
    The World Bank makes no warranties regarding the accuracy, reliability or
    completeness of the results and content.
    """

    def __init__(self, base_url: str = "https://space2stats.ds.io"):
        """Initialize the Space2Stats client.

        Parameters
        ----------
        base_url : str
            Base URL for the Space2Stats API
        """
        self.base_url = base_url
        self.summary_endpoint = f"{base_url}/summary"
        self.aggregation_endpoint = f"{base_url}/aggregate"
        self.fields_endpoint = f"{base_url}/fields"
        self.catalog = Catalog.from_file(
            "https://raw.githubusercontent.com/worldbank/DECAT_Space2Stats/refs/heads/main/space2stats_api/src/space2stats_ingest/METADATA/stac/catalog.json"
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
        response = requests.get(self.fields_endpoint)
        if response.status_code != 200:
            raise Exception(f"Failed to get fields: {response.text}")

        return response.json()

    @staticmethod
    def fetch_admin_boundaries(iso3: str, adm: str) -> gpd.GeoDataFrame:
        """Fetch administrative boundaries from GeoBoundaries API."""
        url = f"https://www.geoboundaries.org/api/current/gbOpen/{iso3}/{adm}/"
        res = requests.get(url).json()
        return gpd.read_file(res["gjDownloadURL"])

    def get_summary(
        self,
        gdf: gpd.GeoDataFrame,
        spatial_join_method: Literal["touches", "centroid", "within"],
        fields: List[str],
        geometry: Optional[Literal["polygon", "point"]] = None,
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

        Returns
        -------
        DataFrame
            A DataFrame with the requested fields for each H3 cell.
        """
        res_all = {}
        for idx, row in gdf.iterrows():
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
            response = requests.post(self.summary_endpoint, json=request_payload)

            if response.status_code != 200:
                raise Exception(f"Failed to get summary: {response.text}")

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

        Returns
        -------
        DataFrame
            A DataFrame with the aggregated statistics.
        """
        res_all = []
        for idx, row in gdf.iterrows():
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
            response = requests.post(self.aggregation_endpoint, json=request_payload)

            if response.status_code != 200:
                raise Exception(f"Failed to get aggregate: {response.text}")

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

        Returns
        -------
        DataFrame
            A DataFrame with the requested fields for each H3 cell.
        """
        request_payload = {
            "hex_ids": hex_ids,
            "fields": fields,
            "geometry": geometry,
        }
        response = requests.post(
            f"{self.base_url}/summary_by_hexids", json=request_payload
        )

        if response.status_code != 200:
            raise Exception(f"Failed to get summary by hexids: {response.text}")

        summary_data = response.json()
        return pd.DataFrame(summary_data)

    def get_aggregate_by_hexids(
        self,
        hex_ids: List[str],
        fields: List[str],
        aggregation_type: Literal["sum", "avg", "count", "max", "min"],
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

        Returns
        -------
        DataFrame
            A DataFrame with the aggregated statistics.
        """
        request_payload = {
            "hex_ids": hex_ids,
            "fields": fields,
            "aggregation_type": aggregation_type,
        }
        response = requests.post(
            f"{self.base_url}/aggregate_by_hexids", json=request_payload
        )

        if response.status_code != 200:
            raise Exception(f"Failed to get aggregate by hexids: {response.text}")

        aggregate_data = response.json()
        return pd.DataFrame([aggregate_data])
