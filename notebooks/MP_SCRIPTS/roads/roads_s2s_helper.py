import sys, os, io, multiprocessing
import urllib3
import boto3
import rasterio
import overturemaps
import h3
import shapely

import geopandas as gpd
import pandas as pd
import GOSTrocks.rasterMisc as rMisc

from GOSTrocks.misc import tPrint
from shapely.geometry import shape, box, Polygon
from urllib3.exceptions import InsecureRequestWarning
from tqdm.notebook import tqdm
from botocore import UNSIGNED
from botocore.config import Config
from pystac_client import Client

sys.path.append("../../src")
import global_zonal
import h3_helper

sys.path.insert(0, r"C:\WBG\Work\Code\GOSTrocks\src")
import GOSTrocks.dataMisc as dMisc

sys.path.append(r"C:\WBG\Work\Code\GOSTnetsraster\src")
import GOSTnetsraster.market_access as ma
import GOSTnetsraster.conversion_tables as speed_tables

import urllib
import requests
from io import BytesIO

GeoE3_roads_classification = {'bus_guideway':'OSMLR level 0',
'construction':'OSMLR level 0',
'escape':'OSMLR level 0',
'proposed':'OSMLR level 0',
'raceway':'OSMLR level 0',
'motorway': 'OSMLR level 1',
 'motorway_link': 'OSMLR level 1',
 'trunk': 'OSMLR level 1',
 'trunk_link': 'OSMLR level 1',
 'primary': 'OSMLR level 2',
 'primary_link': 'OSMLR level 2',
'share_busway': 'OSMLR level 2',
'shoulder': 'OSMLR level 2',
'bridleway': 'OSMLR level 3',
'road': 'OSMLR level 3',
'secondary': 'OSMLR level 3',
 'secondary_link': 'OSMLR level 3',
 'service': 'OSMLR level 3',
'link': 'OSMLR level 3',
'unclassified': 'OSMLR level 3',
'tertiary': 'OSMLR level 4',
 'tertiary_link': 'OSMLR level 4',
 'cycleway': 'OSMLR level 4',
'path': 'OSMLR level 4',
'shared_lane': 'OSMLR level 4',
 'unclassified_link': 'OSMLR level 3',
 'residential': 'OSMLR level 5',
 'residential_link': 'OSMLR level 5',
 'footway': 'OSMLR level 5',
'living_street': 'OSMLR level 5',
'pedestrian': 'OSMLR level 5',
'steps': 'OSMLR level 5',
'crossing': 'OSMLR level 5',
'lane': 'OSMLR level 5',
'separate': 'OSMLR level 5',
'track': 'OSMLR level 5'}


def download_wb_boundaries(level: str,
                            iso3: str=None,                                                       
                            url: str='https://services.arcgis.com/iQ1dY19aHwbSDYIF/arcgis/rest/services/World_Bank_Global_Administrative_Divisions/FeatureServer/{level}/query?',
                            verify_ssl: bool=False                            
                           ):
    """ Download WB_GAD boundaries from ArcGIS REST API
    Parameters
    ----------
    level : str
        Administrative level to download, must be one of "ADM0_Lines", "ADM0_All", "ADM0_Countries", "ADM1", "ADM2", "NDLSA"
    iso3 : str, optional
        ISO3 code of country to download, by default None which downloads all countries
    url : str, optional 
        URL to the ArcGIS REST API, by default 'https://services.arcgis.com/iQ1dY19aHwbSDYIF/arcgis/rest/services/World_Bank_Global_Administrative_Divisions/FeatureServer/{level}/query?'
    verify_ssl : bool, optional
        Whether to verify SSL certificates when making the request, by default False
    """
    if level not in ["ADM0_Lines", "ADM0_All", "ADM0", "ADM1", "ADM2", "NDLSA"]:
        raise ValueError("Level must be one of 'ADM0_Lines', 'ADM0_All', 'ADM0', 'ADM1', 'ADM2', 'NDLSA'")
    level_dictionary = {
        "ADM0_Lines": "0",
        "ADM0_All": "4",
        "ADM0": "1",
        "ADM1": "2",
        "ADM2": "3",
        "NDLSA": "4"
    }
    query_params = {
        "outfields": "*",
        "f": "pgeojson"
    }
    if iso3 is not None:
        query_params["where"] = f"\"ISO_A3\"='{iso3}'"
    query_str = urllib.parse.urlencode(query_params)
    query_url = url.format(level=level_dictionary[level]) + query_str
    response = requests.get(query_url, verify=verify_ssl) # <--- Ignore SSL if verify_ssl is False
    adm_gdf = gpd.read_file(BytesIO(response.content))
    return(adm_gdf)

def calculate_ewing_connectivity_index(roads_shape):
    # Get a list of all coordinates in the multi-line geometry
    coords = []
    geom_cnt = 0
    for geom in roads_shape.geoms:
        if geom.geom_type == 'LineString':
            geom_cnt += 1
            coords.extend(list(geom.coords))
        elif geom.geom_type == 'MultiLineString':
            for line in geom.geoms:
                geom_cnt += 1
                coords.extend(list(line.coords))

    # Get a dataframe of the unique coordinates with count of occurrences
    coords_df = pd.DataFrame(coords, columns=['x', 'y'])
    coords_df['count'] = 1
    coords_df = coords_df.groupby(['x', 'y']).count().reset_index()
    coords_df.sort_values('count', ascending=False).head(10)

    # Calculate the Ewing connectivity index as the ratio of the number of coords_df and the number of lines
    ewing = geom_cnt / coords_df.shape[0]
    return ewing