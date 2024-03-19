import sys, os, importlib, json
import shapely, rasterio, matplotlib

import pandas as pd
import geopandas as gpd
import numpy as np

from rasterio.crs import CRS
from h3 import h3
from shapely.geometry import Polygon, Point, mapping
from shapely.ops import unary_union
from urllib.request import urlopen
from tqdm import tqdm

import h3_helper
import GOSTRocks.rasterMisc as rMisc
import GOSTRocks.ntlMisc as ntl
from GOSTRocks.misc import tPrint

def generate_lvl0_lists(h3_lvl):
    """ generate a dictionary with keys as lvl0 codes with all children at h3_lvl level as values

    Parameters
    ----------
    h3_lvl : int
        h3 level to generate children of h0 parents

    Returns
    -------
    dict
        dictionary with keys as lvl0 codes with all children at h3_lvl level as values
    """
    # Get list of all h3 lvl 0 cells
    h3_lvl0 = list(h3.get_res0_indexes())

    # Generate list of all children of h3 lvl 0 cells
    h3_lvl0_children = {}
    for h3_0 in h3_lvl0:
        h3_lvl0_children[h3_0] = list(h3.h3_to_children(h3_0, h3_lvl))

    return h3_lvl0_children

def calculate_zonal_h3_list(h3_list, raster_data, output_file=""):
    """_summary_

    Parameters
    ----------
    h3_list : _type_
        _description_
    raster_data : _type_
        _description_
    output_file : _type_
        _description_
    """
    # Convert list of h3 cells to geometry
    hex_poly = lambda hex_id: Polygon(h3.h3_to_geo_boundary(hex_id, geo_json=True))
    
    all_polys = gpd.GeoSeries(list(map(hex_poly, h3_list)), index=h3_list, crs=4326)
    all_polys = gpd.GeoDataFrame(all_polys, crs=4326, columns=['geometry'])
    all_polys['shape_id'] = list(all_polys.index)

    res = rMisc.zonalStats(all_polys, raster_data)
    res = pd.DataFrame(res, columns=['SUM', 'MIN', 'MAX', 'MEAN'])

    if output_file != "":
        res.to_csv(output_file)
    
    return(res)