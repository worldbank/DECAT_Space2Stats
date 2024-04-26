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
import GOSTrocks.rasterMisc as rMisc
import GOSTrocks.ntlMisc as ntl
from GOSTrocks.misc import tPrint

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

def zonal_stats_numerical(gdf, gdf_id, raster_file, out_file,
                          buffer0=False, minVal=None, maxVal=None, verbose=False):
    ''' Run zonal stats on a continuous raster file using a list of h3 cells
    '''
    if verbose:
        tPrint(f'Starting zonal stats on {raster_file}')
    if buffer0:
        gdf['geometry'] = gdf['geometry'].buffer(0)        
    res = rMisc.zonalStats(gdf, raster_file, minVal=minVal, maxVal=maxVal, verbose=verbose)
    res = pd.DataFrame(res, columns=['SUM', 'MIN', 'MAX', 'MEAN'])
    res['id'] = gdf[gdf_id].values
    if verbose:
        tPrint(f'**** finished {cName}')
    return({out_file:res})


def zonal_stats_categories(gdf, gdf_id, raster_file, categories, out_file,
                          buffer0=False, verbose=False):
    ''' Run zonal stats on a categorical raster file using a list of h3 cells
    '''
    if verbose:
        tPrint(f'Starting zonal stats on {raster_file}')
    if buffer0:
        gdf['geometry'] = gdf['geometry'].buffer(0)        
    res = rMisc.zonalStats(gdf, raster_file, rastType="C", unqVals=categories, verbose=verbose)
    res = pd.DataFrame(res, columns=[f'c_{x}' for x in categories])
    res['id'] = gdf[gdf_id].values
    if verbose:
        tPrint(f'**** finished {cName}')
    return({out_file:res})


def zonal_stats_categorical(gdf, gdf_id, raster_file, category_raster_file, out_file, categories=None, reclass_dict=None,
                          buffer0=False, minVal=None, maxVal=None, verbose=False):
    ''' Run zonal stats on a continuous raster file using a matching categorical raster 
        file and a list of h3 cells. For each defined category in the categorical 
        raster file, calculate the sum, min, max, mean for that category.
    '''
    
    tPrint(f'Starting zonal stats on {out_file}')
    if buffer0:
        gdf['geometry'] = gdf['geometry'].buffer(0)        
    
    #extract category raster to gdf extent
    cat_d, cat_profile = rMisc.clipRaster(category_raster_file, gdf)
    # reclasify if necessary
    if not reclass_dict is None:
        categories = []
        for key, range in reclass_dict.items():
            cat_d[(cat_d >= range[0]) & (cat_d <= range[1])] = key
            categories.append(key)
    # extract raster to gdf extent
    rast_d, rast_profile = rMisc.clipRaster(raster_file, gdf)
        
    # standardize categorical raster to zonal raster
    final_zonal_res = []
    with rMisc.create_rasterio_inmemory(rast_profile, rast_d) as rast_src:
        with rMisc.create_rasterio_inmemory(cat_profile, cat_d) as cat_src:
            cat_d, cat_profile = rMisc.standardizeInputRasters(cat_src, rast_src, resampling_type='nearest')
            # Loop through each category
            for cur_cat in categories:
                cur_cat_d = (cat_d == cur_cat) * 1
                cur_rast_d = rast_d * cur_cat_d
                with rMisc.create_rasterio_inmemory(rast_profile, cur_rast_d) as cur_rast_src:
                    res = rMisc.zonalStats(gdf, cur_rast_src, minVal=minVal, maxVal=maxVal, verbose=verbose)
                    res = pd.DataFrame(res, columns=[f'{cur_cat}_SUM', f'{cur_cat}_MIN', f'{cur_cat}_MAX', f'{cur_cat}_MEAN'])
                    res['id'] = gdf[gdf_id].values
                    res.set_index('id', inplace=True)
                    final_zonal_res.append(res)
    ret = pd.concat(final_zonal_res, axis=1)
    if verbose:
        tPrint(f'**** finished')
    return({out_file:ret})