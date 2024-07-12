import boto3, os

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

def get_global_table_from_s3(variable, bucket='wbg-geography01', prefix='Space2Stats/h3_stats_data/GLOBAL/', verbose=False, read_data=True):
    """ Get pandas dataframe of all csv files in S3 bucket that match the variable name

    Parameters
    ----------
    variable : string
        variable name to search for in S3 bucket
    bucket : str, optional
        S3 bucket to search, by default 'wbg-geography01'
    prefix : str, optional
        Prefix in bucket to search for zonal results, by default 'Space2Stats/h3_stats_data/GLOBAL/'
    verbose : bool, optional
        If True, print lots of updates, by default False
    read_data : bool, optional
        If True, return results as pandas data frames for each sub-value in variable,
        otherwise returns a list of s3 prefixes for each sub-value, by default True
    """
    
    s3client = boto3.client('s3')
    
    # Loop through the S3 bucket and get all the keys for files that are .tif 
    prefix = f"{prefix}{variable}"
    more_results = True
    loops = 0
    good_res = {}
    while more_results:
        if verbose:
            print(f"Completed loop: {loops}")
        if loops > 0:
            objects = s3client.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=token)
        else:
            objects = s3client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        more_results = objects['IsTruncated']
        if more_results:
            token = objects['NextContinuationToken']
        loops += 1
        for res in objects['Contents']:
            if res['Key'].endswith('csv'):
                cur_variable = os.path.basename(res['Key']).replace(".csv", "")
                try:
                    good_res[cur_variable].append(res['Key'])
                except:
                    good_res[cur_variable] = [res['Key']]
    if read_data:
        for key, value in good_res.items():
            for idx, val in enumerate(value):
                if idx == 0:
                    cur_df = pd.read_csv(f"s3://{bucket}/{val}")
                else:
                    cur_df = pd.concat([cur_df, pd.read_csv(f"s3://{bucket}/{val}")])
            good_res[key] = cur_df
    return(good_res)


def calculate_value(in_shp, zonal_res, h3_level, feat_id, fractional_res=True, 
                    zonal_res_id='id', default_sum='SUM'):
    ''' tabulate hexabin stats for all bins that intersect shape in_shp

    :param in_shp: shape of boundary to intersect with hexabins
    :type in_shp: shapely polygon
    :param zonal_res: DataFrame of h3 results to search through
    :type zonal_res: pandas.DataFrame
    :param h3_level: level of h3 grid to search for
    :type h3_level: int
    :param fractional_res: should admin intersection with h3 account for fractional overlaps, default is True
    :type fractional_res: Boolean, optional
    :param zonal_res_id: id column in zonal_res thta contains hex ids, defaults to "shape_id"
    :type zonal_res_id: string, optional
    :param default_sum: default function for aggregating intersecting hexs if arg is not in coulmn name;
                        function sill pick up [SUM,MIN,MAX,MEAN], defaults to sum
    :type default_sum: string, optional


    :return: dictionary of results summarized based on type (SUM, MIN, MEAN, MAX)
    :rtype: Dictionary
    '''
    def get_intersection(admin_shp, hex_shp):
        ''' get fraction of hex_shp that is inside admin_shp
        '''
        if admin_shp.contains(hex_shp):
            return(1)
        else:
            return(admin_shp.intersection(hex_shp).area/hex_shp.area)
    
    res = {'id':feat_id}
    process_h3 = True
    # Generate h3 cells that intersect current shape; if none are generated first time through, buffer
    #  the geometry by a little bit, and then search again
    while process_h3:        
        if in_shp.geom_type == 'Polygon':
            sel_h3 = h3.polyfill(in_shp.__geo_interface__, h3_level, geo_json_conformant=True)
        else:
            for cPoly in in_shp:
                temp_h3 = h3.polyfill(cPoly.__geo_interface__, h3_level, geo_json_conformant=True)
                try:
                    sel_h3 = sel_h3.union(temp_h3)
                except:
                    sel_h3 = temp_h3
        process_h3 = len(sel_h3) == 0
        in_shp = in_shp.buffer(0.1)

    if len(sel_h3) > 0:
        hex_poly = lambda hex_id: Polygon(h3.h3_to_geo_boundary(hex_id, geo_json=True))
        all_polys = gpd.GeoSeries(list(map(hex_poly, sel_h3)), index=sel_h3, crs="EPSG:4326")
        all_polys = gpd.GeoDataFrame(all_polys, crs=4326, columns=['geometry'])
        all_polys['shape_id'] = list(all_polys.index)
        if fractional_res:
            all_polys['inter_area'] = all_polys['geometry'].apply(lambda x: get_intersection(in_shp, x))
        else:
            all_polys['inter_area'] = 1        
        all_polys = pd.merge(all_polys, zonal_res, left_on='shape_id', right_on=zonal_res_id)
        for col in all_polys.columns: 
            if not col in ['inter_area','geometry','shape_id']: 
                calc_type = default_sum
                if "SUM" in col: calc_type = "SUM"
                if "MIN" in col: calc_type = "MIN"
                if "MAX" in col: calc_type = "MAX"
                if "MEAN" in col: calc_type = "MEAN"            
                try:
                    if calc_type == "SUM": # For sum columns, multiply column by inter_area and sum results
                        cur_val = sum(all_polys[col] * all_polys['inter_area'])
                    elif calc_type == "MIN":
                        cur_val = all_polys[col].min()
                    elif calc_type == "MAX":
                        cur_val = all_polys[col].max()
                    elif calc_type == "MEAN":
                        cur_val = sum(all_polys[col] * all_polys['inter_area'])/sum(all_polys['inter_area'])
                    res[col] = cur_val
                except:
                    pass
                try:
                    del(cur_val)
                except:
                    pass
    else:
        pass
    return(res)

def connect_polygons_h3_stats(inA, stats_df, h3_level, id_col, fractional_res=True,
                              zonal_res_id='id', default_sum='SUM'):
    ''' merge stats from hexabin stats dataframe (stats_df) with the inA geodataframe
    
    :param inA: input boundary dataset
    :type inA: geopandas.GeoDataFrame
    :param stats_df: input hexabin stats dataset
    :type stats_df: pandas.DataFrame
    :param h3_level: h3 hex level
    :type h3_level: int
    :param id_col: column in inA that uniquely identifies rows
    :type id_col: string
    :param zonal_res_id: id column in zonal_res thta contains hex ids, defaults to "shape_id"
    :type zonal_res_id: string, optional
    :param default_sum: default function for aggregating intersecting hexs if arg is not in coulmn name;
                        function sill pick up [SUM,MIN,MAX,MEAN], defaults to sum
    :type default_sum: string, optional

    
    :return: pandas dataframe with attached statistics and matching id from id_col
    :rtype: geopandas.GeoDataFrame
    '''
    all_res = []
    for idx, row in inA.iterrows():               
        all_res.append(calculate_value(row['geometry'], stats_df, h3_level, row[id_col], fractional_res, zonal_res_id, default_sum))        
        '''
        try:
            all_res.append(calculate_value(row['geometry'], stats_df, h3_level, row[id_col], fractional_res, zonal_res_id, default_sum))
        except:
            print(f'Error processing {idx}')        
        '''
        
    return(pd.DataFrame(all_res))

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
    res = rMisc.zonalStats(gdf, raster_file, minVal=minVal, maxVal=maxVal, verbose=verbose, reProj=True)
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
    res = rMisc.zonalStats(gdf, raster_file, rastType="C", unqVals=categories, verbose=verbose, reProj=True)
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
                    res = rMisc.zonalStats(gdf, cur_rast_src, minVal=minVal, maxVal=maxVal, verbose=verbose, reProj=True)
                    res = pd.DataFrame(res, columns=[f'{cur_cat}_SUM', f'{cur_cat}_MIN', f'{cur_cat}_MAX', f'{cur_cat}_MEAN'])
                    res['id'] = gdf[gdf_id].values
                    res.set_index('id', inplace=True)
                    final_zonal_res.append(res)
    ret = pd.concat(final_zonal_res, axis=1)
    if verbose:
        tPrint(f'**** finished')
    return({out_file:ret})