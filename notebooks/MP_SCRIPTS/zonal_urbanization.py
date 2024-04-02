import sys, os, multiprocessing

import pandas as pd
#import geopandas as gpd
#import numpy as np

from h3 import h3

import GOSTrocks.rasterMisc as rMisc
import GOSTrocks.ntlMisc as ntl
from GOSTrocks.misc import tPrint

sys.path.append("../../src")
import h3_helper

AWS_S3_BUCKET = 'wbg-geography01'
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

def run_zonal(gdf, cur_raster_file, out_file, buffer0=False, verbose=False):
    cName = f'{os.path.basename(os.path.dirname(out_file))}-{os.path.basename(cur_raster_file)}'
    if verbose:
        tPrint(f'Starting {cName}')
    if buffer0:
        gdf['geometry'] = gdf['geometry'].buffer(0)        
    res = rMisc.zonalStats(gdf, cur_raster_file, minVal=0, verbose=False)
    res = pd.DataFrame(res, columns=['SUM', 'MIN', 'MAX', 'MEAN'])
    res['id'] = gdf['shape_id'].values
    if verbose:
        tPrint(f'**** finished {cName}')
    return({out_file:res})

def run_zonal_cat(gdf, cur_raster_file, out_file, unq_vals=None, buffer0=False, verbose=False):
    cName = out_file
    if verbose:
        tPrint(f'Starting {cName}')
    if buffer0:
        gdf['geometry'] = gdf['geometry'].buffer(0)        
    if not unq_vals is None:
        res = rMisc.zonalStats(gdf, cur_raster_file, rastType='C', unqVals=unq_vals, verbose=False, reProj=True)
        res = pd.DataFrame(res, columns=[f'c_{x}' for x in unq_vals])
    else:
        res = rMisc.zonalStats(gdf, cur_raster_file, minVal=0, verbose=False)
        res = pd.DataFrame(res, columns=['SUM', 'MIN', 'MAX', 'MEAN'])
        res['id'] = gdf['shape_id'].values
    if verbose:
        tPrint(f'**** finished {cName}')
    return({out_file:res})

if __name__ == "__main__":
    multiprocess=True
    verbose = True
    tPrint("Starting")
    h3_level = 6
    data_prefix = "Urbanization"
    
    # Urbanization layers
    unq_urban = [11,12,13,21,22,23,30]
    ghsl_folder = "/home/public/Data/GLOBAL/GHSL/"
    ghs_smod = os.path.join(ghsl_folder, "SMOD", "GHS_SMOD_E2020_GLOBE_R2023A_54009_1000_V1_0.tif")
    ghs_pop = os.path.join(ghsl_folder, "POP", "GHS_POP_E2020_GLOBE_R2023A_54009_100_V1_0.tif")
    
    h3_0_list = h3_helper.generate_lvl0_lists(h3_level, return_gdf=True, buffer0=False)
    if verbose:
        tPrint("H3_0 list generated")
    
    # set up mp arguments
    for h3_0_key, cur_gdf in h3_0_list.items():
        arg_list = []
        processed_list = []    
        filename = 'GHS_SMOD_2020_Urban_counts.csv'
        out_s3_key = f'Space2Stats/h3_stats_data/GLOBAL/{data_prefix}/{h3_0_key}/{filename}'
        full_path = os.path.join("s3://", AWS_S3_BUCKET, out_s3_key)        
        try:
            tempPD = pd.read_csv(full_path)
            processed_list.append(filename)
        except:
            arg_list.append([cur_gdf, ghs_smod, out_s3_key, unq_urban, True, verbose])

    if multiprocess:
        with multiprocessing.Pool(processes=min([70,len(arg_list)])) as pool:
            results = pool.starmap(run_zonal_cat, arg_list)    
    else:
        for a in arg_list:
            results = run_zonal(*a)

    for combo in results:
        out_file = list(combo.keys())[0]
        res = combo[out_file]
        res.to_csv(
            f"s3://{AWS_S3_BUCKET}/{out_file}",
            index=False,
            storage_options={
                "key": AWS_ACCESS_KEY_ID,
                "secret": AWS_SECRET_ACCESS_KEY,
                "token": AWS_SESSION_TOKEN,
            },
        )