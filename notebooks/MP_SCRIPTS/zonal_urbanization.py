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
import global_zonal

AWS_S3_BUCKET = 'wbg-geography01'
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

if __name__ == "__main__":
    multiprocess=True
    verbose = True
    tPrint("Starting")
    h3_level = 6
    data_prefix = "Urbanization"
    data_prefix_pop = "Urbanization_Pop"
    
    # Urbanization layers
    unq_urban = [11,12,13,21,22,23,30]
    ghsl_folder = "/home/public/Data/GLOBAL/GHSL/"
    ghs_smod = os.path.join(ghsl_folder, "SMOD", "GHS_SMOD_E2020_GLOBE_R2023A_54009_1000_V1_0.tif")
    ghs_pop = os.path.join(ghsl_folder, "POP", "GHS_POP_E2020_GLOBE_R2023A_54009_100_V1_0.tif")
    
    h3_0_list = h3_helper.generate_lvl0_lists(h3_level, return_gdf=True, buffer0=False)
    if verbose:
        tPrint("H3_0 list generated")
    
    # Set up mp arguments for urban population
    h3_1_list = h3_helper.generate_lvl0_lists(h3_level, return_gdf=True, buffer0=False, read_pickle=False)
    urban_pop_args = []
    processed_pop_list = []
    for h3_1_key, cur_gdf in h3_1_list.items():
        filename = 'GHS_POP_2020_Urban_Breakdown.csv'
        out_s3_key = f'Space2Stats/h3_stats_data/GLOBAL/{data_prefix_pop}/{h3_1_key}/{filename}'
        full_path = os.path.join("s3://", AWS_S3_BUCKET, out_s3_key)
        try:
            tempPD = pd.read_csv(full_path)
            processed_pop_list.append(filename)
        except:
            urban_pop_args.append([cur_gdf, ghs_pop, out_s3_key, unq_urban, True, verbose])

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