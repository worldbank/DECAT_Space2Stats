import multiprocessing
import os
import sys

import GOSTrocks.ntlMisc as ntl
import GOSTrocks.rasterMisc as rMisc
import pandas as pd
from GOSTrocks.misc import tPrint
from h3 import h3

# import geopandas as gpd
# import numpy as np


sys.path.append("../../src")
import global_zonal
import h3_helper

AWS_S3_BUCKET = "wbg-geography01"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

if __name__ == "__main__":

    verbose = True
    run_urban = False
    run_urban_pop = True
    run_urban_pop = False
    tPrint("Starting")
    h3_level = 6
    data_prefix = "Urbanization"
    data_prefix_pop = "Urbanization_Pop"

    # Urbanization layers
    unq_urban = [11, 12, 13, 21, 22, 23, 30]
    ghsl_folder = "/home/public/Data/GLOBAL/GHSL/"

    ghs_smod = os.path.join(ghsl_folder, "SMOD", "GHS_SMOD_E2020_GLOBE_R2023A_54009_1000_V1_0.tif")
    ghs_pop = os.path.join(ghsl_folder, "Pop", "GHS_POP_E2020_GLOBE_R2023A_54009_100_V1_0.tif")
    
    #h3_0_list = h3_helper.generate_lvl0_lists(h3_level, return_gdf=True, buffer0=False)
    #if verbose:

    ghs_smod = os.path.join(
        ghsl_folder, "SMOD", "GHS_SMOD_E2020_GLOBE_R2023A_54009_1000_V1_0.tif"
    )
    ghs_pop = os.path.join(
        ghsl_folder, "POP", "GHS_POP_E2020_GLOBE_R2023A_54009_100_V1_0.tif"
    )

    # h3_0_list = h3_helper.generate_lvl0_lists(h3_level, return_gdf=True, buffer0=False)
    # if verbose:

    #    tPrint("H3_0 list generated")

    h3_1_list = h3_helper.generate_lvl1_lists(
        h3_level, return_gdf=True, buffer0=True, read_pickle=True, write_pickle=False
    )
    if verbose:
        tPrint("H3_1 list generated")

    urban_pop_args = []
    urban_args = []
    for h3_1_key, cur_gdf in h3_1_list.items():
        if run_urban_pop:
            # Set up mp arguments for urban population        
            pop_filename = 'GHS_POP_2020_Urban_Breakdown.csv'
            pop_out_s3_key = f'Space2Stats/h3_stats_data/GLOBAL/{data_prefix_pop}/{h3_1_key}/{pop_filename}'
            pop_full_path = os.path.join("s3://", AWS_S3_BUCKET, pop_out_s3_key)
            urban_pop_args.append([cur_gdf, "shape_id", ghs_pop, ghs_smod, pop_full_path, unq_urban])
        if run_urban:
            # set up mp arguments for urban summary
            urban_filename = 'GHS_SMOD_2020.csv'
            urban_out_s3_key = f'Space2Stats/h3_stats_data/GLOBAL/{data_prefix}/{h3_1_key}/{urban_filename}'
            urban_full_path = os.path.join("s3://", AWS_S3_BUCKET, urban_out_s3_key)
            urban_args.append([cur_gdf, "shape_id", ghs_smod, unq_urban, urban_full_path])
        # Set up mp arguments for urban population
        pop_filename = "GHS_POP_2020_Urban_Breakdown.csv"
        pop_out_s3_key = f"Space2Stats/h3_stats_data/GLOBAL/{data_prefix_pop}/{h3_1_key}/{pop_filename}"
        pop_full_path = os.path.join("s3://", AWS_S3_BUCKET, pop_out_s3_key)
        try:
            tempPD = pd.read_csv(pop_full_path)
        except:
            urban_pop_args.append(
                [cur_gdf, "shape_id", ghs_pop, ghs_smod, pop_full_path, unq_urban]
            )

        # set up mp arguments for urban summary
        urban_filename = "GHS_SMOD_2020.csv"
        urban_out_s3_key = f"Space2Stats/h3_stats_data/GLOBAL/{data_prefix}/{h3_1_key}/{urban_filename}"
        urban_full_path = os.path.join("s3://", AWS_S3_BUCKET, urban_out_s3_key)
        urban_args.append([cur_gdf, "shape_id", ghs_smod, unq_urban, urban_full_path])
    
    
    
    if run_urban:
        tPrint(f"Running calculations on urban: {len(urban_args)} processes")
        # Run multi processing on urban
        if multiprocess:
            with multiprocessing.Pool(processes=min([70,len(urban_args)])) as pool:
                results = pool.starmap(global_zonal.zonal_stats_categories, urban_args)  
            tPrint(f"Finished urban calculations: {len(results)}")
            for combo in results:
                out_file = list(combo.keys())[0]
                res = combo[out_file]
                res.to_csv(
                    out_file,
                    storage_options={
                        "key": AWS_ACCESS_KEY_ID,
                        "secret": AWS_SECRET_ACCESS_KEY,
                        "token": AWS_SESSION_TOKEN,
                    },
                )
        else:
            for a in urban_args:
                results = global_zonal.zonal_stats_categories(*a)
                out_file = list(results.keys())[0]
                res = combo[out_file]
                res.to_csv(
                    out_file,
                    storage_options={
                        "key": AWS_ACCESS_KEY_ID,
                        "secret": AWS_SECRET_ACCESS_KEY,
                        "token": AWS_SESSION_TOKEN,
                    },
                )
                tPrint(f"Finished {out_file}")

    if run_urban_pop:
        tPrint(f"Running calculations on urban population: {len(urban_pop_args)} processes")
        # Run multi processing on urban_pop_calculations
        if multiprocess:
            with multiprocessing.Pool(processes=min([40,len(urban_pop_args)])) as pool:
                results = pool.starmap(global_zonal.zonal_stats_categorical, urban_pop_args)    
            tPrint(f"Finished multiprocessing urban pop calculations: {len(results)}")
            for combo in results:
                out_file = list(combo.keys())[0]
                res = combo[out_file]
                res.to_csv(
                    out_file,
                    storage_options={
                        "key": AWS_ACCESS_KEY_ID,
                        "secret": AWS_SECRET_ACCESS_KEY,
                        "token": AWS_SESSION_TOKEN,
                    },
                )
        else:
            for a in urban_pop_args:
                combo = global_zonal.zonal_stats_categorical(*a, verbose=verbose, minVal=0)
                out_file = list(combo.keys())[0]
                tPrint(f"Completed {out_file}")
                res = combo[out_file]
                res.to_csv(
                    out_file,
                    storage_options={
                        "key": AWS_ACCESS_KEY_ID,
                        "secret": AWS_SECRET_ACCESS_KEY,
                        "token": AWS_SESSION_TOKEN,
                    },
                )        
    tPrint("Finished")
