import sys, os, multiprocessing

import pandas as pd
#import geopandas as gpd
#import numpy as np

from h3 import h3

sys.path.insert(0, "c:\\WBG\\Work\\Code\\GOSTrocks\\src")
import GOSTrocks.rasterMisc as rMisc
import GOSTrocks.ntlMisc as ntl
import GOSTrocks.dataMisc as dMisc
from GOSTrocks.misc import tPrint

sys.path.append("../../src")
import h3_helper
import global_zonal

AWS_S3_BUCKET = 'wbg-geography01'
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

if __name__ == "__main__":
    multiprocess = True
    verbose = False
    tPrint("Starting")
    h3_level = 6
    data_prefix_flood = "Flood"
    data_prefix_pop = "Flood_Pop"
    flood_reclass_dict = { 0: [-9999, 0], 
                1: [0, 10], 
                2: [10.1, 50], 
                3: [50, 100000.0],}
    
    # Define input layers
    pop_layer = r"C:\WBG\Work\data\POP\ppp_2020_1km_Aggregated.tif"
    # Select layer to downlaod
    flood_type = ["PLUVIAL","FLUVIAL","COASTAL"]
    defence = ["DEFENDED"]
    return_period = ['1in100']
    climate_model = ["PERCENTILE50"]
    year = ["2020"]

    all_vrts = dMisc.get_fathom_vrts(True)
    sel_images = all_vrts.loc[(all_vrts['FLOOD_TYPE'].isin(flood_type)) & (all_vrts['DEFENCE'].isin(defence)) & 
                (all_vrts['RETURN'].isin(return_period))  & (all_vrts['CLIMATE_MODEL'].isin(climate_model))]
    fathom_vrt_path = sel_images['PATH'].iloc[0]

    # h3_0_list = h3_helper.generate_lvl0_lists(h3_level, return_gdf=True, buffer0=False, read_pickle=True)
    h3_1_list = h3_helper.generate_lvl1_lists(h3_level, return_gdf=True, buffer0=False, read_pickle=True)
    if verbose:
        tPrint("H3_1 list generated")
    # set up arguments for zonal processing
    flood_depth_args = []
    flood_pop_args = []
    for h3_1_key, cur_gdf in h3_1_list.items():
        for fathom_index, fathom_row in sel_images.iterrows():
            fathom_path = fathom_row['PATH']
            fathom_file = "_".join([fathom_row['FLOOD_TYPE'], fathom_row['RETURN'], fathom_row['CLIMATE_MODEL'], fathom_row['YEAR']])    
            
            flood_pop_filename   = f'FATHOM_total_pop_{fathom_file}.csv'
            pop_out_s3_key = f'Space2Stats/h3_stats_data/GLOBAL/{data_prefix_pop}/{h3_1_key}/{flood_pop_filename}'
            full_path_pop = os.path.join("s3://", AWS_S3_BUCKET, pop_out_s3_key)
            try:
                tempPD = pd.read_csv(full_path_pop)                
            except:
                flood_pop_args.append([cur_gdf, "shape_id", pop_layer, fathom_path, pop_out_s3_key, 
                                         None, flood_reclass_dict, 
                                         True, 0, 10000000, verbose])
            total_flood_filename = f'FATHOM_total_depth_{fathom_file}.csv'
            depth_out_s3_key = f'Space2Stats/h3_stats_data/GLOBAL/{data_prefix_pop}/{h3_1_key}/{total_flood_filename}'
            full_path_depth = os.path.join("s3://", AWS_S3_BUCKET, depth_out_s3_key)
            try:
                tempPD = pd.read_csv(full_path_depth)                
            except:
                flood_depth_args.append([cur_gdf, "shape_id", fathom_path, depth_out_s3_key, True, 0, 1000, verbose])
    tPrint("Arguments generated")
    # Multiprocess flood population results
    if multiprocess:
        with multiprocessing.Pool(multiprocessing.cpu_count()-2) as pool:
            pop_results = pool.starmap(global_zonal.zonal_stats_categorical, flood_pop_args)    
    else:
        pop_results = []
        for a in flood_pop_args:
            results = global_zonal.zonal_stats_categorical(*a)
            pop_results.append(results)

    for combo in pop_results:
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

    # Multiprocess flood depth results
    if multiprocess:
        with multiprocessing.Pool(processes=min([multiprocessing.cpu_count()-2,len(arg_list)])) as pool:
            depth_results = pool.starmap(global_zonal.zonal_stats_numerical, flood_depth_args)    
    else:
        depth_results = []
        for a in flood_depth_args:
            results = global_zonal.zonal_stats_numerical(*a)
            depth_results.append(results)

    for combo in depth_results:
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