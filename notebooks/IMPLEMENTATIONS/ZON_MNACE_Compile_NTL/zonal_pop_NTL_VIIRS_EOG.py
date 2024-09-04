import multiprocessing
import os
import sys

import geopandas as gpd
import GOSTrocks.ntlMisc as ntl
import GOSTrocks.rasterMisc as rMisc
import pandas as pd
from GOSTrocks.misc import tPrint
from h3 import h3

# import numpy as np


sys.path.append("../../src")
import h3_helper

AWS_S3_BUCKET = "wbg-geography01"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")


def run_zonal(gdf, cur_raster_file, out_file, buffer0=False, verbose=False):
    cName = f"{os.path.basename(os.path.dirname(out_file))}-{os.path.basename(cur_raster_file)}"
    if verbose:
        tPrint(f"Starting {cName}")
    if buffer0:
        gdf["geometry"] = gdf["geometry"].buffer(0)
    res = rMisc.zonalStats(gdf, cur_raster_file, minVal=0, verbose=False)
    res = pd.DataFrame(res, columns=["SUM", "MIN", "MAX", "MEAN"])
    res["id"] = gdf["id"].values
    if verbose:
        tPrint(f"**** finished {cName}")
    return {out_file: res}


if __name__ == "__main__":
    multiprocess = True
    verbose = True
    tPrint("Starting")
    h3_level = 6
    data_prefix = "VIIRS_ANNUAL_EOG"

    # Get list of nighttime lights VIIRS data
    # ntl_files = ntl.aws_search_ntl()
    ntl_folder = "/home/public/Data/GLOBAL/NighttimeLights/VIIRS_ANNUAL_EOG_V21"
    ntl_files = [
        os.path.join(ntl_folder, x)
        for x in os.listdir(ntl_folder)
        if x.endswith(".tif")
    ]

    # h3_0_list = h3_helper.generate_lvl0_lists(h3_level, return_gdf=True, buffer0=False)
    admin_bounds = "/home/wb411133/data/Global/ADMIN/Admin2_Polys.shp"
    # Generate a list from the global admin boundaries
    inA = gpd.read_file(admin_bounds)
    inA["id"] = list(inA.index)
    h3_0_list = {}
    for region, countries in inA.groupby("WB_REGION"):
        h3_0_list[region] = countries

    if verbose:
        tPrint("H3_0 list generated")
    # set up mp arguments

    for h3_0_key, cur_gdf in h3_0_list.items():
        arg_list = []
        processed_list = []
        for pop_file in ntl_files:
            filename = os.path.basename(f'{pop_file.replace(".tif", "")}_zonal.csv')
            out_s3_key = (
                f"Space2Stats/h3_stats_data/GLOBAL/{data_prefix}/{h3_0_key}/{filename}"
            )
            out_s3_key = f"Space2Stats/h3_stats_data/ADM_GLOBAL/{data_prefix}/{h3_0_key}/{filename}"
            full_path = os.path.join("s3://", AWS_S3_BUCKET, out_s3_key)
            try:
                tempPD = pd.read_csv(full_path)
                processed_list.append(filename)
            except:
                arg_list.append([cur_gdf, pop_file, out_s3_key, True, verbose])

        if multiprocess:
            with multiprocessing.Pool(processes=min([70, len(ntl_files)])) as pool:
                results = pool.starmap(run_zonal, arg_list)
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
