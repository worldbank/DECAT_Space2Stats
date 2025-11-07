import importlib
import io
import math
import multiprocessing
import os
import sys

import awswrangler as wr
import boto3
import geojson
import geopandas as gpd
import GOSTrocks.rasterMisc as rMisc
import numpy as np
import pandas as pd
import s3fs
import urllib3
from GOSTrocks.misc import tPrint
from shapely.geometry import Polygon
from tqdm import tqdm

sys.path.append("../../src")
import h3_helper

AWS_S3_BUCKET = "wbg-geography01"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
s3 = s3fs.S3FileSystem(
    anon=False, key=AWS_ACCESS_KEY_ID, secret=AWS_SECRET_ACCESS_KEY, use_ssl=False
)
s3session = boto3.Session()
s3client = s3session.client("s3", verify=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def run_zonal(gdf, cur_raster_file, out_file, buffer0=False, verbose=False):
    cName = f"{os.path.basename(os.path.dirname(out_file))}-{os.path.basename(cur_raster_file)}"
    if verbose:
        tPrint(f"Starting {cName}")
    if buffer0:
        gdf["geometry"] = gdf["geometry"].buffer(0)
    res = rMisc.zonalStats(gdf, cur_raster_file, minVal=0, verbose=False)
    res = pd.DataFrame(res, columns=["SUM", "MIN", "MAX", "MEAN"])
    res["shape_id"] = gdf["shape_id"].values
    if verbose:
        tPrint(f"**** finished {cName}")
    return {out_file: res}


if __name__ == "__main__":
    multiprocess = True
    verbose = True
    tPrint("Starting")
    h3_level = 6
    data_prefix = "WorldPop_2025_Demographics"
    h3_0_list = h3_helper.generate_lvl0_lists(
        h3_level,
        return_gdf=True,
        buffer0=False,
        pickle_file="h0_dictionary_of_h6_geodata_frames_land.pickle",
    )

    pop_folder = r"C:\WBG\Work\data\POP\WorldPop\Demographics"
    pop_files = [
        os.path.join(pop_folder, f)
        for f in os.listdir(pop_folder)
        if f.endswith(".tif")
    ]

    if verbose:
        tPrint("H3_0 list generated")

    # set up mp arguments
    for h3_0_key, cur_gdf in h3_0_list.items():
        arg_list = []
        processed_list = []
        for pop_file in pop_files:
            filename = os.path.basename(pop_file).replace(".tif", ".parquet")
            out_s3_key = (
                f"Space2Stats/h3_stats_data/GLOBAL/{data_prefix}/{h3_0_key}/{filename}"
            )
            full_path = f"s3://{AWS_S3_BUCKET}/{out_s3_key}"
            try:
                s3client.head_object(Bucket=AWS_S3_BUCKET, Key=out_s3_key)
                processed_list.append(filename)
            except:
                arg_list.append([cur_gdf, pop_file, out_s3_key, True, verbose])
        tPrint(
            f"{h3_0_key} - {len(arg_list)} files to process, {len(processed_list)} already processed"
        )
        if len(arg_list) > 0:
            if multiprocess:
                with multiprocessing.Pool(processes=min([70, len(arg_list)])) as pool:
                    results = pool.starmap(run_zonal, arg_list)
            else:
                for a in arg_list:
                    results = run_zonal(*a)

            for combo in results:
                out_file = list(combo.keys())[0]
                res = combo[out_file]

                parquet_buffer = io.BytesIO()
                res.to_parquet(parquet_buffer, index=False)
                s3client.put_object(
                    Bucket=AWS_S3_BUCKET, Key=out_file, Body=parquet_buffer.getvalue()
                )
