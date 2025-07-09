import io
import multiprocessing
import os
import sys

import boto3
import geopandas as gpd
import GOSTrocks.rasterMisc as rMisc
import pandas as pd
import rasterio
import urllib3
from botocore import UNSIGNED
from botocore.config import Config
from GOSTrocks.misc import tPrint
from pystac_client import Client
from shapely.geometry import box, shape
from urllib3.exceptions import InsecureRequestWarning

sys.path.append("../../src")
import global_zonal
import h3_helper

AWS_S3_BUCKET = "wbg-geography01"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
# Suppress InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)


def summarize_landcover(h0_lbl, h0_level, catalog, s3_client, lc_bucket, out_lc_file):
    tPrint(f"Processing {h0_lbl}")
    # https://pystac-client.readthedocs.io/en/latest/tutorials/pystac-client-introduction.html#API-Search
    query = catalog.search(
        collections=["io-lulc-9-class"],
        datetime="2022-01-01/2022-12-31",
        intersects=h0_level.geometry.union_all(),
    )
    query_items = list(query.item_collection())
    all_res = []
    # Loop through all the returned landcover raster files
    for lc_feature in query_items:
        lc_label = lc_feature.id.replace("-", "_")
        try:
            obj = s3_client.get_object(Bucket=lc_bucket, Key=f"{lc_label}.tif")
            process = True
        except:
            print(f"Could not find file for {lc_label}, skipping")
            process = False
            continue
        if process:
            raw_data: bytes = obj["Body"].read()
            cur_lc = rasterio.open(io.BytesIO(raw_data))
            if h0_level.crs != cur_lc.crs:
                h0_level = h0_level.to_crs(cur_lc.crs)
            lc_box = gpd.GeoDataFrame(
                pd.DataFrame([[1, box(*cur_lc.bounds)]], columns=["id", "geometry"]),
                crs=cur_lc.crs,
                geometry="geometry",
            )
            sel_hexes = gpd.sjoin(h0_level, lc_box, how="inner", predicate="intersects")
            lc_res = rMisc.zonalStats(
                sel_hexes, cur_lc, rastType="C", unqVals=list(range(1, 13))
            )
            lc_res = pd.DataFrame(lc_res, columns=[f"c_{x}" for x in range(1, 13)])
            lc_res["shape_id"] = sel_hexes["shape_id"].values
            all_res.append(lc_res)
    # Concatenate all results for the current h0
    if len(all_res) > 0:
        cur_h0_res = pd.concat(all_res, ignore_index=True)
        cur_h0_res.set_index("shape_id", inplace=True)
        cur_h0_res["total_lc_cells"] = cur_h0_res.sum(axis=1)
        cur_h0_res = cur_h0_res.loc[cur_h0_res["total_lc_cells"] > 0]
        cur_h0_res["hex_id"] = cur_h0_res.index.values

        # If there are duplicates, sum the values for each hex_id
        cur_all_res = []
        for hex_id, curD in cur_h0_res.groupby("hex_id"):
            if len(curD) > 1:
                # sum the columns
                curD = curD.sum()
                curD["hex_id"] = hex_id
                curD = curD.to_frame().T
            cur_all_res.append(curD)

        if len(cur_all_res) > 0:
            final_h0_res = pd.concat(cur_all_res)
            final_h0_res.to_parquet(out_lc_file, index=False)
    return 1


if __name__ == "__main__":
    # Define local variables
    verbose = True
    tPrint("Starting")
    h3_level = 6
    data_prefix = "Landcover_IO"
    multiprocess = False
    landcover_bucket = "io-10m-annual-lulc"

    base_folder = f"s3://{AWS_S3_BUCKET}"
    h3_0_list = h3_helper.generate_lvl0_lists(
        h3_level,
        return_gdf=True,
        buffer0=False,
        read_pickle=True,
        pickle_file="h0_dictionary_of_h6_geodata_frames_land.pickle",
    )
    s3_client = boto3.client(
        "s3", verify=False, config=Config(signature_version=UNSIGNED)
    )
    s3_client_wb = boto3.client("s3", verify=False)

    if verbose:
        tPrint("H3_0 list generated")

    catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

    # Setup multi-processing arguments
    built_args = []
    for h0_lbl, h0_level in h3_0_list.items():
        result_key = f"Space2Stats/h3_stats_data/GLOBAL/{data_prefix}/landcover_{h0_lbl}_2022.parquet"
        out_lc_file = f"{base_folder}/{result_key}"
        # Check if the file already exists on S3
        file_exists = False
        try:
            test = s3_client_wb.head_object(Bucket=AWS_S3_BUCKET, Key=result_key)
            file_exists = True
            print(f"File {out_lc_file} already exists")
        except Exception as e:
            tPrint(f"File {out_lc_file} does not exist")
            pass
        if not file_exists:
            built_args.append(
                [h0_lbl, h0_level, catalog, s3_client, landcover_bucket, out_lc_file]
            )

    # Run multi processing
    if multiprocess:
        nCores = min([70, len(built_args), multiprocessing.cpu_count() - 2])
        tPrint(
            f"Running calculations on built area: {len(built_args)} processes using {nCores} cores"
        )
        with multiprocessing.Pool(processes=nCores) as pool:
            results = pool.starmap(summarize_landcover, built_args)
        tPrint(f"Finished urban calculations: {len(results)}")
    else:
        for a in built_args:
            results = summarize_landcover(*a)
            tPrint(f"Finished {a[-1]}")
    tPrint("Finished")
