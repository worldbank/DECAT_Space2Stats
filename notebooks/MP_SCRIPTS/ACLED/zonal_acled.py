import multiprocessing
import os
import sys

sys.path.append("C:/WBG/Work/Code/GOSTrocks/src")
from GOSTrocks.misc import tPrint

sys.path.append("../../src")
import global_zonal
import h3_helper

AWS_S3_BUCKET = "wbg-geography01"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

if __name__ == "__main__":
    # Define local variables
    verbose = True
    tPrint("Starting")
    h3_level = 6
    data_prefix = "GHSL"
    multiprocess = True

    # Define input and output
    h3_0_list = h3_helper.generate_lvl0_lists(
        h3_level,
        return_gdf=True,
        buffer0=False,
        read_pickle=True,
        pickle_file="h0_dictionary_of_h6_geodata_frames_land.pickle",
    )
    if verbose:
        tPrint("H3_0 list generated")

    def process_acled_data(h3_lvl0_key, h3_lvl_key, outfile):
        # get bounding box of input geodataframe
        pass

    # Run multi processing on urban
    if multiprocess:
        nCores = min([70, len(built_args), multiprocessing.cpu_count() - 2])
        tPrint(
            f"Running calculations on built area: {len(built_args)} processes using {nCores} cores"
        )
        with multiprocessing.Pool(processes=nCores) as pool:
            results = pool.starmap(global_zonal.zonal_stats_numerical, built_args)
        tPrint(f"Finished urban calculations: {len(results)}")
        for combo in results:
            out_file = list(combo.keys())[0]
            res = combo[out_file]
            res.to_csv(
                out_file,
                # storage_options={
                #    "key": AWS_ACCESS_KEY_ID,
                #    "secret": AWS_SECRET_ACCESS_KEY,
                #    "token": AWS_SESSION_TOKEN,
                # },
            )
    else:
        for a in built_args:
            results = global_zonal.zonal_stats_numerical(*a)
            out_file = list(results.keys())[0]
            res = results[out_file]
            res.to_csv(
                out_file,
                # storage_options={
                #    "key": AWS_ACCESS_KEY_ID,
                #    "secret": AWS_SECRET_ACCESS_KEY,
                #    "token": AWS_SESSION_TOKEN,
                # },
            )
            tPrint(f"Finished {out_file}")

    tPrint("Finished")
