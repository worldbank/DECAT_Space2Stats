import json
import os
import sys

import geojson


class s2s_geo_data:
    def __init__(self, json_path):
        """Extract metatdata and processing information for input geospatial layers

        Args:
            json_path (string): path to json file to process
        """
        with open(json_path, "r") as in_data:
            in_json = json.load(in_data)

        self.data_info = in_json

    def get_path(self, yyyy="", mm="", dd=""):
        """Get path to geospatial data for processing

        Args:
            yyyy (str, optional): specific year to process. Defaults to ''.
            mm (str, optional): specific month to process. Defaults to ''.
            dd (str, optional): specific day to process. Defaults to ''.
        """
        inD = self.data_info.copy()

        s3_path = os.path.join(
            inD["s3_bucket_base"],
        )
