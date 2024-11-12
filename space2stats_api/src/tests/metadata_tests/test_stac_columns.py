import json
import os

import pandas as pd
import pytest


@pytest.mark.skipif(
    "GITHUB_ACTIONS" in os.environ,
    reason="Skipping in GitHub Actions due to Pandas Dependency",
)
def test_stac_columns_vs_metadata_xlsx(stac_file_path, metadata_excel_file_path):
    # Load the expected column types from the Metadata Content Excel
    feature_catalog = pd.read_excel(
        metadata_excel_file_path, sheet_name="Feature Catalog"
    )
    expected_columns = feature_catalog[feature_catalog["source"] == "Population"]

    # Convert the DataFrame to a dictionary for easier comparison
    expected_columns_dict = dict(
        zip(expected_columns["variable"], expected_columns["type"])
    )

    # Load the STAC item from the JSON file
    with open(stac_file_path, "r") as f:
        stac_item = json.load(f)

    # Extract column names and types from the STAC item
    stac_columns = {
        col["name"]: col["type"] for col in stac_item["properties"]["table:columns"]
    }

    # Assert that the number of columns in the STAC file matches the number of columns in the types TABLE file
    assert (
        len(stac_columns) == len(expected_columns_dict)
    ), f"Mismatch in column count: STAC ({len(stac_columns)}) vs TABLE ({len(expected_columns_dict)})"

    # Assert that column names and types match
    for column_name, column_type in expected_columns_dict.items():
        assert (
            column_name in stac_columns
        ), f"Column {column_name} is missing in the STAC file"
        assert (
            stac_columns[column_name] == column_type
        ), f"Mismatch in column type for {column_name}: STAC ({stac_columns[column_name]}) vs TABLE ({column_type})"
