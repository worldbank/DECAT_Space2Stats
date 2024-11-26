import os

import pandas as pd
import pystac
import pytest


@pytest.mark.skipif(
    "GITHUB_ACTIONS" in os.environ,
    reason="Skipping in GitHub Actions due to Pandas Dependency",
)
def test_stac_columns_vs_metadata_xlsx(pop_stac_item_path, metadata_excel_file_path):
    """
    Compares column names in a STAC item with the expected metadata from an Excel file.
    """

    # Load the feature catalog from the Excel file
    feature_catalog = pd.read_excel(
        metadata_excel_file_path, sheet_name="Feature Catalog"
    )

    # Filter the DataFrame
    expected_columns = feature_catalog[
        feature_catalog["item"] == "space2stats_population_2020"
    ]

    # Convert the filtered DataFrame to a list of expected variable names
    expected_variable_names = expected_columns["variable"].tolist()

    # Load the STAC item
    item = pystac.Item.from_file(pop_stac_item_path)

    # Extract column names
    item_column_names = [
        col["name"]
        for col in item.properties.get("table:columns", [])
        if col["name"] != "hex_id"
    ]

    # Assert that the number of columns in the STAC file matches the number in the Excel file
    assert (
        len(item_column_names) == len(expected_variable_names)
    ), f"Mismatch in column count: STAC ({len(item_column_names)}) vs Excel ({len(expected_variable_names)})"

    # Assert that all expected variable names are in the STAC item
    for variable_name in expected_variable_names:
        assert (
            variable_name in item_column_names
        ), f"Column '{variable_name}' is missing in the STAC file"


def test_no_duplicate_stac_items(stac_catalog_path):
    # Load the STAC catalog
    catalog = pystac.Catalog.from_file(stac_catalog_path)

    # Iterate over the items in the catalog
    for item in catalog.get_items():
        # Extract column name
        stac_columns = [col["name"] for col in item.properties["table:columns"]]

        # Check for duplicates
        assert len(stac_columns) == len(
            set(stac_columns)
        ), f"Duplicate columns found in the STAC item {item.id}"
