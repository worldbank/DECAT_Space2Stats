import json
import pytest
import os
import pandas as pd


@pytest.fixture
def stac_file_path():
    test_file_dir = os.path.dirname(
        "space2stats_api/src/space2stats_ingest/METADATA/tests/test_stack_columns.py"
    )
    json_file_path = "space2stats_api/src/space2stats_ingest/METADATA/stac/space2stats-collection/space2stats_population_2020/space2stats_population_2020.json"

    relative_dir = os.path.relpath(json_file_path, start=test_file_dir)

    return relative_dir


@pytest.fixture
def parquet_file_path():
    test_file_dir = os.path.dirname(
        "space2stats_api/src/space2stats_ingest/METADATA/tests/test_stack_columns.py"
    )
    parquet_file_path = "space2stats_api/src/local.parquet"

    relative_dir = os.path.relpath(parquet_file_path, start=test_file_dir)
    return relative_dir


def test_stac_columns_vs_parquet(stac_file_path, parquet_file_path):
    # Load the STAC item from the JSON file
    with open(stac_file_path, "r") as f:
        stac_item = json.load(f)

    # Extract column names and types from the STAC item
    stac_columns = {
        col["name"]: col["type"] for col in stac_item["properties"]["table:columns"]
    }

    # Load the Parquet file into a DataFrame
    df = pd.read_parquet(parquet_file_path)

    # Extract column names and data types from the DataFrame
    parquet_columns = {col: str(df[col].dtype) for col in df.columns}

    # Assert that the number of columns in the Parquet file matches the number of columns in the STAC file
    assert len(parquet_columns) == len(
        stac_columns
    ), f"Mismatch in column count: Parquet ({len(parquet_columns)}) vs STAC ({len(stac_columns)})"

    # Assert that column names and types match
    for column_name, column_type in stac_columns.items():
        assert (
            column_name in parquet_columns
        ), f"Column {column_name} is missing in the Parquet file"
        assert (
            parquet_columns[column_name] == column_type
        ), f"Mismatch in column type for {column_name}: Parquet ({parquet_columns[column_name]}) vs STAC ({column_type})"
