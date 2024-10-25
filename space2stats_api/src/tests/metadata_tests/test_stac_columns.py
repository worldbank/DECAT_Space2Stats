import json


def test_stac_columns_vs_types_json(stac_file_path, types_json_file_path):
    # Load the expected column types from the types JSON file
    with open(types_json_file_path, "r") as f:
        expected_columns = json.load(f)

    # Load the STAC item from the JSON file
    with open(stac_file_path, "r") as f:
        stac_item = json.load(f)

    # Extract column names and types from the STAC item
    stac_columns = {
        col["name"]: col["type"] for col in stac_item["properties"]["table:columns"]
    }

    # Assert that the number of columns in the STAC file matches the number of columns in the types JSON file
    assert (
        len(stac_columns) == len(expected_columns)
    ), f"Mismatch in column count: STAC ({len(stac_columns)}) vs JSON ({len(expected_columns)})"

    # Assert that column names and types match
    for column_name, column_type in expected_columns.items():
        assert (
            column_name in stac_columns
        ), f"Column {column_name} is missing in the STAC file"
        assert (
            stac_columns[column_name] == column_type
        ), f"Mismatch in column type for {column_name}: STAC ({stac_columns[column_name]}) vs JSON ({column_type})"
