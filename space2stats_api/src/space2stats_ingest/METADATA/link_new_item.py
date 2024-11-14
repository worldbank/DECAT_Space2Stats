import argparse
import json
import os
from datetime import datetime
from os.path import join
from typing import Dict

import git
import pandas as pd
import pyarrow as pa
from pyarrow.parquet import ParquetFile
from pystac import Asset, CatalogType, Collection, Item
from pystac.extensions.table import TableExtension

argParser = argparse.ArgumentParser()
argParser.add_argument(
    "-i", "--input_parquet", type=str, help="Path of new parquet data", required=True
)


# Function to get the root of the git repository
def get_git_root() -> str:
    git_repo = git.Repo(os.getcwd(), search_parent_directories=True)
    return git_repo.git.rev_parse("--show-toplevel")


# Function to get column types from a given parquet file, ignore hex_id (optional).
def get_types(parquet_file: str):
    pf = ParquetFile(parquet_file)
    first_ten_rows = next(pf.iter_batches(batch_size=10))
    df = pa.Table.from_batches([first_ten_rows]).to_pandas()

    # Get the column names and their types
    column_types = {col: str(df[col].dtype) for col in df.columns if col != "hex_id"}
    return column_types


# Function to save an updated dictionary of column types. Will not be used for now.
def save_parquet_types_to_json(parquet_file: str):
    git_root = get_git_root()
    json_file = join(
        git_root, "space2stats_api/src/space2stats_ingest/METADATA/types.json"
    )
    df = pd.read_parquet(parquet_file, nrow=10)

    # Get the column names and their types
    column_types = {col: str(df[col].dtype) for col in df.columns}

    # Save the column types to a JSON file
    with open(json_file, "r+") as f:
        data_types = json.load(f)  # Read the existing data
        data_types.update(column_types)  # Update with new columns
        f.seek(0)  # Move to the start of the file
        json.dump(data_types, f, indent=4)  # Write updated data
        f.truncate()

    print(f"Column types saved to {json_file}")


# Function to load metadata from the Excel file
def load_metadata(file: str) -> Dict[str, pd.DataFrame]:
    overview = pd.read_excel(file, sheet_name="DDH Dataset", index_col="Field")
    nada = pd.read_excel(file, sheet_name="NADA", index_col="Field")
    feature_catalog = pd.read_excel(file, sheet_name="Feature Catalog")
    sources = pd.read_excel(file, sheet_name="Sources")
    return {
        "overview": overview,
        "nada": nada,
        "feature_catalog": feature_catalog,
        "sources": sources,
    }


# Function to read the existing STAC collection
def load_existing_collection(collection_path: str) -> Collection:
    return Collection.from_file(collection_path)


# Function to create a new STAC item
def create_new_item(
    sources: pd.DataFrame,
    column_types: dict,
    item_id: str,
    feature_catalog: pd.DataFrame,
) -> tuple[Item, str]:
    # Define geometry and bounding box (you may want to customize these)
    geom = {
        "type": "Polygon",
        "coordinates": [
            [
                [-179.99999561620714, -89.98750455101016],
                [-179.99999561620714, 89.98750455101016],
                [179.99999096313272, 89.98750455101016],
                [179.99999096313272, -89.98750455101016],
                [-179.99999561620714, -89.98750455101016],
            ]
        ],
    }
    bbox = [
        -179.99999561620714,
        -89.98750455101016,
        179.99999096313272,
        89.98750455101016,
    ]

    # Get metadata for the new item
    try:
        src_metadata = sources[sources["Item"] == item_id].iloc[0]
    except IndexError:
        raise IndexError(f"Item '{item_id}' not found in the metadata sources sheet")

    # Define the item
    item = Item(
        id=item_id,
        geometry=geom,
        bbox=bbox,
        datetime=datetime.now(),
        properties={
            "name": src_metadata["Name"],
            "description": src_metadata["Description"],
            "methodological_notes": src_metadata["Methodological Notes"],
            "source_data": src_metadata["Source Data"],
            "sci:citation": src_metadata["Citation source"],
            "method": src_metadata["Method"],
            "resolution": src_metadata["Resolution"],
            "themes": src_metadata["Theme"],
        },
        stac_extensions=[
            "https://stac-extensions.github.io/table/v1.2.0/schema.json",
            "https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
        ],
    )

    # Add table columns as properties
    TableExtension.add_to(item)
    table_extension = TableExtension.ext(item, add_if_missing=True)
    table_extension.columns = [
        {
            "name": col,
            "description": feature_catalog.loc[col, "description"],
            "type": dtype,
        }
        for col, dtype in column_types.items()
    ]

    # Add asset
    item.add_asset(
        "api-docs",
        Asset(
            href="https://space2stats.ds.io/docs",
            title="API Documentation",
            media_type="text/html",
            roles=["metadata"],
        ),
    )

    return (item, src_metadata["Name"])


# Function to add the new item to the existing collection
def add_item_to_collection(collection: Collection, item: Item):
    collection.add_item(item)


# Save the updated collection
def save_collection(collection: Collection, collection_path: str):
    collection.normalize_hrefs(collection_path)
    collection.save(catalog_type=CatalogType.RELATIVE_PUBLISHED)


# Main function
def main():
    args = argParser.parse_args()
    input_parquet = args.input_parquet
    if not os.path.exists(input_parquet):
        raise FileNotFoundError(f"File not found: {input_parquet}")
    git_root = get_git_root()
    metadata_dir = join(git_root, "space2stats_api/src/space2stats_ingest/METADATA")

    # Paths and metadata setup
    collection_path = join(metadata_dir, "stac/space2stats-collection/collection.json")
    excel_path = join(metadata_dir, "Space2Stats Metadata Content.xlsx")
    column_types = get_types(input_parquet)

    # Load metadata and column types
    metadata = load_metadata(excel_path)
    feature_catalog = metadata["feature_catalog"]

    # Find item name and metadata based on column names
    feature_catalog.set_index("variable", inplace=True)
    try:
        feature_catalog = feature_catalog.loc[column_types.keys()]
    except KeyError as e:
        raise KeyError(f"Column '{e}' not found in the metadata feature catalog sheet")
    item_ids = feature_catalog["item"].unique()
    item_id = [id for id in item_ids if id != "all"]
    if len(item_id) != 1:
        raise ValueError(f"Expected one item name, found {len(item_id)}")
    item_id = item_id[0]

    # Load existing collection
    collection = load_existing_collection(collection_path)

    # Create a new item
    new_item, item_title = create_new_item(
        metadata["sources"], column_types, item_id, feature_catalog
    )

    # Add the new item to the collection
    collection.add_item(new_item, title=item_title)

    # Save the updated collection
    save_collection(collection, collection_path)


if __name__ == "__main__":
    main()
