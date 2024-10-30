import ast
import json
import os
from datetime import datetime
from os.path import join
from typing import Dict

import git
import pandas as pd
from pystac import Asset, CatalogType, Collection, Item
from pystac.extensions.table import TableExtension


# Function to get the root of the git repository
def get_git_root() -> str:
    git_repo = git.Repo(os.getcwd(), search_parent_directories=True)
    return git_repo.git.rev_parse("--show-toplevel")


# Function to load metadata from the Excel file
def load_metadata(file: str) -> Dict[str, pd.DataFrame]:
    overview = pd.read_excel(file, sheet_name="DDH Dataset", index_col="Field")
    nada = pd.read_excel(file, sheet_name="NADA", index_col="Field")
    feature_catalog = pd.read_excel(file, sheet_name="Feature Catalog")
    sources = pd.read_excel(file, sheet_name="Sources")
    sources["Variables"] = sources.apply(
        lambda x: ast.literal_eval(x["Variables"]), axis=1
    )
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
def create_new_item(sources: pd.DataFrame, column_types: dict, item_name: str) -> Item:
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

    # Get metadata for Population item
    src_metadata = sources[sources["Name"] == "Nighttime Lights"].iloc[0]

    # Define the item
    item = Item(
        id=item_name,
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
        {"name": col, "type": dtype} for col, dtype in column_types.items()
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

    return item


# Function to add the new item to the existing collection
def add_item_to_collection(collection: Collection, item: Item):
    collection.add_item(item)


# Save the updated collection
def save_collection(collection: Collection, collection_path: str):
    collection.normalize_hrefs(collection_path)
    collection.save(catalog_type=CatalogType.RELATIVE_PUBLISHED)


# Main function
def main():
    git_root = get_git_root()
    metadata_dir = join(git_root, "space2stats_api/src/space2stats_ingest/METADATA")

    # Paths and metadata setup
    item_name = "space2stats_ntl_2013"
    collection_path = join(metadata_dir, "stac/space2stats-collection/collection.json")
    excel_path = join(metadata_dir, "Space2Stats Metadata Content.xlsx")
    column_types_file = join(metadata_dir, "types.json")

    # Load metadata and column types
    metadata = load_metadata(excel_path)
    with open(column_types_file, "r") as f:
        column_types = json.load(f)

    # Load existing collection
    collection = load_existing_collection(collection_path)

    # Create a new item
    new_item = create_new_item(metadata["sources"], column_types, item_name)

    # Add the new item to the collection
    collection.add_item(new_item, title="Space2Stats NTL 2013 Data Item")

    # Save the updated collection
    save_collection(collection, collection_path)


if __name__ == "__main__":
    main()
