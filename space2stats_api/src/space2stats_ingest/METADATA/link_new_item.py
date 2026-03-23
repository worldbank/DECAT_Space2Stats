import argparse
import csv
import os
from datetime import datetime
from os.path import join
from typing import Dict, List

import git
import pyarrow as pa
from dateutil.parser import parse as dtparse  # type: ignore[import-untyped]
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
    df.columns = df.columns.str.lower()

    # Get the column names and their types
    column_types = {col: str(df[col].dtype) for col in df.columns}
    return column_types


def _read_csv(path: str) -> List[dict]:
    """Read a CSV file and return a list of row dicts."""
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


# Function to load metadata from CSV files
def load_metadata(metadata_dir: str) -> Dict[str, object]:
    feature_rows = _read_csv(
        join(metadata_dir, "Space2Stats_Metadata_Feature_Catalog.csv")
    )
    sources = _read_csv(join(metadata_dir, "Space2Stats_Metadata_Sources.csv"))

    # Build feature_catalog as a dict keyed by variable name
    feature_catalog = {row["variable"]: row for row in feature_rows}

    return {
        "feature_catalog": feature_catalog,
        "sources": sources,
    }


# Function to read the existing STAC collection
def load_existing_collection(collection_path: str) -> Collection:
    return Collection.from_file(collection_path)


# Function to create a new STAC item
def create_new_item(
    sources: List[dict],
    column_types: dict,
    item_id: str,
    feature_catalog: Dict[str, dict],
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
    src_metadata = None
    for row in sources:
        if row["Item"] == item_id:
            src_metadata = row
            break
    if src_metadata is None:
        raise IndexError(f"Item '{item_id}' not found in the metadata sources sheet")

    if not src_metadata["Start Date"] or not src_metadata["End Date"]:
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
    else:
        # Define the item with a time range
        def _parse_date(val):
            return dtparse(str(val).strip())

        item = Item(
            id=item_id,
            geometry=geom,
            bbox=bbox,
            datetime=None,
            start_datetime=_parse_date(src_metadata["Start Date"]),
            end_datetime=_parse_date(src_metadata["End Date"]),
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
            "description": feature_catalog[col]["description"],
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
    column_types = get_types(input_parquet)

    # Load metadata from CSVs
    metadata = load_metadata(join(metadata_dir, "metadata_content"))
    feature_catalog = metadata["feature_catalog"]

    # Find item name and metadata based on column names
    for col in column_types:
        if col not in feature_catalog:
            raise KeyError(
                f"Column '{col}' not found in the metadata feature catalog sheet"
            )
    item_ids = {feature_catalog[col]["item"] for col in column_types}
    item_ids.discard("all")
    if len(item_ids) != 1:
        raise ValueError(f"Expected one item name, found {len(item_ids)}")
    item_id = item_ids.pop()

    # Filter feature_catalog to only columns in the parquet
    feature_catalog = {
        col: feature_catalog[col] for col in column_types if col in feature_catalog
    }

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
