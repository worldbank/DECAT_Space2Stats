import json
import os
from datetime import datetime
from os.path import join
from typing import Dict

import git
import pandas as pd
from pystac import (
    Asset,
    Catalog,
    CatalogType,
    Collection,
    Extent,
    Item,
    SpatialExtent,
    TemporalExtent,
)


# Function to get the root of the git repository
def get_git_root() -> str:
    git_repo = git.Repo(os.getcwd(), search_parent_directories=True)
    return git_repo.git.rev_parse("--show-toplevel")


# Function to load column types from a JSON file
def load_column_types_from_json(json_file: str) -> dict:
    with open(json_file, "r") as f:
        column_types = json.load(f)
    return column_types


# Function to read Excel metadata
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


# Function to create STAC catalog
def create_stac_catalog(overview: pd.DataFrame, nada: pd.DataFrame) -> Catalog:
    catalog = Catalog(
        id="space2stats-catalog",
        description=overview.loc["Description Resource"].values[0],
        title=overview.loc["Title"].values[0],
        extra_fields={
            "License": overview.loc["License"].values[0],
            "Responsible Party": nada.loc["Responsible party", "Value"],
            "Purpose": nada.loc["Purpose", "Value"],
            "Keywords": ["space2stats", "sub-national", "h3", "hexagons", "global"],
        },
        href="https://worldbank.github.io/DECAT_Space2Stats/stac/catalog.json",
    )

    return catalog


# Updated function to create STAC collection
def create_stac_collection(overview: pd.DataFrame) -> Collection:
    spatial_extent = SpatialExtent([[-180.0, -90.0, 180.0, 90.0]])
    temporal_extent = TemporalExtent([[datetime(2020, 1, 1), None]])

    extent = Extent(spatial=spatial_extent, temporal=temporal_extent)

    collection = Collection(
        id="space2stats-collection",
        description="This collection contains geospatial statistics for the entire globe standardized to a hexagonal grid (H3 level 6). It covers various themes, including demographic, socio-economic, and environmental data.",
        title="Space2Stats Collection",
        extent=extent,
        license="CC-BY-4.0",
        extra_fields={
            "Title": overview.loc["Title"].values[0],
            "Description": overview.loc["Description Resource"].values[0],
            "Keywords": ["space2stats", "sub-national", "h3", "hexagons", "global"],
            "summaries": {"datetime": {"min": "2020-01-01T00:00:00Z", "max": None}},
            "providers": [
                {
                    "name": "World Bank",
                    "roles": ["producer", "licensor"],
                    "url": "https://www.worldbank.org/",
                }
            ],
            "assets": {
                "documentation": {
                    "href": "https://space2stats.ds.io/docs",
                    "type": "text/html",
                    "title": "API Documentation",
                    "roles": ["metadata"],
                }
            },
        },
    )
    # collection.set_self_href("collection.json")
    return collection


# Function to create STAC Item from GeoDataFrame
def create_stac_item(column_types: dict, metadata: pd.DataFrame) -> Item:
    data_dict = []

    feature_catalog = metadata["feature_catalog"]
    feature_catalog.set_index("variable", inplace=True)
    try:
        feature_catalog = feature_catalog.loc[column_types.keys()]
    except KeyError as e:
        raise KeyError(f"Column '{e}' not found in the metadata feature catalog sheet")
    item_ids = feature_catalog["item"].unique()
    item_id = [id for id in item_ids if id != "all"]
    if len(item_id) != 1:
        raise ValueError(f"Expected one item name, got {len(item_id)}")
    item_id = item_id[0]

    for column, dtype in column_types.items():
        description = feature_catalog.loc[column, "description"]
        data_dict.append(
            {
                "name": column,
                "description": description,
                "type": dtype,
            }
        )

    # Add 'geometry' to vector:layers
    column_types_with_geometry = column_types.copy()
    column_types_with_geometry["geometry"] = "geometry"

    # Use the specific polygon from the example STAC file
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

    sources = metadata["sources"]
    src_metadata = sources[sources["Item"] == item_id].iloc[0]
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
            "organization": src_metadata["Organization"],
            "method": src_metadata["Method"],
            "resolution": src_metadata["Resolution"],
            "table:primary_geometry": "geometry",
            "table:columns": data_dict,
            "vector:layers": {
                "space2stats": column_types_with_geometry,
            },
            "themes": src_metadata["Theme"],
        },
        stac_extensions=[
            "https://stac-extensions.github.io/table/v1.2.0/schema.json",
            "https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
        ],
    )

    return item


# Function to add assets to the item
def add_assets_to_item(item: Item):
    asset_api = Asset(
        href="https://space2stats.ds.io/docs",
        title="API Documentation",
        media_type="text/html",
        roles=["metadata"],
    )
    item.add_asset("api-docs", asset_api)


# Function to remove absolute paths from the Catalog
def adjust_self_href(catalog_path: str):
    with open(catalog_path, "r") as f:
        catalog_json = json.load(f)

    # Modify the self link
    for link in catalog_json.get("links", []):
        if link.get("rel") == "self":
            link["href"] = "./catalog.json"

    # Write the updated catalog.json back to the file
    with open(catalog_path, "w") as f:
        json.dump(catalog_json, f, indent=2)


def save_stac_catalog(catalog: Catalog, dest_dir: str):
    catalog.save(dest_href=dest_dir, catalog_type=CatalogType.RELATIVE_PUBLISHED)
    # adjust_self_href(join(dest_dir, "catalog.json"))


def main():
    git_root = get_git_root()
    metadata_dir = "space2stats_api/src/space2stats_ingest/METADATA"

    # Load the column types from JSON
    column_types_file = join(git_root, metadata_dir, "types.json")
    column_types = load_column_types_from_json(column_types_file)

    # Load metadata from the Excel file
    excel_path = join(git_root, metadata_dir, "Space2Stats Metadata Content.xlsx")
    metadata = load_metadata(excel_path)

    # Create STAC catalog
    catalog = create_stac_catalog(
        metadata["overview"],
        metadata["nada"],
    )

    # Create STAC collection
    collection = create_stac_collection(metadata["overview"])

    # Create STAC item
    item = create_stac_item(
        column_types,
        metadata,
    )

    # Add assets to item
    add_assets_to_item(item)

    # Add the collection to the catalog
    catalog.add_child(collection, title="Space2Stats Collection")

    # Add the item to the collection
    collection.add_item(item, title=item.properties["name"])

    # Save the catalog
    save_stac_catalog(catalog, join(git_root, metadata_dir, "stac"))


if __name__ == "__main__":
    main()
