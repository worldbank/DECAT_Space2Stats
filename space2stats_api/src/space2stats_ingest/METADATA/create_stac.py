import ast
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
    sources["Variables"] = sources.apply(
        lambda x: ast.literal_eval(x["Variables"]), axis=1
    )
    return {
        "overview": overview,
        "nada": nada,
        "feature_catalog": feature_catalog,
        "sources": sources,
    }


# Function to create STAC catalog
def create_stac_catalog(
    overview: pd.DataFrame, nada: pd.DataFrame, catalog_dir: str
) -> Catalog:
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
        href="./catalog.json",
    )

    catalog.set_self_href(os.path.relpath("catalog.json", start=catalog_dir))

    return catalog


# Updated function to create STAC collection
def create_stac_collection(overview: pd.DataFrame) -> Collection:
    spatial_extent = SpatialExtent([[-180.0, -90.0, 180.0, 90.0]])
    temporal_extent = TemporalExtent([[datetime(2020, 1, 1), None]])

    extent = Extent(spatial=spatial_extent, temporal=temporal_extent)

    collection = Collection(
        id="space2stats-collection",
        description="This collection contains geospatial statistics for the entire globe standardized to a hexagonal grid (H3 level 6). It covers various themes, including demographic, socio-economic, and environmental data.",
        extent=extent,
        extra_fields={
            "Title": overview.loc["Title"].values[0],
            "Description": overview.loc["Description Resource"].values[0],
            "Keywords": ["space2stats", "sub-national", "h3", "hexagons", "global"],
        },
    )
    collection.set_self_href("collection.json")
    return collection


# Function to create STAC Item from GeoDataFrame
def create_stac_item(
    column_types: dict, feature_catalog: pd.DataFrame, item_dir: str
) -> Item:
    data_dict = []

    for column, dtype in column_types.items():
        description = feature_catalog.loc[
            feature_catalog["variable"] == column, "description"
        ].values[0]
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

    item = Item(
        id="space2stats_population_2020",
        geometry=geom,
        bbox=bbox,
        datetime=datetime.now(),
        properties={
            "name": "Population Data",
            "description": "Gridded population disaggregated by gender for the year 2020, with data available for different age groups.",
            "methodological_notes": "Global raster files are processed for each hexagonal grid using zonal statistics.",
            "source_data": "WorldPop gridded population, 2020, Unconstrained, UN-Adjusted",
            "sci:citation": "Stevens FR, Gaughan AE, Linard C, Tatem AJ (2015) Disaggregating Census Data for Population Mapping Using Random Forests with Remotely-Sensed and Ancillary Data.",
            "organization": "WorldPop, https://www.worldpop.org",
            "method": "sum",
            "resolution": "100 meters",
            "table:primary_geometry": "geometry",
            "table:columns": data_dict,
            "vector:layers": {
                "space2stats": column_types_with_geometry,
            },
            "themes": ["Demographics", "Population"],
        },
        stac_extensions=[
            "https://stac-extensions.github.io/table/v1.2.0/schema.json",
            "https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
        ],
    )

    item.set_self_href(os.path.join("items", f"{item.id}.json"))
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
    catalog.normalize_and_save(
        root_href=dest_dir, catalog_type=CatalogType.RELATIVE_PUBLISHED
    )
    adjust_self_href(join(dest_dir, "catalog.json"))


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
        join(git_root, metadata_dir, "stac"),
    )

    # Create STAC collection
    collection = create_stac_collection(metadata["overview"])

    # Create STAC item
    item = create_stac_item(
        column_types,
        metadata["feature_catalog"],
        join(git_root, metadata_dir, "stac"),
    )

    # Add assets to item
    add_assets_to_item(item)

    # Add the item to the collection
    collection.add_item(item)

    # Add the collection to the catalog
    catalog.add_child(collection)

    # Save the catalog
    save_stac_catalog(catalog, join(git_root, metadata_dir, "stac"))


if __name__ == "__main__":
    main()