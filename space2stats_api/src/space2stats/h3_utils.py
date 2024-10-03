from typing import Any, Dict, List, Literal

import pyarrow as pa
from h3ronpy import ContainmentMode
from h3ronpy.arrow.vector import (  # Import cells_to_coordinates for point geometries
    cells_to_coordinates,
    cells_to_wkb_polygons,
    geometry_to_cells,
)
from shapely import wkb
from shapely.geometry import Point, mapping, shape

CONTAINMENT_MODE_MAP = {
    "centroid": ContainmentMode.ContainsCentroid,
    "touches": ContainmentMode.IntersectsBoundary,
    "within": ContainmentMode.ContainsBoundary,
}


def generate_h3_ids(
    aoi_geojson: Dict[str, Any],
    resolution: int,
    spatial_join_method: Literal["touches", "within", "centroid"] = "centroid",
) -> List[int]:
    """
    Generate H3 IDs using h3ronpy's geometry_to_cells with the correct containment mode.
    Returns the H3 IDs in uint64 format for geometry creation.
    """
    geom = shape(aoi_geojson)
    containment_mode = CONTAINMENT_MODE_MAP.get(spatial_join_method)

    if containment_mode is None:
        raise ValueError(f"Invalid spatial join method: {spatial_join_method}")

    # Generate H3 IDs as uint64
    h3_ids_uint64 = geometry_to_cells(
        geom, resolution, containment_mode=containment_mode
    )

    return h3_ids_uint64


def generate_h3_geometries(
    h3_ids_uint64: List[int], geometry_type: Literal["polygon", "point"] = "polygon"
) -> List[Dict]:
    if geometry_type == "polygon":
        return _get_polygon(h3_ids_uint64)
    elif geometry_type == "point":
        return _get_point(h3_ids_uint64)
    else:
        raise ValueError("Invalid geometry type. Use 'polygon' or 'point'.")


def _get_polygon(h3_ids_uint64: List[int]) -> List[Dict]:
    wkb_geometries = cells_to_wkb_polygons(h3_ids_uint64)
    geojson_geometries = []
    for geom in wkb_geometries:
        if isinstance(geom, pa.LargeBinaryScalar) or isinstance(geom, pa.BinaryScalar):
            shapely_geom = wkb.loads(geom.as_py())
            geojson_geom = mapping(shapely_geom)
            geojson_geometries.append(geojson_geom)
        else:
            raise TypeError(
                f"Expected pa.LargeBinaryScalar or pa.BinaryScalar, but got {type(geom).__name__}"
            )
    return geojson_geometries


def _get_point(h3_ids_uint64: List[int]) -> List[Dict]:
    coords_table = cells_to_coordinates(h3_ids_uint64)
    latitudes = coords_table["lat"].to_numpy()
    longitudes = coords_table["lng"].to_numpy()

    geojson_geometries = []
    for lat, lon in zip(latitudes, longitudes):
        point_geom = Point(lon, lat)
        geojson_geometries.append(mapping(point_geom))
    return geojson_geometries
