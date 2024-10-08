from typing import Any, Dict, List, Literal

from h3ronpy import ContainmentMode
from h3ronpy.arrow.vector import (  # Import cells_to_coordinates for point geometries
    cells_to_wkb_points,
    cells_to_wkb_polygons,
    geometry_to_cells,
)
from shapely import from_wkb, to_geojson
from shapely.geometry import shape

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
        wkb_geometries = cells_to_wkb_polygons(h3_ids_uint64)
    elif geometry_type == "point":
        wkb_geometries = cells_to_wkb_points(h3_ids_uint64)
    else:
        raise ValueError(
            f"Invalid geometry type. Use 'polygon' or 'point', not {geometry_type}"
        )

    return to_geojson(from_wkb(wkb_geometries))
