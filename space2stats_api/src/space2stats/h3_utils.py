from typing import Any, Dict, List, Literal

import pyarrow as pa
from h3ronpy import ContainmentMode
from h3ronpy.arrow.vector import (
    cells_to_wkb_points,
    cells_to_wkb_polygons,
    geometry_to_cells,
)
from shapely import wkb
from shapely.geometry import mapping, shape

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
    """
    Generate geometries (GeoJSON polygons or points) for a list of H3 hexagon IDs (as uint64).

    Parameters:
        h3_ids_uint64 (List[int]): A list of H3 hexagon IDs in uint64 format.
        geometry_type (Optional[str]):
            The type of geometry to generate ('polygon' or 'point').

    Returns:
        List[Dict]: A list of GeoJSON geometries (Polygon or Point).
    """
    if geometry_type == "polygon":
        # Convert H3 cells to WKB polygons
        wkb_geometries = cells_to_wkb_polygons(h3_ids_uint64)
    elif geometry_type == "point":
        # Convert H3 cells to WKB points (centroid of each hexagon)
        wkb_geometries = cells_to_wkb_points(h3_ids_uint64)
    else:
        raise ValueError("Invalid geometry type. Use 'polygon' or 'point'.")

    # Convert pyarrow Array to GeoJSON geometries
    geojson_geometries = []
    for geom in wkb_geometries:
        if isinstance(geom, pa.Scalar):
            # Convert to buffer and then to bytes
            shapely_geom = wkb.loads(geom.as_buffer().to_pybytes())
            # Convert Shapely geometry to GeoJSON-like dict
            geojson_geometries.append(mapping(shapely_geom))
        else:
            raise TypeError(f"Expected pyarrow Scalar, but got {type(geom).__name__}")

    return geojson_geometries
