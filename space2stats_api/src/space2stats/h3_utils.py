from typing import Any, Dict, List, Optional

import h3
from shapely.geometry import Point, Polygon, mapping, shape

# https://h3geo.org/docs/core-library/restable
MAX_RESOLUTION = 15


def _recursive_parent(
    h3_ids: List[str], resolution: int, target_resolution: int
) -> List[str]:
    """
    Recursively finds H3 hexagon IDs given a list of h3_ids, their resolution, and a target resolution.

    Parameters:
        h3_ids (List[str]): The list of h3_ids at the given resolution.
        resolution (int): The current H3 resolution.
        target_resolution (int): The target resolution for the parent cells.

    Returns:
        List[str]: A list of H3 hexagon IDs at the specified resolution.
    """
    # If we are already at the target resolution, return the h3_ids
    if resolution == target_resolution:
        return list(set(h3_ids))  # Return unique IDs only

    # If we need to go to a higher resolution (lower level)
    if resolution > target_resolution:
        parent_ids = {h3.h3_to_parent(h3_id, target_resolution) for h3_id in h3_ids}
        return list(parent_ids)

    return []


def _recursive_polyfill(
    aoi_geojson: Dict[str, Any], resolution: int, original_resolution: int
) -> List[str]:
    """
    Recursively finds H3 hexagon IDs for a given AOI GeoJSON object
    by using higher resolutions until a valid result is found,
    then returns the H3 IDs at the initial requested resolution.

    Parameters:
        aoi_geojson (Dict[str, Any]): The AOI GeoJSON object.
        resolution (int): The current H3 resolution.
        original_resolution (int): The original requested H3 resolution.

    Returns:
        List[str]: A list of H3 hexagon IDs at the specified resolution.
    """
    # Attempt to get H3 IDs at the current resolution
    h3_ids = h3.polyfill(aoi_geojson, resolution, geo_json_conformant=True)

    # If valid H3 IDs are found, return them
    if h3_ids:
        return _recursive_parent(h3_ids, resolution, original_resolution)

    # If we haven't reached the maximum resolution, try the next higher resolution
    if resolution < MAX_RESOLUTION:
        return _recursive_polyfill(aoi_geojson, resolution + 1, original_resolution)

    return []


def generate_h3_ids(
    aoi_geojson: Dict[str, Any], resolution: int, spatial_join_method: str
) -> List[str]:
    """
    Generate H3 hexagon IDs for a given AOI GeoJSON object.

    Parameters:
        aoi_geojson (Dict[str, Any]): The AOI GeoJSON object.
        resolution (int): The H3 resolution.
        spatial_join_method (str): The spatial join method to use.

    Returns:
        List[str]: A list of H3 hexagon IDs.
    """
    if spatial_join_method not in ["touches", "within", "centroid"]:
        raise ValueError("Invalid spatial join method")

    # Convert GeoJSON to Shapely geometry
    aoi_shape = shape(aoi_geojson)

    # Generate H3 hexagons covering the AOI
    h3_ids = _recursive_polyfill(aoi_geojson, resolution, resolution)

    # Filter hexagons based on spatial join method
    if spatial_join_method == "within":
        h3_ids = [
            h3_id
            for h3_id in h3_ids
            if aoi_shape.contains(Polygon(h3.h3_to_geo_boundary(h3_id, geo_json=True)))
        ]
    elif spatial_join_method == "centroid":
        h3_ids = [
            h3_id
            for h3_id in h3_ids
            if aoi_shape.contains(
                Polygon(h3.h3_to_geo_boundary(h3_id, geo_json=True)).centroid
            )
        ]

    return h3_ids


def generate_h3_geometries(
    h3_ids: List[str], geometry_type: Optional[str]
) -> List[Dict[str, Any]]:
    """
    Generate geometries (polygon or point) for a list of H3 hexagon IDs.

    Parameters:
        h3_ids (List[str]): A list of H3 hexagon IDs.
        geometry_type (Optional[str]):
            The type of geometry to generate ('polygon' or 'point').

    Returns:
        List[Dict[str, Any]]: A list of geometries in GeoJSON format.
    """
    if geometry_type not in ["polygon", "point"]:
        raise ValueError("Invalid geometry type")

    geometries = []
    for h3_id in h3_ids:
        if geometry_type == "polygon":
            hex_boundary = h3.h3_to_geo_boundary(h3_id, geo_json=True)
            geometries.append(mapping(Polygon(hex_boundary)))
        elif geometry_type == "point":
            # h3_to_geo does not have geo_json parameter to invert order of coords
            x, y = h3.h3_to_geo(h3_id)
            geometries.append(mapping(Point(y, x)))

    return geometries
