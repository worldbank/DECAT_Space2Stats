from itertools import chain
from typing import Any, Dict, List, Optional

import h3
from shapely.geometry import MultiPolygon, Point, Polygon, mapping, shape


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
    geoms = (
        [mapping(geom) for geom in aoi_shape.geoms]
        if isinstance(aoi_shape, MultiPolygon)
        else [aoi_geojson]
    )
    h3_ids = list(
        # Use set to remove duplicates
        set(
            # Treat list of sets as single iterable
            chain(
                *[
                    # Generate H3 hexagons for each geometry
                    h3.polyfill(geom, resolution, geo_json_conformant=True)
                    for geom in geoms
                ]
            )
        )
    )

    # Filter hexagons based on spatial join method
    # Touches method returns plain h3_ids
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
