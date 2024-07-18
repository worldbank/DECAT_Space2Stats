from typing import Dict, Any, List
from shapely.geometry import shape, Polygon
import h3

def generate_h3_ids(aoi_geojson: Dict[str, Any], resolution: int, spatial_join_method: str) -> List[str]:
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
    h3_ids = h3.polyfill(aoi_geojson, resolution, geo_json_conformant=True)

    # Filter hexagons based on spatial join method
    # Touches method returns plain h3_ids
    if spatial_join_method == "within":
        h3_ids = [h3_id for h3_id in h3_ids if aoi_shape.contains(Polygon(h3.h3_to_geo_boundary(h3_id, geo_json=True)))]
    elif spatial_join_method == "centroid":
        h3_ids = [h3_id for h3_id in h3_ids if aoi_shape.contains(Polygon(h3.h3_to_geo_boundary(h3_id, geo_json=True)).centroid)]

    return h3_ids