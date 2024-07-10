from shapely.geometry import shape, Point, Polygon
import h3

def h3_to_shapely_polygon(h):
    """Convert H3 hexagon to a shapely polygon."""
    boundary = h3.h3_to_geo_boundary(h, geo_json=True)
    return Polygon(boundary)

def generate_h3_ids(aoi_geom, resolution, spatial_join_method):
    if spatial_join_method not in ['touches', 'centroid', 'within']:
        raise ValueError("invalid spatial join method")

    geom_shape = shape(aoi_geom)

    # generate hexes to filter afterwards
    hexes = h3.polyfill(aoi_geom, resolution, geo_json_conformant=True)

    if spatial_join_method == 'centroid':
        hexes = [hex for hex in hexes if Point(h3.h3_to_geo(hex)[::-1]).intersects(geom_shape)]
    elif spatial_join_method == 'within':
        hexes = [h for h in hexes if h3_to_shapely_polygon(h).within(geom_shape)]

    return hexes