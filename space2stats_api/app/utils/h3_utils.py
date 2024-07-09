from shapely.geometry import shape, Point
import h3

def generate_h3_ids(aoi_geom, resolution, spatial_join_method):
    if spatial_join_method == 'touches':
        hexes = h3.polyfill(aoi_geom, resolution, geo_json_conformant=True)
    elif spatial_join_method == 'centroid':
        hexes = [h3.geo_to_h3(*shape(aoi_geom).centroid.xy[::-1], resolution)]
    elif spatial_join_method == 'within':
        hexes = [h for h in h3.polyfill(aoi_geom, resolution, geo_json_conformant=True)
                 if Point(h3.h3_to_geo(h)).within(shape(aoi_geom))]
    else:
        raise ValueError("invalid spatial join method")
    return hexes