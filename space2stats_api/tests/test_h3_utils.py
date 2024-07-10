import pytest
from shapely.geometry import Polygon, mapping
from app.utils.h3_utils import generate_h3_ids

def test_generate_h3_ids_within():
    polygon = Polygon([
        [-74.1, 40.6],
        [-73.9, 40.6],
        [-73.9, 40.8],
        [-74.1, 40.8],
        [-74.1, 40.6]
    ])
    
    aoi_geojson = mapping(polygon)
    resolution = 6
    h3_ids = generate_h3_ids(aoi_geojson, resolution, 'within')
    
    print(f"Test 'within' - Generated H3 IDs: {h3_ids}")  # Debug print
    assert len(h3_ids) > 0, "Expected at least one H3 ID"

def test_generate_h3_ids_touches():
    polygon = Polygon([
        [-74.1, 40.6],
        [-73.9, 40.6],
        [-73.9, 40.8],
        [-74.1, 40.8],
        [-74.1, 40.6]
    ])
    
    aoi_geojson = mapping(polygon)
    resolution = 6
    h3_ids = generate_h3_ids(aoi_geojson, resolution, 'touches')
    
    print(f"Test 'touches' - Generated H3 IDs: {h3_ids}")  # Debug print
    assert len(h3_ids) > 0, "Expected at least one H3 ID"

def test_generate_h3_ids_centroid():
    polygon = Polygon([
        [-74.1, 40.6],
        [-73.9, 40.6],
        [-73.9, 40.8],
        [-74.1, 40.8],
        [-74.1, 40.6]
    ])
    
    aoi_geojson = mapping(polygon)
    resolution = 6
    h3_ids = generate_h3_ids(aoi_geojson, resolution, 'centroid')
    
    print(f"Test 'centroid' - Generated H3 IDs: {h3_ids}")  # Debug print
    assert len(h3_ids) > 0, "Expected at least one H3 ID for centroid"

def test_generate_h3_ids_invalid_method():
    polygon = Polygon([
        [-74.1, 40.6],
        [-73.9, 40.6],
        [-73.9, 40.8],
        [-74.1, 40.8],
        [-74.1, 40.6]
    ])
    
    aoi_geojson = mapping(polygon)
    resolution = 6
    
    with pytest.raises(ValueError, match="invalid spatial join method"):
        generate_h3_ids(aoi_geojson, resolution, 'invalid_method')

if __name__ == "__main__":
    pytest.main()