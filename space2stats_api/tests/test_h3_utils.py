import pytest
from shapely.geometry import Polygon, mapping
from app.utils.h3_utils import generate_h3_ids, generate_h3_geometries

polygon_coords = [
    [-74.3, 40.5],
    [-73.7, 40.5],
    [-73.7, 40.9],
    [-74.3, 40.9],
    [-74.3, 40.5],
]
polygon = Polygon(polygon_coords)
aoi_geojson = mapping(polygon)
resolution = 6


def test_generate_h3_ids_within():
    h3_ids = generate_h3_ids(aoi_geojson, resolution, "within")
    print(f"Test 'within' - Generated H3 IDs: {h3_ids}")
    assert len(h3_ids) > 0, "Expected at least one H3 ID"


def test_generate_h3_ids_touches():
    h3_ids = generate_h3_ids(aoi_geojson, resolution, "touches")
    print(f"Test 'touches' - Generated H3 IDs: {h3_ids}")
    assert len(h3_ids) > 0, "Expected at least one H3 ID"


def test_generate_h3_ids_centroid():
    h3_ids = generate_h3_ids(aoi_geojson, resolution, "centroid")
    print(f"Test 'centroid' - Generated H3 IDs: {h3_ids}")
    assert len(h3_ids) > 0, "Expected at least one H3 ID for centroid"


def test_generate_h3_ids_invalid_method():
    with pytest.raises(ValueError, match="Invalid spatial join method"):
        generate_h3_ids(aoi_geojson, resolution, "invalid_method")


def test_generate_h3_geometries_polygon():
    h3_ids = generate_h3_ids(aoi_geojson, resolution, "touches")
    geometries = generate_h3_geometries(h3_ids, "polygon")
    assert len(geometries) == len(
        h3_ids
    ), "Expected the same number of geometries as H3 IDs"
    for geom in geometries:
        assert geom["type"] == "Polygon", "Expected Polygon geometry"


def test_generate_h3_geometries_point():
    h3_ids = generate_h3_ids(aoi_geojson, resolution, "touches")
    geometries = generate_h3_geometries(h3_ids, "point")
    assert len(geometries) == len(
        h3_ids
    ), "Expected the same number of geometries as H3 IDs"
    for geom in geometries:
        assert geom["type"] == "Point", "Expected Point geometry"


def test_generate_h3_geometries_invalid_type():
    h3_ids = generate_h3_ids(aoi_geojson, resolution, "touches")
    with pytest.raises(ValueError, match="Invalid geometry type"):
        generate_h3_geometries(h3_ids, "invalid_type")


if __name__ == "__main__":
    pytest.main()
