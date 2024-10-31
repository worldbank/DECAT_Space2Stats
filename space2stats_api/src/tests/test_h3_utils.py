import pytest
from shapely.geometry import MultiPolygon, Polygon, mapping
from space2stats.h3_utils import generate_h3_geometries, generate_h3_ids

polygon_coords_1 = [
    [-74.3, 40.5],
    [-74.2, 40.5],
    [-74.2, 40.6],
    [-74.3, 40.6],
    [-74.3, 40.5],
]

# Coordinates for another polygon in a different, non-overlapping area
polygon_coords_2 = [
    [-74.1, 40.7],
    [-74.0, 40.7],
    [-74.0, 40.8],
    [-74.1, 40.8],
    [-74.1, 40.7],
]

# Create a MultiPolygon object
multi_polygon = MultiPolygon([Polygon(polygon_coords_1), Polygon(polygon_coords_2)])
aoi_geojson_multi = mapping(multi_polygon)
resolution = 6


def test_generate_h3_ids_within_multipolygon():
    h3_ids = generate_h3_ids(aoi_geojson_multi, resolution, "within")
    print(h3_ids)
    print(f"Test 'within' MultiPolygon - Generated H3 IDs: {h3_ids}")
    assert len(h3_ids) > 0, "Expected at least one H3 ID for MultiPolygon"


def test_generate_h3_ids_touches_multipolygon():
    h3_ids = generate_h3_ids(aoi_geojson_multi, resolution, "touches")
    print(h3_ids)
    print(f"Test 'touches' MultiPolygon - Generated H3 IDs: {h3_ids}")
    assert len(h3_ids) > 0, "Expected at least one H3 ID for MultiPolygon"


def test_generate_h3_ids_centroid_multipolygon():
    h3_ids = generate_h3_ids(aoi_geojson_multi, resolution, "centroid")
    print(h3_ids)
    print(f"Test 'centroid' MultiPolygon - Generated H3 IDs: {h3_ids}")
    assert len(h3_ids) > 0, "Expected at least one H3 ID for centroid with MultiPolygon"


def test_generate_h3_geometries_polygon_multipolygon():
    h3_ids = generate_h3_ids(aoi_geojson_multi, resolution, "touches")
    geometries = generate_h3_geometries(h3_ids, "polygon")
    assert len(geometries) == len(
        h3_ids
    ), "Expected the same number of geometries as H3 IDs"
    for geom in geometries:
        assert geom["type"] == "Polygon", "Expected Polygon geometry for MultiPolygon"


def test_generate_h3_geometries_point_multipolygon():
    h3_ids = generate_h3_ids(aoi_geojson_multi, resolution, "touches")
    geometries = generate_h3_geometries(h3_ids, "point")
    assert len(geometries) == len(
        h3_ids
    ), "Expected the same number of geometries as H3 IDs"
    for geom in geometries:
        assert geom["type"] == "Point", "Expected Point geometry for MultiPolygon"


def test_generate_h3_sliver_polygon_touches():
    data = generate_h3_ids(
        {
            "type": "Polygon",
            "coordinates": [
                [
                    [41.14127371265408, -2.1034653113510444],
                    [41.140645873470845, -2.104696345752785],
                    [41.14205369446421, -2.104701102391104],
                    [41.14127371265408, -2.1034653113510444],
                ]
            ],
        },
        6,
        "touches",
    )

    for h in ["867a74817ffffff", "867a74807ffffff"]:
        assert h in data, f"Missing {h} in generated hexagons"
    assert len(data) == 2


def test_generate_h3_sliver_polygon_within():
    data = generate_h3_ids(
        {
            "type": "Polygon",
            "coordinates": [
                [
                    [41.14127371265408, -2.1034653113510444],
                    [41.140645873470845, -2.104696345752785],
                    [41.14205369446421, -2.104701102391104],
                    [41.14127371265408, -2.1034653113510444],
                ]
            ],
        },
        6,
        "within",
    )
    assert len(data) == 0, "Expected no hexagons to match"


def test_generate_h3_sliver_polygon_centroid():
    data = generate_h3_ids(
        {
            "type": "Polygon",
            "coordinates": [
                [
                    [41.14127371265408, -2.1034653113510444],
                    [41.140645873470845, -2.104696345752785],
                    [41.14205369446421, -2.104701102391104],
                    [41.14127371265408, -2.1034653113510444],
                ]
            ],
        },
        6,
        "centroid",
    )
    h = "867a74817ffffff"
    assert "867a74817ffffff" in data, f"{h} not in generated hexagons"
    assert len(data) == 1


if __name__ == "__main__":
    pytest.main()
