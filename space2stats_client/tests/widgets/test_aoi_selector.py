import sys
from unittest.mock import MagicMock, patch

import ipywidgets as widgets
import pytest

sys.modules["ipyleaflet"] = MagicMock()


class MockMap(widgets.Widget):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.add_control = MagicMock()


class MockDrawControl(widgets.Widget):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.on_draw = MagicMock()
        self.clear = MagicMock()


with patch.dict("sys.modules", {"ipyleaflet": MagicMock()}):
    with patch("space2stats_client.widgets.aoi_selector.Map", MockMap), patch(
        "space2stats_client.widgets.aoi_selector.DrawControl", MockDrawControl
    ):
        from space2stats_client.widgets.aoi_selector import AOIContainer


class TestAOIContainer:
    """Test the core business logic that actually matters."""

    def test_init_empty(self):
        """Test container starts empty."""
        container = AOIContainer()
        assert not container
        assert container.gdf is None
        assert container.features == []

    def test_update_creates_feature(self):
        """Test updating with geometry creates features correctly."""
        container = AOIContainer()
        geo_json = {
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            }
        }

        with patch("geopandas.GeoDataFrame.from_features") as mock_gdf:
            container.update(geo_json)

            # Check feature was added with correct name
            assert len(container.features) == 1
            assert container.features[0]["geometry"] == geo_json["geometry"]
            assert container.features[0]["properties"]["name"] == "User AOI 1"

            # Check container state
            assert container.is_selected
            assert container.geojson["type"] == "FeatureCollection"
            mock_gdf.assert_called_once_with(container.features, crs="EPSG:4326")

    def test_multiple_updates_increment_names(self):
        """Test multiple features get incrementing names."""
        container = AOIContainer()

        with patch("geopandas.GeoDataFrame.from_features"):
            container.update({"geometry": {"type": "Point", "coordinates": [0, 0]}})
            container.update({"geometry": {"type": "Point", "coordinates": [1, 1]}})

            assert len(container.features) == 2
            assert container.features[0]["properties"]["name"] == "User AOI 1"
            assert container.features[1]["properties"]["name"] == "User AOI 2"

    def test_clear_resets_everything(self):
        """Test clearing removes all data."""
        container = AOIContainer()

        with patch("geopandas.GeoDataFrame.from_features"):
            container.update({"geometry": {"type": "Point", "coordinates": [0, 0]}})
            assert container.is_selected

            container.clear()

            assert not container
            assert container.features == []
            assert container.gdf is None
            assert container.geojson is None

    def test_repr_states(self):
        """Test string representation in different states."""
        container = AOIContainer()

        # Empty state
        assert "No AOI selected" in repr(container)

        # With data
        with patch("geopandas.GeoDataFrame.from_features") as mock_gdf:
            mock_gdf.return_value.__str__.return_value = "Mock GeoDataFrame"
            container.update({"geometry": {"type": "Point", "coordinates": [0, 0]}})
            assert "Mock GeoDataFrame" in str(container)

    def test_boolean_evaluation(self):
        """Test container boolean evaluation works correctly."""
        container = AOIContainer()

        # Empty container should be falsy
        assert not container
        assert bool(container) is False

        # Container with data should be truthy
        with patch("geopandas.GeoDataFrame.from_features"):
            container.update({"geometry": {"type": "Point", "coordinates": [0, 0]}})
            assert container
            assert bool(container) is True

    def test_geojson_structure(self):
        """Test that geojson property returns proper FeatureCollection structure."""
        container = AOIContainer()

        with patch("geopandas.GeoDataFrame.from_features"):
            # Add multiple features
            container.update({"geometry": {"type": "Point", "coordinates": [0, 0]}})
            container.update({"geometry": {"type": "Point", "coordinates": [1, 1]}})

            # Verify FeatureCollection structure
            geojson = container.geojson
            assert geojson["type"] == "FeatureCollection"
            assert "features" in geojson
            assert len(geojson["features"]) == 2

            # Verify each feature has correct structure
            for i, feature in enumerate(geojson["features"]):
                assert feature["type"] == "Feature"
                assert "geometry" in feature
                assert "properties" in feature
                assert feature["properties"]["name"] == f"User AOI {i+1}"

    def test_container_workflow(self):
        """Test complete container workflow with multiple operations."""
        container = AOIContainer()

        # Test multiple geometries
        geometries = [
            {
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
                }
            },
            {"geometry": {"type": "Point", "coordinates": [2, 2]}},
            {"geometry": {"type": "LineString", "coordinates": [[3, 3], [4, 4]]}},
        ]

        with patch("geopandas.GeoDataFrame.from_features"):
            # Add geometries one by one
            for i, geom in enumerate(geometries):
                container.update(geom)
                assert len(container.features) == i + 1
                assert (
                    container.features[i]["properties"]["name"] == f"User AOI {i + 1}"
                )
                assert container.is_selected

            # Verify final state
            assert len(container.features) == 3
            assert container.geojson["type"] == "FeatureCollection"
            assert len(container.geojson["features"]) == 3

            # Test clearing
            container.clear()
            assert not container.is_selected
            assert container.features == []
            assert container.gdf is None
            assert container.geojson is None
