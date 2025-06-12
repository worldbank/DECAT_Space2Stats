"""Interactive AOI (Area of Interest) selection widget for Space2Stats client."""

import json
from typing import Dict, List, Optional, Tuple

import geopandas as gpd
import ipywidgets as widgets
from ipyleaflet import DrawControl, Map
from IPython.display import display


class AOIContainer:
    """Container to hold the selected AOI that gets automatically updated."""

    def __init__(self):
        self.gdf = None
        self.geojson = None
        self.features = []  # Keep track of all features
        self.is_selected = False

    def update(self, geo_json: Dict):
        """Update the container with new AOI data."""
        # Create new feature
        new_feature = {
            "type": "Feature",
            "geometry": geo_json["geometry"],
            "properties": {"name": f"User AOI {len(self.features) + 1}"},
        }

        # Add to features list
        self.features.append(new_feature)

        # Update GeoDataFrame with all features
        self.gdf = gpd.GeoDataFrame.from_features(self.features, crs="EPSG:4326")

        # Update geojson to be a FeatureCollection
        self.geojson = {"type": "FeatureCollection", "features": self.features}

        self.is_selected = True

    def clear(self):
        """Clear the AOI data."""
        self.gdf = None
        self.geojson = None
        self.features = []
        self.is_selected = False

    def __bool__(self):
        """Return True if AOI is selected."""
        return self.is_selected

    def __repr__(self):
        if self.is_selected:
            return str(self.gdf)
        else:
            return "AOI Container (No AOI selected)"


class AOISelector:
    """Interactive AOI (Area of Interest) selection widget for Space2Stats client.

    This widget provides an interactive map interface for selecting Areas of Interest (AOI)
    by drawing polygons or rectangles directly on the map. The AOI is automatically captured
    in a GeoDataFrame that can be used directly with Space2Stats API calls.

    Example:
        ```python
        from space2stats_client.widgets.aoi_selector import AOISelector

        # Create and display the selector
        aoi_selector = AOISelector(center=(27.0, 29.7), zoom=6)
        aoi_selector.display()

        # After drawing on the map, access the AOI data
        if aoi_selector.aoi:
            data = client.get_summary(
                gdf=aoi_selector.aoi.gdf,
                fields=fields,
                spatial_join_method="touches"
            )
        ```
    """

    def __init__(self, center: Tuple[float, float] = (27.0, 29.7), zoom: int = 6):
        """Initialize the AOI selector widget.

        Args:
            center: Initial map center as (lat, lon) tuple
            zoom: Initial zoom level
        """
        self.center = center
        self.zoom = zoom

        # Create the AOI container that will be populated
        self.aoi_container = AOIContainer()

        # Create UI components
        self._create_ui_components()
        self._setup_widget()

    def _create_ui_components(self):
        """Create all UI components."""
        # Create output widget for feedback
        self.output = widgets.Output()
        self.info_display = widgets.HTML()

        # Initialize map
        self.map_widget = Map(center=self.center, zoom=self.zoom)

        # Create drawing control
        self.draw_control = DrawControl(
            polygon={
                "shapeOptions": {"color": "#6e6d6b", "weight": 2, "fillOpacity": 0.5},
                "drawError": {"color": "#dd253b", "message": "Error drawing shape!"},
            },
            rectangle={
                "shapeOptions": {"color": "#6e6d6b", "weight": 2, "fillOpacity": 0.5}
            },
            marker={},
            circlemarker={},
            polyline={},
        )

        # Create clear button
        self.clear_button = widgets.Button(
            description="Clear AOI",
            button_style="warning",
            tooltip="Clear the selected AOI",
        )

        # Create header
        self.header = widgets.HTML("<h3>🗺️ Area of Interest (AOI) Selector</h3>")

        # Create collapsible instructions
        instructions_content = widgets.HTML(
            "• Use the drawing tools on the map to create a polygon or rectangle<br>"
            "• Click on the polygon or rectangle tool in the map toolbar<br>"
            "• Draw your area of interest by clicking points on the map<br>"
            "• The AOI variable will be automatically populated when you finish drawing<br>"
            "• Use the AOI variable directly in your Space2Stats client calls"
        )

        self.instructions = widgets.Accordion(children=[instructions_content])
        self.instructions.set_title(0, "📋 Instructions")
        self.instructions.selected_index = None  # Start collapsed

    def _setup_widget(self):
        """Set up widget interactions and layout."""
        # Set up event handlers
        self.draw_control.on_draw(self._handle_draw)
        self.map_widget.add_control(self.draw_control)
        self.clear_button.on_click(self._on_clear_clicked)

        # Initial status
        self.info_display.value = (
            "<div style='padding: 10px; background-color: #fff3cd; border: 1px solid #ffeaa7; border-radius: 5px; color: #856404;'>"
            "No AOI selected. Draw a polygon or rectangle on the map to select an area."
            "</div>"
        )

        # Layout all components
        self.widget = widgets.VBox(
            [
                self.header,
                self.info_display,
                self.map_widget,
                widgets.HBox([self.clear_button]),
                self.instructions,
                self.output,
            ]
        )

    def _handle_draw(self, target, action, geo_json):
        """Handle drawing events on the map."""
        if action == "created":
            # Update the AOI container
            self.aoi_container.update(geo_json)

            with self.output:
                self.output.clear_output()
                print("✓ AOI captured successfully!")
                print(f"Geometry type: {geo_json['geometry']['type']}")

                # Calculate basic info about the LAST drawn polygon (most recent)
                try:
                    if self.aoi_container.gdf is not None:
                        # Get the last row (most recently added polygon)
                        last_polygon = self.aoi_container.gdf.iloc[-1:]
                        area_km2 = (
                            last_polygon.to_crs("EPSG:3857").area.iloc[0] / 1_000_000
                        )
                        bounds = last_polygon.bounds.iloc[0]
                        max_lat = max(abs(bounds["miny"]), abs(bounds["maxy"]))
                        if max_lat > 60:
                            print(
                                "⚠️ Area calculation may have >50% error at high latitudes"
                            )
                        elif max_lat > 30:
                            print(
                                "⚠️ Area calculation may have 10-40% error at mid-latitudes"
                            )
                        print(f"Approx. Area: {area_km2:.2f} km² (see docs)")
                        print(
                            f"Bounds: ({bounds['minx']:.4f}, {bounds['miny']:.4f}) to ({bounds['maxx']:.4f}, {bounds['maxy']:.4f})"
                        )

                        # Show total count if multiple polygons
                        if len(self.aoi_container.gdf) > 1:
                            print(f"Total polygons: {len(self.aoi_container.gdf)}")
                except Exception as e:
                    print(f"Could not calculate area: {e}")

            # Update info display
            polygon_count = (
                len(self.aoi_container.gdf) if self.aoi_container.gdf is not None else 0
            )
            if polygon_count > 1:
                status_text = (
                    f"<strong>✓ AOI Selected ({polygon_count} polygons)</strong><br>"
                )
            else:
                status_text = "<strong>✓ AOI Selected</strong><br>"

            self.info_display.value = (
                "<div style='padding: 10px; background-color: #d4edda; border: 1px solid #c3e6cb; border-radius: 5px; color: #155724;'>"
                + status_text
                + "AOI variable is now populated and ready to use."
                "</div>"
            )

    def _on_clear_clicked(self, b):
        """Handle clear button click."""
        self.aoi_container.clear()
        self.draw_control.clear()

        with self.output:
            self.output.clear_output()
            print("AOI cleared.")

        self.info_display.value = (
            "<div style='padding: 10px; background-color: #fff3cd; border: 1px solid #ffeaa7; border-radius: 5px; color: #856404;'>"
            "No AOI selected. Draw a polygon or rectangle on the map to select an area."
            "</div>"
        )

    @property
    def aoi(self) -> AOIContainer:
        """Get the AOI container with selected areas.

        Returns:
            AOIContainer: Container with the selected AOI data (gdf, geojson, features)
        """
        return self.aoi_container

    def clear_aoi(self):
        """Programmatically clear the selected AOI."""
        self._on_clear_clicked(None)

    def display(self):
        """Display the AOI selector widget."""
        display(self.widget)
