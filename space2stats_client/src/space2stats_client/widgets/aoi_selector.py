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


def AOISelector(
    center: Tuple[float, float] = (27.0, 29.7), zoom: int = 6
) -> Tuple[widgets.Widget, AOIContainer]:
    """Create an AOI selector widget and container.

    Args:
        center: Initial map center as (lat, lon) tuple
        zoom: Initial zoom level

    Returns:
        Tuple of (widget, aoi_container) where:
        - widget: The interactive map widget to display
        - aoi_container: Container that gets populated with AOI when user draws

    Example:
        ```python
        from space2stats_client import AOISelector

        # Create the selector and container
        widget, aoi = AOISelector(center=(27.0, 29.7), zoom=6)
        display(widget)

        # After user draws on map, aoi will be populated
        if aoi:
            data = client.get_summary(gdf=aoi.gdf, fields=fields, spatial_join_method="touches")
        ```
    """

    # Create the AOI container that will be populated
    aoi_container = AOIContainer()

    # Create output widget for feedback
    output = widgets.Output()
    info_display = widgets.HTML()

    # Initialize map
    map_widget = Map(center=center, zoom=zoom)

    # Create drawing control
    draw_control = DrawControl(
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

    def handle_draw(target, action, geo_json):
        """Handle drawing events on the map."""
        if action == "created":
            # Update the AOI container
            aoi_container.update(geo_json)

            with output:
                output.clear_output()
                print("✓ AOI captured successfully!")
                print(f"Geometry type: {geo_json['geometry']['type']}")

                # Calculate basic info about the LAST drawn polygon (most recent)
                try:
                    if aoi_container.gdf is not None:
                        # Get the last row (most recently added polygon)
                        last_polygon = aoi_container.gdf.iloc[-1:]
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
                        if len(aoi_container.gdf) > 1:
                            print(f"Total polygons: {len(aoi_container.gdf)}")
                except Exception as e:
                    print(f"Could not calculate area: {e}")

            # Update info display
            polygon_count = (
                len(aoi_container.gdf) if aoi_container.gdf is not None else 0
            )
            if polygon_count > 1:
                status_text = (
                    f"<strong>✓ AOI Selected ({polygon_count} polygons)</strong><br>"
                )
            else:
                status_text = "<strong>✓ AOI Selected</strong><br>"

            info_display.value = (
                "<div style='padding: 10px; background-color: #d4edda; border: 1px solid #c3e6cb; border-radius: 5px; color: #155724;'>"
                + status_text
                + "AOI variable is now populated and ready to use."
                "</div>"
            )

    # Set up event handler and add to map
    draw_control.on_draw(handle_draw)
    map_widget.add_control(draw_control)

    # Create clear button
    clear_button = widgets.Button(
        description="Clear AOI",
        button_style="warning",
        tooltip="Clear the selected AOI",
    )

    def on_clear_clicked(b):
        aoi_container.clear()
        draw_control.clear()

        with output:
            output.clear_output()
            print("AOI cleared.")

        info_display.value = (
            "<div style='padding: 10px; background-color: #fff3cd; border: 1px solid #ffeaa7; border-radius: 5px; color: #856404;'>"
            "No AOI selected. Draw a polygon or rectangle on the map to select an area."
            "</div>"
        )

    clear_button.on_click(on_clear_clicked)

    # Create header
    header = widgets.HTML("<h3>🗺️ Area of Interest (AOI) Selector</h3>")

    # Create collapsible instructions
    instructions_content = widgets.HTML(
        "• Use the drawing tools on the map to create a polygon or rectangle<br>"
        "• Click on the polygon or rectangle tool in the map toolbar<br>"
        "• Draw your area of interest by clicking points on the map<br>"
        "• The AOI variable will be automatically populated when you finish drawing<br>"
        "• Use the AOI variable directly in your Space2Stats client calls"
    )

    instructions = widgets.Accordion(children=[instructions_content])
    instructions.set_title(0, "📋 Instructions")
    instructions.selected_index = None  # Start collapsed

    # Initial status
    info_display.value = (
        "<div style='padding: 10px; background-color: #fff3cd; border: 1px solid #ffeaa7; border-radius: 5px; color: #856404;'>"
        "No AOI selected. Draw a polygon or rectangle on the map to select an area."
        "</div>"
    )

    # Layout all components
    widget = widgets.VBox(
        [
            header,
            info_display,
            map_widget,
            widgets.HBox([clear_button]),
            instructions,
            output,
        ]
    )

    return widget, aoi_container
