# Space2Stats Python Client

A Python client for accessing the Space2Stats API, providing easy access to consistent, comparable, and authoritative sub-national variation data from the World Bank.


## API Methods

### `get_topics()`
Returns a DataFrame containing available dataset themes/topics from the STAC catalog.

---

### `get_fields()`
Returns a list of all available fields that can be used with the API.

---

### `get_properties(item_id: str)`
Returns a DataFrame with descriptions of variables for a specific dataset.

---

### `fetch_admin_boundaries(iso3: str, adm: str)`
Fetches administrative boundaries from GeoBoundaries API for a given country and admin level.

---

### `get_summary(gdf, spatial_join_method, fields)`
Extracts H3 level data for areas of interest.
- **Parameters:**
  - `gdf`: GeoDataFrame containing areas of interest
  - `spatial_join_method`: "touches", "centroid", or "within"
  - `fields`: List of field names to retrieve
  - `geometry`: Optional "polygon" or "point" to include H3 geometries
  - `verbose`: Optional boolean to display progress messages

---

### `get_aggregate(gdf, spatial_join_method, fields, aggregation_type)`
Extracts summary statistics from H3 data.
- **Parameters:**
  - `gdf`: GeoDataFrame containing areas of interest
  - `spatial_join_method`: "touches", "centroid", or "within"
  - `fields`: List of field names to retrieve
  - `aggregation_type`: "sum", "avg", "count", "max", or "min"
  - `verbose`: Optional boolean to display progress messages

---

### `get_summary_by_hexids(hex_ids, fields, geometry)`
Retrieves statistics for specific H3 hexagon IDs.
- **Parameters:**
  - `hex_ids`: List of H3 hexagon IDs to query
  - `fields`: List of field names to retrieve
  - `geometry`: Optional; specifies if H3 geometries should be included ("polygon" or "point")
  - `verbose`: Optional boolean to display progress messages


---

### `get_aggregate_by_hexids(hex_ids, fields, aggregation_type)`
Aggregates statistics for specific H3 hexagon IDs.
- **Parameters:**
  - `hex_ids`: List of H3 hexagon IDs to aggregate
  - `fields`: List of field names to aggregate
  - `aggregation_type`: Type of aggregation ("sum", "avg", "count", "max", "min")
  - `verbose`: Optional boolean to display progress messages


---

### `get_timeseries_fields()`
Returns a list of available fields from the timeseries table.

---

### `get_timeseries(gdf, spatial_join_method, fields, start_date=None, end_date=None)`
Gets timeseries data for areas of interest.
- **Parameters:**
  - `gdf`: GeoDataFrame containing areas of interest
  - `spatial_join_method`: "touches", "centroid", or "within"
  - `fields`: List of field names to retrieve
  - `start_date`: Optional start date (format: 'YYYY-MM-DD')
  - `end_date`: Optional end date (format: 'YYYY-MM-DD')
  - `geometry`: Optional "polygon" or "point" to include H3 geometries
  - `verbose`: Optional boolean to display progress messages

---

### `get_timeseries_by_hexids(hex_ids, fields, start_date=None, end_date=None)`
Gets timeseries data for specific H3 hexagon IDs.
- **Parameters:**
  - `hex_ids`: List of H3 hexagon IDs to query
  - `fields`: List of field names to retrieve
  - `start_date`: Optional start date (format: 'YYYY-MM-DD')
  - `end_date`: Optional end date (format: 'YYYY-MM-DD')
  - `geometry`: Optional "polygon" or "point" to include H3 geometries
  - `verbose`: Optional boolean to display progress messages

## Interactive Widgets

Space2Stats provides interactive widgets that make it easy to explore, select data fields, and define areas of interest in Jupyter notebooks.

### AOISelector

This widget provides an interactive map interface for selecting Areas of Interest (AOI) by drawing polygons or rectangles directly on the map. The AOI is automatically captured in a GeoDataFrame that can be used directly with Space2Stats API calls.

```python
from space2stats_client import AOISelector

# Create the AOI selector widget and container
widget, aoi = AOISelector(center=(27.0, 29.7), zoom=6)
display(widget)

# After drawing on the map, the aoi container is automatically populated
# Check if an AOI has been selected
if aoi:
    print(f"AOI selected with {len(aoi.gdf)} polygon(s)")
    print(aoi.gdf)
    
    # Use the AOI directly in API calls
    client = Space2StatsClient()
    summary = client.get_summary(
        gdf=aoi.gdf,
        spatial_join_method="centroid",
        fields=["population", "gdp"]
    )
```

**Key Features:**
- Interactive map with drawing tools (polygon and rectangle)
- Real-time feedback on selected area with approximate area calculation
- Support for multiple polygons in a single AOI
- Clear button to reset the selection
- Collapsible instructions panel
- Automatic population of GeoDataFrame for immediate use

### CrossSectionFieldSelector

This widget helps users interactively select fields from the Space2Stats API for cross-sectional data. Fields are organized by their source STAC items for easier navigation.

```python
from space2stats_client import Space2StatsClient, CrossSectionFieldSelector

# Initialize the client
client = Space2StatsClient()

# Create the field selector widget
selector = CrossSectionFieldSelector(client)

# Display the interactive widget in your notebook
selector.display()

# Later, retrieve the selected fields
selected_fields = selector.get_selected_fields()

# Use the selected fields in an API call
gdf = gpd.read_file("path/to/your/area.geojson")
summary = client.get_summary(
    gdf=gdf,
    spatial_join_method="centroid",
    fields=selected_fields
)
```

### TimeSeriesFieldSelector

This widget allows users to interactively select fields for time series data and specify a valid time period based on the available data range.

```python
from space2stats_client import Space2StatsClient, TimeSeriesFieldSelector

# Initialize the client
client = Space2StatsClient()

# Create the time series field selector widget
ts_selector = TimeSeriesFieldSelector(client)

# Display the interactive widget in your notebook
ts_selector.display()

# Later, retrieve the selected fields and time period
selections = ts_selector.get_selections()

# Use the selections in a time series API call
gdf = gpd.read_file("path/to/your/area.geojson")
timeseries_data = client.get_timeseries(
    gdf=gdf,
    spatial_join_method="centroid",
    fields=selections['fields'],
    start_date=selections['time_period']['start_date'].strftime('%Y-%m-%d'),
    end_date=selections['time_period']['end_date'].strftime('%Y-%m-%d')
)
```

## Quick Start

```bash
pip install space2stats-client
```

```python
from space2stats_client import Space2StatsClient
import geopandas as gpd

# Initialize the client
client = Space2StatsClient()

# Get available topics/datasets
topics = client.get_topics()
print(topics)

# Get fields for all datasets
fields = client.get_fields()
print(fields)

# Get data for an area of interest
gdf = gpd.read_file("path/to/your/area.geojson")
summary = client.get_summary(
    gdf=gdf,
    spatial_join_method="centroid",  # Options: "touches", "centroid", "within"
    fields=["population", "gdp"],
    geometry="polygon"  # Optional: "polygon" or "point"
)

# Get aggregated statistics
aggregates = client.get_aggregate(
    gdf=gdf,
    spatial_join_method="centroid",  # Options: "touches", "centroid", "within"
    fields=["population", "gdp"],
    aggregation_type="sum"  # Options: "sum", "avg", "count", "max", "min"
)

# Get timeseries data
timeseries = client.get_timeseries(
    gdf=gdf,
    spatial_join_method="centroid",
    fields=["spi"],
    start_date="2020-01-01",  # Optional
    end_date="2020-12-31",    # Optional
    geometry="polygon"         # Optional
)

# Get time series data
timeseries = client.get_timeseries(
    gdf=gdf,
    spatial_join_method="centroid",
    fields=["spi"],
    start_date="2020-01-01",
    end_date="2020-12-31"
)
```

## Documentation

For full documentation, visit [Space2Stats Documentation](https://worldbank.github.io/DECAT_Space2Stats/).

## License

This project is licensed under the World Bank Master Community License Agreement. See the LICENSE file for details.

## Disclaimer

The World Bank makes no warranties regarding the accuracy, reliability, or completeness of the results and content. The World Bank disclaims any liability arising from the use of this software.