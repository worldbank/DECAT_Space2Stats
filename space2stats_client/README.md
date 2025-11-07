# Space2Stats Python Client

A Python client for accessing the Space2Stats API, providing easy access to consistent, comparable, and authoritative sub-national variation data from the World Bank.

## Installation

### Core package only
```bash
pip install space2stats-client
```

### With optional interactive widgets
```bash
pip install space2stats-client[widgets]
```

The widgets provide interactive Jupyter notebook components for data exploration and area selection. They require additional dependencies (`ipywidgets`, `ipyleaflet`, `IPython`) that are not needed for core API functionality.

## Configuration

When creating a `Space2StatsClient`, you can adjust how it connects to the API:

```python
from space2stats_client import Space2StatsClient

client = Space2StatsClient(
    base_url="https://space2stats.ds.io",  # point to a staging endpoint if needed
    verify_ssl=True,                        # set to False when working behind intercepting proxies
)
```

Both arguments are optional. `base_url` defaults to the production API endpoint and `verify_ssl` defaults to `True` for secure requests.

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

### `fetch_admin_boundaries(iso3: str, adm: str, source="WB")`
Fetches administrative boundaries for a given country and admin level.
- **Parameters:**
  - `source`: Data source for the boundaries. `"WB"` (default) uses the World Bank Global Administrative Divisions feature service, `"GB"` uses GeoBoundaries.

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
  - `fields`: List of fields to return.
  - `start_date`: Optional start date (format: 'YYYY-MM-DD')
  - `end_date`: Optional end date (format: 'YYYY-MM-DD')
  - `geometry`: Optional "polygon" or "point" to include H3 geometries
  - `verbose`: Optional boolean to display progress messages

---

### `get_timeseries_by_hexids(hex_ids, fields, start_date=None, end_date=None, geometry=None)`
Gets timeseries data for specific H3 hexagon IDs.
- **Parameters:**
  - `hex_ids`: List of H3 hexagon IDs to query
  - `fields`: List of field names to retrieve
  - `start_date`: Optional start date (format: 'YYYY-MM-DD')
  - `end_date`: Optional end date (format: 'YYYY-MM-DD')
  - `geometry`: Optional "polygon" or "point" to include H3 geometries
  - `verbose`: Optional boolean to display progress messages

---

## ADM2 Summaries

Access pre-computed administrative level 2 (ADM2) summaries from the World Bank Development Data Hub.

### `get_adm2_dataset_info()`
Returns information about available ADM2 datasets.
- **Returns:** DataFrame with dataset names, resource IDs, descriptions, and URLs

---

### `get_adm2_summaries(dataset, iso3_filter=None)`
Retrieves ADM2-level summary statistics from the World Bank DDH API.
- **Parameters:**
  - `dataset`: Dataset to retrieve. Options:
    - `"urbanization"`: Urban and rural settlement data (GHS settlement model)
    - `"nighttimelights"`: Nighttime lights intensity data (satellite-derived luminosity)
    - `"population"`: Population statistics (demographic data)
    - `"flood_exposure"`: Flood exposure risk data (flood hazard and exposure metrics)
  - `iso3_filter`: Optional ISO3 country code to filter by (e.g., 'USA', 'BRA', 'IND')
  - `verbose`: Optional boolean to display progress messages
- **Returns:** DataFrame containing ADM2-level statistics records

## Interactive Widgets

> **Note:** Widgets are optional components that require additional dependencies. Install with `pip install space2stats-client[widgets]`

Space2Stats provides interactive widgets that make it easy to explore, select data fields, and define areas of interest in Jupyter notebooks.


### AOISelector

This widget provides an interactive map interface for selecting Areas of Interest (AOI) by drawing polygons or rectangles directly on the map. The AOI is automatically captured in a GeoDataFrame that can be used directly with Space2Stats API calls.

```python
from space2stats_client.widgets.aoi_selector import AOISelector
from space2stats_client import Space2StatsClient

# Create and display the AOI selector
aoi_selector = AOISelector(center=(27.0, 29.7), zoom=6)
aoi_selector.display()

# After drawing on the map, access the AOI data
if aoi_selector.aoi:
    print(f"AOI selected with {len(aoi_selector.aoi.gdf)} polygon(s)")
    print(aoi_selector.aoi.gdf)
    
    # Use the AOI directly in API calls
    client = Space2StatsClient()
    summary = client.get_summary(
        gdf=aoi_selector.aoi.gdf,
        spatial_join_method="centroid",
        fields=["population", "gdp"]
    )

# Programmatically clear the AOI if needed
aoi_selector.clear_aoi()
```

**Key Features:**
- Interactive map with drawing tools (polygon and rectangle)
- Real-time feedback on selected area with approximate area calculation
- Support for multiple polygons in a single AOI
- Clear button to reset the selection
- Collapsible instructions panel
- Automatic population of GeoDataFrame for immediate use
- Consistent API with other widgets using `.display()` method

**Area Calculation:**
The approximate area is calculated by projecting the geometry from WGS84 (EPSG:4326) to Web Mercator (EPSG:3857) and computing the planar area. This provides a quick estimation but has inherent accuracy limitations:
- **Equatorial regions**: Generally accurate within 5-10%
- **Mid-latitudes (30°-60°)**: May have 10-40% error
- **High latitudes (>60°)**: May have >50% error

For precise area calculations, consider using appropriate local coordinate reference systems or geodesic calculations.

### CrossSectionFieldSelector

This widget helps users interactively select fields from the Space2Stats API for cross-sectional data. Fields are organized by their source STAC items for easier navigation.

```python
from space2stats_client.widgets.cross_section_field_selector import CrossSectionFieldSelector
from space2stats_client import Space2StatsClient
import geopandas as gpd

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
from space2stats_client.widgets.time_series_field_selector import TimeSeriesFieldSelector
from space2stats_client import Space2StatsClient
import geopandas as gpd

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

# Get ADM2 summaries for a specific country
adm2_pop = client.get_adm2_summaries(
    dataset="population",
    iso3_filter="USA"  # Optional country filter
)

# Get all available ADM2 datasets info
adm2_info = client.get_adm2_dataset_info()
print(adm2_info)
```
## Documentation

For full documentation, visit [Space2Stats Documentation](https://worldbank.github.io/DECAT_Space2Stats/).

## License

This project is licensed under the World Bank Master Community License Agreement. See the LICENSE file for details.

## Disclaimer

The World Bank makes no warranties regarding the accuracy, reliability, or completeness of the results and content. The World Bank disclaims any liability arising from the use of this software.
