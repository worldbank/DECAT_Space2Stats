# Space2Stats Python Client

A Python client for accessing the Space2Stats API, providing easy access to consistent, comparable, and authoritative sub-national variation data from the World Bank.


## API Methods

### `get_topics()`
Returns a DataFrame containing available dataset themes/topics from the STAC catalog.

### `get_fields()`
Returns a list of all available fields that can be used with the API.

### `get_properties(item_id: str)`
Returns a DataFrame with descriptions of variables for a specific dataset.

### `fetch_admin_boundaries(iso3: str, adm: str)`
Fetches administrative boundaries from GeoBoundaries API for a given country and admin level.

### `get_summary(gdf, spatial_join_method, fields, geometry=None)`
Extracts H3 level data for areas of interest.
- `gdf`: GeoDataFrame containing areas of interest
- `spatial_join_method`: "touches", "centroid", or "within"
- `fields`: List of field names to retrieve
- `geometry`: Optional "polygon" or "point" to include H3 geometries

### `get_aggregate(gdf, spatial_join_method, fields, aggregation_type)`
Extracts summary statistics from H3 data.
- `gdf`: GeoDataFrame containing areas of interest
- `spatial_join_method`: "touches", "centroid", or "within"
- `fields`: List of field names to retrieve
- `aggregation_type`: "sum", "avg", "count", "max", or "min"


## Quick Start

```bash
cd space2stats_client
pip install e .
```

```python
from space2stats import Space2StatsClient
import geopandas as gpd

# Initialize the client
client = Space2StatsClient()

# Get available topics/datasets
topics = client.get_topics()
print(topics)

# Get fields for a specific dataset
fields = client.get_fields("dataset_id")
print(fields)

# Get data for an area of interest
gdf = gpd.read_file("path/to/your/area.geojson")
summary = client.get_summary(
    gdf=gdf,
    spatial_join_method="centroid",
    fields=["population", "gdp"]
)

# Get aggregated statistics
aggregates = client.get_aggregate(
    gdf=gdf,
    spatial_join_method="centroid",
    fields=["population", "gdp"],
    aggregation_type="sum"
)
```

## Documentation

For full documentation, visit [Space2Stats Documentation](https://worldbank.github.io/DECAT_Space2Stats/).

## License

This project is licensed under the World Bank Master Community License Agreement. See the LICENSE file for details.

## Disclaimer

The World Bank makes no warranties regarding the accuracy, reliability or completeness of the results and content. The World Bank disclaims any liability arising from the use of this software. 