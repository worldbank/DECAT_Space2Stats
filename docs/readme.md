# Space2Stats
The Space2Stats program is designed to provide academics, statisticians, and data scientists with easier access to regularly requested geospatial aggregate data. The primary deliverable is a database of geospatial aggregates at two official scales:
1. Official World Bank boundaries at admin level 2  
2. A global database of h3 hexagons at level 6 (~36km2)  

The Space2Stats program is funded by the World Bank's [Global Data Facility](https://www.worldbank.org/en/programs/global-data-facility), which is a World-Bank hosted funding instrument for the world's most critical data impact opportunities.

```{image} images/GDF_logo.png
---
alt: GDF
---
```

## Core Datasets
The World Bank's GOST team is responsible for curating key geographic datasets to populate the Space2Stats database. Raw geographic data is typically available in raster format and has been processed to a consistent spatial grid (h3) using zonal statistics.

The database currently contains four datasets with global coverage:

::::{grid} 1 2 2 2
:class-container: landing-grid
:gutter: 2

:::{grid-item-card}
:link: https://radiantearth.github.io/stac-browser/#/external/raw.githubusercontent.com/worldbank/DECAT_Space2Stats/refs/heads/main/space2stats_api/src/space2stats_ingest/METADATA/stac/space2stats-collection/space2stats_population_2020/space2stats_population_2020.json

Population Demographics
^^^
Total Population, 2020, disaggregated by age and gender. (WorldPop)
:::

:::{grid-item-card}
:link: https://radiantearth.github.io/stac-browser/#/external/raw.githubusercontent.com/worldbank/DECAT_Space2Stats/refs/heads/main/space2stats_api/src/space2stats_ingest/METADATA/stac/space2stats-collection/urbanization_ghssmod/urbanization_ghssmod.json

Degree of Urbanization
^^^
Population and number of cells in different rural/urban classes. (GHSSMOD)
:::

:::{grid-item-card}
:link: https://radiantearth.github.io/stac-browser/#/external/raw.githubusercontent.com/worldbank/DECAT_Space2Stats/refs/heads/main/space2stats_api/src/space2stats_ingest/METADATA/stac/space2stats-collection/nighttime_lights/nighttime_lights.json

Nighttime Lights
^^^
Sum of luminosity values measured by annual composites from the VIIRS satellite. (World Bank, Light Every Night)
:::

:::{grid-item-card}
:link: https://radiantearth.github.io/stac-browser/#/external/raw.githubusercontent.com/worldbank/DECAT_Space2Stats/refs/heads/main/space2stats_api/src/space2stats_ingest/METADATA/stac/space2stats-collection/flood_exposure_15cm_1in100/flood_exposure_15cm_1in100.json

Flood Exposure
^^^
Population exposed to floods greater than 15 cm, 1-in-100 return period. (Fathom v3 and WorldPop)
:::

:::{grid-item-card}
:link: https://radiantearth.github.io/stac-browser/#/external/raw.githubusercontent.com/worldbank/DECAT_Space2Stats/refs/heads/main/space2stats_api/src/space2stats_ingest/METADATA/stac/space2stats-collection/builtarea_ghsl/builtarea_ghsl.json

Built Area
^^^
Built area (in m2) in 5-year epochs. (GHSL)
:::

:::{grid-item-card}
:link: https://radiantearth.github.io/stac-browser/#/external/raw.githubusercontent.com/worldbank/DECAT_Space2Stats/refs/heads/main/space2stats_api/src/space2stats_ingest/METADATA/stac/space2stats-collection/climate/climate.json

Climate Time Series
^^^
Index measuring drought severity based on precipitation anomalies (SPI)
:::
::::

Our [STAC Metadata](https://tinyurl.com/s2s-stac) contains key information about each data source and the variables available in the database. Additional datasets under consideration are listed in our [Annex](./annexA.md).

## API

Space2Stats data is publicly available through an API built and hosted by [Development Seed](https://developmentseed.org/). The API supports querying the space2stats data by location. The API is a FastAPI application that accesses the underlying PostgreSQL database. Documentation for the API endpoints and expected parameters can be found at https://space2stats.ds.io/docs.

Below are some examples on how to use the API endpoints using Python or R. The base URL is always `https://space2stats.ds.io`.

### `/fields`

Returns a list of all fields in the database.

`````{tab-set}
````{tab-item} python

```{code-block} python
import requests

BASE_URL = "https://space2stats.ds.io"
FIELDS_ENDPOINT = f"{BASE_URL}/fields"

response = requests.get(FIELDS_ENDPOINT)
if response.status_code != 200:
    raise Exception(f"Failed to get fields: {response.text}")

available_fields = response.json()
print("Available Fields:", available_fields)
```
````

````{tab-item} r

```{code-block} r
library(httr2)
base_url <- "https://space2stats.ds.io"

# Set up the request to fetch available fields
req <- request(base_url) |>
  req_url_path_append("fields")  # Append the correct endpoint

# Perform the request and get the response
resp <- req |> req_perform()

# Check the status code
if (resp_status(resp) != 200) {
  stop("Failed to get fields: ", resp_body_string(resp))
}

# Parse the response body as JSON
available_fields <- resp |> resp_body_json()

# Print the available fields in a simplified format
print("Available Fields:")
print(unlist(available_fields))
```
````
`````

Sample response:
```{code-block}
Available Fields: ['sum_pop_2020', 'ogc_fid', 'sum_pop_f_0_2020', 'sum_pop_f_10_2020', 'sum_pop_f_15_2020', 'sum_pop_f_1_2020', 'sum_pop_f_20_2020', 'sum_pop_f_25_2020', 'sum_pop_f_30_2020', 'sum_pop_f_35_2020', 'sum_pop_f_40_2020', 'sum_pop_f_45_2020', 'sum_pop_f_50_2020', 'sum_pop_f_55_2020', 'sum_pop_f_5_2020', 'sum_pop_f_60_2020', 'sum_pop_f_65_2020', 'sum_pop_f_70_2020', 'sum_pop_f_75_2020', 'sum_pop_f_80_2020', 'sum_pop_m_0_2020', 'sum_pop_m_10_2020', 'sum_pop_m_15_2020', 'sum_pop_m_1_2020', 'sum_pop_m_20_2020', 'sum_pop_m_25_2020', 'sum_pop_m_30_2020', 'sum_pop_m_35_2020', 'sum_pop_m_40_2020', 'sum_pop_m_45_2020', 'sum_pop_m_50_2020', 'sum_pop_m_55_2020', 'sum_pop_m_5_2020', 'sum_pop_m_60_2020', 'sum_pop_m_65_2020', 'sum_pop_m_70_2020', 'sum_pop_m_75_2020', 'sum_pop_m_80_2020', 'sum_pop_m_2020', 'sum_pop_f_2020']
```

### `/summary`

The summary endpoint returns data at the h3 level for a specified area of interest (AOI). A summary json can be retrieved with the following parameters included as part of the request body:

- **aoi**: The Area of Interest, either as a `Feature` or an instance of `AoiModel`.
- **spatial_join_method** (`Literal["touches", "centroid", "within"]`): The method to use for performing the spatial join between the AOI and H3 cells.
  - `"touches"`: Includes H3 cells that touch the AOI.
  - `"centroid"`: Includes H3 cells where the centroid falls within the AOI.
  - `"within"`: Includes H3 cells entirely within the AOI.
- **fields** (`List[str]`): A list of field names to retrieve from the statistics table.
- **geometry** (`Optional[Literal["polygon", "point"]]`): Specifies if the H3 geometries should be included in the response. It can be either `"polygon"` or `"point"`. If `None`, geometries are not included.

`````{tab-set}
````{tab-item} python

```{code-block} python
import requests
import pandas as pd

BASE_URL = "https://space2stats.ds.io"
SUMMARY_ENDPOINT = f"{BASE_URL}/summary"

# Bounding box around Kenya
aoi = {
    "type": "Feature",
    "geometry": {
        "type": "Polygon",
        "coordinates": [
            [
                [33.78593974945852, 5.115816884114494],
                [33.78593974945852, -4.725410543134203],
                [41.94362577283266, -4.725410543134203],
                [41.94362577283266, 5.115816884114494],
                [33.78593974945852, 5.115816884114494],
            ]
        ],
    },
    "properties": {"name": "Updated AOI"},
}

# Define the Request Payload
request_payload = {
    "aoi": aoi,
    "spatial_join_method": "touches",
    "fields": ["sum_pop_2020"],
    "geometry": "polygon",
}

# Get Summary Data
response = requests.post(SUMMARY_ENDPOINT, json=request_payload)
if response.status_code != 200:
    raise Exception(f"Failed to get summary: {response.text}")

summary_data = response.json()
df = pd.DataFrame(summary_data)

df.head()
```
````

````{tab-item} r

```{code-block} r
library(httr2)
library(jsonlite)

base_url <- "https://space2stats.ds.io"

# Bounding box around Kenya
aoi <- list(
  type = "Feature",
  properties = NULL,  # Empty properties
  geometry = list(
    type = "Polygon",
    coordinates = list(
      list(
        c(33.78593974945852, 5.115816884114494),
        c(33.78593974945852, -4.725410543134203),
        c(41.94362577283266, -4.725410543134203),
        c(41.94362577283266, 5.115816884114494),
        c(33.78593974945852, 5.115816884114494)
      )
    )
  )
)

request_payload <- list(
  aoi = aoi,
  spatial_join_method = "centroid",
  fields = list("sum_pop_2020"),
  geometry = "polygon"
)

# Set up the base URL and create the request
req <- request(base_url) |>
  req_url_path_append("summary") |>
  req_body_json(request_payload)

# Perform the request and get the response
resp <- req |> req_perform()

# Turn response into a data frame
summary_data <- resp |> resp_body_string() |> fromJSON(flatten = TRUE)

head(summary_data)
```
````
`````

The expected response is a JSON containing the hexagon ID and the requested fields:

```text
            hex_id                                           geometry  \
0  866a4a00fffffff  {"type":"Polygon","coordinates":[[[36.20299996...   
1  866a4a017ffffff  {"type":"Polygon","coordinates":[[[36.10071731...   
2  866a4a01fffffff  {"type":"Polygon","coordinates":[[[36.15684403...   
3  866a4a047ffffff  {"type":"Polygon","coordinates":[[[36.30522474...   
4  866a4a04fffffff  {"type":"Polygon","coordinates":[[[36.36131294...   

   sum_pop_2020  
0    476.538185  
1    676.912804  
2    347.182722  
3    380.988678  
4    285.943490  
```

### `/aggregate`

The aggregate endpoint is very similar to the summary endpoint, but it returns an aggregate statistic for the entire area, based on an additional `aggregation type` function ('sum', 'avg', 'count', 'max' or 'min'). The request body is the same as the summary endpoint, with the addition of the `aggregation_type` field. 

This example uses an admin-1 province boundary from GeoBoundaries, retrieved as a `geopandas geodataframe` or `simple feature` (r).

`````{tab-set}
````{tab-item} python

```{code-block} python
import requests
import geopandas as gpd

BASE_URL = "https://space2stats.ds.io"
AGGREGATION_ENDPOINT = f"{BASE_URL}/aggregate"

def fetch_admin_boundaries(iso3: str, adm: str) -> gpd.GeoDataFrame:
    """Fetch administrative boundaries from GeoBoundaries API."""
    url = f"https://www.geoboundaries.org/api/current/gbOpen/{iso3}/{adm}/"
    res = requests.get(url).json()
    return gpd.read_file(res["gjDownloadURL"])

ISO3 = "KEN"
ADM = "ADM1"
adm_boundaries = fetch_admin_boundaries(ISO3, ADM)
row = adm_boundaries.iloc[0]

request_payload = {
    "aoi": {
        "type": "Feature",
        "geometry": row.geometry.__geo_interface__,
        "properties": {},
    },
    "spatial_join_method": "touches",
    "fields": ["sum_pop_2020"],
    "aggregation_type": "sum",
}

response = requests.post(AGGREGATION_ENDPOINT, json=request_payload)

if response.status_code == 200:
    result = response.json()
    print(result)
else:
    print(response.content)
```
````

````{tab-item} r

```{code-block} r
library(httr2)
library(sf)
library(jsonlite)
library(geojsonsf)

base_url <- "https://space2stats.ds.io"

fetch_admin_boundaries <- function(iso3, adm) {
  # Fetch administrative boundaries from GeoBoundaries API
  url <- sprintf("https://www.geoboundaries.org/api/current/gbOpen/%s/%s/", iso3, adm)
  
  response <- request(url) %>%
    req_perform() %>%
    resp_body_json()
  
  sf::read_sf(response$gjDownloadURL)
}

ISO3 <- "KEN"
ADM <- "ADM1"
adm_boundaries <- fetch_admin_boundaries(ISO3, ADM)

# Select the first row from the adm_boundaries
row <- adm_boundaries[1, ]

sf_geoj <- sf_geojson(row, atomise=T)
geojson_list <- fromJSON(sf_geoj[1])

# Create the request payload
request_payload <- list(
  aoi = geojson_list,
  spatial_join_method = "touches",
  fields = list("sum_pop_2020"),
  aggregation_type = "sum"
)

# Set up the base URL and create the request
req <- request(base_url) |>
  req_url_path_append("aggregate") |>
  req_body_json(request_payload)

# Perform the request and get the response
resp <- req |> req_perform()

# Turn response into a data frame
aggregate_data <- resp |> resp_body_string() |> fromJSON(flatten = TRUE)

print(aggregate_data)
```
````
`````

The expected response is a JSON containing the requested aggregate statistic for the area:

```text
{'sum_pop_2020': 1374175.833772784}
```

### `/summary_by_hexids`

The summary endpoint returns data at the h3 level for a specified h3 hexagon (level 6).

- **hex_ids**: List of H3 hexagon IDs to query (`List[str]`).
- **fields** (`List[str]`): A list of field names to retrieve from the statistics table.
- **geometry** (`Optional[Literal["polygon", "point"]]`): Specifies if the H3 geometries should be included in the response. It can be either `"polygon"` or `"point"`. If `None`, geometries are not included.

`````{tab-set}
````{tab-item} python

```{code-block} python
import requests
import pandas as pd

BASE_URL = "https://space2stats.ds.io"
SUMMARY_ENDPOINT = f"{BASE_URL}/summary_by_hexids"

hex_ids = [
    "866a4a00fffffff",
    "866a4a017ffffff",
    "866a4a01fffffff",
    "866a4a047ffffff",
    "866a4a04fffffff",
]

# Define the Request Payload
request_payload = {
    "hex_ids": hex_ids,
    "fields": ["sum_pop_2020"],
}

# Get Summary Data
response = requests.post(SUMMARY_ENDPOINT, json=request_payload)
if response.status_code != 200:
    raise Exception(f"Failed to get summary: {response.text}")

summary_data = response.json()
df = pd.DataFrame(summary_data)
df
```
````

````{tab-item} r

```{code-block} r
library(httr2)
library(jsonlite)

base_url <- "https://space2stats.ds.io"

hex_ids = c(
  "866a4a00fffffff",
  "866a4a017ffffff",
  "866a4a01fffffff",
  "866a4a047ffffff",
  "866a4a04fffffff"
)

request_payload <- list(
  hex_ids = hex_ids,
  fields = list("sum_pop_2020"),
)

# Set up the base URL and create the request
req <- request(base_url) |>
  req_url_path_append("summary_by_hexids") |>
  req_body_json(request_payload)

# Perform the request and get the response
resp <- req |> req_perform()

# Turn response into a data frame
summary_data <- resp |> resp_body_string() |> fromJSON(flatten = TRUE)

head(summary_data)
```
````
`````

The expected response is a JSON containing the hexagon ID and the requested fields:

```text
	hex_id	sum_pop_2020
0	866a4a00fffffff	476.538185
1	866a4a017ffffff	676.912804
2	866a4a01fffffff	347.182722
3	866a4a047ffffff	380.988678
4	866a4a04fffffff	285.943490
```

### `/aggregate_by_hexids`

The aggregate endpoint is very similar to the summary endpoint, but it returns an aggregate statistic for all of the h3 cells provided, based on an additional `aggregation type` function ('sum', 'avg', 'count', 'max' or 'min').

`````{tab-set}
````{tab-item} python

```{code-block} python
import requests

BASE_URL = "https://space2stats.ds.io"
AGGREGATION_ENDPOINT = f"{BASE_URL}/aggregate_by_hexids"

hex_ids = [
    "866a4a00fffffff",
    "866a4a017ffffff",
    "866a4a01fffffff",
    "866a4a047ffffff",
    "866a4a04fffffff",
]

request_payload = {
    "hex_ids": hex_ids,
    "fields": ["sum_pop_2020"],
    "aggregation_type": "sum",
}

response = requests.post(AGGREGATION_ENDPOINT, json=request_payload)

if response.status_code == 200:
    result = response.json()
    print(result)
else:
    print(response.content)
```
````

````{tab-item} r

```{code-block} r
library(httr2)
library(sf)
library(jsonlite)
library(geojsonsf)

base_url <- "https://space2stats.ds.io"

hex_ids <- c(
  "866a4a00fffffff",
  "866a4a017ffffff",
  "866a4a01fffffff",
  "866a4a047ffffff",
  "866a4a04fffffff"
)

# Create the request payload
request_payload <- list(
  hex_ids = hex_ids,
  fields = list("sum_pop_2020"),
  aggregation_type = "sum"
)

# Set up the base URL and create the request
req <- request(base_url) |>
  req_url_path_append("aggregate_by_hexids") |>
  req_body_json(request_payload)

# Perform the request and get the response
resp <- req |> req_perform()

# Turn response into a data frame
aggregate_data <- resp |> resp_body_string() |> fromJSON(flatten = TRUE)

print(aggregate_data)
```
````
`````

The expected response is a JSON containing the requested aggregate statistic for the area:

```text
{'sum_pop_2020': 2167.565878674388}
```

### `/timeseries`

The timeseries endpoint provides access to monthly data hosted in a separate [time-enabled table](https://radiantearth.github.io/stac-browser/#/external/raw.githubusercontent.com/worldbank/DECAT_Space2Stats/refs/heads/main/space2stats_api/src/space2stats_ingest/METADATA/stac/space2stats-collection/climate/climate.json). A summary json can be retrieved with the following parameters included as part of the request body. Note that you can only request fields from this table, and not from the other datasets.

- **aoi**: The Area of Interest, either as a `Feature` or an instance of `AoiModel`.
- **spatial_join_method** (`Literal["touches", "centroid", "within"]`): The method to use for performing the spatial join between the AOI and H3 cells.
  - `"touches"`: Includes H3 cells that touch the AOI.
  - `"centroid"`: Includes H3 cells where the centroid falls within the AOI.
  - `"within"`: Includes H3 cells entirely within the AOI.
- **start_date** (`str`): The start date for the time series data in ISO format (YYYY-MM-DD). (If not provided, defaults to the earliest date in the database.)
- **end_date** (`str`): The end date for the time series data in ISO format (YYYY-MM-DD). (If not provided, defaults to the latest date in the database.)
- **fields** (`List[str]`): A list of field names to retrieve from the statistics table.
- **geometry** (`Optional[Literal["polygon", "point"]]`): Specifies if the H3 geometries should be included in the response. It can be either `"polygon"` or `"point"`. If `None`, geometries are not included.

`````{tab-set}
````{tab-item} python

```{code-block} python
import requests
import pandas as pd

BASE_URL = "https://space2stats.ds.io"
TIMESERIES_ENDPOINT = f"{BASE_URL}/timeseries"

# Bounding box around Kenya
aoi = {
    "type": "Feature",
    "geometry": {
        "type": "Polygon",
        "coordinates": [
            [
                [33.78593974945852, 5.115816884114494],
                [33.78593974945852, -4.725410543134203],
                [41.94362577283266, -4.725410543134203],
                [41.94362577283266, 5.115816884114494],
                [33.78593974945852, 5.115816884114494],
            ]
        ],
    },
    "properties": {"name": "Updated AOI"},
}

# Define the Request Payload
request_payload = {
    "aoi": aoi,
    "spatial_join_method": "touches",
    "start_date": "2020-01-01",
    "end_date": "2020-12-31",
    "fields": ["spi"]
}

# Get Time Series Data
response = requests.post(TIMESERIES_ENDPOINT, json=request_payload)
if response.status_code != 200:
    raise Exception(f"Failed to get timeseries: {response.text}")

timeseries_data = response.json()
df = pd.DataFrame(timeseries_data)
```
````

````{tab-item} r

```{code-block} r
library(httr2)
library(jsonlite)

base_url <- "https://space2stats.ds.io"

# Bounding box around Kenya
aoi <- list(
  type = "Feature",
  properties = NULL,  # Empty properties
  geometry = list(
    type = "Polygon",
    coordinates = list(
      list(
        c(33.78593974945852, 5.115816884114494),
        c(33.78593974945852, -4.725410543134203),
        c(41.94362577283266, -4.725410543134203),
        c(41.94362577283266, 5.115816884114494),
        c(33.78593974945852, 5.115816884114494)
      )
    )
  )
)

request_payload <- list(
  aoi = aoi,
  spatial_join_method = "centroid",
  start_date = "2020-01-01",
  end_date = "2020-12-31",
  fields = list("spi")
)

# Set up the base URL and create the request
req <- request(base_url) |>
  req_url_path_append("timeseries") |>
  req_body_json(request_payload)

# Perform the request and get the response
resp <- req |> req_perform()

# Turn response into a data frame
timeseries_data <- resp |> resp_body_string() |> fromJSON(flatten = TRUE)

head(timeseries_data)
```
````
`````

### `/timeseries_by_hexids`

Access timeseries data for specified h3 hexagons (level 6).

`````{tab-set}
````{tab-item} python

```{code-block} python
import requests
import pandas as pd

BASE_URL = "https://space2stats.ds.io"
TIMESERIES_ENDPOINT = f"{BASE_URL}/timeseries_by_hexids"

hex_ids = [
    "866a4a00fffffff",
    "866a4a017ffffff",
    "866a4a01fffffff",
    "866a4a047ffffff",
    "866a4a04fffffff",
]

# Define the Request Payload
request_payload = {
    "hex_ids": hex_ids,
    "start_date": "2020-01-01",
    "end_date": "2020-12-31",
    "fields": ["spi"]
}

# Get Time Series Data
response = requests.post(TIMESERIES_ENDPOINT, json=request_payload)
if response.status_code != 200:
    raise Exception(f"Failed to get timeseries: {response.text}")

timeseries_data = response.json()
df = pd.DataFrame(timeseries_data)
```
````

````{tab-item} r

```{code-block} r
library(httr2)
library(jsonlite)

base_url <- "https://space2stats.ds.io"

hex_ids <- c(
  "866a4a00fffffff",
  "866a4a017ffffff",
  "866a4a01fffffff",
  "866a4a047ffffff",
  "866a4a04fffffff"
)

request_payload <- list(
  hex_ids = hex_ids,
  start_date = "2020-01-01",
  end_date = "2020-12-31",
  fields = list("spi")
)

# Set up the base URL and create the request
req <- request(base_url) |>
  req_url_path_append("timeseries_by_hexids") |>
  req_body_json(request_payload)

# Perform the request and get the response
resp <- req |> req_perform()

# Turn response into a data frame
timeseries_data <- resp |> resp_body_string() |> fromJSON(flatten = TRUE)

head(timeseries_data)
```
````
`````

## Notebook Examples

- [**API Demo (Python)**](user-docs/space2stats_api_demo.ipynb)
- [**API Demo (R)**](user-docs/space2stats_api_demo_R.md)
- [**Exploring Flood Exposure (Python Client)**](user-docs/space2stats_floods_with_client.ipynb)
- [**Exploring Time Series Data (Python Client)**](user-docs/space2stats_api_demo_spi_droughts.ipynb)

## Python Client

In addition to the API, the `Space2StatsClient` python package provides wrapper functions to retrieve metadata and easily access API endpoints (_fields_, _summaries_, and _aggregate_, _timeseries_). The package is available on [PyPI](https://pypi.org/project/space2stats-client/) and can be installed using pip:

```bash
pip install space2stats-client
```

Or, from source:

```bash
conda create -n s2s python=3.11
conda activate s2s
pip install poetry
cd space2stats_client/src
poetry install
```

Full documentation for the client is available [in this page](pythonclient.rst).