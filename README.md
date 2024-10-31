# Space2Stats

Consistent, comparable, authoritative data describing sub-national variation is a constant point of complication for World Bank teams, our development partners, and client countries when assessing and investigating economic issues and national policy. This project will focus on creating and disseminating such data through aggregation of geospatial information at standard administrative divisions, and through the attribution of household survey data with foundational geospatial variables.

## Getting Started Locally

### Installation

The module can be installed via `pip` directly from Github:

```
pip install "git+https://github.com/worldbank/DECAT_Space2Stats.git#subdirectory=space2stats_api/src"
```

### Setup

- Setup the database:

    ```
    docker-compose up -d
    ```

- Create a `db.env` file:

    ```.env
    PGHOST=localhost
    PGPORT=5439
    PGDATABASE=postgis
    PGUSER=username
    PGPASSWORD=password
    PGTABLENAME=space2stats
    ```

- Ingest the dataset into the database:

    Use the space2stats-ingest CLI to download the Parquet file from S3 and load it into your database. You’ll also need the STAC metadata file to validate the Parquet schema during ingestion.

    To download the Parquet file from S3 and load it into the database:
    ```
    poetry run space2stats-ingest download-and-load \
    "s3://<bucket>/space2stats.parquet" \
    "postgresql://username:password@localhost:5439/postgis" \
    "<path>/space2stats.json" \
    --parquet-file "local.parquet"
    ```

Alternatively, you can download the Parquet file and load it into the database separately:

- Download the Parquet file:

    ```
    poetry run space2stats-ingest download \
        "s3://<bucket>/space2stats.parquet" \
        --local-path "local.parquet"
    ```

- Load the Parquet file into the database:

    ```
    poetry run space2stats-ingest load \
    "postgresql://username:password@localhost:5439/postgis" \
    "<path>/space2stats.json" \
    --parquet-file "local.parquet"
    ```

- Finally, access your data using the Space2stats API! See the [example notebook](notebooks/space2stats_api_demo.ipynb).

## Usage

### API

The API server can be run locally with the following command:

```
python -m space2stats
```

Once running, interactive API documentation can be found at the `/docs` endpoint (e.g. http://localhost:8000/docs).

API server properties can be customized via the following environment variables:

- `UVICORN_HOST`: Server host. _Default: `127.0.0.1`_
- `UVICORN_PORT`: Server port. _Default: `8000`_

### Module

The `space2stats` module surfaces the `StatsTable` class to enable direct DB queries (ie without using the API server).

```py
from space2stats import StatsTable

with StatsTable.connect() as stats_table:
    ...
```

Connection parameters may be explicitely provided. Otherwise, connection parameters will expected to be available via standard [PostgreSQL Environment Variables](https://www.postgresql.org/docs/current/libpq-envars.html#LIBPQ-ENVARS).

```py
from space2stats import StatsTable

with StatsTable.connect(
    PGHOST="localhost",
    PGPORT="5432",
    PGUSER="postgres",
    PGPASSWORD="changeme",
    PGDATABASE="postgis",
    PGTABLENAME="space2stats",
) as stats_table:
    ...

# alternatively:
# settings = Settings(
#     PGHOST="localhost",
#     PGPORT="5432",
#     PGUSER="postgres",
#     PGPASSWORD="changeme",
#     PGDATABASE="postgis",
#     PGTABLENAME="space2stats",
# )
# with StatsTable.connect(settings):
#     ...
```

#### `StatsTable.summaries()`

```python
StatsTable.summaries(
    aoi: AoiModel,
    spatial_join_method: Literal["touches", "centroid", "within"],
    fields: List[str],
    geometry: Optional[Literal["polygon", "point"]] = None,
) -> List[Dict]
```

##### Description

Retrieves statistical summaries for the specified Area of Interest (AOI) from a PostgreSQL table.

This method is used to obtain aggregated statistics from hexagonal H3 cells that intersect with the AOI's geometry, using a chosen spatial join method.

##### Parameters

- **aoi** (`AoiModel`): The Area of Interest, either as a `Feature` or an instance of `AoiModel`.
- **spatial_join_method** (`Literal["touches", "centroid", "within"]`): The method to use for performing the spatial join between the AOI and H3 cells.
  - `"touches"`: Includes H3 cells that touch the AOI.
  - `"centroid"`: Includes H3 cells where the centroid falls within the AOI.
  - `"within"`: Includes H3 cells entirely within the AOI.
- **fields** (`List[str]`): A list of field names to retrieve from the statistics table.
- **geometry** (`Optional[Literal["polygon", "point"]]`): Specifies if the H3 geometries should be included in the response. It can be either `"polygon"` or `"point"`. If `None`, geometries are not included.

##### Returns

- **`List[Dict]`**: A list of dictionaries containing statistical summaries for each H3 cell. Each dictionary contains:
  - `"hex_id"`: The H3 cell identifier.
  - `"geometry"` (optional): The geometry of the H3 cell, if `geometry` is specified.
  - Other fields from the statistics table, based on the specified `fields`.

##### Example

```python
aoi = AoiModel(type="Feature", geometry=...)
with StatsTable.connect() as stats_table:
    summaries = stats_table.summaries(
        aoi=aoi,
        spatial_join_method="centroid",
        fields=["population", "average_income"],
        geometry="polygon"
    )
```

#### `fields` Method

```python
StatsTable.fields() -> List[str]
```

##### Description

Retrieves the list of column names from the PostgreSQL statistics table, excluding the `"hex_id"` column.

This method is helpful for understanding the available fields that can be queried for statistical summaries.

##### Returns

- **`List[str]`**: A list of column names from the statistics table, excluding `"hex_id"`.

##### Example

```python
with StatsTable.connect() as stats_table:
    columns = stats_table.fields()
    print(columns)  # Output: ['population', 'average_income', ...]
```