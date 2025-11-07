# Space2Stats

Consistent, comparable, authoritative data describing sub-national variation is a constant point of complication for World Bank teams, our development partners, and client countries when assessing and investigating economic issues and national policy. This project will focus on creating and disseminating such data through aggregation of geospatial information at standard administrative divisions, and through the attribution of household survey data with foundational geospatial variables.

## Getting Started Locally

### Installation

The module can be installed via `pip` directly from Github:

```
pip install "git+https://github.com/worldbank/DECAT_Space2Stats.git#subdirectory=space2stats_api/src"
```

### Setup Local DB

- See `dev-db/README.md`

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

## Documentation

Our documentation is hosted at https://worldbank.github.io/DECAT_Space2Stats/, and is built using Jupyter Book and Sphinx. 

All configuration, markdown, and notebook files relevant for this are stored under `/docs`. 

### Environment Setup

To install the package and docs dependencies:

```bash
conda create -n book python=3.11
conda activate book
pip install poetry
cd space2stats_api/src
poetry install --with docs # Installs space2stats, dependencies and jupyter-book
pip install folium matplotlib mapclassify # for additional notebook visualizations
```

To build the documentation locally, run the following command from the **repository root** (not inside `space2stats_api/src`). This is important because the `docs/` folder lives at the top level.

```bash
sphinx-build docs _build/html -b html
```

After the build completes, open `_build/html/index.html` in your browser. This renders the same Jupyter Book site that is deployed to GitHub Pages.

To remove the generated files:

```bash
jupyter-book clean .
```

### Contributing

A Github Action is used to automatically build and deploy the documentation to Github Pages. To contribute to the documentation, follow the steps below:

1. Create a new branch from the latest `main`.

    ```bash
    git checkout -b new_docs
    ```

2. Make changes to the documentation, e.g: markdown files, and table of contents (`docs/_toc.yml`).
3. Build the documentation locally to ensure it renders correctly.
4. Commit, push the changes to your branch and create a pull request.

    ```bash
    git add .
    git commit -m "Update documentation"
    git push origin new_docs
    ```

The site will be updated automatically once the branch is merged to main.

Note that the sphinx build command uses the conf.py file. If we need to make changes to _conf.yml, then rebuild the conf.py file by running:

```bash
jupyter-book config sphinx docs
```
