# Space2Stats

Consistent, comparable, authoritative data describing sub-national variation is a constant point of complication for World Bank teams, our development partners, and client countries when assessing and investigating economic issues and national policy. This project will focus on creating and disseminating such data through aggregation of geospatial information at standard administrative divisions, and through the attribution of household survey data with foundational geospatial variables.

## Getting Started Locally

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

- Load our dataset into the database

```
./postgres/download_parquet.sh
python postgres/chunk_parquet.py
./postgres/load_parquet_chunks.sh
```

> You can get started with a subset of data for NYC with `./load_nyc_sample.sh` which requires changing your `db.env` value for `PGTABLENAME` to `space2stats_nyc_sample`.

- Access your data using the Space2statS API! See the [example notebook](notebooks/space2stats_api_demo.ipynb).

## Usage as a module

The module can be installed via `pip` directly from Github:

```
pip install "git+https://github.com/worldbank/DECAT_Space2Stats.git#subdirectory=space2stats_api/src
```

It can then be used within Python as such:

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
