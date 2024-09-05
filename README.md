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



