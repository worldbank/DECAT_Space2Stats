# Space2Stats

Consistent, comparable, authoritative data describing sub-national variation is a constant point of complication for World Bank teams, our development partners, and client countries when assessing and investigating economic issues and national policy. This project will focus on creating and disseminating such data through aggregation of geospatial information at standard administrative divisions, and through the attribution of household survey data with foundational geospatial variables. 

## Getting Started Locally

- Setup the database: 
```
docker-compose up -d
```

- Create a `db.env` file:
```.env
DB_HOST=localhost
DB_PORT=5439
DB_NAME=postgis
DB_USER=username
DB_PASSWORD=password
DB_TABLE_NAME=space2stats
```

- Load our dataset into the database
```
./postgres/download_parquet.sh
python postgres/chunk_parquet.py
./postgres/load_parquet_chunks.sh
```

> You can get started with a subset of data for NYC with `./load_nyc_sample.sh` which requires changing your `db.env` value for `DB_TABLE_NAME` to `space2stats_nyc_sample`.

- Access your data using the Space2statS API! See the [example notebook](notebooks/space2stats_api_demo.ipynb).



