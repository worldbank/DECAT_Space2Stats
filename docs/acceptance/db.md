## Database Deliverable Acceptance Test

### Description of Deliverable

This deliverable includes the implementation of an ETL process, database schema design, raw data storage in Parquet format, infrastructure as code (IaC), database configuration, and comprehensive documentation.

The following acceptance test outlines the steps to verify that the deliverable meets the specified requirements.

### Input Data

The input data is stored in Parquet format on AWS S3, located in the file `space2stats_updated.parquet`. Any additional fields should be appended to this file. The structure of the Parquet file is as follows:

- `hex_id`
- `{variable_name}_{aggregation_method[sum, mean, etc.]}_{year}`

In addition to the Parquet file, a corresponding STAC metadata file is required to ensure that the data structure in the Parquet file matches the metadata specification. The STAC metadata file describes the columns present in the Parquet file and is used to perform schema validation before loading the data into the database.

### Database Setup

You can use a local database for this acceptance test by running:

```bash
docker-compose up
```

Alternatively, connect to a remote database, such as the production database used in [Tembo](reluctantly-simple-spoonbill.data-1.use1.tembo.io).

### Data Ingestion Using CLI

Before running the ingestion process, ensure the database environment variables are set in `db.env`:

```bash
PGHOST=localhost
PGPORT=5432
PGDATABASE=postgis
PGUSER=postgres
PGPASSWORD=password
PGTABLENAME=space2stats
```

> If using `docker-compose`, these configurations should be accurate for local use.

### CLI Usage:

You can use the CLI tool for data ingestion, which includes validation of the Parquet file against the STAC metadata file to ensure consistency between the data structure and the metadata.

#### Ingestion Process

The ingestion process now includes an additional parameter for specifying the STAC metadata file, which ensures that the Parquet file schema matches the metadata. This validation step ensures that there are no extra columns in either the Parquet file or the metadata, providing a 1:1 correspondence between them.

You can use the CLI tool for data ingestion. First, ensure you have the required dependencies installed via Poetry:

```bash
poetry install
```

To load a Parquet file it into the database, run the following command:

```bash
poetry run space2stats-ingest load \
    "postgresql://username:password@localhost:5439/postgres" \
    "<item_path>" \
    "local.parquet"
```

### Database Configuration

Once connected to the database via `psql` or a PostgreSQL client (e.g., `pgAdmin`), execute the following SQL command to create an index on the `space2stats` table:

```sql
CREATE INDEX idx_hex_id ON space2stats (hex_id);
```

This index improves query performance, especially when filtering data by `hex_id`.

### Testing the Database Table

You can perform sample queries to ensure the data is accessible and properly loaded. Here are some example queries:

```sql
SELECT * FROM space2stats LIMIT 100;

SELECT * FROM space2stats WHERE hex_id = '86beabd8fffffff';

SELECT sum_pop_2020 FROM space2stats WHERE hex_id IN ('86beabd8fffffff', '86beabdb7ffffff', '86beac01fffffff');
```

### Other Considerations

- **Chunked Loading**: The ingestion process uses chunked loading to improve performance, especially for larger datasets.
- **Progress Bar**: When running the CLI, the loading process displays a progress bar to provide visual feedback during the data ingestion.

### Conclusion

Ensure all steps are followed to verify the ETL process, database setup, and data ingestion pipeline. Reach out to the development team for any further assistance or troubleshooting.


#### Updating test

- Spin up database with docker:
```
docker-compose up
```
- Download initial dataset:
```
aws s3 cp s3://wbg-geography01/Space2Stats/parquet/GLOBAL/space2stats.parquet .
download: s3://wbg-geography01/Space2Stats/parquet/GLOBAL/space2stats.parquet to ./space2stats.parquet
```
- Upload initial dataset:
```
space2stats-ingest postgresql://username:password@localhost:5439/postgres ./space2stats_ingest/METADATA/stac/space2stats/space2stats_population_2020/space2stats_population_2020.json space2stats.parquet
```
- Generate second dataset:
```
python space2stats_ingest/METADATA/generate_test_data.py 
```
- Upload second dataset:
```
space2stats-ingest postgresql://username:password@localhost:5439/postgres ./space2stats_ingest/METADATA/stac/space2stats/space2stats_population_2020/space2stats_reupload_test.json space2stats_test.parquet 
```