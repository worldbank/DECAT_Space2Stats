## Database Deliverable Acceptance Test 

### Description of Deliverable

This deliverable includes the implementation of an ETL process, the design of a database schema, the selection of a format for raw data storage, infrastructure as code, database configuration, and accompanying documentation.

The acceptance test below provides steps to verify that the deliverable meets our agreed-upon specifications.

### Input Data

The input data is stored in Parquet format on AWS S3 (object storage), specifically in the file `space2stats_updated.parquet`. Any additional fields must be appended to this file. The Parquet file is tabular with the following columns:
- `hex_id`
- `{variable_name}_{aggregation_method[sum, mean, etc.]}_{year}`

### Database Setup

You can use a local database for this acceptance test by running the following command in the root directory:

```bash
docker-compose up
```

Alternatively, you can connect to a remote database, such as the [Tembo database](reluctantly-simple-spoonbill.data-1.use1.tembo.io) used for production.

### Data Ingestion

Set the database environment variables in `db.env`:

```bash
PGHOST=localhost
PGPORT=5432
PGDATABASE=postgis
PGUSER=postgres
PGPASSWORD=password
PGTABLENAME=space2stats
```

> Note: If using the `docker-compose` approach, the above configuration is suitable.

To ingest data, run the following script:

```bash
chmod +x postgres/load_to_prod.sh
./postgres/load_to_prod.sh
```

### Database Configuration

Once connected to your database via `psql` or another PostgreSQL client (e.g., `pgAdmin`):

- Create an index on the `space2stats` table:

```sql
CREATE INDEX idx_hex_id ON space2stats (hex_id);
```

### Testing the Database Table

You can run sample queries to verify data is accessible in the database. Our primary access patterns involve filtering by specific hex identifiers and returning specified fields. Here are some example queries:

```sql
SELECT * FROM space2stats LIMIT 100;
SELECT * FROM space2stats WHERE hex_id = '86beabd8fffffff';
SELECT sum_pop_2020 FROM space2stats WHERE hex_id IN ('86beabd8fffffff', '86beabdb7ffffff', '86beac01fffffff');
```