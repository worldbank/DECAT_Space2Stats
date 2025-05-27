# Space2Stats API

### Testing locally

Export the database variables:
```bash
export PGHOST="your_pg_host"
export PGPORT="5432"
export PGDATABASE="your_database"
export PGUSER="your_username"
export PGPASSWORD="your_password"
export PGTABLENAME="your_pgtable"
export TIMESERIES_TABLE_NAME="your_timeseries_table"
export S3_BUCKET_NAME="your_s3_bucket"
```

Run this command from the `space2stats_api/space2stats/src` directory:
```
poetry run python -m space2stats    
```