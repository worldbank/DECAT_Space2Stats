# Space2Stats Development Database

This directory contains everything needed to run a local PostgreSQL development database with sample data for Space2Stats.

## Quick Start

```bash
# Navigate to dev-db directory
cd dev-db

# Download the sample data
aws s3 sync s3://wbg-geography01/Space2Stats/sample_data/local_db/ .

# Start the development database
make up

```

## Available Commands

Run `make help` to see all available commands:

- `make up` - Start database with sample data
- `make down` - Stop database
- `make logs` - Show database logs
- `make reset` - Reset database (removes all data)
- `make generate` - Generate additional sample data
- `make clean` - Stop and remove everything

## Database Access

- **PostgreSQL**: `postgresql://username:password@localhost:5439/postgres`
- **pgAdmin**: http://localhost:8080 (admin@space2stats.com / admin)

## Sample Data

The seeding script loads two parquet datasets into the development database:

- `space2stats_sample_cs.parquet` → inserted into the `space2stats` table. It contains administrative boundaries, demographic breakdowns, nighttime lights, built area statistics, GHS indicators, and flood exposure metrics.
- `space2stats_sample_ts.parquet` → inserted into the `climate` table. It provides Standardized Precipitation Index (SPI) climate time series aggregated by H3 hexagon.

Both parquet files must live in `init-scripts/data/` with the exact filenames above (the seed script reads from that directory).

The latest copies of these parquet files are stored in `s3://wbg-geography01/Space2Stats/sample_data/local_db/`.

## Troubleshooting

- **Database won't start**: Check if port 5439 is available
- **Connection issues**: Verify the connection string
- **Sample data missing**: Run `make seed` to load data from parquet file
- **Reset everything**: Run `make reset` or Run `make clean` then `make up`

## Integration with Main Project

The development database is designed to work with the main Space2Stats API. Update your `space2stats_api/db.env` to point to the development database:

```bash
PGHOST=localhost
PGPORT=5439
PGDATABASE=postgres
PGUSER=username
PGPASSWORD=password
PGTABLENAME=space2stats
TIMESERIES_TABLE_NAME=climate
```
```
