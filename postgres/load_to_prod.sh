#!/bin/bash


# Load environment variables from db.env file
if [ -f db.env ]; then
  export $(cat db.env | grep -v '#' | awk '/=/ {print $1}')
fi

# Check if required environment variables are set
if [ -z "$PGHOST" ] || [ -z "$PGPORT" ] || [ -z "$PGDATABASE" ] || [ -z "$PGUSER" ] || [ -z "$PGPASSWORD" ]; then
  echo "One or more required environment variables are missing."
  exit 1
fi

# Directory containing the Parquet chunks
CHUNKS_DIR="parquet_chunks"

# Name of the target table
TABLE_NAME="space2stats"
PARQUET_FILE=space2stats.parquet

echo "Starting"

ogr2ogr -progress -f "PostgreSQL" \
    PG:"host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER password=$PGPASSWORD" \
    "$PARQUET_FILE" \
    -nln $TABLE_NAME \
    -append \
    -lco SPATIAL_INDEX=NONE

