#!/bin/bash


# Load environment variables from db.env file
if [ -f db.env ]; then
  export $(cat db.env | grep -v '#' | awk '/=/ {print $1}')
fi

# Check if required environment variables are set
if [ -z "$DB_HOST" ] || [ -z "$DB_PORT" ] || [ -z "$DB_NAME" ] || [ -z "$DB_USER" ] || [ -z "$DB_PASSWORD" ]; then
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
    PG:"host=$DB_HOST port=$DB_PORT dbname=$DB_NAME user=$DB_USER password=$DB_PASSWORD" \
    "$PARQUET_FILE" \
    -nln $TABLE_NAME \
    -append \
    -lco SPATIAL_INDEX=NONE

