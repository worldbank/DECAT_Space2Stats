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

# Path to the sample Parquet file
PARQUET_FILE="nyc_sample.parquet"

# Name of the target table
TABLE_NAME="space2stats_nyc_sample"

# Check if the table exists
TABLE_EXISTS=$(psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -tAc "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema='public' AND table_name='$TABLE_NAME');")

echo "Importing $PARQUET_FILE..."

if [ "$TABLE_EXISTS" = "t" ]; then
    # Table exists, append data
    ogr2ogr -f "PostgreSQL" \
        PG:"host=$DB_HOST port=$DB_PORT dbname=$DB_NAME user=$DB_USER password=$DB_PASSWORD" \
        "$PARQUET_FILE" \
        -nln $TABLE_NAME \
        -append 
else
    # Table does not exist, create table and import data
    ogr2ogr -f "PostgreSQL" \
        PG:"host=$DB_HOST port=$DB_PORT dbname=$DB_NAME user=$DB_USER password=$DB_PASSWORD" \
        "$PARQUET_FILE" \
        -nln $TABLE_NAME 
    
    TABLE_EXISTS="t"
fi

if [ $? -ne 0 ]; then
    echo "Failed to import $PARQUET_FILE"
    exit 1
fi

echo "Successfully imported $PARQUET_FILE"

echo "The Parquet file has been imported."