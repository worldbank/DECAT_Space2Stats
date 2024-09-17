!/bin/bash

# Load environment variables from wb_aws.env
source wb_aws.env

# S3 and file configuration
S3_BUCKET="wbg-geography01"
PARQUET_FILE="Space2Stats/parquet/GLOBAL/combined_population.parquet"
LOCAL_PARQUET_FILE="space2stats.parquet"

# PostgreSQL configuration
PGHOST="${MY_DOCKER_IP:-127.0.0.1}"
PGPORT=5439
PGDATABASE="postgis"
PGUSER="username"
PGPASSWORD="password"

# Download Parquet file from S3
echo "Downloading Parquet file from S3..."
aws s3 cp --quiet s3://$S3_BUCKET/$PARQUET_FILE $LOCAL_PARQUET_FILE
