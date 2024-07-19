!/bin/bash

# Load environment variables from wb_aws.env
source wb_aws.env

# S3 and file configuration
S3_BUCKET="wbg-geography01"
PARQUET_FILE="Space2Stats/parquet/GLOBAL/combined_population.parquet"
LOCAL_PARQUET_FILE="space2stats.parquet"

# PostgreSQL configuration
DB_HOST="${MY_DOCKER_IP:-127.0.0.1}"
DB_PORT=5439
DB_NAME="postgis"
DB_USER="username"
DB_PASSWORD="password"

# Download Parquet file from S3
echo "Downloading Parquet file from S3..."
aws s3 cp --quiet s3://$S3_BUCKET/$PARQUET_FILE $LOCAL_PARQUET_FILE
