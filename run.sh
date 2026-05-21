#!/usr/bin/env bash
# Usage: source .env && ./run.sh
# The .env file should contain:
#   S3_BUCKET="your-bucket"
#   S3_PATH="mydb/sub1"
#   S3_ACCESS_KEY="your-access-key"
#   S3_SECRET_KEY="your-secret-key"
#   S3_ENDPOINT="https://your-endpoint.com"
#   S3_REGION="ap-shanghai"
#   S3_CACHE_DIR="/tmp/levels3-cache"

# Load from .env if present
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

# Validate required variables
: "${S3_BUCKET:?S3_BUCKET not set}"
: "${S3_PATH:?S3_PATH not set}"
: "${S3_ACCESS_KEY:?S3_ACCESS_KEY not set}"
: "${S3_SECRET_KEY:?S3_SECRET_KEY not set}"

export S3_BUCKET S3_PATH S3_ACCESS_KEY S3_SECRET_KEY S3_ENDPOINT S3_REGION S3_CACHE_DIR
go run cmd/leveldb_print/main.go
