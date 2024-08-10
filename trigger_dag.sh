#!/bin/bash

DEFAULT_URL="http://example.com/segment.gz"
COMMONCRAWL_URL="${1:-$DEFAULT_URL}"

DAG_ID="extract_and_load_dag"

airflow dags trigger -c "{\"commoncrawl_url\": \"$COMMONCRAWL_URL\"}" $DAG_ID

# Optional: Print a message indicating the DAG was triggered
echo "Triggered DAG '$DAG_ID' with Common Crawl URL: $COMMONCRAWL_URL"
