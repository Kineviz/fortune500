#!/bin/bash
set -e

# Parse optional ticker argument (default to *)
TICKER=${1:-*}

echo "1. Uploading JSONL files to GCS..."
gsutil -m rsync -r data/json gs://kineviz_fortune500_sec_filing/json

if [ "$TICKER" == "*" ]; then
  echo "Loading ALL data (Replace mode)..."
  bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect --replace sec_filings.sections "gs://kineviz_fortune500_sec_filing/json/*/*/sections.jsonl"
else
  echo "Loading data for $TICKER (Incremental mode)..."
  # 1. Clear existing data for this ticker to prevent duplicates
  bq query --use_legacy_sql=false "DELETE FROM sec_filings.sections WHERE company = '$TICKER'"
  
  # 2. Append new data
  bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect --noreplace sec_filings.sections "gs://kineviz_fortune500_sec_filing/json/$TICKER/*/sections.jsonl"
fi

echo "3. Running AI Extraction (sec_filings.insights)..."
bq query --use_legacy_sql=false --location=US "$(cat extraction.sql)"

echo "4. Transforming to Graph Edges (sec_filings.graph_edges)..."
cat create_graph.sql | bq query --use_legacy_sql=false --location=US

echo "5. Preparing Node/Edge Tables..."
cat prepare_property_graph.sql | bq query --use_legacy_sql=false --location=US

echo "6. Creating Property Graph DDL..."
cat create_property_graph_ddl.sql | bq query --use_legacy_sql=false --location=US

echo "Pipeline Complete."
