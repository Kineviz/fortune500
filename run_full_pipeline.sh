#!/bin/bash
set -e

echo "1. Uploading JSONL files to GCS..."
gsutil -m rsync -r data/json gs://kineviz_fortune500_sec_filing/json

echo "2. Loading data into BigQuery (sec_filings.sections)..."
bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect --replace sec_filings.sections "gs://kineviz_fortune500_sec_filing/json/AAPL/*/sections.jsonl"

echo "3. Running AI Extraction (sec_filings.insights)..."
bq query --use_legacy_sql=false --location=US "$(cat extraction.sql)"

echo "4. Transforming to Graph Edges (sec_filings.graph_edges)..."
cat create_graph.sql | bq query --use_legacy_sql=false --location=US

echo "5. Preparing Node/Edge Tables..."
cat prepare_property_graph.sql | bq query --use_legacy_sql=false --location=US

echo "6. Creating Property Graph DDL..."
cat create_property_graph_ddl.sql | bq query --use_legacy_sql=false --location=US

echo "Pipeline Complete."
