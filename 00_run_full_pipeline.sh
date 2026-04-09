#!/bin/bash
set -e

# Parse optional ticker argument (default to *)
TICKER=${1:-*}

# --- CONFIGURATION ---
# Set these to match your GCP environment
export GCP_PROJECT="kineviz-bigquery-graph"
export BQ_LOCATION="US"
export BQ_DATASET="fortune500"
export GCS_BUCKET="gs://kineviz-fortune500-data"
export GEMINI_MODEL="gemini-3-flash"

# Python Pipeline Env Vars
export LLM_PROVIDER="vertex"
export MODEL_NAME="$GEMINI_MODEL"
export VERTEX_PROJECT="$GCP_PROJECT"
export VERTEX_LOCATION="$BQ_LOCATION"
export DATA_DIR="../data/json"

# ---------------------

echo "1. Running Scraper..."
if [ "$TICKER" == "*" ]; then
    python3 01_scraper.py
else
    # If a specific ticker is provided, we might need to modify scraper or just run it as is.
    # For now, we'll just run it.
    python3 01_scraper.py
fi

echo "2. Extracting Sections..."
python3 03_extract_sections.py

echo "3. Uploading Sections to GCS and Generating Insights in BigQuery..."
# Upload sections to GCS
gsutil -m rsync -r data/json "${GCS_BUCKET}/json"

# Initialize tables
sed -e "s/sec_filings\./\`${GCP_PROJECT}.${BQ_DATASET}\`\./g" -e "s/sec_filings;/\`${GCP_PROJECT}.${BQ_DATASET}\`;/g" 04_init_tables.sql | bq query --use_legacy_sql=false --location="$BQ_LOCATION"

# Load sections into BigQuery
cat << 'EOF' > load_sections_temp.py
import os
from google.cloud import bigquery

GCP_PROJECT = os.environ["GCP_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
GCS_BUCKET = os.environ["GCS_BUCKET"]

client = bigquery.Client(project=GCP_PROJECT)

print("Loading sections from sections.jsonl...")
sections_schema = "filing_id:STRING,company:STRING,company_name:STRING,cik:STRING,sic:STRING,irs_number:STRING,state_of_inc:STRING,org_name:STRING,sec_file_number:STRING,film_number:STRING,business_street_1:STRING,business_street_2:STRING,business_city:STRING,business_state:STRING,business_zip:STRING,business_phone:STRING,mail_street_1:STRING,mail_street_2:STRING,mail_city:STRING,mail_state:STRING,mail_zip:STRING,filing_url:STRING,year:INTEGER,section_id:STRING,content:STRING"
sections_uri = f"{GCS_BUCKET}/json/*/*/sections.jsonl"
query = f"""
LOAD DATA OVERWRITE `{GCP_PROJECT}.{BQ_DATASET}.sections`
FROM FILES (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['{sections_uri}']
)
WITH SCHEMA (
  {', '.join([f"{col.split(':')[0]} {col.split(':')[1]}" for col in sections_schema.split(',')])}
);
"""
job = client.query(query)
job.result()
print("Sections loaded.")
EOF
export GCS_BUCKET
python3 load_sections_temp.py
rm load_sections_temp.py

# Run extraction in BigQuery
echo "Running AI Extraction in BigQuery..."
sed -e "s/sec_filings\./\`${GCP_PROJECT}.${BQ_DATASET}\`\./g" -e "s/sec_filings;/\`${GCP_PROJECT}.${BQ_DATASET}\`;/g" 05_extraction.sql | bq query --use_legacy_sql=false --location="$BQ_LOCATION"

# Export insights back to GCS and download locally
echo "Exporting insights to GCS and downloading locally..."
bq extract --destination_format=NEWLINE_DELIMITED_JSON "${GCP_PROJECT}:${BQ_DATASET}.insights" "${GCS_BUCKET}/parquets/${GEMINI_MODEL}/insights.jsonl"

mkdir -p "python_pipeline/output/${GEMINI_MODEL}/extractions"
gsutil cp "${GCS_BUCKET}/parquets/${GEMINI_MODEL}/insights.jsonl" "python_pipeline/output/${GEMINI_MODEL}/extractions/insights.jsonl"

echo "4. Running Python Parquet Pipeline (Transformation & Normalization)..."
cd python_pipeline
# Skip extraction, just run transformation
uv run python transform.py

echo "4.1 Running Entity Normalization..."
uv run python entity_normalization/resolve_competitors.py
uv run python entity_normalization/categorize_risks.py
uv run python entity_normalization/categorize_markets.py
uv run python entity_normalization/categorize_competitor_markets.py
cd ..

echo "5. Uploading Parquet files to GCS..."
PARQUET_LOCAL_DIR="python_pipeline/output/${GEMINI_MODEL}/parquet"
GCS_PARQUET_PATH="${GCS_BUCKET}/parquets/${GEMINI_MODEL}/parquet"

gsutil -m rsync -r "$PARQUET_LOCAL_DIR" "$GCS_PARQUET_PATH"

echo "6. Loading Parquet files into BigQuery..."
# Create a temporary python script to load the tables
cat << 'EOF' > load_bq_temp.py
import os
from google.cloud import bigquery

GCP_PROJECT = os.environ["GCP_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
GCS_PARQUET_PATH = os.environ["GCS_PARQUET_PATH"]
GCS_BUCKET = os.environ["GCS_BUCKET"]
GEMINI_MODEL = os.environ["GEMINI_MODEL"]

client = bigquery.Client(project=GCP_PROJECT)

# 1. Load Parquet Tables
tables_to_load = {
    "nodes_company": "nodes_company.parquet",
    "nodes_document": "nodes_document.parquet",
    "nodes_section": "nodes_section.parquet",
    "nodes_reference": "nodes_reference.parquet",
    "nodes_opportunity": "nodes_opportunity.parquet",
    "nodes_competitor": "nodes_competitor.parquet",
    "nodes_market": "nodes_market_categorized.parquet",
    "nodes_risk": "nodes_risk_categorized.parquet",
    "nodes_normalized_competitor": "nodes_normalized_competitor.parquet",
    "nodes_geographic_region": "nodes_geographic_region.parquet",
    "nodes_market_category": "nodes_market_category.parquet",
    "nodes_risk_category": "nodes_risk_category.parquet",
    "edges_filed": "edges_filed.parquet",
    "edges_doc_contains_section": "edges_doc_contains_section.parquet",
    "edges_section_contains_ref": "edges_section_contains_ref.parquet",
    "edges_entering": "edges_entering.parquet",
    "edges_exiting": "edges_exiting.parquet",
    "edges_expanding": "edges_expanding.parquet",
    "edges_faces_risk": "edges_faces_risk.parquet",
    "edges_pursuing": "edges_pursuing.parquet",
    "edges_competes": "edges_competes.parquet",
    "edges_market_has_reference": "edges_market_has_reference.parquet",
    "edges_risk_has_reference": "edges_risk_has_reference.parquet",
    "edges_opportunity_has_reference": "edges_opportunity_has_reference.parquet",
    "edges_competitor_has_reference": "edges_competitor_has_reference.parquet",
    "edges_instance_of": "edges_instance_of.parquet",
    "edges_subsidiary_of": "edges_subsidiary_of.parquet",
    "edges_has_risk_category": "edges_has_risk_category.parquet",
    "edges_in_region": "edges_in_region.parquet",
    "edges_in_product_category": "edges_in_product_category.parquet",
    "edges_in_market_category": "edges_in_market_category.parquet"
}

for bq_table_name, parquet_file in tables_to_load.items():
    uri = f"{GCS_PARQUET_PATH}/{parquet_file}"
    query = f"""
    LOAD DATA OVERWRITE `{GCP_PROJECT}.{BQ_DATASET}.{bq_table_name}`
    FROM FILES (
      format = 'PARQUET',
      uris = ['{uri}']
    );
    """
    print(f"Loading {bq_table_name} from {parquet_file}...")
    job = client.query(query)
    job.result()

# 2. Load JSONL Tables (sections and insights)
print("Loading sections from sections.jsonl...")
sections_schema = "filing_id:STRING,company:STRING,company_name:STRING,cik:STRING,sic:STRING,irs_number:STRING,state_of_inc:STRING,org_name:STRING,sec_file_number:STRING,film_number:STRING,business_street_1:STRING,business_street_2:STRING,business_city:STRING,business_state:STRING,business_zip:STRING,business_phone:STRING,mail_street_1:STRING,mail_street_2:STRING,mail_city:STRING,mail_state:STRING,mail_zip:STRING,filing_url:STRING,year:INTEGER,section_id:STRING,content:STRING"
sections_uri = f"{GCS_BUCKET}/json/*/*/sections.jsonl"
query = f"""
LOAD DATA OVERWRITE `{GCP_PROJECT}.{BQ_DATASET}.sections`
FROM FILES (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['{sections_uri}']
)
WITH SCHEMA (
  {', '.join([f"{col.split(':')[0]} {col.split(':')[1]}" for col in sections_schema.split(',')])}
);
"""
job = client.query(query)
job.result()

print("Loading insights from insights.jsonl...")
insights_uri = f"{GCS_BUCKET}/parquets/{GEMINI_MODEL}/insights.jsonl"
query = f"""
LOAD DATA OVERWRITE `{GCP_PROJECT}.{BQ_DATASET}.insights`
FROM FILES (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['{insights_uri}'],
  ignore_unknown_values = true
);
"""
job = client.query(query)
job.result()

EOF

export GCS_PARQUET_PATH
export GCS_BUCKET
export GEMINI_MODEL
python3 load_bq_temp.py
rm load_bq_temp.py

echo "7. Creating Property Graph DDL..."
# Replace sec_filings. with the actual dataset
sed -e "s/sec_filings\./\`${GCP_PROJECT}.${BQ_DATASET}\`\./g" -e "s/sec_filings;/\`${GCP_PROJECT}.${BQ_DATASET}\`;/g" 06_create_property_graph_ddl.sql | bq query --use_legacy_sql=false --location="$BQ_LOCATION"

echo "Pipeline Complete."
