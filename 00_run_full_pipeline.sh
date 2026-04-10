#!/bin/bash
set -euo pipefail

# Optional single ticker override via CLI argument
TICKER_ARG="${1:-}"
export TICKER_ARG

# --- CONFIGURATION ---
export GCP_PROJECT="${GCP_PROJECT:-kineviz-bigquery-graph}"
export BQ_LOCATION="${BQ_LOCATION:-US}"
export BQ_DATASET="${BQ_DATASET:-fortune500_test}"
export GCS_BUCKET="${GCS_BUCKET:-gs://kineviz-fortune500-data}"
export GEMINI_MODEL="${GEMINI_MODEL:-gemini-3.1-pro-preview}"
# Optional explicit list of tickers to scrape.
# Example: TICKERS=("AAPL" "MSFT" "GOOGL")
# Leave empty to use list.csv + SCRAPER_LIMIT defaults from scraper.
TICKERS=()

# Python pipeline env vars (kept in sync with notebook 6.3)
export LLM_PROVIDER="vertex"
export MODEL_NAME="$GEMINI_MODEL"
export VERTEX_PROJECT="$GCP_PROJECT"
export GOOGLE_CLOUD_PROJECT="$GCP_PROJECT"
export DATA_DIR="./data/json"
export OUTPUT_DIR="output"

if [[ -n "${TICKER_ARG}" ]]; then
  echo "Running pipeline for single ticker override: ${TICKER_ARG}"
elif [[ ${#TICKERS[@]} -gt 0 ]]; then
  echo "Running pipeline for configured tickers: ${TICKERS[*]}"
else
  echo "Running pipeline for default scraper scope (list.csv + scraper limits)"
fi
echo "GCP_PROJECT=${GCP_PROJECT}, BQ_DATASET=${BQ_DATASET}, MODEL=${GEMINI_MODEL}"

echo "1. Running Scraper..."
if [[ -n "${TICKER_ARG}" ]]; then
  python3 01_scraper.py --ticker "${TICKER_ARG}"
elif [[ ${#TICKERS[@]} -gt 0 ]]; then
  for t in "${TICKERS[@]}"; do
    echo "  -> scraping ticker ${t}"
    python3 01_scraper.py --ticker "${t}"
  done
else
  python3 01_scraper.py
fi

echo "2. Parsing SGML (optional markdown stage)..."
if [[ -n "${TICKER_ARG}" ]]; then
  python3 02_parser.py --ticker "${TICKER_ARG}"
elif [[ ${#TICKERS[@]} -gt 0 ]]; then
  for t in "${TICKERS[@]}"; do
    python3 02_parser.py --ticker "${t}"
  done
else
  python3 02_parser.py
fi

echo "3. Extracting Sections..."
if [[ -n "${TICKER_ARG}" ]]; then
  python3 03_extract_sections.py --ticker "${TICKER_ARG}"
elif [[ ${#TICKERS[@]} -gt 0 ]]; then
  for t in "${TICKERS[@]}"; do
    python3 03_extract_sections.py --ticker "${t}"
  done
else
  python3 03_extract_sections.py
fi

echo "4. Uploading Sections to GCS..."
gsutil -m rsync -r data/json "${GCS_BUCKET}/json"

echo "5. Initializing BigQuery tables + loading scoped sections..."
python3 - <<'PY'
import os
import subprocess
from pathlib import Path

from google.cloud import bigquery

GCP_PROJECT = os.environ["GCP_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
GCS_BUCKET = os.environ["GCS_BUCKET"]
BQ_LOCATION = os.environ["BQ_LOCATION"]

client = bigquery.Client(project=GCP_PROJECT)

def run_bq_query(filename):
    with open(filename, "r") as f:
        sql = f.read()
    sql = sql.replace("sec_filings.", f"{BQ_DATASET}.")
    sql = sql.replace("sec_filings;", f"{BQ_DATASET};")
    print(f"Executing {filename}...")
    job = client.query(sql, location=BQ_LOCATION)
    job.result()
    print(f"✓ Executed {filename}")

run_bq_query("04_init_tables.sql")

# Load specific URIs matching local extraction to save gsutil wildcards hanging
local_sections = sorted(Path("./data/json").glob("*/*/sections.jsonl"))
if os.environ.get("TICKER_ARG"):
    local_sections = [p for p in local_sections if p.parent.parent.name.upper() == os.environ["TICKER_ARG"].upper()]

if local_sections:
    section_uris = [
        f"{GCS_BUCKET}/json/{p.parent.parent.name}/{p.parent.name}/sections.jsonl"
        for p in local_sections
    ]
    print(f"✓ Scoped sections load to {len(section_uris)} file(s)")
else:
    print("⚠ No local sections found; falling back to loading entire bucket via wildcard")
    section_uris = [f"{GCS_BUCKET}/json/*/*/sections.jsonl"]

schema_spec = "filing_id:STRING,company:STRING,company_name:STRING,cik:STRING,sic:STRING,irs_number:STRING,state_of_inc:STRING,org_name:STRING,sec_file_number:STRING,film_number:STRING,business_street_1:STRING,business_street_2:STRING,business_city:STRING,business_state:STRING,business_zip:STRING,business_phone:STRING,mail_street_1:STRING,mail_street_2:STRING,mail_city:STRING,mail_state:STRING,mail_zip:STRING,filing_url:STRING,year:INTEGER,section_id:STRING,content:STRING"
type_map = {"STRING": "STRING", "INTEGER": "INT64"}
schema = []
for col in schema_spec.split(","):
    name, typ = col.split(":")
    schema.append(bigquery.SchemaField(name, type_map.get(typ, typ)))

table_id = f"{GCP_PROJECT}.{BQ_DATASET}.sections"
load_config = bigquery.LoadJobConfig(
    schema=schema,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
)

print("Loading sections into BigQuery...")
job = client.load_table_from_uri(section_uris, table_id, job_config=load_config)
job.result()
print("✓ Sections loaded")
PY

echo "6. Running BigQuery extraction..."
python3 - <<'PY'
import os
from google.cloud import bigquery

GCP_PROJECT = os.environ["GCP_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_LOCATION = os.environ["BQ_LOCATION"]
client = bigquery.Client(project=GCP_PROJECT)

with open("05_extraction.sql", "r") as f:
    sql = f.read()
sql = sql.replace("sec_filings.", f"{BQ_DATASET}.").replace("sec_filings;", f"{BQ_DATASET};")
print("Executing 05_extraction.sql...")
job = client.query(sql, location=BQ_LOCATION)
job.result()
print("✓ Extraction complete")
PY

echo "7. Exporting insights and running python normalization..."
bq extract --destination_format=NEWLINE_DELIMITED_JSON "${GCP_PROJECT}:${BQ_DATASET}.insights" "${GCS_BUCKET}/parquets/${GEMINI_MODEL}/insights.jsonl"
mkdir -p "python_pipeline/output/${GEMINI_MODEL}/extractions"
gsutil cp "${GCS_BUCKET}/parquets/${GEMINI_MODEL}/insights.jsonl" "python_pipeline/output/${GEMINI_MODEL}/extractions/insights.jsonl"

python3 - <<'PY'
import base64
import os
import subprocess
from pathlib import Path
import pandas as pd

PIPELINE_MODEL_NAME = os.environ["GEMINI_MODEL"]
env = os.environ.copy()

def run_step(cmd):
    print("$", " ".join(cmd))
    p = subprocess.run(cmd, cwd="python_pipeline", env=env, text=True, capture_output=True)
    if p.stdout:
        print(p.stdout)
    if p.returncode != 0:
        if p.stderr:
            print(p.stderr)
        raise RuntimeError(f"Step failed ({p.returncode}): {' '.join(cmd)}")

def parquet_rows(path):
    p = Path(path)
    if not p.exists():
        return 0
    return len(pd.read_parquet(p))

def ensure_entity_norm_taxonomy_files():
    # In local repo runs these files should already exist.
    # This keeps parity with notebook behavior for fragile runtimes.
    data_dir = Path("python_pipeline/entity_normalization/data")
    data_dir.mkdir(parents=True, exist_ok=True)
    required = [
        "competitor_sectors.csv",
        "competitor_types.csv",
        "risk_categories.csv",
        "market_geographic_regions.csv",
        "market_product_categories.csv",
    ]
    missing = [name for name in required if not (data_dir / name).exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing normalization taxonomy files: {missing}. "
            "Ensure python_pipeline/entity_normalization/data is present."
        )

print("Preparing python_pipeline environment...")
subprocess.run(["uv", "sync", "--extra", "vertex"], cwd="python_pipeline", env=env, check=True)
ensure_entity_norm_taxonomy_files()

print("Running transform...")
run_step(["uv", "run", "python", "transform.py"])

parquet_dir = Path("python_pipeline") / "output" / PIPELINE_MODEL_NAME / "parquet"
n_comp = parquet_rows(parquet_dir / "nodes_competitor.parquet")
n_risk = parquet_rows(parquet_dir / "nodes_risk.parquet")
n_market = parquet_rows(parquet_dir / "nodes_market.parquet")
print(f"Detected rows -> competitors: {n_comp}, risks: {n_risk}, markets: {n_market}")

if n_comp > 0:
    run_step(["uv", "run", "python", "entity_normalization/resolve_competitors.py", "--model", PIPELINE_MODEL_NAME])
    run_step([
        "uv", "run", "python", "entity_normalization/categorize_competitor_markets.py",
        "--model", PIPELINE_MODEL_NAME,
        "--parquet-dir", f"output/{PIPELINE_MODEL_NAME}/parquet",
    ])
else:
    print("Skipping competitor normalization (0 competitor rows)")

if n_risk > 0:
    run_step(["uv", "run", "python", "entity_normalization/categorize_risks.py", "--model", PIPELINE_MODEL_NAME])
else:
    print("Skipping risk categorization (0 risk rows)")

if n_market > 0:
    run_step(["uv", "run", "python", "entity_normalization/categorize_markets.py", "--model", PIPELINE_MODEL_NAME])
else:
    print("Skipping market categorization (0 market rows)")
PY

echo "8. Uploading parquet output to GCS..."
PARQUET_LOCAL_DIR="python_pipeline/output/${GEMINI_MODEL}/parquet"
GCS_PARQUET_PATH="${GCS_BUCKET}/parquets/${GEMINI_MODEL}/parquet"
gsutil -m rsync -r "${PARQUET_LOCAL_DIR}" "${GCS_PARQUET_PATH}"

echo "9. Loading parquet tables into BigQuery (skip missing/invalid optional tables)..."
python3 - <<'PY'
import os
import subprocess
from google.cloud import bigquery
import pandas as pd

GCP_PROJECT = os.environ["GCP_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_LOCATION = os.environ["BQ_LOCATION"]
PARQUET_LOCAL_DIR = f"python_pipeline/output/{os.environ['GEMINI_MODEL']}/parquet"
gcs_parquet_path = f"{os.environ['GCS_BUCKET']}/parquets/{os.environ['GEMINI_MODEL']}/parquet"

client = bigquery.Client(project=GCP_PROJECT)

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
    "edges_in_market_category": "edges_in_market_category.parquet",
}

loaded_tables = []
skipped_tables = []

for bq_table_name, parquet_file in tables_to_load.items():
    uri = f"{gcs_parquet_path}/{parquet_file}"
    local_parquet = f"{PARQUET_LOCAL_DIR}/{parquet_file}"

    exists = subprocess.run(["gsutil", "ls", uri], capture_output=True, text=True)
    if exists.returncode != 0:
        print(f"Skipping {bq_table_name}: missing {uri}")
        skipped_tables.append((bq_table_name, parquet_file))
        continue

    if os.path.exists(local_parquet):
        try:
            col_count = len(pd.read_parquet(local_parquet).columns)
        except Exception as e:
            print(f"Skipping {bq_table_name}: unable to inspect {local_parquet} ({e})")
            skipped_tables.append((bq_table_name, parquet_file))
            continue
        if col_count == 0:
            print(f"Skipping {bq_table_name}: zero-column parquet {local_parquet}")
            skipped_tables.append((bq_table_name, parquet_file))
            continue

    query = f"""
    LOAD DATA OVERWRITE `{GCP_PROJECT}.{BQ_DATASET}.{bq_table_name}`
    FROM FILES (
      format = 'PARQUET',
      uris = ['{uri}']
    );
    """
    print(f"Loading {bq_table_name} from {parquet_file}...")
    job = client.query(query, location=BQ_LOCATION)
    job.result()
    loaded_tables.append((bq_table_name, parquet_file))

print(f"\nLoaded {len(loaded_tables)} parquet tables.")
if skipped_tables:
    print(f"Skipped {len(skipped_tables)} tables:")
    for n, p in skipped_tables:
        print(f"  - {n} ({p})")
PY

echo "10. Creating Property Graph DDL..."
python3 - <<'PY'
import os
from google.cloud import bigquery

GCP_PROJECT = os.environ["GCP_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_LOCATION = os.environ["BQ_LOCATION"]
client = bigquery.Client(project=GCP_PROJECT)

fallback_ddls = [
    f"CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BQ_DATASET}.nodes_normalized_competitor` (id STRING, label STRING, competitor_type STRING, sector STRING, product_category STRING)",
    f"CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BQ_DATASET}.nodes_geographic_region` (id STRING, label STRING, description STRING)",
    f"CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BQ_DATASET}.nodes_market_category` (id STRING, label STRING, description STRING)",
    f"CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BQ_DATASET}.nodes_risk_category` (id STRING, label STRING, description STRING)",
    f"CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BQ_DATASET}.edges_instance_of` (source_node STRING, target_node STRING)",
    f"CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BQ_DATASET}.edges_subsidiary_of` (source_node STRING, target_node STRING)",
    f"CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BQ_DATASET}.edges_has_risk_category` (source_node STRING, target_node STRING)",
    f"CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BQ_DATASET}.edges_in_region` (source_node STRING, target_node STRING)",
    f"CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BQ_DATASET}.edges_in_product_category` (source_node STRING, target_node STRING)",
    f"CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BQ_DATASET}.edges_in_market_category` (source_node STRING, target_node STRING)",
]
for ddl in fallback_ddls:
    client.query(ddl, location=BQ_LOCATION).result()

with open("06_create_property_graph_ddl.sql", "r") as f:
    sql = f.read()
sql = sql.replace("sec_filings.", f"{BQ_DATASET}.").replace("sec_filings;", f"{BQ_DATASET};")
job = client.query(sql, location=BQ_LOCATION)
job.result()
print("✓ Property graph DDL applied")
PY

echo "Pipeline complete."
