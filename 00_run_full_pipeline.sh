#!/bin/bash
set -euo pipefail

# Optional ticker override via CLI argument.
# Accepts either a single ticker ("AAPL") or comma-separated tickers ("GOOGL,AAPL").
TICKER_ARG="${1:-}"
export TICKER_ARG

ARG_TICKERS=()
if [[ -n "${TICKER_ARG}" ]]; then
  IFS=',' read -r -a _raw_arg_tickers <<< "${TICKER_ARG}"
  for t in "${_raw_arg_tickers[@]}"; do
    # Trim leading/trailing whitespace without external tools
    t="${t#"${t%%[![:space:]]*}"}"
    t="${t%"${t##*[![:space:]]}"}"
    if [[ -n "${t}" ]]; then
      ARG_TICKERS+=("${t}")
    fi
  done
fi

# CSV form consumed by embedded python snippets for scoped loading
if [[ ${#ARG_TICKERS[@]} -gt 0 ]]; then
  export TICKERS_CSV="$(IFS=,; echo "${ARG_TICKERS[*]}")"
else
  export TICKERS_CSV=""
fi

# --- CONFIGURATION ---
export GCP_PROJECT="${GCP_PROJECT:-kineviz-bigquery-graph}"
export BQ_LOCATION="${BQ_LOCATION:-US}"
export BQ_DATASET="${BQ_DATASET:-fortune500_test}"
export GCS_BUCKET="${GCS_BUCKET:-gs://kineviz-fortune500-data}"
export GEMINI_MODEL="${GEMINI_MODEL:-gemini-3.1-pro-preview}"
# Set to 1 to DROP `insights` before init (full re-extract). Default: keep `insights` so
# 05_extraction.sql only inserts missing rows (incremental via NOT EXISTS).
export FORCE_FULL_INSIGHTS_REFRESH="${FORCE_FULL_INSIGHTS_REFRESH:-0}"
# Optional explicit list of tickers to scrape.
# Example: TICKERS=("AAPL" "MSFT" "GOOGL")
# Leave empty to use list.csv + SCRAPER_LIMIT defaults from scraper.
TICKERS=()
if [[ ${#TICKERS[@]} -gt 0 ]]; then
  CONFIG_TICKERS_CSV="$(IFS=,; echo "${TICKERS[*]}")"
else
  CONFIG_TICKERS_CSV=""
fi

# Python pipeline env vars (kept in sync with notebook 6.3)
export LLM_PROVIDER="vertex"
export MODEL_NAME="$GEMINI_MODEL"
export VERTEX_PROJECT="$GCP_PROJECT"
export GOOGLE_CLOUD_PROJECT="$GCP_PROJECT"
export DATA_DIR="./data/json"
export OUTPUT_DIR="output"

if [[ ${#ARG_TICKERS[@]} -gt 0 ]]; then
  echo "Running pipeline for ticker override(s): ${ARG_TICKERS[*]}"
elif [[ ${#TICKERS[@]} -gt 0 ]]; then
  echo "Running pipeline for configured tickers: ${TICKERS[*]}"
else
  echo "Running pipeline for default scraper scope (list.csv + scraper limits)"
fi
echo "GCP_PROJECT=${GCP_PROJECT}, BQ_DATASET=${BQ_DATASET}, MODEL=${GEMINI_MODEL}"
case "${FORCE_FULL_INSIGHTS_REFRESH}" in
  1|true|TRUE|yes|YES)
    echo "FORCE_FULL_INSIGHTS_REFRESH set (will drop insights before init)"
    ;;
  *)
    echo "Incremental insights: existing rows kept; extraction fills gaps only (FORCE_FULL_INSIGHTS_REFRESH=1 to wipe)"
    ;;
esac

echo "1. Running Scraper..."
if [[ -n "${TICKER_ARG}" ]]; then
  echo "  -> scraping tickers ${TICKERS_CSV}"
  python3 01_scraper.py --tickers "${TICKERS_CSV}" --output-dir "data/sgml" --last-n-years 2
elif [[ ${#TICKERS[@]} -gt 0 ]]; then
  echo "  -> scraping tickers ${CONFIG_TICKERS_CSV}"
  python3 01_scraper.py --tickers "${CONFIG_TICKERS_CSV}" --output-dir "data/sgml" --last-n-years 2
else
  python3 01_scraper.py --output-dir "data/sgml" --last-n-years 2
fi

echo "2. Parsing SGML (optional markdown stage)..."
# Parse once over the full scraped corpus from stage 1.
python3 02_parser.py

echo "3. Extracting Sections..."
# Extract once over the full scraped corpus from stage 1.
python3 03_extract_sections.py

echo "4. Uploading Sections to GCS..."
gcloud storage cp --recursive data/json/* "${GCS_BUCKET}/json"

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


def bq_gemini_endpoint(model_env: str, project_id: str) -> str:
    """OPTIONS(ENDPOINT) for BigQuery remote Gemini. Bare model ids become the global
    Vertex URL (see cloud.google.com BigQuery remote models); avoids regional 'model not found'."""
    m = (model_env or "gemini-2.5-pro").strip()
    if m.startswith("http://") or m.startswith("https://"):
        return m
    if m.startswith("models/"):
        m = m[len("models/") :].lstrip()
    return (
        f"https://aiplatform.googleapis.com/v1/projects/{project_id}/"
        f"locations/global/publishers/google/models/{m}"
    )


def run_bq_query(filename):
    with open(filename, "r") as f:
        sql = f.read()
    sql = sql.replace("sec_filings.", f"{BQ_DATASET}.")
    sql = sql.replace("sec_filings;", f"{BQ_DATASET};")
    endpoint = bq_gemini_endpoint(os.environ.get("GEMINI_MODEL"), GCP_PROJECT)
    sql = sql.replace("__GEMINI_ENDPOINT__", endpoint)
    print(f"Executing {filename}...")
    job = client.query(sql, location=BQ_LOCATION)
    job.result()
    print(f"✓ Executed {filename}")

# Optional full wipe: default keeps `insights` so 05_extraction INSERT only adds missing keys.
fr = os.environ.get("FORCE_FULL_INSIGHTS_REFRESH", "").strip().lower()
if fr in ("1", "true", "yes", "y"):
    try:
        client.query(
            f"DROP TABLE IF EXISTS `{GCP_PROJECT}.{BQ_DATASET}.insights`",
            location=BQ_LOCATION,
        ).result()
        print("✓ Dropped insights table (FORCE_FULL_INSIGHTS_REFRESH)")
    except Exception as e:
        print(f"ℹ Skipped insights drop: {e}")
else:
    print("ℹ Keeping existing insights table (incremental extraction)")

run_bq_query("04_init_tables.sql")

# Load specific URIs matching local extraction to save gsutil wildcards hanging
local_sections = sorted(Path("./data/json").glob("*/*/sections.jsonl"))
ticker_csv = os.environ.get("TICKERS_CSV", "").strip()
if ticker_csv:
    ticker_set = {t.strip().upper() for t in ticker_csv.split(",") if t.strip()}
    local_sections = [p for p in local_sections if p.parent.parent.name.upper() in ticker_set]

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
from google.api_core import exceptions as gexc
from google.cloud import bigquery

GCP_PROJECT = os.environ["GCP_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_LOCATION = os.environ["BQ_LOCATION"]
client = bigquery.Client(project=GCP_PROJECT)

insights_id = f"{GCP_PROJECT}.{BQ_DATASET}.insights"
sections_id = f"{GCP_PROJECT}.{BQ_DATASET}.sections"
model_id = f"{GCP_PROJECT}.{BQ_DATASET}.gemini_pro_latest"

def insights_column_names_from_model() -> tuple[str, ...] | None:
    """Column names AI.GENERATE_TEXT returns for this remote model + sections row shape."""
    q = f"""
    SELECT * FROM AI.GENERATE_TEXT(
      MODEL `{model_id}`,
      (SELECT 'dummy' AS prompt, * FROM `{sections_id}` LIMIT 0),
      STRUCT(0.2 AS temperature, 8192 AS max_output_tokens)
    )
    LIMIT 0
    """
    try:
        dry = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = client.query(q, job_config=dry, location=BQ_LOCATION)
        job.result()
        sch = job.schema
        if sch:
            return tuple(f.name for f in sch)
    except Exception as e:
        print(f"⚠ Dry-run schema probe failed ({e}); running LIMIT 0 query once...")
    try:
        job = client.query(q, location=BQ_LOCATION)
        rows = job.result()
        job.reload()
        sch = job.schema or getattr(rows, "schema", None)
        if sch:
            return tuple(f.name for f in sch)
    except Exception as e:
        print(f"⚠ Could not probe AI.GENERATE_TEXT output schema: {e}")
    return None

def rebuild_insights_empty_canonical() -> None:
    q = f"""
    CREATE OR REPLACE TABLE `{insights_id}` AS
    SELECT * FROM AI.GENERATE_TEXT(
      MODEL `{model_id}`,
      (SELECT 'dummy' AS prompt, * FROM `{sections_id}` LIMIT 0),
      STRUCT(0.2 AS temperature, 8192 AS max_output_tokens)
    )
    LIMIT 0
    """
    client.query(q, location=BQ_LOCATION).result()

expected = insights_column_names_from_model()
try:
    cur = client.get_table(insights_id)
    actual = tuple(f.name for f in cur.schema)
except gexc.NotFound:
    actual = None

if expected is not None and actual == expected:
    pass
else:
    if expected is None:
        print("ℹ Rebuilding insights table (could not compare schemas; ensuring INSERT compatibility)")
    else:
        print(
            f"ℹ Aligning insights schema to current model output "
            f"({'missing table' if actual is None else f'{len(actual)} cols'} → {len(expected)} cols)"
        )
    rebuild_insights_empty_canonical()
    print(
        "✓ insights table recreated empty (05_extraction refills from sections per NOT EXISTS)"
    )

with open("05_extraction.sql", "r") as f:
    sql = f.read()
sql = sql.replace("sec_filings.", f"{BQ_DATASET}.").replace("sec_filings;", f"{BQ_DATASET};")
print("Executing 05_extraction.sql...")
job = client.query(sql, location=BQ_LOCATION)
job.result()
print("✓ Extraction complete")
PY

echo "7. Building graph tables in BigQuery..."
python3 - <<'PY'
import json
import os
from google.cloud import bigquery

GCP_PROJECT = os.environ["GCP_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_LOCATION = os.environ["BQ_LOCATION"]
GEMINI_MODEL = os.environ["GEMINI_MODEL"]

client = bigquery.Client(project=GCP_PROJECT)

# Ensure notebook 6.3 sees expected insights model-output columns.
insights_table = f"{GCP_PROJECT}.{BQ_DATASET}.insights"
t = client.get_table(insights_table)
schema_names = {f.name for f in t.schema}

if "ml_generate_text_result" not in schema_names:
    if "result" in schema_names:
        client.query(
            f"""
            CREATE OR REPLACE TABLE `{insights_table}` AS
            SELECT i.*, CAST(i.result AS STRING) AS ml_generate_text_result
            FROM `{insights_table}` AS i
            """,
            location=BQ_LOCATION,
        ).result()
        print("✓ Added ml_generate_text_result from result column")
    elif "ml_generate_text_llm_result" in schema_names:
        client.query(
            f"""
            CREATE OR REPLACE TABLE `{insights_table}` AS
            SELECT i.*, CAST(i.ml_generate_text_llm_result AS STRING) AS ml_generate_text_result
            FROM `{insights_table}` AS i
            """,
            location=BQ_LOCATION,
        ).result()
        print("✓ Added ml_generate_text_result from ml_generate_text_llm_result column")

# Some notebook logic references both column names.
t = client.get_table(insights_table)
schema_names = {f.name for f in t.schema}
if "ml_generate_text_llm_result" not in schema_names and "ml_generate_text_result" in schema_names:
    client.query(
        f"""
        CREATE OR REPLACE TABLE `{insights_table}` AS
        SELECT i.*, CAST(i.ml_generate_text_result AS STRING) AS ml_generate_text_llm_result
        FROM `{insights_table}` AS i
        """,
        location=BQ_LOCATION,
    ).result()
    print("✓ Added ml_generate_text_llm_result mirror column")

# Execute the same BigQuery-only graph build cell used in pipeline.ipynb (cell id: zz1-q8mT2It0)
with open("pipeline.ipynb", "r", encoding="utf-8") as f:
    nb = json.load(f)

cell = None
for c in nb.get("cells", []):
    if c.get("metadata", {}).get("id") == "zz1-q8mT2It0":
        cell = c
        break
if cell is None:
    raise RuntimeError("Could not find pipeline cell id zz1-q8mT2It0 for BigQuery graph build.")

code = "".join(cell.get("source", []))
ctx = {
    "__name__": "__main__",
    "os": os,
    "client": client,
    "GCP_PROJECT": GCP_PROJECT,
    "BQ_DATASET": BQ_DATASET,
    "BQ_LOCATION": BQ_LOCATION,
    "GEMINI_MODEL": GEMINI_MODEL,
}
exec(code, ctx, ctx)
print("✓ BigQuery-only graph table build complete")
PY

echo "8. Creating Property Graph DDL..."
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
    f"CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BQ_DATASET}.edges_section_belongs_to_document` (source_node STRING, target_node STRING)",
]
for ddl in fallback_ddls:
    client.query(ddl, location=BQ_LOCATION).result()

# Backfill Section -> Document edge from Document -> Section if needed.
section_belongs_id = f"{GCP_PROJECT}.{BQ_DATASET}.edges_section_belongs_to_document"
try:
    t = client.get_table(section_belongs_id)
    row_count = list(client.query(
        f"SELECT COUNT(1) AS n FROM `{section_belongs_id}`",
        location=BQ_LOCATION,
    ).result())[0]["n"]
except Exception:
    row_count = 0

if int(row_count) == 0:
    client.query(
        f"""
        CREATE OR REPLACE TABLE `{section_belongs_id}` AS
        SELECT DISTINCT target_node AS source_node, source_node AS target_node
        FROM `{GCP_PROJECT}.{BQ_DATASET}.edges_doc_contains_section`
        """,
        location=BQ_LOCATION,
    ).result()
    print("✓ Backfilled edges_section_belongs_to_document from edges_doc_contains_section")

# Normalize key columns to STRING before creating property graph
key_columns = [
    ("nodes_company", ["id"]),
    ("nodes_market", ["id"]),
    ("nodes_risk", ["id"]),
    ("nodes_opportunity", ["id"]),
    ("nodes_competitor", ["id"]),
    ("nodes_reference", ["id"]),
    ("nodes_document", ["id"]),
    ("nodes_section", ["id"]),
    ("nodes_normalized_competitor", ["id"]),
    ("nodes_geographic_region", ["id"]),
    ("nodes_market_category", ["id"]),
    ("nodes_risk_category", ["id"]),
    ("edges_entering", ["source_node", "target_node"]),
    ("edges_expanding", ["source_node", "target_node"]),
    ("edges_exiting", ["source_node", "target_node"]),
    ("edges_faces_risk", ["source_node", "target_node"]),
    ("edges_pursuing", ["source_node", "target_node"]),
    ("edges_competes", ["source_node", "target_node"]),
    ("edges_market_has_reference", ["source_node", "target_node"]),
    ("edges_risk_has_reference", ["source_node", "target_node"]),
    ("edges_opportunity_has_reference", ["source_node", "target_node"]),
    ("edges_competitor_has_reference", ["source_node", "target_node"]),
    ("edges_filed", ["source_node", "target_node"]),
    ("edges_doc_contains_section", ["source_node", "target_node"]),
    ("edges_section_belongs_to_document", ["source_node", "target_node"]),
    ("edges_section_contains_ref", ["source_node", "target_node"]),
    ("edges_instance_of", ["source_node", "target_node"]),
    ("edges_subsidiary_of", ["source_node", "target_node"]),
    ("edges_has_risk_category", ["source_node", "target_node"]),
    ("edges_in_region", ["source_node", "target_node"]),
    ("edges_in_product_category", ["source_node", "target_node"]),
    ("edges_in_market_category", ["source_node", "target_node"]),
]

for tbl, cols in key_columns:
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{tbl}"
    try:
        t = client.get_table(table_id)
    except Exception:
        continue
    select_exprs = []
    needs_rebuild = False
    for f in t.schema:
        if f.name in cols and f.field_type != "STRING":
            select_exprs.append(f"CAST(`{f.name}` AS STRING) AS `{f.name}`")
            needs_rebuild = True
        else:
            select_exprs.append(f"`{f.name}`")
    if needs_rebuild:
        client.query(
            f"CREATE OR REPLACE TABLE `{table_id}` AS SELECT {', '.join(select_exprs)} FROM `{table_id}`",
            location=BQ_LOCATION,
        ).result()
        print(f"Rebuilt {tbl} with STRING key columns")

with open("06_create_property_graph_ddl.sql", "r") as f:
    sql = f.read()
sql = sql.replace("sec_filings.", f"{BQ_DATASET}.").replace("sec_filings;", f"{BQ_DATASET};")
job = client.query(sql, location=BQ_LOCATION)
job.result()
print("✓ Property graph DDL applied")
PY

echo "Pipeline complete."
