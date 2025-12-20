#!/bin/bash
set -e

# Parse optional ticker argument (default to *)
TICKER=${1:-*}

echo "1. Uploading JSONL files to GCS..."
gsutil -m rsync -r data/json gs://kineviz_fortune500_sec_filing/json

# --- HELPER FUNCTION: Process a batch of tickers ---
process_batch() {
    local uris="$1"
    
    if [ -z "$uris" ]; then
        return
    fi
    
    # Hide output for cleaner progress bar, or log to file? 
    # For now, let's keep it visible but maybe indented or just rely on the bar updates between steps.
    # Actually, bq output might mess up the bar. Let's just print a status line.
    
    echo "  -> Loading batch into staging table..."
    bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect --replace sec_filings.sections_staging "$uris" > /dev/null 2>&1
    
    echo "  -> Running AI Extraction on batch..."
    cat 04_extraction.sql | bq query --use_legacy_sql=false --location=US > /dev/null 2>&1
    
    echo "  -> Archiving batch to master table..."
    bq query --use_legacy_sql=false --location=US "INSERT INTO sec_filings.sections SELECT * FROM sec_filings.sections_staging" > /dev/null 2>&1
}

draw_progress_bar() {
    local processed=$1
    local total=$2
    local width=50
    local percent=$((processed * 100 / total))
    local filled=$((width * percent / 100))
    local empty=$((width - filled))
    
    # Create the bar string
    local bar=$(printf "%${filled}s" | tr ' ' '=')
    local spaces=$(printf "%${empty}s" | tr ' ' ' ')
    
    printf "\rProgress: [%s%s] %d%% (%d/%d)" "$bar" "$spaces" "$percent" "$processed" "$total"
}
# ---------------------------------------------------

if [ "$TICKER" == "*" ]; then
  echo "Loading ALL data (Batched mode)..."
  
  # 0. Initialize Tables (Create IF NOT EXISTS or CLEAR for full run)
  echo "0. Initializing Destination Tables..."
  cat 03_init_tables.sql | bq query --use_legacy_sql=false --location=US

  echo "  Discovery: Finding all company directories..."
  COMPANIES=$(find data/json -mindepth 1 -maxdepth 1 -type d)
  TOTAL_COMPANIES=$(echo "$COMPANIES" | wc -w | xargs) # xargs trims whitespace
  
  BATCH_SIZE=10
  CURRENT_URIS=""
  COUNT=0
  PROCESSED_TOTAL=0
  
  echo "Starting Batch Processing of $TOTAL_COMPANIES companies..."
  draw_progress_bar 0 "$TOTAL_COMPANIES"
  
  for company_dir in $COMPANIES; do
      company_uris=$(find "$company_dir" -name "sections.jsonl" | sed 's|^data/json/|gs://kineviz_fortune500_sec_filing/json/|' | tr '\n' ',')
      
      if [ -n "$company_uris" ]; then
          CURRENT_URIS="${CURRENT_URIS}${company_uris}"
          ((COUNT++))
          
          if [ "$COUNT" -ge "$BATCH_SIZE" ]; then
              CURRENT_URIS=${CURRENT_URIS%,}
              
              # Newline to not overwrite the bar with verbose output if any
              echo "" 
              process_batch "$CURRENT_URIS"
              
              # Update progress
              ((PROCESSED_TOTAL+=COUNT))
              draw_progress_bar "$PROCESSED_TOTAL" "$TOTAL_COMPANIES"
              
              CURRENT_URIS=""
              COUNT=0
          fi
      fi
  done
  
  # Process remaining companies
  if [ -n "$CURRENT_URIS" ]; then
      CURRENT_URIS=${CURRENT_URIS%,}
      echo ""
      process_batch "$CURRENT_URIS"
      ((PROCESSED_TOTAL+=COUNT))
      draw_progress_bar "$PROCESSED_TOTAL" "$TOTAL_COMPANIES"
  fi
  echo "" # Done with bar

else
  echo "Loading data for $TICKER (Incremental mode)..."
  
  bq query --use_legacy_sql=false "DELETE FROM sec_filings.sections WHERE company = '$TICKER'"
  bq query --use_legacy_sql=false "DELETE FROM sec_filings.insights WHERE company = '$TICKER'"
  
  bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect --replace sec_filings.sections_staging "gs://kineviz_fortune500_sec_filing/json/$TICKER/*/sections.jsonl"
  
  cat 04_extraction.sql | bq query --use_legacy_sql=false --location=US
  
  bq query --use_legacy_sql=false --location=US "INSERT INTO sec_filings.sections SELECT * FROM sec_filings.sections_staging"
fi

echo "4. Transforming to Graph Edges (sec_filings.graph_edges)..."
cat 05_create_graph.sql | bq query --use_legacy_sql=false --location=US

echo "5. Preparing Node/Edge Tables..."
cat 06_prepare_property_graph.sql | bq query --use_legacy_sql=false --location=US

echo "6. Creating Property Graph DDL..."
cat 07_create_property_graph_ddl.sql | bq query --use_legacy_sql=false --location=US

echo "Pipeline Complete."
