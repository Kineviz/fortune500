# Fortune 500 SEC Filings - Manual Scripts Guide

This guide provides instructions for running the individual components of the pipeline from the command line, as an alternative to using the Jupyter Notebook.

## 1. SEC EDGAR Scraper (`01_scraper.py`)

Run the `01_scraper.py` script from the command line.

### Basic Usage

Download filings for the top 10 companies for the current year:
```bash
python 01_scraper.py --limit 10
```

### Crawl Specific Company (Skips list.csv)

**By Ticker:**
```bash
python 01_scraper.py --ticker AAPL --year 2024
```

**By CIK:**
```bash
python 01_scraper.py --cik 0000320193 --year 2024
```

### Advanced Usage

**Filter by Year:**
Download filings for the top 20 companies for the year 2023:
```bash
python 01_scraper.py --limit 20 --year 2023
```

**Filter by Last N Years:**
Download filings for the top 50 companies for the last 5 years:
```bash
python 01_scraper.py --limit 50 --last-n-years 5
```

**Dry Run (Simulation):**
See what would be downloaded without actually downloading/saving:
```bash
python 01_scraper.py --limit 1 --year 2024 --dry-run
```

**Custom Output Directory:**
Save filings to a specific folder:
```bash
python 01_scraper.py --limit 10 --output-dir my_custom_folder
```
(Default is `sec-edgar-filings`)

**Concurrency:**
Adjust the number of worker threads (default is 5):
```bash
python 01_scraper.py --workers 10
```

### All Parameters Example

Run with all options combined:
```bash
python 01_scraper.py --limit 50 --year 2024 --workers 20 --output-dir /tmp/sec_data --dry-run
```

### Scraper Output Structure

Filings are saved in the following directory structure:

```
data/
├── sgml
    ├── [Ticker]
    │   ├── 10-K
    │   │   └── [Accession Number]
    │   │       └── full-submission.txt
    │   └── 10-Q
    │       └── [Accession Number]
    │           └── full-submission.txt
```

Example:
```
data/
├── WMT
│   ├── 10-K
│   │   └── 0000104169-24-000056
│   │       └── full-submission.txt
...
```


## 2. Filing Parser (`02_parser.py`)

Convert the raw SGML filings into clean, readable Markdown documents.

### Features
- **SGML to Markdown**: Converts messy SGML/HTML into structured Markdown.
- **Strict Filtering**: Extracts *only* the main filing (`full-submission.md`), proper images (`.jpg`, `.gif`), and spreadsheets (`.xlsx`, `.csv`). Filtering out XML trash and other noise.
- **Parallel Processing**: Uses multiple CPU cores for fast parallel parsing (`--workers`).
- **Resume Capability**: Automatically skips filings that have already been processed (`full-submission.md` exists).
- **SEC Link**: Adds a direct link to the official SEC filing at the top of the document.

### Usage

Run `02_parser.py` to process the downloaded filings.

**Basic Usage:**
Process all filings in `data/sgml` and save to `data/markdown`:
```bash
python 02_parser.py --input_base data/sgml --output_base data/markdown
```

**Parallel Processing:**
Use 8 worker processes to speed up parsing:
```bash
python 02_parser.py --workers 8
```

**Custom Paths:**
```bash
python 02_parser.py --input_base /path/to/raw_filings --output_base /path/to/clean_markdown
```

### Parser Output Structure

The scraper produces `data/sgml/`, and the parser produces `data/markdown/`.

```
data/markdown/
├── [Ticker]
│   ├── 10-K
│   │   └── [Accession Number]
│       │   ├── full-submission.md   (Main Document)
│       │   ├── Financial_Report.xlsx
│       │   ├── graphic1.jpg
│       │   └── ...
```

## 3. Section Extraction (`03_extract_sections.py`)

Extract structured sections (Item 1, Item 1A, Item 7, etc.) from the raw `full-submission.txt` (or cleaned HTML) into JSONL format. This prepares the data for AI processing.

### Usage

Run `03_extract_sections.py` to process the SGML/Text filings.

**Basic Usage:**
Process all filings in `data/sgml` and save to `data/json`:
```bash
python 03_extract_sections.py
```

**Specific Ticker/Year:**
Process only AAPL for 2023:
```bash
python 03_extract_sections.py --ticker AAPL --year 2023
```

**Custom Paths:**
```bash
python 03_extract_sections.py --input_base /path/to/sgml --output_base /path/to/output_json
```

### Extractor Output Structure

The script produces `sections.jsonl` files:

```
data/json/
├── [Ticker]
│   ├── [Year]
│   │   └── sections.jsonl
```

Each line in `sections.jsonl` is a JSON object containing:
- `filing_id`: Original filename
- `company`: Ticker symbol
- `year`: Filing year
- `section_id`: The extracted section header (e.g., "Item 1. Business")
- `content`: The text content of that section

## 4. Parquet Pipeline & BigQuery Graph

A complete workflow to transform unstructured 10-K text into a queried Property Graph in BigQuery, using Vertex AI for insight extraction and Python for entity normalization.

### Overview

This pipeline leverages **Gemini 2.5 Pro** directly within BigQuery to extract markets, risks, and competitors. It then exports these insights to run the Python entity normalization locally (e.g., resolving "Walmart Inc." to "Walmart", categorizing risks). Finally, it loads the finalized Parquet files back into BigQuery to construct a compliant SQL/PGQ Property Graph.

### Full Pipeline Execution (Helper Script)

You can use the helper script `00_run_full_pipeline.sh` for an end-to-end execution.

**1. Full Load:**
This runs the scraper, extraction, python parquet pipeline, entity normalization, GCS upload, BigQuery load, and graph creation.
```bash
./00_run_full_pipeline.sh
```

**2. Incremental Load (Specific Company):**
This runs the pipeline for a specific company (e.g., `AAPL`).
```bash
./00_run_full_pipeline.sh AAPL
```

### Alternative: Manual Step-by-Step

```bash
# 1. Upload sections to GCS and load into BigQuery
gsutil -m rsync -r data/json gs://your_bucket/json
cat 04_init_tables.sql | bq query --use_legacy_sql=false --location=US
# (Load sections.jsonl into BigQuery using python script or bq load)

# 2. Extract Insights with AI in BigQuery
cat 05_extraction.sql | bq query --use_legacy_sql=false --location=US

# 3. Export Insights to GCS and download locally
bq extract --destination_format=NEWLINE_DELIMITED_JSON your_project:your_dataset.insights gs://your_bucket/parquets/gemini-3-flash/insights.jsonl
gsutil cp gs://your_bucket/parquets/gemini-3-flash/insights.jsonl python_pipeline/output/gemini-3-flash/extractions/insights.jsonl

# 4. Run Python Parquet Pipeline (Transformation & Normalization)
cd python_pipeline
uv run python transform.py
uv run python entity_normalization/resolve_competitors.py
uv run python entity_normalization/categorize_risks.py
uv run python entity_normalization/categorize_markets.py
uv run python entity_normalization/categorize_competitor_markets.py
cd ..

# 5. Upload Parquet files to GCS and load into BigQuery
gsutil -m rsync -r python_pipeline/output/gemini-3-flash/parquet gs://your_bucket/parquets/gemini-3-flash/parquet
# (See load_bq_temp.py logic inside 00_run_full_pipeline.sh for loading)

# 6. Create Property Graph Object
cat 06_create_property_graph_ddl.sql | bq query --use_legacy_sql=false --location=US
```

### Querying the Graph (GQL)

Once created, you can query the graph using standard GQL directly in BigQuery:

```sql
GRAPH sec_filings.SecGraph
MATCH (c:Company)-[:ENTERING]->(m:Market)
WHERE m.year = 2020
RETURN c.id, m.id, m.evidence
```
