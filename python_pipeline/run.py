#!/usr/bin/env python3
"""
SEC Filing Entity Extraction Pipeline — Orchestrator

Usage:
  # Full pipeline with Vertex AI batch (default)
  python run.py --model gemini-2.5-flash

  # Single ticker via OpenRouter for testing
  LLM_PROVIDER=openrouter python run.py --model gemini-2.5-flash --ticker AAPL --year 2024

  # Compare models
  python run.py --model gemini-2.5-pro
  python run.py --model gemini-2.5-flash-lite

  # Skip extraction (reuse existing insights.jsonl)
  python run.py --model gemini-2.5-flash --skip-extract

  # Only load into KuzuDB (reuse existing parquet)
  python run.py --model gemini-2.5-flash --only-load

Output structure:
  output/{model_name}/extractions/insights.jsonl
  output/{model_name}/parquet/*.parquet
  sec_filings_db/{model_name}/           (KuzuDB)

Environment variables:
  LLM_PROVIDER=vertex|openrouter|local
  MODEL_NAME=gemini-2.5-flash            (default model)
  VERTEX_PROJECT=...                     (required for vertex)
  GCS_BUCKET=gs://...                    (required for vertex)
  OPENROUTER_API_KEY=sk-or-...           (required for openrouter)
  LOCAL_LLM_URL=http://...               (for local provider)
"""

import argparse
import asyncio
import sys
from pathlib import Path

from config import DATA_DIR, MODEL_NAME, PROVIDER, get_kuzu_db_path, get_output_dir


def main():
    parser = argparse.ArgumentParser(
        description="SEC Filing Entity Extraction Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--model", type=str, default=None,
                        help="Model name from registry (e.g. gemini-2.5-flash)")
    parser.add_argument("--ticker", type=str, nargs="+",
                        help="One or more tickers (e.g. AAPL AMZN GOOGL)")
    parser.add_argument("--year", type=int, help="Process single year (e.g. 2024)")
    parser.add_argument("--data-dir", type=str, default=DATA_DIR)
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Override output dir (default: output/{model}/)")
    parser.add_argument("--db-path", type=str, default=None,
                        help="Override KuzuDB path (default: sec_filings_db/{model}/)")
    parser.add_argument("--skip-extract", action="store_true",
                        help="Skip LLM extraction, reuse existing insights.jsonl")
    parser.add_argument("--only-load", action="store_true",
                        help="Only load existing parquet into KuzuDB")
    parser.add_argument("--reset-db", action="store_true",
                        help="Delete and recreate KuzuDB")
    parser.add_argument("--poll-interval", type=int, default=60,
                        help="Vertex AI batch polling interval in seconds")
    args = parser.parse_args()

    model = args.model or MODEL_NAME
    output_dir = args.output_dir or get_output_dir(model)
    db_path = args.db_path or get_kuzu_db_path(model)

    print(f"Pipeline: model={model}, provider={PROVIDER}")
    print(f"  Output:  {output_dir}")
    print(f"  KuzuDB:  {db_path}")

    # ── Step 1: Extract ─────────────────────────────────────────────
    if not args.skip_extract and not args.only_load:
        print("\n" + "=" * 60)
        print(f"STEP 1: LLM Entity Extraction ({PROVIDER})")
        print("=" * 60)

        if PROVIDER == "vertex":
            from extract_vertex import run_vertex_batch
            run_vertex_batch(
                data_dir=args.data_dir,
                output_dir=output_dir,
                model_name=model,
                ticker=args.ticker,
                year=args.year,
                poll_interval=args.poll_interval,
            )
        elif PROVIDER in ("openrouter", "local"):
            from extract import run_extraction
            asyncio.run(run_extraction(
                args.data_dir, output_dir,
                model_name=model,
                ticker=args.ticker, year=args.year,
            ))
        else:
            sys.exit(f"Unknown provider: {PROVIDER}")
    else:
        print("Skipping extraction (using existing insights.jsonl)")

    # ── Step 2: Transform ───────────────────────────────────────────
    if not args.only_load:
        print("\n" + "=" * 60)
        print("STEP 2: Transform to Node/Edge Parquet")
        print("=" * 60)
        from transform import build_tables, parse_insights, save_tables

        insights_file = Path(output_dir) / "extractions" / "insights.jsonl"
        if not insights_file.exists():
            sys.exit(f"ERROR: No extraction results at {insights_file}. Step 1 may have failed.")

        records = parse_insights(output_dir)
        ok = sum(1 for r in records if r.get("parsed"))
        print(f"  {ok}/{len(records)} records with valid JSON")

        tables = build_tables(records)
        save_tables(tables, output_dir)

        print("\nTable summary:")
        for name, df in tables.items():
            if len(df) > 0:
                print(f"  {name}: {len(df)} rows")
    else:
        print("Skipping transform (using existing parquet)")

    # ── Step 3: Load into KuzuDB ────────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 3: Load into KuzuDB")
    print("=" * 60)
    from load_kuzu import create_database

    parquet_dir = Path(output_dir) / "parquet"
    if not parquet_dir.exists():
        sys.exit(f"ERROR: No parquet directory at {parquet_dir}")

    create_database(db_path, str(parquet_dir), reset=args.reset_db)

    print("\n" + "=" * 60)
    print("Pipeline complete!")
    print(f"  Model:   {model}")
    print(f"  KuzuDB:  {db_path}")
    print(f"  Parquet: {parquet_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()
