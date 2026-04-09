"""
Vertex AI extraction provider using the Google GenAI SDK.

Uses google.genai with vertexai=True and location='global' for Gemini 3.x models.
Runs concurrent async requests (not batch API, which doesn't support 3.x yet).

Requires: google-genai (included in google-cloud-aiplatform)
"""

import asyncio
import json
import re
import time
from pathlib import Path

from google import genai
from google.genai.types import GenerateContentConfig

from config import (
    EXTRACTION_PROMPT,
    MAX_CONCURRENT_REQUESTS,
    MAX_CONTENT_CHARS,
    MAX_TOKENS,
    TEMPERATURE,
    VERTEX_PROJECT,
    get_model_id,
)
from extract import load_sections


def _build_meta(rec: dict, model_name: str) -> dict:
    """Extract metadata fields from a section record."""
    return {
        "company": rec.get("company"),
        "company_name": rec.get("company_name"),
        "cik": rec.get("cik"),
        "sic": rec.get("sic"),
        "irs_number": rec.get("irs_number"),
        "state_of_inc": rec.get("state_of_inc"),
        "org_name": rec.get("org_name"),
        "sec_file_number": rec.get("sec_file_number"),
        "film_number": rec.get("film_number"),
        "business_street_1": rec.get("business_street_1"),
        "business_street_2": rec.get("business_street_2"),
        "business_city": rec.get("business_city"),
        "business_state": rec.get("business_state"),
        "business_zip": rec.get("business_zip"),
        "business_phone": rec.get("business_phone"),
        "mail_street_1": rec.get("mail_street_1"),
        "mail_street_2": rec.get("mail_street_2"),
        "mail_city": rec.get("mail_city"),
        "mail_state": rec.get("mail_state"),
        "mail_zip": rec.get("mail_zip"),
        "filing_url": rec.get("filing_url"),
        "year": rec.get("year"),
        "section_id": rec.get("section_id"),
        "model": model_name,
    }


async def call_vertex(
    client: genai.Client,
    model_id: str,
    record: dict,
    model_name: str,
    semaphore: asyncio.Semaphore,
) -> dict:
    """Call Vertex AI GenAI for a single section."""
    prompt = EXTRACTION_PROMPT.format(
        section_id=record.get("section_id", ""),
        content=(record.get("content") or "")[:MAX_CONTENT_CHARS],
    )
    meta = _build_meta(record, model_name)

    config = GenerateContentConfig(
        temperature=TEMPERATURE,
        max_output_tokens=MAX_TOKENS,
    )

    async with semaphore:
        try:
            resp = await client.aio.models.generate_content(
                model=model_id,
                contents=prompt,
                config=config,
            )
            raw_text = resp.text or ""
            # Strip markdown fences
            raw_text = re.sub(r"^```json\s*\n?", "", raw_text)
            raw_text = re.sub(r"\n?```\s*$", "", raw_text)
            meta["result"] = raw_text
            meta["error"] = None
        except Exception as e:
            meta["result"] = None
            meta["error"] = str(e)
            print(f"  ERROR {meta['company']}/{meta['year']}/{meta['section_id']}: {e}")

    return meta


def _load_existing_keys(out_file: Path) -> set:
    """Load already-processed (company, year, section_id) keys from insights.jsonl."""
    keys = set()
    if out_file.exists():
        with open(out_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if rec.get("result"):  # only count successful extractions
                    keys.add((rec.get("company"), rec.get("year"), rec.get("section_id")))
    return keys


async def run_vertex_extraction(
    data_dir: str,
    output_dir: str,
    model_name: str,
    ticker: str = None,
    year: int = None,
):
    """Run extraction via Vertex AI GenAI SDK (async concurrent).

    Incremental: skips sections already in insights.jsonl and appends new results.
    """
    print(f"Loading sections from {data_dir} ...")
    records = load_sections(data_dir, ticker=ticker, year=year)
    print(f"Found {len(records)} total sections")

    extraction_dir = Path(output_dir) / "extractions"
    extraction_dir.mkdir(parents=True, exist_ok=True)
    out_file = extraction_dir / "insights.jsonl"

    # Skip already-processed sections
    existing = _load_existing_keys(out_file)
    if existing:
        print(f"Already processed: {len(existing)} sections")
    pending = [
        r for r in records
        if (r.get("company"), r.get("year"), r.get("section_id")) not in existing
    ]
    print(f"New sections to process: {len(pending)}")

    if not pending:
        print("Nothing new to extract.")
        return

    model_id = get_model_id(model_name, "vertex")
    print(f"Model: {model_name} -> {model_id}")
    print(f"Concurrency: {MAX_CONCURRENT_REQUESTS}")

    client = genai.Client(
        vertexai=True,
        project=VERTEX_PROJECT,
        location="global",
    )

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    tasks = [
        call_vertex(client, model_id, rec, model_name, semaphore)
        for rec in pending
    ]
    total = len(tasks)
    done = 0

    # Append results incrementally to insights.jsonl
    with open(out_file, "a") as f:
        for coro in asyncio.as_completed(tasks):
            result = await coro
            f.write(json.dumps(result, default=str) + "\n")
            f.flush()
            done += 1
            status = "OK" if result.get("result") else "FAIL"
            print(f"  [{done}/{total}] {result['company']}/{result['year']}/{result['section_id']} - {status}")

    total_existing = _load_existing_keys(out_file)
    print(f"\nDone. {done} new sections processed. Total in file: {len(total_existing)}")


def run_vertex_batch(
    data_dir: str,
    output_dir: str,
    model_name: str,
    ticker: str = None,
    year: int = None,
    poll_interval: int = 30,
):
    """Entry point called by run.py. Runs async extraction."""
    asyncio.run(run_vertex_extraction(
        data_dir=data_dir,
        output_dir=output_dir,
        model_name=model_name,
        ticker=ticker,
        year=year,
    ))


def main():
    import argparse

    from config import DATA_DIR, MODEL_NAME, get_output_dir

    parser = argparse.ArgumentParser(description="Vertex AI extraction")
    parser.add_argument("--ticker", type=str, help="Process single ticker")
    parser.add_argument("--year", type=int, help="Process single year")
    parser.add_argument("--model", type=str, default=None, help="Model name")
    parser.add_argument("--data-dir", type=str, default=DATA_DIR)
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    model = args.model or MODEL_NAME
    output_dir = args.output_dir or get_output_dir(model)

    asyncio.run(run_vertex_extraction(
        data_dir=args.data_dir,
        output_dir=output_dir,
        model_name=model,
        ticker=args.ticker,
        year=args.year,
    ))


if __name__ == "__main__":
    main()
