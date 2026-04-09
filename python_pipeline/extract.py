"""
Step 1: LLM-based entity extraction from SEC filing sections.

OpenRouter / local provider (async HTTP). For Vertex AI batch, see extract_vertex.py.

Reads sections.jsonl files from data_2021-2025/, sends target sections
to the configured LLM, and saves raw JSON results to output/{model}/extractions/.
"""

import asyncio
import json
import re
import sys
from pathlib import Path

import httpx

from config import (
    DATA_DIR,
    EXTRACTION_PROMPT,
    LOCAL_BASE_URL,
    MAX_CONCURRENT_REQUESTS,
    MAX_CONTENT_CHARS,
    MAX_TOKENS,
    MODEL_NAME,
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    PROVIDER,
    TARGET_SECTIONS,
    TEMPERATURE,
    get_model_id,
    get_output_dir,
)


def get_llm_config(model_name: str = None):
    """Return (base_url, model_id, headers) based on PROVIDER."""
    model_name = model_name or MODEL_NAME
    if PROVIDER == "openrouter":
        if not OPENROUTER_API_KEY:
            sys.exit("ERROR: OPENROUTER_API_KEY env var is required for openrouter provider")
        return (
            OPENROUTER_BASE_URL,
            get_model_id(model_name, "openrouter"),
            {
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
            },
        )
    elif PROVIDER == "local":
        return (
            LOCAL_BASE_URL,
            get_model_id(model_name, "local"),
            {"Content-Type": "application/json"},
        )
    else:
        sys.exit(f"ERROR: extract.py supports 'openrouter' or 'local', got '{PROVIDER}'. Use extract_vertex.py for vertex.")


def load_sections(data_dir: str, ticker=None, year: int = None):
    """Load all sections.jsonl records, optionally filtered by ticker(s)/year.

    ticker can be a single string or a list of strings.
    """
    data_path = Path(data_dir)
    # Normalize ticker filter to a set
    if isinstance(ticker, str):
        ticker_set = {ticker}
    elif ticker:
        ticker_set = set(ticker)
    else:
        ticker_set = None

    records = []
    for ticker_dir in sorted(data_path.iterdir()):
        if not ticker_dir.is_dir():
            continue
        if ticker_set and ticker_dir.name not in ticker_set:
            continue
        for year_dir in sorted(ticker_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            if year and year_dir.name != str(year):
                continue
            jsonl_file = year_dir / "sections.jsonl"
            if not jsonl_file.exists():
                continue
            with open(jsonl_file) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    record = json.loads(line)
                    if record.get("section_id") in TARGET_SECTIONS:
                        records.append(record)
    return records


async def call_llm(client: httpx.AsyncClient, base_url: str, model: str,
                   headers: dict, record: dict, model_name: str,
                   semaphore: asyncio.Semaphore):
    """Call LLM for a single section and return record with result."""
    prompt = EXTRACTION_PROMPT.format(
        section_id=record["section_id"],
        content=record.get("content", "")[:MAX_CONTENT_CHARS],
    )

    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": TEMPERATURE,
        "max_tokens": MAX_TOKENS,
    }

    meta = {
        "company": record.get("company"),
        "company_name": record.get("company_name"),
        "cik": record.get("cik"),
        "sic": record.get("sic"),
        "irs_number": record.get("irs_number"),
        "state_of_inc": record.get("state_of_inc"),
        "org_name": record.get("org_name"),
        "sec_file_number": record.get("sec_file_number"),
        "film_number": record.get("film_number"),
        "business_street_1": record.get("business_street_1"),
        "business_street_2": record.get("business_street_2"),
        "business_city": record.get("business_city"),
        "business_state": record.get("business_state"),
        "business_zip": record.get("business_zip"),
        "business_phone": record.get("business_phone"),
        "mail_street_1": record.get("mail_street_1"),
        "mail_street_2": record.get("mail_street_2"),
        "mail_city": record.get("mail_city"),
        "mail_state": record.get("mail_state"),
        "mail_zip": record.get("mail_zip"),
        "filing_url": record.get("filing_url"),
        "year": record.get("year"),
        "section_id": record.get("section_id"),
        "model": model_name,
    }

    async with semaphore:
        try:
            resp = await client.post(
                f"{base_url}/chat/completions",
                json=payload,
                headers=headers,
                timeout=120.0,
            )
            resp.raise_for_status()
            data = resp.json()
            raw_text = data["choices"][0]["message"]["content"]
            # Strip markdown code fences if present
            raw_text = re.sub(r"^```json\s*\n?", "", raw_text)
            raw_text = re.sub(r"\n?```\s*$", "", raw_text)
            meta["result"] = raw_text
            meta["error"] = None
        except Exception as e:
            meta["result"] = None
            meta["error"] = str(e)
            print(f"  ERROR {meta['company']}/{meta['year']}/{meta['section_id']}: {e}")

    return meta


async def run_extraction(data_dir: str, output_dir: str, model_name: str = None,
                         ticker: str = None, year: int = None):
    """Run extraction for all matching sections."""
    model_name = model_name or MODEL_NAME

    print(f"Loading sections from {data_dir} ...")
    records = load_sections(data_dir, ticker=ticker, year=year)
    print(f"Found {len(records)} sections to process")

    if not records:
        print("Nothing to extract.")
        return

    base_url, model_id, headers = get_llm_config(model_name)
    print(f"Using provider={PROVIDER}, model={model_name} ({model_id})")

    extraction_dir = Path(output_dir) / "extractions"
    extraction_dir.mkdir(parents=True, exist_ok=True)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    results = []

    async with httpx.AsyncClient() as client:
        tasks = [
            call_llm(client, base_url, model_id, headers, rec, model_name, semaphore)
            for rec in records
        ]
        total = len(tasks)
        done = 0
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
            done += 1
            status = "OK" if result.get("result") else "FAIL"
            print(f"  [{done}/{total}] {result['company']}/{result['year']}/{result['section_id']} - {status}")

    # Save results as JSONL
    out_file = extraction_dir / "insights.jsonl"
    with open(out_file, "w") as f:
        for r in results:
            f.write(json.dumps(r, default=str) + "\n")

    ok_count = sum(1 for r in results if r.get("result"))
    print(f"\nDone. {ok_count}/{len(results)} successful. Saved to {out_file}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Extract entities via OpenRouter/local LLM")
    parser.add_argument("--ticker", type=str, help="Process single ticker (e.g. AAPL)")
    parser.add_argument("--year", type=int, help="Process single year (e.g. 2024)")
    parser.add_argument("--model", type=str, default=None, help="Model name from registry")
    parser.add_argument("--data-dir", type=str, default=DATA_DIR)
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    model = args.model or MODEL_NAME
    output_dir = args.output_dir or get_output_dir(model)

    asyncio.run(run_extraction(args.data_dir, output_dir,
                               model_name=model,
                               ticker=args.ticker, year=args.year))


if __name__ == "__main__":
    main()
