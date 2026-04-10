#!/usr/bin/env python3
"""
Categorize markets from nodes_market.parquet using LLM.

Reads unique market labels, sends them in batches to Gemini,
and writes an enriched parquet with geographic_region, sector,
and product_category columns.

Usage:
    python categorize_markets.py                          # default: gemini-2.0-flash
    python categorize_markets.py --model gemini-2.5-flash-lite
    python categorize_markets.py --batch-size 30
    python categorize_markets.py --dry-run
"""

import argparse
import asyncio
import csv
import json
import re
import time
from pathlib import Path

import pandas as pd
from google import genai
from google.genai.types import GenerateContentConfig

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import VERTEX_PROJECT, get_model_id

# ── Paths ──────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
SECTORS_CSV = SCRIPT_DIR / "data" / "competitor_sectors.csv"  # reuse same 13 sectors
REGIONS_CSV = SCRIPT_DIR / "data" / "market_geographic_regions.csv"
PRODUCTS_CSV = SCRIPT_DIR / "data" / "market_product_categories.csv"
CACHE_FILE = SCRIPT_DIR / "data" / "market_label_categories_cache.json"


def load_csv(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_system_prompt(
    sectors: list[dict], regions: list[dict], products: list[dict]
) -> str:
    lines = [
        "You are a market classification expert. For each market label from SEC 10-K filings, assign three dimensions.",
        "",
        "## 1. Geographic Region",
        "Assign one region, or null if the market is not geographic.",
        "",
    ]
    for r in regions:
        lines.append(f"- **{r['geographic_region']}**: {r['description']}")
    lines += [
        "",
        "## 2. Sector",
        "Assign one sector, or null if unclear.",
        "",
    ]
    for s in sectors:
        lines.append(f"- **{s['sector']}**: {s['description']}")
    lines += [
        "",
        "## 3. Product Category",
        "Assign one product category, or null if the market is too broad or purely geographic.",
        "",
    ]
    for p in products:
        lines.append(f"- **{p['product_category']}**: {p['description']}")
    lines += [
        "",
        "## Rules",
        "- A market label can have 0-3 dimensions assigned.",
        "- Country/region names (Russia, China, India) get geographic_region but usually null sector and null product_category.",
        '- Specific markets (Renewable Diesel, Cloud Computing, Biosimilars) get sector and/or product_category.',
        '- Broad labels (Emerging Markets, International Operations) get geographic_region only.',
        "- Return ONLY valid JSON — an array of objects.",
        '- Each object: {"label": "<exact label>", "geographic_region": "...|null", "sector": "...|null", "product_category": "...|null"}',
        "- Use exact names from the lists above. Use null (not the string, the JSON null) for unassigned dimensions.",
        "- Do NOT use markdown code blocks. Return raw JSON only.",
    ]
    return "\n".join(lines)


def build_batch_prompt(labels: list[str]) -> str:
    items = "\n".join(f"- {label}" for label in labels)
    return f"Classify these market labels:\n\n{items}\n\nJSON:"


def _repair_json(text: str) -> str:
    """Attempt to repair truncated JSON."""
    text = re.sub(r",\s*([}\]])", r"\1", text)
    last_complete = text.rfind("}")
    opens_obj = text.count("{") - text.count("}")
    opens_arr = text.count("[") - text.count("]")
    if last_complete > 0 and (opens_arr > 0 or opens_obj > 0):
        text = text[: last_complete + 1]
        opens_obj = text.count("{") - text.count("}")
        text += "}" * max(0, opens_obj)
        opens_arr = text.count("[") - text.count("]")
        text += "]" * max(0, opens_arr)
        text = re.sub(r",\s*([}\]])", r"\1", text)
    return text


def parse_response(text: str, expected_labels: list[str]) -> dict[str, dict]:
    """Parse LLM response into {label: {geographic_region, sector, product_category}}."""
    text = text.strip()
    text = re.sub(r"^```json\s*\n?", "", text)
    text = re.sub(r"\n?```\s*$", "", text)

    start = text.find("[")
    end = text.rfind("]") + 1
    items = None

    if start >= 0 and end > start:
        try:
            items = json.loads(text[start:end])
        except json.JSONDecodeError:
            pass

    if items is None and start >= 0:
        repaired = _repair_json(text[start:])
        try:
            items = json.loads(repaired)
        except json.JSONDecodeError as e:
            print(f"  WARN: JSON parse error (after repair): {e}", flush=True)
            return {}

    if items is None:
        print("  WARN: no JSON array found", flush=True)
        return {}

    result = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        label = item.get("label", "")
        if not label:
            continue

        def clean(val):
            if val is None or val == "null" or val == "":
                return None
            return val

        result[label] = {
            "geographic_region": clean(item.get("geographic_region")),
            "sector": clean(item.get("sector")),
            "product_category": clean(item.get("product_category")),
        }
    return result


async def classify_batch(
    client: genai.Client,
    model_id: str,
    system_prompt: str,
    labels: list[str],
    semaphore: asyncio.Semaphore,
    batch_num: int,
    total_batches: int,
) -> dict[str, dict]:
    user_prompt = build_batch_prompt(labels)
    config = GenerateContentConfig(
        temperature=0.1,
        max_output_tokens=8192,
        system_instruction=system_prompt,
    )

    async with semaphore:
        try:
            resp = await asyncio.wait_for(
                client.aio.models.generate_content(
                    model=model_id, contents=user_prompt, config=config
                ),
                timeout=60,
            )
            raw = resp.text or ""
            result = parse_response(raw, labels)
            mapped = len(result)
            print(
                f"  Batch {batch_num}/{total_batches}: {mapped}/{len(labels)} mapped",
                flush=True,
            )
            return result
        except asyncio.TimeoutError:
            print(f"  Batch {batch_num}/{total_batches} TIMEOUT", flush=True)
            return {}
        except Exception as e:
            print(f"  Batch {batch_num}/{total_batches} ERROR: {e}", flush=True)
            return {}


def load_cache() -> dict[str, dict]:
    if CACHE_FILE.exists():
        with open(CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache: dict[str, dict]):
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


# ── Dedup ──────────────────────────────────────────────────────────

SUFFIX_RE = re.compile(
    r"[\s,]*\b(Inc\.?|Corp\.?|Corporation|LLC|Ltd\.?|Limited|Co\.?|Company|"
    r"plc|PLC|S\.A\.?|N\.V\.?|AG|GmbH|SE|L\.P\.?)\s*$",
    re.IGNORECASE,
)


def make_dedup_key(label: str) -> str:
    s = re.sub(r"^The\s+", "", label, flags=re.IGNORECASE)
    s = re.sub(r"\s*\([^)]*\)", "", s)
    s = SUFFIX_RE.sub("", s)
    s = re.sub(r"[,.\s]+$", "", s)
    s = s.lower().strip()
    s = " ".join(s.split())
    return s


def phase1_dedup(labels: list[str], freq: dict[str, int]) -> dict[str, str]:
    from collections import defaultdict

    groups: dict[str, list[str]] = defaultdict(list)
    for label in labels:
        groups[make_dedup_key(label)].append(label)
    label_to_rep = {}
    for key, variants in groups.items():
        best = max(variants, key=lambda v: freq.get(v, 0))
        for v in variants:
            label_to_rep[v] = best
    return label_to_rep


# ── Main ───────────────────────────────────────────────────────────


async def run_categorization(
    parquet_path: Path,
    output_path: Path,
    model_name: str,
    batch_size: int = 50,
    max_concurrent: int = 5,
    dry_run: bool = False,
):
    print(f"Reading {parquet_path} ...")
    df = pd.read_parquet(parquet_path)
    print(f"  {len(df)} rows, {df['label'].nunique()} unique labels")

    # Load classification dimensions
    sectors = load_csv(SECTORS_CSV)
    regions = load_csv(REGIONS_CSV)
    products = load_csv(PRODUCTS_CSV)
    system_prompt = build_system_prompt(sectors, regions, products)
    print(f"  {len(sectors)} sectors, {len(regions)} regions, {len(products)} product categories")

    # Dedup
    print("\n── Phase 1: Dedup ──")
    freq = df["label"].value_counts().to_dict()
    unique_labels = sorted(df["label"].dropna().unique().tolist())
    label_to_rep = phase1_dedup(unique_labels, freq)
    representatives = sorted(set(label_to_rep.values()))
    print(f"  {len(unique_labels)} unique labels -> {len(representatives)} dedup groups")

    # Cache
    cache = load_cache()
    remaining = [l for l in representatives if l not in cache]
    print(f"  {len(cache)} already cached, {len(remaining)} to classify")

    if dry_run:
        n_batches = (len(remaining) + batch_size - 1) // batch_size
        print(f"\n  DRY RUN: would classify {len(remaining)} labels in {n_batches} batches")
        print(f"  System prompt: {len(system_prompt)} chars (~{len(system_prompt) // 4} tokens)")
        return

    # ── Phase 2: LLM Classification ──
    if remaining:
        print("\n── Phase 2: LLM Classification ──")
        client = genai.Client(vertexai=True, project=VERTEX_PROJECT, location="global")
        model_id = get_model_id(model_name, "vertex")
        print(f"  Model: {model_id}")
        print(f"  Batch size: {batch_size}, Max concurrent: {max_concurrent}")

        batches = [remaining[i : i + batch_size] for i in range(0, len(remaining), batch_size)]
        total_batches = len(batches)
        print(f"  {total_batches} batches to process\n", flush=True)

        semaphore = asyncio.Semaphore(max_concurrent)
        start_time = time.time()
        save_interval = 50

        for chunk_start in range(0, total_batches, save_interval):
            chunk_end = min(chunk_start + save_interval, total_batches)
            chunk_batches = batches[chunk_start:chunk_end]

            tasks = [
                classify_batch(
                    client, model_id, system_prompt, batch, semaphore,
                    chunk_start + i + 1, total_batches,
                )
                for i, batch in enumerate(chunk_batches)
            ]
            results = await asyncio.gather(*tasks)

            new_entries = 0
            for batch_result in results:
                for label, dims in batch_result.items():
                    cache[label] = dims
                    new_entries += 1

            save_cache(cache)
            elapsed = time.time() - start_time
            done = chunk_end
            rate = done / elapsed * 60 if elapsed > 0 else 0
            eta = (total_batches - done) / rate if rate > 0 else 0
            print(
                f"  -- Checkpoint: {done}/{total_batches} batches, "
                f"{len(cache)} cached, +{new_entries} new, "
                f"{rate:.0f} batches/min, ETA {eta:.1f} min",
                flush=True,
            )

        elapsed = time.time() - start_time
        print(f"\n  Classified {len(remaining)} labels in {elapsed:.1f}s")
        print(f"  Throughput: {len(remaining) / elapsed * 60:.0f} labels/min")
        print(f"  Cache saved: {len(cache)} total entries", flush=True)

    # Check coverage
    uncached = [l for l in representatives if l not in cache]
    if uncached:
        print(f"\n  WARNING: {len(uncached)} labels not in cache")

    # ── Write output ──
    print(f"\nWriting enriched parquet to {output_path} ...")

    # Map each label through dedup -> cache
    def lookup(label):
        rep = label_to_rep.get(label, label)
        dims = cache.get(rep, cache.get(label, {}))
        return dims

    df["geographic_region"] = df["label"].map(lambda l: lookup(l).get("geographic_region") or "")
    df["sector"] = df["label"].map(lambda l: lookup(l).get("sector") or "")
    df["product_category"] = df["label"].map(lambda l: lookup(l).get("product_category") or "")
    df.to_parquet(output_path, index=False)

    # Summary
    classified_geo = (df["geographic_region"] != "").sum()
    classified_sec = (df["sector"] != "").sum()
    classified_prod = (df["product_category"] != "").sum()
    print(f"  geographic_region: {classified_geo}/{len(df)} ({classified_geo / len(df) * 100:.1f}%)")
    print(f"  sector: {classified_sec}/{len(df)} ({classified_sec / len(df) * 100:.1f}%)")
    print(f"  product_category: {classified_prod}/{len(df)} ({classified_prod / len(df) * 100:.1f}%)")

    print(f"\n  Geographic region distribution:")
    for val, count in df["geographic_region"].value_counts().head(10).items():
        if val:
            print(f"    {val}: {count}")

    print(f"\n  Sector distribution:")
    for val, count in df["sector"].value_counts().head(15).items():
        if val:
            print(f"    {val}: {count}")

    print(f"\n  Product category distribution:")
    for val, count in df["product_category"].value_counts().head(15).items():
        if val:
            print(f"    {val}: {count}")


def main():
    parser = argparse.ArgumentParser(description="Categorize markets using LLM")
    parser.add_argument("--model", default="gemini-2.0-flash", help="Model (default: gemini-2.0-flash)")
    parser.add_argument("--batch-size", type=int, default=50, help="Labels per batch (default: 50)")
    parser.add_argument("--max-concurrent", type=int, default=5, help="Max concurrent (default: 5)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    pipeline_dir = SCRIPT_DIR.parent
    default_input = pipeline_dir / "output" / args.model / "parquet" / "nodes_market.parquet"
    input_path = Path(args.input) if args.input else default_input
    default_output = input_path.parent / "nodes_market_categorized.parquet"
    output_path = Path(args.output) if args.output else default_output

    if not input_path.exists():
        print(f"ERROR: {input_path} not found")
        sys.exit(1)

    print("=" * 60)
    print("Market Categorization")
    print("=" * 60)
    asyncio.run(
        run_categorization(
            parquet_path=input_path,
            output_path=output_path,
            model_name=args.model,
            batch_size=args.batch_size,
            max_concurrent=args.max_concurrent,
            dry_run=args.dry_run,
        )
    )
    print("\nDone.")


if __name__ == "__main__":
    main()
