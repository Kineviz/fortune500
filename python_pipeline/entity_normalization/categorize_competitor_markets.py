#!/usr/bin/env python3
"""
Assign market product categories to NormalizedCompetitor entities.

Quick LLM pass on 5,455 unique canonical competitors to assign one of
12 MarketCategory product categories (shared with market categorization).

Also generates:
- nodes_normalized_competitor.parquet (canonical entities with product_category)
- edges_instance_of.parquet (Competitor → NormalizedCompetitor)
- edges_in_market_category.parquet (NormalizedCompetitor → MarketCategory)
- nodes_market_category.parquet (12 hub nodes)

Usage:
    python categorize_competitor_markets.py --dry-run
    python categorize_competitor_markets.py
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

SCRIPT_DIR = Path(__file__).resolve().parent
PRODUCTS_CSV = SCRIPT_DIR / "data" / "market_product_categories.csv"
CACHE_FILE = SCRIPT_DIR / "data" / "competitor_market_category_cache.json"


def load_csv(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_system_prompt(products: list[dict]) -> str:
    lines = [
        "You are a market classification expert. For each competitor entity, assign the most relevant market/product category.",
        "",
        "## Product Categories",
        "",
    ]
    for p in products:
        lines.append(f"- **{p['product_category']}**: {p['description']}")
    lines += [
        "",
        "## Rules",
        "- Assign ONE product category, or null if the competitor doesn't clearly fit any category.",
        "- Use the competitor name and type to determine the category.",
        "- Company-type entities: assign based on what the company is known for (e.g., AWS → Cloud & Software).",
        "- Category-type entities: assign based on what the category describes (e.g., Online Retailers → E-commerce & Digital).",
        "- Generic-type entities: usually null unless the description is specific enough.",
        "- Return ONLY valid JSON — an array of objects.",
        '- Each object: {"name": "<exact name>", "product_category": "...|null"}',
        "- Use exact category names from the list above.",
        "- Do NOT use markdown code blocks. Return raw JSON only.",
    ]
    return "\n".join(lines)


def build_batch_prompt(entities: list[dict]) -> str:
    items = []
    for e in entities:
        parts = [e["label"]]
        if e.get("competitor_type"):
            parts.append(f"(type: {e['competitor_type']})")
        if e.get("sector"):
            parts.append(f"(sector: {e['sector']})")
        items.append("- " + " ".join(parts))
    return "Assign product categories:\n\n" + "\n".join(items) + "\n\nJSON:"


def _repair_json(text: str) -> str:
    text = re.sub(r",\s*([}\]])", r"\1", text)
    last = text.rfind("}")
    if last > 0:
        text = text[: last + 1]
        text += "}" * max(0, text.count("{") - text.count("}"))
        text += "]" * max(0, text.count("[") - text.count("]"))
        text = re.sub(r",\s*([}\]])", r"\1", text)
    return text


def parse_response(text: str) -> dict[str, str | None]:
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
        try:
            items = json.loads(_repair_json(text[start:]))
        except json.JSONDecodeError as e:
            print(f"  WARN: JSON parse error: {e}", flush=True)
            return {}

    if items is None:
        print("  WARN: no JSON array found", flush=True)
        return {}

    result = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        name = item.get("name", "")
        cat = item.get("product_category")
        if name:
            result[name] = cat if cat and cat != "null" else None
    return result


async def classify_batch(client, model_id, system_prompt, entities, semaphore, batch_num, total):
    prompt = build_batch_prompt(entities)
    config = GenerateContentConfig(
        temperature=0.1, max_output_tokens=4096, system_instruction=system_prompt
    )
    async with semaphore:
        try:
            resp = await asyncio.wait_for(
                client.aio.models.generate_content(model=model_id, contents=prompt, config=config),
                timeout=60,
            )
            result = parse_response(resp.text or "")
            print(f"  Batch {batch_num}/{total}: {len(result)}/{len(entities)} mapped", flush=True)
            return result
        except asyncio.TimeoutError:
            print(f"  Batch {batch_num}/{total} TIMEOUT", flush=True)
            return {}
        except Exception as e:
            print(f"  Batch {batch_num}/{total} ERROR: {e}", flush=True)
            return {}


def load_cache():
    if CACHE_FILE.exists():
        with open(CACHE_FILE) as f:
            return json.load(f)
    return {}


def save_cache(cache):
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


async def run(parquet_dir: Path, model_name: str, batch_size: int, max_concurrent: int, dry_run: bool):
    # Load resolved competitors
    resolved_path = parquet_dir / "nodes_competitor_resolved.parquet"
    df = pd.read_parquet(resolved_path)
    print(f"  {len(df)} resolved rows, {df['canonical_id'].nunique()} canonical entities")

    # Build unique NormalizedCompetitor table
    nc = df.drop_duplicates("canonical_id")[["canonical_id", "label", "competitor_type", "sector"]].copy()
    nc = nc.reset_index(drop=True)
    print(f"  {len(nc)} NormalizedCompetitor entities")

    # Load product categories
    products = load_csv(PRODUCTS_CSV)
    system_prompt = build_system_prompt(products)
    valid_cats = {p["product_category"] for p in products}

    # Cache
    cache = load_cache()
    remaining = nc[~nc["label"].isin(cache)].to_dict("records")
    print(f"  {len(cache)} cached, {len(remaining)} to classify")

    if dry_run:
        n_batches = (len(remaining) + batch_size - 1) // batch_size
        print(f"\n  DRY RUN: {len(remaining)} entities in {n_batches} batches")
        return

    if remaining:
        print(f"\n── LLM Classification ──")
        client = genai.Client(vertexai=True, project=VERTEX_PROJECT, location="global")
        model_id = get_model_id(model_name, "vertex")
        print(f"  Model: {model_id}", flush=True)

        batches = [remaining[i : i + batch_size] for i in range(0, len(remaining), batch_size)]
        total = len(batches)
        semaphore = asyncio.Semaphore(max_concurrent)
        start = time.time()

        for chunk_start in range(0, total, 50):
            chunk_end = min(chunk_start + 50, total)
            tasks = [
                classify_batch(client, model_id, system_prompt, b, semaphore, chunk_start + i + 1, total)
                for i, b in enumerate(batches[chunk_start:chunk_end])
            ]
            results = await asyncio.gather(*tasks)
            for r in results:
                cache.update(r)
            save_cache(cache)
            elapsed = time.time() - start
            print(f"  -- Checkpoint: {chunk_end}/{total}, {len(cache)} cached, {elapsed:.0f}s", flush=True)

        print(f"\n  Classified in {time.time() - start:.1f}s, {len(cache)} cached")

    # ── Generate output files ──
    print("\n── Output Generation ──")

    # 1. NormalizedCompetitor nodes with product_category
    nc["product_category"] = nc["label"].map(cache).fillna("")
    nc = nc.rename(columns={"canonical_id": "id"})
    nc_path = parquet_dir / "nodes_normalized_competitor.parquet"
    nc.to_parquet(nc_path, index=False)
    print(f"  nodes_normalized_competitor: {len(nc)} rows -> {nc_path.name}")

    cat_coverage = (nc["product_category"] != "").sum()
    print(f"  product_category coverage: {cat_coverage}/{len(nc)} ({cat_coverage / len(nc) * 100:.1f}%)")

    # 2. INSTANCE_OF edges (Competitor → NormalizedCompetitor)
    instance_of = df[df["original_id"] != ""][["original_id", "canonical_id"]].drop_duplicates()
    instance_of = instance_of.rename(columns={"original_id": "source_node", "canonical_id": "target_node"})
    io_path = parquet_dir / "edges_instance_of.parquet"
    instance_of.to_parquet(io_path, index=False)
    print(f"  edges_instance_of: {len(instance_of)} rows -> {io_path.name}")

    # 3. MarketCategory nodes
    cats = []
    for p in products:
        cats.append({"id": p["product_category"].lower().replace(" & ", "_and_").replace(" ", "_"),
                      "label": p["product_category"], "description": p["description"]})
    mc_df = pd.DataFrame(cats)
    mc_path = parquet_dir / "nodes_market_category.parquet"
    mc_df.to_parquet(mc_path, index=False)
    print(f"  nodes_market_category: {len(mc_df)} rows -> {mc_path.name}")

    # 4. IN_MARKET_CATEGORY edges (NormalizedCompetitor → MarketCategory)
    mc_id_map = {p["product_category"]: p["product_category"].lower().replace(" & ", "_and_").replace(" ", "_")
                 for p in products}
    in_mc_rows = []
    for _, row in nc.iterrows():
        if row["product_category"] and row["product_category"] in mc_id_map:
            in_mc_rows.append({"source_node": row["id"], "target_node": mc_id_map[row["product_category"]]})
    in_mc_df = pd.DataFrame(in_mc_rows)
    in_mc_path = parquet_dir / "edges_in_market_category.parquet"
    in_mc_df.to_parquet(in_mc_path, index=False)
    print(f"  edges_in_market_category: {len(in_mc_df)} rows -> {in_mc_path.name}")

    # Summary
    print(f"\n  Product category distribution:")
    for cat, count in nc["product_category"].value_counts().head(15).items():
        if cat:
            print(f"    {cat}: {count}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="gemini-2.5-flash-lite")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--max-concurrent", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--parquet-dir", default=None)
    args = parser.parse_args()

    parquet_dir = Path(args.parquet_dir) if args.parquet_dir else (
        SCRIPT_DIR.parent / "output" / "gemini-3-flash" / "parquet"
    )

    print("=" * 60)
    print("NormalizedCompetitor Market Category Assignment")
    print("=" * 60)
    asyncio.run(run(parquet_dir, args.model, args.batch_size, args.max_concurrent, args.dry_run))
    print("\nDone.")


if __name__ == "__main__":
    main()
