#!/usr/bin/env python3
"""
Categorize risks from nodes_risk.parquet using LLM.

Reads unique risk labels, sends them in batches to Gemini,
and writes an enriched parquet with a risk_categories column.

Usage:
    python categorize_risks.py                          # default: gemini-3-flash
    python categorize_risks.py --model gemini-2.5-flash # override model
    python categorize_risks.py --batch-size 30          # smaller batches
    python categorize_risks.py --dry-run                # show plan, don't call LLM
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

# Add parent dir to path for config imports
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import VERTEX_PROJECT, get_model_id

# ── Paths ──────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
CATEGORIES_CSV = SCRIPT_DIR / "data" / "risk_categories.csv"
CACHE_FILE = SCRIPT_DIR / "data" / "risk_label_categories_cache.json"


def load_categories() -> list[dict]:
    """Load the 17 risk categories from CSV."""
    cats = []
    with open(CATEGORIES_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cats.append(row)
    return cats


def build_category_prompt(categories: list[dict]) -> str:
    """Build the system context describing all categories."""
    lines = ["You are a risk classification expert. Assign one or more risk categories to each risk label.\n"]
    lines.append("## Risk Categories\n")
    for i, cat in enumerate(categories, 1):
        lines.append(f"{i}. **{cat['risk_category']}**: {cat['description']}")
        lines.append(f"   Keywords: {cat['keywords']}")
        lines.append(f"   Examples: {cat['example_risks']}")
        lines.append("")
    lines.append("## Rules")
    lines.append("- Assign 1-3 categories per risk. Most risks fit 1-2.")
    lines.append("- Only assign a category if there is clear evidence in the label.")
    lines.append("- Return ONLY valid JSON — an array of objects.")
    lines.append("- Each object: {\"label\": \"<exact label>\", \"categories\": [\"Cat1\", \"Cat2\"]}")
    lines.append("- Use exact category names from the list above.")
    lines.append("- Do NOT use markdown code blocks. Return raw JSON only.")
    return "\n".join(lines)


def build_batch_prompt(labels: list[str]) -> str:
    """Build the user prompt for a batch of labels."""
    items = "\n".join(f"- {label}" for label in labels)
    return f"Classify these risk labels:\n\n{items}\n\nJSON:"


def _repair_json(text: str) -> str:
    """Attempt to repair truncated/malformed JSON from LLM output."""
    text = re.sub(r",\s*([}\]])", r"\1", text)
    last_complete = text.rfind("}")
    if last_complete > 0:
        text = text[: last_complete + 1]
        open_obj = text.count("{") - text.count("}")
        text += "}" * max(0, open_obj)
        open_arr = text.count("[") - text.count("]")
        text += "]" * max(0, open_arr)
        text = re.sub(r",\s*([}\]])", r"\1", text)
    return text


def parse_response(text: str, expected_labels: list[str]) -> dict[str, list[str]]:
    """Parse LLM response into {label: [categories]} mapping."""
    text = text.strip()
    # Strip markdown fences if present
    text = re.sub(r"^```json\s*\n?", "", text)
    text = re.sub(r"\n?```\s*$", "", text)

    # Find JSON array
    start = text.find("[")
    end = text.rfind("]") + 1
    if start < 0 or end <= start:
        print(f"  WARN: no JSON array found in response")
        return {}

    try:
        items = json.loads(text[start:end])
    except json.JSONDecodeError:
        # Try repair on truncated/malformed output
        repaired = _repair_json(text[start:])
        try:
            items = json.loads(repaired)
        except json.JSONDecodeError as e2:
            print(f"  WARN: JSON parse error (after repair): {e2}")
            return {}

    result = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        label = item.get("label", "")
        cats = item.get("categories", [])
        if isinstance(cats, str):
            cats = [c.strip() for c in cats.split(",") if c.strip()]
        if label and cats:
            result[label] = cats
    return result


async def classify_batch(
    client: genai.Client,
    model_id: str,
    system_prompt: str,
    labels: list[str],
    semaphore: asyncio.Semaphore,
    batch_num: int,
    total_batches: int,
) -> dict[str, list[str]]:
    """Classify a batch of labels via Gemini."""
    user_prompt = build_batch_prompt(labels)
    config = GenerateContentConfig(
        temperature=0.1,
        max_output_tokens=4096,
        system_instruction=system_prompt,
    )

    async with semaphore:
        try:
            resp = await client.aio.models.generate_content(
                model=model_id,
                contents=user_prompt,
                config=config,
            )
            raw = resp.text or ""
            result = parse_response(raw, labels)
            mapped = len(result)
            print(f"  Batch {batch_num}/{total_batches}: {mapped}/{len(labels)} mapped")
            return result
        except Exception as e:
            print(f"  Batch {batch_num}/{total_batches} ERROR: {e}")
            return {}


def load_cache() -> dict[str, str]:
    """Load previously classified labels from cache."""
    if CACHE_FILE.exists():
        with open(CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache: dict[str, str]):
    """Save classified labels to cache."""
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


async def run_categorization(
    parquet_path: Path,
    output_path: Path,
    model_name: str,
    batch_size: int = 50,
    max_concurrent: int = 5,
    dry_run: bool = False,
):
    """Main categorization pipeline."""
    # Load data
    print(f"Reading {parquet_path} ...")
    df = pd.read_parquet(parquet_path)
    print(f"  {len(df)} rows, {df['label'].nunique()} unique labels")

    # Load categories
    categories = load_categories()
    system_prompt = build_category_prompt(categories)
    valid_categories = {cat["risk_category"] for cat in categories}
    print(f"  {len(categories)} risk categories loaded")

    # Get unique labels to classify
    unique_labels = sorted(df["label"].dropna().unique().tolist())

    # Load cache (previously classified labels)
    cache = load_cache()
    remaining = [l for l in unique_labels if l not in cache]
    print(f"  {len(cache)} already cached, {len(remaining)} to classify")

    if dry_run:
        print(f"\n  DRY RUN: would classify {len(remaining)} labels")
        print(f"  Batches: {(len(remaining) + batch_size - 1) // batch_size}")
        print(f"  System prompt: {len(system_prompt)} chars (~{len(system_prompt)//4} tokens)")
        return

    if remaining:
        # Set up Gemini client
        client = genai.Client(vertexai=True, project=VERTEX_PROJECT, location="global")
        model_id = get_model_id(model_name, "vertex")
        print(f"\n  Model: {model_id}")
        print(f"  Batch size: {batch_size}, Max concurrent: {max_concurrent}")

        # Create batches
        batches = [remaining[i:i + batch_size] for i in range(0, len(remaining), batch_size)]
        total_batches = len(batches)
        print(f"  {total_batches} batches to process\n")

        semaphore = asyncio.Semaphore(max_concurrent)
        start_time = time.time()

        # Process batches concurrently
        tasks = [
            classify_batch(client, model_id, system_prompt, batch, semaphore, i + 1, total_batches)
            for i, batch in enumerate(batches)
        ]
        results = await asyncio.gather(*tasks)

        # Merge results into cache
        for batch_result in results:
            for label, cats in batch_result.items():
                # Validate category names
                valid_cats = [c for c in cats if c in valid_categories]
                if valid_cats:
                    cache[label] = ", ".join(valid_cats)
                else:
                    cache[label] = ", ".join(cats)  # keep LLM output even if not exact match

        elapsed = time.time() - start_time
        print(f"\n  Classified {len(remaining)} labels in {elapsed:.1f}s")
        print(f"  Throughput: {len(remaining)/elapsed*60:.0f} labels/min")

        # Save cache
        save_cache(cache)
        print(f"  Cache saved: {len(cache)} total entries")

    # Check for labels that weren't classified
    unclassified = [l for l in unique_labels if l not in cache]
    if unclassified:
        print(f"\n  WARNING: {len(unclassified)} labels not classified")

    # Map categories to all rows
    print(f"\nWriting enriched parquet to {output_path} ...")
    df["risk_categories"] = df["label"].map(cache).fillna("")
    df.to_parquet(output_path, index=False)

    # Summary
    classified = (df["risk_categories"] != "").sum()
    print(f"  {classified}/{len(df)} rows have categories ({classified/len(df)*100:.1f}%)")

    # Category distribution
    all_cats = df["risk_categories"].str.split(", ").explode()
    all_cats = all_cats[all_cats != ""]
    print(f"\n  Category distribution:")
    for cat, count in all_cats.value_counts().head(20).items():
        print(f"    {cat}: {count}")


def main():
    parser = argparse.ArgumentParser(description="Categorize risks using LLM")
    parser.add_argument("--model", default="gemini-3-flash", help="Model name (default: gemini-3-flash)")
    parser.add_argument("--batch-size", type=int, default=50, help="Labels per batch (default: 50)")
    parser.add_argument("--max-concurrent", type=int, default=5, help="Max concurrent requests (default: 5)")
    parser.add_argument("--dry-run", action="store_true", help="Show plan without calling LLM")
    parser.add_argument("--input", default=None, help="Input parquet path (auto-detected from model)")
    parser.add_argument("--output", default=None, help="Output parquet path (default: nodes_risk_categorized.parquet)")
    args = parser.parse_args()

    pipeline_dir = SCRIPT_DIR.parent
    default_input = pipeline_dir / "output" / args.model / "parquet" / "nodes_risk.parquet"
    input_path = Path(args.input) if args.input else default_input
    default_output = input_path.parent / "nodes_risk_categorized.parquet"
    output_path = Path(args.output) if args.output else default_output

    if not input_path.exists():
        print(f"ERROR: {input_path} not found")
        sys.exit(1)

    print("=" * 60)
    print("Risk Categorization")
    print("=" * 60)
    asyncio.run(run_categorization(
        parquet_path=input_path,
        output_path=output_path,
        model_name=args.model,
        batch_size=args.batch_size,
        max_concurrent=args.max_concurrent,
        dry_run=args.dry_run,
    ))
    print("\nDone.")


if __name__ == "__main__":
    main()
