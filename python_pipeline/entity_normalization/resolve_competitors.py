#!/usr/bin/env python3
"""
Resolve competitor entities from nodes_competitor.parquet.

5-phase pipeline:
  Phase 1: Deterministic pre-dedup (suffix stripping, case-fold)
  Phase 2: LLM resolution (split, canonicalize, classify, link)
  Phase 3: Post-LLM canonical dedup
  Phase 4: (separate script) Senzing validation
  Phase 5: Output generation (resolved parquet + edge files)

Usage:
    python resolve_competitors.py                        # default: gemini-3-flash
    python resolve_competitors.py --model gemini-2.5-flash
    python resolve_competitors.py --batch-size 20
    python resolve_competitors.py --dry-run              # show plan, don't call LLM
"""

import argparse
import asyncio
import csv
import json
import re
import time
import uuid
from collections import Counter, defaultdict
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
SECTORS_CSV = SCRIPT_DIR / "data" / "competitor_sectors.csv"
TYPES_CSV = SCRIPT_DIR / "data" / "competitor_types.csv"
CACHE_FILE = SCRIPT_DIR / "data" / "competitor_resolved_cache.json"

# ── Company suffix regex (for deterministic dedup) ─────────────────
SUFFIX_RE = re.compile(
    r"[\s,]*\b("
    r"Inc\.?|Incorporated|Corp\.?|Corporation|LLC|L\.L\.C\.|"
    r"Ltd\.?|Limited|Co\.?|Company|Companies|"
    r"plc|PLC|S\.A\.?|N\.V\.?|AG|GmbH|SE|"
    r"L\.P\.?|LP|S\.p\.A\.?|Oyj|A\.S\.?|A\.Ş\.|B\.V\.?|S\.A\.S\.?"
    r")\s*$",
    re.IGNORECASE,
)
LEADING_THE_RE = re.compile(r"^The\s+", re.IGNORECASE)
PAREN_RE = re.compile(r"\s*\([^)]*\)")
TRAILING_PUNCT_RE = re.compile(r"[,.\s]+$")


# ====================================================================
# Phase 1: Deterministic Pre-Dedup
# ====================================================================


def make_dedup_key(label: str) -> str:
    """Normalize a label into a dedup key for grouping obvious variants."""
    s = label
    s = LEADING_THE_RE.sub("", s)
    s = PAREN_RE.sub("", s)
    s = SUFFIX_RE.sub("", s)
    s = TRAILING_PUNCT_RE.sub("", s)
    s = s.lower()
    s = " ".join(s.split())
    return s.strip()


def phase1_dedup(labels: list[str], freq: dict[str, int]) -> dict[str, str]:
    """Group labels by dedup key. Return {label: representative} mapping."""
    groups: dict[str, list[str]] = defaultdict(list)
    for label in labels:
        key = make_dedup_key(label)
        groups[key].append(label)

    # Pick the most frequent variant as representative
    label_to_rep: dict[str, str] = {}
    for key, variants in groups.items():
        best = max(variants, key=lambda v: freq.get(v, 0))
        for v in variants:
            label_to_rep[v] = best

    return label_to_rep


# ====================================================================
# Phase 2: LLM Resolution
# ====================================================================


def load_sectors() -> list[dict]:
    """Load sector definitions from CSV."""
    sectors = []
    with open(SECTORS_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sectors.append(row)
    return sectors


def build_system_prompt(sectors: list[dict]) -> str:
    """Build the system prompt for LLM competitor resolution."""
    lines = [
        "You are a competitor entity resolution expert. For each competitor label from SEC 10-K filings:",
        "",
        "1. **Split**: If the label lists multiple distinct entities (comma-separated, 'and'-joined, or slash-separated), split them into individual entities. If it's a single entity or a descriptive category, keep it as one.",
        "2. **Category with examples**: Labels like 'Cloud Service Providers (Amazon, Alphabet, Microsoft)' should be split into the individual companies AND the category. Return all entities.",
        "3. **Canonicalize**: Return the commonly known short name. 'The Boeing Company' → 'Boeing'. 'NVIDIA Corporation' → 'NVIDIA'. 'Walmart, Inc.' → 'Walmart'.",
        "4. **Classify type**:",
        '   - "Company": A specific named company (Apple, Boeing, AWS)',
        '   - "Category": A named group/class of competitors (Online Retailers, Cloud Service Providers)',
        '   - "Generic": A vague/descriptive reference (Large global competitors, Third-party providers)',
        "5. **Classify sector**: One of the sectors below, or null if unclear/cross-sector.",
        "6. **Identify parent**: For well-known subsidiaries/divisions/products, set parent to the parent company name. AWS → parent: Amazon. Google Cloud → parent: Alphabet. Leave null for standalone companies.",
        "7. **Identify category**: When the label explicitly contains a category (e.g., parenthetical pattern), set category to that category name for each company extracted. Leave null otherwise.",
        "",
        "## Sectors",
        "",
    ]
    for s in sectors:
        lines.append(
            f"- **{s['sector']}**: {s['description']}. Keywords: {s['keywords']}"
        )
    lines += [
        "",
        "## Important Rules",
        "- Do NOT split company names that contain 'and' as part of the name: 'Air Products and Chemicals' is ONE company.",
        "- Do NOT split slash pairs that are one entity: 'Anheuser-Busch InBev SA/NV' is ONE company.",
        "- DO split: 'Apple, Google, Microsoft' → 3 companies. 'Affirm and Afterpay' → 2 companies.",
        "- DO split 'Category (Company1, Company2)' → category + companies, with category field set.",
        "- OPEC, OPEC+ are organizations — type: Company, sector: Energy.",
        "- Country names as competitors (China, Russia) — type: Generic, sector: null.",
        "- Return ONLY valid JSON — an array of objects.",
        '- Each object: {"label": "<exact input label>", "entities": [{"name": "...", "competitor_type": "...", "sector": "...|null", "parent": "...|null", "category": "...|null"}]}',
        "- Use exact sector names from the list above.",
        "- Do NOT use markdown code blocks. Return raw JSON only.",
    ]
    return "\n".join(lines)


def build_batch_prompt(labels: list[str]) -> str:
    """Build the user prompt for a batch of labels."""
    items = "\n".join(f"- {label}" for label in labels)
    return f"Resolve these competitor labels:\n\n{items}\n\nJSON:"


def _repair_json(text: str) -> str:
    """Attempt to repair truncated/malformed JSON from LLM output."""
    # Remove trailing commas before } or ]
    text = re.sub(r",\s*([}\]])", r"\1", text)
    # If truncated mid-object, try to close open braces/brackets
    opens = text.count("[") - text.count("]")
    opens_obj = text.count("{") - text.count("}")
    # Truncate back to last complete object in the array
    # Find the last complete "}" that closes an object in the array
    last_complete = text.rfind("}")
    if last_complete > 0 and (opens > 0 or opens_obj > 0):
        # Cut after the last complete object, close the array
        text = text[: last_complete + 1]
        # Close any remaining open structures
        opens_obj = text.count("{") - text.count("}")
        text += "}" * max(0, opens_obj)
        opens = text.count("[") - text.count("]")
        text += "]" * max(0, opens)
        # Remove trailing commas again after surgery
        text = re.sub(r",\s*([}\]])", r"\1", text)
    return text


def parse_response(text: str) -> dict[str, list[dict]]:
    """Parse LLM response into {label: [entities]} mapping."""
    text = text.strip()
    text = re.sub(r"^```json\s*\n?", "", text)
    text = re.sub(r"\n?```\s*$", "", text)

    start = text.find("[")
    end = text.rfind("]") + 1
    if start < 0 or end <= start:
        # Try repair on truncated response
        if start >= 0:
            repaired = _repair_json(text[start:])
            try:
                items = json.loads(repaired)
                # fall through to processing below
                return _extract_entities(items)
            except json.JSONDecodeError:
                pass
        print("  WARN: no JSON array found in response")
        return {}

    try:
        items = json.loads(text[start:end])
    except json.JSONDecodeError:
        # Try repair
        repaired = _repair_json(text[start:])
        try:
            items = json.loads(repaired)
        except json.JSONDecodeError as e2:
            print(f"  WARN: JSON parse error (after repair): {e2}")
            return {}

    return _extract_entities(items)


def _extract_entities(items: list) -> dict[str, list[dict]]:
    """Extract entities from parsed JSON items."""
    result = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        label = item.get("label", "")
        entities = item.get("entities", [])
        if not label or not entities:
            continue
        # Validate each entity
        valid_entities = []
        for ent in entities:
            if not isinstance(ent, dict):
                continue
            name = ent.get("name", "")
            if not name:
                continue
            valid_entities.append(
                {
                    "name": name,
                    "competitor_type": ent.get("competitor_type", "Company"),
                    "sector": ent.get("sector") if ent.get("sector") != "null" else None,
                    "parent": ent.get("parent") if ent.get("parent") != "null" else None,
                    "category": (
                        ent.get("category") if ent.get("category") != "null" else None
                    ),
                }
            )
        if valid_entities:
            result[label] = valid_entities
    return result


async def classify_batch(
    client: genai.Client,
    model_id: str,
    system_prompt: str,
    labels: list[str],
    semaphore: asyncio.Semaphore,
    batch_num: int,
    total_batches: int,
) -> dict[str, list[dict]]:
    """Classify a batch of labels via Gemini."""
    user_prompt = build_batch_prompt(labels)
    config = GenerateContentConfig(
        temperature=0.1,
        max_output_tokens=16384,
        system_instruction=system_prompt,
    )

    async with semaphore:
        try:
            resp = await asyncio.wait_for(
                client.aio.models.generate_content(
                    model=model_id,
                    contents=user_prompt,
                    config=config,
                ),
                timeout=60,
            )
            raw = resp.text or ""
            result = parse_response(raw)
            mapped = len(result)
            print(
                f"  Batch {batch_num}/{total_batches}: {mapped}/{len(labels)} mapped",
                flush=True,
            )
            return result
        except asyncio.TimeoutError:
            print(
                f"  Batch {batch_num}/{total_batches} TIMEOUT (60s)", flush=True
            )
            return {}
        except Exception as e:
            print(f"  Batch {batch_num}/{total_batches} ERROR: {e}", flush=True)
            return {}


def load_cache() -> dict[str, list[dict]]:
    """Load previously resolved labels from cache."""
    if CACHE_FILE.exists():
        with open(CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache: dict[str, list[dict]]):
    """Save resolved labels to cache."""
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


# ====================================================================
# Phase 3: Post-LLM Canonical Dedup
# ====================================================================


def phase3_canonical_dedup(
    cache: dict[str, list[dict]],
) -> tuple[dict[str, dict], dict[str, str]]:
    """
    Deduplicate resolved entity names across all cache entries.

    Returns:
        canonical_entities: {canonical_id: {name, competitor_type, sector, parent, category}}
        name_to_canonical_id: {dedup_key(name): canonical_id}
    """
    # Collect all entity names and their metadata
    name_occurrences: dict[str, list[dict]] = defaultdict(list)
    for label, entities in cache.items():
        for ent in entities:
            key = make_dedup_key(ent["name"])
            if key:
                name_occurrences[key].append(ent)

    # For each dedup group, pick canonical name and merge metadata
    canonical_entities: dict[str, dict] = {}
    name_to_canonical_id: dict[str, str] = {}

    for key, occurrences in name_occurrences.items():
        # Pick most frequent name form
        name_counts = Counter(ent["name"] for ent in occurrences)
        canonical_name = name_counts.most_common(1)[0][0]

        # Pick most frequent non-null values for each field
        def most_common_non_null(field):
            vals = [ent[field] for ent in occurrences if ent.get(field)]
            if vals:
                return Counter(vals).most_common(1)[0][0]
            return None

        cid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"competitor:{key}"))
        canonical_entities[cid] = {
            "name": canonical_name,
            "competitor_type": most_common_non_null("competitor_type") or "Company",
            "sector": most_common_non_null("sector"),
            "parent": most_common_non_null("parent"),
            "category": most_common_non_null("category"),
        }
        name_to_canonical_id[key] = cid

    return canonical_entities, name_to_canonical_id


# ====================================================================
# Phase 5: Output Generation
# ====================================================================


def phase5_generate_output(
    df: pd.DataFrame,
    label_to_rep: dict[str, str],
    cache: dict[str, list[dict]],
    canonical_entities: dict[str, dict],
    name_to_cid: dict[str, str],
    edges_competes: pd.DataFrame,
    edges_ref: pd.DataFrame,
    output_dir: Path,
):
    """Generate resolved parquet and edge files."""
    # Build rows
    node_rows = []
    competes_rows = []
    ref_rows = []
    subsidiary_rows = set()
    in_category_rows = set()

    # Track which canonical IDs we've already created synthesized nodes for
    synthesized_cids = set()

    # Pre-index edges for O(1) lookup instead of O(n) per row
    competes_by_target = defaultdict(list)
    for _, edge in edges_competes.iterrows():
        competes_by_target[edge["target_node"]].append(edge["source_node"])
    ref_by_source = defaultdict(list)
    for _, edge in edges_ref.iterrows():
        ref_by_source[edge["source_node"]].append(edge["target_node"])

    for idx, row in df.iterrows():
        original_id = row["id"]
        original_label = row["label"]

        # Look up the representative label, then the cache
        rep = label_to_rep.get(original_label, original_label)
        entities = cache.get(rep, None)
        if entities is None:
            # Fallback: try original label directly
            entities = cache.get(original_label, None)
        if entities is None:
            # Still no cache entry — keep original as-is
            entities = [
                {
                    "name": original_label,
                    "competitor_type": "Company",
                    "sector": None,
                    "parent": None,
                    "category": None,
                }
            ]

        # Look up edges for this original competitor node (O(1))
        orig_competes_sources = competes_by_target.get(original_id, [])
        orig_ref_targets = ref_by_source.get(original_id, [])

        for ent in entities:
            ent_name = ent["name"]
            ent_key = make_dedup_key(ent_name)
            cid = name_to_cid.get(ent_key)
            if cid is None:
                cid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"competitor:{ent_key}"))

            new_id = str(uuid.uuid4())

            canon = canonical_entities.get(cid, ent)

            node_rows.append(
                {
                    "id": new_id,
                    "canonical_id": cid,
                    "label": canon.get("name", ent_name),
                    "original_label": original_label,
                    "original_id": original_id,
                    "competitor_type": canon.get("competitor_type", "Company"),
                    "sector": canon.get("sector") or "",
                    "year": row["year"],
                    "section": row["section"],
                    "link": row["link"],
                    "relationship": row["relationship"],
                }
            )

            # COMPETES_WITH: original source -> new id
            for source in orig_competes_sources:
                competes_rows.append(
                    {"source_node": source, "target_node": new_id}
                )

            # COMPETITOR_HAS_REFERENCE: new id -> original reference target
            for target in orig_ref_targets:
                ref_rows.append(
                    {"source_node": new_id, "target_node": target}
                )

            # SUBSIDIARY_OF: if entity has a parent
            parent_name = ent.get("parent") or canon.get("parent")
            if parent_name:
                parent_key = make_dedup_key(parent_name)
                parent_cid = name_to_cid.get(parent_key)
                if parent_cid is None:
                    parent_cid = str(
                        uuid.uuid5(uuid.NAMESPACE_URL, f"competitor:{parent_key}")
                    )
                subsidiary_rows.add((cid, parent_cid))
                # Ensure parent node exists
                if parent_cid not in canonical_entities and parent_cid not in synthesized_cids:
                    synthesized_cids.add(parent_cid)

            # IN_CATEGORY: if entity has a category
            cat_name = ent.get("category") or canon.get("category")
            if cat_name:
                cat_key = make_dedup_key(cat_name)
                cat_cid = name_to_cid.get(cat_key)
                if cat_cid is None:
                    cat_cid = str(
                        uuid.uuid5(uuid.NAMESPACE_URL, f"competitor:{cat_key}")
                    )
                in_category_rows.add((cid, cat_cid))
                if cat_cid not in canonical_entities and cat_cid not in synthesized_cids:
                    synthesized_cids.add(cat_cid)

    # Create synthesized nodes for parents/categories not in the data
    for cid in synthesized_cids:
        # Find the name from the entities that reference this cid
        name = None
        comp_type = "Company"
        sector = None
        # Search through cache for the parent/category references
        for entities in cache.values():
            for ent in entities:
                parent_name = ent.get("parent")
                if parent_name:
                    pk = make_dedup_key(parent_name)
                    pcid = name_to_cid.get(pk) or str(
                        uuid.uuid5(uuid.NAMESPACE_URL, f"competitor:{pk}")
                    )
                    if pcid == cid:
                        name = parent_name
                        sector = ent.get("sector")
                        break
                cat_name = ent.get("category")
                if cat_name:
                    ck = make_dedup_key(cat_name)
                    ccid = name_to_cid.get(ck) or str(
                        uuid.uuid5(uuid.NAMESPACE_URL, f"competitor:{ck}")
                    )
                    if ccid == cid:
                        name = cat_name
                        comp_type = "Category"
                        sector = ent.get("sector")
                        break
            if name:
                break

        if name:
            node_rows.append(
                {
                    "id": cid,  # Use canonical_id as id for synthesized nodes
                    "canonical_id": cid,
                    "label": name,
                    "original_label": "",
                    "original_id": "",
                    "competitor_type": comp_type,
                    "sector": sector or "",
                    "year": 0,
                    "section": "",
                    "link": "",
                    "relationship": "",
                }
            )

    # Write output
    output_dir.mkdir(parents=True, exist_ok=True)

    nodes_df = pd.DataFrame(node_rows)
    nodes_path = output_dir / "nodes_competitor_resolved.parquet"
    nodes_df.to_parquet(nodes_path, index=False)
    print(f"  nodes_competitor_resolved: {len(nodes_df)} rows -> {nodes_path}")

    competes_df = pd.DataFrame(competes_rows)
    competes_path = output_dir / "edges_competes_resolved.parquet"
    competes_df.to_parquet(competes_path, index=False)
    print(f"  edges_competes_resolved: {len(competes_df)} rows -> {competes_path}")

    ref_df = pd.DataFrame(ref_rows)
    ref_path = output_dir / "edges_competitor_has_reference_resolved.parquet"
    ref_df.to_parquet(ref_path, index=False)
    print(f"  edges_competitor_has_reference_resolved: {len(ref_df)} rows -> {ref_path}")

    sub_df = pd.DataFrame(
        [{"source_node": s, "target_node": t} for s, t in subsidiary_rows],
        columns=["source_node", "target_node"],
    )
    sub_path = output_dir / "edges_subsidiary_of.parquet"
    sub_df.to_parquet(sub_path, index=False)
    print(f"  edges_subsidiary_of: {len(sub_df)} rows -> {sub_path}")

    cat_df = pd.DataFrame(
        [{"source_node": s, "target_node": t} for s, t in in_category_rows],
        columns=["source_node", "target_node"],
    )
    cat_path = output_dir / "edges_in_category.parquet"
    cat_df.to_parquet(cat_path, index=False)
    print(f"  edges_in_category: {len(cat_df)} rows -> {cat_path}")

    # Summary
    print(f"\n  Unique canonical entities: {len(canonical_entities) + len(synthesized_cids)}")
    type_counts = Counter(r["competitor_type"] for r in node_rows)
    for t, c in type_counts.most_common():
        print(f"    {t}: {c}")
    sector_counts = Counter(r["sector"] for r in node_rows if r["sector"])
    print(f"\n  Sector distribution (top 10):")
    for s, c in sector_counts.most_common(10):
        print(f"    {s}: {c}")


# ====================================================================
# Main
# ====================================================================


async def run_resolution(
    parquet_path: Path,
    output_dir: Path,
    model_name: str,
    batch_size: int = 30,
    max_concurrent: int = 5,
    dry_run: bool = False,
):
    """Main resolution pipeline."""
    # Load data
    print(f"Reading {parquet_path} ...")
    df = pd.read_parquet(parquet_path)
    print(f"  {len(df)} rows, {df['label'].nunique()} unique labels")

    # Load edges
    parquet_dir = parquet_path.parent
    edges_competes = pd.read_parquet(parquet_dir / "edges_competes.parquet")
    edges_ref = pd.read_parquet(parquet_dir / "edges_competitor_has_reference.parquet")
    print(f"  {len(edges_competes)} COMPETES_WITH edges")
    print(f"  {len(edges_ref)} COMPETITOR_HAS_REFERENCE edges")

    # ── Phase 1 ──
    print("\n── Phase 1: Deterministic Pre-Dedup ──")
    freq = df["label"].value_counts().to_dict()
    unique_labels = sorted(df["label"].dropna().unique().tolist())
    label_to_rep = phase1_dedup(unique_labels, freq)
    representatives = sorted(set(label_to_rep.values()))
    print(f"  {len(unique_labels)} unique labels -> {len(representatives)} dedup groups")

    # ── Phase 2 ──
    print("\n── Phase 2: LLM Resolution ──")
    sectors = load_sectors()
    system_prompt = build_system_prompt(sectors)
    valid_sectors = {s["sector"] for s in sectors}
    print(f"  {len(sectors)} sectors loaded")

    cache = load_cache()
    remaining = [l for l in representatives if l not in cache]
    print(f"  {len(cache)} already cached, {len(remaining)} to classify")

    if dry_run:
        n_batches = (len(remaining) + batch_size - 1) // batch_size
        print(f"\n  DRY RUN: would classify {len(remaining)} labels in {n_batches} batches")
        print(f"  System prompt: {len(system_prompt)} chars (~{len(system_prompt)//4} tokens)")
        return

    if remaining:
        client = genai.Client(vertexai=True, project=VERTEX_PROJECT, location="global")
        model_id = get_model_id(model_name, "vertex")
        print(f"\n  Model: {model_id}")
        print(f"  Batch size: {batch_size}, Max concurrent: {max_concurrent}")

        batches = [
            remaining[i : i + batch_size]
            for i in range(0, len(remaining), batch_size)
        ]
        total_batches = len(batches)
        print(f"  {total_batches} batches to process\n", flush=True)

        semaphore = asyncio.Semaphore(max_concurrent)
        start_time = time.time()
        save_interval = 50  # Save cache every 50 batches

        # Process in chunks to save incrementally
        for chunk_start in range(0, total_batches, save_interval):
            chunk_end = min(chunk_start + save_interval, total_batches)
            chunk_batches = batches[chunk_start:chunk_end]

            tasks = [
                classify_batch(
                    client,
                    model_id,
                    system_prompt,
                    batch,
                    semaphore,
                    chunk_start + i + 1,
                    total_batches,
                )
                for i, batch in enumerate(chunk_batches)
            ]
            results = await asyncio.gather(*tasks)

            # Merge results into cache
            new_entries = 0
            for batch_result in results:
                for label, entities in batch_result.items():
                    cache[label] = entities
                    new_entries += 1

            # Save cache incrementally
            save_cache(cache)
            elapsed = time.time() - start_time
            done = chunk_end
            rate = done / elapsed * 60 if elapsed > 0 else 0
            remaining_batches = total_batches - done
            eta = remaining_batches / rate if rate > 0 else 0
            print(
                f"  -- Checkpoint: {done}/{total_batches} batches, "
                f"{len(cache)} cached, +{new_entries} new, "
                f"{rate:.0f} batches/min, ETA {eta:.1f} min",
                flush=True,
            )

        elapsed = time.time() - start_time
        print(f"\n  Classified {len(remaining)} labels in {elapsed:.1f}s")
        print(f"  Throughput: {len(remaining)/elapsed*60:.0f} labels/min")
        print(f"  Cache saved: {len(cache)} total entries", flush=True)

    # Check coverage
    uncached = [l for l in representatives if l not in cache]
    if uncached:
        print(f"\n  WARNING: {len(uncached)} labels not in cache")

    # ── Phase 3 ──
    print("\n── Phase 3: Post-LLM Canonical Dedup ──")
    canonical_entities, name_to_cid = phase3_canonical_dedup(cache)
    print(f"  {sum(len(v) for v in cache.values())} resolved entity mentions")
    print(f"  -> {len(canonical_entities)} unique canonical entities")

    type_counts = Counter(e["competitor_type"] for e in canonical_entities.values())
    for t, c in type_counts.most_common():
        print(f"    {t}: {c}")

    # ── Phase 5 ──
    print("\n── Phase 5: Output Generation ──")
    phase5_generate_output(
        df, label_to_rep, cache, canonical_entities, name_to_cid,
        edges_competes, edges_ref, output_dir,
    )

    print("\nDone.")


def main():
    parser = argparse.ArgumentParser(description="Resolve competitor entities")
    parser.add_argument(
        "--model", default="gemini-3-flash", help="Model name (default: gemini-3-flash)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=10, help="Labels per batch (default: 10)"
    )
    parser.add_argument(
        "--max-concurrent", type=int, default=5, help="Max concurrent requests (default: 5)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show plan without calling LLM"
    )
    parser.add_argument("--input", default=None, help="Input parquet path")
    parser.add_argument("--output-dir", default=None, help="Output directory for parquet files")
    args = parser.parse_args()

    pipeline_dir = SCRIPT_DIR.parent
    default_input = (
        pipeline_dir / "output" / args.model / "parquet" / "nodes_competitor.parquet"
    )
    input_path = Path(args.input) if args.input else default_input
    output_dir = (
        Path(args.output_dir) if args.output_dir else input_path.parent
    )

    if not input_path.exists():
        print(f"ERROR: {input_path} not found")
        sys.exit(1)

    print("=" * 60)
    print("Competitor Entity Resolution")
    print("=" * 60)
    asyncio.run(
        run_resolution(
            parquet_path=input_path,
            output_dir=output_dir,
            model_name=args.model,
            batch_size=args.batch_size,
            max_concurrent=args.max_concurrent,
            dry_run=args.dry_run,
        )
    )


if __name__ == "__main__":
    main()
