# Entity Normalization — Work Journal

## Principles

### Non-Destructive Output

Always write enrichment results to a **new file**, never overwrite source parquet.

| Entity | Source File | Enriched File |
|---|---|---|
| Risk | `nodes_risk.parquet` | `nodes_risk_categorized.parquet` |
| Competitor | `nodes_competitor.parquet` | `nodes_competitor_resolved.parquet` |
| Market | `nodes_market.parquet` | `nodes_market_categorized.parquet` |

The original extracted data is the source of truth. The KuzuDB loader picks which version to use.

### Use Non-Thinking Models for Classification

For structured output tasks (categorization, classification, entity splitting), use non-thinking models like `gemini-2.0-flash`. Thinking models (`gemini-2.5-flash`) generate hidden reasoning tokens at $3.50/M that can inflate costs 80x with no quality benefit for these tasks.

---

## Entity Type Comparison

| | Risk | Competitor | Market |
|---|---|---|---|
| **Rows** | 17,265 | 14,572 | 24,018 |
| **Unique labels** | 10,299 | 6,546 | 15,736 |
| **Approach** | Multi-label category | Entity resolution + graph edges | Multi-dimensional classification |
| **Classification dims** | 1 (17 risk categories) | 2 (type, sector) + edges | 3 (geographic_region, sector, product_category) |
| **Entity splitting** | No | Yes (726 multi-entity labels) | No |
| **Dedup needed** | No | Critical (650+ groups) | Yes (case variants, abbreviations) |
| **Output** | Added column to parquet | Separate parquet + edge files | Add columns to parquet |
| **New graph edges** | None | SUBSIDIARY_OF, IN_CATEGORY | None |
| **Row count change** | No | Yes (+1,867 from splits) | No |
| **Model used** | gemini-3-flash-preview | gemini-2.5-flash (thinking!) | gemini-2.0-flash (non-thinking) |
| **Actual cost** | ~$0.50 | ~$19 (thinking tokens) | ~$0.30 (estimated) |
| **Time** | ~15 min | ~14 min (final run) | ~5 min (estimated) |

## LLM Cost Analysis

### The Thinking Model Trap

gemini-2.5-flash is a **thinking model** — it generates hidden reasoning tokens billed at $3.50/M, about 6x the visible output rate ($0.60/M). These thinking tokens are typically 10-20x the visible output volume.

| Model | Input $/M | Output $/M | Thinking $/M | Thinking? |
|---|---|---|---|---|
| gemini-2.0-flash | $0.10 | $0.40 | — | No |
| gemini-2.5-flash-lite | $0.075 | $0.30 | — | No |
| gemini-2.5-flash | $0.15 | $0.60 | **$3.50** | **Yes** |
| gemini-3-flash-preview | $0.10 | $0.40 | — | No |

**The competitor run used gemini-2.5-flash**, resulting in ~$19 cost instead of the estimated $0.24 (80x underestimate). The thinking tokens dominated: ~5.4M tokens × $3.50/M = $18.90.

### Rule of Thumb

For classification/categorization tasks (structured output, no complex reasoning):
- **Use non-thinking models** (gemini-2.0-flash, 2.5-flash-lite, or 3-flash)
- gemini-2.0-flash is the sweet spot: GA (reliable), non-thinking ($0.10-$0.40/M), handles batch-50
- Reserve thinking models for tasks that genuinely benefit from chain-of-thought

---

## Session: 2026-04-03 → 2026-04-04

### Context

This project extracts entities (markets, risks, opportunities, competitors) from SEC 10-K filings using Gemini LLM, stores them in Parquet files, and loads into KuzuDB for GraphXR visualization. The `entity_normalization/` sub-project enriches the raw extracted entities with classification and deduplication.

Prior to this session, risk categorization was complete (17 categories, 99.9% coverage). This session tackled **competitor entity resolution**.

---

### Phase 1: Data Exploration (2026-04-03 afternoon)

**Goal:** Understand the competitor data before designing a solution.

**Findings from `nodes_competitor.parquet` (gemini-3-flash extraction):**

| Metric | Value |
|---|---|
| Total rows | 14,572 |
| Unique labels | 6,546 |
| Singletons | 3,779 (58%) |
| Labels with company suffixes (Inc., Corp., etc.) | 934 |
| Category/generic labels (plural nouns) | ~2,554 |
| Multi-entity labels (lists of companies) | ~726 |

**Key data challenges identified:**

1. **Multi-entity labels** — `"Apple, Google, Microsoft"` or `"Cloud Service Providers (Amazon, Alphabet, Microsoft)"` need splitting
2. **Name deduplication** — 650+ groups like `Walmart` / `Walmart Inc.` / `Walmart, Inc.`
3. **Subsidiary disambiguation** — AWS vs Amazon vs Amazon Prime (same corporate family, different specificity)
4. **Mixed entity types** — companies, category groups, and vague generics all in the same column
5. **Non-competitor relationships** — 36% of rows are actually partners, acquisitions, investments (data quality issue from extraction)

**Also explored:**
- The old `entity_resolution/` approach (local Ollama gemma3:12b, one label at a time, CSV output)
- Senzing's entity resolution capabilities via MCP tools (Dynamic ER with in-memory SQLite, `NAME_ORG` matching)

---

### Phase 2: Design (2026-04-03 evening)

**Key design decisions made through discussion:**

1. **Graph-first approach** — Parent companies and categories become edges (`SUBSIDIARY_OF`, `IN_CATEGORY`) rather than flat columns. This enables traversal in GraphXR:
   ```
   Company A → [COMPETES_WITH] → AWS → [SUBSIDIARY_OF] → Amazon
   Company A → [COMPETES_WITH] → Google Cloud → [IN_CATEGORY] → Cloud Service Providers
   ```

2. **"Category with examples in parens" splitting** — `Cloud Service Providers (Amazon, Alphabet, Microsoft)` splits into 3 company entities + 1 category entity, linked by `IN_CATEGORY`. Not kept as a single category.

3. **Non-destructive output** — Separate `nodes_competitor_resolved.parquet` + new edge files. Original `nodes_competitor.parquet` untouched.

4. **5-phase pipeline:**
   - Phase 1: Deterministic pre-dedup (suffix stripping, case-fold)
   - Phase 2: LLM resolution (split, canonicalize, classify, link)
   - Phase 3: Post-LLM canonical dedup
   - Phase 4: Optional Senzing validation (separate script, not implemented yet)
   - Phase 5: Output generation

**Files created:**
- `COMPETITOR_RESOLUTION.md` — Full design doc with graph model, pipeline stages, output schema, Cypher queries
- `COMPETITOR_TAXONOMY.md` — Bottom-up derivation of 3 types and 13 sectors
- `data/competitor_types.csv` — Company / Category / Generic definitions
- `data/competitor_sectors.csv` — 13 sectors with keywords and examples

**Commit:** `3ccfd77` — Add competitor resolution design and taxonomy

---

### Phase 3: Implementation (2026-04-03 night)

**Script:** `resolve_competitors.py`

Modeled after `categorize_risks.py` but significantly more complex:
- Entity splitting (not just classification)
- Canonical dedup across split results
- Multiple output files (nodes + 4 edge types)
- Parent/category relationship extraction

**Commit:** `da60de5` — Add competitor resolution script and pipeline diagram

---

### Phase 4: Execution and Debugging (2026-04-03 night → 2026-04-04)

**Run 1: gemini-3-flash-preview, batch-size 30, concurrency 5**
- Result: **93% failure rate** — JSON parse errors
- Root cause: Output truncation. 30 labels produce ~7,500 chars of JSON. The model stopped generating mid-response, producing invalid JSON.
- Only 390/5,515 labels cached.

**Run 2: gemini-3-flash-preview, batch-size 10, concurrency 10**
- Result: **~60% timeout rate** — requests hung for >60s
- Root cause: Preview model rate limiting. 10 concurrent requests to a preview endpoint caused queueing.
- Bug found: Phase 5 had O(n²) complexity from DataFrame scans inside a loop (14,572 × 14,572 lookups). Fixed by pre-indexing edges into dicts.
- Bug found: No incremental cache saving — all progress lost on crash. Fixed with checkpoint saves every 50 batches.
- Bug found: No per-request timeout — one hung request blocked the entire `asyncio.gather`. Fixed with `asyncio.wait_for(timeout=60)`.
- Bug found: Python stdout buffering hid all progress output. Fixed with `flush=True`.
- Progress: 720/5,515 cached before process was killed.

**Run 3: gemini-2.5-flash (GA), batch-size 10, concurrency 5**
- Result: **100% success rate**, 14 minutes, 342 labels/min
- GA model was dramatically more reliable than the preview model.
- Also added JSON repair for truncated responses (close open braces/brackets, strip trailing commas).
- All 5,515 labels classified.

**Lessons learned:**
1. **Use GA models for production runs.** gemini-3-flash-preview had both truncation and timeout issues. gemini-2.5-flash (GA) was flawless.
2. **Small batch sizes for structured output.** 30 labels exceeded the model's reliable output length. 10 labels = ~3,000 chars output, well within limits.
3. **Always save incrementally.** The first two runs lost all progress on failure.
4. **Always add timeouts to async calls.** One hung request can block everything.
5. **Pre-index lookup tables.** O(n²) DataFrame scans killed Phase 5 performance.

**Commit:** `332a362` — Add competitor resolution results and cache (100% coverage)

---

### Final Results

**Output files in `output/gemini-3-flash/parquet/`:**

| File | Rows | Description |
|---|---|---|
| `nodes_competitor_resolved.parquet` | 15,333 | Resolved competitor entities |
| `edges_competes_resolved.parquet` | 15,243 | Company → Competitor |
| `edges_competitor_has_reference_resolved.parquet` | 15,243 | Competitor → Reference |
| `edges_subsidiary_of.parquet` | 197 | Subsidiary → Parent company |
| `edges_in_category.parquet` | 57 | Company → Category hub |

**Entity statistics:**

| Metric | Value |
|---|---|
| Canonical entities | 5,455 |
| — Company type | 4,193 |
| — Category type | 815 |
| — Generic type | 447 |
| SUBSIDIARY_OF relationships | 197 |
| IN_CATEGORY relationships | 57 |

**Sector distribution (canonical entities):**

| Sector | Count |
|---|---|
| Financial Services | 367 |
| Technology | 268 |
| Energy | 198 |
| Retail & E-commerce | 191 |
| Automotive & Transportation | 157 |
| Industrial & Manufacturing | 141 |
| Healthcare & Pharma | 131 |
| Consumer Goods | 115 |
| Telecommunications & Media | 100 |
| Materials & Chemicals | 66 |
| Real Estate | 49 |
| Utilities | 45 |
| Aerospace & Defense | 36 |
| (no sector) | 3,591 |

**Sample SUBSIDIARY_OF edges:** Citibank→Citigroup, BPX Energy→BP, TD Ameritrade→Charles Schwab, Häagen-Dazs Japan→General Mills, Protective Life→Dai-ichi Life

**Sample IN_CATEGORY edges:** Google→Mobile OS Providers, Amazon→Big Tech, Volkswagen→Major Vehicle Manufacturers, PayPal→Digital Wallet Providers

**Cost:** ~$0.35 total across all runs (Gemini Flash is very cheap).

---

### What's Not Done Yet

1. **Phase 4: Senzing validation** — Optional dedup verification for Company-type entities using Senzing Dynamic ER. Script not yet written (`resolve_competitors_senzing.py`).

2. **KuzuDB loader update** — `load_kuzu.py` needs an option to load resolved/categorized files and the two new edge tables (`SUBSIDIARY_OF`, `IN_CATEGORY`).

3. **High null-sector rate (competitors)** — 66% of canonical entities have no sector. The old Ollama approach had 23%. May need a follow-up pass with sector-focused prompting, or using the `relationship` column for context.

4. **Opportunity normalization** — Lower priority, similar approach to markets.

---

## Session: 2026-04-04

### Market Categorization

**Approach:** Column-based classification (like risks), not entity resolution (like competitors). Markets don't need entity splitting — they're descriptive labels, not company name lists.

**3 classification dimensions:**
- **geographic_region** (7 regions): Global, North America, Europe, Asia Pacific, Latin America, Middle East & Africa, Russia & CIS
- **sector** (13 sectors): reused from competitor taxonomy
- **product_category** (12 categories): Cloud & Software, Renewable Energy, Electric Vehicles & Mobility, Oil & Gas, Healthcare Services, Financial Products, E-commerce & Digital, Telecommunications, Power & Utilities, Food & Agriculture, Construction & Infrastructure, Defense & Space

**Model selection:**
- First attempt: `gemini-2.0-flash` — 404 NOT_FOUND (not available on this project via `location=global`)
- Successful run: `gemini-2.5-flash-lite` — non-thinking, cheapest option

**Results:**

| Metric | Value |
|---|---|
| Labels classified | 12,987 / 12,991 (99.97%) |
| Time | 3.7 min (260 batches of 50) |
| Model | gemini-2.5-flash-lite |
| Cost | ~$0.20 |
| Batch success rate | 260/260 (100%) |

**Coverage:**

| Dimension | Rows | % of 24,018 |
|---|---|---|
| geographic_region | 9,172 | 38.2% |
| sector | 17,328 | 72.1% |
| product_category | 11,190 | 46.6% |

Geographic is lower because many markets are product/sector-specific without a geographic component (e.g., "Renewable Diesel", "Biosimilars"). This is expected — not every market has a geography.

**Output:** `nodes_market_categorized.parquet` (non-destructive, original `nodes_market.parquet` untouched)

### Risk Categorization Fix (non-destructive)

The original `categorize_risks.py` overwrote `nodes_risk.parquet` in place. Fixed without re-running:
1. Split existing `nodes_risk.parquet` (which had `risk_categories` column) into:
   - `nodes_risk.parquet` — restored to original 6 columns (id, label, year, section, link, description)
   - `nodes_risk_categorized.parquet` — full 7 columns including risk_categories
2. Updated `categorize_risks.py` to output `nodes_risk_categorized.parquet` by default

**Commit:** `04ec23b` — Add market categorization

### NormalizedCompetitor & Graph Model

**Design decision: Two-level competitor model** (documented in `GRAPH_MODEL.md`)

The key insight: 14,572 Competitor nodes contain only 5,455 unique entities. Rather than replacing the original Competitor nodes, we add a normalization layer on top:

```
Company →[COMPETES_WITH]→ Competitor →[INSTANCE_OF]→ NormalizedCompetitor
                                                         │
                                                         ├→[SUBSIDIARY_OF]→ NormalizedCompetitor
                                                         └→[IN_MARKET_CATEGORY]→ MarketCategory
```

**Why this model:**
1. Non-destructive — original graph unchanged, normalization is additive
2. INSTANCE_OF handles entity splitting naturally (many-to-many)
3. Entity-level properties (type, sector, product_category) separate from mention-level (year, section, relationship)
4. MarketCategory nodes are **shared with Markets** — creates cross-entity bridge for queries like "companies entering Cloud markets AND their Cloud competitors"

**MarketCategory assignment:**
- Ran LLM pass on 5,455 NormalizedCompetitors to assign 12 product categories (shared with market categorization)
- Model: gemini-2.5-flash-lite, 72 seconds, ~$0.08
- Coverage: 3,905/5,455 (71.6%)

**Output files:**

| File | Rows |
|---|---|
| `nodes_normalized_competitor.parquet` | 5,455 (canonical entities) |
| `edges_instance_of.parquet` | 15,243 (Competitor → NormalizedCompetitor) |
| `nodes_market_category.parquet` | 12 (shared hub nodes) |
| `edges_in_market_category.parquet` | 3,905 (NC → MarketCategory) |

---

### Git History (all sessions)

```
30c78d3 Add NormalizedCompetitor market categories and output files
fedc1fc Add graph model design and competitor market category script
351c682 Fix risk categorization to non-destructive output, update journal
04ec23b Add market categorization (3 dimensions, 99.97% coverage)
f4950ac Add entity normalization work journal
332a362 Add competitor resolution results and cache (100% coverage)
da60de5 Add competitor resolution script and pipeline diagram
3ccfd77 Add competitor resolution design and taxonomy (3 types, 13 sectors)
a6b009b Add risk categorization results and cache (99.9% coverage)
026f8d6 Add risk categorization script using Gemini LLM in batches
80fc9db Document analysis for skipping normalized_risk layer
b059cfc Add risk category taxonomy for entity normalization (17 categories)
```

### File Tree (entity_normalization/)

```
pipeline/entity_normalization/
├── categorize_risks.py                         Risk categorization
├── categorize_markets.py                       Market categorization
├── resolve_competitors.py                      Competitor entity resolution
├── categorize_competitor_markets.py            NormalizedCompetitor market categories
├── COMPETITOR_RESOLUTION.md                    Design doc with pipeline diagram
├── COMPETITOR_TAXONOMY.md                      Type + sector derivation analysis
├── GRAPH_MODEL.md                              Two-level graph model, shared hub nodes
├── NORMALIZED_RISK_ANALYSIS.md                 Why normalized_risk was skipped
├── RISK_CATEGORIES.md                          Risk category taxonomy
├── JOURNAL.md                                  This file
└── data/
    ├── risk_categories.csv                     17 risk categories
    ├── risk_label_categories_cache.json        Risk LLM cache (10,290 entries)
    ├── competitor_types.csv                    3 competitor types
    ├── competitor_sectors.csv                  13 sectors (shared with markets)
    ├── competitor_resolved_cache.json          Competitor LLM cache (5,515 entries)
    ├── competitor_market_category_cache.json   NC market category cache (5,456 entries)
    ├── market_geographic_regions.csv           7 geographic regions
    ├── market_product_categories.csv           12 product categories (shared with competitors)
    └── market_label_categories_cache.json      Market LLM cache (12,987 entries)
```

### Output Files (output/gemini-3-flash/parquet/)

```
# Original (untouched)
nodes_risk.parquet                              17,265 rows
nodes_competitor.parquet                        14,572 rows
nodes_market.parquet                            24,018 rows

# Enriched (non-destructive)
nodes_risk_categorized.parquet                  17,265 rows (+risk_categories)
nodes_competitor_resolved.parquet               15,333 rows (+canonical_id, type, sector)
nodes_market_categorized.parquet                24,018 rows (+geographic_region, sector, product_category)
nodes_normalized_competitor.parquet              5,455 rows (canonical entities)
nodes_market_category.parquet                       12 rows (shared hub nodes)

# New edges
edges_instance_of.parquet                       15,243 rows (Competitor → NormalizedCompetitor)
edges_subsidiary_of.parquet                        197 rows (NC → NC)
edges_in_market_category.parquet                 3,905 rows (NC → MarketCategory)
edges_competes_resolved.parquet                 15,243 rows
edges_competitor_has_reference_resolved.parquet  15,243 rows
edges_in_category.parquet                           57 rows
```

### Output Files (output/gemini-3-flash/parquet/)

```
# Original (untouched)
nodes_risk.parquet                              17,265 rows
nodes_competitor.parquet                        14,572 rows
nodes_market.parquet                            24,018 rows

# Enriched (non-destructive)
nodes_risk_categorized.parquet                  17,265 rows (+risk_categories)
nodes_competitor_resolved.parquet               15,333 rows (+canonical_id, competitor_type, sector)
nodes_market_categorized.parquet                24,018 rows (+geographic_region, sector, product_category)
edges_competes_resolved.parquet                 15,243 rows
edges_competitor_has_reference_resolved.parquet  15,243 rows
edges_subsidiary_of.parquet                        197 rows (NEW)
edges_in_category.parquet                           57 rows (NEW)
```
