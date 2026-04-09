# Competitor Entity Resolution

## Goal

Resolve 6,546 unique competitor labels (14,572 rows) from `nodes_competitor.parquet` into clean, deduplicated entities with type and sector classification. Model subsidiary and category relationships as **graph edges** rather than flat columns, enabling rich traversal in GraphXR.

## Graph Model

The resolved competitor data introduces two new relationship types:

```
Company ─[COMPETES_WITH]─> Competitor (Company|Category|Generic)

Competitor ─[SUBSIDIARY_OF]─> Competitor    (AWS -> Amazon)
Competitor ─[IN_CATEGORY]──> Competitor     (AWS -> Cloud Service Providers)
```

This enables powerful graph traversal:

```
                                  ┌─ Google Cloud ─[SUBSIDIARY_OF]─> Alphabet
                                  │
Company A ─[COMPETES_WITH]─> Cloud Service  ─[IN_CATEGORY]<── AWS ─[SUBSIDIARY_OF]─> Amazon
                            Providers       │
                                  └─[IN_CATEGORY]<── Azure ─[SUBSIDIARY_OF]─> Microsoft
```

**Why edges instead of columns:**
- `SUBSIDIARY_OF` edges let you visually see Google Cloud linked to Alphabet, AWS linked to Amazon
- `IN_CATEGORY` edges turn categories into navigable hub nodes — click "Cloud Service Providers" to see all companies that compete in that space
- Both dimensions are traversable in Cypher queries and GraphXR exploration

## Data Characteristics

| Metric | Value |
|---|---|
| Total rows | 14,572 |
| Unique labels | 6,546 |
| Singletons (1x) | 3,779 (58%) |
| Appear 2-5x | 2,373 (36%) |
| Appear 6+x | 394 (6%) |
| Labels with company suffixes | 934 (14%) |
| Category/generic labels | ~2,554 (39%) |
| Ambiguous (no clear signal) | ~3,058 (47%) |
| Multi-entity labels | ~726 (11%) |
| Non-competitor relationships | ~5,288 rows (36%) |

## Challenges

### 1. Multi-Entity Labels (726 labels, 1,084 rows)

Some labels pack multiple entities into one string. Four sub-patterns:

| Pattern | Count | Example |
|---|---|---|
| Two-entity "and" pair | 435 | `Affirm and Afterpay` |
| Pure company list (3+) | 118 | `BAE Systems, Boeing, General Dynamics, Lockheed Martin` |
| Slash pair | 117 | `Alphabet/Google`, `Genentech/Roche` |
| Category with examples in parens | 56 | `Cloud Service Providers (Amazon, Alphabet, Microsoft)` |

**Complication:** Many "and" labels are categories, not multi-entity (`Banks and Credit Unions`, `AI and Automation Providers`). The LLM must distinguish these using world knowledge.

**Complication:** Some slash pairs are one entity (`Anheuser-Busch InBev SA/NV`) not two.

**Decision on "Category with examples in parens":**
Labels like `Cloud Service Providers (Amazon, Alphabet, Microsoft)` are split into 3 company entities + 1 category entity, linked by `IN_CATEGORY` edges. This gives us both the specific companies as COMPETES_WITH targets and the category as a navigable hub.

### 2. Name Deduplication (650+ duplicate groups)

Same entity appears with different surface forms:

| Variant Group | Rows |
|---|---|
| `Walmart` (41x) / `Walmart Inc.` (20x) / `Walmart, Inc.` (4x) | 65 |
| `NVIDIA` (2x) / `NVIDIA Corporation` (13x) / `Nvidia` (3x) / `Nvidia Corporation` (4x) | 22 |
| `FinTech Companies` (6x) / `Fintech Companies` (3x) / `FinTech companies` (4x) / `Fintech companies` (7x) | 20 |
| `The Boeing Company` (19x) / `Boeing` (27x) | 46 |

### 3. Subsidiary / Product / Division Disambiguation

Entities from the same corporate family appear at different levels of specificity:

| Parent | Subsidiaries / Products / Divisions |
|---|---|
| Amazon | `AWS`, `Amazon Prime`, `Amazon Web Services`, `Amazon.com, Inc.` |
| Google / Alphabet | `Google Cloud`, `GCS`, `Google Video`, `YouTube`, `Alphabet Inc.` |
| Meta | `Facebook`, `Instagram`, `WhatsApp`, `Meta Platforms` |
| Microsoft | `Azure`, `Microsoft Teams`, `LinkedIn`, `Xbox` |
| Berkshire Hathaway | `GEICO`, `BNSF`, `Berkshire Hathaway Energy` |

**Decision: Keep subsidiaries as separate entities, link with SUBSIDIARY_OF edges.**

Rationale:
- Preserves the specificity of the SEC filing (a company competes with AWS specifically, not Amazon broadly)
- SUBSIDIARY_OF edges let you visually navigate the corporate hierarchy
- In GraphXR, you can expand from AWS to Amazon to see the parent, or collapse the subgraph

The LLM returns `parent` only when the entity is a well-known subsidiary/division/product of a larger company. For standalone companies, `parent` is null. Parent entities referenced by the LLM that don't appear as competitor labels get created as new Competitor nodes (type=Company, with whatever sector the LLM inferred).

### 4. Entity Type Classification

Three types (same as old approach):

| Type | Description | Example | Role in Graph |
|---|---|---|---|
| **Company** | Specific named company | `NVIDIA`, `Boeing`, `AWS` | Leaf node or SUBSIDIARY_OF source |
| **Category** | Named group/class of competitors | `Online Retailers`, `Cloud Service Providers` | Hub node via IN_CATEGORY |
| **Generic** | Vague/descriptive, not a specific group | `Large global competitors`, `Third-party providers` | Leaf node, limited graph value |

Category-type nodes are especially valuable in the graph — they become hubs that connect otherwise-unrelated companies:

```
Walmart ─[IN_CATEGORY]─> Online Retailers <─[IN_CATEGORY]─ Amazon
Target  ─[IN_CATEGORY]─> Online Retailers <─[IN_CATEGORY]─ Shopify
```

### 5. Sector Classification

13 sectors (old 12 + Materials & Chemicals):

| Sector |
|---|
| Technology |
| Financial Services |
| Healthcare & Pharma |
| Energy |
| Retail & E-commerce |
| Industrial & Manufacturing |
| Consumer Goods |
| Automotive & Transportation |
| Aerospace & Defense |
| Telecommunications & Media |
| Utilities |
| Real Estate |
| Materials & Chemicals |

Null for entities where sector is unclear or spans many sectors.

### 6. Non-Competitor Relationships (36% of rows)

The `relationship` column reveals that many "competitors" are actually partners, acquisitions, investments, customers, or suppliers. This is a data quality issue from the extraction LLM, not something the resolution pipeline should fix.

**Decision: Not addressed in this pipeline.** We classify the entity itself (type, sector), not the relationship. A future pass could add a `relationship_type` dimension using the `relationship` column.

## Pipeline Overview

```
                         ┌─────────────────────────────────────┐
                         │         Configuration               │
                         │                                     │
                         │  COMPETITOR_TAXONOMY.md              │
                         │  ├── 3 types: Company/Category/     │
                         │  │   Generic (bottom-up analysis)    │
                         │  └── 13 sectors (keyword coverage    │
                         │      analysis, gap analysis)         │
                         │                                     │
                         │  data/competitor_types.csv           │
                         │  data/competitor_sectors.csv         │
                         └──────────────┬──────────────────────┘
                                        │
                    ┌───────────────────┼───────────────────────┐
                    ▼                   ▼                       ▼
        ┌──────────────────┐ ┌──────────────────┐ ┌────────────────────┐
        │  Phase 1         │ │  Phase 2         │ │  Phase 3           │
        │  Deterministic   │ │  LLM Resolution  │ │  Post-LLM          │
        │  Pre-Dedup       │→│  (Gemini Flash)  │→│  Canonical Dedup   │
        │                  │ │                  │ │                    │
        │  Strip suffixes, │ │  Split multi-    │ │  Merge collisions  │
        │  case-fold,      │ │  entity labels,  │ │  from splits,      │
        │  group variants  │ │  canonicalize,   │ │  assign canonical   │
        │                  │ │  classify type/  │ │  IDs               │
        │  6,546 → ~5,500  │ │  sector, identify│ │  ~7,000 → ~5,500  │
        └──────────────────┘ │  parent/category │ └────────────────────┘
                             │  ~5,500 → ~7,000 │           │
                             └──────────────────┘           │
                                                            ▼
                                               ┌────────────────────┐
                                               │  Phase 4 (optional)│
                                               │  Senzing Validation│
                                               │                    │
                                               │  Dynamic ER on     │
                                               │  Company-type only │
                                               │  NAME_ORG matching │
                                               └────────┬───────────┘
                                                        │
                                                        ▼
                                               ┌────────────────────┐
                                               │  Phase 5           │
                                               │  Output Generation │
                                               │                    │
                                               │  nodes_competitor  │
                                               │  _resolved.parquet │
                                               │  edges_competes_   │
                                               │  resolved.parquet  │
                                               │  edges_subsidiary  │
                                               │  _of.parquet       │
                                               │  edges_in_category │
                                               │  .parquet          │
                                               └────────────────────┘
```

## Pipeline Design

### Phase 1: Deterministic Pre-Dedup (no LLM)

Build a normalization function to create **dedup keys** that group obvious variants:

**Steps applied in order:**
1. Strip leading `The ` (case-insensitive)
2. Strip parentheticals: `(Patterson Dental division)` -> removed
3. Strip trailing company suffixes (case-insensitive):
   `Inc.?`, `Corp.?`, `Corporation`, `LLC`, `L.L.C.`, `Ltd.?`, `Limited`,
   `Co.?`, `Company`, `Companies`, `plc`, `PLC`, `S.A.`, `N.V.`, `AG`,
   `GmbH`, `SE`, `L.P.`, `LP`
4. Strip trailing commas/periods
5. Case-fold to lowercase
6. Collapse whitespace, strip

**What this catches:**
- `Walmart` / `Walmart Inc.` / `Walmart, Inc.` -> `walmart`
- `Eisai Co., LTD` / `Eisai Co., LTD.` / `Eisai Co., Ltd.` -> `eisai`
- `FinTech Companies` / `Fintech companies` -> `fintech companies`

**What this deliberately does NOT catch:**
- `The Boeing Company` -> `boeing company` (not `boeing` — "Company" is stripped as suffix but "boeing company" != "boeing")
- `NVIDIA` / `Nvidia` -> `nvidia` / `nvidia` (this one works via case-fold)
- `Air Products and Chemicals, Inc.` stays intact (the "and" is part of the name)

The dedup key is conservative — only merges things we're 100% certain about.

**For each dedup group:** pick the most frequent variant as the representative label to send to the LLM.

**Estimated reduction: 6,546 -> ~5,500 unique groups**

### Phase 2: LLM Resolution (batched Gemini Flash)

Send ~5,500 representative labels to Gemini in batches of 30. For each label, the LLM returns entities and relationships:

**Example: Company list**
```json
{
  "label": "Apple, Google, Microsoft",
  "entities": [
    {"name": "Apple", "competitor_type": "Company", "sector": "Technology"},
    {"name": "Google", "competitor_type": "Company", "sector": "Technology", "parent": "Alphabet"},
    {"name": "Microsoft", "competitor_type": "Company", "sector": "Technology"}
  ]
}
```

**Example: Simple company**
```json
{
  "label": "The Boeing Company",
  "entities": [
    {"name": "Boeing", "competitor_type": "Company", "sector": "Aerospace & Defense"}
  ]
}
```

**Example: Subsidiary**
```json
{
  "label": "AWS",
  "entities": [
    {"name": "AWS", "competitor_type": "Company", "sector": "Technology", "parent": "Amazon"}
  ]
}
```

**Example: Category with examples in parens (SPLIT into companies + category)**
```json
{
  "label": "Cloud Service Providers (Amazon, Alphabet, Microsoft)",
  "entities": [
    {"name": "Cloud Service Providers", "competitor_type": "Category", "sector": "Technology"},
    {"name": "Amazon", "competitor_type": "Company", "sector": "Technology", "category": "Cloud Service Providers"},
    {"name": "Alphabet", "competitor_type": "Company", "sector": "Technology", "category": "Cloud Service Providers"},
    {"name": "Microsoft", "competitor_type": "Company", "sector": "Technology", "category": "Cloud Service Providers"}
  ]
}
```

**Example: Standalone category**
```json
{
  "label": "Online Retailers",
  "entities": [
    {"name": "Online Retailers", "competitor_type": "Category", "sector": "Retail & E-commerce"}
  ]
}
```

**Example: Generic**
```json
{
  "label": "Large global competitors",
  "entities": [
    {"name": "Large Global Competitors", "competitor_type": "Generic", "sector": null}
  ]
}
```

**LLM entity fields:**
| Field | Type | Description |
|---|---|---|
| `name` | STRING | Canonical entity name |
| `competitor_type` | STRING | Company / Category / Generic |
| `sector` | STRING or null | One of 13 sectors |
| `parent` | STRING or null | Parent company name -> generates SUBSIDIARY_OF edge |
| `category` | STRING or null | Category name -> generates IN_CATEGORY edge |

**LLM responsibilities:**
1. **Split** multi-entity labels into individual entities (including "Category (examples)" pattern)
2. **Canonicalize** names — `The Boeing Company` -> `Boeing`, `NVIDIA Corporation` -> `NVIDIA`
3. **Classify** type (Company / Category / Generic) and sector
4. **Identify parent** for known subsidiaries/divisions/products (-> SUBSIDIARY_OF edge)
5. **Identify category** when explicitly present in the label (-> IN_CATEGORY edge)

**Note on `category`:** The LLM only assigns `category` when it's explicitly present in the label (the parenthetical pattern, or when context is obvious). We do NOT ask the LLM to infer categories for every company — that would be scope creep and unreliable. Additional IN_CATEGORY edges emerge organically when the same category appears as a standalone label elsewhere in the data.

**Caching:** Results cached in `competitor_resolved_cache.json` (label -> entities list). Resumable across runs.

### Phase 3: Post-LLM Canonical Dedup

After all LLM results, apply Phase 1's normalization to the **LLM-returned `name` fields** to catch collisions:

```
Split "Apple, Google, Microsoft"                        -> name: "Apple"
Standalone label "Apple"                                -> name: "Apple"
Split "Cloud Service Providers (Amazon, ..., Microsoft)"-> name: "Microsoft"
                                                        -> all share dedup key "microsoft", merge
```

**Steps:**
1. Normalize each LLM-returned `name` using the same dedup-key function
2. Group by dedup key
3. For each group: pick the most frequent `name` as the final canonical name
4. Merge `competitor_type`, `sector`, `parent`, `category` (prefer non-null, most frequent)
5. Assign one stable canonical ID (UUID) per group

**Important:** `parent` and `category` reference entities **by name**. These names also go through canonical dedup, so `parent: "Amazon"` resolves to the same canonical ID as standalone label `"Amazon"`.

**Entities referenced by `parent` or `category` that don't appear as labels:** Phase 5 creates new Competitor nodes for them with whatever type/sector info is available. For example, if `"Alphabet"` appears as a `parent` reference but never as a competitor label, a new Company node is created.

### Phase 4: Senzing Validation (Optional, Company-type only)

For entities classified as `competitor_type: "Company"`, use Senzing's **Dynamic ER** (in-memory SQLite) as a validation/dedup step:

**What Senzing adds:**
- Built-in organization name matching (`NAME_ORG` feature) with fuzzy logic
- Can catch merges that deterministic dedup and LLM miss, e.g.:
  - `SK hynix` vs `SK Hynix` (casing in proper nouns)
  - `Express Scripts` vs `Express Scripts Inc.` (if Phase 1 missed it)
  - Name transpositions or abbreviation variants

**How it works:**
1. Filter resolved entities where `competitor_type == "Company"`
2. Map each to a Senzing record: `{"DATA_SOURCE": "SEC_COMPETITORS", "RECORD_ID": "<canonical_id>", "RECORD_TYPE": "ORGANIZATION", "NAME_ORG": "<canonical_name>"}`
3. Load into Senzing Dynamic ER (in-memory SQLite — no server needed)
4. Export resolved entities — Senzing groups records it thinks are the same entity
5. Review Senzing's merge proposals against our canonical IDs
6. Accept or reject each merge

**Limitations:**
- We only have organization names — no addresses, identifiers, or other features. This is Senzing's weakest matching mode.
- Senzing will likely be conservative with name-only matching (few false positives, some false negatives)
- Only useful for Company-type entities — Category/Generic labels are not real organizations

**Decision: Optional step, run after Phase 3.** If Senzing is installed, use it as a validation layer. If not, skip — the LLM + deterministic dedup should handle 95%+ of cases. Log Senzing's merge proposals for human review rather than auto-accepting.

**Installation:** `brew install senzingsdk-runtime-unofficial` (macOS) or see [sz_mem-v4](https://github.com/brianmacy/sz_mem-v4) for the in-memory pattern.

### Phase 5: Output Generation

Map every original row through the resolution chain and write output files.

**Example: "Cloud Service Providers (Amazon, Alphabet, Microsoft)"**
```
Original row (id=X, label="Cloud Service Providers (Amazon, Alphabet, Microsoft)")
  -> Phase 2 LLM -> 4 entities:
       Cloud Service Providers (Category, cid=CSP)
       Amazon (Company, cid=AMZ, category=CSP)
       Alphabet (Company, cid=ALP, category=CSP)
       Microsoft (Company, cid=MS, category=CSP)
  -> Phase 3 canonical dedup -> resolve to canonical IDs
  -> Output:
       3 Competitor rows: Amazon, Alphabet, Microsoft (each with new id, original_id=X)
       1 Competitor row: Cloud Service Providers (if not already created by another label)
       3 COMPETES_WITH edges: filing_company -> Amazon, Alphabet, Microsoft
       3 COMPETITOR_HAS_REFERENCE edges: Amazon/Alphabet/Microsoft -> original_reference
       3 IN_CATEGORY edges: Amazon/Alphabet/Microsoft -> Cloud Service Providers
```

**Example: "AWS" (subsidiary)**
```
Original row (id=Y, label="AWS")
  -> Phase 2 LLM -> 1 entity: AWS (Company, cid=AWS, parent=Amazon)
  -> Output:
       1 Competitor row: AWS
       1 Competitor row: Amazon (if not already created)
       1 COMPETES_WITH edge: filing_company -> AWS
       1 COMPETITOR_HAS_REFERENCE edge: AWS -> original_reference
       1 SUBSIDIARY_OF edge: AWS -> Amazon
```

**Example: "Walmart Inc." (simple dedup)**
```
Original row (id=Z, label="Walmart Inc.")
  -> Phase 1 dedup key "walmart" -> representative "Walmart"
  -> Phase 2 LLM -> 1 entity: Walmart (Company, cid=W)
  -> Output:
       1 Competitor row: Walmart
       1 COMPETES_WITH edge, 1 COMPETITOR_HAS_REFERENCE edge
```

**Example: "Online Retailers" (standalone category)**
```
Original row (id=Q, label="Online Retailers")
  -> Phase 2 LLM -> 1 entity: Online Retailers (Category, cid=OR)
  -> Output:
       1 Competitor row: Online Retailers
       1 COMPETES_WITH edge: filing_company -> Online Retailers
       1 COMPETITOR_HAS_REFERENCE edge: Online Retailers -> original_reference
```

## Output Schema

### nodes_competitor_resolved.parquet

| Column | Type | Description |
|---|---|---|
| `id` | STRING | New UUID for this row (used by edges) |
| `canonical_id` | STRING | Canonical entity UUID (shared across all mentions of same entity) |
| `label` | STRING | Resolved entity name (canonical) |
| `original_label` | STRING | Raw label from the original parquet (empty for parent/category nodes created from references) |
| `original_id` | STRING | Original competitor node ID (empty for synthesized nodes) |
| `competitor_type` | STRING | Company / Category / Generic |
| `sector` | STRING | Industry sector or empty |
| `year` | INT64 | From original (0 for synthesized nodes) |
| `section` | STRING | From original (empty for synthesized) |
| `link` | STRING | From original (empty for synthesized) |
| `relationship` | STRING | From original (empty for synthesized) |

### edges_competes_resolved.parquet

| Column | Type | Description |
|---|---|---|
| `source_node` | STRING | Company ticker |
| `target_node` | STRING | New competitor id |

### edges_competitor_has_reference_resolved.parquet

| Column | Type | Description |
|---|---|---|
| `source_node` | STRING | New competitor id |
| `target_node` | STRING | Reference id |

### edges_subsidiary_of.parquet (NEW)

| Column | Type | Description |
|---|---|---|
| `source_node` | STRING | Child competitor canonical_id (e.g., AWS) |
| `target_node` | STRING | Parent competitor canonical_id (e.g., Amazon) |

### edges_in_category.parquet (NEW)

| Column | Type | Description |
|---|---|---|
| `source_node` | STRING | Company competitor canonical_id |
| `target_node` | STRING | Category competitor canonical_id |

## KuzuDB Schema Changes

Updated `Competitor` node table:

```sql
CREATE NODE TABLE Competitor(
    id STRING PRIMARY KEY,
    canonical_id STRING,
    label STRING,
    original_label STRING,
    original_id STRING,
    competitor_type STRING,
    sector STRING,
    year INT64,
    section STRING,
    link STRING,
    relationship STRING
)
```

New relationship tables:

```sql
CREATE REL TABLE SUBSIDIARY_OF(FROM Competitor TO Competitor)
CREATE REL TABLE IN_CATEGORY(FROM Competitor TO Competitor)
```

The `load_kuzu.py` loader needs:
1. Option to use resolved vs raw competitor files
2. Load the two new edge tables

### Useful Cypher Queries

```cypher
-- Find all companies that compete in Cloud Service Providers
MATCH (c:Competitor)-[:IN_CATEGORY]->(cat:Competitor {label: 'Cloud Service Providers'})
RETURN c.label, c.sector

-- Find a company's parent
MATCH (c:Competitor {label: 'AWS'})-[:SUBSIDIARY_OF]->(parent:Competitor)
RETURN parent.label

-- Which filing companies compete with Amazon subsidiaries?
MATCH (co:Company)-[:COMPETES_WITH]->(c:Competitor)-[:SUBSIDIARY_OF]->(parent:Competitor {label: 'Amazon'})
RETURN co.label, c.label

-- Which categories have the most companies competing in them?
MATCH (c:Competitor)-[:IN_CATEGORY]->(cat:Competitor)
WHERE cat.competitor_type = 'Category'
RETURN cat.label, count(DISTINCT c) AS company_count
ORDER BY company_count DESC
LIMIT 20

-- Two-hop: find companies that compete in the same categories
MATCH (co1:Company)-[:COMPETES_WITH]->(c1:Competitor)-[:IN_CATEGORY]->(cat:Competitor)
      <-[:IN_CATEGORY]-(c2:Competitor)<-[:COMPETES_WITH]-(co2:Company)
WHERE co1.label < co2.label
RETURN co1.label, co2.label, cat.label, count(*) AS shared_categories
ORDER BY shared_categories DESC
LIMIT 20
```

## File Structure

```
pipeline/entity_normalization/
    resolve_competitors.py              Main script (Phases 1-3 + 5)
    resolve_competitors_senzing.py      Optional Senzing validation (Phase 4)
    data/
        competitor_resolved_cache.json  LLM cache (label -> entities)
    COMPETITOR_RESOLUTION.md            This document
```

## How to Run

```bash
cd pipeline

# Dry run
uv run python entity_normalization/resolve_competitors.py --dry-run

# Run resolution (default: gemini-3-flash, batch-size 30)
uv run python entity_normalization/resolve_competitors.py

# Options
uv run python entity_normalization/resolve_competitors.py --model gemini-2.5-flash --batch-size 20

# Optional: Senzing validation (requires Senzing SDK installed)
uv run python entity_normalization/resolve_competitors_senzing.py
```

The script is **resumable** — cached results are reused on re-run.

## Estimated Numbers

| Stage | Unique entities |
|---|---|
| Raw labels | 6,546 |
| After Phase 1 (deterministic dedup) | ~5,500 |
| After Phase 2 (LLM split + canonicalize) | ~6,000-7,000 (splits increase count) |
| After Phase 3 (post-LLM dedup) | ~5,000-5,500 |
| Synthesized nodes (parents/categories from references) | ~200-500 |
| SUBSIDIARY_OF edges | ~300-600 |
| IN_CATEGORY edges | ~500-2,000 |
| Output rows in nodes_competitor_resolved | ~15,000-17,000 |

## Comparison with Risk Categorization

| | Risk | Competitor |
|---|---|---|
| Input | 10,299 unique labels | 6,546 unique labels |
| Approach | Multi-label category assignment | Entity resolution + classification |
| Output | Added `risk_categories` column | Separate resolved parquet + new edge files |
| Row count change | No (same 17,265 rows) | Yes (splits increase rows) |
| New graph edges | None | SUBSIDIARY_OF, IN_CATEGORY |
| LLM task | Classify only | Split + canonicalize + classify + link |
| Dedup | Not needed (categories, not entities) | Critical (company name variants) |
| Cache | label -> categories string | label -> entities list |
