# Competitor Taxonomy — Derivation Analysis

## Overview

This document records how the competitor type and sector classifications were derived from the data. The methodology mirrors the bottom-up approach used for risk categories: frequency analysis, keyword clustering, iterative gap analysis.

## Source Data

- 14,572 rows in `nodes_competitor.parquet` (gemini-3-flash extraction)
- 6,546 unique labels
- Each row has: `id`, `label`, `year`, `section`, `link`, `relationship`

## Type Classification

### The 3 Types

| Type | Description |
|---|---|
| **Company** | Specific named company, corporation, or legal entity |
| **Category** | Named group/class of competitors (hub nodes in graph) |
| **Generic** | Vague/descriptive reference, no specific group |

### Derivation Process

#### Round 1: Keyword Classification

Applied regex patterns to all 6,546 unique labels:

| Signal | Rule | Matched |
|---|---|---|
| Company suffix | Ends with Inc., Corp., Ltd., plc, AG, GmbH, etc. | 994 |
| Category ending | Ends with plural group noun (providers, companies, carriers, retailers, manufacturers, etc.) | 2,531 |
| Generic start | Starts with vague adjective (large, small, other, unnamed, emerging, etc.) | 208 |
| **Ambiguous** | None of the above | **2,813** |

Coverage: 57.0% (3,733 / 6,546)

The category ending list was comprehensive — 40+ plural nouns covering business types. Key additions beyond obvious ones: `utilities`, `generators`, `developers`, `processors`, `wholesalers`, `merchants`, `exchanges`, `underwriters`, `contractors`, `restaurants`, `hospitals`, `aggregators`, `installers`, `integrators`, `resellers`, `warehouses`.

#### Round 2: Expanded Keywords

Added patterns for the 2,813 ambiguous labels:

| Signal | Rule | Newly Matched |
|---|---|---|
| Company with abbreviation | Pattern like `HCSC (Health Care Service Corporation)` | Part of 327 |
| ALL-CAPS abbreviation | 1-7 uppercase chars like `AMD`, `IBM`, `BNSF` | Part of 327 |
| Expanded category endings | `industry`, `market`, `sector`, `brands`, `systems`, `facilities`, `organizations`, `OEMs`, `REITs`, `ISPs`, etc. | 183 |
| Expanded generic starts | `independent`, `private`, `alternative`, `specialized`, `diversified`, `non-bank`, `mid-size`, etc. | 72 |

Coverage: 65.9% (4,317 / 6,546)

#### Round 3: Analysis of Remaining 2,231 Ambiguous

Breakdown by word count:

| Words | Count | Typical Content |
|---|---|---|
| 1 word | 574 | Company names without suffix: `Apple` (59x), `OPEC+` (38x), `Boeing` (27x), `Cisco` (10x) |
| 2 words | 626 | Mix of companies (`Alaska Airlines`, `Robert Half`) and descriptions (`Cloud-based vendors`, `Energy marketers`) |
| 3 words | 393 | Companies (`Emerson Electric Co.`, `Norfolk Southern Railway`) and multi-entity (`AEP and FirstEnergy`) |
| 4+ words | 638 | Multi-entity lists, companies with long names, category phrases with unusual endings |

**Key finding:** The 1-word ambiguous (574 labels) are almost entirely well-known company names. The LLM handles these trivially — no keyword rule can distinguish `Apple` (company) from `Self-Generation` (generic) without world knowledge.

**Decision:** The 3-type taxonomy (Company/Category/Generic) is validated by the data. Keyword rules cover ~66% deterministically; the remaining ~34% require LLM classification. This is expected and acceptable — the ambiguous labels are genuinely ambiguous to regex.

### Type Distribution (estimated)

Based on the old entity_resolution results (10,704 labels classified by Ollama gemma3:12b):

| Type | Count | % |
|---|---|---|
| Company | 4,642 | 43% |
| Category | 4,018 | 38% |
| Generic | 2,020 | 19% |

This distribution is reasonable: ~43% specific companies, ~38% category groups (become hub nodes), ~19% vague/generic.

## Sector Classification

### The 13 Sectors

Derived from a combination of:
1. The old entity_resolution's 12 sectors (which worked well)
2. Gap analysis of labels that matched no sector
3. Adding "Materials & Chemicals" (previously fragmented across Industrial and a tiny standalone "Chemicals")

| # | Sector | Label Keyword Matches | Relationship Keyword Matches |
|---|---|---|---|
| 1 | Technology | 1,011 | 2,086 |
| 2 | Financial Services | 584 | 1,155 |
| 3 | Retail & E-commerce | 459 | 686 |
| 4 | Energy | 364 | 520 |
| 5 | Healthcare & Pharma | 307 | 393 |
| 6 | Automotive & Transportation | 307 | 554 |
| 7 | Industrial & Manufacturing | 295 | 475 |
| 8 | Consumer Goods | 151 | 431 |
| 9 | Telecommunications & Media | 164 | 282 |
| 10 | Utilities | 106 | 247 |
| 11 | Materials & Chemicals | 74 | 145 |
| 12 | Real Estate | 58 | 186 |
| 13 | Aerospace & Defense | 53 | 392 |

### Derivation Process

#### Keyword Coverage Analysis

Applied sector keyword lists against both label text and relationship text for all 6,546 unique labels:

- **Via label text:** 3,933 unique labels matched at least one sector keyword (60%)
- **Via relationship text:** Expanded coverage significantly, especially for Aerospace & Defense (53 -> 392), Consumer Goods (151 -> 431), and Automotive & Transportation (307 -> 554)
- **No match in either:** 1,210 labels (18.5%) — these need LLM classification

#### Analysis of No-Sector Labels (1,210)

Sampled and categorized the labels with no keyword match:

| Reason | Example | Action |
|---|---|---|
| Company name only, no sector in label or relationship | `ALSO Holding`, `Checkpoint Systems`, `PageGroup` | LLM infers from world knowledge |
| Sector keywords not in our list | `consulting`, `publishing`, `advertising`, `staffing` | These are sub-sectors of existing categories; LLM maps to closest sector |
| Cross-sector / diversified | `Companies in other industries`, `Various competitors` | LLM returns null sector |
| Non-competitor entity | `Acquired company`, `Joint venture partner` | LLM classifies the entity itself |

**Decision:** 13 sectors is sufficient. The 18.5% unmatched labels are primarily company names that need world knowledge (LLM handles) or genuinely cross-sector entities (get null sector). Adding more sectors would fragment the taxonomy without improving graph navigation.

#### Sector Changes from Old Approach

| Old Sector | New Sector | Change |
|---|---|---|
| Industrial | Industrial & Manufacturing | Renamed for clarity |
| Retail | Retail & E-commerce | Added e-commerce explicitly |
| Healthcare | Healthcare & Pharma | Added pharma explicitly |
| Telecommunications | Telecommunications & Media | Merged media/entertainment into telecom |
| *(none)* | Materials & Chemicals | New — was 7 labels in old "Chemicals" + split from Industrial |
| Chemicals | *(merged into Materials & Chemicals)* | — |
| Gaming | *(merged into Telecommunications & Media)* | 5 labels, too small for standalone |
| Hospitality | *(merged into Real Estate)* | 3 labels, too small for standalone |

#### Null Sector Analysis

The old approach had 22.8% null sectors. Breakdown by type:

| Type | Null Sector Count | % of Null |
|---|---|---|
| Generic | 1,429 | 58% |
| Company | 644 | 26% |
| Category | 347 | 14% |

Generic-type labels dominate null sectors — this is expected. "Large global competitors" genuinely has no sector. For Company-type, these are typically obscure companies where the local Ollama model lacked world knowledge. Gemini Flash should reduce this significantly.

**Target:** <15% null sector overall, <10% for Company-type.

## Null Sector Policy

- **Company:** LLM should always attempt a sector based on world knowledge. Null only for truly unknown entities.
- **Category:** LLM assigns sector when the category clearly belongs to one sector. Null for cross-sector categories like "Large-format retailers, discounters and e-tailers" (could be Retail or Consumer Goods).
- **Generic:** Null is expected and acceptable for most Generic-type labels.

## Reference Data

| File | Description |
|---|---|
| `data/competitor_types.csv` | 3 types with descriptions, keyword signals, examples |
| `data/competitor_sectors.csv` | 13 sectors with descriptions, keywords, example companies and categories |

### competitor_types.csv Schema

`competitor_type`, `description`, `keyword_signals`, `examples`

### competitor_sectors.csv Schema

`sector`, `description`, `keywords`, `example_companies`, `example_categories`

## Reusability

The sector taxonomy is shared across entity types. The same 13 sectors could be used for:
- Market normalization (geographic_region would be an additional dimension)
- Opportunity categorization

The type taxonomy is competitor-specific. Other entity types have their own type distinctions (e.g., markets have entering/exiting/expanding; risks have the 17 risk categories).
