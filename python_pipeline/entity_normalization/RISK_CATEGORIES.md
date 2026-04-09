# Risk Categories — Taxonomy Reference

## Overview

This document describes the 17 risk categories used to classify risks extracted from SEC 10-K filings. Each risk can belong to **multiple categories** (multi-label assignment).

These categories serve as the reference taxonomy for entity normalization. Each extracted risk label is mapped to one or more categories using an LLM (Gemini Flash) in batched API calls.

## The 17 Categories

| # | Category | Description |
|---|---|---|
| 1 | Cybersecurity & Data Privacy | Cyber threats, data breaches, ransomware, privacy regulation, data localization |
| 2 | Climate & Environment | Physical/transition climate risks, extreme weather, GHG, environmental remediation |
| 3 | Regulatory & Compliance | Laws, regulations, enforcement, compliance costs, political/policy shifts |
| 4 | AI & Emerging Technology | AI implementation, generative AI, quantum computing, tech disruption |
| 5 | Supply Chain & Operations | Supply disruptions, component shortages, logistics, supplier concentration |
| 6 | Macroeconomic & Inflation | Inflation, recession, commodity prices, input costs, margin pressure |
| 7 | Financial & Capital Markets | Interest rates, LIBOR/SOFR, credit risk, banking regulation, asset impairment |
| 8 | Foreign Currency & Exchange | FX volatility, currency translation, hedging risks |
| 9 | Geopolitical & Trade | Political instability, trade wars, tariffs, sanctions, export controls |
| 10 | Tax & Fiscal Policy | Global minimum tax, OECD Pillar Two, tax reform, transfer pricing |
| 11 | Labor & Human Capital | Labor shortages, unionization, wages, talent, remote/hybrid work |
| 12 | Legal & Litigation | IP litigation, class actions, product liability, antitrust, enforcement |
| 13 | Healthcare & Pharma | Drug pricing, clinical trials, FDA, reimbursement, biosimilars, GLP-1 |
| 14 | ESG & Sustainability | ESG scrutiny, anti-ESG, sustainability mandates, greenwashing |
| 15 | Competition & Market Position | Market share, disruption, consolidation, new entrants, saturation |
| 16 | Corporate Strategy & Execution | M&A, restructuring, spin-offs, ERP, accounting changes, transformation |
| 17 | Customer & Demand | Customer concentration, demand shifts, consumer behavior, cord-cutting |

## Multi-Label Assignment

Many risks span multiple categories. Examples:

| Risk Label | Categories |
|---|---|
| Climate Change and GHG Regulations | Climate & Environment, Regulatory & Compliance |
| AI-Enhanced Cybersecurity Threats | AI & Emerging Technology, Cybersecurity & Data Privacy |
| Supply Chain and Inflationary Pressures | Supply Chain & Operations, Macroeconomic & Inflation |
| Cybersecurity and Data Privacy Regulation | Cybersecurity & Data Privacy, Regulatory & Compliance |
| ESG and Climate Change Regulation | ESG & Sustainability, Climate & Environment, Regulatory & Compliance |

## How This Category Set Was Derived

### Methodology

The categories were derived through a data-driven, bottom-up analysis of all 10,299 unique risk labels extracted from SEC 10-K filings (17,265 total rows across ~438 companies, 2021-2025).

#### Step 1: Frequency Analysis
- Extracted all unique labels and their frequencies
- Top 50 labels (appearing 20+ times each) revealed the dominant themes: cybersecurity, climate, inflation, supply chain, LIBOR, etc.
- Distribution: 13 labels appear 50+ times, 143 appear 10+, 383 appear 5+, while 8,164 are singletons

#### Step 2: Keyword Clustering (Round 1)
- Assigned labels to candidate categories using keyword matching
- 14 initial categories covered ~64% of unique labels (6,631 of 10,299)
- Identified 3,668 unmatched labels

#### Step 3: Gap Analysis (Round 2)
- Analyzed the 3,668 unmatched labels to find missing themes
- Discovered significant clusters: IT Infrastructure (223), Competition (216), Commodity Costs (178), Customer/Demand (171), Energy Transition (162), etc.
- Added 3 new categories: Competition & Market Position, Corporate Strategy & Execution, Customer & Demand
- Merged some sub-themes into existing categories (e.g., natural disasters → Climate & Environment)

#### Step 4: Deep Tail Analysis (Round 3)
- Analyzed the remaining 1,876 labels that still didn't match keywords
- Found many were matched by existing categories but with keywords not in the initial set (e.g., "GLP-1" → Healthcare, "Basel III" → Financial, "deepfake" → Cybersecurity)
- Expanded keyword lists for existing categories
- Confirmed ~1,333 truly industry-specific labels (e.g., "Cattle Supply Shortage", "5G Spectrum Interference", "Jif Peanut Butter Recall") that map to existing categories via their descriptions and multi-label assignment

#### Step 5: Validation
- Confirmed 17 categories with multi-label assignment can cover the full dataset
- No need for an "Other" catch-all — industry-specific risks map to categories like Supply Chain, Regulatory, Competition via description context
- The LLM in Phase 2 uses the `description` field (not just labels) to accurately assign categories for the long tail

### Coverage Summary

| Round | Labels Matched | Cumulative Coverage |
|---|---|---|
| Initial 14 categories (keyword) | 6,631 | 64% |
| + 3 new categories + expanded keywords | 8,423 | 82% |
| + LLM with description context (Phase 2) | 10,299 | 100% |

## Categorization Results (Phase 2)

Categorization was run on 2026-04-03 using `categorize_risks.py` with Gemini 3 Flash.

- **10,299 unique labels** classified across 3 resumable runs (~15 min total)
- **99.9% coverage** (17,254 / 17,265 rows in `nodes_risk.parquet`)
- 10 labels remain unclassified due to persistent JSON parse errors
- Results written as `risk_categories` column in `nodes_risk.parquet` (comma-separated list)

### Category Distribution (across 17,265 rows)

| Category | Count |
|---|---|
| Regulatory & Compliance | 5,366 |
| Climate & Environment | 2,791 |
| Macroeconomic & Inflation | 2,361 |
| Supply Chain & Operations | 2,198 |
| Financial & Capital Markets | 2,047 |
| Cybersecurity & Data Privacy | 2,026 |
| AI & Emerging Technology | 1,639 |
| Geopolitical & Trade | 1,548 |
| Legal & Litigation | 1,072 |
| Competition & Market Position | 928 |
| Labor & Human Capital | 925 |
| Corporate Strategy & Execution | 911 |
| ESG & Sustainability | 894 |
| Healthcare & Pharma | 823 |
| Tax & Fiscal Policy | 811 |
| Customer & Demand | 731 |
| Foreign Currency & Exchange | 267 |

Note: Totals exceed 17,265 because risks can have multiple categories.

## Reference Data

| File | Description |
|---|---|
| `data/risk_categories.csv` | 17 categories with descriptions, example risks, and keywords |
| `data/risk_label_categories_cache.json` | Cached label → categories mapping (10,290 entries). Resumable — new labels are appended on re-run. |

### risk_categories.csv Schema

`risk_category`, `description`, `example_risks`, `keywords`

## How to Run

```bash
cd pipeline

# Dry run — show what would be classified
uv run python entity_normalization/categorize_risks.py --dry-run

# Run categorization (default: gemini-3-flash, batch-size 50)
uv run python entity_normalization/categorize_risks.py

# Options
uv run python entity_normalization/categorize_risks.py --model gemini-2.5-flash --batch-size 30
```

The script is **resumable** — it caches results in `risk_label_categories_cache.json` and skips already-classified labels on re-run. This also supports **incremental processing** when new filings are added.

## How to Extend

When new filings introduce risks that don't fit existing categories:

1. Run the same frequency + keyword analysis on new labels
2. Check if unmatched labels cluster into a new theme (50+ labels)
3. If yes, propose a new category with description, examples, and keywords
4. Add to `risk_categories.csv` and update this document
5. Re-run Phase 2 mapping only for newly added risks

If the new theme is small (<50 labels), it likely maps to an existing category via multi-label assignment.

## Reusability

This same methodology applies to other entity types:
- **Competitor normalization**: Derive competitor_type and sector categories
- **Market normalization**: Derive geographic_region, sector, and product_category categories

The approach is: frequency analysis → keyword clustering → iterative gap analysis → validate against full dataset.
