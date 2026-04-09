# Normalized Risk Analysis — Why We Skipped It

## Decision

We decided **not** to create a `normalized_risk` (canonical risk label) intermediate layer. The `risk_category` (17 categories) is the useful abstraction. This document records the analysis behind that decision.

## The Question

Could we reduce 10,299 unique risk labels to a manageable set of ~100-200 canonical risks (e.g., mapping "LIBOR Transition", "LIBOR Phase-out", "LIBOR Discontinuation" all to one canonical "LIBOR Transition")?

## Label Frequency Distribution

| Bucket | Count | % of Labels |
|---|---|---|
| Singletons (appear 1x) | 8,164 | 79% |
| Appear 2-5x | 1,843 | 18% |
| Appear 6-20x | 248 | 2.4% |
| Appear 21-50x | 32 | 0.3% |
| Appear 51+x | 12 | 0.1% |
| **Total unique** | **10,299** | |

The core problem: **79% of labels are singletons** — company-specific phrasings that appear only once.

## What Normalization Techniques Achieve

### Token-sort normalization (no LLM)
Handles word-order variants like "Trade Restrictions and Tariffs" vs "Tariffs and Trade Restrictions".

- Reduction: 10,299 → 9,944 (**only 355 merges**)
- 335 multi-variant clusters found, merging 690 labels total

### Semantic clustering by theme keywords
Tested major themes to see how many labels collapse:

| Theme | Unique Labels | Realistic Canonicals | Reduction |
|---|---|---|---|
| Cybersecurity cluster | 597 | ~5-8 | ~590 merged |
| Climate cluster | 628 | ~4-5 | ~623 merged |
| Supply Chain cluster | 428 | ~4-5 | ~423 merged |
| LIBOR/Rate Reform | 28 | 1 | 27 merged |
| Pillar Two / Min Tax | 93 | 1 | 92 merged |

But these are the **easy** clusters (high-frequency, keyword-identifiable). The remaining ~8,000+ singletons are the problem.

### Realistic best-case with LLM

| Step | Labels |
|---|---|
| Start | 10,299 |
| Token-sort merges | -355 → 9,944 |
| Case-insensitive merges | ~-300 → ~9,600 |
| Semantic merges (LLM) | ~-1,000 → ~8,500 |
| **Best case** | **~7,000-8,000** |

## Why 7-8K Is Not Useful

The point of normalized_risk would be to create a navigable middle tier between 17 categories and 10K raw labels. But 7-8K canonical risks is still too many for:
- Graph visualization (too many nodes)
- Cross-company comparison (still too granular)
- Trend analysis (most canonicals would still be singletons)

The singletons are genuinely different risks — "Cattle Supply Shortage", "Boeing 737 MAX Production Caps", "Jif Peanut Butter Recall", "5G Spectrum Interference" are all real, distinct risks. They can't be meaningfully merged into fewer canonical labels without losing information.

## What Works Instead

- **risk_category** (17 categories, multi-label) provides the useful abstraction
- For graph exploration: group by risk_category, then drill into individual labels
- For cross-company comparison: compare at the category level
- For trend analysis: track category prevalence over time (e.g., "Cybersecurity & Data Privacy" risk mentions grew 40% from 2021 to 2025)

## When to Revisit

If the dataset grows significantly (e.g., 50K+ labels from more filings) and new high-frequency clusters emerge that are distinct enough to warrant intermediate grouping, this analysis should be re-run.
