# Graph Model — Entity Normalization Layer

## Design Decision: Two-Level Model for Competitors

### Problem

The raw competitor extraction produces 14,572 Competitor nodes, but many refer to the same real-world entity:
- `Walmart` (41x), `Walmart Inc.` (20x), `Walmart, Inc.` (4x) → 65 nodes for one company
- `Apple, Google, Microsoft` → one node containing 3 entities

After entity resolution, we have 5,455 unique canonical entities. The question: how do we model this in the graph?

### Options Considered

**Option A: Replace Competitor nodes with resolved versions**
- Drop original 14,572 Competitor nodes
- Load 15,333 resolved rows (more rows due to splits) as new Competitor nodes
- Loses provenance — original extraction data overwritten

**Option B: Add columns to existing Competitor nodes**
- ALTER TABLE to add `canonical_id`, `competitor_type`, `sector`
- Simple but no entity-level graph traversal
- SUBSIDIARY_OF edges can't work (they're between canonical entities, not mentions)

**Option C (chosen): Normalization layer on top of original graph**
- Keep original Competitor nodes untouched (14,572)
- Add NormalizedCompetitor nodes (5,455 unique entities)
- Link them with INSTANCE_OF edges
- SUBSIDIARY_OF and IN_MARKET_CATEGORY operate on NormalizedCompetitor level

### Why Option C?

1. **Truly non-destructive.** Original graph works exactly as before. Normalization is an additive layer.

2. **INSTANCE_OF handles splits naturally.** When `"Apple, Google, Microsoft"` was one Competitor node, INSTANCE_OF creates three edges to three NormalizedCompetitors. Many-to-many is natural:
   ```
   Competitor{"Apple, Google, Microsoft"} →[INSTANCE_OF]→ NormalizedCompetitor{Apple}
   Competitor{"Apple, Google, Microsoft"} →[INSTANCE_OF]→ NormalizedCompetitor{Google}
   Competitor{"Apple, Google, Microsoft"} →[INSTANCE_OF]→ NormalizedCompetitor{Microsoft}
   Competitor{"Apple"}                    →[INSTANCE_OF]→ NormalizedCompetitor{Apple}
   ```

3. **Entity-level properties separate from mention-level.** The canonical entity (NormalizedCompetitor) carries stable properties: `competitor_type`, `sector`, `product_category`. The mention (Competitor) carries context: `year`, `section`, `link`, `relationship`.

4. **SUBSIDIARY_OF makes sense at entity level.** AWS → Amazon is a relationship between entities, not between individual SEC filing mentions.

5. **Enables the bridge queries** the user wants:
   ```
   Company →[COMPETES_WITH]→ Competitor →[INSTANCE_OF]→ NormalizedCompetitor →[IN_MARKET_CATEGORY]→ MarketCategory
   ```

---

## Full Graph Schema (with normalization layer)

### Original Layer (untouched)

```
Company ─[FILED]─> Document ─[CONTAINS]─> Section ─[CONTAINS]─> Reference
   │                                                                 ^
   ├─[ENTERING]──> Market ──[MARKET_HAS_REFERENCE]──────────────────┤
   ├─[EXITING]───> Market                                            │
   ├─[EXPANDING]─> Market                                            │
   ├─[FACES_RISK]──────> Risk ──[RISK_HAS_REFERENCE]────────────────┤
   ├─[PURSUING]───> Opportunity ──[OPPORTUNITY_HAS_REFERENCE]───────┤
   └─[COMPETES_WITH]──> Competitor ──[COMPETITOR_HAS_REFERENCE]─────┘
```

### Normalization Layer (additive)

```
Risk ──[HAS_RISK_CATEGORY]──> RiskCategory (17 nodes)

Market ──[IN_REGION]──────────> GeographicRegion (7 nodes)
Market ──[IN_PRODUCT_CATEGORY]─> MarketCategory (12 nodes)

Competitor ──[INSTANCE_OF]──> NormalizedCompetitor (5,455 nodes)
                                  │
                                  ├──[SUBSIDIARY_OF]──> NormalizedCompetitor
                                  └──[IN_MARKET_CATEGORY]──> MarketCategory (shared with Market!)
```

### MarketCategory as the Cross-Entity Bridge

The 12 MarketCategory nodes are shared between Markets and NormalizedCompetitors:

```
Company{X} ─[ENTERING]─> Market{Cloud Computing}
                              │
                              └─[IN_PRODUCT_CATEGORY]─> MarketCategory{Cloud & Software}
                                                             │
                              ┌─[IN_MARKET_CATEGORY]─────────┘
                              │
                    NormalizedCompetitor{AWS} <─[INSTANCE_OF]─ Competitor <─[COMPETES_WITH]─ Company{Y}
```

This enables: "Show me companies entering Cloud markets AND the competitors in that space."

### Why MarketCategory as shared hub nodes?

We considered three options for connecting competitors to market categories:

1. **Map sector → product categories** — Too lossy. "Technology" spans Cloud & Software, Telecommunications, etc.

2. **Use the 815 Category-type competitors as-is** — Too noisy. "Cloud Service Providers", "Online Retailers", "Fintech Companies" are organic but not normalized. 815 distinct values vs 12.

3. **(Chosen) Run a quick LLM pass** to assign the 12 product categories to NormalizedCompetitors — Clean, cheap (~$0.10), creates a small set of high-connectivity hub nodes shared with Markets. Both Markets and Competitors link to the same 12 MarketCategory nodes.

The shared MarketCategory nodes are what makes the graph powerful for cross-domain exploration. Without them, Markets and Competitors are disconnected subgraphs.

---

## Node Types Summary

| Node | Count | Source | Key Properties |
|---|---|---|---|
| Company | 438 | Original | label, cik, state_of_inc, ... |
| Document | 2,068 | Original | year, link, company |
| Section | 6,183 | Original | label, section |
| Reference | 56,953 | Original | text, link |
| Market | 24,018 | Original | label, evidence, market_action |
| Risk | 17,265 | Original | label, description |
| Opportunity | 11,249 | Original | label, description |
| Competitor | 14,572 | Original | label, relationship |
| **NormalizedCompetitor** | **5,455** | **Resolution** | label, competitor_type, sector, product_category |
| **RiskCategory** | **17** | **Taxonomy** | label, description |
| **GeographicRegion** | **7** | **Taxonomy** | label, description |
| **MarketCategory** | **12** | **Taxonomy** | label, description |

## Edge Types Summary

| Edge | From → To | Count | Source |
|---|---|---|---|
| FILED | Company → Document | 2,068 | Original |
| CONTAINS | Document → Section | 6,183 | Original |
| CONTAINS | Section → Reference | 56,953 | Original |
| ENTERING | Company → Market | 6,551 | Original |
| EXITING | Company → Market | 5,771 | Original |
| EXPANDING | Company → Market | 11,696 | Original |
| FACES_RISK | Company → Risk | 17,265 | Original |
| PURSUING | Company → Opportunity | 11,249 | Original |
| COMPETES_WITH | Company → Competitor | 14,572 | Original |
| MARKET_HAS_REFERENCE | Market → Reference | 24,018 | Original |
| RISK_HAS_REFERENCE | Risk → Reference | 17,265 | Original |
| OPPORTUNITY_HAS_REFERENCE | Opportunity → Reference | 11,249 | Original |
| COMPETITOR_HAS_REFERENCE | Competitor → Reference | 14,572 | Original |
| **INSTANCE_OF** | **Competitor → NormalizedCompetitor** | **~15,500** | **Resolution** |
| **SUBSIDIARY_OF** | **NormalizedCompetitor → NormalizedCompetitor** | **197** | **Resolution** |
| **IN_MARKET_CATEGORY** | **NormalizedCompetitor → MarketCategory** | **~TBD** | **Resolution** |
| **HAS_RISK_CATEGORY** | **Risk → RiskCategory** | **~26,000** | **Categorization** |
| **IN_REGION** | **Market → GeographicRegion** | **~9,000** | **Categorization** |
| **IN_PRODUCT_CATEGORY** | **Market → MarketCategory** | **~11,000** | **Categorization** |

### Edge Count Impact

```
Original graph:    213,451 edges
+ INSTANCE_OF:     ~15,500 edges
+ SUBSIDIARY_OF:       197 edges
+ IN_MARKET_CATEGORY: ~TBD edges
+ HAS_RISK_CATEGORY:~26,000 edges
+ IN_REGION:        ~9,000 edges
+ IN_PRODUCT_CATEGORY: ~11,000 edges
─────────────────────────────────
New total:        ~275,000+ edges (+29%)
```

---

## Key Cypher Queries Enabled

```cypher
-- Companies sharing risk profiles
MATCH (c1:Company)-[:FACES_RISK]->(:Risk)-[:HAS_RISK_CATEGORY]->(rc:RiskCategory)
      <-[:HAS_RISK_CATEGORY]-(:Risk)<-[:FACES_RISK]-(c2:Company)
WHERE c1.label < c2.label AND rc.label = 'Cybersecurity & Data Privacy'
RETURN c1.label, c2.label, count(*) AS shared_risks

-- Companies entering markets in a region
MATCH (c:Company)-[:ENTERING]->(m:Market)-[:IN_REGION]->(r:GeographicRegion {label: 'Asia Pacific'})
RETURN c.label, m.label

-- Cross-domain: companies in Cloud markets AND their Cloud competitors
MATCH (c:Company)-[:ENTERING]->(m:Market)-[:IN_PRODUCT_CATEGORY]->(mc:MarketCategory {label: 'Cloud & Software'})
WITH c, mc
MATCH (c)-[:COMPETES_WITH]->(:Competitor)-[:INSTANCE_OF]->(nc:NormalizedCompetitor)-[:IN_MARKET_CATEGORY]->(mc)
RETURN c.label, nc.label

-- Corporate family of a competitor
MATCH (nc:NormalizedCompetitor)-[:SUBSIDIARY_OF]->(parent:NormalizedCompetitor {label: 'Amazon'})
RETURN nc.label, nc.sector

-- All competitors in a market category
MATCH (nc:NormalizedCompetitor)-[:IN_MARKET_CATEGORY]->(mc:MarketCategory {label: 'Renewable Energy'})
RETURN nc.label, nc.competitor_type, nc.sector
```
