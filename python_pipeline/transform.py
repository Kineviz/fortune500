"""
Step 2: Transform raw LLM extraction results into node/edge parquet files.

Mirrors fortune500 schema exactly:
  Node tables: Company, Document, Section, Reference,
               Market, Risk, Opportunity, Competitor
  Edge tables: FILED, CONTAINS (Doc→Section), CONTAINS (Section→Reference),
               ENTERING, EXITING, EXPANDING, FACES_RISK, PURSUING, COMPETES_WITH,
               MARKET_HAS_REFERENCE, RISK_HAS_REFERENCE,
               OPPORTUNITY_HAS_REFERENCE, COMPETITOR_HAS_REFERENCE

Saves all tables as parquet in output/parquet/.
"""

import hashlib
import json
import urllib.parse
import uuid
from pathlib import Path

import pandas as pd

from config import OUTPUT_BASE, get_output_dir


def md5hex(text: str) -> str:
    """Return hex MD5 of text (matches BigQuery TO_HEX(MD5(...)))."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def text_fragment_url(filing_url: str, reference: str) -> str:
    """Build a text fragment URL matching fortune500's textFragmentStart UDF."""
    if not reference or not filing_url:
        return ""
    # Strip ix?doc=/ prefix like BigQuery does
    base_url = filing_url.replace("ix?doc=/", "")
    words = reference.strip().split()
    if len(words) <= 10:
        fragment = urllib.parse.quote(reference)
    else:
        start = urllib.parse.quote(" ".join(words[:5]))
        end = urllib.parse.quote(" ".join(words[-5:]))
        fragment = f"{start},{end}"
    return f"{base_url}#:~:text={fragment}"


def _extract_result_text(rec: dict) -> str:
    """Return LLM output text from either local or BigQuery export schemas."""
    # Local extractor schema
    if rec.get("result"):
        return rec.get("result") or ""
    # BigQuery AI.GENERATE_TEXT schema
    for key in ("ml_generate_text_result", "ml_generate_text_llm_result"):
        if rec.get(key):
            return rec.get(key) or ""
    return ""


def _parse_result_json(text: str):
    """Parse JSON payload from raw model text, including fenced responses."""
    if not text:
        return None
    raw = text.strip()
    if not raw:
        return None

    # Remove common markdown fencing
    if raw.startswith("```"):
        raw = raw.removeprefix("```json").removeprefix("```").strip()
        if raw.endswith("```"):
            raw = raw[:-3].strip()

    # Best case: full JSON document
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Fallback: parse the largest JSON object substring
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(raw[start : end + 1])
        except json.JSONDecodeError:
            return None
    return None


def parse_insights(output_dir: str):
    """Read insights.jsonl and return list of parsed records."""
    insights_file = Path(output_dir) / "extractions" / "insights.jsonl"
    if not insights_file.exists():
        raise FileNotFoundError(f"Run extract.py first. Missing: {insights_file}")

    records = []
    with open(insights_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            parsed = _parse_result_json(_extract_result_text(rec))
            rec["parsed"] = parsed if isinstance(parsed, dict) else None
            records.append(rec)
    return records


def build_tables(records: list):
    """Build all node and edge DataFrames from parsed extraction records."""

    # Node accumulators
    companies = {}       # ticker -> row
    documents = {}       # filing_url -> row
    sections = {}        # section_key -> row
    references = {}      # md5hex -> row
    markets = []
    risks = []
    opportunities = []
    competitors = []

    # Edge accumulators
    e_filed = []                          # Company -> Document
    e_doc_contains_section = []           # Document -> Section
    e_section_contains_ref = set()        # Section -> Reference (deduped)
    e_entering = []
    e_exiting = []
    e_expanding = []
    e_faces_risk = []
    e_pursuing = []
    e_competes = []
    e_market_has_ref = []
    e_risk_has_ref = []
    e_opp_has_ref = []
    e_comp_has_ref = []

    for rec in records:
        ticker = rec.get("company") or ""
        year = rec.get("year")
        section_id = rec.get("section_id") or ""
        parsed = rec.get("parsed")
        if not ticker or not year:
            continue

        filing_url = rec.get("filing_url") or ""
        company_name = rec.get("company_name") or ticker

        # ── Company node ────────────────────────────────────────────
        if ticker not in companies:
            companies[ticker] = {
                "id": ticker,
                "label": company_name,
                "cik": str(rec.get("cik") or ""),
                "sic": str(rec.get("sic") or ""),
                "irs_number": str(rec.get("irs_number") or ""),
                "state_of_inc": rec.get("state_of_inc") or "",
                "org_name": rec.get("org_name") or "",
                "sec_file_number": str(rec.get("sec_file_number") or ""),
                "film_number": str(rec.get("film_number") or ""),
                "business_street_1": rec.get("business_street_1") or "",
                "business_street_2": rec.get("business_street_2") or "",
                "business_city": rec.get("business_city") or "",
                "business_state": rec.get("business_state") or "",
                "business_zip": str(rec.get("business_zip") or ""),
                "business_phone": str(rec.get("business_phone") or ""),
                "mail_street_1": rec.get("mail_street_1") or "",
                "mail_street_2": rec.get("mail_street_2") or "",
                "mail_city": rec.get("mail_city") or "",
                "mail_state": rec.get("mail_state") or "",
                "mail_zip": str(rec.get("mail_zip") or ""),
            }

        # ── Document node (id = filing_url) ─────────────────────────
        if filing_url and filing_url not in documents:
            documents[filing_url] = {
                "id": filing_url,
                "year": int(year),
                "sec_file_number": str(rec.get("sec_file_number") or ""),
                "film_number": str(rec.get("film_number") or ""),
                "link": filing_url,
                "company": ticker,
                "company_name": company_name,
                "cik": str(rec.get("cik") or ""),
            }
            e_filed.append({"source_node": ticker, "target_node": filing_url})

        # ── Section node (id = filing_url#section_id) ───────────────
        section_key = f"{filing_url}#{section_id}" if filing_url else f"{ticker}_{year}_{section_id}"
        if section_key not in sections:
            sections[section_key] = {
                "id": section_key,
                "label": section_id,
                "section": section_id,
                "document_id": filing_url,
                "year": int(year),
                "company": ticker,
                "company_name": company_name,
                "link": section_key,
            }
            e_doc_contains_section.append({
                "source_node": filing_url,
                "target_node": section_key,
            })

        if not parsed:
            continue

        def add_reference(ref_text: str, entity_id: str, entity_type: str):
            """Create Reference node and edges for a given entity."""
            if not ref_text or not ref_text.strip():
                return
            ref_id = md5hex(ref_text)
            if ref_id not in references:
                references[ref_id] = {
                    "id": ref_id,
                    "text": ref_text,
                    "link": text_fragment_url(filing_url, ref_text),
                    "year": int(year),
                    "section": section_id,
                    "company": ticker,
                    "company_name": company_name,
                }
            # Entity -> Reference
            has_ref_row = {"source_node": entity_id, "target_node": ref_id}
            if entity_type == "Market":
                e_market_has_ref.append(has_ref_row)
            elif entity_type == "Risk":
                e_risk_has_ref.append(has_ref_row)
            elif entity_type == "Opportunity":
                e_opp_has_ref.append(has_ref_row)
            elif entity_type == "Competitor":
                e_comp_has_ref.append(has_ref_row)
            # Section -> Reference (deduped)
            e_section_contains_ref.add((section_key, ref_id))

        # ── Markets ─────────────────────────────────────────────────
        for action, edge_type in [("entering", "ENTERING"), ("exiting", "EXITING"), ("expanding", "EXPANDING")]:
            items = (parsed.get("markets") or {}).get(action) or []
            for item in items:
                if not isinstance(item, dict):
                    continue
                market_name = item.get("market")
                if not market_name:
                    continue
                eid = str(uuid.uuid4())
                evidence = item.get("evidence") or item.get("details") or ""
                reference = item.get("reference") or ""
                markets.append({
                    "id": eid,
                    "label": market_name,
                    "year": int(year),
                    "section": section_id,
                    "link": filing_url,
                    "evidence": evidence,
                    "market_action": action.capitalize(),
                })
                edge_row = {"source_node": ticker, "target_node": eid}
                if edge_type == "ENTERING":
                    e_entering.append(edge_row)
                elif edge_type == "EXITING":
                    e_exiting.append(edge_row)
                else:
                    e_expanding.append(edge_row)
                add_reference(reference, eid, "Market")

        # ── Risks ───────────────────────────────────────────────────
        for item in (parsed.get("risks_opportunities") or {}).get("emerging_risks") or []:
            if not isinstance(item, dict):
                continue
            risk_name = item.get("risk")
            if not risk_name:
                continue
            eid = str(uuid.uuid4())
            risks.append({
                "id": eid,
                "label": risk_name,
                "year": int(year),
                "section": section_id,
                "link": filing_url,
                "description": item.get("description") or "",
            })
            e_faces_risk.append({"source_node": ticker, "target_node": eid})
            add_reference(item.get("reference") or "", eid, "Risk")

        # ── Opportunities ───────────────────────────────────────────
        for item in (parsed.get("risks_opportunities") or {}).get("emerging_opportunities") or []:
            if not isinstance(item, dict):
                continue
            opp_name = item.get("opportunity")
            if not opp_name:
                continue
            eid = str(uuid.uuid4())
            opportunities.append({
                "id": eid,
                "label": opp_name,
                "year": int(year),
                "section": section_id,
                "link": filing_url,
                "description": item.get("description") or "",
            })
            e_pursuing.append({"source_node": ticker, "target_node": eid})
            add_reference(item.get("reference") or "", eid, "Opportunity")

        # ── Competitors ─────────────────────────────────────────────
        for item in parsed.get("competitors") or []:
            if not isinstance(item, dict):
                continue
            comp_name = item.get("name")
            if not comp_name:
                continue
            eid = str(uuid.uuid4())
            competitors.append({
                "id": eid,
                "label": comp_name,
                "year": int(year),
                "section": section_id,
                "link": filing_url,
                "relationship": item.get("relationship") or "",
            })
            e_competes.append({"source_node": ticker, "target_node": eid})
            add_reference(item.get("reference") or "", eid, "Competitor")

    # Convert section_contains_ref set to list of dicts
    e_section_contains_ref_list = [
        {"source_node": s, "target_node": r} for s, r in e_section_contains_ref
    ]

    def df_or_empty(data, columns):
        return pd.DataFrame(data) if data else pd.DataFrame(columns=columns)

    cols2 = ["source_node", "target_node"]

    tables = {
        # Nodes
        "nodes_company": pd.DataFrame(list(companies.values())),
        "nodes_document": df_or_empty(list(documents.values()), ["id", "year", "sec_file_number", "film_number", "link", "company", "company_name", "cik"]),
        "nodes_section": df_or_empty(list(sections.values()), ["id", "label", "section", "document_id", "year", "company", "company_name", "link"]),
        "nodes_reference": df_or_empty(list(references.values()), ["id", "text", "link", "year", "section", "company", "company_name"]),
        "nodes_market": df_or_empty(markets, ["id", "label", "year", "section", "link", "evidence", "market_action"]),
        "nodes_risk": df_or_empty(risks, ["id", "label", "year", "section", "link", "description"]),
        "nodes_opportunity": df_or_empty(opportunities, ["id", "label", "year", "section", "link", "description"]),
        "nodes_competitor": df_or_empty(competitors, ["id", "label", "year", "section", "link", "relationship"]),
        # Provenance edges
        "edges_filed": df_or_empty(e_filed, cols2),
        "edges_doc_contains_section": df_or_empty(e_doc_contains_section, cols2),
        "edges_section_contains_ref": df_or_empty(e_section_contains_ref_list, cols2),
        # Company -> Entity edges
        "edges_entering": df_or_empty(e_entering, cols2),
        "edges_exiting": df_or_empty(e_exiting, cols2),
        "edges_expanding": df_or_empty(e_expanding, cols2),
        "edges_faces_risk": df_or_empty(e_faces_risk, cols2),
        "edges_pursuing": df_or_empty(e_pursuing, cols2),
        "edges_competes": df_or_empty(e_competes, cols2),
        # Entity -> Reference edges
        "edges_market_has_reference": df_or_empty(e_market_has_ref, cols2),
        "edges_risk_has_reference": df_or_empty(e_risk_has_ref, cols2),
        "edges_opportunity_has_reference": df_or_empty(e_opp_has_ref, cols2),
        "edges_competitor_has_reference": df_or_empty(e_comp_has_ref, cols2),
    }

    return tables


def save_tables(tables: dict, output_dir: str):
    """Save all tables as parquet files."""
    parquet_dir = Path(output_dir) / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    for name, df in tables.items():
        out_path = parquet_dir / f"{name}.parquet"
        df.to_parquet(out_path, index=False)
        print(f"  {name}: {len(df)} rows -> {out_path}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Transform LLM extractions to parquet node/edge tables")
    parser.add_argument("--output-dir", type=str, default=get_output_dir())
    args = parser.parse_args()

    print("Parsing insights...")
    records = parse_insights(args.output_dir)
    ok = sum(1 for r in records if r.get("parsed"))
    has_text = sum(1 for r in records if _extract_result_text(r))
    print(f"  {ok}/{len(records)} records with valid JSON")
    if has_text < len(records):
        print(f"  {len(records) - has_text} records missing model output text")

    print("Building node/edge tables...")
    tables = build_tables(records)

    print("Saving parquet files...")
    save_tables(tables, args.output_dir)

    print("\nSummary:")
    for name, df in tables.items():
        if len(df) > 0:
            print(f"  {name}: {len(df)} rows")


if __name__ == "__main__":
    main()
