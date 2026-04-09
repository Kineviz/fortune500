"""
Step 3: Create KuzuDB schema and load parquet data.

KuzuDB v0.11.3 schema mirroring fortune500 BigQuery property graph:

  Original layer (8 node tables, 13 relationship tables):
    Company, Document, Section, Reference,
    Market, Risk, Opportunity, Competitor

  Normalization layer (4 node tables, 6 relationship tables):
    NormalizedCompetitor, RiskCategory, GeographicRegion, MarketCategory
    INSTANCE_OF, SUBSIDIARY_OF, IN_MARKET_CATEGORY,
    HAS_RISK_CATEGORY, IN_REGION, IN_PRODUCT_CATEGORY
"""

import shutil
from pathlib import Path

import kuzu

from config import get_kuzu_db_path, get_output_dir

# ── Schema DDL ──────────────────────────────────────────────────────

NODE_TABLES = [
    # ── Original layer ────────────────────────────────────────────
    """CREATE NODE TABLE Company(
        id STRING PRIMARY KEY,
        label STRING,
        cik STRING,
        sic STRING,
        irs_number STRING,
        state_of_inc STRING,
        org_name STRING,
        sec_file_number STRING,
        film_number STRING,
        business_street_1 STRING,
        business_street_2 STRING,
        business_city STRING,
        business_state STRING,
        business_zip STRING,
        business_phone STRING,
        mail_street_1 STRING,
        mail_street_2 STRING,
        mail_city STRING,
        mail_state STRING,
        mail_zip STRING
    )""",
    """CREATE NODE TABLE Document(
        id STRING PRIMARY KEY,
        year INT64,
        sec_file_number STRING,
        film_number STRING,
        link STRING,
        company STRING,
        company_name STRING,
        cik STRING
    )""",
    """CREATE NODE TABLE Section(
        id STRING PRIMARY KEY,
        label STRING,
        section STRING,
        document_id STRING,
        year INT64,
        company STRING,
        company_name STRING,
        link STRING
    )""",
    """CREATE NODE TABLE Reference(
        id STRING PRIMARY KEY,
        text STRING,
        link STRING,
        year INT64,
        section STRING,
        company STRING,
        company_name STRING
    )""",
    """CREATE NODE TABLE Market(
        id STRING PRIMARY KEY,
        label STRING,
        year INT64,
        section STRING,
        link STRING,
        evidence STRING,
        market_action STRING
    )""",
    """CREATE NODE TABLE Risk(
        id STRING PRIMARY KEY,
        label STRING,
        year INT64,
        section STRING,
        link STRING,
        description STRING,
        risk_categories STRING
    )""",
    """CREATE NODE TABLE Opportunity(
        id STRING PRIMARY KEY,
        label STRING,
        year INT64,
        section STRING,
        link STRING,
        description STRING
    )""",
    """CREATE NODE TABLE Competitor(
        id STRING PRIMARY KEY,
        label STRING,
        year INT64,
        section STRING,
        link STRING,
        relationship STRING
    )""",
    # ── Normalization layer ───────────────────────────────────────
    """CREATE NODE TABLE NormalizedCompetitor(
        id STRING PRIMARY KEY,
        label STRING,
        competitor_type STRING,
        sector STRING,
        product_category STRING
    )""",
    """CREATE NODE TABLE RiskCategory(
        id STRING PRIMARY KEY,
        label STRING,
        description STRING
    )""",
    """CREATE NODE TABLE GeographicRegion(
        id STRING PRIMARY KEY,
        label STRING,
        description STRING
    )""",
    """CREATE NODE TABLE MarketCategory(
        id STRING PRIMARY KEY,
        label STRING,
        description STRING
    )""",
]

REL_TABLES = [
    # ── Original layer ────────────────────────────────────────────
    # Provenance chain
    "CREATE REL TABLE FILED(FROM Company TO Document)",
    # CONTAINS has two FROM-TO pairs: Doc→Section and Section→Reference
    "CREATE REL TABLE CONTAINS(FROM Document TO Section, FROM Section TO Reference)",
    # Company → Entity
    "CREATE REL TABLE ENTERING(FROM Company TO Market)",
    "CREATE REL TABLE EXITING(FROM Company TO Market)",
    "CREATE REL TABLE EXPANDING(FROM Company TO Market)",
    "CREATE REL TABLE FACES_RISK(FROM Company TO Risk)",
    "CREATE REL TABLE PURSUING(FROM Company TO Opportunity)",
    "CREATE REL TABLE COMPETES_WITH(FROM Company TO Competitor)",
    # Entity → Reference
    "CREATE REL TABLE MARKET_HAS_REFERENCE(FROM Market TO Reference)",
    "CREATE REL TABLE RISK_HAS_REFERENCE(FROM Risk TO Reference)",
    "CREATE REL TABLE OPPORTUNITY_HAS_REFERENCE(FROM Opportunity TO Reference)",
    "CREATE REL TABLE COMPETITOR_HAS_REFERENCE(FROM Competitor TO Reference)",
    # ── Normalization layer ───────────────────────────────────────
    "CREATE REL TABLE INSTANCE_OF(FROM Competitor TO NormalizedCompetitor)",
    "CREATE REL TABLE SUBSIDIARY_OF(FROM NormalizedCompetitor TO NormalizedCompetitor)",
    "CREATE REL TABLE IN_MARKET_CATEGORY(FROM NormalizedCompetitor TO MarketCategory)",
    "CREATE REL TABLE HAS_RISK_CATEGORY(FROM Risk TO RiskCategory)",
    "CREATE REL TABLE IN_REGION(FROM Market TO GeographicRegion)",
    "CREATE REL TABLE IN_PRODUCT_CATEGORY(FROM Market TO MarketCategory)",
]

# (parquet_file, kuzu_table, is_rel, copy_options)
# copy_options is for multi-FROM-TO rels that need from=/to= specifiers
LOAD_ORDER = [
    # ── Original nodes (referential integrity) ────────────────────
    ("nodes_company", "Company", False, None),
    ("nodes_document", "Document", False, None),
    ("nodes_section", "Section", False, None),
    ("nodes_reference", "Reference", False, None),
    ("nodes_market", "Market", False, None),
    ("nodes_risk_categorized", "Risk", False, None),
    ("nodes_opportunity", "Opportunity", False, None),
    ("nodes_competitor", "Competitor", False, None),
    # ── Normalization nodes ───────────────────────────────────────
    ("nodes_normalized_competitor", "NormalizedCompetitor", False, None),
    ("nodes_risk_category", "RiskCategory", False, None),
    ("nodes_geographic_region", "GeographicRegion", False, None),
    ("nodes_market_category", "MarketCategory", False, None),
    # ── Original provenance edges ─────────────────────────────────
    ("edges_filed", "FILED", True, None),
    ("edges_doc_contains_section", "CONTAINS", True, "(from='Document', to='Section')"),
    ("edges_section_contains_ref", "CONTAINS", True, "(from='Section', to='Reference')"),
    # ── Original Company → Entity edges ───────────────────────────
    ("edges_entering", "ENTERING", True, None),
    ("edges_exiting", "EXITING", True, None),
    ("edges_expanding", "EXPANDING", True, None),
    ("edges_faces_risk", "FACES_RISK", True, None),
    ("edges_pursuing", "PURSUING", True, None),
    ("edges_competes", "COMPETES_WITH", True, None),
    # ── Original Entity → Reference edges ─────────────────────────
    ("edges_market_has_reference", "MARKET_HAS_REFERENCE", True, None),
    ("edges_risk_has_reference", "RISK_HAS_REFERENCE", True, None),
    ("edges_opportunity_has_reference", "OPPORTUNITY_HAS_REFERENCE", True, None),
    ("edges_competitor_has_reference", "COMPETITOR_HAS_REFERENCE", True, None),
    # ── Normalization edges ───────────────────────────────────────
    ("edges_instance_of", "INSTANCE_OF", True, None),
    ("edges_subsidiary_of", "SUBSIDIARY_OF", True, None),
    ("edges_in_market_category", "IN_MARKET_CATEGORY", True, None),
    ("edges_has_risk_category", "HAS_RISK_CATEGORY", True, None),
    ("edges_in_region", "IN_REGION", True, None),
    ("edges_in_product_category", "IN_PRODUCT_CATEGORY", True, None),
]


def create_database(db_path: str, parquet_dir: str, reset: bool = False):
    """Create KuzuDB and load all parquet data."""
    db_path = Path(db_path)
    parquet_dir = Path(parquet_dir)

    if reset and db_path.exists():
        print(f"Removing existing database at {db_path}")
        if db_path.is_dir():
            shutil.rmtree(db_path)
        else:
            db_path.unlink()

    # Ensure parent directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Opening KuzuDB at {db_path}")
    db = kuzu.Database(str(db_path))
    conn = kuzu.Connection(db)

    # Create schema
    print("Creating node tables...")
    for ddl in NODE_TABLES:
        try:
            conn.execute(ddl)
            table_name = ddl.split("TABLE")[1].split("(")[0].strip()
            print(f"  Created {table_name}")
        except Exception as e:
            if "already exists" in str(e).lower():
                table_name = ddl.split("TABLE")[1].split("(")[0].strip()
                print(f"  {table_name} already exists, skipping")
            else:
                raise

    print("Creating relationship tables...")
    for ddl in REL_TABLES:
        try:
            conn.execute(ddl)
            table_name = ddl.split("TABLE")[1].split("(")[0].strip()
            print(f"  Created {table_name}")
        except Exception as e:
            if "already exists" in str(e).lower():
                table_name = ddl.split("TABLE")[1].split("(")[0].strip()
                print(f"  {table_name} already exists, skipping")
            else:
                raise

    # Load data
    print("\nLoading data from parquet files...")
    for parquet_name, table_name, is_rel, copy_opts in LOAD_ORDER:
        parquet_file = parquet_dir / f"{parquet_name}.parquet"
        if not parquet_file.exists():
            print(f"  SKIP {table_name} (no file: {parquet_file.name})")
            continue

        import pandas as pd
        df = pd.read_parquet(parquet_file)
        if len(df) == 0:
            print(f"  SKIP {table_name} (0 rows)")
            continue

        try:
            query = f'COPY {table_name} FROM "{parquet_file}"'
            if copy_opts:
                query += f" {copy_opts}"
            conn.execute(query)
            print(f"  Loaded {table_name} <- {parquet_name}: {len(df)} rows")
        except Exception as e:
            print(f"  ERROR loading {table_name} <- {parquet_name}: {e}")

    # Verify
    print("\nVerification:")
    for _, table_name, is_rel, _ in LOAD_ORDER:
        try:
            if is_rel:
                result = conn.execute(f"MATCH ()-[r:{table_name}]->() RETURN count(r) AS n")
            else:
                result = conn.execute(f"MATCH (n:{table_name}) RETURN count(n) AS n")
            count = result.get_as_df().iloc[0]["n"]
            if count > 0:
                print(f"  {table_name}: {count}")
        except Exception:
            pass

    print(f"\nDatabase ready at: {db_path}")
    return db


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Load parquet data into KuzuDB")
    parser.add_argument("--db-path", type=str, default=get_kuzu_db_path())
    parser.add_argument("--output-dir", type=str, default=get_output_dir())
    parser.add_argument("--reset", action="store_true", help="Delete and recreate database")
    args = parser.parse_args()

    parquet_dir = Path(args.output_dir) / "parquet"
    if not parquet_dir.exists():
        print(f"ERROR: Run transform.py first. Missing: {parquet_dir}")
        return

    create_database(args.db_path, str(parquet_dir), reset=args.reset)


if __name__ == "__main__":
    main()
