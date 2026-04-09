CREATE OR REPLACE PROPERTY GRAPH sec_filings.SecGraph
  NODE TABLES (
    sec_filings.nodes_company
      KEY (id)
      LABEL Company,

    sec_filings.nodes_market
      KEY (id)
      LABEL Market,

    sec_filings.nodes_risk
      KEY (id)
      LABEL Risk,

    sec_filings.nodes_opportunity
      KEY (id)
      LABEL Opportunity,

    sec_filings.nodes_competitor
      KEY (id)
      LABEL Competitor,

    sec_filings.nodes_reference
      KEY (id)
      LABEL Reference,

    sec_filings.nodes_document
      KEY (id)
      LABEL Document,

    sec_filings.nodes_section
      KEY (id)
      LABEL Section,

    -- Taxonomies Additions
    sec_filings.nodes_normalized_competitor
      KEY (id)
      LABEL NormalizedCompetitor,

    sec_filings.nodes_geographic_region
      KEY (id)
      LABEL GeographicRegion,

    sec_filings.nodes_market_category
      KEY (id)
      LABEL MarketCategory,

    sec_filings.nodes_risk_category
      KEY (id)
      LABEL RiskCategory
  )
  EDGE TABLES (
    sec_filings.edges_entering
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_company (id)
      DESTINATION KEY (target_node) REFERENCES nodes_market (id)
      LABEL ENTERING,

    sec_filings.edges_expanding
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_company (id)
      DESTINATION KEY (target_node) REFERENCES nodes_market (id)
      LABEL EXPANDING,

    sec_filings.edges_exiting
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_company (id)
      DESTINATION KEY (target_node) REFERENCES nodes_market (id)
      LABEL EXITING,

    sec_filings.edges_faces_risk
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_company (id)
      DESTINATION KEY (target_node) REFERENCES nodes_risk (id)
      LABEL FACES_RISK,

    sec_filings.edges_pursuing
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_company (id)
      DESTINATION KEY (target_node) REFERENCES nodes_opportunity (id)
      LABEL PURSUING,

    sec_filings.edges_competes
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_company (id)
      DESTINATION KEY (target_node) REFERENCES nodes_competitor (id)
      LABEL COMPETES_WITH,

    sec_filings.edges_market_has_reference
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_market (id)
      DESTINATION KEY (target_node) REFERENCES nodes_reference (id)
      LABEL MARKET_HAS_REFERENCE,

    sec_filings.edges_risk_has_reference
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_risk (id)
      DESTINATION KEY (target_node) REFERENCES nodes_reference (id)
      LABEL RISK_HAS_REFERENCE,

    sec_filings.edges_opportunity_has_reference
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_opportunity (id)
      DESTINATION KEY (target_node) REFERENCES nodes_reference (id)
      LABEL OPPORTUNITY_HAS_REFERENCE,

    sec_filings.edges_competitor_has_reference
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_competitor (id)
      DESTINATION KEY (target_node) REFERENCES nodes_reference (id)
      LABEL COMPETITOR_HAS_REFERENCE,

    sec_filings.edges_filed
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_company (id)
      DESTINATION KEY (target_node) REFERENCES nodes_document (id)
      LABEL FILED,

    sec_filings.edges_doc_contains_section
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_document (id)
      DESTINATION KEY (target_node) REFERENCES nodes_section (id)
      LABEL `CONTAINS`,

    sec_filings.edges_section_contains_ref
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_section (id)
      DESTINATION KEY (target_node) REFERENCES nodes_reference (id)
      LABEL `CONTAINS`,

    -- Taxonomy Edges
    sec_filings.edges_instance_of
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_competitor (id)
      DESTINATION KEY (target_node) REFERENCES nodes_normalized_competitor (id)
      LABEL INSTANCE_OF,

    sec_filings.edges_subsidiary_of
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_normalized_competitor (id)
      DESTINATION KEY (target_node) REFERENCES nodes_normalized_competitor (id)
      LABEL SUBSIDIARY_OF,

    sec_filings.edges_has_risk_category
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_risk (id)
      DESTINATION KEY (target_node) REFERENCES nodes_risk_category (id)
      LABEL HAS_RISK_CATEGORY,

    sec_filings.edges_in_region
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_market (id)
      DESTINATION KEY (target_node) REFERENCES nodes_geographic_region (id)
      LABEL IN_REGION,

    sec_filings.edges_in_product_category
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_market (id)
      DESTINATION KEY (target_node) REFERENCES nodes_market_category (id)
      LABEL IN_MARKET_CATEGORY,

    sec_filings.edges_in_market_category
      KEY (source_node, target_node)
      SOURCE KEY (source_node) REFERENCES nodes_normalized_competitor (id)
      DESTINATION KEY (target_node) REFERENCES nodes_market_category (id)
      LABEL IN_MARKET_CATEGORY
  );
