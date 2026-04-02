CREATE OR REPLACE PROPERTY GRAPH sec_filings.SecGraph
  NODE TABLES (
    sec_filings.nodes_company
      KEY (id)
      LABEL Company
      PROPERTIES (id, label, cik, sic, irs_number, state_of_inc, org_name, sec_file_number, film_number, business_street_1, business_street_2, business_city, business_state, business_zip, business_phone, mail_street_1, mail_street_2, mail_city, mail_state, mail_zip),

    sec_filings.nodes_market
      KEY (id)
      LABEL Market
      PROPERTIES (id, label, year, section, link, evidence, market_action),

    sec_filings.nodes_risk
      KEY (id)
      LABEL Risk
      PROPERTIES (id, label, year, section, link, description),

    sec_filings.nodes_opportunity
      KEY (id)
      LABEL Opportunity
      PROPERTIES (id, label, year, section, link, description),

    sec_filings.nodes_competitor
      KEY (id)
      LABEL Competitor
      PROPERTIES (id, label, year, section, link, relationship),

    sec_filings.nodes_reference
      KEY (id)
      LABEL Reference
      PROPERTIES (id, text, link, year, section, company, company_name),

    sec_filings.nodes_document
      KEY (id)
      LABEL Document
      PROPERTIES (id, year, sec_file_number, film_number, link, company, company_name, cik),

    sec_filings.nodes_section
      KEY (id)
      LABEL Section
      PROPERTIES (id, label, section, document_id, year, company, company_name, link)
  )
  EDGE TABLES (
    sec_filings.edges_entering
      KEY (edge_id)
      SOURCE KEY (source_node) REFERENCES nodes_company (id)
      DESTINATION KEY (target_node) REFERENCES nodes_market (id)
      LABEL ENTERING,

    sec_filings.edges_expanding
      KEY (edge_id)
      SOURCE KEY (source_node) REFERENCES nodes_company (id)
      DESTINATION KEY (target_node) REFERENCES nodes_market (id)
      LABEL EXPANDING,

    sec_filings.edges_exiting
      KEY (edge_id)
      SOURCE KEY (source_node) REFERENCES nodes_company (id)
      DESTINATION KEY (target_node) REFERENCES nodes_market (id)
      LABEL EXITING,

    sec_filings.edges_faces_risk
      KEY (edge_id)
      SOURCE KEY (source_node) REFERENCES nodes_company (id)
      DESTINATION KEY (target_node) REFERENCES nodes_risk (id)
      LABEL FACES_RISK,

    sec_filings.edges_pursuing
      KEY (edge_id)
      SOURCE KEY (source_node) REFERENCES nodes_company (id)
      DESTINATION KEY (target_node) REFERENCES nodes_opportunity (id)
      LABEL PURSUING,

    sec_filings.edges_competes
      KEY (edge_id)
      SOURCE KEY (source_node) REFERENCES nodes_company (id)
      DESTINATION KEY (target_node) REFERENCES nodes_competitor (id)
      LABEL COMPETES_WITH,

    sec_filings.edges_market_has_reference
      KEY (edge_id)
      SOURCE KEY (source_node) REFERENCES nodes_market (id)
      DESTINATION KEY (target_node) REFERENCES nodes_reference (id)
      LABEL MARKET_HAS_REFERENCE,

    sec_filings.edges_risk_has_reference
      KEY (edge_id)
      SOURCE KEY (source_node) REFERENCES nodes_risk (id)
      DESTINATION KEY (target_node) REFERENCES nodes_reference (id)
      LABEL RISK_HAS_REFERENCE,

    sec_filings.edges_opportunity_has_reference
      KEY (edge_id)
      SOURCE KEY (source_node) REFERENCES nodes_opportunity (id)
      DESTINATION KEY (target_node) REFERENCES nodes_reference (id)
      LABEL OPPORTUNITY_HAS_REFERENCE,

    sec_filings.edges_competitor_has_reference
      KEY (edge_id)
      SOURCE KEY (source_node) REFERENCES nodes_competitor (id)
      DESTINATION KEY (target_node) REFERENCES nodes_reference (id)
      LABEL COMPETITOR_HAS_REFERENCE,

    sec_filings.edges_company_filed_document
      KEY (edge_id)
      SOURCE KEY (source_node) REFERENCES nodes_company (id)
      DESTINATION KEY (target_node) REFERENCES nodes_document (id)
      LABEL FILED,

    sec_filings.edges_document_contains_section
      KEY (edge_id)
      SOURCE KEY (source_node) REFERENCES nodes_document (id)
      DESTINATION KEY (target_node) REFERENCES nodes_section (id)
      LABEL `CONTAINS`,

    sec_filings.edges_section_contains_reference
      KEY (edge_id)
      SOURCE KEY (source_node) REFERENCES nodes_section (id)
      DESTINATION KEY (target_node) REFERENCES nodes_reference (id)
      LABEL `CONTAINS`
  );
