CREATE OR REPLACE PROPERTY GRAPH sec_filings.SecGraph
  NODE TABLES (
    sec_filings.nodes_company
      KEY (id)
      LABEL Company
      PROPERTIES (id, label, cik, sic, irs_number, state_of_inc, org_name, sec_file_number, film_number, business_street_1, business_street_2, business_city, business_state, business_zip, business_phone, mail_street_1, mail_street_2, mail_city, mail_state, mail_zip),

    sec_filings.nodes_market
      KEY (id)
      LABEL Market
      PROPERTIES (id, label, year, section, link, evidence, market_action, reference),

    sec_filings.nodes_risk
      KEY (id)
      LABEL Risk
      PROPERTIES (id, label, year, section, link, description, reference),

    sec_filings.nodes_opportunity
      KEY (id)
      LABEL Opportunity
      PROPERTIES (id, label, year, section, link, description, reference),

    sec_filings.nodes_competitor
      KEY (id)
      LABEL Competitor
      PROPERTIES (id, label, year, section, link, relationship, reference)
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
      LABEL COMPETES_WITH
  );
