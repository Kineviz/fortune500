CREATE OR REPLACE PROPERTY GRAPH sec_filings.SecGraph
  NODE TABLES (
    sec_filings.nodes_company
      KEY (id)
      LABEL Company
      PROPERTIES (id, year),

    sec_filings.nodes_market
      KEY (id)
      LABEL Market
      PROPERTIES (id, year, evidence),

    sec_filings.nodes_risk
      KEY (id)
      LABEL Risk
      PROPERTIES (id, year, description),

    sec_filings.nodes_opportunity
      KEY (id)
      LABEL Opportunity
      PROPERTIES (id, year, description),

    sec_filings.nodes_competitor
      KEY (id)
      LABEL Competitor
      PROPERTIES (id, year, relationship)
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
  );
