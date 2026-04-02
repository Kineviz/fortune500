-- 1. Create Node Tables

-- Company Nodes
CREATE OR REPLACE TABLE sec_filings.nodes_company AS
SELECT DISTINCT 
  source_node AS id, 
  company_name AS label,
  cik,
  sic,
  irs_number,
  state_of_inc,
  org_name,
  sec_file_number,
  film_number,
  business_street_1,
  business_street_2,
  business_city,
  business_state,
  business_zip,
  business_phone,
  mail_street_1,
  mail_street_2,
  mail_city,
  mail_state,
  mail_zip
FROM sec_filings.graph_edges
WHERE source_label = 'Company';

-- Market Nodes (Instance Nodes)
CREATE OR REPLACE TABLE sec_filings.nodes_market AS
SELECT 
  edge_id AS id, 
  target_node AS label, 
  year, 
  section_id AS section, 
  filing_url AS link,
  properties AS evidence,
  reference
FROM sec_filings.graph_edges
WHERE target_label = 'Market' AND target_node IS NOT NULL;

-- Risk Nodes (Instance Nodes)
CREATE OR REPLACE TABLE sec_filings.nodes_risk AS
SELECT 
  edge_id AS id, 
  target_node AS label, 
  year, 
  section_id AS section, 
  filing_url AS link,
  properties AS description,
  reference
FROM sec_filings.graph_edges
WHERE target_label = 'Risk' AND target_node IS NOT NULL;

-- Opportunity Nodes (Instance Nodes)
CREATE OR REPLACE TABLE sec_filings.nodes_opportunity AS
SELECT 
  edge_id AS id, 
  target_node AS label, 
  year, 
  section_id AS section, 
  filing_url AS link,
  properties AS description,
  reference
FROM sec_filings.graph_edges
WHERE target_label = 'Opportunity' AND target_node IS NOT NULL;

-- Competitor Nodes (Instance Nodes)
CREATE OR REPLACE TABLE sec_filings.nodes_competitor AS
SELECT 
  edge_id AS id, 
  target_node AS label, 
  year, 
  section_id AS section, 
  filing_url AS link,
  properties AS relationship,
  reference
FROM sec_filings.graph_edges
WHERE target_label = 'Competitor' AND target_node IS NOT NULL;


-- 2. Create Edge Tables
-- Note: Edges link Source (Ticker) to Target (Instance Node ID = edge_id)

-- Entering Market Edges
CREATE OR REPLACE TABLE sec_filings.edges_entering AS
SELECT
  edge_id,
  source_node,
  edge_id AS target_node -- References nodes_market.id (which is edge_id)
FROM sec_filings.graph_edges
WHERE edge_type = 'ENTERING' AND target_node IS NOT NULL;

-- Expanding Market Edges
CREATE OR REPLACE TABLE sec_filings.edges_expanding AS
SELECT
  edge_id,
  source_node,
  edge_id AS target_node
FROM sec_filings.graph_edges
WHERE edge_type = 'EXPANDING' AND target_node IS NOT NULL;

-- Exiting Market Edges
CREATE OR REPLACE TABLE sec_filings.edges_exiting AS
SELECT
  edge_id,
  source_node,
  edge_id AS target_node
FROM sec_filings.graph_edges
WHERE edge_type = 'EXITING' AND target_node IS NOT NULL;

-- Faces Risk Edges
CREATE OR REPLACE TABLE sec_filings.edges_faces_risk AS
SELECT
  edge_id,
  source_node,
  edge_id AS target_node
FROM sec_filings.graph_edges
WHERE edge_type = 'FACES_RISK' AND target_node IS NOT NULL;

-- Pursuing Opportunity Edges
CREATE OR REPLACE TABLE sec_filings.edges_pursuing AS
SELECT
  edge_id,
  source_node,
  edge_id AS target_node
FROM sec_filings.graph_edges
WHERE edge_type = 'PURSUING' AND target_node IS NOT NULL;

-- Competes With Edges
CREATE OR REPLACE TABLE sec_filings.edges_competes AS
SELECT
  edge_id,
  source_node,
  edge_id AS target_node
FROM sec_filings.graph_edges
WHERE edge_type = 'COMPETES_WITH' AND target_node IS NOT NULL;
