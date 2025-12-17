-- 1. Create Node Tables

-- Company Nodes
CREATE OR REPLACE TABLE sec_filings.nodes_company AS
SELECT DISTINCT source_node AS id, year, 'Company' AS label
FROM sec_filings.graph_edges
WHERE source_label = 'Company';

-- Market Nodes
CREATE OR REPLACE TABLE sec_filings.nodes_market AS
SELECT DISTINCT target_node AS id, year, properties AS evidence, 'Market' AS label
FROM sec_filings.graph_edges
WHERE target_label = 'Market' AND target_node IS NOT NULL;

-- Risk Nodes
CREATE OR REPLACE TABLE sec_filings.nodes_risk AS
SELECT DISTINCT target_node AS id, year, properties AS description, 'Risk' AS label
FROM sec_filings.graph_edges
WHERE target_label = 'Risk' AND target_node IS NOT NULL;

-- Opportunity Nodes
CREATE OR REPLACE TABLE sec_filings.nodes_opportunity AS
SELECT DISTINCT target_node AS id, year, properties AS description, 'Opportunity' AS label
FROM sec_filings.graph_edges
WHERE target_label = 'Opportunity' AND target_node IS NOT NULL;

-- Competitor Nodes
CREATE OR REPLACE TABLE sec_filings.nodes_competitor AS
SELECT DISTINCT target_node AS id, year, properties AS relationship, 'Competitor' AS label
FROM sec_filings.graph_edges
WHERE target_label = 'Competitor' AND target_node IS NOT NULL;


-- 2. Create Edge Tables
-- Note: We generate a unique edge_id using GENERATE_UUID()

-- Entering Market Edges
CREATE OR REPLACE TABLE sec_filings.edges_entering AS
SELECT
  GENERATE_UUID() AS edge_id,
  source_node,
  target_node
FROM sec_filings.graph_edges
WHERE edge_type = 'ENTERING' AND target_node IS NOT NULL;

-- Expanding Market Edges
CREATE OR REPLACE TABLE sec_filings.edges_expanding AS
SELECT
  GENERATE_UUID() AS edge_id,
  source_node,
  target_node
FROM sec_filings.graph_edges
WHERE edge_type = 'EXPANDING' AND target_node IS NOT NULL;

-- Exiting Market Edges
CREATE OR REPLACE TABLE sec_filings.edges_exiting AS
SELECT
  GENERATE_UUID() AS edge_id,
  source_node,
  target_node
FROM sec_filings.graph_edges
WHERE edge_type = 'EXITING' AND target_node IS NOT NULL;

-- Faces Risk Edges
CREATE OR REPLACE TABLE sec_filings.edges_faces_risk AS
SELECT
  GENERATE_UUID() AS edge_id,
  source_node,
  target_node
FROM sec_filings.graph_edges
WHERE edge_type = 'FACES_RISK' AND target_node IS NOT NULL;

-- Pursuing Opportunity Edges
CREATE OR REPLACE TABLE sec_filings.edges_pursuing AS
SELECT
  GENERATE_UUID() AS edge_id,
  source_node,
  target_node
FROM sec_filings.graph_edges
WHERE edge_type = 'PURSUING' AND target_node IS NOT NULL;

-- Competes With Edges
CREATE OR REPLACE TABLE sec_filings.edges_competes AS
SELECT
  GENERATE_UUID() AS edge_id,
  source_node,
  target_node
FROM sec_filings.graph_edges
WHERE edge_type = 'COMPETES_WITH' AND target_node IS NOT NULL;
