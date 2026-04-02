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
  properties AS evidence
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
  properties AS description
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
  properties AS description
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
  properties AS relationship
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

-- 3. Reference Nodes and Edges

-- Reference Nodes
CREATE OR REPLACE FUNCTION sec_filings.textFragmentStart(str STRING)
RETURNS STRING
LANGUAGE js AS """
  if (!str) return '';
  const words = str.trim().split(/\\s+/);
  if (words.length <= 10) {
    return encodeURIComponent(str);
  } else {
    return encodeURIComponent(words.slice(0, 5).join(' ')) + ',' + encodeURIComponent(words.slice(-5).join(' '));
  }
""";

CREATE OR REPLACE TABLE sec_filings.nodes_reference AS
SELECT
  TO_HEX(MD5(reference)) AS id,
  ANY_VALUE(reference) AS text,
  ANY_VALUE(CONCAT(
    REPLACE(filing_url, 'ix?doc=/', ''),
    '#:~:text=',
    sec_filings.textFragmentStart(CAST(reference AS STRING))
  )) AS link,
  ANY_VALUE(year) AS year,
  ANY_VALUE(section_id) AS section,
  ANY_VALUE(source_node) AS company,
  ANY_VALUE(company_name) AS company_name
FROM sec_filings.graph_edges
WHERE reference IS NOT NULL AND TRIM(reference) != ''
GROUP BY 1;

-- Has Reference Edges - Market
CREATE OR REPLACE TABLE sec_filings.edges_market_has_reference AS
SELECT
  CONCAT(edge_id, '_has_ref') AS edge_id,
  edge_id AS source_node,
  TO_HEX(MD5(reference)) AS target_node
FROM sec_filings.graph_edges
WHERE target_label = 'Market' AND target_node IS NOT NULL AND reference IS NOT NULL AND TRIM(reference) != '';

-- Has Reference Edges - Risk
CREATE OR REPLACE TABLE sec_filings.edges_risk_has_reference AS
SELECT
  CONCAT(edge_id, '_has_ref') AS edge_id,
  edge_id AS source_node,
  TO_HEX(MD5(reference)) AS target_node
FROM sec_filings.graph_edges
WHERE target_label = 'Risk' AND target_node IS NOT NULL AND reference IS NOT NULL AND TRIM(reference) != '';

-- Has Reference Edges - Opportunity
CREATE OR REPLACE TABLE sec_filings.edges_opportunity_has_reference AS
SELECT
  CONCAT(edge_id, '_has_ref') AS edge_id,
  edge_id AS source_node,
  TO_HEX(MD5(reference)) AS target_node
FROM sec_filings.graph_edges
WHERE target_label = 'Opportunity' AND target_node IS NOT NULL AND reference IS NOT NULL AND TRIM(reference) != '';

-- Has Reference Edges - Competitor
CREATE OR REPLACE TABLE sec_filings.edges_competitor_has_reference AS
SELECT
  CONCAT(edge_id, '_has_ref') AS edge_id,
  edge_id AS source_node,
  TO_HEX(MD5(reference)) AS target_node
FROM sec_filings.graph_edges
WHERE target_label = 'Competitor' AND target_node IS NOT NULL AND reference IS NOT NULL AND TRIM(reference) != '';

-- 4. Document and Section Nodes

-- Document Nodes
CREATE OR REPLACE TABLE sec_filings.nodes_document AS
SELECT DISTINCT
  filing_url AS id,
  year,
  sec_file_number,
  film_number,
  source_node AS company,
  company_name,
  cik,
  filing_url AS link
FROM sec_filings.graph_edges
WHERE filing_url IS NOT NULL;

-- Section Nodes
CREATE OR REPLACE TABLE sec_filings.nodes_section AS
SELECT DISTINCT
  CONCAT(filing_url, '#', section_id) AS id,
  section_id AS label,
  section_id AS section,
  source_node AS company,
  company_name,
  year,
  filing_url AS document_id,
  CONCAT(filing_url, '#', section_id) AS link
FROM sec_filings.graph_edges
WHERE filing_url IS NOT NULL AND section_id IS NOT NULL;

-- 5. Document and Section Edges

-- Company FILED Document
CREATE OR REPLACE TABLE sec_filings.edges_company_filed_document AS
SELECT DISTINCT
  CONCAT(source_node, '_filed_', TO_HEX(MD5(filing_url))) AS edge_id,
  source_node,
  filing_url AS target_node
FROM sec_filings.graph_edges
WHERE filing_url IS NOT NULL;

-- Document CONTAINS Section
CREATE OR REPLACE TABLE sec_filings.edges_document_contains_section AS
SELECT DISTINCT
  CONCAT(filing_url, '_contains_', TO_HEX(MD5(CONCAT(filing_url, '#', section_id)))) AS edge_id,
  filing_url AS source_node,
  CONCAT(filing_url, '#', section_id) AS target_node
FROM sec_filings.graph_edges
WHERE filing_url IS NOT NULL AND section_id IS NOT NULL;

-- Section CONTAINS Reference
CREATE OR REPLACE TABLE sec_filings.edges_section_contains_reference AS
SELECT DISTINCT
  CONCAT(filing_url, '#', section_id, '_contains_', TO_HEX(MD5(reference))) AS edge_id,
  CONCAT(filing_url, '#', section_id) AS source_node,
  TO_HEX(MD5(reference)) AS target_node
FROM sec_filings.graph_edges
WHERE filing_url IS NOT NULL AND section_id IS NOT NULL AND reference IS NOT NULL AND TRIM(reference) != '';
