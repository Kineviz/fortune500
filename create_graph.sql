CREATE OR REPLACE TABLE sec_filings.graph_edges AS

WITH json_data AS (
   SELECT
    company,
    year,
    -- Extract the text content from the Gemini JSON result
    JSON_VALUE(ml_generate_text_result, '$.candidates[0].content.parts[0].text') as raw_text
   FROM sec_filings.insights
),
parsed_data AS (
  SELECT
    company,
    year,
    -- Parse the extracted text as JSON. Clean markdown if present just in case.
    SAFE.PARSE_JSON(
      REGEXP_REPLACE(REGEXP_REPLACE(raw_text, '^```json\\n', ''), '\\n```$', '')
    ) as data
  FROM json_data
)

-- 1. Market Edges
SELECT
  company AS source_node,
  year,
  'Company' AS source_label,
  JSON_VALUE(market_item, '$.market') AS target_node,
  'Market' AS target_label,
  'ENTERING' AS edge_type,
  JSON_VALUE(market_item, '$.evidence') AS properties
FROM parsed_data,
UNNEST(JSON_QUERY_ARRAY(data, '$.markets.entering')) AS market_item

UNION ALL

SELECT
  company AS source_node,
  year,
  'Company' AS source_label,
  JSON_VALUE(market_item, '$.market') AS target_node,
  'Market' AS target_label,
  'EXITING' AS edge_type,
  JSON_VALUE(market_item, '$.evidence') AS properties
FROM parsed_data,
UNNEST(JSON_QUERY_ARRAY(data, '$.markets.exiting')) AS market_item

UNION ALL

SELECT
  company AS source_node,
  year,
  'Company' AS source_label,
  JSON_VALUE(market_item, '$.market') AS target_node,
  'Market' AS target_label,
  'EXPANDING' AS edge_type,
  JSON_VALUE(market_item, '$.details') AS properties
FROM parsed_data,
UNNEST(JSON_QUERY_ARRAY(data, '$.markets.expanding')) AS market_item

UNION ALL

-- 2. Risk/Opportunity Edges (Emerging Risks)
SELECT
  company AS source_node,
  year,
  'Company' AS source_label,
  JSON_VALUE(risk_item, '$.risk') AS target_node,
  'Risk' AS target_label,
  'FACES_RISK' AS edge_type,
  JSON_VALUE(risk_item, '$.description') AS properties
FROM parsed_data,
UNNEST(JSON_QUERY_ARRAY(data, '$.risks_opportunities.emerging_risks')) AS risk_item

UNION ALL

-- 2b. Emerging Opportunities (New edge type: PURSUING_OPPORTUNITY)
SELECT
  company AS source_node,
  year,
  'Company' AS source_label,
  JSON_VALUE(opp_item, '$.opportunity') AS target_node,
  'Opportunity' AS target_label,
  'PURSUING' AS edge_type,
  JSON_VALUE(opp_item, '$.description') AS properties
FROM parsed_data,
UNNEST(JSON_QUERY_ARRAY(data, '$.risks_opportunities.emerging_opportunities')) AS opp_item

UNION ALL

-- 3. Competitor Edges
SELECT
  company AS source_node,
  year,
  'Company' AS source_label,
  JSON_VALUE(comp_item, '$.name') AS target_node,
  'Competitor' AS target_label,
  'COMPETES_WITH' AS edge_type,
  JSON_VALUE(comp_item, '$.relationship') AS properties
FROM parsed_data,
UNNEST(JSON_QUERY_ARRAY(data, '$.competitors')) AS comp_item;
