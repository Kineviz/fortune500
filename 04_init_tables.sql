CREATE SCHEMA IF NOT EXISTS sec_filings;

-- 0. Configure the Generative AI Model linking to Vertex AI
-- NOTE: Change `us.vertex_ai_connection` if you used a different name for your BigQuery cloud resource connection.
-- ENDPOINT is substituted at run time: bare GEMINI_MODEL ids -> global Vertex URL
-- .../locations/global/publishers/google/models/<id> (or pass a full https:// ENDPOINT).
CREATE OR REPLACE MODEL sec_filings.gemini_pro_latest
  REMOTE WITH CONNECTION `us.vertex_ai_connection`
  OPTIONS (ENDPOINT = '__GEMINI_ENDPOINT__');

-- 1. sections (The master table accumulating all raw data)
CREATE TABLE IF NOT EXISTS sec_filings.sections
(
    filing_id STRING,
    company STRING,
    company_name STRING,
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
    mail_zip STRING,
    filing_url STRING,
    year INT64,
    section_id STRING,
    content STRING
);

-- 2. insights (The master table accumulating AI results)
CREATE TABLE IF NOT EXISTS sec_filings.insights AS
SELECT * FROM 
  AI.GENERATE_TEXT(
    MODEL sec_filings.gemini_pro_latest,
    (SELECT 'dummy' AS prompt, * FROM sec_filings.sections LIMIT 0), 
    STRUCT(0.2 AS temperature, 8192 AS max_output_tokens)
  )
LIMIT 0;
