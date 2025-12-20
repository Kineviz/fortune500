-- 1. sections (The master table accumulating all raw data)
CREATE OR REPLACE TABLE sec_filings.sections
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
CREATE OR REPLACE TABLE sec_filings.insights AS
SELECT * FROM 
  ML.GENERATE_TEXT(
    MODEL `sec_filings.gemini_pro`,
    (SELECT 'dummy' AS prompt, * FROM sec_filings.sections LIMIT 0), 
    STRUCT(0.2 AS temperature, 8192 AS max_output_tokens, FALSE AS flatten_json_output)
  )
LIMIT 0;
