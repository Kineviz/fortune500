CREATE OR REPLACE TABLE sec_filings.insights AS
SELECT *
FROM
  ML.GENERATE_TEXT(
    MODEL `sec_filings.gemini_pro`,
    (
      SELECT
        filing_id,
        company,
        company_name,
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
        mail_zip,
        filing_url,
        year,
        section_id,
        content,
        CONCAT(
          'Analyze the following text from a 10-K filing (Section: ', section_id, '). ',
          'Extract insights for the following questions and return ONLY valid JSON matching this EXACT schema:\n',
          '{\n',
          '  "markets": {\n',
          '    "entering": [{"market": "Name", "evidence": "Details..."}],\n',
          '    "exiting": [{"market": "Name", "evidence": "Details..."}],\n',
          '    "expanding": [{"market": "Name", "details": "Details..."}]\n',
          '  },\n',
          '  "risks_opportunities": {\n',
          '    "emerging_risks": [{"risk": "Name", "description": "Details..."}],\n',
          '    "emerging_opportunities": [{"opportunity": "Name", "description": "Details..."}]\n',
          '  },\n',
          '  "competitors": [{"name": "Name", "relationship": "Details..."}]\n',
          '}\n\n',
          'Do NOT use markdown code blocks. Return raw JSON only.\n',
          'Text:\n',
          SUBSTR(content, 1, 100000)
        ) AS prompt
      FROM
        `sec_filings.sections`
      WHERE
        section_id IN ('Item 1.', 'Item 1A.', 'Item 3.', 'Item 7.', 'Item 7A.')
    ),
    STRUCT(
      0.2 AS temperature,
      8192 AS max_output_tokens,
      FALSE AS flatten_json_output
    )
  );
