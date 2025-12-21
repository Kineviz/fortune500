INSERT INTO sec_filings.insights
SELECT *
FROM
  ML.GENERATE_TEXT(
    MODEL `sec_filings.gemini_pro`,
    (
      SELECT
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
        ) AS prompt,
        filing_id,
        company,
        company_name,
        CAST(cik AS STRING) AS cik,
        CAST(sic AS STRING) AS sic,
        CAST(irs_number AS STRING) AS irs_number,
        state_of_inc,
        org_name,
        CAST(sec_file_number AS STRING) AS sec_file_number,
        CAST(film_number AS STRING) AS film_number,
        business_street_1,
        business_street_2,
        business_city,
        business_state,
        CAST(business_zip AS STRING) AS business_zip,
        CAST(business_phone AS STRING) AS business_phone,
        mail_street_1,
        mail_street_2,
        mail_city,
        mail_state,
        CAST(mail_zip AS STRING) AS mail_zip,
        filing_url,
        year,
        section_id,
        content
      FROM
        `sec_filings.sections_staging`
      WHERE
        section_id IN ('Item 1.', 'Item 1A.', 'Item 7.')
    ),
    STRUCT(
      0.2 AS temperature,
      8192 AS max_output_tokens,
      FALSE AS flatten_json_output
    )
  );
