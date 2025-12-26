-- Add market_action column to nodes_market table
-- Note: If the column already exists, this will fail. Remove this statement if the column already exists.
ALTER TABLE sec_filings.nodes_market
ADD COLUMN market_action STRING;

-- Update market_action based on matches with edge tables
UPDATE sec_filings.nodes_market AS m
SET market_action = 
  CASE
    WHEN EXISTS (
      SELECT 1 
      FROM sec_filings.edges_entering e 
      WHERE e.target_node = m.id
    ) THEN 'Entering'
    WHEN EXISTS (
      SELECT 1 
      FROM sec_filings.edges_exiting e 
      WHERE e.target_node = m.id
    ) THEN 'Exiting'
    WHEN EXISTS (
      SELECT 1 
      FROM sec_filings.edges_expanding e 
      WHERE e.target_node = m.id
    ) THEN 'Expanding'
    ELSE NULL
  END
WHERE TRUE;

