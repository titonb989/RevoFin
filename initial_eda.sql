SELECT
  loan_status,
  COUNT(*) as loan_count,
  SUM(funded_amount) as total_funded,
  ROUND(SUM(funded_amount) / SUM(SUM(funded_amount)) 
  OVER () * 100, 2) as pct_of_total_funding
FROM next-porto.fip.loan

GROUP BY loan_status
ORDER BY loan_count DESC

SELECT
  loan_id,
  customer_id AS raw_customer_id,
  ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(customer_id, r'[a-zA-Z0-9]'), '') AS clean_customer_id,
  loan_status,
  funded_amount
FROM next-porto.fip.loan
LIMIT 10

WITH cleaned_loans AS (
  SELECT
    loan_id,
    ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(customer_id, r'[a-zA-Z0-9]'), '') AS clean_customer_id,
    loan_status,
    funded_amount,
    loan_amount,
    state,
    int_rate,
    grade,
    issue_year,
    purpose
  FROM next-porto.fip.loan
)

SELECT
  loan_id,
  clean_customer_id,
  loan_status,
  funded_amount,
  loan_amount,
  state,
  int_rate,
  grade,
  issue_year,
  purpose
FROM cleaned_loans
WHERE loan_status IN (
  'Current', 
  'In Grace Period', 
  'Late (16-30 days)', 
  'Late (31-120 days)', 
  'Default'
)
ORDER BY clean_customer_id, issue_year
