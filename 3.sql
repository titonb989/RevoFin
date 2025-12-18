WITH portfolio AS (
  SELECT
    ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(customer_id, r"[a-zA-Z0-9]"), "") AS customer_id_clean,
    funded_amount,
    loan_status
  FROM `next-porto.fip.loan`
  WHERE loan_status IN (
    "Current",
    "In Grace Period",
    "Late (16-30 days)",
    "Late (31-120 days)",
    "Default"
  )
)

SELECT
  COUNT(DISTINCT customer_id_clean) AS total_customers_in_portfolio,

  SUM(funded_amount) AS total_OS,

  -- ENR excludes DPD > 30 loans: Late (31-120 days) and Default
  SUM(CASE 
        WHEN loan_status IN ("Default")
        THEN 0 
        ELSE funded_amount 
      END) AS ENR

FROM portfolio;


-- bener
WITH active_loans AS (
  SELECT *
  FROM `fip_project.loan_customer_joined`
  WHERE 
    LOWER(loan_status) IN ('current', 'in grace period', 'default')
    OR LOWER(loan_status) LIKE 'late%'
)

SELECT 
  COUNT(DISTINCT customer_id_clean) AS total_customers_in_portfolio,

  -- OS = total funded amount of all active loans
  SUM(funded_amount) AS total_outstanding_amount_OS,

  -- ENR = OS excluding Default loans
  SUM(
    CASE 
      WHEN LOWER(loan_status) = 'default' THEN 0 
      ELSE funded_amount 
    END
  ) AS ENR
FROM active_loans;
