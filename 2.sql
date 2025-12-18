SELECT *
FROM `fsda-sql-467016.fip_project.loan_cleaned`
WHERE loan_status IN (
  "Current",
  "In Grace Period",
  "Late (16-30 days)",
  "Late (31-120 days)",
  "Default"
);

SELECT 
  COUNT(*) AS total_loans_in_portfolio,
  loan_status
FROM `fsda-sql-467016.fip_project.loan_cleaned`
WHERE loan_status IN (
  "Current",
  "In Grace Period",
  "Late (16-30 days)",
  "Late (31-120 days)",
  "Default"
)
GROUP BY loan_status;

-- bener
SELECT 
  loan_status,
  COUNT(*) AS total_loans_in_portfolio
FROM `fip_project.loan_customer_joined`
WHERE 
  LOWER(loan_status) IN ('current', 'in grace period', 'default')
  OR LOWER(loan_status) LIKE 'late%'
GROUP BY loan_status
ORDER BY total_loans_in_portfolio DESC;
