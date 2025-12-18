CREATE OR REPLACE TABLE `fsda-sql-467016.fip_project_0.customer_cleaned` AS
SELECT
  ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(customer_id, r"[a-zA-Z0-9]"), "") 
      AS customer_id_clean,
  *
EXCEPT(customer_id),
  customer_id AS customer_id_raw
FROM `next-porto.fip.customer`;



CREATE OR REPLACE TABLE `fsda-sql-467016.fip_project_0.loan_cleaned` AS
SELECT
  ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(customer_id, r"[a-zA-Z0-9]"), "")
      AS customer_id_clean,
  *
EXCEPT(customer_id),
  customer_id AS customer_id_raw
FROM `next-porto.fip.loan`;



CREATE OR REPLACE TABLE `fsda-sql-467016.fip_project_0.loan_base` AS
WITH base AS (
  SELECT
    loan_id,
    customer_id_clean,
    loan_status,
    funded_amount,
    loan_amount,
    state,
    purpose,
    type,
    grade,
    installment,
    int_rate,
    term,
    issue_d,
    issue_year,

    -- Flag: loan included in portfolio
    CASE
      WHEN loan_status IN (
        'Current',
        'In Grace Period',
        'Late',
        'Default'
      ) THEN 1 ELSE 0
    END AS in_portfolio_flag,

    -- DPD > 30 classifier (template)
    CASE
      WHEN loan_status IN (
        'Default',
        'Late (31-120 days)',
        'Late (31-60 days)',
        'Late (61-90 days)'
      ) THEN 1
      ELSE 0
    END AS is_dpd_30_plus

  FROM `fsda-sql-467016.fip_project_0.loan_cleaned`
)

SELECT * FROM base;

SELECT * FROM `fsda-sql-467016.fip_project_0.customer_cleaned`

SELECT * FROM `next-porto.fip.customer`

SELECT * FROM `fsda-sql-467016.fip_project_0.loan_cleaned`

SELECT
  COUNT(*) AS total_rows,
  COUNTIF(home_ownership IS NULL) AS null_home_ownership,
  COUNTIF(annual_inc IS NULL) AS null_annual_inc,
  COUNTIF(emp_length IS NULL OR emp_length = 'n/a') AS missing_emp_length
FROM `fsda-sql-467016.fip_project_0.customer_cleaned`;

SELECT
  customer_id_clean,
  emp_length,
  CASE
    WHEN emp_length IS NULL OR emp_length = 'n/a' THEN 'Unknown'
    WHEN emp_length LIKE '%< 1%' THEN '< 1 year'
    WHEN emp_length LIKE '%1%' THEN '1 year'
    WHEN emp_length LIKE '%2%' THEN '2 years'
    WHEN emp_length LIKE '%3%' THEN '3 years'
    WHEN emp_length LIKE '%4%' THEN '4 years'
    WHEN emp_length LIKE '%5%' THEN '5 years'
    WHEN emp_length LIKE '%6%' THEN '6 years'
    WHEN emp_length LIKE '%7%' THEN '7 years'
    WHEN emp_length LIKE '%8%' THEN '8 years'
    WHEN emp_length LIKE '%9%' THEN '9 years'
    WHEN emp_length LIKE '%10%' THEN '10+ years'
    ELSE 'Unknown'
  END AS emp_length_bucket
FROM `fsda-sql-467016.fip_project_0.customer_cleaned`;

CREATE TABLE `fsda-sql-467016.fip_project_0.loan_with_region` AS

SELECT *

FROM `next-porto.fip.loan_with_region`;

CREATE TABLE `fsda-sql-467016.fip_project_0.state_regions` AS

SELECT *

FROM `next-porto.fip.state_regions`;

SELECT
  loan_id,
  loan_amount,
  funded_amount,
  loan_status
FROM `fsda-sql-467016.fip_project_0.loan_cleaned`
LIMIT 50;
