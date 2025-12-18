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
  -- total OS portofolio
  SUM(funded_amount) AS total_OS,

  -- OS dari loan yang DPD > 30 (Late 31-120 + Default)
  SUM(
    CASE 
      WHEN loan_status IN ("Late (31-120 days)", "Default")
      THEN funded_amount 
      ELSE 0 
    END
  ) AS OS_DPD_gt_30,

  -- TKB30 dalam bentuk ratio
  1 - (
    SUM(
      CASE 
        WHEN loan_status IN ("Late (31-120 days)", "Default")
        THEN funded_amount 
        ELSE 0 
      END
    ) 
    / SUM(funded_amount)
  ) AS TKB30_ratio,

  -- TKB30 dalam persen (biar enak buat deck)
  ROUND(
    (
      1 - (
        SUM(
          CASE 
            WHEN loan_status IN ("Late (31-120 days)", "Default")
            THEN funded_amount 
            ELSE 0 
          END
        ) 
        / SUM(funded_amount)
      )
    ) * 100, 
    2
  ) AS TKB30_percent

FROM portfolio;

-- bener

WITH active_loans AS (
  SELECT *
  FROM `fip_project.loan_customer_joined`
  WHERE 
    LOWER(loan_status) IN ('current', 'in grace period', 'default')
    OR LOWER(loan_status) LIKE 'late%'
),

portfolio_os AS (
  SELECT 
    SUM(funded_amount) AS os
  FROM active_loans
),

dpd30_os AS (
  SELECT 
    SUM(funded_amount) AS os_dpd30
  FROM active_loans
  WHERE 
       LOWER(loan_status) LIKE 'late (31-120 days)'
    OR LOWER(loan_status) = 'default'
)

SELECT
  os AS portfolio_os,
  os_dpd30 AS os_dpd_over_30,
  1 - (os_dpd30 / os) AS tkb30,
  ROUND((1 - (os_dpd30 / os)) * 100, 2) AS tkb30_percent
FROM portfolio_os, dpd30_os;
