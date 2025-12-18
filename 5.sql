WITH portfolio AS (
  SELECT
    ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(customer_id, r"[a-zA-Z0-9]"), "") AS customer_id_clean,
    funded_amount,
    loan_status,
    CAST(issue_year AS INT64) AS cohort_year   -- 🟢 pakai issue_year
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
  cohort_year,

  -- OS per cohort
  SUM(funded_amount) AS cohort_OS,

  -- ENR per cohort (exclude DPD>30)
  SUM(
    CASE 
      WHEN loan_status IN ("Default")
      THEN 0
      ELSE funded_amount
    END
  ) AS cohort_ENR,

  -- OS DPD > 30
  SUM(
    CASE 
      WHEN loan_status IN ("Late (31-120 days)", "Default")
      THEN funded_amount
      ELSE 0
    END
  ) AS OS_DPD_gt_30,

  -- TKB30 numeric
  1 - (
    SUM(
      CASE WHEN loan_status IN ("Late (31-120 days)", "Default")
      THEN funded_amount ELSE 0 END
    ) / SUM(funded_amount)
  ) AS TKB30_ratio,

  -- TKB30 in %
  ROUND(
    (
      1 - (
        SUM(
          CASE WHEN loan_status IN ("Late (31-120 days)", "Default")
          THEN funded_amount ELSE 0 END
        ) / SUM(funded_amount)
      )
    ) * 100,
    2
  ) AS TKB30_percent

FROM portfolio
GROUP BY cohort_year
ORDER BY cohort_year;
