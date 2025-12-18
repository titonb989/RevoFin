WITH cohort_metrics AS (
  SELECT
    CAST(issue_year AS INT64) AS cohort_year,
    SUM(funded_amount) AS cohort_OS,

    SUM(
      CASE 
        WHEN loan_status IN ("Late (31-120 days)", "Default")
        THEN 0
        ELSE funded_amount
      END
    ) AS cohort_ENR,

    SUM(
      CASE 
        WHEN loan_status IN ("Late (31-120 days)", "Default")
        THEN funded_amount
        ELSE 0
      END
    ) AS OS_DPD_gt_30,

    1 - (
      SUM(
        CASE 
          WHEN loan_status IN ("Late (31-120 days)", "Default")
          THEN funded_amount ELSE 0 END
      ) / SUM(funded_amount)
    ) AS TKB30_ratio
  FROM `next-porto.fip.loan`
  WHERE loan_status IN (
    "Current",
    "In Grace Period",
    "Late (16-30 days)",
    "Late (31-120 days)",
    "Default"
  )
  GROUP BY cohort_year
),

-- cari percentile 33 & 66
pct AS (
  SELECT
    APPROX_QUANTILES(TKB30_ratio, 100)[OFFSET(33)] AS p33,
    APPROX_QUANTILES(TKB30_ratio, 100)[OFFSET(66)] AS p66
  FROM cohort_metrics
)

SELECT
  c.*,
  ROUND(c.TKB30_ratio * 100, 2) AS TKB30_percent,
  CASE
    WHEN c.TKB30_ratio <  pct.p33 THEN 'Low TKB30'
    WHEN c.TKB30_ratio <  pct.p66 THEN 'Medium TKB30'
    ELSE 'High TKB30'
  END AS TKB30_segment
FROM cohort_metrics c
CROSS JOIN pct
ORDER BY cohort_year;


--------------
WITH yearly_cohort_metrics AS (
  SELECT
    CAST(issue_year AS INT64) AS cohort_year,
    1 - (
      SUM(CASE WHEN loan_status IN ("Default") THEN funded_amount ELSE 0 END)
      / SUM(funded_amount)
    ) AS TKB30
  FROM next-porto.fip.loan
  WHERE loan_status IN (
    "Current", "In Grace Period", "Late (16-30 days)", "Late (31-120 days)", "Default"
  )
  GROUP BY cohort_year
),

percentiles AS (
  SELECT
    PERCENTILE_CONT(TKB30, 0.33) OVER () AS tkb30_33rd_percentile,
    PERCENTILE_CONT(TKB30, 0.67) OVER () AS tkb30_67th_percentile
  FROM yearly_cohort_metrics
  LIMIT 1
),

cohort_segment AS (
  SELECT
    y.*,
    CASE
      WHEN y.TKB30 <= p.tkb30_33rd_percentile THEN 'Low TKB30'
      WHEN y.TKB30 <= p.tkb30_67th_percentile THEN 'Medium TKB30'
      ELSE 'High TKB30'
    END AS tkb30_category
  FROM yearly_cohort_metrics y
  CROSS JOIN percentiles p
)

SELECT *
FROM cohort_segment
ORDER BY cohort_year;
-- bener

-- 1) Ambil dulu hanya loan yang masih ada di portfolio
WITH active_loans AS (
  SELECT *
  FROM `fip_project.loan_customer_joined`
  WHERE 
    LOWER(loan_status) IN ('current', 'in grace period', 'default')
    OR LOWER(loan_status) LIKE 'late%'
),

-- 2) Hitung metric per cohort year
yearly_metrics AS (
  SELECT
    issue_year AS cohort_year,

    -- total OS per cohort
    SUM(funded_amount) AS cohort_OS,

    -- ENR per cohort (exclude default)
    SUM(
      CASE 
        WHEN LOWER(loan_status) = 'default' THEN 0
        ELSE funded_amount
      END
    ) AS cohort_ENR,

    -- OS dengan DPD > 30 (Late 31-120 + Default)
    SUM(
      CASE
        WHEN LOWER(loan_status) LIKE 'late (31-120 days)'
          OR LOWER(loan_status) = 'default'
        THEN funded_amount
        ELSE 0
      END
    ) AS OS_DPD_gt_30
  FROM active_loans
  GROUP BY cohort_year
),

-- 3) Tambah TKB30 ratio
yearly_with_tkb AS (
  SELECT
    cohort_year,
    cohort_OS,
    cohort_ENR,
    OS_DPD_gt_30,
    1 - (OS_DPD_gt_30 / cohort_OS) AS TKB30_ratio
  FROM yearly_metrics
),

-- 4) Hitung percentile untuk segmentasi
percentiles AS (
  SELECT
    PERCENTILE_CONT(TKB30_ratio, 0.33) OVER () AS tkb30_33rd_percentile,
    PERCENTILE_CONT(TKB30_ratio, 0.67) OVER () AS tkb30_67th_percentile
  FROM yearly_with_tkb
  LIMIT 1
),

-- 5) Label Low / Medium / High TKB30
cohort_segment AS (
  SELECT
    y.*,
    CASE
      WHEN y.TKB30_ratio <= p.tkb30_33rd_percentile THEN 'Low TKB30'
      WHEN y.TKB30_ratio <= p.tkb30_67th_percentile THEN 'Medium TKB30'
      ELSE 'High TKB30'
    END AS TKB30_segment
  FROM yearly_with_tkb y
  CROSS JOIN percentiles p
)

-- 6) Final output
SELECT
  cohort_year,
  cohort_OS,
  cohort_ENR,
  OS_DPD_gt_30,
  TKB30_ratio,
  ROUND(TKB30_ratio * 100, 2) AS TKB30_percent,
  TKB30_segment
FROM cohort_segment
ORDER BY cohort_year;
