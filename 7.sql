WITH base AS (
  SELECT
    CAST(issue_year AS INT64) AS cohort_year,
    home_ownership,
    COUNT(*) AS loan_count
  FROM `next-porto.fip.loan` l
  JOIN `next-porto.fip.customer` c
    ON l.customer_id = c.customer_id
  WHERE home_ownership IS NOT NULL
  GROUP BY cohort_year, home_ownership
),

anomaly AS (
  SELECT *
  FROM base
  WHERE cohort_year IN (2012, 2013, 2014)     -- Low TKB30 cohorts
),

others AS (
  SELECT *
  FROM base
  WHERE cohort_year NOT IN (2012, 2013, 2014) -- Non-low cohorts
)

-- hasil akhirnya
SELECT 
  'Low TKB30 (2012–2014)' AS cohort_group,
  home_ownership,
  SUM(loan_count) AS total_loans,
  ROUND(
    SUM(loan_count) / SUM(SUM(loan_count)) OVER (), 
    4
  ) AS percentage
FROM anomaly
GROUP BY home_ownership

UNION ALL

SELECT
  'Other Cohorts' AS cohort_group,
  home_ownership,
  SUM(loan_count) AS total_loans,
  ROUND(
    SUM(loan_count) / SUM(SUM(loan_count)) OVER (), 
    4
  ) AS percentage
FROM others
GROUP BY home_ownership

ORDER BY cohort_group, percentage DESC;

-----------
WITH base AS (
  SELECT
    CAST(l.issue_year AS INT64) AS cohort_year,
    c.home_ownership
  FROM `fsda-sql-467016.fip_project.loan_cleaned` l
  JOIN `fsda-sql-467016.fip_project.customer_cleaned` c
    ON l.customer_id_clean = c.customer_id_clean
  WHERE c.home_ownership IS NOT NULL
),

tagged AS (
  SELECT
    CASE
      WHEN cohort_year IN (2012, 2013, 2014) THEN 'Low TKB30'
      WHEN cohort_year IN (2015, 2017)       THEN 'Medium TKB30'
      WHEN cohort_year IN (2016, 2018, 2019) THEN 'High TKB30'
      ELSE 'Other'
    END AS tkb30_segment,
    home_ownership
  FROM base
),

agg AS (
  SELECT
    tkb30_segment,
    home_ownership,
    COUNT(*) AS total_loans
  FROM tagged
  GROUP BY tkb30_segment, home_ownership
)

SELECT
  tkb30_segment,
  home_ownership,
  total_loans,
  ROUND(
    total_loans / SUM(total_loans) OVER (PARTITION BY tkb30_segment),
    4
  ) AS percentage
FROM agg
WHERE tkb30_segment IN ('Low TKB30', 'Medium TKB30', 'High TKB30')
ORDER BY tkb30_segment, percentage DESC;

-- bener
-- 7(a) Cohort TKB30 segmentation (using percentile) 
--      + Home ownership profile (loans & unique customers)

WITH active_loans AS (
  -- hanya loan yang masih ada di portfolio
  SELECT *
  FROM `fip_project.loan_customer_joined`
  WHERE 
    LOWER(loan_status) IN ('current', 'in grace period', 'default')
    OR LOWER(loan_status) LIKE 'late%'
),

-- 1) Metric per cohort year
yearly_metrics AS (
  SELECT
    issue_year AS cohort_year,
    SUM(funded_amount) AS cohort_OS,

    -- ENR exclude default
    SUM(
      CASE WHEN LOWER(loan_status) = 'default' THEN 0 
           ELSE funded_amount END
    ) AS cohort_ENR,

    -- DPD > 30: Late 31–120 + Default
    SUM(
      CASE WHEN LOWER(loan_status) LIKE 'late (31-120 days)'
             OR LOWER(loan_status) = 'default'
           THEN funded_amount ELSE 0 END
    ) AS OS_DPD_gt_30
  FROM active_loans
  GROUP BY cohort_year
),

-- 2) TKB30 ratio
yearly_with_tkb AS (
  SELECT
    cohort_year,
    cohort_OS,
    cohort_ENR,
    OS_DPD_gt_30,
    1 - SAFE_DIVIDE(OS_DPD_gt_30, cohort_OS) AS TKB30_ratio
  FROM yearly_metrics
),

-- 3) Percentiles (for segmentation only)
percentiles AS (
  SELECT
    PERCENTILE_CONT(TKB30_ratio, 0.33) OVER () AS p33,
    PERCENTILE_CONT(TKB30_ratio, 0.67) OVER () AS p67
  FROM yearly_with_tkb
  LIMIT 1
),

-- 4) Assign TKB30 Segment
cohort_segment AS (
  SELECT
    y.*,
    CASE
      WHEN y.TKB30_ratio <= p.p33 THEN 'Low TKB30'
      WHEN y.TKB30_ratio <= p.p67 THEN 'Medium TKB30'
      ELSE 'High TKB30'
    END AS tkb30_segment
  FROM yearly_with_tkb y
  CROSS JOIN percentiles p
),

-- 5) Attach home ownership to each segmented loan
loans_with_segment AS (
  SELECT
    a.customer_id_clean,
    a.home_ownership,
    s.tkb30_segment
  FROM active_loans a
  JOIN cohort_segment s
    ON a.issue_year = s.cohort_year
)

-- 6) Final profiling summary
SELECT
  tkb30_segment,
  home_ownership,
  COUNT(*) AS total_loans,
  SAFE_DIVIDE(
    COUNT(*),
    SUM(COUNT(*)) OVER (PARTITION BY tkb30_segment)
  ) AS percentage
FROM loans_with_segment
GROUP BY tkb30_segment, home_ownership
ORDER BY tkb30_segment, total_loans DESC;
