WITH base AS (
  SELECT
    CAST(l.issue_year AS INT64) AS cohort_year,
    c.annual_inc
  FROM `next-porto.fip.loan` l
  JOIN `next-porto.fip.customer` c
    ON l.customer_id = c.customer_id
  WHERE c.annual_inc IS NOT NULL
),

tagged AS (
  SELECT
    CASE 
      WHEN cohort_year IN (2012, 2013, 2014) 
        THEN 'Low TKB30 (2012–2014)'
      ELSE 'Other Cohorts'
    END AS cohort_group,
    annual_inc
  FROM base
)

SELECT DISTINCT
  cohort_group,
  ROUND(AVG(annual_inc) OVER (PARTITION BY cohort_group), 2) AS avg_income,
  ROUND(PERCENTILE_CONT(annual_inc, 0.25) OVER (PARTITION BY cohort_group), 2) AS income_Q1,
  ROUND(PERCENTILE_CONT(annual_inc, 0.50) OVER (PARTITION BY cohort_group), 2) AS median_income,
  ROUND(PERCENTILE_CONT(annual_inc, 0.75) OVER (PARTITION BY cohort_group), 2) AS income_Q3
FROM tagged
ORDER BY cohort_group;

-------------------------
WITH base AS (
  SELECT
    CAST(l.issue_year AS INT64) AS cohort_year,
    c.annual_inc
  FROM `next-porto.fip.loan` l
  JOIN `next-porto.fip.customer` c
    ON l.customer_id = c.customer_id
  WHERE c.annual_inc IS NOT NULL
),

tagged AS (
  SELECT
    CASE
      WHEN cohort_year IN (2012, 2013, 2014) THEN 'Low TKB30'
      WHEN cohort_year IN (2015, 2017)       THEN 'Medium TKB30'
      WHEN cohort_year IN (2016, 2018, 2019) THEN 'High TKB30'
      ELSE 'Other'
    END AS tkb30_segment,
    annual_inc
  FROM base
)

SELECT DISTINCT
  tkb30_segment,
  COUNT(*) OVER (PARTITION BY tkb30_segment)                    AS loan_count,
  ROUND(AVG(annual_inc) OVER (PARTITION BY tkb30_segment), 2)   AS avg_income,
  ROUND(PERCENTILE_CONT(annual_inc, 0.25) OVER (PARTITION BY tkb30_segment), 2) AS income_Q1,
  ROUND(PERCENTILE_CONT(annual_inc, 0.50) OVER (PARTITION BY tkb30_segment), 2) AS median_income,
  ROUND(PERCENTILE_CONT(annual_inc, 0.75) OVER (PARTITION BY tkb30_segment), 2) AS income_Q3
FROM tagged
WHERE tkb30_segment IN ('Low TKB30', 'Medium TKB30', 'High TKB30')
ORDER BY tkb30_segment;
-- BENER

-- =========================================
-- 7(b) Annual Income Comparison by TKB30 Segment
-- (Fixed version: NO GROUP BY errors)
-- =========================================

WITH active_loans AS (
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
    SUM(CASE WHEN LOWER(loan_status) = 'default' 
             THEN 0 ELSE funded_amount END) AS cohort_ENR,
    SUM(CASE WHEN LOWER(loan_status) LIKE 'late (31-120 days)' 
               OR LOWER(loan_status) = 'default'
             THEN funded_amount ELSE 0 END) AS OS_DPD_gt_30
  FROM active_loans
  GROUP BY cohort_year
),

-- 2) Hit TKB30
yearly_with_tkb AS (
  SELECT
    cohort_year,
    cohort_OS,
    cohort_ENR,
    OS_DPD_gt_30,
    1 - SAFE_DIVIDE(OS_DPD_gt_30, cohort_OS) AS TKB30_ratio
  FROM yearly_metrics
),

-- 3) Percentile segmentation
percentiles AS (
  SELECT
    PERCENTILE_CONT(TKB30_ratio, 0.33) OVER () AS p33,
    PERCENTILE_CONT(TKB30_ratio, 0.67) OVER () AS p67
  FROM yearly_with_tkb
  LIMIT 1
),

-- 4) Assign Low / Medium / High
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

-- 5) Join segment to each loan, keep annual_inc
loans_segment_income AS (
  SELECT
    a.customer_id_clean,
    a.annual_inc,
    s.tkb30_segment
  FROM active_loans a
  JOIN cohort_segment s
    ON a.issue_year = s.cohort_year
),

-- 6) Compute stats using window functions inside segment
income_stats AS (
  SELECT
    tkb30_segment,
    annual_inc,

    -- window stats
    AVG(annual_inc) OVER (PARTITION BY tkb30_segment) AS avg_income,
    PERCENTILE_CONT(annual_inc, 0.25) OVER (PARTITION BY tkb30_segment) AS income_Q1,
    PERCENTILE_CONT(annual_inc, 0.50) OVER (PARTITION BY tkb30_segment) AS median_income,
    PERCENTILE_CONT(annual_inc, 0.75) OVER (PARTITION BY tkb30_segment) AS income_Q3,
    MIN(annual_inc) OVER (PARTITION BY tkb30_segment) AS min_income,
    MAX(annual_inc) OVER (PARTITION BY tkb30_segment) AS max_income
  FROM loans_segment_income
)

-- 7) Final clean output (unique per segment)
SELECT
  tkb30_segment,
  COUNT(*) AS loan_count,
  AVG(avg_income) AS avg_income,
  AVG(income_Q1) AS income_Q1,
  AVG(median_income) AS median_income,
  AVG(income_Q3) AS income_Q3,
  AVG(min_income) AS min_income,
  AVG(max_income) AS max_income
FROM income_stats
GROUP BY tkb30_segment
ORDER BY median_income;
