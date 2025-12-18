WITH base AS (
  SELECT
    CAST(l.issue_year AS INT64) AS cohort_year,
    c.emp_length
  FROM `next-porto.fip.loan` l
  JOIN `next-porto.fip.customer` c
    ON l.customer_id = c.customer_id
  WHERE c.emp_length IS NOT NULL
),

tagged AS (
  SELECT
    CASE
      WHEN cohort_year IN (2012, 2013, 2014) THEN 'Low TKB30 (2012–2014)'
      ELSE 'Other Cohorts'
    END AS cohort_group,
    emp_length
  FROM base
)

SELECT
  cohort_group,
  emp_length,
  COUNT(*) AS total_loans,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_group), 4) AS percentage
FROM tagged
GROUP BY cohort_group, emp_length
ORDER BY cohort_group, percentage DESC;
--------------------------
WITH base AS (
  SELECT
    CAST(l.issue_year AS INT64) AS cohort_year,
    c.emp_length
  FROM `next-porto.fip.loan` l
  JOIN `next-porto.fip.customer` c
    ON l.customer_id = c.customer_id
  WHERE c.emp_length IS NOT NULL
),

tagged AS (
  SELECT
    CASE
      WHEN cohort_year IN (2012, 2013, 2014) THEN 'Low TKB30'
      WHEN cohort_year IN (2015, 2017)       THEN 'Medium TKB30'
      WHEN cohort_year IN (2016, 2018, 2019) THEN 'High TKB30'
      ELSE 'Other'
    END AS tkb30_segment,
    emp_length
  FROM base
)

SELECT
  tkb30_segment,
  emp_length,
  COUNT(*) AS total_loans,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY tkb30_segment), 4) AS percentage
FROM tagged
WHERE tkb30_segment IN ('Low TKB30', 'Medium TKB30', 'High TKB30')
GROUP BY tkb30_segment, emp_length
ORDER BY tkb30_segment, percentage DESC;

-- BENER

-- =========================================
-- 7(c) Employment Length Comparison by TKB30 Segment
-- =========================================

WITH active_loans AS (
  SELECT *
  FROM `fip_project.loan_customer_joined`
  WHERE 
    LOWER(loan_status) IN ('current', 'in grace period', 'default')
    OR LOWER(loan_status) LIKE 'late%'
),

-- 1) Metric per cohort
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

-- 2) TKB30
yearly_with_tkb AS (
  SELECT
    cohort_year,
    cohort_OS,
    cohort_ENR,
    OS_DPD_gt_30,
    1 - SAFE_DIVIDE(OS_DPD_gt_30, cohort_OS) AS TKB30_ratio
  FROM yearly_metrics
),

-- 3) Percentile thresholds
percentiles AS (
  SELECT
    PERCENTILE_CONT(TKB30_ratio, 0.33) OVER () AS p33,
    PERCENTILE_CONT(TKB30_ratio, 0.67) OVER () AS p67
  FROM yearly_with_tkb
  LIMIT 1
),

-- 4) Assign segment by TKB30
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

-- 5) Attach employment length to every loan
loans_segment_emp AS (
  SELECT
    a.emp_length,
    s.tkb30_segment
  FROM active_loans a
  JOIN cohort_segment s
    ON a.issue_year = s.cohort_year
)

-- 6) FINAL: Employment distribution per TKB segment
SELECT
  tkb30_segment,
  emp_length,
  COUNT(*) AS total_loans,
  SAFE_DIVIDE(
    COUNT(*),
    SUM(COUNT(*)) OVER (PARTITION BY tkb30_segment)
  ) AS percentage
FROM loans_segment_emp
GROUP BY tkb30_segment, emp_length
ORDER BY
  tkb30_segment,
  total_loans DESC;
