WITH base AS (
  SELECT
    CASE
      WHEN CAST(issue_year AS INT64) IN (2012, 2013, 2014) THEN 'Low TKB30'
      WHEN CAST(issue_year AS INT64) IN (2015, 2017)       THEN 'Medium TKB30'
      WHEN CAST(issue_year AS INT64) IN (2016, 2018, 2019) THEN 'High TKB30'
      ELSE 'Other'
    END AS tkb30_segment,
    int_rate AS rate
  FROM `next-porto.fip.loan`
  WHERE int_rate IS NOT NULL
),

filtered AS (
  SELECT *
  FROM base
  WHERE tkb30_segment IN ('Low TKB30', 'Medium TKB30', 'High TKB30')
)

SELECT DISTINCT
  tkb30_segment,
  COUNT(*) OVER (PARTITION BY tkb30_segment)                             AS loan_count,
  ROUND(AVG(rate) OVER (PARTITION BY tkb30_segment), 2)                  AS avg_rate,
  ROUND(PERCENTILE_CONT(rate, 0.25) OVER (PARTITION BY tkb30_segment),2) AS q1_rate,
  ROUND(PERCENTILE_CONT(rate, 0.50) OVER (PARTITION BY tkb30_segment),2) AS median_rate,
  ROUND(PERCENTILE_CONT(rate, 0.75) OVER (PARTITION BY tkb30_segment),2) AS q3_rate
FROM filtered
ORDER BY tkb30_segment;

-- BENER
-- =========================================
-- 7(f) Interest Rate Comparison by TKB30 Segment
-- =========================================

WITH active_loans AS (
  SELECT *
  FROM `fip_project.loan_customer_joined`
  WHERE 
    LOWER(loan_status) IN ('current', 'in grace period', 'default')
    OR LOWER(loan_status) LIKE 'late%'
),

-- 1) Metrics per cohort year
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

-- 2) TKB30 calculation
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

-- 4) Assign segmentation
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

-- 5) Join segment with each loan & keep interest rate
loans_segment_rate AS (
  SELECT
    a.int_rate,
    s.tkb30_segment
  FROM active_loans a
  JOIN cohort_segment s
    ON a.issue_year = s.cohort_year
),

-- 6) Compute window-based stats
rate_stats AS (
  SELECT
    tkb30_segment,
    int_rate,

    AVG(int_rate) OVER (PARTITION BY tkb30_segment) AS avg_rate,
    PERCENTILE_CONT(int_rate, 0.25) OVER (PARTITION BY tkb30_segment) AS q1_rate,
    PERCENTILE_CONT(int_rate, 0.50) OVER (PARTITION BY tkb30_segment) AS median_rate,
    PERCENTILE_CONT(int_rate, 0.75) OVER (PARTITION BY tkb30_segment) AS q3_rate,

    MIN(int_rate) OVER (PARTITION BY tkb30_segment) AS min_rate,
    MAX(int_rate) OVER (PARTITION BY tkb30_segment) AS max_rate
  FROM loans_segment_rate
)

-- 7) Final aggregated output
SELECT
  tkb30_segment,
  COUNT(*) AS loan_count,
  AVG(avg_rate) AS avg_rate,
  AVG(q1_rate) AS q1_rate,
  AVG(median_rate) AS median_rate,
  AVG(q3_rate) AS q3_rate,
  AVG(min_rate) AS min_rate,
  AVG(max_rate) AS max_rate
FROM rate_stats
GROUP BY tkb30_segment
ORDER BY median_rate;
