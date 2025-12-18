WITH base AS (
  SELECT
    CAST(l.issue_year AS INT64) AS cohort_year,
    r.region,
    COUNT(*) AS loan_count
  FROM `next-porto.fip.loan` l
  JOIN `next-porto.fip.loan_with_region` r
    ON l.loan_id = r.loan_id
  WHERE r.region IS NOT NULL
  GROUP BY cohort_year, region
),

anomaly AS (
  SELECT *
  FROM base
  WHERE cohort_year IN (2012, 2013, 2014)     -- Low TKB30 cohorts
),

others AS (
  SELECT *
  FROM base
  WHERE cohort_year NOT IN (2012, 2013, 2014)
)

-- Final Output
SELECT
  'Low TKB30 (2012–2014)' AS cohort_group,
  region,
  SUM(loan_count) AS total_loans,
  ROUND(SUM(loan_count) / SUM(SUM(loan_count)) OVER (), 4) AS pct
FROM anomaly
GROUP BY region

UNION ALL

SELECT
  'Other Cohorts' AS cohort_group,
  region,
  SUM(loan_count) AS total_loans,
  ROUND(SUM(loan_count) / SUM(SUM(loan_count)) OVER (), 4) AS pct
FROM others
GROUP BY region

ORDER BY cohort_group, pct DESC;

--------------------------------

-- 7(d) - Region profile by High / Medium / Low TKB30 (no permanent table)
WITH yearly_cohort_metrics AS (
  SELECT
    CAST(issue_year AS INT64) AS cohort_year,
    SUM(funded_amount) AS cohort_OS,
    SUM(
      CASE 
        WHEN loan_status = "Late (31-120 days)" THEN funded_amount 
        ELSE 0 
      END
    ) AS OS_DPD_gt_30,
    -- TKB30 = 1 - (OS DPD > 30) / OS
    1 - SAFE_DIVIDE(
          SUM(CASE WHEN loan_status = "Late (31-120 days)" THEN funded_amount ELSE 0 END),
          SUM(funded_amount)
        ) AS TKB30
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

percentiles AS (
  SELECT
    PERCENTILE_CONT(TKB30, 0.33) OVER () AS tkb30_p33,
    PERCENTILE_CONT(TKB30, 0.67) OVER () AS tkb30_p67
  FROM yearly_cohort_metrics
  LIMIT 1
),

cohort_with_segment AS (
  SELECT
    y.cohort_year,
    y.TKB30,
    CASE
      WHEN y.TKB30 <= p.tkb30_p33 THEN 'Low TKB30'
      WHEN y.TKB30 <= p.tkb30_p67 THEN 'Medium TKB30'
      ELSE 'High TKB30'
    END AS tkb30_segment
  FROM yearly_cohort_metrics y
  CROSS JOIN percentiles p
),

loans_with_segment_region AS (
  SELECT
    l.loan_id,
    seg.tkb30_segment,
    r.region
  FROM `next-porto.fip.loan` AS l
  JOIN cohort_with_segment AS seg
    ON CAST(l.issue_year AS INT64) = seg.cohort_year
  JOIN `next-porto.fip.loan_with_region` AS r
    ON l.loan_id = r.loan_id
  WHERE r.region IS NOT NULL
)

SELECT
  tkb30_segment,
  region,
  COUNT(*) AS total_loans,
  ROUND(
    COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY tkb30_segment),
    4
  ) AS percentage
FROM loans_with_segment_region
GROUP BY tkb30_segment, region
ORDER BY tkb30_segment, percentage DESC;

---BENER
-- =========================================
-- 7(d) State Concentration by TKB30 Segment
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

-- 3) Percentile thresholds for segmentation
percentiles AS (
  SELECT
    PERCENTILE_CONT(TKB30_ratio, 0.33) OVER () AS p33,
    PERCENTILE_CONT(TKB30_ratio, 0.67) OVER () AS p67
  FROM yearly_with_tkb
  LIMIT 1
),

-- 4) Assign Low / Medium / High TKB30
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

-- 5) Attach state to every loan with segment info
loans_segment_state AS (
  SELECT
    a.addr_state,
    s.tkb30_segment
  FROM active_loans a
  JOIN cohort_segment s
    ON a.issue_year = s.cohort_year
)

-- 6) Final state concentration per TKB30 segment
SELECT
  tkb30_segment,
  addr_state,
  COUNT(*) AS total_loans,
  SAFE_DIVIDE(
    COUNT(*),
    SUM(COUNT(*)) OVER (PARTITION BY tkb30_segment)
  ) AS percentage
FROM loans_segment_state
GROUP BY tkb30_segment, addr_state
ORDER BY
  tkb30_segment,
  total_loans DESC;
