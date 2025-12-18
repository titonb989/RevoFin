WITH base AS (
  SELECT
    CAST(issue_year AS INT64) AS cohort_year,
    purpose,
    COUNT(*) AS loan_count
  FROM `next-porto.fip.loan`
  WHERE purpose IS NOT NULL
  GROUP BY cohort_year, purpose
),

-- Low TKB30 cohorts (2012–2014)
anomaly AS (
  SELECT *
  FROM base
  WHERE cohort_year IN (2012, 2013, 2014)
),

others AS (
  SELECT *
  FROM base
  WHERE cohort_year NOT IN (2012, 2013, 2014)
)

-- FINAL OUTPUT
SELECT 
  'Low TKB30 (2012–2014)' AS cohort_group,
  purpose,
  SUM(loan_count) AS total_loans,
  ROUND(SUM(loan_count) / SUM(SUM(loan_count)) OVER (), 4) AS pct
FROM anomaly
GROUP BY purpose

UNION ALL

SELECT
  'Other Cohorts' AS cohort_group,
  purpose,
  SUM(loan_count) AS total_loans,
  ROUND(SUM(loan_count) / SUM(SUM(loan_count)) OVER (), 4) AS pct
FROM others
GROUP BY purpose

ORDER BY cohort_group, pct DESC;
-----------------------------------
-- 1) Hitung TKB30 per cohort (2012–2019)
WITH yearly_metrics AS (
  SELECT
    CAST(issue_year AS INT64) AS cohort_year,
    SUM(loan_amount) AS cohort_OS,
    SUM(CASE WHEN loan_status IN ('Late (31-120 days)', 'Default') 
             THEN loan_amount ELSE 0 END) AS OS_DPD_gt_30,
    SAFE_DIVIDE(
      SUM(loan_amount) - SUM(CASE WHEN loan_status IN ('Late (31-120 days)', 'Default') 
                                  THEN loan_amount ELSE 0 END),
      SUM(loan_amount)
    ) AS TKB30
  FROM `next-porto.fip.loan`
  GROUP BY cohort_year
),

-- 2) Hitung percentile untuk segmentasi
percentiles AS (
  SELECT
    PERCENTILE_CONT(TKB30, 0.33) OVER () AS p33,
    PERCENTILE_CONT(TKB30, 0.67) OVER () AS p67
  FROM yearly_metrics
  LIMIT 1
),

-- 3) Tambahkan segmentasi ke cohort
cohort_segment AS (
  SELECT
    y.*,
    CASE 
      WHEN y.TKB30 <= p.p33 THEN 'Low TKB30'
      WHEN y.TKB30 <= p.p67 THEN 'Medium TKB30'
      ELSE 'High TKB30'
    END AS tkb30_segment
  FROM yearly_metrics y
  CROSS JOIN percentiles p
),

-- 4) Join loan → cohort → purpose
loan_with_segment AS (
  SELECT
    l.loan_id,
    l.purpose,
    cs.tkb30_segment
  FROM `next-porto.fip.loan` l
  JOIN cohort_segment cs
    ON l.issue_year = cs.cohort_year
  WHERE l.purpose IS NOT NULL
)

-- 5) FINAL: Distribution loan purpose by TKB30 segment
SELECT
  tkb30_segment,
  purpose,
  COUNT(*) AS total_loans,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY tkb30_segment), 4) AS pct
FROM loan_with_segment
GROUP BY tkb30_segment, purpose
ORDER BY tkb30_segment, pct DESC;

-- BENER
-- =========================================
-- 7(e) Loan Purpose Concentration by TKB30 Segment
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
    SUM(
      CASE WHEN LOWER(loan_status) = 'default'
           THEN 0 ELSE funded_amount END
    ) AS cohort_ENR,
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

-- 3) Percentile thresholds
percentiles AS (
  SELECT
    PERCENTILE_CONT(TKB30_ratio, 0.33) OVER () AS p33,
    PERCENTILE_CONT(TKB30_ratio, 0.67) OVER () AS p67
  FROM yearly_with_tkb
  LIMIT 1
),

-- 4) Assign TKB30 segment
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

-- 5) Attach loan purpose to every loan with segment info
loans_segment_purpose AS (
  SELECT
    a.purpose,
    s.tkb30_segment
  FROM active_loans a
  JOIN cohort_segment s
    ON a.issue_year = s.cohort_year
)

-- 6) Final: purpose distribution per TKB30 segment
SELECT
  tkb30_segment,
  purpose,
  COUNT(*) AS total_loans,
  SAFE_DIVIDE(
    COUNT(*),
    SUM(COUNT(*)) OVER (PARTITION BY tkb30_segment)
  ) AS percentage
FROM loans_segment_purpose
GROUP BY tkb30_segment, purpose
ORDER BY
  tkb30_segment,
  total_loans DESC;
