CREATE OR REPLACE TABLE `fsda-sql-467016.fip_project.loan_customer_joined` AS
SELECT
    -- Loan fields
    l.loan_id,
    l.customer_id_clean,
    l.loan_status,
    l.loan_amount,
    l.funded_amount,
    l.state,
    l.term,
    l.int_rate,
    l.installment,
    l.grade,
    l.issue_d,
    l.issue_date,
    l.issue_year,
    l.pymnt_plan,
    l.type,
    l.purpose,
    l.description,
    l.notes,

    -- Customer fields
    c.emp_title,
    c.emp_length,
    c.home_ownership,
    c.annual_inc,
    c.annual_inc_joint,
    c.verification_status,
    c.zip_code,
    c.addr_state,
    c.avg_cur_bal,
    c.tot_cur_bal,

FROM `fip_project.loan_cleaned` l
LEFT JOIN `fip_project.customer_cleaned` c
    ON l.customer_id_clean = c.customer_id_clean;

---------------------------------
CREATE OR REPLACE TABLE `fsda-sql-467016.fip_project.loan_portfolio_active` AS
SELECT *
FROM `fsda-sql-467016.fip_project.loan_customer_joined`
WHERE 
  LOWER(loan_status) IN ('current', 'in grace period', 'default')
  OR LOWER(loan_status) LIKE 'late%';
