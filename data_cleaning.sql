-- 1) Clean customer table → SIMPAN KE TABEL BARU
CREATE OR REPLACE TABLE `fsda-sql-467016.fip_project.customer_cleaned` AS
SELECT
  ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(customer_id, r"[a-zA-Z0-9]"), "") AS customer_id_clean,
  * EXCEPT(customer_id),
  customer_id AS customer_id_raw
FROM `next-porto.fip.customer`;

-- 2) Clean loan table → SIMPAN KE TABEL BARU
CREATE OR REPLACE TABLE `fsda-sql-467016.fip_project.loan_cleaned` AS
SELECT
  ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(customer_id, r"[a-zA-Z0-9]"), "") AS customer_id_clean,
  * EXCEPT(customer_id),
  customer_id AS customer_id_raw
FROM `next-porto.fip.loan`;
