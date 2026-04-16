-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 — Data Quality Checks
-- MAGIC Validates the Bronze layer data and produces a DQ scorecard.
-- MAGIC
-- MAGIC **Note:** Because Bronze stores all columns as strings, numeric checks
-- MAGIC use `CAST(... AS DOUBLE)` for proper comparison.

-- COMMAND ----------

-- DBTITLE 1,Run all quality checks
CREATE OR REPLACE TABLE workspace.sap_pm.dq_results AS

SELECT 'Duplicate Order Numbers' AS check_name,
  'Critical' AS severity,
  COUNT(*) - COUNT(DISTINCT Order_Number) AS failures,
  COUNT(*) AS total_records
FROM workspace.sap_pm.bronze_pm_orders

UNION ALL
SELECT 'Invalid Priority (not 1-4)', 'High',
  SUM(CASE WHEN Priority NOT IN ('1','2','3','4') THEN 1 ELSE 0 END),
  COUNT(*)
FROM workspace.sap_pm.bronze_pm_orders

UNION ALL
SELECT 'Missing Equipment ID', 'Medium',
  SUM(CASE WHEN Equipment_ID IS NULL OR Equipment_ID = '' THEN 1 ELSE 0 END),
  COUNT(*)
FROM workspace.sap_pm.bronze_pm_orders

UNION ALL
SELECT 'Start Date After End Date', 'High',
  SUM(CASE WHEN Planned_Start_Date > Planned_End_Date THEN 1 ELSE 0 END),
  COUNT(*)
FROM workspace.sap_pm.bronze_pm_orders

UNION ALL
SELECT 'Negative Actual Cost', 'High',
  SUM(CASE WHEN CAST(Actual_Cost_USD AS DOUBLE) < 0 THEN 1 ELSE 0 END),
  COUNT(*)
FROM workspace.sap_pm.bronze_pm_orders

UNION ALL
SELECT 'Missing Planned Cost', 'Medium',
  SUM(CASE WHEN Planned_Cost_USD IS NULL THEN 1 ELSE 0 END),
  COUNT(*)
FROM workspace.sap_pm.bronze_pm_orders

UNION ALL
SELECT 'Closed Without Actual Cost', 'Medium',
  SUM(CASE WHEN Status = 'CLSD' AND Actual_Cost_USD IS NULL THEN 1 ELSE 0 END),
  COUNT(*)
FROM workspace.sap_pm.bronze_pm_orders

UNION ALL
SELECT 'Completed Without Actual Dates', 'Medium',
  SUM(CASE WHEN Status IN ('CNF','TECO','CLSD')
    AND (Actual_Start_Date IS NULL OR Actual_Start_Date = '')
    THEN 1 ELSE 0 END),
  COUNT(*)
FROM workspace.sap_pm.bronze_pm_orders

-- COMMAND ----------

-- DBTITLE 1,View DQ scorecard
SELECT
  check_name, severity, failures, total_records,
  ROUND(failures / total_records * 100, 2) AS failure_pct,
  CASE
    WHEN failures = 0 THEN 'PASS'
    WHEN failures / total_records < 0.02 THEN 'WARNING'
    ELSE 'FAIL'
  END AS result
FROM workspace.sap_pm.dq_results
ORDER BY failures DESC

-- COMMAND ----------

-- DBTITLE 1,DQ breakdown by plant
SELECT
  Plant_Name,
  COUNT(*) AS total_records,
  SUM(CASE WHEN Priority NOT IN ('1','2','3','4') THEN 1 ELSE 0 END) AS invalid_priority,
  SUM(CASE WHEN Equipment_ID IS NULL OR Equipment_ID = '' THEN 1 ELSE 0 END) AS missing_equip,
  SUM(CASE WHEN Planned_Cost_USD IS NULL THEN 1 ELSE 0 END) AS missing_cost,
  SUM(CASE WHEN CAST(Actual_Cost_USD AS DOUBLE) < 0 THEN 1 ELSE 0 END) AS negative_cost,
  SUM(CASE WHEN Planned_Start_Date > Planned_End_Date THEN 1 ELSE 0 END) AS date_logic_error
FROM workspace.sap_pm.bronze_pm_orders
GROUP BY Plant_Name
ORDER BY Plant_Name
