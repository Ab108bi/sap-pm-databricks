-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 — Gold Layer Metrics
-- MAGIC Builds 5 Gold tables: order-level fact table, plant KPIs,
-- MAGIC monthly trends, failure analysis, and equipment summary.

-- COMMAND ----------

-- DBTITLE 1,Dashboard-ready order-level fact table
CREATE OR REPLACE TABLE workspace.sap_pm.gold_orders_fact AS
SELECT
  Plant, Plant_Name, Order_Number, Order_Type, Order_Type_Desc,
  CASE 
    WHEN Order_Type IN ('PM01','PM03','PM04') THEN 'Preventive'
    ELSE 'Corrective'
  END AS maintenance_category,
  Priority, Priority_Desc, Status, Status_Desc,
  Activity_Type, Activity_Type_Desc, Work_Center,
  Functional_Location, Equipment_ID, Damage_Code, Cause_Code,
  Breakdown_Indicator,

  -- Date dimensions
  CAST(Created_Date AS DATE) AS created_date,
  DATE_FORMAT(Created_Date, 'yyyy-MM') AS year_month,
  DATE_FORMAT(Created_Date, 'yyyy') AS year,
  DATE_FORMAT(Created_Date, 'MMMM') AS month_name,
  WEEKOFYEAR(Created_Date) AS week_of_year,
  _file_plant, _file_month, _file_year, _file_week,

  Planned_Start_Date, Planned_End_Date,
  Actual_Start_Date, Actual_End_Date,

  -- Cost metrics
  Planned_Cost_USD, Actual_Cost_USD,
  ROUND(Actual_Cost_USD - Planned_Cost_USD, 2) AS cost_variance_usd,
  ROUND(
    CASE WHEN Planned_Cost_USD > 0
    THEN (Actual_Cost_USD - Planned_Cost_USD) / Planned_Cost_USD * 100 END, 1
  ) AS cost_variance_pct,

  -- Labor metrics
  Planned_Labor_Hours, Actual_Labor_Hours,
  ROUND(Actual_Labor_Hours - Planned_Labor_Hours, 1) AS labor_variance_hours,
  ROUND(
    CASE WHEN Planned_Labor_Hours > 0
    THEN Planned_Labor_Hours / NULLIF(Actual_Labor_Hours, 0) * 100 END, 1
  ) AS labor_efficiency_pct,

  -- Duration
  ROUND(
    CASE WHEN Actual_Start_Date IS NOT NULL AND Actual_Start_Date != ''
      AND Actual_End_Date IS NOT NULL AND Actual_End_Date != ''
    THEN (UNIX_TIMESTAMP(Actual_End_Date) - UNIX_TIMESTAMP(Actual_Start_Date)) / 3600 END, 1
  ) AS actual_duration_hours,
  ROUND(
    (UNIX_TIMESTAMP(Planned_End_Date) - UNIX_TIMESTAMP(Planned_Start_Date)) / 3600, 1
  ) AS planned_duration_hours,

  -- Flags
  CASE
    WHEN Actual_End_Date IS NOT NULL AND Actual_End_Date != ''
      AND Actual_End_Date <= Planned_End_Date THEN 'On Time'
    WHEN Actual_End_Date IS NOT NULL AND Actual_End_Date != ''
      AND Actual_End_Date > Planned_End_Date THEN 'Late'
    WHEN Status IN ('CRTD','REL','PCNF') THEN 'In Progress'
    ELSE 'Unknown'
  END AS schedule_status,
  CASE
    WHEN Actual_Cost_USD IS NULL THEN 'No Actual'
    WHEN Actual_Cost_USD <= Planned_Cost_USD THEN 'Under Budget'
    WHEN Actual_Cost_USD <= Planned_Cost_USD * 1.1 THEN 'Within 10%'
    ELSE 'Over Budget'
  END AS budget_status,
  CASE WHEN Status IN ('CRTD','REL') THEN 1 ELSE 0 END AS is_backlog,
  CASE WHEN Breakdown_Indicator = 'X' THEN 1 ELSE 0 END AS is_breakdown

FROM workspace.sap_pm.silver_pm_orders;

SELECT COUNT(*) AS total_rows FROM workspace.sap_pm.gold_orders_fact;

-- COMMAND ----------

-- DBTITLE 1,Plant KPI scorecard
CREATE OR REPLACE TABLE workspace.sap_pm.gold_plant_metrics AS
SELECT
  Plant, Plant_Name,
  COUNT(*) AS total_orders,
  SUM(CASE WHEN maintenance_category = 'Preventive' THEN 1 ELSE 0 END) AS preventive_orders,
  SUM(CASE WHEN maintenance_category = 'Corrective' THEN 1 ELSE 0 END) AS corrective_orders,
  SUM(is_backlog) AS backlog_count,
  SUM(is_breakdown) AS breakdown_count,

  ROUND(SUM(CASE WHEN maintenance_category = 'Preventive' THEN 1.0 ELSE 0 END)
    / NULLIF(SUM(CASE WHEN maintenance_category = 'Corrective' THEN 1.0 ELSE 0 END), 0), 2) AS pm_cm_ratio,
  ROUND(SUM(is_breakdown) * 100.0 / COUNT(*), 1) AS breakdown_rate_pct,

  ROUND(SUM(CASE WHEN schedule_status = 'On Time' THEN 1 ELSE 0 END) * 100.0
    / NULLIF(SUM(CASE WHEN schedule_status IN ('On Time','Late') THEN 1 ELSE 0 END), 0), 1) AS schedule_compliance_pct,

  ROUND(SUM(Actual_Cost_USD), 0) AS total_actual_cost,
  ROUND(SUM(Planned_Cost_USD), 0) AS total_planned_cost,
  ROUND(AVG(cost_variance_pct), 1) AS avg_cost_variance_pct,
  ROUND(SUM(CASE WHEN budget_status = 'Over Budget' THEN 1 ELSE 0 END) * 100.0
    / NULLIF(SUM(CASE WHEN Actual_Cost_USD IS NOT NULL THEN 1 ELSE 0 END), 0), 1) AS pct_orders_over_budget,

  ROUND(AVG(CASE WHEN maintenance_category = 'Corrective' THEN actual_duration_hours END), 1) AS mttr_hours,
  ROUND(AVG(actual_duration_hours), 1) AS avg_duration_hours,
  ROUND(PERCENTILE_APPROX(actual_duration_hours, 0.5), 1) AS median_duration_hours,
  ROUND(PERCENTILE_APPROX(actual_duration_hours, 0.9), 1) AS p90_duration_hours,

  ROUND(AVG(labor_efficiency_pct), 1) AS avg_labor_efficiency_pct,
  ROUND(SUM(Actual_Labor_Hours), 0) AS total_actual_labor_hours,

  ROUND(AVG(CASE WHEN maintenance_category = 'Preventive' THEN Actual_Cost_USD END), 0) AS avg_cost_preventive,
  ROUND(AVG(CASE WHEN maintenance_category = 'Corrective' THEN Actual_Cost_USD END), 0) AS avg_cost_corrective

FROM workspace.sap_pm.gold_orders_fact
GROUP BY Plant, Plant_Name;

SELECT * FROM workspace.sap_pm.gold_plant_metrics;

-- COMMAND ----------

-- DBTITLE 1,Monthly trend metrics
CREATE OR REPLACE TABLE workspace.sap_pm.gold_monthly_trends AS
SELECT
  Plant, Plant_Name, year_month, maintenance_category, Order_Type_Desc,
  COUNT(*) AS order_count,
  SUM(is_breakdown) AS breakdowns,
  SUM(is_backlog) AS backlog,
  ROUND(SUM(Actual_Cost_USD), 0) AS total_cost,
  ROUND(AVG(Actual_Cost_USD), 0) AS avg_cost,
  ROUND(AVG(actual_duration_hours), 1) AS avg_duration_hours,
  ROUND(AVG(Actual_Labor_Hours), 1) AS avg_labor_hours,
  ROUND(AVG(cost_variance_pct), 1) AS avg_cost_variance_pct,
  ROUND(SUM(CASE WHEN schedule_status = 'On Time' THEN 1 ELSE 0 END) * 100.0
    / NULLIF(SUM(CASE WHEN schedule_status IN ('On Time','Late') THEN 1 ELSE 0 END), 0), 1) AS schedule_compliance_pct
FROM workspace.sap_pm.gold_orders_fact
GROUP BY Plant, Plant_Name, year_month, maintenance_category, Order_Type_Desc
ORDER BY year_month, Plant;

SELECT * FROM workspace.sap_pm.gold_monthly_trends;

-- COMMAND ----------

-- DBTITLE 1,Failure & reliability analysis
CREATE OR REPLACE TABLE workspace.sap_pm.gold_failure_analysis AS
SELECT
  Plant, Plant_Name, Damage_Code, Cause_Code,
  COUNT(*) AS occurrence_count,
  ROUND(AVG(Actual_Cost_USD), 0) AS avg_repair_cost,
  ROUND(SUM(Actual_Cost_USD), 0) AS total_repair_cost,
  ROUND(AVG(actual_duration_hours), 1) AS avg_repair_hours,
  ROUND(PERCENTILE_APPROX(actual_duration_hours, 0.9), 1) AS p90_repair_hours,
  ROUND(AVG(Actual_Labor_Hours), 1) AS avg_labor_hours
FROM workspace.sap_pm.gold_orders_fact
WHERE maintenance_category = 'Corrective' AND Damage_Code != 'NONE'
GROUP BY Plant, Plant_Name, Damage_Code, Cause_Code
ORDER BY occurrence_count DESC;

SELECT * FROM workspace.sap_pm.gold_failure_analysis;

-- COMMAND ----------

-- DBTITLE 1,Equipment hot spots
CREATE OR REPLACE TABLE workspace.sap_pm.gold_equipment_summary AS
SELECT
  Plant, Plant_Name, Equipment_ID, Functional_Location,
  COUNT(*) AS total_orders,
  SUM(is_breakdown) AS breakdowns,
  SUM(CASE WHEN maintenance_category = 'Corrective' THEN 1 ELSE 0 END) AS corrective_orders,
  ROUND(SUM(Actual_Cost_USD), 0) AS total_cost,
  ROUND(AVG(actual_duration_hours), 1) AS avg_repair_hours,
  COUNT(DISTINCT Damage_Code) AS unique_failure_modes
FROM workspace.sap_pm.gold_orders_fact
WHERE Equipment_ID IS NOT NULL AND Equipment_ID != ''
GROUP BY Plant, Plant_Name, Equipment_ID, Functional_Location
ORDER BY total_cost DESC;

SELECT * FROM workspace.sap_pm.gold_equipment_summary LIMIT 20;
