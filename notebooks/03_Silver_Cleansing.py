# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver Layer Cleansing
# MAGIC Filters out bad records from Bronze and saves a clean Silver table.

# COMMAND ----------

# DBTITLE 1,Filter and deduplicate
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "sap_pm"

bronze = spark.table(f"{CATALOG}.{SCHEMA}.bronze_pm_orders")
print(f"Bronze: {bronze.count()} records")

silver = bronze.filter(
    (F.col("Priority").isin(['1','2','3','4'])) &
    (F.col("Order_Number").isNotNull()) &
    (F.col("Planned_Start_Date") <= F.col("Planned_End_Date")) &
    ((F.col("Actual_Cost_USD") >= 0) | (F.col("Actual_Cost_USD").isNull()))
)
silver = silver.dropDuplicates(["Order_Number"])

print(f"Silver: {silver.count()} records")
print(f"Quarantined: {bronze.count() - silver.count()} bad records")

# COMMAND ----------

# DBTITLE 1,Save Silver table
silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_pm_orders")

print("Silver table saved!")
display(spark.table(f"{CATALOG}.{SCHEMA}.silver_pm_orders").limit(5))
