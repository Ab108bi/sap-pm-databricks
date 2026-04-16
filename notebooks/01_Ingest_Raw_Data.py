# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest Raw Data (Bronze Layer)
# MAGIC Reads all weekly SAP PM Excel files from a Unity Catalog Volume,
# MAGIC extracts metadata from filenames, and saves as a Bronze Delta table.
# MAGIC
# MAGIC **Note:** All Excel columns are read as strings (`dtype=str`) to avoid
# MAGIC schema-mismatch errors during the union of 39 files with slightly
# MAGIC different inferred types. Type casting happens in the Gold layer.

# COMMAND ----------

# DBTITLE 1,Install openpyxl (run this FIRST)
%pip install openpyxl
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuration
# ⚠️ UPDATE THIS to your actual volume path
VOLUME_PATH = "/Volumes/workspace/default/sap_pm_data"

CATALOG = "workspace"
SCHEMA = "sap_pm"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Schema: {CATALOG}.{SCHEMA}")
print(f"Volume: {VOLUME_PATH}")

# COMMAND ----------

# DBTITLE 1,List uploaded files
files = dbutils.fs.ls(VOLUME_PATH)
xlsx_files = [f for f in files if f.name.endswith(".xlsx")]

print(f"Found {len(xlsx_files)} Excel files:\n")
for f in xlsx_files[:6]:
    print(f"  {f.name}  ({round(f.size/1024, 1)} KB)")
if len(xlsx_files) > 6:
    print(f"  ... and {len(xlsx_files)-6} more")

# COMMAND ----------

# DBTITLE 1,Read all files and combine (strings-first approach)
import pandas as pd
import re
from pyspark.sql import functions as F
from functools import reduce

all_dfs = []
errors = []

for f in xlsx_files:
    try:
        local_path = f"{VOLUME_PATH}/{f.name}"
        # Read everything as strings to avoid type inference conflicts across files
        pdf = pd.read_excel(local_path, dtype=str)
        
        # Extract plant, month, year, week from filename
        match = re.match(
            r"(Plant[ABC])_([A-Za-z]{3})(\d{4})_Week(\d+)\.xlsx",
            f.name
        )
        if match:
            pdf["_source_file"] = f.name
            pdf["_file_plant"] = match.group(1)
            pdf["_file_month"] = match.group(2)
            pdf["_file_year"] = int(match.group(3))
            pdf["_file_week"] = int(match.group(4))
        
        sdf = spark.createDataFrame(pdf)
        all_dfs.append(sdf)
    except Exception as e:
        errors.append(f"{f.name}: {e}")

bronze_df = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    all_dfs
)
bronze_df = bronze_df.withColumn("_ingestion_ts", F.current_timestamp())

print(f"Read {len(all_dfs)} files")
if errors:
    print(f"Errors: {errors}")
print(f"Total records: {bronze_df.count()}")
print(f"Columns: {len(bronze_df.columns)}")

# COMMAND ----------

# DBTITLE 1,Save as Bronze table
bronze_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_pm_orders")

print("Bronze table saved!")
count = spark.table(f"{CATALOG}.{SCHEMA}.bronze_pm_orders").count()
print(f"Records: {count}")

display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_pm_orders").limit(10))
