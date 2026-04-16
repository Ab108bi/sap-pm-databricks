# SAP Plant Maintenance Analytics — Databricks Free Edition

An end-to-end data analytics project built on **Databricks Free Edition** that processes mock SAP Planned Maintenance (PM) data from 3 manufacturing plants through a Medallion Architecture pipeline (Bronze → Silver → Gold), with data quality validation, KPI computation, interactive dashboards, and Genie-powered natural language analytics.

![Databricks](https://img.shields.io/badge/Databricks-Free_Edition-FF3621?logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?logo=delta&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-00A98F)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/Spark_SQL-Serverless-4479A1)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  LOCAL / SAP SOURCE                                                 │
│  39 Excel files: PlantA_Jan2025_Week1.xlsx ...                      │
│  3 Plants × 13 Weeks × ~15 records each                            │
└────────────────────┬────────────────────────────────────────────────┘
                     │ Data Ingestion (Upload to Volume)
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  UNITY CATALOG VOLUME                                               │
│  /Volumes/workspace/default/sap_pm_data/                            │
└────────────────────┬────────────────────────────────────────────────┘
                     │ Notebook 01: Ingest
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER          workspace.sap_pm.bronze_pm_orders            │
│  Raw data, as-is from Excel + filename metadata                     │
└────────────────────┬────────────────────────────────────────────────┘
                     │ Notebook 02: DQ Checks → dq_results table
                     │ Notebook 03: Filter & Deduplicate
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER          workspace.sap_pm.silver_pm_orders            │
│  Cleaned, validated, deduplicated                                   │
└────────────────────┬────────────────────────────────────────────────┘
                     │ Notebook 04: Aggregate & Compute KPIs
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER (5 tables)                                              │
│                                                                     │
│  gold_orders_fact ─── Order-level fact with all dimensions + metrics│
│  gold_plant_metrics ─ Plant scorecard (compliance, MTTR, PM:CM)     │
│  gold_monthly_trends  Monthly aggregates for time-series charts     │
│  gold_failure_analysis Damage codes, causes, repair cost/duration   │
│  gold_equipment_summary Equipment hot spots & cost drivers          │
└────────────────────┬────────────────────────────────────────────────┘
                     │
              ┌──────┴──────┐
              ▼             ▼
     ┌──────────────┐ ┌──────────────┐
     │  Dashboard   │ │    Genie     │
     │  15 tiles    │ │  Natural     │
     │  6 filters   │ │  Language    │
     │  KPIs, trends│ │  Q&A over    │
     │  failures    │ │  Gold tables │
     └──────────────┘ └──────────────┘
```

---

## Project Structure

```
sap-pm-databricks/
├── README.md
├── LICENSE
├── .gitignore
├── data/
│   └── weekly_files/          # 39 mock SAP PM Excel files
│       ├── PlantA_Jan2025_Week1.xlsx
│       ├── PlantA_Jan2025_Week2.xlsx
│       ├── ...
│       └── PlantC_Mar2025_Week5.xlsx
├── notebooks/
│   ├── 01_Ingest_Raw_Data.py       # Bronze layer ingestion
│   ├── 02_Data_Quality_Checks.sql  # 8 DQ validation rules
│   ├── 03_Silver_Cleansing.py      # Filter, deduplicate → Silver
│   └── 04_Gold_Metrics.sql         # 5 Gold tables with KPIs
├── scripts/
│   └── upload_to_databricks.py     # Local → DBFS upload automation
└── docs/
    └── genie_setup.md              # Genie Space configuration guide
```

---

## Data Overview

### 3 Plants with Distinct Profiles

| Plant | Code | Location | Equipment Age | Behavior |
|-------|------|----------|---------------|----------|
| A | 1000 | Houston | Mixed | Balanced maintenance mix |
| B | 2000 | Chicago | Newer | Heavy preventive maintenance |
| C | 3000 | Detroit | Aging | High corrective & emergency orders, more cost overruns |

### File Naming Convention
```
Plant{A|B|C}_{Mon}{Year}_Week{N}.xlsx
Example: PlantA_Jan2025_Week1.xlsx
```

### 30 SAP PM-style Columns
`Order_Number`, `Order_Type`, `Order_Type_Desc`, `Plant`, `Plant_Name`, `Functional_Location`, `Equipment_ID`, `Priority`, `Priority_Desc`, `Maintenance_Plan`, `Activity_Type`, `Activity_Type_Desc`, `Status`, `Status_Desc`, `Work_Center`, `Planner_Group`, `Cost_Center`, `Created_Date`, `Planned_Start_Date`, `Planned_End_Date`, `Actual_Start_Date`, `Actual_End_Date`, `Planned_Cost_USD`, `Actual_Cost_USD`, `Planned_Labor_Hours`, `Actual_Labor_Hours`, `Damage_Code`, `Cause_Code`, `Breakdown_Indicator`, `Notification_Number`

### Embedded Data Quality Issues (Intentional)
- Duplicate `Order_Number` entries
- Invalid priority codes (value `5` outside range 1-4)
- `Planned_Start_Date` after `Planned_End_Date`
- Negative `Actual_Cost_USD` values
- Missing `Planned_Cost_USD` and `Equipment_ID` fields

---

## Gold Layer Tables

### `gold_orders_fact` — Dashboard-ready fact table
Every order with all filterable dimensions and pre-computed metrics:
- **Dimensions:** Plant, Date (year_month, week), Order Type, Priority, Status, Equipment, Damage Code
- **Computed fields:** `maintenance_category` (Preventive/Corrective), `schedule_status` (On Time/Late/In Progress), `budget_status` (Under Budget/Within 10%/Over Budget), `cost_variance_pct`, `labor_efficiency_pct`, `actual_duration_hours`

### `gold_plant_metrics` — Plant KPI scorecard
| Metric | Description |
|--------|-------------|
| `schedule_compliance_pct` | % orders completed on or before planned end date |
| `pm_cm_ratio` | Preventive to corrective ratio (>2.0 = world-class) |
| `mttr_hours` | Mean Time To Repair for corrective orders |
| `breakdown_rate_pct` | % of orders that were emergency breakdowns |
| `avg_cost_variance_pct` | Avg budget overrun/underrun percentage |
| `backlog_count` | Open work orders (Created or Released status) |
| `p90_duration_hours` | 90th percentile repair duration |

### `gold_monthly_trends` — Time-series metrics
Order count, cost, breakdowns, compliance by plant × month × maintenance category.

### `gold_failure_analysis` — Reliability patterns
Damage codes, root causes, average and P90 repair cost/hours for corrective orders.

### `gold_equipment_summary` — Equipment hot spots
Total orders, breakdowns, cost, and failure mode diversity per equipment ID.

---

## Dashboard Layout

15 visualization tiles with 6 interactive filters:

**Filters:** Plant Name, Year-Month, Maintenance Category, Order Type, Priority, Status

| Row | Tiles | Source Table |
|-----|-------|-------------|
| 1 | KPI Counters: Total Orders, Total Cost, Avg MTTR, Schedule Compliance % | `gold_plant_metrics` |
| 2 | Plant Scorecard (table) + PM:CM Ratio (bar) | `gold_plant_metrics` |
| 3 | Monthly Cost Trend (line) + Order Volume by Category (stacked bar) | `gold_monthly_trends` |
| 4 | Order Type Mix (stacked bar) + Planned vs Actual Cost (grouped bar) | `gold_orders_fact` / `gold_plant_metrics` |
| 5 | Top Failure Modes (horizontal bar) + Costliest Equipment (table) | `gold_failure_analysis` / `gold_equipment_summary` |
| 6 | Budget Status (pie) + Compliance Trend (line) + PM vs CM Cost (bar) | `gold_orders_fact` / `gold_monthly_trends` / `gold_plant_metrics` |

---

## Getting Started

### Prerequisites
- Databricks Free Edition account ([sign up here](https://www.databricks.com/try-databricks))
- Serverless Starter Warehouse (included with Free Edition)

### Step-by-step

1. **Upload data** — Use Data Ingestion in Databricks to upload all 39 Excel files from `data/weekly_files/` to a Unity Catalog Volume.

2. **Create schema** — In SQL Editor, run:
   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.sap_pm;
   ```

3. **Import notebooks** — Upload the 4 notebooks from `notebooks/` into a Workspace folder.

4. **Update the volume path** — In `01_Ingest_Raw_Data.py`, update `VOLUME_PATH` to your actual volume path (find it in Catalog browser).

5. **Run notebooks in order:**
   ```
   01_Ingest_Raw_Data.py       → Creates Bronze table
   02_Data_Quality_Checks.sql  → Creates DQ results table
   03_Silver_Cleansing.py      → Creates Silver table
   04_Gold_Metrics.sql         → Creates 5 Gold tables
   ```

6. **Build dashboard** — In Dashboards, create a new dashboard, add the Gold tables as datasets, and add visualization tiles (see layout above).

7. **Set up Genie** — Create a Genie Space with all 5 Gold tables as trusted assets. Add table/column descriptions from `docs/genie_setup.md`.

---

## Genie Space Configuration

See [`docs/genie_setup.md`](docs/genie_setup.md) for complete table descriptions, column descriptions, and sample questions to configure your Genie Space for optimal natural language accuracy.

---

## Weekly Refresh Process

```
1. Upload new weekly files to your Volume via Data Ingestion
2. Re-run notebooks 01 → 02 → 03 → 04
3. Dashboard and Genie auto-refresh (live queries against Gold tables)
```

---

## Technologies Used

| Technology | Purpose |
|-----------|---------|
| **Databricks Free Edition** | Platform (serverless compute, Unity Catalog) |
| **Delta Lake** | Table storage format (ACID, time travel) |
| **Unity Catalog** | Data governance, schema management |
| **Spark SQL** | Data transformations and aggregations |
| **PySpark + pandas** | Excel ingestion and data processing |
| **Databricks SQL Dashboard** | Interactive visualizations with filters |
| **Genie** | Natural language analytics over Gold tables |
| **Medallion Architecture** | Bronze → Silver → Gold data organization |

---

## License

This project is for educational and portfolio purposes. The mock data is synthetically generated and does not represent any real organization.
