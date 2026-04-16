# Genie Space Setup Guide

## Creating the Genie Space

1. Click **Genie** in the left sidebar
2. Click **New** to create a Genie Space
3. Name it: **SAP PM Maintenance Assistant**
4. Select: **Serverless Starter Warehouse**
5. Add these 5 trusted tables from `workspace.sap_pm`:

## Table Descriptions

Add these descriptions in the Genie Space settings (click each table):

**gold_orders_fact:**
> Order-level fact table with every maintenance work order. Contains all filterable dimensions (Plant, Date, Order Type, Priority, Status, Equipment) and pre-computed metrics (cost variance, labor efficiency, duration, schedule status, budget status). Use this as the primary table for detailed analysis and drill-downs.

**gold_plant_metrics:**
> Plant-level KPI scorecard. One row per plant with schedule compliance, MTTR, PM:CM ratio, breakdown rate, backlog, cost totals, duration percentiles, and labor efficiency.

**gold_monthly_trends:**
> Monthly aggregated metrics by plant, maintenance category, and order type. Use for time-series trends of cost, volume, compliance, and breakdowns.

**gold_failure_analysis:**
> Failure patterns for corrective orders. Damage codes, root causes, average and P90 repair cost/hours. Use for reliability and root cause analysis.

**gold_equipment_summary:**
> Equipment-level summary showing total orders, breakdowns, cost, and failure modes per equipment ID. Use to identify problem equipment.

## Column Descriptions

Add these for key calculated fields (click table → click column):

| Column | Description |
|--------|-------------|
| `maintenance_category` | Either 'Preventive' (PM01, PM03, PM04) or 'Corrective' (PM02, PM05). Use for PM vs CM analysis. |
| `pm_cm_ratio` | Ratio of preventive to corrective orders. >2.0 = world-class, <1.0 = mostly reactive. |
| `schedule_status` | 'On Time' if completed by planned date, 'Late' if after, 'In Progress' if still open. |
| `budget_status` | 'Under Budget', 'Within 10%', or 'Over Budget' based on actual vs planned cost. |
| `cost_variance_pct` | Percentage difference between actual and planned cost. Positive = over budget. |
| `mttr_hours` | Mean Time To Repair in hours, for corrective and emergency orders only. |
| `p90_duration_hours` | 90th percentile repair duration — the worst 10% take longer than this. |
| `is_breakdown` | 1 if emergency breakdown, 0 otherwise. Sum for breakdown count. |
| `is_backlog` | 1 if order status is Created or Released (open work). Sum for backlog count. |
| `schedule_compliance_pct` | % of orders completed on/before planned end date. Target >90%. |
| `breakdown_rate_pct` | % of all orders that were emergency breakdowns. Lower = better. |
| `avg_cost_variance_pct` | Average % difference between actual and planned cost. Positive = over budget. |

## Sample Questions

Add these as conversation starters in the Genie Space:

1. Which plant has the highest breakdown rate?
2. Compare schedule compliance across all three plants
3. What is the PM to CM ratio for each plant and is it world-class?
4. Show the monthly cost trend by plant
5. What are the top 5 most expensive damage codes?
6. Which equipment IDs have the most breakdowns?
7. Show orders over budget by plant and maintenance category
8. What is the P90 repair duration for each plant?
9. How does cost per preventive order compare to corrective?
10. Show backlog count trend by month and plant

## Power Questions for Testing

Use these to validate Genie accuracy after setup:

- "What percentage of Detroit's corrective orders are over budget?"
- "Show the top 5 equipment with most breakdowns and their total repair cost"
- "Compare the median vs P90 repair duration across plants"
- "Which damage code costs the most to repair on average?"
- "Show monthly schedule compliance trend for Plant C only"
