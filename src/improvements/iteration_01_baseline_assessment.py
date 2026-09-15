# Databricks notebook source
# MAGIC %md
# MAGIC # Iteration 1: Baseline Assessment
# MAGIC
# MAGIC ## Why This Step Exists
# MAGIC
# MAGIC Before improving anything, we need to answer two questions:
# MAGIC 1. **What does the Supervisor Agent currently get wrong?**
# MAGIC 2. **What are the correct numbers?** (Ground truth from direct SQL)
# MAGIC
# MAGIC Without a measured baseline, every "improvement" is just a guess. This notebook establishes
# MAGIC the **scoreboard** that all subsequent iterations are measured against.
# MAGIC
# MAGIC ## The Core Problem
# MAGIC
# MAGIC A raw Genie Space with just tables and no guidance produces **non-deterministic, often wrong** results because:
# MAGIC
# MAGIC | Failure Mode | Example | Root Cause |
# MAGIC | --- | --- | --- |
# MAGIC | Wrong time windows | Agent compares "July vs August" instead of rolling 30-day periods | No SQL pattern to follow |
# MAGIC | Wrong columns | Logistics queries `WHERE region = 'Western'` on a table that has no `region` column | Column naming mismatch |
# MAGIC | Inconsistent runs | First run says -$282K, second run says -$1.2M for the same question | LLM generates different SQL each time |
# MAGIC | Numbers don't reconcile | Product family subtotals don't sum to regional total | Each sub-query uses different filters |
# MAGIC | Missing domains | Agent only queries 2 of 5 spaces for a cross-domain question | No routing guidance |
# MAGIC
# MAGIC ## What We'll Fix (Preview)
# MAGIC
# MAGIC | Iteration | Fix | What It Solves |
# MAGIC | --- | --- | --- |
# MAGIC | 2 - Certified Queries | Pre-built SQL patterns per Genie Space | Accuracy: correct time windows, correct columns, deterministic SQL |
# MAGIC | 3 - Column Synonyms | Map business terms to column names + entity matching | Robustness: "revenue" -> `total_amount`, "western" -> "Western" |
# MAGIC | 4 - Supervisor Hardening | Structured output format with cross-domain reconciliation | Consistency: numbers agree across agents, root cause chains connect |

# COMMAND ----------

dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# DBTITLE 1,Ground Truth: Revenue by Region
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Ground Truth Queries
# MAGIC
# MAGIC These are the **correct answers**, computed from direct SQL against the actual data.
# MAGIC Every future iteration is measured against these exact numbers.
# MAGIC
# MAGIC ### 1A. Revenue by Region (Last Month vs Prior Month)
# MAGIC
# MAGIC The foundational metric. We use **calendar month boundaries** via `DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))`.
# MAGIC "Last month" = the previous complete calendar month. "Prior month" = the month before that.

# COMMAND ----------

# DBTITLE 1,Revenue Query
df_revenue = spark.sql(f"""
SELECT
  region,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END), 2) AS revenue_last_month,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 2) AS revenue_prior_month,
  ROUND(
    SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) -
    SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)
  , 2) AS revenue_change,
  ROUND(
    (SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) -
     SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)) /
    NULLIF(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 0) * 100
  , 1) AS pct_change
FROM {CATALOG}.demand_analysis.sales_orders
GROUP BY region ORDER BY revenue_change ASC
""")
df_revenue.display()

# COMMAND ----------

# DBTITLE 1,Ground Truth: Revenue
# MAGIC %md
# MAGIC **Ground Truth**: Western revenue dropped **\~$1.28M (-30.4%)**. This is the single most important
# MAGIC number in the entire analysis. If the Supervisor Agent gets this wrong, everything downstream is wrong.
# MAGIC
# MAGIC ### 1B. Western Revenue by Product Family

# COMMAND ----------

# DBTITLE 1,Product Family Query
df_product = spark.sql(f"""
SELECT product_family,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END), 2) AS rev_last_month,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 2) AS rev_prior_month,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) - SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 2) AS change_usd,
  ROUND(
    (SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) -
     SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)) /
    NULLIF(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 0) * 100
  , 1) AS pct_change
FROM {CATALOG}.demand_analysis.sales_orders WHERE region = 'Western'
GROUP BY product_family ORDER BY change_usd ASC
""")
df_product.display()

# COMMAND ----------

# DBTITLE 1,Ground Truth: Product Family
# MAGIC %md
# MAGIC **Ground Truth**: ALL 5 product families declined. Apparel worst (-$415K, -49.8%), then Home Goods, Footwear, Electronics, Accessories.
# MAGIC The sum of these product family changes MUST equal the regional total (\~$1.28M). This is the **reconciliation check**.
# MAGIC
# MAGIC ### 1C. Inventory Stockouts by Region

# COMMAND ----------

df_stockouts = spark.sql(f"""
SELECT region,
  COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS stockout_skus,
  COUNT(DISTINCT sku_id) AS total_skus,
  ROUND(AVG(days_of_supply), 1) AS avg_days_of_supply,
  COUNT(CASE WHEN below_safety_stock_flag = true THEN 1 END) AS below_safety_stock_count
FROM {CATALOG}.inventory_management.inventory_ledger
GROUP BY region ORDER BY stockout_skus DESC
""")
df_stockouts.display()

# COMMAND ----------

# DBTITLE 1,Ground Truth: Late Delivery
# MAGIC %md
# MAGIC **Ground Truth**: Western has **27 SKU stockouts** (other regions: near zero). Western avg days of supply is **13.5** vs 17-18 for others. **107 items below safety stock**.
# MAGIC
# MAGIC ### 1D. Late Delivery Rate by Destination Region (Last Month)
# MAGIC
# MAGIC > **Critical Column Issue**: The `shipments` table has `destination_region` and `origin_region` but
# MAGIC > **NO column named `region`**. If Genie generates `WHERE region = 'Western'`, it returns 0 rows.
# MAGIC > This is the #1 data quality trap in this dataset.

# COMMAND ----------

# DBTITLE 1,Logistics Query
df_late = spark.sql(f"""
SELECT destination_region, COUNT(*) AS total_shipments,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_shipments,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(CASE WHEN is_late THEN delay_days ELSE NULL END), 1) AS avg_delay_when_late
FROM {CATALOG}.logistics_operations.shipments
WHERE ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND ship_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY destination_region ORDER BY late_pct DESC
""")
df_late.display()

# COMMAND ----------

# DBTITLE 1,Ground Truth: Late Delivery Commentary
# MAGIC %md
# MAGIC **Ground Truth**: Western has **96.9% late delivery rate** (1034/1067 shipments, avg 3.0 day delay).
# MAGIC Other regions are 29-31%. This is the smoking gun.
# MAGIC
# MAGIC ### 1E. Supplier Late Rate by Continent (Last Month)

# COMMAND ----------

# DBTITLE 1,Supplier Query
df_supplier = spark.sql(f"""
SELECT supplier_continent, COUNT(*) AS total_pos,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_pos,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(lead_time_variance_days), 1) AS avg_variance_days
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY supplier_continent ORDER BY late_pct DESC
""")
df_supplier.display()

# COMMAND ----------

# DBTITLE 1,Ground Truth: Supplier
# MAGIC %md
# MAGIC **Ground Truth**: Asia **100% late** (338/338 POs, +11.8 day avg variance). Europe 52.0%, North America 20.4%.
# MAGIC
# MAGIC ### 1F. Executive KPIs

# COMMAND ----------

df_kpis = spark.sql(f"SELECT * FROM {CATALOG}.reporting.executive_kpis")
df_kpis.display()

# COMMAND ----------

# DBTITLE 1,Ground Truth: Executive KPIs + Summary + Baseline Errors
# MAGIC %md
# MAGIC **Ground Truth KPIs**: Revenue last month \~$18.3M, stockout SKUs = 29, late delivery = 45.6%, supplier late = 83.8%, service level = 83.9%.
# MAGIC
# MAGIC ---
# MAGIC ## Step 2: Ground Truth Summary Card
# MAGIC
# MAGIC | KPI | Correct Value | Source Table |
# MAGIC | --- | --- | --- |
# MAGIC | Western revenue change | **-$1.28M (-30.4%)** | `demand_analysis.sales_orders` |
# MAGIC | Worst product family | **Apparel -$415K (-49.8%)** | `demand_analysis.sales_orders` |
# MAGIC | Western stockout SKUs | **27** (other regions: near zero) | `inventory_management.inventory_ledger` |
# MAGIC | Western late delivery | **96.9% (1034/1067)** | `logistics_operations.shipments` |
# MAGIC | Asian supplier late rate | **100% (338/338, +11.8d)** | `supplier_procurement.supplier_orders` |
# MAGIC | Service level | **83.9%** (target: 95%) | `reporting.executive_kpis` |
# MAGIC
# MAGIC ---
# MAGIC ## Step 3: Typical Baseline Agent Errors
# MAGIC
# MAGIC When the Supervisor Agent is run **without** any improvements, here are the typical errors observed:
# MAGIC
# MAGIC | KPI | Agent Output (Baseline) | Correct Value | Error Type |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Western revenue | -$878K (-19.8%) | -$1.28M (-30.4%) | **Wrong time window** (rolling 30d vs calendar month) |
# MAGIC | Late delivery rate | 3.1% (wrong column) | 96.9% (Western) | **Wrong column** (`region` vs `destination_region`) + wrong time window |
# MAGIC | Supplier late rate | not computed | 100% Asia | **Wrong aggregation** (all continents blended vs by-continent) |
# MAGIC | Product families | Reports by category, not family | Should use `product_family` | **Wrong column** |
# MAGIC | Cross-domain reconciliation | Not attempted | Numbers don't agree | **No reconciliation logic** |
# MAGIC
# MAGIC > **Key insight**: The agent isn't "hallucinating" -- it's generating syntactically valid SQL that queries
# MAGIC > the wrong columns, the wrong time periods, or the wrong aggregation level. The fix isn't more data --
# MAGIC > it's better guidance on HOW to query the data.
# MAGIC
# MAGIC ---
# MAGIC ## How This Contributes to Consistency
# MAGIC
# MAGIC This iteration doesn't change the agent. It establishes the **measurement framework**:
# MAGIC - Ground truth numbers computed from direct SQL (not agent-generated)
# MAGIC - Clear error taxonomy (wrong time window, wrong column, wrong aggregation)
# MAGIC - Baseline scores that future iterations must beat
# MAGIC
# MAGIC **Without this step, we'd have no way to know if our "improvements" actually improved anything.**