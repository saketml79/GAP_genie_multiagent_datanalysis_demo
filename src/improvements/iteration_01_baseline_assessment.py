# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Iteration 1: Baseline Assessment
# MAGIC 
# MAGIC **Goal**: Document the ground truth numbers from direct SQL queries, then compare them against
# MAGIC what the Supervisor Agent returns. This establishes our baseline for measuring improvement.
# MAGIC 
# MAGIC **What this proves**: Without certified queries, synonyms, or benchmarks, Genie agents
# MAGIC can hallucinate numbers, miss data, or produce inconsistent results across runs.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ground Truth Queries
# MAGIC These are the CORRECT answers. Every future iteration is measured against these.

# COMMAND ----------
dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Demand Analysis: Revenue by Region (Last 30d vs Prior 30d)

# COMMAND ----------
df_revenue = spark.sql(f"""
SELECT
  region,
  ROUND(SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE(), 30) THEN total_amount ELSE 0 END), 2) AS revenue_last_30d,
  ROUND(SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END), 2) AS revenue_prior_30d,
  ROUND(
    SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE(), 30) THEN total_amount ELSE 0 END) -
    SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END)
  , 2) AS revenue_change,
  ROUND(
    (SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE(), 30) THEN total_amount ELSE 0 END) -
     SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END)) /
    NULLIF(SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END), 0) * 100
  , 1) AS pct_change
FROM {CATALOG}.demand_analysis.sales_orders
GROUP BY region ORDER BY revenue_change ASC
""")
df_revenue.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Demand Analysis: Western Revenue by Product Family

# COMMAND ----------
df_western = spark.sql(f"""
SELECT
  product_family,
  ROUND(SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE(), 30) THEN total_amount ELSE 0 END), 2) AS rev_last_30d,
  ROUND(SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END), 2) AS rev_prior_30d,
  ROUND(
    SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE(), 30) THEN total_amount ELSE 0 END) -
    SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END)
  , 2) AS change_usd,
  ROUND(
    (SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE(), 30) THEN total_amount ELSE 0 END) -
     SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END)) /
    NULLIF(SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END), 0) * 100
  , 1) AS pct_change
FROM {CATALOG}.demand_analysis.sales_orders
WHERE region = 'Western'
GROUP BY product_family ORDER BY change_usd ASC
""")
df_western.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Inventory: Stockouts by Region

# COMMAND ----------
df_inv = spark.sql(f"""
SELECT region,
  COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS stockout_skus,
  COUNT(DISTINCT sku_id) AS total_skus,
  ROUND(AVG(days_of_supply), 1) AS avg_days_of_supply,
  COUNT(CASE WHEN below_safety_stock_flag = true THEN 1 END) AS below_safety_stock_count
FROM {CATALOG}.inventory_management.inventory_ledger
GROUP BY region ORDER BY stockout_skus DESC
""")
df_inv.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4. Logistics: Shipment Performance by Destination Region (Last 30d)

# COMMAND ----------
df_ship = spark.sql(f"""
SELECT
  destination_region,
  COUNT(*) AS total_shipments,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_shipments,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(CASE WHEN is_late THEN delay_days ELSE NULL END), 1) AS avg_delay_when_late
FROM {CATALOG}.logistics_operations.shipments
WHERE ship_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY destination_region ORDER BY late_pct DESC
""")
df_ship.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5. Supplier: Performance by Continent (Last 30d)

# COMMAND ----------
df_sup = spark.sql(f"""
SELECT
  supplier_continent,
  COUNT(*) AS total_pos,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_pos,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(lead_time_variance_days), 1) AS avg_variance_days
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY supplier_continent ORDER BY late_pct DESC
""")
df_sup.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 6. Executive KPIs

# COMMAND ----------
df_kpi = spark.sql(f"SELECT * FROM {CATALOG}.reporting.executive_kpis")
df_kpi.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Baseline Issues Found
# MAGIC 
# MAGIC After running the Supervisor Agent with the all-agents prompt **before any improvements**:
# MAGIC 
# MAGIC | Issue | Category | Description |
# MAGIC | --- | --- | --- |
# MAGIC | Revenue numbers wrong | **Accuracy** | Agent reported -$282K (-6.6%) but actual is -$1.23M (-29.1%) |
# MAGIC | Sub-totals don't sum | **Consistency** | Product family breakdowns don't add to the regional total |
# MAGIC | Logistics returned 0 rows | **Completeness** | Agent queried `region` instead of `destination_region` |
# MAGIC | Different results per run | **Consistency** | Second run returned different data than first |
# MAGIC | No source SQL shown | **Traceability** | No way to verify which queries produced the numbers |
# MAGIC | No charts | **Presentation** | Pure text output, no visual analysis |
# MAGIC 
# MAGIC **Baseline Score: 2/10** - Numbers wrong, incomplete coverage, no traceability.
