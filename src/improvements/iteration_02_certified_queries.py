# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Iteration 2: Add Certified Queries (Sample SQL)
# MAGIC 
# MAGIC **Goal**: Add pre-built SQL queries to each Genie Space. When a user asks a question that
# MAGIC matches a certified query pattern, Genie uses the exact SQL instead of generating its own.
# MAGIC 
# MAGIC **What this fixes**:
# MAGIC - Revenue calculations: ensures correct period comparison logic
# MAGIC - Logistics: forces use of `destination_region` not `region`
# MAGIC - Consistency: same question always returns the same SQL pattern
# MAGIC 
# MAGIC **Improvement**: Accuracy + Consistency

# COMMAND ----------
dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")

import requests, json, hashlib, time

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    host = w.config.host
    headers = w.config.authenticate()
    headers["Content-Type"] = "application/json"
except:
    host = f"https://{spark.conf.get('spark.databricks.workspaceUrl', '')}"
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def md5_id(text):
    return hashlib.md5(text.encode()).hexdigest()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Certified Queries by Genie Space

# COMMAND ----------
# Discover existing Genie Spaces
resp = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers)
all_spaces = resp.json().get("spaces", [])
space_lookup = {s["title"]: s["space_id"] for s in all_spaces if s.get("title", "").startswith("SC - ")}
print("Found spaces:", json.dumps(space_lookup, indent=2))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Demand Analysis Certified Queries

# COMMAND ----------
demand_queries = [
    {
        "question": "revenue by region last 30 days vs prior 30 days",
        "sql": f"""SELECT
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
GROUP BY region
ORDER BY revenue_change ASC"""
    },
    {
        "question": "Western region revenue by product family",
        "sql": f"""SELECT
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
GROUP BY product_family
ORDER BY change_usd ASC"""
    },
    {
        "question": "Western region order status breakdown",
        "sql": f"""SELECT order_status, COUNT(*) AS order_count,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_orders
FROM {CATALOG}.demand_analysis.sales_orders
WHERE region = 'Western' AND order_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY order_status ORDER BY order_count DESC"""
    },
    {
        "question": "Western forecast accuracy and bias",
        "sql": f"""SELECT region, product_family, forecast_demand_units, actual_demand_units,
  forecast_accuracy_pct, bias
FROM {CATALOG}.demand_analysis.demand_forecasts
WHERE forecast_month = (SELECT MAX(forecast_month) FROM {CATALOG}.demand_analysis.demand_forecasts)
  AND region = 'Western'
ORDER BY bias DESC"""
    },
]

# COMMAND ----------
# MAGIC %md
# MAGIC ### Inventory Management Certified Queries

# COMMAND ----------
inventory_queries = [
    {
        "question": "stockouts by region",
        "sql": f"""SELECT region,
  COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS stockout_skus,
  COUNT(DISTINCT sku_id) AS total_skus,
  ROUND(AVG(days_of_supply), 1) AS avg_days_of_supply,
  COUNT(CASE WHEN below_safety_stock_flag = true THEN 1 END) AS below_safety_stock_count
FROM {CATALOG}.inventory_management.inventory_ledger
GROUP BY region ORDER BY stockout_skus DESC"""
    },
    {
        "question": "Western inventory by product family",
        "sql": f"""SELECT product_family,
  COUNT(DISTINCT sku_id) AS total_skus,
  COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS stockout_skus,
  ROUND(AVG(on_hand_qty), 1) AS avg_on_hand,
  ROUND(AVG(safety_stock_level), 1) AS avg_safety_stock,
  ROUND(AVG(days_of_supply), 1) AS avg_days_of_supply
FROM {CATALOG}.inventory_management.inventory_ledger
WHERE region = 'Western'
GROUP BY product_family ORDER BY stockout_skus DESC"""
    },
    {
        "question": "stock movement net flow by region last 30 days",
        "sql": f"""SELECT region,
  SUM(CASE WHEN quantity > 0 THEN quantity ELSE 0 END) AS total_inbound,
  SUM(CASE WHEN quantity < 0 THEN ABS(quantity) ELSE 0 END) AS total_outbound,
  SUM(quantity) AS net_flow
FROM {CATALOG}.inventory_management.stock_movements
WHERE movement_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY region ORDER BY net_flow ASC"""
    },
]

# COMMAND ----------
# MAGIC %md
# MAGIC ### Logistics Operations Certified Queries

# COMMAND ----------
logistics_queries = [
    {
        "question": "late delivery rate by region last 30 days",
        "sql": f"""SELECT
  destination_region,
  COUNT(*) AS total_shipments,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_shipments,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(CASE WHEN is_late THEN delay_days ELSE NULL END), 1) AS avg_delay_when_late
FROM {CATALOG}.logistics_operations.shipments
WHERE ship_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY destination_region ORDER BY late_pct DESC"""
    },
    {
        "question": "Western delay reasons",
        "sql": f"""SELECT delay_reason, COUNT(*) AS cnt,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
FROM {CATALOG}.logistics_operations.shipments
WHERE destination_region = 'Western'
  AND ship_date >= DATE_SUB(CURRENT_DATE(), 30)
  AND is_late = true
GROUP BY delay_reason ORDER BY cnt DESC"""
    },
    {
        "question": "carrier performance for Western shipments",
        "sql": f"""SELECT c.carrier_name, c.carrier_type,
  COUNT(*) AS total_shipments,
  SUM(CASE WHEN s.is_late THEN 1 ELSE 0 END) AS late,
  ROUND(AVG(CASE WHEN s.is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(s.delay_days), 1) AS avg_delay
FROM {CATALOG}.logistics_operations.shipments s
JOIN {CATALOG}.logistics_operations.carriers c ON s.carrier_id = c.carrier_id
WHERE s.destination_region = 'Western' AND s.ship_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY c.carrier_name, c.carrier_type ORDER BY late_pct DESC"""
    },
]

# COMMAND ----------
# MAGIC %md
# MAGIC ### Supplier Risk Certified Queries

# COMMAND ----------
supplier_queries = [
    {
        "question": "supplier performance by continent last 30 days",
        "sql": f"""SELECT
  supplier_continent,
  COUNT(*) AS total_pos,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_pos,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(lead_time_variance_days), 1) AS avg_variance_days,
  ROUND(SUM(quantity_received) * 100.0 / NULLIF(SUM(quantity_ordered), 0), 1) AS fill_rate_pct
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY supplier_continent ORDER BY late_pct DESC"""
    },
    {
        "question": "top risk suppliers",
        "sql": f"""SELECT supplier_id, supplier_name, country, risk_tier,
  composite_risk_score, breach_count, total_penalty,
  latest_variance AS lead_time_variance_days, on_time_delivery_pct
FROM {CATALOG}.reporting.supply_chain_risk_scorecard
ORDER BY composite_risk_score ASC LIMIT 5"""
    },
    {
        "question": "SLA breaches with penalties",
        "sql": f"""SELECT supplier_name, sla_metric,
  target_pct, actual_pct, variance_pct, penalty_amount
FROM {CATALOG}.supplier_procurement.vendor_slas
WHERE is_breached = true
ORDER BY penalty_amount DESC"""
    },
]

# COMMAND ----------
# MAGIC %md
# MAGIC ### Executive Reporting Certified Queries

# COMMAND ----------
exec_queries = [
    {
        "question": "executive KPI dashboard",
        "sql": f"SELECT * FROM {CATALOG}.reporting.executive_kpis"
    },
    {
        "question": "regional performance comparison",
        "sql": f"SELECT * FROM {CATALOG}.reporting.regional_performance_summary ORDER BY total_revenue DESC"
    },
]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Apply Certified Queries to Genie Spaces

# COMMAND ----------
all_certified = {
    "SC - Demand Analysis": demand_queries,
    "SC - Inventory Management": inventory_queries,
    "SC - Logistics Operations": logistics_queries,
    "SC - Supplier Risk": supplier_queries,
    "SC - Executive Reporting": exec_queries,
}

for space_name, queries in all_certified.items():
    space_id = space_lookup.get(space_name)
    if not space_id:
        print(f"\u2717 Space not found: {space_name}")
        continue

    # Get current serialized_space
    resp = requests.get(f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true", headers=headers)
    current = resp.json()
    etag = current.get("etag", "")
    ss = json.loads(current.get("serialized_space", "{}"))

    # Add sample queries to config
    sample_questions = []
    for q in queries:
        sample_questions.append({
            "question": q["question"],
            "sql": q["sql"]
        })

    if "config" not in ss:
        ss["config"] = {}
    ss["config"]["sample_questions"] = sample_questions

    # PATCH
    patch_headers = dict(headers)
    if etag:
        patch_headers["If-Match"] = etag

    resp = requests.patch(
        f"{host}/api/2.0/genie/spaces/{space_id}",
        headers=patch_headers,
        json={"serialized_space": json.dumps(ss)}
    )
    if resp.status_code == 200:
        print(f"\u2713 {space_name}: {len(queries)} certified queries added")
    else:
        print(f"\u2717 {space_name}: {resp.status_code} - {resp.text[:200]}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Expected Improvement
# MAGIC 
# MAGIC | Metric | Before | After |
# MAGIC | --- | --- | --- |
# MAGIC | Revenue accuracy | Wrong (-$282K) | Correct (-$1.23M) |
# MAGIC | Logistics data | 0 rows (wrong column) | Full data (destination_region) |
# MAGIC | Period comparison | Inconsistent | Standardized 30d/60d windows |
# MAGIC | Query patterns | Random generation | Pre-certified SQL |
# MAGIC 
# MAGIC **Next**: Run the Supervisor Agent prompt again and compare results.
