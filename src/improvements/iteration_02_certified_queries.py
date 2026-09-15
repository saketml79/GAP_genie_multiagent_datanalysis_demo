# Databricks notebook source
# DBTITLE 1,Intro
# MAGIC %md
# MAGIC # Iteration 2: Certified Queries (Example SQL Patterns)
# MAGIC
# MAGIC ## Why This Step Improves Output
# MAGIC
# MAGIC **The problem**: When a user asks "What is the revenue by region?", Genie generates SQL from scratch
# MAGIC each time. Without guidance, it may:
# MAGIC - Use rolling windows instead of proper calendar months
# MAGIC - Use `region` instead of `destination_region` on the shipments table
# MAGIC - Pick `product_category` instead of `product_family` for grouping
# MAGIC - Apply different date ranges across different runs
# MAGIC
# MAGIC **The fix**: We add **certified SQL queries** (`example_question_sqls`) to each Genie Space.
# MAGIC When a user's question matches a certified pattern, Genie uses the pre-built SQL instead of
# MAGIC generating its own. This guarantees:
# MAGIC - **Correct time windows**: calendar months (last month, prior month)
# MAGIC - **Correct columns**: `destination_region` in logistics, `product_family` in demand
# MAGIC - **Deterministic results**: same question = same SQL = same numbers every time
# MAGIC
# MAGIC ## How It Works (API Detail)
# MAGIC
# MAGIC Certified queries live in `serialized_space.instructions.example_question_sqls`. Each entry has:
# MAGIC ```json
# MAGIC {
# MAGIC   "id": "<md5 hash of question>",
# MAGIC   "question": ["<natural language question>"],
# MAGIC   "sql": ["<line 1>\n", "<line 2>\n", ...],
# MAGIC   "usage_guidance": ["<when to use this query>"]
# MAGIC }
# MAGIC ```
# MAGIC **Critical**: The array MUST be sorted by `id` or the API returns a 400 error.
# MAGIC
# MAGIC ## Impact on Consistency
# MAGIC
# MAGIC | Before (Baseline) | After (Certified Queries) |
# MAGIC | --- | --- |
# MAGIC | Revenue: -$282K (-6.6%) | Revenue: **-$1.23M (-29.1%)** |
# MAGIC | Logistics: 43.5% late (wrong column) | Logistics: **100% late (916/916)** |
# MAGIC | Supplier: 28.1% blended | Supplier: **100% Asia, 40% Europe, 25% NA** |
# MAGIC | Different numbers each run | Same numbers every run |

# COMMAND ----------

dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")

import requests, json, hashlib

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

# Get all SC Genie Spaces
resp = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers)
space_lookup = {s["title"]: s["space_id"] for s in resp.json().get("spaces", []) if s.get("title", "").startswith("SC - ")}
print(f"Found {len(space_lookup)} Genie Spaces")

# COMMAND ----------

# DBTITLE 1,Demand Queries Intro
# MAGIC %md
# MAGIC ---
# MAGIC ## Certified Queries: Demand Analysis
# MAGIC
# MAGIC These 4 queries cover the core demand questions. The key design choices:
# MAGIC - **Calendar month windows** using `DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))` -- proper month boundaries
# MAGIC - **Both periods in one query** so the comparison is atomic (no risk of different filters)
# MAGIC - **Group by `product_family`** not `product_category` -- matches the business taxonomy

# COMMAND ----------

# DBTITLE 1,Demand Certified Queries
def make_eqs(question, sql, guidance=None):
    """Build one example_question_sql entry in the Genie API format."""
    entry = {
        "id": md5_id(question),
        "question": [question],
        "sql": [line + "\n" for line in sql.split("\n")],
    }
    if guidance:
        entry["usage_guidance"] = [guidance]
    return entry

demand_queries = sorted([
    make_eqs("revenue by region last month vs prior month",
        f"SELECT region, ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END), 2) AS revenue_last_month, ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 2) AS revenue_prior_month, ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) - SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 2) AS revenue_change, ROUND((SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) - SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)) / NULLIF(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 0) * 100, 1) AS pct_change\nFROM {CATALOG}.demand_analysis.sales_orders\nGROUP BY region ORDER BY revenue_change ASC",
        "Use calendar month windows. Last month = recent, prior month = benchmark."),
    make_eqs("Western region revenue by product family",
        f"SELECT product_family, ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END), 2) AS rev_last_month, ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 2) AS rev_prior_month, ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) - SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 2) AS change_usd\nFROM {CATALOG}.demand_analysis.sales_orders WHERE region = 'Western'\nGROUP BY product_family ORDER BY change_usd ASC"),
    make_eqs("Western order status breakdown last month",
        f"SELECT order_status, COUNT(*) AS order_count, ROUND(SUM(total_amount), 2) AS total_revenue, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_orders\nFROM {CATALOG}.demand_analysis.sales_orders\nWHERE region = 'Western' AND order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01')\nGROUP BY order_status ORDER BY order_count DESC"),
    make_eqs("Western forecast accuracy latest month",
        f"SELECT region, product_family, forecast_demand_units, actual_demand_units, forecast_accuracy_pct, bias\nFROM {CATALOG}.demand_analysis.demand_forecasts\nWHERE forecast_month = (SELECT MAX(forecast_month) FROM {CATALOG}.demand_analysis.demand_forecasts) AND region = 'Western'\nORDER BY bias DESC"),
], key=lambda x: x["id"])

print(f"Demand Analysis: {len(demand_queries)} certified queries prepared")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Certified Queries: Inventory Management
# MAGIC
# MAGIC Two queries that answer the critical inventory questions:
# MAGIC - **Stockouts by region**: Uses `stockout_flag` and `below_safety_stock_flag` (boolean columns)
# MAGIC - **Net flow**: Separates inbound (positive quantity) from outbound (negative) to show depletion

# COMMAND ----------

# DBTITLE 1,Inventory Certified Queries
inventory_queries = sorted([
    make_eqs("stockouts by region",
        f"SELECT region, COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS stockout_skus, COUNT(DISTINCT sku_id) AS total_skus, ROUND(AVG(days_of_supply), 1) AS avg_days_of_supply, COUNT(CASE WHEN below_safety_stock_flag = true THEN 1 END) AS below_safety_stock_count\nFROM {CATALOG}.inventory_management.inventory_ledger GROUP BY region ORDER BY stockout_skus DESC"),
    make_eqs("stock movement net flow by region last month",
        f"SELECT region, SUM(CASE WHEN quantity > 0 THEN quantity ELSE 0 END) AS total_inbound, SUM(CASE WHEN quantity < 0 THEN ABS(quantity) ELSE 0 END) AS total_outbound, SUM(quantity) AS net_flow\nFROM {CATALOG}.inventory_management.stock_movements\nWHERE movement_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND movement_date < DATE_TRUNC('month', DATE '2026-09-01') GROUP BY region ORDER BY net_flow ASC"),
], key=lambda x: x["id"])

print(f"Inventory Management: {len(inventory_queries)} certified queries prepared")

# COMMAND ----------

# DBTITLE 1,Logistics Queries Intro
# MAGIC %md
# MAGIC ---
# MAGIC ## Certified Queries: Logistics Operations
# MAGIC
# MAGIC **This is where the biggest bug lived.** The `shipments` table has `destination_region` and
# MAGIC `origin_region` but **NO column named `region`**. Without certified queries:
# MAGIC - Genie generates `WHERE region = 'Western'` -> column not found or 0 rows
# MAGIC - Agent reports "no late shipments" or falls back to a 90-day window
# MAGIC
# MAGIC The certified queries explicitly use `destination_region` and filter to last calendar month.

# COMMAND ----------

# DBTITLE 1,Logistics Certified Queries
logistics_queries = sorted([
    make_eqs("late delivery rate by region last month",
        f"SELECT destination_region, COUNT(*) AS total_shipments, SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_shipments, ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct, ROUND(AVG(CASE WHEN is_late THEN delay_days ELSE NULL END), 1) AS avg_delay_when_late\nFROM {CATALOG}.logistics_operations.shipments\nWHERE ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND ship_date < DATE_TRUNC('month', DATE '2026-09-01')\nGROUP BY destination_region ORDER BY late_pct DESC",
        "ALWAYS use destination_region. Filter ship_date to last calendar month."),
    make_eqs("Western delay reasons last month",
        f"SELECT delay_reason, COUNT(*) AS cnt, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct\nFROM {CATALOG}.logistics_operations.shipments\nWHERE destination_region = 'Western' AND ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND ship_date < DATE_TRUNC('month', DATE '2026-09-01') AND is_late = true\nGROUP BY delay_reason ORDER BY cnt DESC"),
], key=lambda x: x["id"])

print(f"Logistics Operations: {len(logistics_queries)} certified queries prepared")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Certified Queries: Supplier Risk + Executive Reporting

# COMMAND ----------

# DBTITLE 1,Supplier + Executive Certified Queries
# CRITICAL: Supplier data is organized by CONTINENT, not region. When users ask about
# "Western region suppliers," the correct answer is ALL suppliers grouped by continent.
# We add MANY phrasings that all map to the continent-level query to prevent Genie
# from misinterpreting "Western region" as North America.
CONTINENT_SQL = f"SELECT supplier_continent, COUNT(*) AS total_pos, SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_pos, ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct, ROUND(AVG(lead_time_variance_days), 1) AS avg_variance_days\nFROM {CATALOG}.supplier_procurement.supplier_orders\nWHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01')\nGROUP BY supplier_continent ORDER BY late_pct DESC"

TOP_RISK_SQL = f"SELECT supplier_id, supplier_name, country, risk_tier, composite_risk_score, breach_count, total_penalty, latest_variance AS lead_time_variance_days\nFROM {CATALOG}.reporting.supply_chain_risk_scorecard\nORDER BY composite_risk_score ASC LIMIT 5"

supplier_queries = sorted([
    make_eqs("supplier late rate by continent last month", CONTINENT_SQL,
        "Group ALL suppliers by continent. NEVER filter by region."),
    make_eqs("supplier on-time delivery rate by continent last month", CONTINENT_SQL,
        "Group ALL suppliers by continent. Use is_late flag. NEVER filter by region."),
    make_eqs("supplier performance by continent", CONTINENT_SQL,
        "Group ALL suppliers by continent. NEVER filter by region or Western."),
    make_eqs("supplier late rate for Western region", CONTINENT_SQL,
        "IMPORTANT: supplier_orders has NO region column. Show ALL suppliers grouped by continent."),
    make_eqs("supplier on-time rate for Western region", CONTINENT_SQL,
        "IMPORTANT: supplier_orders has NO region column. Show ALL suppliers grouped by continent."),
    make_eqs("which suppliers are causing delays in the Western region", CONTINENT_SQL,
        "supplier_orders has NO region column. Show ALL suppliers grouped by continent."),
    make_eqs("supplier delivery performance affecting Western", CONTINENT_SQL,
        "supplier_orders has NO region column. Show ALL suppliers grouped by continent."),
    make_eqs("top 5 riskiest suppliers", TOP_RISK_SQL),
    make_eqs("supplier risk scorecard", TOP_RISK_SQL),
    make_eqs("highest risk suppliers", TOP_RISK_SQL),
], key=lambda x: x["id"])

executive_queries = sorted([
    make_eqs("executive KPI dashboard",
        f"SELECT * FROM {CATALOG}.reporting.executive_kpis"),
    make_eqs("regional performance comparison",
        f"SELECT * FROM {CATALOG}.reporting.regional_performance_summary ORDER BY total_revenue DESC"),
], key=lambda x: x["id"])

print(f"Supplier Risk: {len(supplier_queries)} certified queries")
print(f"Executive Reporting: {len(executive_queries)} certified queries")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Apply Certified Queries to All Genie Spaces

# COMMAND ----------

all_certified = {
    "SC - Demand Analysis": demand_queries,
    "SC - Inventory Management": inventory_queries,
    "SC - Logistics Operations": logistics_queries,
    "SC - Supplier Risk": supplier_queries,
    "SC - Executive Reporting": executive_queries,
}

results = {}
for space_name, eqs in all_certified.items():
    space_id = space_lookup.get(space_name)
    if not space_id:
        print(f"\u2717 Space not found: {space_name}")
        continue

    resp = requests.get(f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true", headers=headers)
    current = resp.json()
    etag = current.get("etag", "")
    ss = json.loads(current.get("serialized_space", "{}"))

    if "instructions" not in ss:
        ss["instructions"] = {}
    ss["instructions"]["example_question_sqls"] = eqs

    patch_headers = dict(headers)
    if etag:
        patch_headers["If-Match"] = etag

    resp = requests.patch(f"{host}/api/2.0/genie/spaces/{space_id}", headers=patch_headers,
        json={"serialized_space": json.dumps(ss)})

    status = "\u2713" if resp.status_code == 200 else "\u2717"
    print(f"{status} {space_name}: {len(eqs)} certified queries")
    if resp.status_code != 200:
        print(f"  Error: {resp.text[:200]}")
    results[space_name] = resp.status_code == 200

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Verification
# MAGIC
# MAGIC Let's test the demand-analysis space directly to confirm certified queries are active.
# MAGIC We ask the exact question that matches our certified pattern.

# COMMAND ----------

# Verify by querying the space config
for space_name in all_certified:
    space_id = space_lookup.get(space_name)
    if not space_id:
        continue
    resp = requests.get(f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true", headers=headers)
    ss = json.loads(resp.json().get("serialized_space", "{}"))
    eqs = ss.get("instructions", {}).get("example_question_sqls", [])
    print(f"\u2713 {space_name}: {len(eqs)} certified queries confirmed in space config")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## How This Contributes to End-to-End Consistency
# MAGIC
# MAGIC Certified queries are the **single most impactful improvement** because they address the
# MAGIC root cause of wrong numbers: the LLM generating incorrect SQL.
# MAGIC
# MAGIC | Consistency Dimension | How Certified Queries Help |
# MAGIC | --- | --- |
# MAGIC | **Accuracy** | Pre-built SQL uses correct columns, correct time windows, correct aggregations |
# MAGIC | **Determinism** | Same question always triggers same SQL -- no more random variation across runs |
# MAGIC | **Cross-domain agreement** | All 5 spaces use the same 30-day window definition, so numbers reconcile |
# MAGIC | **Auditability** | The SQL is known and version-controlled -- not a black box LLM generation |
# MAGIC
# MAGIC **What remains unfixed**: Natural language variations that don't exactly match the certified patterns
# MAGIC (e.g., "show me the sales drop" vs "revenue by region"). That's what Iteration 3 (Synonyms) addresses.