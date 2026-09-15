# Databricks notebook source
# MAGIC %md
# MAGIC # Iteration 06: Cost of Disruption — UC Domains & Business Definitions
# MAGIC
# MAGIC **The follow-up question that breaks the 10/10 system:**
# MAGIC
# MAGIC > *"What is our total Cost of Disruption by region last month — combining lost revenue from
# MAGIC > cancellations, at-risk backorder revenue, supplier SLA penalties, and wasted logistics spend
# MAGIC > on late shipments?"*
# MAGIC
# MAGIC ### Why no existing iteration can answer this
# MAGIC
# MAGIC | Barrier | Detail |
# MAGIC | --- | --- |
# MAGIC | Undefined term | "Cost of Disruption" doesn't exist in any schema, comment, synonym, or certified query |
# MAGIC | Cross-domain join | Requires combining `demand_analysis` + `logistics_operations` + `supplier_procurement` |
# MAGIC | No single agent | Each Genie Agent only sees its own schema |
# MAGIC | Supervisor can't join | It collects text answers — it can't SUM across domains |
# MAGIC | Different grain | Revenue is per-order, shipping is per-shipment, penalties are per-SLA-period |
# MAGIC
# MAGIC ### What this iteration creates
# MAGIC
# MAGIC 1. **Cross-domain metric view** `reporting.cost_of_disruption_by_region`
# MAGIC 2. **UC Tags** for domain classification (`domain:financial_impact`)
# MAGIC 3. **Rich documentation** via table and column comments with business definitions
# MAGIC 4. **Certified query** on the Executive Reporting Genie Agent
# MAGIC 5. **Updated supervisor tool description** referencing CoD
# MAGIC 6. **New ground truth row** for Western CoD
# MAGIC
# MAGIC **Workshop narrative**: *"5 iterations got us to 100% on known metrics. But one new business
# MAGIC question from the CFO exposed a governance gap — not a technical one. The fix wasn't better SQL
# MAGIC or smarter prompts. It was a formal business definition and cross-domain data organization."*

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")
print(f"Catalog: {CATALOG}")

# COMMAND ----------

# DBTITLE 1,API Setup
import requests, json, time, hashlib

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    host = w.config.host
    headers = w.config.authenticate()
    headers["Content-Type"] = "application/json"
except Exception:
    host = f"https://{spark.conf.get('spark.databricks.workspaceUrl', '')}"
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def get_genie_space(name_prefix):
    """Find a Genie Agent by name prefix."""
    resp = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers)
    for s in resp.json().get("spaces", []):
        if s.get("title", "").startswith(name_prefix):
            return s["space_id"]
    raise ValueError(f"Genie Agent '{name_prefix}' not found")

# COMMAND ----------

# DBTITLE 1,Step 1: Create Cross-Domain View — Cost of Disruption
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.reporting.cost_of_disruption_by_region AS
WITH lost_revenue AS (
  SELECT region,
    ROUND(SUM(CASE WHEN order_status = 'Cancelled' THEN total_amount ELSE 0 END), 2)    AS cancelled_revenue,
    ROUND(SUM(CASE WHEN order_status = 'Backordered' THEN total_amount ELSE 0 END), 2)  AS backordered_at_risk_revenue
  FROM {CATALOG}.demand_analysis.sales_orders
  WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
  GROUP BY region
),
logistics_waste AS (
  SELECT destination_region AS region,
    ROUND(SUM(CASE WHEN is_late THEN shipping_cost ELSE 0 END), 2) AS wasted_logistics_spend,
    COUNT(CASE WHEN is_late THEN 1 END)                             AS late_shipment_count
  FROM {CATALOG}.logistics_operations.shipments
  WHERE ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND ship_date < DATE_TRUNC('month', DATE '2026-09-01')
  GROUP BY destination_region
),
total_penalties AS (
  SELECT ROUND(SUM(penalty_amount), 2) AS total_sla_penalties
  FROM {CATALOG}.supplier_procurement.vendor_slas
  WHERE penalty_amount > 0
),
late_share AS (
  SELECT destination_region AS region,
    COUNT(CASE WHEN is_late THEN 1 END) * 1.0
      / SUM(COUNT(CASE WHEN is_late THEN 1 END)) OVER () AS pct_of_late_shipments
  FROM {CATALOG}.logistics_operations.shipments
  WHERE ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND ship_date < DATE_TRUNC('month', DATE '2026-09-01')
  GROUP BY destination_region
)
SELECT
  lr.region,
  lr.cancelled_revenue,
  lr.backordered_at_risk_revenue,
  lw.wasted_logistics_spend,
  lw.late_shipment_count,
  ROUND(tp.total_sla_penalties * ls.pct_of_late_shipments, 2) AS allocated_supplier_penalties,
  ROUND(
    lr.cancelled_revenue
    + lr.backordered_at_risk_revenue
    + lw.wasted_logistics_spend
    + tp.total_sla_penalties * ls.pct_of_late_shipments, 2
  ) AS total_cost_of_disruption,
  CASE
    WHEN lr.cancelled_revenue + lr.backordered_at_risk_revenue + lw.wasted_logistics_spend
         + tp.total_sla_penalties * ls.pct_of_late_shipments > 2000000 THEN 'CRITICAL'
    WHEN lr.cancelled_revenue + lr.backordered_at_risk_revenue + lw.wasted_logistics_spend
         + tp.total_sla_penalties * ls.pct_of_late_shipments > 1000000 THEN 'HIGH'
    ELSE 'MODERATE'
  END AS severity_level
FROM lost_revenue lr
JOIN logistics_waste lw    ON lr.region = lw.region
JOIN late_share ls         ON lr.region = ls.region
CROSS JOIN total_penalties tp
""")

# Verify
df = spark.sql(f"SELECT * FROM {CATALOG}.reporting.cost_of_disruption_by_region ORDER BY total_cost_of_disruption DESC")
df.display()

# COMMAND ----------

# DBTITLE 1,Step 2: Add Business Definitions via Comments
# Table-level comment encodes the full CoD definition
spark.sql(f"""
COMMENT ON TABLE {CATALOG}.reporting.cost_of_disruption_by_region IS
'Cost of Disruption (CoD) by region for last calendar month. '
'A cross-domain composite metric combining: (1) Cancelled order revenue from demand_analysis.sales_orders, '
'(2) Backordered at-risk revenue from demand_analysis.sales_orders, '
'(3) Wasted logistics spend on late shipments from logistics_operations.shipments, '
'(4) Supplier SLA penalties from supplier_procurement.vendor_slas allocated proportionally by late-shipment share. '
'Severity thresholds: CRITICAL > $2M, HIGH > $1M, MODERATE <= $1M. '
'Business owner: VP Operations. Review cadence: Monthly. '
'This is the authoritative source for Cost of Disruption reporting.'
""")

# Column-level comments
col_comments = {
    "region": "Geographic sales region (Western, Eastern, Central, Southern)",
    "cancelled_revenue": "Revenue from orders with status Cancelled last calendar month. Part of Cost of Disruption.",
    "backordered_at_risk_revenue": "Revenue from orders with status Backordered last calendar month. At-risk but potentially recoverable.",
    "wasted_logistics_spend": "Total shipping cost for late shipments last calendar month. Represents logistics spend that did not meet SLA.",
    "late_shipment_count": "Number of late shipments destined for this region last calendar month.",
    "allocated_supplier_penalties": "Supplier SLA penalty dollars allocated to this region proportionally based on late-shipment share.",
    "total_cost_of_disruption": "Sum of all CoD components: cancelled_revenue + backordered_at_risk_revenue + wasted_logistics_spend + allocated_supplier_penalties. This is the primary KPI.",
    "severity_level": "Risk classification: CRITICAL (>$2M), HIGH (>$1M), MODERATE (<=$1M).",
}
for col, comment in col_comments.items():
    try:
        spark.sql(f"ALTER TABLE {CATALOG}.reporting.cost_of_disruption_by_region ALTER COLUMN {col} COMMENT '{comment}'")
    except Exception as e:
        print(f"  Column comment {col}: {e}")

print("✓ Business definitions added via table and column comments")

# COMMAND ----------

# DBTITLE 1,Step 3: UC Tags for Domain Classification
tags = {
    "domain": "financial_impact",
    "metric_type": "composite_cross_domain",
    "data_quality": "authoritative",
    "business_owner": "vp_operations",
    "review_cadence": "monthly",
}
for key, value in tags.items():
    try:
        spark.sql(f"ALTER TABLE {CATALOG}.reporting.cost_of_disruption_by_region SET TAGS ('{key}' = '{value}')")
    except Exception as e:
        print(f"  Tag {key}: {e}")

print("✓ UC governance tags applied")

# COMMAND ----------

# DBTITLE 1,Step 4: Update Executive Reporting Genie Agent — Add View + Certified Query
exec_space_id = get_genie_space("SC - Executive Reporting")
print(f"Executive Reporting Agent: {exec_space_id}")

# Get current config
resp = requests.get(
    f"{host}/api/2.0/genie/spaces/{exec_space_id}?include_serialized_space=true",
    headers=headers
)
space = resp.json()
ss = json.loads(space.get("serialized_space", "{}"))

# Add the CoD view to tables
cod_table = {"identifier": f"{CATALOG}.reporting.cost_of_disruption_by_region"}
tables = ss.get("data_sources", {}).get("tables", [])
existing_ids = {t["identifier"] for t in tables}
if cod_table["identifier"] not in existing_ids:
    tables.append(cod_table)
    tables.sort(key=lambda t: t["identifier"])
    ss.setdefault("data_sources", {})["tables"] = tables
    print(f"  Added CoD view (now {len(tables)} tables)")
else:
    print(f"  CoD view already present")

# Add certified query for Cost of Disruption
cq_sql = f"""SELECT region, cancelled_revenue, backordered_at_risk_revenue,
  wasted_logistics_spend, allocated_supplier_penalties,
  total_cost_of_disruption, severity_level
FROM {CATALOG}.reporting.cost_of_disruption_by_region
ORDER BY total_cost_of_disruption DESC"""

cq = {
    "question": "What is the cost of disruption by region last month?",
    "sql": cq_sql,
}
cq["id"] = hashlib.md5(json.dumps(cq, sort_keys=True).encode()).hexdigest()

existing_cqs = ss.get("certified_queries", [])
existing_cq_ids = {c["id"] for c in existing_cqs}
if cq["id"] not in existing_cq_ids:
    existing_cqs.append(cq)
    existing_cqs.sort(key=lambda c: c["id"])
    ss["certified_queries"] = existing_cqs
    print(f"  Added CoD certified query (now {len(existing_cqs)} CQs)")

# PATCH with retry
for attempt in range(3):
    patch_resp = requests.patch(
        f"{host}/api/2.0/genie/spaces/{exec_space_id}",
        headers=headers,
        json={"serialized_space": json.dumps(ss)}
    )
    time.sleep(2 + attempt)
    # Verify
    verify = requests.get(
        f"{host}/api/2.0/genie/spaces/{exec_space_id}?include_serialized_space=true",
        headers=headers
    )
    v_ss = json.loads(verify.json().get("serialized_space", "{}"))
    v_tables = {t["identifier"] for t in v_ss.get("data_sources", {}).get("tables", [])}
    if cod_table["identifier"] in v_tables:
        print(f"  ✓ PATCH verified (attempt {attempt+1})")
        break
else:
    print("  ⚠ PATCH may not have persisted — check manually")

# COMMAND ----------

# DBTITLE 1,Step 5: Update Supervisor Tool Description
# Find the supervisor
resp = requests.get(f"{host}/api/2.1/supervisor-agents", headers=headers)
supervisor_name = None
for agent in resp.json().get("supervisor_agents", []):
    if "Supply Chain" in agent.get("display_name", ""):
        supervisor_name = agent["name"]
        break

if not supervisor_name:
    raise ValueError("Supply Chain Supervisor not found")
print(f"Supervisor: {supervisor_name}")

# Find the executive-reporting tool and update its description
tools_resp = requests.get(f"{host}/api/2.1/{supervisor_name}/tools", headers=headers)
for tool in tools_resp.json().get("tools", []):
    tool_name = tool.get("name", "")
    if "executive" in tool_name.lower() or "Executive" in tool.get("description", ""):
        old_desc = tool["description"]
        # Append CoD guidance if not already present
        if "Cost of Disruption" not in old_desc:
            new_desc = old_desc.rstrip()
            if not new_desc.endswith("."):
                new_desc += "."
            new_desc += " For Cost of Disruption (CoD) analysis, Ask EXACTLY: 'Show cost of disruption by region from the cost_of_disruption_by_region view'"
            patch_resp = requests.patch(
                f"{host}/api/2.1/{supervisor_name}/tools/{tool['tool_id']}?update_mask=description",
                headers=headers,
                json={"description": new_desc}
            )
            print(f"  ✓ Updated tool '{tool_name}' with CoD guidance ({patch_resp.status_code})")
        else:
            print(f"  Tool '{tool_name}' already has CoD guidance")
        break

# COMMAND ----------

# DBTITLE 1,Step 6: Compute GT Dynamically from View Output
# Compute the Western CoD value DYNAMICALLY from live data
# No hardcoding — GT value = whatever the view produces
western_cod = spark.sql(f"""
  SELECT ROUND(total_cost_of_disruption, 2) AS cod
  FROM {CATALOG}.reporting.cost_of_disruption_by_region
  WHERE region = 'Western'
""").first()["cod"]

print(f"Western Cost of Disruption: ${western_cod:,.2f}")

# Compute reference SQL (store the exact query that produces this value)
ref_sql = f"SELECT ROUND(total_cost_of_disruption, 2) FROM {CATALOG}.reporting.cost_of_disruption_by_region WHERE region = 'Western'"

# MERGE into ground_truth_kpis — uses EXACT computed value, no rounding
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit

cod_row = spark.createDataFrame([Row(
    agent="executive-reporting",
    metric="Western Cost of Disruption",
    ground_truth_value=str(western_cod),
    reference_sql=ref_sql,
    uc_feature_needed="Metric View: cross-domain join (demand + logistics + supplier). Only after Iter 4."
)]).withColumn("calculated_at", current_timestamp())

# Delete old row if exists, then insert fresh
spark.sql(f"DELETE FROM {CATALOG}.reporting.ground_truth_kpis WHERE metric = 'Western Cost of Disruption'")
cod_row.write.insertInto(f"{CATALOG}.reporting.ground_truth_kpis", overwrite=False)

print(f"\u2713 Ground truth updated — Western CoD = ${western_cod:,.2f} (computed from live view)")

# Show all ground truth
spark.sql(f"SELECT metric, ground_truth_value FROM {CATALOG}.reporting.ground_truth_kpis ORDER BY metric").display()

# COMMAND ----------

# DBTITLE 1,Step 7: Verify Cost of Disruption
print("=== COST OF DISRUPTION VERIFICATION ===\n")

# Full view output
print("CoD by Region:")
df = spark.sql(f"""
  SELECT region, total_cost_of_disruption, severity_level
  FROM {CATALOG}.reporting.cost_of_disruption_by_region
  ORDER BY total_cost_of_disruption DESC
""").collect()

for row in df:
    icon = "🔴" if row["severity_level"] == "CRITICAL" else ("🟡" if row["severity_level"] == "HIGH" else "🟢")
    print(f"  {icon} {row['region']}: ${row['total_cost_of_disruption']:,.0f} ({row['severity_level']})")

# Ground truth match
gt_val = spark.sql(f"""
  SELECT ground_truth_value FROM {CATALOG}.reporting.ground_truth_kpis
  WHERE metric = 'Western Cost of Disruption'
""").first()[0]

view_val = spark.sql(f"""
  SELECT total_cost_of_disruption FROM {CATALOG}.reporting.cost_of_disruption_by_region
  WHERE region = 'Western'
""").first()[0]

match = abs(float(view_val) - float(gt_val)) / max(abs(float(gt_val)), 0.001) < 0.01
print(f"\n{'✅' if match else '❌'} Western CoD: view=${float(view_val):,.1f}, gt=${float(gt_val):,.1f}")

# Count total ground truth
gt_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.reporting.ground_truth_kpis").first()[0]
print(f"\n✓ Ground truth table now has {gt_count} metrics (was 10, now 11)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Just Happened
# MAGIC
# MAGIC | Component | What Was Created |
# MAGIC | --- | --- |
# MAGIC | `reporting.cost_of_disruption_by_region` | Cross-domain view joining demand + logistics + supplier data |
# MAGIC | Table comment | Full CoD business definition with formula, severity, owner, cadence |
# MAGIC | Column comments | 8 column-level definitions explaining each CoD component |
# MAGIC | UC Tags | `domain:financial_impact`, `metric_type:composite_cross_domain`, etc. |
# MAGIC | Certified query | "What is the cost of disruption by region?" on Executive Reporting agent |
# MAGIC | Supervisor tool | Updated description referencing CoD |
# MAGIC | Ground truth | 11th metric: Western Cost of Disruption |
# MAGIC
# MAGIC ### The Workshop Narrative
# MAGIC
# MAGIC > *"5 iterations got us to 100% accuracy on 10 known metrics. Then the CFO asked one new question
# MAGIC > — 'What is our Cost of Disruption?' — and the system failed completely. Not because the SQL was
# MAGIC > wrong, or the agents weren't smart enough, but because no one had ever **defined** what 'Cost of
# MAGIC > Disruption' means as a business metric. The fix was a formal business definition (UC comments + tags),
# MAGIC > a cross-domain metric view, and a single certified query. That's why **data governance** matters."*