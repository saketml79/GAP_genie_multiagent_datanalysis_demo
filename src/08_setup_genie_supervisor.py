# Databricks notebook source
# MAGIC %md
# MAGIC # Step 8: Create Genie Spaces, Evaluator, and Supervisor Agent
# MAGIC Creates 5 domain Genie Spaces with **minimal baseline** instructions (no certified queries,
# MAGIC no column synonyms, no hardened prompts). Also creates the Evaluator Space with ground truth
# MAGIC KPIs, and a Supervisor Agent that orchestrates all 6 as sub-agents.
# MAGIC
# MAGIC **This is the V1 (raw baseline) that the improvement iterations will progressively enhance.**
# MAGIC
# MAGIC Expected baseline accuracy: ~30% (3/10 metrics match ground truth).
# MAGIC After all 4 iterations: 100% (10/10 EXACT).

# COMMAND ----------

dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID")
CATALOG = dbutils.widgets.get("catalog_name")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
print(f"Catalog: {CATALOG}, Warehouse: {WAREHOUSE_ID}")

# COMMAND ----------

import requests, json, hashlib, time

host = spark.conf.get("spark.databricks.workspaceUrl", "")
if not host.startswith("http"):
    host = f"https://{host}"

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    headers = w.config.authenticate()
    headers["Content-Type"] = "application/json"
except:
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def md5_id(text):
    return hashlib.md5(text.encode()).hexdigest()

print(f"Connected to: {host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Space Definitions

# COMMAND ----------

# ====================================================================
# GENIE SPACE CONFIGURATIONS -- RAW BASELINE (deliberately minimal)
# These spaces have just table identifiers and minimal instructions.
# Certified queries, column synonyms, and enhanced instructions are
# added progressively by the iteration scripts (02, 03, 04).
# ====================================================================
spaces_config = {
    "SC - Demand Analysis": {
        "description": "Demand Analysis specialist for the Supply Chain Control Tower.",
        "instructions": "You are a Demand Analysis agent. Answer questions about sales, revenue, and demand forecasts.",
        "tables": sorted([
            {"identifier": f"{CATALOG}.demand_analysis.customer_segments"},
            {"identifier": f"{CATALOG}.demand_analysis.demand_forecasts"},
            {"identifier": f"{CATALOG}.demand_analysis.pos_data"},
            {"identifier": f"{CATALOG}.demand_analysis.products"},
            {"identifier": f"{CATALOG}.demand_analysis.promotions"},
            {"identifier": f"{CATALOG}.demand_analysis.sales_orders"},
        ], key=lambda t: t["identifier"])
    },
    "SC - Inventory Management": {
        "description": "Inventory Management specialist for the Supply Chain Control Tower.",
        "instructions": "You are an Inventory Management agent. Answer questions about inventory levels, stockouts, and warehouse operations.",
        "tables": sorted([
            {"identifier": f"{CATALOG}.inventory_management.inventory_ledger"},
            {"identifier": f"{CATALOG}.inventory_management.stock_movements"},
            {"identifier": f"{CATALOG}.inventory_management.store_inventory"},
            {"identifier": f"{CATALOG}.inventory_management.warehouse_data"},
        ], key=lambda t: t["identifier"])
    },
    "SC - Logistics Operations": {
        "description": "Logistics Operations specialist for the Supply Chain Control Tower.",
        "instructions": "You are a Logistics Operations agent. Answer questions about shipments, deliveries, and transit performance.",
        "tables": sorted([
            {"identifier": f"{CATALOG}.logistics_operations.carriers"},
            {"identifier": f"{CATALOG}.logistics_operations.distribution_centers"},
            {"identifier": f"{CATALOG}.logistics_operations.shipments"},
            {"identifier": f"{CATALOG}.logistics_operations.transit_data"},
        ], key=lambda t: t["identifier"])
    },
    "SC - Supplier Risk": {
        "description": "Supplier Risk specialist for the Supply Chain Control Tower.",
        "instructions": "You are a Supplier Risk agent. Answer questions about supplier performance, lead times, and procurement.",
        "tables": sorted([
            {"identifier": f"{CATALOG}.supplier_procurement.procurement_data"},
            {"identifier": f"{CATALOG}.supplier_procurement.supplier_lead_times"},
            {"identifier": f"{CATALOG}.supplier_procurement.supplier_orders"},
            {"identifier": f"{CATALOG}.supplier_procurement.suppliers"},
            {"identifier": f"{CATALOG}.supplier_procurement.vendor_slas"},
        ], key=lambda t: t["identifier"])
    },
    "SC - Executive Reporting": {
        "description": "Executive Reporting specialist for the Supply Chain Control Tower.",
        "instructions": "You are an Executive Reporting agent. Answer questions about overall KPI performance and regional comparisons.",
        "tables": sorted([
            {"identifier": f"{CATALOG}.reporting.executive_kpis"},
            {"identifier": f"{CATALOG}.reporting.regional_performance_summary"},
            {"identifier": f"{CATALOG}.reporting.revenue_trend"},
            {"identifier": f"{CATALOG}.reporting.supply_chain_risk_scorecard"},
        ], key=lambda t: t["identifier"])
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Genie Spaces

# COMMAND ----------

space_ids = {}

for space_name, config in spaces_config.items():
    # Create the space
    create_resp = requests.post(
        f"{host}/api/2.0/genie/spaces",
        headers=headers,
        json={
            "title": space_name,
            "description": config["description"],
            "warehouse_id": WAREHOUSE_ID,
            "table_identifiers": [t["identifier"] for t in config["tables"]],
            "serialized_space": json.dumps({"version": 2})
        }
    )
    if create_resp.status_code in (200, 201):
        space_id = create_resp.json().get("space_id") or create_resp.json().get("id")
        space_ids[space_name] = space_id
        print(f"✓ Created: {space_name} ({space_id})")
    else:
        print(f"✗ Failed to create {space_name}: {create_resp.status_code} - {create_resp.text[:200]}")
        continue

    # Patch instructions + tables. The Genie API has a race condition: if we
    # PATCH too soon after CREATE, the serialized_space may not have tables yet.
    # We retry up to 3 times with increasing delays to work around this.
    serialized = {
        "version": 2,
        "instructions": {
            "text_instructions": [{"content": [config["instructions"]]}]
        },
        "data_sources": {
            "tables": config["tables"]
        }
    }

    for attempt in range(3):
        time.sleep(2 + attempt)  # 2s, 3s, 4s
        get_resp = requests.get(
            f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true",
            headers=headers
        )
        etag = get_resp.json().get("etag", "")
        patch_headers = dict(headers)
        if etag:
            patch_headers["If-Match"] = etag
        patch_resp = requests.patch(
            f"{host}/api/2.0/genie/spaces/{space_id}",
            headers=patch_headers,
            json={"serialized_space": json.dumps(serialized)}
        )
        if patch_resp.status_code != 200:
            print(f"  ✗ PATCH attempt {attempt+1} failed: {patch_resp.status_code}")
            continue
        # Verify tables persisted
        verify = requests.get(f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true", headers=headers)
        vss = json.loads(verify.json().get("serialized_space", "{}"))
        if len(vss.get("data_sources", {}).get("tables", [])) > 0:
            print(f"  ✓ Instructions + {len(config['tables'])} tables configured")
            break
        print(f"  ⚠ Tables lost after PATCH, retrying...")
    else:
        print(f"  ✗ Tables failed to persist after 3 attempts")

print(f"\n✓ All Genie Spaces created: {json.dumps(space_ids, indent=2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Supervisor Agent

# COMMAND ----------

# ====================================================================
# CREATE EVALUATOR SPACE + GROUND TRUTH TABLE
# ====================================================================
print("\n--- Creating Evaluator Space and Ground Truth Table ---")

# Create ground_truth_kpis table from live data
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.reporting")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.reporting.ground_truth_kpis")
spark.sql(f"""
CREATE TABLE {CATALOG}.reporting.ground_truth_kpis AS
WITH demand AS (
  SELECT ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
                          AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END)
       - SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2))
                          AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)) AS western_rev_change
  FROM {CATALOG}.demand_analysis.sales_orders WHERE region = 'Western'
),
inventory AS (
  SELECT COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS western_stockouts
  FROM {CATALOG}.inventory_management.inventory_ledger WHERE region = 'Western'
),
inv_safety AS (
  SELECT COUNT(CASE WHEN below_safety_stock_flag = true THEN 1 END) AS western_below_safety
  FROM {CATALOG}.inventory_management.inventory_ledger WHERE region = 'Western'
),
logistics AS (
  SELECT ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS western_late_pct,
         ROUND(AVG(CASE WHEN is_late THEN delay_days ELSE NULL END), 1) AS western_avg_delay
  FROM {CATALOG}.logistics_operations.shipments
  WHERE ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND ship_date < DATE_TRUNC('month', DATE '2026-09-01') AND destination_region = 'Western'
),
supplier AS (
  SELECT ROUND(AVG(CASE WHEN supplier_continent = 'Asia' AND is_late THEN 1.0
                        WHEN supplier_continent = 'Asia' THEN 0.0 END) * 100, 1) AS asia_late_pct,
         ROUND(AVG(CASE WHEN supplier_continent = 'Asia' THEN lead_time_variance_days END), 1) AS asia_avg_variance
  FROM {CATALOG}.supplier_procurement.supplier_orders
  WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
),
exec_kpis AS (
  SELECT service_level_pct, supplier_late_pct_last_month, total_stockout_skus
  FROM {CATALOG}.reporting.executive_kpis
)
SELECT 'demand-analysis' AS agent, 'Western revenue change (Aug 2026 vs Jul 2026 MoM)' AS metric, CAST(western_rev_change AS STRING) AS ground_truth_value FROM demand
UNION ALL SELECT 'inventory-management', 'Western stockout SKUs', CAST(western_stockouts AS STRING) FROM inventory
UNION ALL SELECT 'inventory-management', 'Western below safety stock', CAST(western_below_safety AS STRING) FROM inv_safety
UNION ALL SELECT 'logistics-operations', 'Western late delivery rate (Aug 2026)', CAST(western_late_pct AS STRING) FROM logistics
UNION ALL SELECT 'logistics-operations', 'Western avg delay days (Aug 2026)', CAST(western_avg_delay AS STRING) FROM logistics
UNION ALL SELECT 'supplier-risk', 'Asia supplier late rate (Aug 2026)', CAST(asia_late_pct AS STRING) FROM supplier
UNION ALL SELECT 'supplier-risk', 'Asia avg lead time variance (Aug 2026)', CAST(asia_avg_variance AS STRING) FROM supplier
UNION ALL SELECT 'executive-reporting', 'Service level pct', CAST(service_level_pct AS STRING) FROM exec_kpis
UNION ALL SELECT 'executive-reporting', 'Supplier late pct (Aug 2026)', CAST(supplier_late_pct_last_month AS STRING) FROM exec_kpis
UNION ALL SELECT 'executive-reporting', 'Total stockout SKUs', CAST(total_stockout_skus AS STRING) FROM exec_kpis
""")
print(f"✓ Ground truth table created: {CATALOG}.reporting.ground_truth_kpis")
spark.sql(f"SELECT * FROM {CATALOG}.reporting.ground_truth_kpis ORDER BY agent, metric").display()

# Create Evaluator Genie Space
evaluator_config = {
    "title": "SC - Evaluator",
    "description": "Evaluator Agent with ground truth KPIs for validation.",
    "warehouse_id": WAREHOUSE_ID,
    "table_identifiers": [f"{CATALOG}.reporting.ground_truth_kpis"],
    "serialized_space": json.dumps({"version": 2})
}
create_resp = requests.post(f"{host}/api/2.0/genie/spaces", headers=headers, json=evaluator_config)
if create_resp.status_code in (200, 201):
    eval_space_id = create_resp.json().get("space_id") or create_resp.json().get("id")
    space_ids["SC - Evaluator"] = eval_space_id
    print(f"✓ Created: SC - Evaluator ({eval_space_id})")
    
    # Define evaluator instructions and serialized space config
    eval_instructions = """You are the Evaluator Agent. Your ONLY job is to return ground truth KPIs from the ground_truth_kpis table so the Supervisor can compare its findings against verified correct numbers.

RULES:
1. When asked for ground truth, ALWAYS run: SELECT * FROM ground_truth_kpis ORDER BY agent, metric
2. Return the full table -- do NOT filter or summarize
3. The 'ground_truth_value' column has the exact correct number for each metric
4. The 'agent' column shows which sub-agent should have produced that number
5. The 'metric' column describes what was measured"""

    eval_ss = {
        "version": 2,
        "instructions": {
            "text_instructions": [{"content": [eval_instructions]}],
            "example_question_sqls": sorted([
                {"id": md5_id("ground truth KPIs"), "question": ["ground truth KPIs"],
                 "sql": [f"SELECT * FROM {CATALOG}.reporting.ground_truth_kpis ORDER BY agent, metric\n"]},
                {"id": md5_id("show all ground truth values"), "question": ["show all ground truth values"],
                 "sql": [f"SELECT * FROM {CATALOG}.reporting.ground_truth_kpis ORDER BY agent, metric\n"]},
                {"id": md5_id("what are the correct numbers"), "question": ["what are the correct numbers"],
                 "sql": [f"SELECT * FROM {CATALOG}.reporting.ground_truth_kpis ORDER BY agent, metric\n"]},
            ], key=lambda x: x["id"])
        },
        "data_sources": {
            "tables": [{"identifier": f"{CATALOG}.reporting.ground_truth_kpis"}]
        }
    }

    # Retry PATCH with etag to handle race condition (same pattern as domain spaces)
    for attempt in range(3):
        time.sleep(2 + attempt)
        get_resp = requests.get(f"{host}/api/2.0/genie/spaces/{eval_space_id}?include_serialized_space=true", headers=headers)
        etag = get_resp.json().get("etag", "")
        ph = dict(headers)
        if etag: ph["If-Match"] = etag
        resp = requests.patch(f"{host}/api/2.0/genie/spaces/{eval_space_id}", headers=ph, json={"serialized_space": json.dumps(eval_ss)})
        if resp.status_code == 200:
            verify = requests.get(f"{host}/api/2.0/genie/spaces/{eval_space_id}?include_serialized_space=true", headers=headers)
            vss = json.loads(verify.json().get("serialized_space", "{}"))
            if len(vss.get("data_sources", {}).get("tables", [])) > 0:
                print(f"  ✓ Evaluator instructions + certified queries + 1 table configured")
                break
            print(f"  ⚠ Evaluator table lost, retrying...")
    else:
        print(f"  ⚠ Evaluator table may not have persisted; certified queries use fully-qualified name")
else:
    print(f"✗ Failed to create Evaluator: {create_resp.status_code} - {create_resp.text[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Supervisor Agent

# COMMAND ----------

# ====================================================================
# CREATE SUPERVISOR AGENT -- RAW BASELINE (minimal instructions)
# ====================================================================
supervisor_payload = {
    "display_name": "Supply Chain Control Tower",
    "description": "Multi-agent supply chain analysis system that coordinates demand, inventory, logistics, supplier, and executive reporting agents.",
    "instructions": """You are a Supply Chain Control Tower Supervisor Agent. You coordinate multiple specialist agents to answer supply chain questions.

Available agents:
- demand-analysis: Revenue and sales data
- inventory-management: Inventory and stockout data
- logistics-operations: Shipment and delivery data
- supplier-risk: Supplier performance data
- executive-reporting: KPI summaries and dashboards
- evaluator: Ground truth validation data

When answering questions:
1. Determine which agents to query
2. Ask each agent relevant questions
3. Synthesize findings into a comprehensive answer
4. Call the evaluator last to validate your findings"""
}

create_resp = requests.post(
    f"{host}/api/2.1/supervisor-agents",
    headers=headers,
    json=supervisor_payload
)

if create_resp.status_code in (200, 201):
    supervisor_data = create_resp.json()
    supervisor_name = supervisor_data.get("name", "")
    print(f"✓ Supervisor Agent created: {supervisor_name}")
    print(f"  Display name: {supervisor_data.get('display_name')}")
else:
    print(f"✗ Failed: {create_resp.status_code} - {create_resp.text[:300]}")
    dbutils.notebook.exit(f"Supervisor creation failed: {create_resp.status_code}")

# COMMAND ----------

# ====================================================================
# ADD SUB-AGENT TOOLS
# ====================================================================
tool_configs = {
    "demand-analysis": {
        "description": "Demand Analysis agent. Answers questions about sales, revenue, and demand forecasts.",
        "space_key": "SC - Demand Analysis"
    },
    "inventory-management": {
        "description": "Inventory Management agent. Answers questions about inventory levels, stockouts, and warehouse data.",
        "space_key": "SC - Inventory Management"
    },
    "logistics-operations": {
        "description": "Logistics Operations agent. Answers questions about shipments, deliveries, and transit data.",
        "space_key": "SC - Logistics Operations"
    },
    "supplier-risk": {
        "description": "Supplier Risk agent. Answers questions about supplier performance, lead times, and procurement.",
        "space_key": "SC - Supplier Risk"
    },
    "executive-reporting": {
        "description": "Executive Reporting agent. Answers questions about KPIs, regional performance, and dashboards.",
        "space_key": "SC - Executive Reporting"
    },
    "evaluator": {
        "description": "Evaluator agent. Returns ground truth KPI values for validation. Call this LAST after all other agents. Ask: 'Show all ground truth values'.",
        "space_key": "SC - Evaluator"
    },
}

for tool_id, config in tool_configs.items():
    genie_space_id = space_ids.get(config["space_key"])
    if not genie_space_id:
        print(f"✗ Skipping {tool_id}: Genie Space not found")
        continue

    resp = requests.post(
        f"{host}/api/2.1/{supervisor_name}/tools?tool_id={tool_id}",
        headers=headers,
        json={
            "tool_type": "genie_space",
            "description": config["description"],
            "genie_space": {"id": genie_space_id}
        }
    )
    if resp.status_code in (200, 201):
        print(f"✓ Tool added: {tool_id} -> {genie_space_id}")
    else:
        print(f"✗ Tool {tool_id} failed: {resp.status_code} - {resp.text[:200]}")

# COMMAND ----------

# ====================================================================
# ADD QUALITY EXAMPLES
# ====================================================================
quality_examples = [
    {
        "request": "Why did revenue drop in the Western Region last month?",
        "expected_response": "Consult demand-analysis for revenue comparison (last month vs prior month by region), inventory-management for stockout impact, and logistics-operations for fulfillment delays. Quantify the dollar impact and identify which product families drove the decline."
    },
    {
        "request": "Are we going to miss our quarterly service-level targets?",
        "expected_response": "Consult executive-reporting for current service_level_pct and late_delivery_pct_last_month, demand-analysis for order fulfillment rates, and logistics-operations for delivery trends. Compare current trajectory against targets and project end-of-quarter outcomes."
    },
    {
        "request": "Which suppliers are causing the most problems right now?",
        "expected_response": "Consult supplier-risk for lead time variances, SLA breaches, and fill rates. Cross-reference with logistics-operations for inbound delay impacts and inventory-management for resulting stockouts. Rank suppliers by composite risk score."
    },
    {
        "request": "What is the current inventory health across all regions?",
        "expected_response": "Consult inventory-management for stockout counts, days of supply, and below-safety-stock SKUs by region. Check demand-analysis for demand patterns driving depletion. Include warehouse utilization and store-level replenishment status."
    },
    {
        "request": "Give me a full executive summary of supply chain performance.",
        "expected_response": "Consult ALL five agents: executive-reporting for KPI dashboard, demand-analysis for revenue trends, inventory-management for stock health, logistics-operations for delivery performance, supplier-risk for procurement risks. Synthesize into headline KPIs, regional breakdown, risk areas, and recommended actions."
    },
]

for i, example in enumerate(quality_examples):
    resp = requests.post(
        f"{host}/api/2.1/{supervisor_name}/examples",
        headers=headers,
        json=example
    )
    status = "✓" if resp.status_code in (200, 201) else "✗"
    print(f"{status} Example {i+1}: {example['request'][:60]}...")

# COMMAND ----------

print("\n" + "="*60)
print("SETUP COMPLETE")
print("="*60)
print(f"Catalog: {CATALOG}")
print(f"Warehouse: {WAREHOUSE_ID}")
print(f"\nGenie Spaces:")
for name, sid in space_ids.items():
    print(f"  {name}: {sid}")
print(f"\nSupervisor Agent: {supervisor_name}")
print(f"\nTest prompt:")
print('"Why did revenue drop in the Western Region, are we going to miss our service-level targets, and what actions should we take?"')