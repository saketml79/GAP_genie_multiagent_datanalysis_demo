# Databricks notebook source
# MAGIC %md
# MAGIC # Step 8: Create Genie Spaces, Evaluator, and Supervisor Agent
# MAGIC Creates 5 domain Genie Spaces with **minimal baseline** instructions (no certified queries,
# MAGIC no column synonyms, no hardened prompts). Also creates the Evaluator Space with ground truth
# MAGIC KPIs (for **external** scoring only — NOT a supervisor tool), and a Supervisor Agent that orchestrates the 5 domain agents.
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

# DBTITLE 1,Define Genie Agent Spaces (Baseline — tables + lean instructions only)
# ====================================================================
# GENIE SPACE CONFIGURATIONS -- BASELINE
# Tables + proper instructions describing domain, tables, key columns,
# and how to handle out-of-scope questions. NO certified queries,
# NO column synonyms, NO metric views — those come in iterations.
# ====================================================================
spaces_config = {
    "SC - Demand Analysis": {
        "description": "Demand Analysis specialist for the Supply Chain Control Tower.",
        "instructions": (
            "You are a Demand Analysis agent for a retail supply chain. "
            "You answer questions about sales orders, revenue, demand forecasts, "
            "product performance, and customer segmentation.\n\n"
            "TABLES:\n"
            "- sales_orders: order_id, order_date, region, state, product_family, product_category, "
            "sku_id, customer_id, channel, quantity, unit_price, total_amount, order_status, "
            "fulfillment_warehouse.\n"
            "- demand_forecasts: Forecasted demand by SKU and period.\n"
            "- products: Product master data.\n"
            "- customer_segments: Customer segmentation.\n"
            "- promotions: Promotional campaigns.\n"
            "- pos_data: Point-of-sale transactions.\n\n"
            "If you cannot answer a question from the tables available to you, say so."
        ),
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
        "instructions": (
            "You are an Inventory Management agent for a retail supply chain. "
            "You answer questions about inventory levels, safety stock, stockouts, "
            "warehouse capacity, and stock movements.\n\n"
            "TABLES:\n"
            "- inventory_ledger: ledger_id, sku_id, warehouse_id, region, on_hand_qty, available_qty, "
            "allocated_qty, safety_stock_level, reorder_point, below_safety_stock_flag (BOOLEAN), "
            "stockout_flag (BOOLEAN), days_of_supply, snapshot_date, product_family, product_category.\n"
            "- stock_movements: Inbound/outbound stock movement transactions.\n"
            "- store_inventory: Store-level inventory positions.\n"
            "- warehouse_data: Warehouse master data.\n\n"
            "If you cannot answer a question from the tables available to you, say so."
        ),
        "tables": sorted([
            {"identifier": f"{CATALOG}.inventory_management.inventory_ledger"},
            {"identifier": f"{CATALOG}.inventory_management.stock_movements"},
            {"identifier": f"{CATALOG}.inventory_management.store_inventory"},
            {"identifier": f"{CATALOG}.inventory_management.warehouse_data"},
        ], key=lambda t: t["identifier"])
    },
    "SC - Logistics Operations": {
        "description": "Logistics Operations specialist for the Supply Chain Control Tower.",
        "instructions": (
            "You are a Logistics Operations agent for a retail supply chain. "
            "You answer questions about shipment performance, delivery timeliness, "
            "freight costs, carriers, and distribution centers.\n\n"
            "TABLES:\n"
            "- shipments: shipment_id, ship_date, destination_region, destination_state, origin_region, "
            "origin_warehouse, carrier_id, is_late (BOOLEAN), delay_days (INTEGER), "
            "delay_reason, shipping_cost (DOUBLE), actual_delivery_date, "
            "expected_delivery_date, actual_transit_days, planned_transit_days, shipment_status, "
            "shipment_type, total_weight_kg, total_pallets, order_count.\n"
            "- carriers: Carrier master data.\n"
            "- distribution_centers: DC locations and capacity.\n"
            "- transit_data: Detailed transit leg data.\n\n"
            "If you cannot answer a question from the tables available to you, say so."
        ),
        "tables": sorted([
            {"identifier": f"{CATALOG}.logistics_operations.carriers"},
            {"identifier": f"{CATALOG}.logistics_operations.distribution_centers"},
            {"identifier": f"{CATALOG}.logistics_operations.shipments"},
            {"identifier": f"{CATALOG}.logistics_operations.transit_data"},
        ], key=lambda t: t["identifier"])
    },
    "SC - Supplier Risk": {
        "description": "Supplier Risk specialist for the Supply Chain Control Tower.",
        "instructions": (
            "You are a Supplier Risk agent for a retail supply chain. "
            "You answer questions about supplier performance, purchase orders, "
            "lead times, quality, and SLA compliance.\n\n"
            "TABLES:\n"
            "- supplier_orders: po_id, order_date, supplier_id, supplier_name, supplier_continent, "
            "supplier_country, is_late (BOOLEAN), lead_time_variance_days (INTEGER), "
            "actual_lead_time_days, contracted_lead_time_days, total_cost, unit_cost, "
            "quantity_ordered, quantity_received, quality_score, product_family, po_status, "
            "delay_reason, expected_delivery_date, actual_delivery_date.\n"
            "- supplier_lead_times: Monthly aggregated supplier performance summaries per supplier.\n"
            "- vendor_slas: sla_id, supplier_id, supplier_name, sla_metric, target_pct, actual_pct, "
            "variance_pct, is_breached (BOOLEAN), penalty_amount (DOUBLE), review_period, last_updated.\n"
            "- suppliers: Supplier master data.\n"
            "- procurement_data: Procurement transaction details.\n\n"
            "If you cannot answer a question from the tables available to you, say so."
        ),
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
        "instructions": (
            "You are an Executive Reporting agent for a retail supply chain. "
            "You answer questions about company-wide KPIs, regional performance, "
            "revenue trends, and risk scorecards.\n\n"
            "TABLES:\n"
            "- executive_kpis: revenue_last_month, revenue_prior_month, total_stockout_skus, "
            "avg_days_of_supply, late_delivery_pct_last_month, avg_delay_days_last_month, "
            "supplier_late_pct_last_month, total_sla_breaches, service_level_pct.\n"
            "- regional_performance_summary: region, total_orders, avg_order_value, total_revenue, "
            "fulfillment_rate, stockout_skus, late_shipment_pct.\n"
            "- revenue_trend: Historical revenue data across periods.\n"
            "- supply_chain_risk_scorecard: Risk scores by dimension.\n\n"
            "If you cannot answer a question from the tables available to you, say so."
        ),
        "tables": sorted([
            {"identifier": f"{CATALOG}.reporting.executive_kpis"},
            {"identifier": f"{CATALOG}.reporting.regional_performance_summary"},
            {"identifier": f"{CATALOG}.reporting.revenue_trend"},
            {"identifier": f"{CATALOG}.reporting.supply_chain_risk_scorecard"},
        ], key=lambda t: t["identifier"])
    },
}

# Append provenance requirement to ALL agent instructions.
# This makes agents self-document their reasoning: which tables/columns they used,
# and whether they assumed any thresholds not defined in the data.
# The [PROVENANCE:] tag is parsed by parse_provenance() in 00_run_all for classification.
PROVENANCE_SUFFIX = (
    "\n\n"
    "PROVENANCE REQUIREMENT: After every analytical answer, append exactly one line:\n"
    "[PROVENANCE: tables={table_names}, key_columns={column_names}, "
    "assumed_thresholds={none_or_values}, method={metric_view|base_table|computed|assumed_definition}]\n"
    "If you assumed any threshold or business definition not found in the data, say so explicitly."
)
for _space_name in spaces_config:
    spaces_config[_space_name]["instructions"] += PROVENANCE_SUFFIX
print(f"Provenance requirement appended to {len(spaces_config)} agent instructions")

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

# DBTITLE 1,Create Evaluator Space + Ground Truth Table (external scoring only)
# ====================================================================
# CREATE EVALUATOR SPACE + GROUND TRUTH TABLE
# ====================================================================
print("\n--- Creating Evaluator Space and Ground Truth Table ---")

# Create ground_truth_kpis table from live data
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.reporting")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.reporting.ground_truth_kpis")
# Ground truth: 9 prompt-aligned metrics — ALL computed dynamically from live data
# Each metric maps to a business term in MAIN_PROMPT
# uc_feature_needed documents which UC feature is required for agents to find it
spark.sql(f"""
CREATE TABLE {CATALOG}.reporting.ground_truth_kpis (
  agent STRING COMMENT 'Which Genie sub-agent should answer this',
  metric STRING COMMENT 'Business metric name as referenced in the prompt',
  ground_truth_value STRING COMMENT 'Correct value computed from source tables',
  reference_sql STRING COMMENT 'The exact SQL that produces the correct value',
  uc_feature_needed STRING COMMENT 'UC semantic feature required to answer correctly',
  calculated_at TIMESTAMP COMMENT 'When this ground truth was last computed'
) USING DELTA
""")
# Use INSERT ... SELECT with UNION ALL to avoid scalar subqueries in VALUES clause
spark.sql(f"""
INSERT INTO {CATALOG}.reporting.ground_truth_kpis (agent, metric, ground_truth_value, uc_feature_needed, calculated_at)

SELECT 'demand-analysis', 'Western revenue MoM change (Aug vs Jul 2026)',
  CAST(ROUND(
    SUM(CASE WHEN order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01' THEN total_amount ELSE 0 END)
  - SUM(CASE WHEN order_date >= DATE '2026-07-01' AND order_date < DATE '2026-08-01' THEN total_amount ELSE 0 END), 2) AS STRING),
  'synonym: revenue to total_amount + certified query for MoM calc', CURRENT_TIMESTAMP()
FROM {CATALOG}.demand_analysis.sales_orders WHERE region = 'Western'

UNION ALL
SELECT 'logistics-operations', 'Western on-time delivery rate (Aug 2026)',
  CAST(ROUND(AVG(CASE WHEN is_late = false THEN 1.0 ELSE 0.0 END) * 100, 1) AS STRING),
  'synonym: OTD to is_late (inverse) + metric view with pre-computed OTD', CURRENT_TIMESTAMP()
FROM {CATALOG}.logistics_operations.shipments
WHERE ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'
  AND destination_region = 'Western'

UNION ALL
SELECT 'executive-reporting', 'Fill rate',
  CAST(service_level_pct AS STRING),
  'synonym: fill_rate to service_level_pct', CURRENT_TIMESTAMP()
FROM {CATALOG}.reporting.executive_kpis

UNION ALL
SELECT 'inventory-management', 'Western below safety stock positions',
  CAST(COUNT(*) AS STRING),
  'column comment: per SKU-warehouse position not per SKU', CURRENT_TIMESTAMP()
FROM {CATALOG}.inventory_management.inventory_ledger
WHERE region = 'Western' AND below_safety_stock_flag = true

UNION ALL
SELECT 'inventory-management', 'Western stockout SKUs',
  CAST(COUNT(DISTINCT sku_id) AS STRING),
  'column comment: COUNT DISTINCT sku_id WHERE stockout_flag', CURRENT_TIMESTAMP()
FROM {CATALOG}.inventory_management.inventory_ledger
WHERE region = 'Western' AND stockout_flag = true

UNION ALL
SELECT 'supplier-risk', 'Total vendor SLA penalties (Aug 2026)',
  CAST(ROUND(SUM(penalty_amount), 1) AS STRING),
  'synonym: vendor to supplier + penalty_amount in vendor_slas', CURRENT_TIMESTAMP()
FROM {CATALOG}.supplier_procurement.vendor_slas
WHERE is_breached = true

UNION ALL
SELECT 'supplier-risk', 'Vendor late delivery pct (Aug 2026)',
  CAST(ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS STRING),
  'synonym: vendor to supplier', CURRENT_TIMESTAMP()
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'

UNION ALL
SELECT 'logistics-operations', 'Western avg delay days (Aug 2026)',
  CAST(ROUND(AVG(CASE WHEN is_late THEN delay_days END), 1) AS STRING),
  'direct: delay_days column is self-documenting', CURRENT_TIMESTAMP()
FROM {CATALOG}.logistics_operations.shipments
WHERE ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'
  AND destination_region = 'Western'

UNION ALL
SELECT 'executive-reporting', 'Western Cost of Disruption',
  CAST(ROUND(lr.cancelled_revenue + lr.backordered_at_risk_revenue
             + lw.wasted_logistics_spend
             + tp.total_sla_penalties * ls.pct_of_late_shipments, 2) AS STRING),
  'metric view: cross-domain join only available after Iter 4', CURRENT_TIMESTAMP()
FROM (
  SELECT ROUND(SUM(CASE WHEN order_status = 'Cancelled' THEN total_amount ELSE 0 END), 2) AS cancelled_revenue,
         ROUND(SUM(CASE WHEN order_status = 'Backordered' THEN total_amount ELSE 0 END), 2) AS backordered_at_risk_revenue
  FROM {CATALOG}.demand_analysis.sales_orders
  WHERE region = 'Western' AND order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'
) lr
CROSS JOIN (
  SELECT ROUND(SUM(CASE WHEN is_late THEN shipping_cost ELSE 0 END), 2) AS wasted_logistics_spend
  FROM {CATALOG}.logistics_operations.shipments
  WHERE destination_region = 'Western' AND ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'
) lw
CROSS JOIN (
  SELECT ROUND(SUM(penalty_amount), 2) AS total_sla_penalties
  FROM {CATALOG}.supplier_procurement.vendor_slas WHERE is_breached = true
) tp
CROSS JOIN (
  SELECT COUNT(CASE WHEN is_late AND destination_region = 'Western' THEN 1 END) * 1.0
         / COUNT(CASE WHEN is_late THEN 1 END) AS pct_of_late_shipments
  FROM {CATALOG}.logistics_operations.shipments
  WHERE ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'
) ls
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
    eval_instructions = """You are the Evaluator Agent. Your job is to COMPARE the Supervisor's reported findings against the ground_truth_kpis table.

RULES:
1. When the Supervisor sends you its findings, query the ground_truth_kpis table
2. For EACH metric in ground_truth_kpis, check if the Supervisor found a matching value:
   - EXACT: value within 2% of ground truth
   - CLOSE: value within 10%
   - MISS: value found but differs by more than 10%
   - NOT_FOUND: the Supervisor did not report this metric at all
3. Report: 'X of Y metrics were EXACT, Z NOT_FOUND' with per-metric detail
4. Do NOT simply return raw ground truth values without comparison
5. The ground truth values are SECRET benchmarks for validation only"""

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

# DBTITLE 1,Create Supervisor Agent (no evaluator tool)
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

When answering questions:
1. Determine which agents to query
2. Ask each agent relevant questions
3. Synthesize findings into a comprehensive answer
4. Include the SQL each agent used so the user can verify"""
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

# DBTITLE 1,Add Sub-Agent Tools (5 domain agents, no evaluator)
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
    # NOTE: Evaluator is NOT added as a supervisor tool.
    # It is called externally by the Python scorer in 00_run_all.py.
    # This prevents the supervisor from accessing ground truth values.
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