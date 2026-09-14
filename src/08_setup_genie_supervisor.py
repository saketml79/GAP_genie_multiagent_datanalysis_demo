# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Step 8: Create Genie Spaces and Supervisor Agent
# MAGIC Creates 5 domain Genie Spaces (with instructions, table descriptions, column synonyms),
# MAGIC then a Supervisor Agent that orchestrates all 5 as sub-agents.

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
# GENIE SPACE CONFIGURATIONS
# ====================================================================
spaces_config = {
    "SC - Demand Analysis": {
        "description": "Demand Analysis specialist for the Supply Chain Control Tower.",
        "instructions": """You are a Demand Analysis specialist for a retail supply chain.

KEY METRICS:
- Revenue by region, product family, time period: SUM(total_amount) from sales_orders
- Forecast accuracy: forecast_accuracy_pct and bias in demand_forecasts
- Lost sales: SUM(total_amount) WHERE order_status IN ('Backordered','Cancelled')
- Customer patterns: GROUP BY segment, region from customer_segments joined to sales_orders
- Promotion lift: actual_lift_pct vs estimated_lift_pct in promotions

ANALYSIS RULES:
1. Compare last 30 days vs prior 30 days using DATEDIFF(CURRENT_DATE(), order_date)
2. Always break down by region AND product_family
3. Identify which customer segments are affected
4. Check demand_forecasts for forecast accuracy in impacted areas
5. Quantify dollar impact with exact SUM(total_amount)

EVIDENCE REQUIREMENTS: exact SQL, row counts, dollar amounts, % changes, confidence level.
Regions: Western, Eastern, Central, Southern
Product Families: Apparel, Accessories, Footwear, Home Goods, Electronics
Statuses: Fulfilled, Partially_Fulfilled, Backordered, Cancelled
Channels: Online, Retail, Wholesale, Marketplace""",
        "tables": [
            {"identifier": f"{CATALOG}.demand_analysis.customer_segments", "description": ["Customer master: segment (Premium/Standard/Value/Wholesale), region, revenue tier, loyalty."]},
            {"identifier": f"{CATALOG}.demand_analysis.demand_forecasts", "description": ["Monthly demand forecasts vs actuals by region and product family. Accuracy, bias. 6 months."]},
            {"identifier": f"{CATALOG}.demand_analysis.pos_data", "description": ["Point-of-sale transactions from retail stores. Daily sales with store, SKU, revenue. 60 days."]},
            {"identifier": f"{CATALOG}.demand_analysis.products", "description": ["Product catalog: SKU details, product families, categories, costs and prices."]},
            {"identifier": f"{CATALOG}.demand_analysis.promotions", "description": ["Promotional campaigns with discount details, target regions, budget, actual vs estimated lift."]},
            {"identifier": f"{CATALOG}.demand_analysis.sales_orders", "description": ["Sales order transactions. Key: order_date, region, product_family, total_amount, order_status. 90 days."]},
        ]
    },
    "SC - Inventory Management": {
        "description": "Inventory Management specialist for the Supply Chain Control Tower.",
        "instructions": """You are an Inventory Management specialist.

KEY METRICS:
- Stockout count: COUNT(*) WHERE stockout_flag = true in inventory_ledger
- Days of supply: days_of_supply column in inventory_ledger
- Safety stock breaches: COUNT(*) WHERE below_safety_stock_flag = true
- Warehouse utilization: current_utilization_pct in warehouse_data
- Net flow: SUM(quantity) grouped by movement_type in stock_movements
- Store health: COUNT by replenishment_status (Stockout/Low/Adequate)

ANALYSIS RULES:
1. Check stockout_flag and below_safety_stock_flag first
2. Compare on_hand_qty vs safety_stock_level vs reorder_point
3. stock_movements: positive quantity = inbound, negative = outbound
4. Break down by region and product_family

EVIDENCE REQUIREMENTS: stockout counts, affected SKU IDs, on_hand vs safety_stock, days_of_supply, confidence level.
Warehouse mapping: WH-001 to WH-003=Western, WH-004-006=Eastern, WH-007-009=Central, WH-010-012=Southern""",
        "tables": [
            {"identifier": f"{CATALOG}.inventory_management.inventory_ledger", "description": ["Inventory per warehouse per SKU. on_hand_qty, safety_stock, days_of_supply, stockout_flag."]},
            {"identifier": f"{CATALOG}.inventory_management.stock_movements", "description": ["Inbound/outbound movements. Positive qty=inbound, negative=outbound. 60 days."]},
            {"identifier": f"{CATALOG}.inventory_management.store_inventory", "description": ["Store-level inventory with replenishment_status per SKU."]},
            {"identifier": f"{CATALOG}.inventory_management.warehouse_data", "description": ["Warehouse/DC master: capacity, utilization, region."]},
        ]
    },
    "SC - Logistics Operations": {
        "description": "Logistics Operations specialist for the Supply Chain Control Tower.",
        "instructions": """You are a Logistics Operations specialist.

CRITICAL: shipments table has NO 'region' column. It has:
- destination_region: where shipment is going (use for delivery analysis)
- origin_region: where shipment came from
When asked about a region, use destination_region unless explicitly asked about origin.

KEY METRICS:
- Late rate: COUNT(CASE WHEN is_late THEN 1 END) / COUNT(*)
- Avg delay: AVG(delay_days) WHERE is_late = true
- Delay reasons: GROUP BY delay_reason with counts
- Carrier performance: JOIN shipments to carriers on carrier_id
- DC congestion: current_load_pct in distribution_centers (>85% = congested)

ANALYSIS RULES:
1. ALWAYS use destination_region (NOT region) when filtering shipments
2. Compare last 30 vs prior 30 days
3. Break down delay_reason for late shipments
4. Check distribution_centers current_load_pct

EVIDENCE REQUIREMENTS: late counts/%, avg delay, delay reason breakdown, carrier performance, confidence.
Delay reasons: Port_Congestion, Carrier_Capacity, Weather, Customs_Hold, Labor_Shortage, Warehouse_Congestion""",
        "tables": [
            {"identifier": f"{CATALOG}.logistics_operations.carriers", "description": ["Carrier master: type, on-time rates, cost per mile. Join to shipments on carrier_id."]},
            {"identifier": f"{CATALOG}.logistics_operations.distribution_centers", "description": ["DC details: capacity, current_load_pct, dock doors."]},
            {
                "identifier": f"{CATALOG}.logistics_operations.shipments",
                "description": ["Shipments. Use destination_region (NOT region) to filter by region. is_late=true when delayed. 90 days."],
                "column_configs": [
                    {"column_name": "carrier_id", "synonyms": ["carrier", "shipping company"], "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "delay_days", "synonyms": ["days delayed", "days late"]},
                    {"column_name": "delay_reason", "synonyms": ["cause of delay", "root cause"], "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "destination_region", "synonyms": ["region", "delivery region", "target region"], "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "is_late", "synonyms": ["late", "delayed", "overdue"]},
                    {"column_name": "origin_region", "synonyms": ["source region", "ship from region"], "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "ship_date", "synonyms": ["shipment date", "date shipped"]},
                ]
            },
            {"identifier": f"{CATALOG}.logistics_operations.transit_data", "description": ["Shipment tracking events. Join to shipments on shipment_id."]},
        ]
    },
    "SC - Supplier Risk": {
        "description": "Supplier Risk specialist for the Supply Chain Control Tower.",
        "instructions": """You are a Supplier Risk specialist.

KEY METRICS:
- On-time rate: COUNT(CASE WHEN NOT is_late THEN 1 END) / COUNT(*) per supplier
- Lead time variance: actual_lead_time_days - contracted_lead_time_days
- SLA breaches: COUNT(*) WHERE is_breached = true in vendor_slas
- Fill rate: SUM(quantity_received) / SUM(quantity_ordered)
- Geographic risk: GROUP BY supplier_continent

ANALYSIS RULES:
1. Check supplier_lead_times for most recent 2-3 months
2. Identify suppliers with highest lead_time_variance_days
3. Review vendor_slas WHERE is_breached = true
4. Flag suppliers with risk_tier = 'High'

EVIDENCE REQUIREMENTS: supplier names/IDs, contracted vs actual lead times, SLA breach counts, penalty amounts, fill rate, confidence.
Key risk: SUP-001 TextilePro Asia (China) - severe delays, factory shutdowns.
Asia suppliers show elevated delays.""",
        "tables": [
            {"identifier": f"{CATALOG}.supplier_procurement.procurement_data", "description": ["Monthly procurement spend by product family with budget variance, geographic risk."]},
            {"identifier": f"{CATALOG}.supplier_procurement.supplier_lead_times", "description": ["Monthly lead time trends: contracted vs actual, on-time rates. 12 months."]},
            {"identifier": f"{CATALOG}.supplier_procurement.supplier_orders", "description": ["Purchase orders with lead times. is_late=true when actual exceeds contracted. 120 days."]},
            {"identifier": f"{CATALOG}.supplier_procurement.suppliers", "description": ["12 suppliers. risk_tier, reliability_score, contract values."]},
            {"identifier": f"{CATALOG}.supplier_procurement.vendor_slas", "description": ["SLA compliance: On_Time_Delivery, Quality_Acceptance, Fill_Rate, Response_Time, Documentation_Accuracy."]},
        ]
    },
    "SC - Executive Reporting": {
        "description": "Executive Reporting specialist for the Supply Chain Control Tower.",
        "instructions": """You are an Executive Reporting specialist with cross-domain views.

KEY METRICS (from executive_kpis, single row):
- revenue_last_30d / revenue_prior_30d: revenue trend
- total_stockout_skus: inventory health
- late_delivery_pct_30d: logistics health
- supplier_late_pct_30d / total_sla_breaches: supplier health
- service_level_pct: overall service level

ANALYSIS RULES:
1. Always start with SELECT * FROM executive_kpis
2. Use regional_performance_summary for regional comparison
3. Use revenue_trend with period column for Last_30_Days vs Prior_30_Days
4. Rank suppliers from supply_chain_risk_scorecard by composite_risk_score
5. Structure: Headline KPIs > Regional Breakdown > Risk Areas > Actions

EVIDENCE REQUIREMENTS: all KPI values, regional comparison, revenue change, top 3 risk suppliers, confidence.""",
        "tables": [
            {"identifier": f"{CATALOG}.reporting.executive_kpis", "description": ["Single-row KPI view: revenue, stockouts, delivery, supplier metrics, service level."]},
            {"identifier": f"{CATALOG}.reporting.regional_performance_summary", "description": ["Cross-domain regional view: revenue, inventory, logistics, supplier issues."]},
            {"identifier": f"{CATALOG}.reporting.revenue_trend", "description": ["Daily revenue by region, product family. period: Last_30_Days, Prior_30_Days, Older."]},
            {"identifier": f"{CATALOG}.reporting.supply_chain_risk_scorecard", "description": ["Supplier risk ranking by composite score. Lower = higher risk."]},
        ]
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

    time.sleep(1)

    # Get etag for PATCH
    get_resp = requests.get(
        f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true",
        headers=headers
    )
    etag = get_resp.json().get("etag", "")

    # Build serialized_space with instructions, tables, and column_configs
    serialized = {
        "version": 2,
        "instructions": {
            "text_instructions": [{
                "id": md5_id(config["instructions"][:50]),
                "content": [config["instructions"]]
            }]
        },
        "data_sources": {
            "tables": config["tables"]
        }
    }

    patch_headers = dict(headers)
    if etag:
        patch_headers["If-Match"] = etag

    patch_resp = requests.patch(
        f"{host}/api/2.0/genie/spaces/{space_id}",
        headers=patch_headers,
        json={"serialized_space": json.dumps(serialized)}
    )
    if patch_resp.status_code == 200:
        print(f"  ✓ Instructions + tables configured")
    else:
        print(f"  ✗ PATCH failed: {patch_resp.status_code} - {patch_resp.text[:200]}")

print(f"\n✓ All Genie Spaces created: {json.dumps(space_ids, indent=2)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create Supervisor Agent

# COMMAND ----------
# ====================================================================
# CREATE SUPERVISOR AGENT
# ====================================================================
supervisor_payload = {
    "display_name": "Supply Chain Control Tower",
    "instructions": """You are the Supply Chain Control Tower Supervisor Agent. You coordinate specialist agents to investigate supply chain issues comprehensively.

MANDATORY RESPONSE FORMAT (use these exact section headers):

## 1. What I Understood
Restate the business question in your own words.

## 2. Investigation Plan
List which agents you will consult and what each should answer.

## 3. Findings by Agent
For each agent consulted, report:
- Agent name
- Key finding with exact numbers
- Confidence level (High/Medium/Low)
- Evidence (specific values, counts, percentages)

## 4. Cross-Domain Root Cause Chain
Connect the findings into a causal chain showing how issues in one domain cascade to others.

## 5. Conclusion & Actions
- Direct answer to the business question
- Immediate actions (next 1-2 weeks)
- Medium-term actions (next 1-3 months)
- KPIs to monitor

RULES:
- Always consult ALL relevant agents, not just one
- Include exact numbers from agent responses
- Show how root causes connect across domains
- Provide actionable recommendations with specific targets"""
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
        "description": "Demand Analysis specialist. Query for: revenue trends by region/product family, forecast accuracy, POS sales, customer segment analysis, promotion effectiveness. Covers 90 days of sales orders and 6 months of forecasts.",
        "space_key": "SC - Demand Analysis"
    },
    "inventory-management": {
        "description": "Inventory Management specialist. Query for: stockout counts, days of supply, safety stock breaches, warehouse utilization, stock movement trends, store inventory health.",
        "space_key": "SC - Inventory Management"
    },
    "logistics-operations": {
        "description": "Logistics Operations specialist. Query for: late delivery rates, transit delays and delay reasons, carrier performance, DC congestion. IMPORTANT: specify 'destination region' not 'region'. 90 days of shipments.",
        "space_key": "SC - Logistics Operations"
    },
    "supplier-risk": {
        "description": "Supplier Risk specialist. Query for: supplier on-time delivery, lead time variances, SLA breaches and penalties, fill rates, geographic concentration risk. 12 months of lead time history.",
        "space_key": "SC - Supplier Risk"
    },
    "executive-reporting": {
        "description": "Executive Reporting specialist with cross-domain views. Query for: executive KPIs, regional performance comparison, revenue period-over-period, supply chain risk scorecard.",
        "space_key": "SC - Executive Reporting"
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
        "expected_response": "Consult demand-analysis for revenue comparison (last 30 vs prior 30 days by region), inventory-management for stockout impact, and logistics-operations for fulfillment delays. Quantify the dollar impact and identify which product families drove the decline."
    },
    {
        "request": "Are we going to miss our quarterly service-level targets?",
        "expected_response": "Consult executive-reporting for current service_level_pct and late_delivery_pct_30d, demand-analysis for order fulfillment rates, and logistics-operations for delivery trends. Compare current trajectory against targets and project end-of-quarter outcomes."
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
