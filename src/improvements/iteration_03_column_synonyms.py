# Databricks notebook source
# MAGIC %md
# MAGIC # Iteration 3: Column Synonyms and Entity Matching
# MAGIC
# MAGIC ## Why This Step Improves Output
# MAGIC
# MAGIC Certified queries (Iteration 2) fixed the problem when questions **exactly match** the pattern.
# MAGIC But users don't speak in SQL. They say:
# MAGIC
# MAGIC | User Says | Column Name | Without Synonyms |
# MAGIC | --- | --- | --- |
# MAGIC | "revenue" or "sales" | `total_amount` | Genie looks for a `revenue` column, fails or picks wrong one |
# MAGIC | "region" (in logistics) | `destination_region` | Generates `WHERE region = ...` -> 0 rows |
# MAGIC | "stockout" | `stockout_flag` | May search text columns or generate wrong predicate |
# MAGIC | "late" or "delayed" | `is_late` | May try `WHERE status = 'late'` instead of boolean flag |
# MAGIC | "western" (lowercase) | Value should be "Western" | Case-sensitive match returns 0 rows |
# MAGIC
# MAGIC **The fix**: Column synonyms map business language to actual column names. Entity matching
# MAGIC auto-corrects value formats (e.g., "western" -> "Western").
# MAGIC
# MAGIC ## How It Works (API Detail)
# MAGIC
# MAGIC Synonyms live in `serialized_space.data_sources.tables[].column_configs`. Each entry:
# MAGIC ```json
# MAGIC {
# MAGIC   "column_name": "total_amount",
# MAGIC   "synonyms": ["revenue", "sales", "order value", "dollars"],
# MAGIC   "enable_entity_matching": true,
# MAGIC   "enable_format_assistance": true
# MAGIC }
# MAGIC ```
# MAGIC **Critical**: The `column_configs` array must be sorted alphabetically by `column_name`.
# MAGIC
# MAGIC ## Impact on Consistency
# MAGIC
# MAGIC Synonyms make the system **robust to paraphrasing**. The same underlying certified query
# MAGIC gets triggered regardless of whether the user says "revenue drop", "sales decline", or
# MAGIC "total amount change" -- because all three map to `total_amount`.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")

import requests, json

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

resp = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers)
space_lookup = {s["title"]: s["space_id"] for s in resp.json().get("spaces", []) if s.get("title", "").startswith("SC - ")}

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Synonym Definitions
# MAGIC
# MAGIC ### Demand Analysis Space
# MAGIC
# MAGIC The sales_orders table is the most queried table. Key mappings:
# MAGIC - `total_amount` <- "revenue", "sales", "order value" (prevents confusion with `unit_price` or `quantity`)
# MAGIC - `region` <- "geography", "market" + **entity matching** (auto-corrects "western" to "Western")
# MAGIC - `product_family` <- "family", "product line" (prevents confusion with `product_category`)

# COMMAND ----------

# DBTITLE 1,Synonym and Enhanced Instruction Definitions
# ====================================================================
# ENHANCED SPACE INSTRUCTIONS (the key improvement that drives 20% → 50%)
# These replace the minimal baseline instructions with domain-specific
# guidance about time windows, column names, and query patterns.
# ====================================================================
enhanced_instructions = {
    "SC - Demand Analysis": """You are a Demand Analysis specialist for supply chain revenue and forecasting.

TIME PERIOD RULES (CRITICAL):
- The demo dataset uses September 1, 2026 as the reference date. 'Last month' = August 2026, 'Prior month' = July 2026.
- 'Last month': order_date >= '2026-08-01' AND order_date < '2026-09-01'
- 'Prior month': order_date >= '2026-07-01' AND order_date < '2026-08-01'
- ALWAYS use these exact date boundaries, never rolling day windows.

KEY COLUMNS: Use 'total_amount' for revenue. Use 'product_family' (not product_category) for top-level grouping. Region values: Western, Eastern, Central, Southern.""",

    "SC - Inventory Management": """You are an Inventory Management specialist.

KEY COLUMNS:
- stockout_flag = true means zero inventory (complete stockout)
- below_safety_stock_flag = true means inventory is at risk
- days_of_supply < 7 is critical
- Use COUNT(DISTINCT sku_id) when counting stockout SKUs, not COUNT(*)

When asked about stockouts by region, query: SELECT region, COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS stockout_skus FROM inventory_ledger GROUP BY region""",

    "SC - Logistics Operations": """You are a Logistics Operations specialist.

CRITICAL SCHEMA NOTE:
- The shipments table has 'destination_region' and 'origin_region' but NO column named 'region'
- ALWAYS use destination_region when filtering or grouping by region
- The demo dataset reference date is September 1, 2026. 'Last month' = August 2026.
- 'Last month': ship_date >= '2026-08-01' AND ship_date < '2026-09-01'
- ALWAYS use these exact date boundaries
- is_late = true for late shipments, delay_days for delay duration""",

    "SC - Supplier Risk": """You are a Supplier Risk specialist.

CRITICAL SCHEMA NOTE:
- supplier_orders has supplier_continent (Asia, Europe, North America) but NO region column
- There is NO way to filter suppliers by 'Western region' or any other domestic region
- When asked about suppliers for any region, ALWAYS show ALL suppliers grouped by continent
- NEVER try to filter WHERE supplier_continent = 'North America' when asked about 'Western region'
- The demo dataset reference date is September 1, 2026. 'Last month' = August 2026.
- 'Last month': order_date >= '2026-08-01' AND order_date < '2026-09-01'""",

    "SC - Executive Reporting": """You are an Executive Reporting specialist with cross-domain views.

KEY VIEWS:
- executive_kpis: Single row with ALL headline KPIs (pre-computed for last calendar month)
- regional_performance_summary: Cross-domain metrics per region
- revenue_trend: Daily revenue with 'period' column (Last_Month / Prior_Month)
- supply_chain_risk_scorecard: Supplier risk ranking (lowest composite_risk_score = highest risk)""",
}

# ====================================================================
# COLUMN SYNONYMS
# ====================================================================
synonyms_config = {
    "SC - Demand Analysis": {
        f"{CATALOG}.demand_analysis.sales_orders": [
            {"column_name": "channel", "synonyms": ["sales channel", "order channel"], "enable_entity_matching": True},
            {"column_name": "order_date", "synonyms": ["date", "when ordered", "order time"]},
            {"column_name": "order_status", "synonyms": ["status", "fulfillment status"], "enable_entity_matching": True},
            {"column_name": "product_family", "synonyms": ["family", "product line", "product group"], "enable_entity_matching": True},
            {"column_name": "region", "synonyms": ["geography", "area", "market"], "enable_entity_matching": True, "enable_format_assistance": True},
            {"column_name": "total_amount", "synonyms": ["revenue", "sales", "order value", "amount", "dollars"]},
        ],
        f"{CATALOG}.demand_analysis.demand_forecasts": [
            {"column_name": "bias", "synonyms": ["forecast bias", "over-forecast", "under-forecast"]},
            {"column_name": "forecast_accuracy_pct", "synonyms": ["accuracy", "forecast accuracy"]},
            {"column_name": "forecast_demand_units", "synonyms": ["forecast", "predicted demand"]},
            {"column_name": "forecast_month", "synonyms": ["month", "period"]},
        ],
        f"{CATALOG}.demand_analysis.pos_data": [
            {"column_name": "quantity_sold", "synonyms": ["units sold", "volume"]},
            {"column_name": "return_flag", "synonyms": ["returned", "return", "refund"]},
            {"column_name": "total_sales", "synonyms": ["revenue", "sales amount", "pos revenue"]},
            {"column_name": "transaction_date", "synonyms": ["date", "sale date"]},
        ],
    },
    "SC - Inventory Management": {
        f"{CATALOG}.inventory_management.inventory_ledger": [
            {"column_name": "below_safety_stock_flag", "synonyms": ["below safety stock", "low stock", "at risk"]},
            {"column_name": "days_of_supply", "synonyms": ["days remaining", "supply days", "runway"]},
            {"column_name": "on_hand_qty", "synonyms": ["on hand", "inventory level", "stock level", "quantity"]},
            {"column_name": "product_family", "synonyms": ["family", "product line"], "enable_entity_matching": True},
            {"column_name": "region", "synonyms": ["geography", "area"], "enable_entity_matching": True, "enable_format_assistance": True},
            {"column_name": "safety_stock_level", "synonyms": ["safety stock", "minimum stock"]},
            {"column_name": "stockout_flag", "synonyms": ["stockout", "out of stock", "zero inventory"]},
        ],
        f"{CATALOG}.inventory_management.stock_movements": [
            {"column_name": "movement_date", "synonyms": ["date", "when"]},
            {"column_name": "movement_type", "synonyms": ["type", "direction"], "enable_entity_matching": True},
            {"column_name": "quantity", "synonyms": ["units", "volume", "amount"]},
            {"column_name": "region", "synonyms": ["geography"], "enable_entity_matching": True},
        ],
    },
    "SC - Logistics Operations": {
        f"{CATALOG}.logistics_operations.shipments": [
            {"column_name": "carrier_id", "synonyms": ["carrier", "shipping company"], "enable_format_assistance": True, "enable_entity_matching": True},
            {"column_name": "delay_days", "synonyms": ["days delayed", "days late"]},
            {"column_name": "delay_reason", "synonyms": ["cause of delay", "root cause"], "enable_format_assistance": True, "enable_entity_matching": True},
            {"column_name": "destination_region", "synonyms": ["region", "delivery region", "target region"], "enable_format_assistance": True, "enable_entity_matching": True},
            {"column_name": "is_late", "synonyms": ["late", "delayed", "overdue"]},
            {"column_name": "origin_region", "synonyms": ["source region", "ship from region"], "enable_format_assistance": True, "enable_entity_matching": True},
            {"column_name": "ship_date", "synonyms": ["shipment date", "date shipped"]},
        ],
    },
    "SC - Supplier Risk": {
        f"{CATALOG}.supplier_procurement.supplier_orders": [
            {"column_name": "actual_lead_time_days", "synonyms": ["actual lead time", "delivery time"]},
            {"column_name": "contracted_lead_time_days", "synonyms": ["contracted lead time", "expected lead time"]},
            {"column_name": "delay_reason", "synonyms": ["cause", "root cause"], "enable_entity_matching": True},
            {"column_name": "is_late", "synonyms": ["late", "delayed", "overdue"]},
            {"column_name": "lead_time_variance_days", "synonyms": ["variance", "delay days", "slippage"]},
            {"column_name": "quality_score", "synonyms": ["quality", "quality rating"]},
            {"column_name": "quantity_received", "synonyms": ["received", "delivered qty"]},
            {"column_name": "supplier_continent", "synonyms": ["continent", "geography", "source region"], "enable_entity_matching": True},
        ],
        f"{CATALOG}.supplier_procurement.vendor_slas": [
            {"column_name": "is_breached", "synonyms": ["breached", "violated", "failed"]},
            {"column_name": "penalty_amount", "synonyms": ["penalty", "fine"]},
            {"column_name": "sla_metric", "synonyms": ["metric", "kpi", "measure"], "enable_entity_matching": True},
        ],
    },
}

total_synonyms = sum(len(cc.get("synonyms", [])) for tables in synonyms_config.values() for cols in tables.values() for cc in cols)
print(f"Total synonym mappings: {total_synonyms} across {len(synonyms_config)} spaces")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Apply Synonyms to Genie Spaces
# MAGIC
# MAGIC We update each space's `data_sources.tables[].column_configs` to include the synonym definitions.
# MAGIC Logistics and Executive Reporting don't need synonyms -- Logistics already has the critical
# MAGIC `destination_region` synonym from the initial setup, and Executive Reporting uses self-documenting view columns.

# COMMAND ----------

# DBTITLE 1,Apply Enhanced Instructions + Synonyms
# Apply BOTH enhanced instructions AND synonyms to each space
all_spaces_to_update = set(list(enhanced_instructions.keys()) + list(synonyms_config.keys()))

for space_name in sorted(all_spaces_to_update):
    space_id = space_lookup.get(space_name)
    if not space_id:
        print(f"\u2717 Space not found: {space_name}")
        continue

    resp = requests.get(f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true", headers=headers)
    current = resp.json()
    etag = current.get("etag", "")
    ss = json.loads(current.get("serialized_space", "{}"))

    # 1. Update instructions if we have enhanced ones
    if space_name in enhanced_instructions:
        ss["instructions"]["text_instructions"] = [{"content": [enhanced_instructions[space_name]]}]

    # 2. Add column synonyms if we have them
    syn_count = 0
    if space_name in synonyms_config:
        tables_config = synonyms_config[space_name]
        if "data_sources" in ss and "tables" in ss["data_sources"]:
            for table_entry in ss["data_sources"]["tables"]:
                table_id = table_entry.get("identifier", "")
                if table_id in tables_config:
                    table_entry["column_configs"] = tables_config[table_id]
        syn_count = sum(len(cc.get("synonyms", [])) for cols in tables_config.values() for cc in cols)

    patch_headers = dict(headers)
    if etag:
        patch_headers["If-Match"] = etag

    resp = requests.patch(f"{host}/api/2.0/genie/spaces/{space_id}", headers=patch_headers,
        json={"serialized_space": json.dumps(ss)})

    status = "\u2713" if resp.status_code == 200 else "\u2717"
    instr_flag = "+ enhanced instructions" if space_name in enhanced_instructions else ""
    syn_flag = f"+ {syn_count} synonyms" if syn_count > 0 else ""
    print(f"{status} {space_name}: {instr_flag} {syn_flag}")
    if resp.status_code != 200:
        print(f"  Error: {resp.text[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## How This Contributes to End-to-End Consistency
# MAGIC
# MAGIC | Consistency Dimension | How Synonyms Help |
# MAGIC | --- | --- |
# MAGIC | **Robustness** | Users can phrase questions in business language; Genie maps to correct columns |
# MAGIC | **Zero-row prevention** | Entity matching auto-corrects case ("western" -> "Western"), preventing empty results |
# MAGIC | **Cross-space alignment** | "revenue" maps to `total_amount` in demand, `total_sales` in POS -- both are correct in context |
# MAGIC | **Reduced hallucination** | Without synonyms, Genie might fabricate a column name; with synonyms, it maps to a real one |
# MAGIC
# MAGIC **Cumulative improvement after Iterations 1-3**:
# MAGIC - Iteration 1: We know the correct answers (ground truth)
# MAGIC - Iteration 2: Agent uses correct SQL patterns (accuracy)
# MAGIC - **Iteration 3: Agent understands business language (robustness)**
# MAGIC
# MAGIC **What remains unfixed**: The Supervisor Agent itself may still present sub-agent findings
# MAGIC inconsistently or fail to reconcile numbers across domains. That's Iteration 4.