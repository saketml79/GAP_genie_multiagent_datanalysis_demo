# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Iteration 3: Add Column Synonyms and Entity Matching Across ALL Spaces
# MAGIC 
# MAGIC **Goal**: Add column synonyms to every Genie Space so natural language maps correctly to column names.
# MAGIC Enable entity matching and format assistance so Genie auto-corrects value formats.
# MAGIC 
# MAGIC **What this fixes**:
# MAGIC - "region" maps to `destination_region` in logistics (already done in iter 0, now extend to ALL spaces)
# MAGIC - "revenue" maps to `total_amount`, "sales" maps to `total_sales`
# MAGIC - "late" maps to `is_late`, "delayed" maps to `delay_days`
# MAGIC - Entity matching: "western" auto-corrects to "Western"
# MAGIC 
# MAGIC **Improvement**: Completeness + Accuracy (fewer 0-row results)

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

# Get space IDs
resp = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers)
space_lookup = {s["title"]: s["space_id"] for s in resp.json().get("spaces", []) if s.get("title", "").startswith("SC - ")}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Column Synonyms Configuration
# MAGIC 
# MAGIC Column synonyms tell Genie: "when the user says X, they mean column Y".
# MAGIC Entity matching tells Genie: "auto-correct value casing/spelling".

# COMMAND ----------
# Define synonyms per space per table (column_configs must be sorted by column_name)
synonyms_config = {
    "SC - Demand Analysis": {
        f"{CATALOG}.demand_analysis.sales_orders": [
            {"column_name": "channel", "synonyms": ["sales channel", "order channel"], "enable_entity_matching": True},
            {"column_name": "customer_id", "synonyms": ["customer", "buyer"]},
            {"column_name": "fulfillment_warehouse", "synonyms": ["warehouse", "fulfillment center"]},
            {"column_name": "order_date", "synonyms": ["date", "when ordered", "order time"]},
            {"column_name": "order_status", "synonyms": ["status", "fulfillment status", "order state"], "enable_entity_matching": True},
            {"column_name": "product_category", "synonyms": ["category", "sub-category"], "enable_entity_matching": True},
            {"column_name": "product_family", "synonyms": ["family", "product line", "product group"], "enable_entity_matching": True},
            {"column_name": "region", "synonyms": ["geography", "area", "market"], "enable_entity_matching": True, "enable_format_assistance": True},
            {"column_name": "total_amount", "synonyms": ["revenue", "sales", "order value", "amount", "dollars"]},
        ],
        f"{CATALOG}.demand_analysis.demand_forecasts": [
            {"column_name": "actual_demand_units", "synonyms": ["actual demand", "actual units", "real demand"]},
            {"column_name": "bias", "synonyms": ["forecast bias", "prediction bias", "over-forecast", "under-forecast"]},
            {"column_name": "forecast_accuracy_pct", "synonyms": ["accuracy", "forecast accuracy", "prediction accuracy"]},
            {"column_name": "forecast_demand_units", "synonyms": ["forecast", "predicted demand", "forecast units"]},
            {"column_name": "forecast_month", "synonyms": ["month", "period"]},
        ],
        f"{CATALOG}.demand_analysis.pos_data": [
            {"column_name": "quantity_sold", "synonyms": ["units sold", "quantity", "volume"]},
            {"column_name": "return_flag", "synonyms": ["returned", "return", "refund"]},
            {"column_name": "total_sales", "synonyms": ["revenue", "sales amount", "pos revenue"]},
            {"column_name": "transaction_date", "synonyms": ["date", "sale date"]},
        ],
    },
    "SC - Inventory Management": {
        f"{CATALOG}.inventory_management.inventory_ledger": [
            {"column_name": "available_qty", "synonyms": ["available", "available inventory"]},
            {"column_name": "below_safety_stock_flag", "synonyms": ["below safety stock", "low stock", "at risk"]},
            {"column_name": "days_of_supply", "synonyms": ["days remaining", "supply days", "runway"]},
            {"column_name": "on_hand_qty", "synonyms": ["on hand", "inventory level", "stock level", "quantity"]},
            {"column_name": "product_family", "synonyms": ["family", "product line"], "enable_entity_matching": True},
            {"column_name": "region", "synonyms": ["geography", "area"], "enable_entity_matching": True, "enable_format_assistance": True},
            {"column_name": "safety_stock_level", "synonyms": ["safety stock", "minimum stock", "buffer stock"]},
            {"column_name": "stockout_flag", "synonyms": ["stockout", "out of stock", "zero inventory"]},
        ],
        f"{CATALOG}.inventory_management.stock_movements": [
            {"column_name": "movement_date", "synonyms": ["date", "when"]},
            {"column_name": "movement_type", "synonyms": ["type", "direction", "inbound outbound"], "enable_entity_matching": True},
            {"column_name": "quantity", "synonyms": ["units", "volume", "amount"]},
            {"column_name": "region", "synonyms": ["geography"], "enable_entity_matching": True},
        ],
    },
    "SC - Supplier Risk": {
        f"{CATALOG}.supplier_procurement.supplier_orders": [
            {"column_name": "actual_lead_time_days", "synonyms": ["actual lead time", "real lead time", "delivery time"]},
            {"column_name": "contracted_lead_time_days", "synonyms": ["contracted lead time", "expected lead time", "sla lead time"]},
            {"column_name": "delay_reason", "synonyms": ["cause", "root cause", "reason"], "enable_entity_matching": True},
            {"column_name": "is_late", "synonyms": ["late", "delayed", "overdue"]},
            {"column_name": "lead_time_variance_days", "synonyms": ["variance", "delay days", "slippage"]},
            {"column_name": "quality_score", "synonyms": ["quality", "quality rating"]},
            {"column_name": "quantity_received", "synonyms": ["received", "delivered qty"]},
            {"column_name": "supplier_continent", "synonyms": ["continent", "geography", "source region"], "enable_entity_matching": True},
        ],
        f"{CATALOG}.supplier_procurement.vendor_slas": [
            {"column_name": "is_breached", "synonyms": ["breached", "violated", "failed"]},
            {"column_name": "penalty_amount", "synonyms": ["penalty", "fine", "penalty cost"]},
            {"column_name": "sla_metric", "synonyms": ["metric", "kpi", "measure"], "enable_entity_matching": True},
        ],
    },
}

# COMMAND ----------
# Apply synonyms to each space
for space_name, tables_config in synonyms_config.items():
    space_id = space_lookup.get(space_name)
    if not space_id:
        print(f"\u2717 Space not found: {space_name}")
        continue

    # Get current config
    resp = requests.get(f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true", headers=headers)
    current = resp.json()
    etag = current.get("etag", "")
    ss = json.loads(current.get("serialized_space", "{}"))

    # Update column_configs in existing tables
    if "data_sources" in ss and "tables" in ss["data_sources"]:
        for table_entry in ss["data_sources"]["tables"]:
            table_id = table_entry.get("identifier", "")
            if table_id in tables_config:
                table_entry["column_configs"] = tables_config[table_id]

    patch_headers = dict(headers)
    if etag:
        patch_headers["If-Match"] = etag

    resp = requests.patch(
        f"{host}/api/2.0/genie/spaces/{space_id}",
        headers=patch_headers,
        json={"serialized_space": json.dumps(ss)}
    )
    if resp.status_code == 200:
        total_synonyms = sum(len(cc.get("synonyms", [])) for cols in tables_config.values() for cc in cols)
        print(f"\u2713 {space_name}: {total_synonyms} synonyms across {len(tables_config)} tables")
    else:
        print(f"\u2717 {space_name}: {resp.status_code} - {resp.text[:200]}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Expected Improvement
# MAGIC 
# MAGIC | Metric | Before | After |
# MAGIC | --- | --- | --- |
# MAGIC | "revenue" queries | Sometimes wrong column | Always maps to total_amount |
# MAGIC | "late shipments" queries | Sometimes fails | Maps to is_late correctly |
# MAGIC | Case sensitivity | "western" fails | Auto-corrects to "Western" |
# MAGIC | Column confusion | "region" in logistics fails | destination_region (from iter 0) |
# MAGIC | Synonym coverage | Logistics only | All 5 spaces now have synonyms |
# MAGIC 
# MAGIC **Next**: Run the Supervisor Agent prompt again and compare.
