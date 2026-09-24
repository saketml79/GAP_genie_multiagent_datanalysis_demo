# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1: Create Catalog and Schemas
# MAGIC Creates the parameterized catalog and all 5 domain schemas for the Supply Chain demo.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")
print(f"Target catalog: {CATALOG}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
print(f"✓ Catalog {CATALOG} created")

# COMMAND ----------

schemas = {
    "demand_analysis": "Demand Analysis domain - sales orders, forecasts, POS data, customer segments, promotions",
    "inventory_management": "Inventory Management domain - inventory ledger, warehouse data, store inventory, stock movements",
    "logistics_operations": "Logistics Operations domain - shipments, carriers, transit data, distribution centers",
    "supplier_procurement": "Supplier Procurement domain - supplier orders, lead times, procurement, vendor SLAs",
    "reporting": "Reporting domain - cross-domain summary views and executive reporting",
}

for schema_name, comment in schemas.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema_name} COMMENT '{comment}'")
    print(f"✓ Schema {CATALOG}.{schema_name} created")

print(f"\n✓ All schemas created in {CATALOG}")

# COMMAND ----------

# DBTITLE 1,Verify: catalog and schemas exist
# ── Assertions ──
expected_schemas = ['demand_analysis', 'inventory_management', 'logistics_operations', 'supplier_procurement', 'reporting']
actual = [r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()]
for s in expected_schemas:
    assert s in actual, f"MISSING schema: {CATALOG}.{s}"
print(f"✓ ASSERT PASS: catalog {CATALOG} exists with all {len(expected_schemas)} schemas")