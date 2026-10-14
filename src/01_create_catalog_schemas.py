# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1: Create Catalog and Schemas
# MAGIC Creates the parameterized catalog and all 5 domain schemas for the Supply Chain demo.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
dbutils.widgets.text("user_email", "", "Owner Email (optional)")
CATALOG = dbutils.widgets.get("catalog_name")
USER_EMAIL = dbutils.widgets.get("user_email").strip()
print(f"Target catalog: {CATALOG}")
if USER_EMAIL:
    print(f"Ownership will be transferred to: {USER_EMAIL}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
print(f"✓ Catalog {CATALOG} created")
if USER_EMAIL:
    spark.sql(f"ALTER CATALOG {CATALOG} SET OWNER TO `{USER_EMAIL}`")
    print(f"  ↳ Ownership transferred to {USER_EMAIL}")

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
    if USER_EMAIL:
        spark.sql(f"ALTER SCHEMA {CATALOG}.{schema_name} SET OWNER TO `{USER_EMAIL}`")
    print(f"✓ Schema {CATALOG}.{schema_name} created")

if USER_EMAIL:
    # Also transfer default schema ownership
    try:
        spark.sql(f"ALTER SCHEMA {CATALOG}.default SET OWNER TO `{USER_EMAIL}`")
    except Exception:
        pass  # default schema may not exist

print(f"\n✓ All schemas created in {CATALOG}" + (f" (owned by {USER_EMAIL})" if USER_EMAIL else ""))

# COMMAND ----------

# DBTITLE 1,Verify: catalog and schemas exist
# ── Assertions ──
expected_schemas = ['demand_analysis', 'inventory_management', 'logistics_operations', 'supplier_procurement', 'reporting']
actual = [r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()]
for s in expected_schemas:
    assert s in actual, f"MISSING schema: {CATALOG}.{s}"
print(f"✓ ASSERT PASS: catalog {CATALOG} exists with all {len(expected_schemas)} schemas")