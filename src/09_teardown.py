# Databricks notebook source
# MAGIC %md
# MAGIC # Teardown: Remove All Demo Resources
# MAGIC **WARNING**: This will permanently delete the catalog, all Genie Spaces, and the Supervisor Agent.
# MAGIC Run this to clean up a deployment before redeploying, or to remove the demo entirely.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")

print(f"TEARDOWN STARTING for {CATALOG}")

# COMMAND ----------

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

# COMMAND ----------

# ---- Delete Supervisor Agents ----
resp = requests.get(f"{host}/api/2.1/supervisor-agents", headers=headers)
if resp.status_code == 200:
    for agent in resp.json().get("supervisor_agents", []):
        if "Supply Chain" in agent.get("display_name", ""):
            name = agent["name"]
            del_resp = requests.delete(f"{host}/api/2.1/{name}", headers=headers)
            status = "✓" if del_resp.status_code in (200, 204) else "✗"
            print(f"{status} Deleted supervisor: {agent['display_name']}")

# COMMAND ----------

# ---- Delete ALL Genie Spaces (all iterations: Baseline, Iter 1, 2, 3, Evaluator) ----
# With per-iteration agent creation, there can be up to 21 agents:
#   5 × Baseline + 5 × Iteration 1 + 5 × Iteration 2 + 5 × Iteration 3 + 1 Evaluator
resp = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers)
_deleted_count = 0
if resp.status_code == 200:
    for space in resp.json().get("spaces", []):
        title = space.get("title", "")
        if title.startswith("SC - ") or ("SC" in title and any(k in title for k in ["Demand", "Inventory", "Logistics", "Supplier", "Executive", "Evaluator"])):
            sid = space["space_id"]
            del_resp = requests.delete(f"{host}/api/2.0/genie/spaces/{sid}", headers=headers)
            status = "✓" if del_resp.status_code in (200, 204) else "✗"
            print(f"{status} Deleted space: {title}")
            if del_resp.status_code in (200, 204):
                _deleted_count += 1
    print(f"\n  Total deleted: {_deleted_count} Genie agents")
else:
    print(f"✗ Could not list spaces: {resp.status_code}")

# COMMAND ----------

# ---- Drop Catalog (CASCADE) ----
try:
    spark.sql(f"DROP CATALOG IF EXISTS {CATALOG} CASCADE")
    print(f"✓ Dropped catalog: {CATALOG}")
except Exception as e:
    print(f"✗ Failed to drop catalog: {e}")

print(f"\n✓ Teardown complete for {CATALOG}")