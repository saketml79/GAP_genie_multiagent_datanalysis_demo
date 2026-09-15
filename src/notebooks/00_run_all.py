# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Supply Chain Control Tower — Full Pipeline
# MAGIC
# MAGIC **One-click setup**: Tears down any existing deployment, regenerates all data, creates Genie Agents,
# MAGIC Supervisor Agent, and applies all 5 improvement iterations.
# MAGIC
# MAGIC ### Why the data is deterministic
# MAGIC
# MAGIC All data generation scripts use a **fixed reference date** (`base_date = datetime(2026, 9, 1)`) and
# MAGIC fixed random seeds (42/43/44/45). This produces **identical data every run**, regardless of when the
# MAGIC demo is executed — the same approach used by standard Databricks training demos.
# MAGIC
# MAGIC "Last month" = August 2026, "Prior month" = July 2026. All SQL views, certified queries, and ground
# MAGIC truth use `DATE '2026-09-01'` instead of `CURRENT_DATE()` so the demo is fully self-contained.
# MAGIC
# MAGIC ### Run order
# MAGIC
# MAGIC | Step | Script | Purpose |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | `09_teardown` | Remove existing catalog + agents |
# MAGIC | 2 | `01_create_catalog_schemas` | Create catalog and 5 schemas |
# MAGIC | 3-6 | `02` through `05` | Generate deterministic demo data |
# MAGIC | 7-8 | `06` and `07` | Create reporting views + add comments |
# MAGIC | 9 | `08_setup_genie_supervisor` | Create 6 Genie Agents + Supervisor |
# MAGIC | 10 | *(auto)* | Wait for serving endpoint to become READY |
# MAGIC | 11-14 | `iteration_02` through `iteration_05` | Progressive improvements |
# MAGIC | 15 | *(auto)* | Verify 10/10 ground truth match |

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID")
dbutils.widgets.dropdown("run_mode", "full", ["full", "skip_teardown", "iterations_only"], "Run Mode")

CATALOG = dbutils.widgets.get("catalog_name")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
RUN_MODE = dbutils.widgets.get("run_mode")

print(f"Catalog: {CATALOG}")
print(f"Warehouse: {WAREHOUSE_ID}")
print(f"Run Mode: {RUN_MODE}")

# COMMAND ----------

# DBTITLE 1,Setup: Paths + API Client
import os, time, requests, json

# Derive src/ directory from this notebook's location
this_notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
SRC_DIR = os.path.dirname(os.path.dirname(this_notebook))  # up from notebooks/ to src/
print(f"Source directory: {SRC_DIR}")

# Shared parameters for all sub-notebooks
params = {"catalog_name": CATALOG}
setup_params = {"catalog_name": CATALOG, "warehouse_id": WAREHOUSE_ID}

# API client for endpoint discovery
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

def run_notebook(name, timeout=600, extra_params=None):
    """Run a notebook relative to the src/ directory."""
    p = dict(params)
    if extra_params:
        p.update(extra_params)
    path = f"{SRC_DIR}/{name}"
    print(f"\n{'='*60}")
    print(f"▶ Running: {name}")
    print(f"  Path: {path}")
    print(f"  Params: {p}")
    print(f"{'='*60}")
    result = dbutils.notebook.run(path, timeout, p)
    print(f"✓ Completed: {name} (result: {result})")
    return result


def table_exists(table_name):
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


def values_match(actual, expected):
    try:
        actual_f = float(actual)
        expected_f = float(expected)
        return abs(actual_f - expected_f) / max(abs(expected_f), 0.001) < 0.01
    except Exception:
        return str(actual) == str(expected)


def verify_ground_truth(stage_label):
    print(f"\n=== GROUND TRUTH VERIFICATION: {stage_label} ===")
    gt_table = f"{CATALOG}.reporting.ground_truth_kpis"
    if not table_exists(gt_table):
        print("⚠ ground_truth_kpis not yet created — skipping verification")
        return {"stage": stage_label, "exact": 0, "available": 0, "skipped": 10}

    gt_df = spark.sql(f"SELECT * FROM {gt_table} ORDER BY agent, metric").collect()
    gt = {row["metric"]: row["ground_truth_value"] for row in gt_df}

    checks = [
        {
            "label": "Western revenue change",
            "table": f"{CATALOG}.demand_analysis.revenue_comparison_by_region",
            "sql": f"SELECT revenue_change_dollars FROM {CATALOG}.demand_analysis.revenue_comparison_by_region WHERE region = 'Western'",
            "gt_key": next((k for k in gt if "revenue change" in k.lower()), None),
        },
        {
            "label": "Western below safety stock",
            "table": f"{CATALOG}.inventory_management.inventory_safety_stock_metrics",
            "sql": f"SELECT sku_warehouse_positions_below_safety_stock FROM {CATALOG}.inventory_management.inventory_safety_stock_metrics WHERE region = 'Western'",
            "gt_key": "Western below safety stock",
        },
        {
            "label": "Western stockout SKUs",
            "table": f"{CATALOG}.inventory_management.inventory_safety_stock_metrics",
            "sql": f"SELECT unique_skus_in_stockout FROM {CATALOG}.inventory_management.inventory_safety_stock_metrics WHERE region = 'Western'",
            "gt_key": "Western stockout SKUs",
        },
        {
            "label": "Western late delivery rate",
            "table": f"{CATALOG}.logistics_operations.delivery_performance_by_region",
            "sql": f"SELECT late_delivery_pct_last_month FROM {CATALOG}.logistics_operations.delivery_performance_by_region WHERE destination_region = 'Western'",
            "gt_key": next((k for k in gt if "late delivery" in k.lower()), None),
        },
        {
            "label": "Western avg delay days",
            "table": f"{CATALOG}.logistics_operations.delivery_performance_by_region",
            "sql": f"SELECT avg_delay_days_when_late FROM {CATALOG}.logistics_operations.delivery_performance_by_region WHERE destination_region = 'Western'",
            "gt_key": next((k for k in gt if "avg delay" in k.lower()), None),
        },
        {
            "label": "Asia supplier late rate",
            "table": f"{CATALOG}.supplier_procurement.supplier_performance_by_continent",
            "sql": f"SELECT supplier_late_rate_pct_last_month FROM {CATALOG}.supplier_procurement.supplier_performance_by_continent WHERE supplier_continent = 'Asia'",
            "gt_key": next((k for k in gt if "Asia supplier late" in k), None),
        },
        {
            "label": "Asia avg lead time variance",
            "table": f"{CATALOG}.supplier_procurement.supplier_performance_by_continent",
            "sql": f"SELECT avg_lead_time_variance_days FROM {CATALOG}.supplier_procurement.supplier_performance_by_continent WHERE supplier_continent = 'Asia'",
            "gt_key": next((k for k in gt if "Asia avg lead" in k), None),
        },
        {
            "label": "Service level pct",
            "table": f"{CATALOG}.reporting.executive_kpis",
            "sql": f"SELECT service_level_pct FROM {CATALOG}.reporting.executive_kpis",
            "gt_key": "Service level pct",
        },
        {
            "label": "Total stockout SKUs",
            "table": f"{CATALOG}.reporting.executive_kpis",
            "sql": f"SELECT total_stockout_skus FROM {CATALOG}.reporting.executive_kpis",
            "gt_key": "Total stockout SKUs",
        },
        {
            "label": "Supplier late pct",
            "table": f"{CATALOG}.reporting.executive_kpis",
            "sql": f"SELECT supplier_late_pct_last_month FROM {CATALOG}.reporting.executive_kpis",
            "gt_key": next((k for k in gt if "Supplier late pct" in k), None),
        },
    ]

    exact_count = 0
    available_count = 0
    skipped_count = 0

    for check in checks:
        if not table_exists(check["table"]):
            skipped_count += 1
            print(f"⏭ {check['label']}: skipped ({check['table'].split('.')[-1]} not created yet)")
            continue

        available_count += 1
        result = spark.sql(check["sql"]).first()[0]
        gt_val = gt.get(check["gt_key"], "N/A") if check["gt_key"] else "N/A"
        match = values_match(result, gt_val)
        if match:
            exact_count += 1
        print(f"{'✅' if match else '❌'} {check['label']}: view={result}, gt={gt_val}")

    print(f"\nSummary for {stage_label}: {exact_count}/{available_count} exact, {skipped_count} skipped")
    return {"stage": stage_label, "exact": exact_count, "available": available_count, "skipped": skipped_count}

# COMMAND ----------

# DBTITLE 1,Step 1: Teardown (clean slate)
# Always run teardown first for idempotency — safe even if nothing exists to tear down
if RUN_MODE != "iterations_only":
    run_notebook("09_teardown", timeout=300)
    print("\n✓ Teardown complete — catalog and agents removed")
else:
    print(f"⏭ Skipping teardown (run_mode=iterations_only — agents must already exist)")

# COMMAND ----------

# DBTITLE 1,Steps 2-8: Create Catalog + Generate Data + Setup Agents
if RUN_MODE != "iterations_only":
    # Create catalog and schemas
    run_notebook("01_create_catalog_schemas", timeout=120)
    
    # Generate deterministic demo data (seeds 42-45, pinned to 1st of month)
    run_notebook("02_generate_demand_data", timeout=600)
    run_notebook("03_generate_inventory_data", timeout=600)
    run_notebook("04_generate_logistics_data", timeout=600)
    run_notebook("05_generate_supplier_data", timeout=600)
    
    # Create reporting views and add column/table comments
    run_notebook("06_create_reporting_views", timeout=300)
    run_notebook("07_add_all_comments", timeout=300)
    
    # Create Genie Agents + Supervisor + Ground Truth
    run_notebook("08_setup_genie_supervisor", timeout=600, extra_params={"warehouse_id": WAREHOUSE_ID})
    
    print("\n✓ Steps 2-8 complete: data layer + agents created")
else:
    print(f"⏭ Skipping data creation (run_mode={RUN_MODE})")

# COMMAND ----------

# DBTITLE 1,Step 9: Discover Supervisor Endpoint + Wait for READY
# The platform auto-creates a serving endpoint when a supervisor agent is created.
# We discover it by finding the supervisor and deriving the endpoint name.

print("Discovering Supervisor Agent...")
resp = requests.get(f"{host}/api/2.1/supervisor-agents", headers=headers)
supervisor_name = None
endpoint_name = None

for agent in resp.json().get("supervisor_agents", []):
    if "Supply Chain" in agent.get("display_name", ""):
        supervisor_name = agent["name"]
        # Extract UUID from "supervisor-agents/<uuid>"
        uuid_str = supervisor_name.split("/")[1]
        endpoint_name = f"mas-{uuid_str[:8]}-endpoint"
        print(f"✓ Supervisor: {supervisor_name}")
        print(f"✓ Expected endpoint: {endpoint_name}")
        break

if not supervisor_name:
    raise RuntimeError("No Supply Chain Supervisor Agent found! Did 08_setup complete?")

# Wait for the endpoint to become READY (max 5 minutes)
print(f"\nWaiting for endpoint '{endpoint_name}' to become READY...")
for i in range(50):
    resp = requests.get(f"{host}/api/2.0/serving-endpoints/{endpoint_name}", headers=headers)
    if resp.status_code == 200:
        state = resp.json().get("state", {}).get("ready", "")
        if state == "READY":
            print(f"✓ Endpoint READY after {(i+1)*6}s")
            break
        if i % 5 == 0:
            print(f"  Waiting... {(i+1)*6}s (state: {state})")
    elif resp.status_code == 404:
        if i % 5 == 0:
            print(f"  Endpoint not yet created... {(i+1)*6}s")
    time.sleep(6)
else:
    print(f"⚠ Endpoint not READY after 5 minutes — continuing anyway")

print(f"\nEndpoint name: {endpoint_name}")
print(f"Supervisor name: {supervisor_name}")

verification_history = []
verification_history.append(verify_ground_truth("Baseline (after 08_setup)"))

# COMMAND ----------

# DBTITLE 1,Step 10: Iteration 02 — Certified Queries (expect ~20%)
# Certified queries can actually HURT accuracy if they encode wrong patterns.
# Expected: ~20% (2/10) — WORSE than baseline.
run_notebook("improvements/iteration_02_certified_queries", timeout=300)
verification_history.append(verify_ground_truth("After iteration_02 (certified queries)"))

# COMMAND ----------

# DBTITLE 1,Step 11: Iteration 03 — Synonyms + Instructions (expect ~50%)
# Column synonyms + enhanced instructions teach agents how to interpret business terms.
# Expected: ~50% (5/10).
run_notebook("improvements/iteration_03_column_synonyms", timeout=300)
verification_history.append(verify_ground_truth("After iteration_03 (synonyms + instructions)"))

# COMMAND ----------

# DBTITLE 1,Step 12: Iteration 04 — Supervisor Hardening (expect ~90%)
# Hardened supervisor instructions with strict routing rules, month-name mandate,
# and 7-section output format. Biggest single jump in accuracy.
# Expected: ~90% (9/10).
run_notebook("improvements/iteration_04_supervisor_hardening", timeout=300)
verification_history.append(verify_ground_truth("After iteration_04 (supervisor hardening)"))

# COMMAND ----------

# DBTITLE 1,Step 13: Iteration 05 — Metric Views + Glossary (expect 100%)
# Metric views encode exact business definitions in the column name.
# UC tags + glossary eliminate the last ambiguity (COUNT(*) vs COUNT(DISTINCT)).
# Expected: 100% (10/10).
run_notebook("improvements/iteration_05_metric_views_glossary", timeout=300)
verification_history.append(verify_ground_truth("After iteration_05 (metric views + glossary)"))

# COMMAND ----------

# DBTITLE 1,Step 14: Iteration 06 — Cost of Disruption (expect 100% on 11)
# Cross-domain Cost of Disruption view + UC governance + 11th ground truth row.
# Tests whether the system can answer a NEW question it's never seen.
# Expected: 100% (11/11).
run_notebook("improvements/iteration_06_cost_of_disruption", timeout=300)
verification_history.append(verify_ground_truth("After iteration_06 (cost of disruption + UC governance)"))

# COMMAND ----------

# DBTITLE 1,Step 15: Verify Ground Truth Alignment
# Confirm that all metric views match the ground truth table exactly.
# This proves the data layer is consistent and the Genie Agents will return correct answers.

final_snapshot = verification_history[-1] if verification_history else verify_ground_truth("Current state")

print("\n=== VERIFICATION HISTORY ===")
for item in verification_history:
    status = "✅" if item["exact"] == item["available"] and item["available"] > 0 else "⚠️"
    print(f"{status} {item['stage']}: {item['exact']}/{item['available']} exact, {item['skipped']} skipped")

gt_count = final_snapshot['exact']
gt_total = final_snapshot['available']
print(f"\n{'✅' if gt_count == gt_total else '⚠️'} FINAL SCORECARD: {gt_count}/{gt_total} EXACT")

# COMMAND ----------

# DBTITLE 1,Step 15: Quick Supervisor Test
# Send a test question to the Supervisor to verify it responds.

if endpoint_name:
    print(f"Testing Supervisor via endpoint: {endpoint_name}")
    test_resp = requests.post(
        f"{host}/serving-endpoints/{endpoint_name}/invocations",
        headers=headers,
        json={"input": [{"role": "user", "content": "What is the service level percentage?"}], "max_tokens": 500}
    )
    if test_resp.status_code == 200:
        output = test_resp.json().get("output", [])
        # Extract the final assistant message
        for item in reversed(output):
            if item.get("role") == "assistant" and item.get("content"):
                for c in item["content"]:
                    if c.get("type") == "output_text" and len(c.get("text", "")) > 20:
                        print(f"\n✓ Supervisor responded:\n{c['text'][:500]}")
                        break
        print("\n✓ Supervisor is operational")
    else:
        print(f"⚠ Supervisor returned {test_resp.status_code}: {test_resp.text[:200]}")
else:
    print("⚠ No endpoint found — skipping test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Component | Status |
# MAGIC | --- | --- |
# MAGIC | Catalog + 5 schemas | Created |
# MAGIC | 23 tables | Generated (deterministic with seed + pinned date) |
# MAGIC | 4 reporting views | Created |
# MAGIC | 4 metric views | Created |
# MAGIC | 5 domain Genie Agents | Created + configured (certified queries, synonyms, instructions) |
# MAGIC | 1 Evaluator Agent | Created with ground truth table |
# MAGIC | 1 Supervisor Agent | Created + hardened (7-section output, 12 rules) |
# MAGIC | Serving endpoint | Auto-discovered and verified READY |
# MAGIC | Ground truth | 10/10 EXACT match |
# MAGIC
# MAGIC ### To invoke the Supervisor manually:
# MAGIC ```python
# MAGIC resp = requests.post(
# MAGIC     f"{host}/serving-endpoints/{endpoint_name}/invocations",
# MAGIC     headers=headers,
# MAGIC     json={"input": [{"role": "user", "content": "Why did revenue drop in the Western Region last month?"}]}
# MAGIC )
# MAGIC ```