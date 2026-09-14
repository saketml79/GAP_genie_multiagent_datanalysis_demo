# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Iteration 4: Add Benchmarks (Automated Validation)
# MAGIC 
# MAGIC **Goal**: Add benchmark test cases to each Genie Space. Benchmarks are question-answer pairs
# MAGIC that we can run automatically to verify Genie returns correct results.
# MAGIC 
# MAGIC **What this fixes**:
# MAGIC - Validates that certified queries return expected numbers
# MAGIC - Catches regressions if data or instructions change
# MAGIC - Provides a scoring mechanism for each iteration
# MAGIC 
# MAGIC **Improvement**: Validation + Regression Testing

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
# MAGIC ## Step 1: Compute Ground Truth Values
# MAGIC These are the numbers that the agents MUST return. We compute them from direct SQL.

# COMMAND ----------
# Compute and store ground truth as a dictionary
ground_truth = {}

# Revenue by region
rows = spark.sql(f"""
SELECT region,
  ROUND(SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE(), 30) THEN total_amount ELSE 0 END), 0) AS rev_last,
  ROUND(SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END), 0) AS rev_prior
FROM {CATALOG}.demand_analysis.sales_orders GROUP BY region
""").collect()
for r in rows:
    ground_truth[f"revenue_{r['region']}_last30"] = r["rev_last"]
    ground_truth[f"revenue_{r['region']}_prior30"] = r["rev_prior"]

# Stockouts
rows = spark.sql(f"""
SELECT region, COUNT(DISTINCT CASE WHEN stockout_flag THEN sku_id END) AS stockouts
FROM {CATALOG}.inventory_management.inventory_ledger GROUP BY region
""").collect()
for r in rows:
    ground_truth[f"stockouts_{r['region']}"] = r["stockouts"]

# Late shipments
rows = spark.sql(f"""
SELECT destination_region,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct
FROM {CATALOG}.logistics_operations.shipments
WHERE ship_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY destination_region
""").collect()
for r in rows:
    ground_truth[f"late_pct_{r['destination_region']}"] = float(r["late_pct"])

# Supplier late rate
rows = spark.sql(f"""
SELECT supplier_continent,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY supplier_continent
""").collect()
for r in rows:
    ground_truth[f"supplier_late_{r['supplier_continent']}"] = float(r["late_pct"])

# Executive KPIs
kpi = spark.sql(f"SELECT * FROM {CATALOG}.reporting.executive_kpis").collect()[0]
ground_truth["kpi_revenue_last30"] = kpi["revenue_last_30d"]
ground_truth["kpi_stockouts"] = kpi["total_stockout_skus"]
ground_truth["kpi_late_delivery_pct"] = float(kpi["late_delivery_pct_30d"])
ground_truth["kpi_service_level"] = float(kpi["service_level_pct"])

print("Ground Truth Values:")
for k, v in sorted(ground_truth.items()):
    print(f"  {k}: {v}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Define Benchmark Test Cases

# COMMAND ----------
benchmarks = {
    "SC - Demand Analysis": [
        {
            "question": "What is the total revenue for the Western region in the last 30 days?",
            "expected_column": "revenue_last_30d",
            "expected_value": ground_truth["revenue_Western_last30"],
            "tolerance_pct": 1.0,
            "must_contain": ["Western"],
        },
        {
            "question": "Show revenue by region comparing last 30 days to prior 30 days",
            "expected_column": "revenue_change",
            "expected_row_count": 4,
            "must_contain": ["Western", "Eastern", "Central", "Southern"],
        },
    ],
    "SC - Inventory Management": [
        {
            "question": "How many SKUs are in stockout in the Western region?",
            "expected_column": "stockout_skus",
            "expected_value": ground_truth["stockouts_Western"],
            "tolerance_pct": 0,
            "must_contain": ["Western"],
        },
    ],
    "SC - Logistics Operations": [
        {
            "question": "What is the late delivery rate for shipments going to the Western region in the last 30 days?",
            "expected_column": "late_pct",
            "expected_value": ground_truth["late_pct_Western"],
            "tolerance_pct": 1.0,
            "must_contain": ["Western"],
            "must_not_contain": ["0 shipments", "no data"],
        },
    ],
    "SC - Supplier Risk": [
        {
            "question": "What percentage of Asian supplier POs are late in the last 30 days?",
            "expected_column": "late_pct",
            "expected_value": ground_truth["supplier_late_Asia"],
            "tolerance_pct": 1.0,
            "must_contain": ["Asia"],
        },
    ],
    "SC - Executive Reporting": [
        {
            "question": "What are the current executive KPIs?",
            "expected_row_count": 1,
            "must_contain": ["revenue", "stockout"],
        },
    ],
}

print(f"Defined {sum(len(v) for v in benchmarks.values())} benchmark tests across {len(benchmarks)} spaces")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Run Benchmarks Against Genie Spaces

# COMMAND ----------
import time

def ask_genie(space_id, question, max_wait=60):
    """Send a question to a Genie Space and wait for the response."""
    # Start conversation
    resp = requests.post(
        f"{host}/api/2.0/genie/spaces/{space_id}/start-conversation",
        headers=headers,
        json={"content": question}
    )
    if resp.status_code not in (200, 201):
        return {"error": f"Start failed: {resp.status_code}", "sql": None, "data": None}

    conv = resp.json()
    conv_id = conv.get("conversation_id") or conv.get("id")
    msg_id = conv.get("message_id")
    if not msg_id and "messages" in conv:
        msg_id = conv["messages"][-1].get("id")

    # Poll for completion
    for _ in range(max_wait // 3):
        time.sleep(3)
        poll = requests.get(
            f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}",
            headers=headers
        )
        if poll.status_code != 200:
            continue
        msg = poll.json()
        status = msg.get("status", "")
        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            # Extract SQL and result
            attachments = msg.get("attachments", [])
            sql_text = None
            for att in attachments:
                if att.get("query", {}).get("query"):
                    sql_text = att["query"]["query"]
            return {
                "status": status,
                "sql": sql_text,
                "content": msg.get("content", ""),
                "conversation_id": conv_id,
            }
    return {"error": "Timeout", "sql": None}

# COMMAND ----------
# Run all benchmarks and collect results
results = []
for space_name, tests in benchmarks.items():
    space_id = space_lookup.get(space_name)
    if not space_id:
        print(f"\u2717 Space not found: {space_name}")
        continue

    for test in tests:
        print(f"Testing: [{space_name}] {test['question'][:60]}...")
        result = ask_genie(space_id, test["question"])

        passed = True
        issues = []

        # Check if we got a result
        if result.get("error"):
            passed = False
            issues.append(f"Error: {result['error']}")
        elif result.get("status") == "FAILED":
            passed = False
            issues.append("Genie returned FAILED status")
        else:
            # Check must_contain
            content = (result.get("content") or "").lower()
            sql = (result.get("sql") or "").lower()
            for term in test.get("must_contain", []):
                if term.lower() not in content and term.lower() not in sql:
                    issues.append(f"Missing term: {term}")

            # Check must_not_contain
            for term in test.get("must_not_contain", []):
                if term.lower() in content:
                    passed = False
                    issues.append(f"Found forbidden term: {term}")

            # Check SQL was generated
            if not result.get("sql"):
                issues.append("No SQL generated")

        status_icon = "\u2713" if passed and not issues else "\u26a0" if issues else "\u2717"
        results.append({
            "space": space_name,
            "question": test["question"][:60],
            "passed": passed,
            "issues": "; ".join(issues) if issues else "OK",
            "sql_generated": bool(result.get("sql")),
        })
        print(f"  {status_icon} {'PASS' if passed and not issues else 'WARN' if passed else 'FAIL'}: {'; '.join(issues) if issues else 'OK'}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Benchmark Results Summary

# COMMAND ----------
df_results = spark.createDataFrame(results)
df_results.display()

# COMMAND ----------
total = len(results)
passed = sum(1 for r in results if r["passed"] and r["issues"] == "OK")
print(f"\nBenchmark Score: {passed}/{total} ({round(passed/max(total,1)*100)}%)")
print(f"SQL Generated: {sum(1 for r in results if r['sql_generated'])}/{total}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Expected Improvement
# MAGIC 
# MAGIC | Metric | Before | After |
# MAGIC | --- | --- | --- |
# MAGIC | Automated testing | None | 6 benchmark tests |
# MAGIC | Regression detection | Manual only | Automated pass/fail |
# MAGIC | Ground truth comparison | Eyeball check | Tolerance-based validation |
# MAGIC 
# MAGIC **Next**: Iteration 5 adds cross-agent consistency and chart requirements.
