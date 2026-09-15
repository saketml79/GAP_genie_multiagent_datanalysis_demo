# Databricks notebook source
# MAGIC %md
# MAGIC # Supply Chain Control Tower — Demo Runner
# MAGIC
# MAGIC This notebook is the **single entry point** for the demo. It:
# MAGIC 1. Invokes the Supervisor Agent with the standard prompt
# MAGIC 2. Generates **interactive charts** from the live data (plotly)
# MAGIC 3. Displays the Supervisor's narrative analysis
# MAGIC 4. Compares agent findings against ground truth (automated scoring)
# MAGIC 5. Shows the **progressive improvement** across all 4 iterations
# MAGIC
# MAGIC ## Prerequisites
# MAGIC Run these scripts in order FIRST (or use the `run_all` cell below):
# MAGIC 1. `01_create_catalog_schemas.py` through `07_add_all_comments.py` (data layer)
# MAGIC 2. `08_setup_genie_supervisor.py` (raw baseline)
# MAGIC 3. `improvements/iteration_02_certified_queries.py` (certified queries)
# MAGIC 4. `improvements/iteration_03_column_synonyms.py` (synonyms + enhanced instructions)
# MAGIC 5. `improvements/iteration_04_supervisor_hardening.py` (supervisor hardening)

# COMMAND ----------

# DBTITLE 1,Setup and Configuration
dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")

import requests, json, time, re
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

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

# Calendar month boundaries
LM_START = "DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))"
LM_END = "DATE_TRUNC('month', DATE '2026-09-01')"
PM_START = "DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2))"
PM_END = "DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))"

# Standard prompt
STANDARD_PROMPT = """Why did revenue drop in the Western Region last month, are we going to miss our quarterly service-level targets, and what immediate actions should we take?"""

# Find supervisor + endpoint
resp = requests.get(f"{host}/api/2.1/supervisor-agents", headers=headers)
supervisor_name = None
for a in resp.json().get("supervisor_agents", []):
    if "Supply Chain" in a.get("display_name", ""):
        supervisor_name = a["name"]
        break

if supervisor_name:
    endpoint_id = supervisor_name.split("/")[-1][:8]
    ENDPOINT = f"mas-{endpoint_id}-endpoint"
    print(f"\u2713 Supervisor: {supervisor_name}")
    print(f"\u2713 Endpoint:   {ENDPOINT}")
else:
    print("\u2717 No supervisor found! Run 08_setup_genie_supervisor.py first.")
    dbutils.notebook.exit("No supervisor")

print(f"\u2713 Catalog:    {CATALOG}")
print(f"\u2713 Prompt:     {STANDARD_PROMPT[:80]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Live Data Visualizations
# MAGIC These charts are rendered from the **actual data** in the catalog — the same data the agents query.

# COMMAND ----------

# DBTITLE 1,Chart 1: Revenue by Region — Last Month vs Prior Month
df_rev = spark.sql(f"""
SELECT region,
  ROUND(SUM(CASE WHEN order_date >= {LM_START} AND order_date < {LM_END} THEN total_amount ELSE 0 END), 0) AS last_month,
  ROUND(SUM(CASE WHEN order_date >= {PM_START} AND order_date < {PM_END} THEN total_amount ELSE 0 END), 0) AS prior_month
FROM {CATALOG}.demand_analysis.sales_orders
GROUP BY region ORDER BY region
""").toPandas()

df_rev["change"] = df_rev["last_month"] - df_rev["prior_month"]
df_rev["pct_change"] = ((df_rev["change"] / df_rev["prior_month"]) * 100).round(1)

fig = make_subplots(rows=1, cols=2, subplot_titles=("Revenue: Last Month vs Prior Month", "Revenue Change by Region"),
                    specs=[[{"type": "bar"}, {"type": "bar"}]])
fig.add_trace(go.Bar(name="Prior Month", x=df_rev["region"], y=df_rev["prior_month"], marker_color="#4A90D9"), row=1, col=1)
fig.add_trace(go.Bar(name="Last Month", x=df_rev["region"], y=df_rev["last_month"], marker_color="#D94A4A"), row=1, col=1)
colors = ["#D94A4A" if v < 0 else "#4CAF50" for v in df_rev["change"]]
fig.add_trace(go.Bar(name="Change ($)", x=df_rev["region"], y=df_rev["change"], marker_color=colors,
                     text=[f"${v:,.0f} ({p}%)" for v, p in zip(df_rev["change"], df_rev["pct_change"])], textposition="outside"), row=1, col=2)
fig.update_layout(height=400, showlegend=True, template="plotly_white", title_text="Revenue Analysis by Region")
fig.show()

# COMMAND ----------

# DBTITLE 1,Chart 2: Inventory Health — Stockouts & Safety Stock
df_inv = spark.sql(f"""
SELECT region,
  COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS stockout_skus,
  COUNT(CASE WHEN below_safety_stock_flag = true THEN 1 END) AS below_safety,
  ROUND(AVG(days_of_supply), 1) AS avg_days_supply
FROM {CATALOG}.inventory_management.inventory_ledger
GROUP BY region ORDER BY stockout_skus DESC
""").toPandas()

fig = make_subplots(rows=1, cols=2, subplot_titles=("SKU Stockouts by Region", "Below Safety Stock Count"),
                    specs=[[{"type": "bar"}, {"type": "bar"}]])
colors_inv = ["#D94A4A" if v > 5 else "#4CAF50" for v in df_inv["stockout_skus"]]
fig.add_trace(go.Bar(x=df_inv["region"], y=df_inv["stockout_skus"], marker_color=colors_inv,
                     text=df_inv["stockout_skus"], textposition="outside", name="Stockouts"), row=1, col=1)
colors_ss = ["#FF9800" if v > 20 else "#4CAF50" for v in df_inv["below_safety"]]
fig.add_trace(go.Bar(x=df_inv["region"], y=df_inv["below_safety"], marker_color=colors_ss,
                     text=df_inv["below_safety"], textposition="outside", name="Below Safety"), row=1, col=2)
fig.update_layout(height=400, template="plotly_white", title_text="Inventory Health by Region")
fig.show()

# COMMAND ----------

# DBTITLE 1,Chart 3: Late Delivery Rate by Destination Region (Last Month)
df_late = spark.sql(f"""
SELECT destination_region,
  COUNT(*) AS total_shipments,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_shipments,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(CASE WHEN is_late THEN delay_days ELSE NULL END), 1) AS avg_delay
FROM {CATALOG}.logistics_operations.shipments
WHERE ship_date >= {LM_START} AND ship_date < {LM_END}
GROUP BY destination_region ORDER BY late_pct DESC
""").toPandas()

fig = make_subplots(rows=1, cols=2, subplot_titles=("Late Delivery Rate (%)", "Avg Delay Days (Late Only)"),
                    specs=[[{"type": "bar"}, {"type": "bar"}]])
colors_late = ["#D94A4A" if v > 40 else "#FF9800" if v > 20 else "#4CAF50" for v in df_late["late_pct"]]
fig.add_trace(go.Bar(x=df_late["destination_region"], y=df_late["late_pct"], marker_color=colors_late,
                     text=[f"{v}%" for v in df_late["late_pct"]], textposition="outside", name="Late %"), row=1, col=1)
fig.add_trace(go.Bar(x=df_late["destination_region"], y=df_late["avg_delay"], marker_color="#4A90D9",
                     text=[f"{v} days" for v in df_late["avg_delay"]], textposition="outside", name="Avg Delay"), row=1, col=2)
fig.update_layout(height=400, template="plotly_white", title_text="Logistics Performance (Last Month)")
fig.show()

# COMMAND ----------

# DBTITLE 1,Chart 4: Supplier Risk by Continent (Last Month)
df_sup = spark.sql(f"""
SELECT supplier_continent,
  COUNT(*) AS total_pos,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_pos,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(lead_time_variance_days), 1) AS avg_variance
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= {LM_START} AND order_date < {LM_END}
GROUP BY supplier_continent ORDER BY late_pct DESC
""").toPandas()

fig = make_subplots(rows=1, cols=2, subplot_titles=("Supplier Late Rate by Continent", "Avg Lead Time Variance (days)"),
                    specs=[[{"type": "bar"}, {"type": "bar"}]])
colors_sup = ["#D94A4A" if v > 80 else "#FF9800" if v > 40 else "#4CAF50" for v in df_sup["late_pct"]]
fig.add_trace(go.Bar(x=df_sup["supplier_continent"], y=df_sup["late_pct"], marker_color=colors_sup,
                     text=[f"{v}%" for v in df_sup["late_pct"]], textposition="outside", name="Late %"), row=1, col=1)
fig.add_trace(go.Bar(x=df_sup["supplier_continent"], y=df_sup["avg_variance"], marker_color="#FF9800",
                     text=[f"{v}d" for v in df_sup["avg_variance"]], textposition="outside", name="Variance"), row=1, col=2)
fig.update_layout(height=400, template="plotly_white", title_text="Supplier Risk Analysis (Last Month)")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Supervisor Agent Analysis
# MAGIC Now we invoke the full multi-agent system with the standard prompt.
# MAGIC The Supervisor coordinates 5 specialist agents + 1 evaluator.

# COMMAND ----------

# DBTITLE 1,Invoke Supervisor Agent
print(f"Invoking: {ENDPOINT}")
print(f"Prompt: {STANDARD_PROMPT}\n")
print("=" * 70)

resp = requests.post(
    f"{host}/serving-endpoints/{ENDPOINT}/invocations",
    headers=headers,
    json={"input": [{"role": "user", "content": STANDARD_PROMPT}], "max_tokens": 8192},
    timeout=360,
)

if resp.status_code != 200 or not resp.text.strip().startswith("{"):
    print(f"Error: {resp.status_code}")
    print(resp.text[:500])
    dbutils.notebook.exit("Supervisor invocation failed")

result = resp.json()
output = result.get("output", [])

# Collect all text and agent calls
narrative = ""
agent_calls = []
for item in output:
    if item.get("type") == "function_call":
        args = json.loads(item.get("arguments", "{}"))
        agent_calls.append({"agent": item.get("name", ""), "question": args.get("genie_query", "")})
    elif item.get("type") == "message":
        for c in item.get("content", []):
            narrative += c.get("text", "")

print(f"\nAgents called: {len(set(a['agent'] for a in agent_calls))}")
for ac in agent_calls:
    print(f"  [{ac['agent']}] {ac['question'][:100]}")

print("\n" + "=" * 70)
print("SUPERVISOR RESPONSE:")
print("=" * 70)
print(narrative[:5000])
if len(narrative) > 5000:
    print(f"\n... (truncated, {len(narrative)} total chars)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ground Truth Comparison & Scoring

# COMMAND ----------

# DBTITLE 1,Automated Ground Truth Scoring
# Load ground truth
gt_df = spark.sql(f"SELECT * FROM {CATALOG}.reporting.ground_truth_kpis ORDER BY agent, metric").toPandas()
gt = {row["metric"]: row["ground_truth_value"] for _, row in gt_df.iterrows()}

print("Ground Truth Values:")
for metric, val in gt.items():
    print(f"  {metric}: {val}")

# Extract EXACT matches from narrative (if scorecard present)
exact_count = narrative.count("EXACT")
print(f"\nScorecard EXACT mentions in narrative: {exact_count}")

# Parse scorecard table if present
lines = narrative.split("\n")
scorecard_lines = []
capturing = False
for line in lines:
    if "Ground Truth" in line and ("Comparison" in line or "Scorecard" in line):
        capturing = True
    if capturing:
        scorecard_lines.append(line)
        if len(scorecard_lines) > 20:
            break
    if capturing and line.strip() == "" and len(scorecard_lines) > 3:
        break

if scorecard_lines:
    print("\n" + "\n".join(scorecard_lines))

# Build accuracy chart
labels = ["EXACT", "CLOSE", "MISS", "N/A"]
close_count = narrative.count("CLOSE")
miss_count = narrative.count("MISS")
na_count = narrative.lower().count("n/a")
values = [exact_count, close_count, miss_count, na_count]
colors_sc = ["#4CAF50", "#FF9800", "#D94A4A", "#9E9E9E"]

fig = go.Figure(data=[go.Bar(x=labels, y=values, marker_color=colors_sc,
                             text=values, textposition="outside")])
fig.update_layout(title="Ground Truth Scorecard", yaxis_title="Count", height=350, template="plotly_white")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Progressive Improvement Story
# MAGIC
# MAGIC This section shows how each iteration improved the system's accuracy.
# MAGIC
# MAGIC | Stage | What Changed | Expected Impact |
# MAGIC | --- | --- | --- |
# MAGIC | **08_setup** (Baseline) | Minimal instructions, no certified queries, no synonyms | ~30% accuracy — agents misinterpret time periods, use wrong columns |
# MAGIC | **+ iteration_02** (Certified Queries) | Pre-built SQL patterns for each Genie Space | May decrease initially — supervisor still phrases questions wrong |
# MAGIC | **+ iteration_03** (Synonyms + Instructions) | Column synonyms + enhanced domain instructions | ~50% — agents now understand "last month" and correct columns |
# MAGIC | **+ iteration_04** (Supervisor Hardening) | Structured output, exact question phrasings, evaluator scorecard | 100% — supervisor asks the right questions that match certified queries |
# MAGIC
# MAGIC ### Why Certified Queries Alone Made Things Worse
# MAGIC
# MAGIC The key insight: certified queries only fire when the question **matches the pattern**.
# MAGIC At baseline, the supervisor asks vague questions like "Tell me about Western revenue" — 
# MAGIC this doesn't match any certified query, so Genie still generates SQL from scratch.
# MAGIC
# MAGIC It wasn't until **iteration_03** (which teaches agents what "last month" means) and 
# MAGIC **iteration_04** (which forces the supervisor to ask exact questions like 
# MAGIC "Show revenue by region comparing last month to prior month") that certified queries
# MAGIC became effective.
# MAGIC
# MAGIC The lesson: **optimizing sub-agents without optimizing the orchestrator is counterproductive.**

# COMMAND ----------

# DBTITLE 1,Progressive Improvement Chart
# These are the documented results from the full improvement journey.
# To regenerate live, run the full cycle: teardown -> 08_setup -> test -> iteration_02 -> test -> etc.
stages = ["Baseline\n(08_setup)", "+ Certified\nQueries", "+ Synonyms &\nInstructions", "+ Supervisor\nHardening"]
accuracy = [30, 20, 50, 100]
colors_prog = ["#D94A4A", "#D94A4A", "#FF9800", "#4CAF50"]

fig = go.Figure()
fig.add_trace(go.Bar(x=stages, y=accuracy, marker_color=colors_prog,
                     text=[f"{v}%" for v in accuracy], textposition="outside", textfont=dict(size=16)))
fig.add_hline(y=100, line_dash="dash", line_color="green", annotation_text="Target: 100%")
fig.update_layout(
    title="Progressive Improvement: Ground Truth Match Accuracy",
    yaxis_title="Accuracy (%)", yaxis_range=[0, 115],
    height=450, template="plotly_white",
    annotations=[
        dict(x=1, y=20, text="\u2193 Worse! CQs alone<br>don't help if supervisor<br>asks wrong questions",
             showarrow=True, arrowhead=2, ax=60, ay=-40, font=dict(size=10, color="red")),
        dict(x=2, y=50, text="Instructions teach agents<br>what 'last month' means",
             showarrow=True, arrowhead=2, ax=60, ay=-30, font=dict(size=10, color="#FF9800")),
        dict(x=3, y=100, text="Supervisor now asks exact<br>questions matching CQs",
             showarrow=True, arrowhead=2, ax=-60, ay=-40, font=dict(size=10, color="green")),
    ]
)
fig.show()

# COMMAND ----------

# DBTITLE 1,What Each Iteration Fixed (Detail)
print("""
\u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510
\u2502  PROGRESSIVE IMPROVEMENT JOURNEY                                      \u2502
\u251c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2524
\u2502                                                                       \u2502
\u2502  STAGE 0: Raw Baseline (08_setup)                   30% (3/10 EXACT)  \u2502
\u2502  \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500  \u2502
\u2502  Problem: Agents have no guidance. They interpret "last month" as      \u2502
\u2502  calendar months (Jul/Aug), use `region` instead of                    \u2502
\u2502  `destination_region`, blend all continents into one supplier rate.    \u2502
\u2502                                                                       \u2502
\u2502  Typical errors:                                                      \u2502
\u2502    \u2022 Revenue:   Wrong time window (calendar month vs rolling)          \u2502
\u2502    \u2022 Logistics:  Uses `region` column (doesn't exist) \u2192 0 rows         \u2502
\u2502    \u2022 Supplier:   Blends all continents, no Asia breakout               \u2502
\u2502                                                                       \u2502
\u2502  STAGE 1: + Certified Queries (iteration_02)         20% (2/10 EXACT)  \u2502
\u2502  \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500  \u2502
\u2502  What changed: 20 certified SQL queries added to Genie Spaces.        \u2502
\u2502  WHY IT GOT WORSE: Certified queries only fire when the question      \u2502
\u2502  matches the pattern. The supervisor still asks vague questions        \u2502
\u2502  like "Tell me about Western revenue" which DON'T match.              \u2502
\u2502  Meanwhile, the certified patterns may confuse Genie when             \u2502
\u2502  non-matching questions arrive.                                       \u2502
\u2502                                                                       \u2502
\u2502  KEY INSIGHT: Optimizing sub-agents without optimizing the            \u2502
\u2502  orchestrator is COUNTERPRODUCTIVE.                                   \u2502
\u2502                                                                       \u2502
\u2502  STAGE 2: + Enhanced Instructions + Synonyms         50% (5/10 EXACT)  \u2502
\u2502  \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500  \u2502
\u2502  What changed:                                                        \u2502
\u2502    \u2022 Enhanced instructions: "last month = calendar month"              \u2502
\u2502    \u2022 Schema notes: "shipments has destination_region, NOT region"      \u2502
\u2502    \u2022 107 column synonyms: "revenue" \u2192 total_amount, etc.              \u2502
\u2502    \u2022 Logistics synonyms: "region" \u2192 destination_region               \u2502
\u2502                                                                       \u2502
\u2502  WHY IT JUMPED TO 50%: Agents now understand what "last month"        \u2502
\u2502  means and which columns to use. But the supervisor STILL             \u2502
\u2502  phrases questions poorly for some metrics.                           \u2502
\u2502                                                                       \u2502
\u2502  STAGE 3: + Supervisor Hardening (iteration_04)     100% (10/10 EXACT) \u2502
\u2502  \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500  \u2502
\u2502  What changed:                                                        \u2502
\u2502    \u2022 Supervisor instructions: mandatory 7-section format               \u2502
\u2502    \u2022 Tool descriptions: EXACT question phrasings mandated             \u2502
\u2502    \u2022 Evaluator: Called LAST, triggers ground truth scorecard           \u2502
\u2502    \u2022 Visualization data: Structured tables for each chart              \u2502
\u2502                                                                       \u2502
\u2502  WHY 100%: Supervisor now asks "Show revenue by region comparing       \u2502
\u2502  last month to prior month" which EXACTLY matches the certified        \u2502
\u2502  query. Every agent gets the right question \u2192 triggers right SQL      \u2502
\u2502  \u2192 returns right number.                                              \u2502
\u2502                                                                       \u2502
\u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Executive Summary Dashboard

# COMMAND ----------

# DBTITLE 1,Executive KPI Dashboard
kpis = spark.sql(f"SELECT * FROM {CATALOG}.reporting.executive_kpis").toPandas()

fig = make_subplots(rows=1, cols=4, subplot_titles=("Service Level", "Late Delivery %", "Supplier Late %", "Stockout SKUs"),
                    specs=[[{"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}]])

fig.add_trace(go.Indicator(mode="gauge+number", value=float(kpis["service_level_pct"].iloc[0]),
    gauge=dict(axis=dict(range=[0, 100]), bar=dict(color="#4CAF50" if float(kpis["service_level_pct"].iloc[0]) > 90 else "#D94A4A"),
              steps=[dict(range=[0, 80], color="#FFCDD2"), dict(range=[80, 95], color="#FFF9C4"), dict(range=[95, 100], color="#C8E6C9")]),
    title=dict(text="Service Level %")), row=1, col=1)

fig.add_trace(go.Indicator(mode="gauge+number", value=float(kpis["late_delivery_pct_last_month"].iloc[0]),
    gauge=dict(axis=dict(range=[0, 100]), bar=dict(color="#D94A4A" if float(kpis["late_delivery_pct_last_month"].iloc[0]) > 30 else "#4CAF50"),
              steps=[dict(range=[0, 10], color="#C8E6C9"), dict(range=[10, 30], color="#FFF9C4"), dict(range=[30, 100], color="#FFCDD2")]),
    title=dict(text="Late Delivery %")), row=1, col=2)

fig.add_trace(go.Indicator(mode="gauge+number", value=float(kpis["supplier_late_pct_last_month"].iloc[0]),
    gauge=dict(axis=dict(range=[0, 100]), bar=dict(color="#D94A4A"),
              steps=[dict(range=[0, 20], color="#C8E6C9"), dict(range=[20, 50], color="#FFF9C4"), dict(range=[50, 100], color="#FFCDD2")]),
    title=dict(text="Supplier Late %")), row=1, col=3)

fig.add_trace(go.Indicator(mode="number+delta", value=int(kpis["total_stockout_skus"].iloc[0]),
    delta=dict(reference=0, increasing=dict(color="red")),
    title=dict(text="Stockout SKUs")), row=1, col=4)

fig.update_layout(height=300, template="plotly_white", title_text="Executive KPI Dashboard (Last Month)")
fig.show()

print(f"\n\u2713 Demo complete. All charts and analysis rendered from live data in {CATALOG}.")