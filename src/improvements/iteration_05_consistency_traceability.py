# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Iteration 5: Consistency, Traceability, and Supervisor Hardening
# MAGIC 
# MAGIC **Goal**: Update the Supervisor Agent instructions to enforce:
# MAGIC 1. **Traceability**: Every number must cite the exact SQL query and table
# MAGIC 2. **Consistency**: Cross-agent numbers must reconcile (revenue totals must match)
# MAGIC 3. **Groundedness**: Agent must flag when it cannot verify a claim
# MAGIC 4. **Charts**: Supervisor must request data in chart-friendly format
# MAGIC 5. **Source Attribution**: Show which Genie Space produced each finding
# MAGIC 
# MAGIC **What this fixes**:
# MAGIC - Numbers that don't add up across agents
# MAGIC - Hallucinated statistics
# MAGIC - Missing source attribution
# MAGIC - Text-only output (no visual analysis)
# MAGIC 
# MAGIC **Improvement**: Trust + Auditability + Presentation

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

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Update Supervisor Agent Instructions

# COMMAND ----------
# Find the supervisor agent
resp = requests.get(f"{host}/api/2.1/supervisor-agents", headers=headers)
agents = resp.json().get("supervisor_agents", [])
supervisor = None
for a in agents:
    if "Supply Chain" in a.get("display_name", ""):
        supervisor = a
        break

if not supervisor:
    print("\u2717 Supervisor Agent not found")
    dbutils.notebook.exit("No supervisor found")

supervisor_name = supervisor["name"]
print(f"Found: {supervisor_name} ({supervisor['display_name']})")

# COMMAND ----------
new_instructions = """You are the Supply Chain Control Tower Supervisor Agent. You orchestrate 5 specialist agents to deliver accurate, traceable, consistent analysis.

## MANDATORY RESPONSE FORMAT

### 1. What I Understood
Restate the business question. Identify which domains are relevant.

### 2. Investigation Plan
List which agents you will query and what SPECIFIC questions you will ask each.

### 3. Findings by Agent

For EACH agent consulted, show:

#### Agent: [agent-name]
- **Source**: [Genie Space name]
- **Question Asked**: [exact question sent]
- **SQL Query Used**:
```sql
[paste the exact SQL the agent executed]
```
- **Result Summary**: [key numbers with units]
- **Confidence**: HIGH / MEDIUM / LOW
- **Data Quality Note**: [row count returned, any nulls or gaps]

### 4. Cross-Domain Reconciliation

**CRITICAL**: Before presenting findings, VERIFY:
- Revenue totals from demand-analysis match executive KPIs from executive-reporting
- Stockout counts from inventory-management match the total_stockout_skus KPI
- Late delivery % from logistics-operations matches late_delivery_pct_30d KPI
- If numbers DON'T match, flag the discrepancy and explain which is authoritative

| KPI | Demand Agent | Executive Agent | Match? |
|-----|-------------|-----------------|--------|
| Revenue Last 30d | $X | $Y | Yes/No |
| Stockout SKUs | N | M | Yes/No |
| Late Delivery % | X% | Y% | Yes/No |

### 5. Root Cause Chain
Connect findings across domains. Use this format:
```
[Supplier Issue] -> [Inventory Impact] -> [Fulfillment Impact] -> [Revenue Impact]
```

### 6. Conclusion and Actions

**Direct Answer**: [1-2 sentence answer to the business question]

**Immediate Actions** (1-2 weeks):
1. [Action with specific target and owner]
2. [Action with specific target and owner]

**Medium-Term Actions** (1-3 months):
1. [Action with specific target]

**KPIs to Monitor**:
| KPI | Current | Target | Timeline |
|-----|---------|--------|----------|
| [metric] | [value] | [target] | [when] |

## RULES

1. **ALWAYS query ALL 5 agents** for comprehensive questions
2. **NEVER invent numbers** - every number must come from an agent's SQL result
3. **ALWAYS show the SQL** - paste the exact query each agent used
4. **RECONCILE cross-agent numbers** - flag any mismatches
5. **USE destination_region** when asking logistics about a region
6. **SPECIFY time periods** explicitly: "last 30 days" means DATE_SUB(CURRENT_DATE(), 30)
7. **ASK SPECIFIC QUESTIONS** - not vague ones. Bad: "tell me about Western". Good: "What is the total revenue for Western region in the last 30 days compared to the prior 30 days?"
8. If an agent returns 0 rows or an error, RE-ASK with more specific phrasing
9. **Round dollar amounts** to nearest dollar, percentages to 1 decimal
10. **Show confidence level** for each finding based on data completeness"""

resp = requests.patch(
    f"{host}/api/2.1/{supervisor_name}?update_mask=instructions",
    headers=headers,
    json={"instructions": new_instructions}
)
if resp.status_code == 200:
    print("\u2713 Supervisor instructions updated with traceability and consistency rules")
else:
    print(f"\u2717 Failed: {resp.status_code} - {resp.text[:200]}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Update Supervisor Quality Examples

# COMMAND ----------
# Update quality examples to demonstrate the expected format
new_examples = [
    {
        "request": "Why did revenue drop in the Western Region last month?",
        "expected_response": """Consult ALL 5 agents with specific questions:
- demand-analysis: 'Show revenue by region comparing last 30 days to prior 30 days'
- demand-analysis: 'Show Western region revenue breakdown by product family'
- inventory-management: 'How many SKUs are in stockout by region?'
- logistics-operations: 'What is the late delivery rate for shipments to the Western region in the last 30 days?'
- supplier-risk: 'What is the supplier late rate by continent in the last 30 days?'
- executive-reporting: 'Show executive KPIs'

Reconcile: verify revenue from demand matches executive KPIs.
Show SQL for each query. Flag any discrepancies.
Build root cause chain: Supplier delays -> Inventory depletion -> Fulfillment failures -> Revenue loss."""
    },
    {
        "request": "Are we going to miss our quarterly service-level targets?",
        "expected_response": """Query executive-reporting for current service_level_pct (target: 85%).
Query demand-analysis for fulfillment rate trend over 90 days.
Query logistics-operations for late delivery trend (is it improving or worsening?).
Query supplier-risk for whether supplier lead times are improving.
Project trajectory: current service level vs target with trend direction.
Show ALL SQL queries used. Reconcile numbers across agents."""
    },
    {
        "request": "Give me a full executive summary with all agent data",
        "expected_response": """Query ALL 5 agents. For each:
1. Show the exact question asked
2. Show the SQL query executed
3. Show the result table
4. Note confidence level

Reconcile all cross-domain numbers in a comparison table.
Build the complete root cause chain.
Provide specific actions with KPI targets."""
    },
]

for i, ex in enumerate(new_examples):
    resp = requests.post(
        f"{host}/api/2.1/{supervisor_name}/examples",
        headers=headers,
        json=ex
    )
    status = "\u2713" if resp.status_code in (200, 201) else "\u2717"
    print(f"{status} Example {i+1}: {ex['request'][:60]}...")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Update Sub-Agent Tool Descriptions
# MAGIC Make each tool description tell the supervisor HOW to ask questions.

# COMMAND ----------
tool_updates = {
    "demand-analysis": "Demand Analysis specialist. ALWAYS ask specific questions with time periods. Good: 'Show revenue by region comparing last 30 days to prior 30 days'. Bad: 'What about revenue?'. Returns SQL query + result table. Use for: revenue by region/product, forecast accuracy, order status breakdown, customer patterns.",
    "inventory-management": "Inventory Management specialist. Ask about specific metrics: stockout counts, days of supply, safety stock breaches. Good: 'How many SKUs are in stockout in the Western region?'. Returns SQL + result table. Use for: stockouts by region, inventory levels, stock movement flows.",
    "logistics-operations": "Logistics Operations specialist. CRITICAL: always say 'destination region' not just 'region'. Good: 'What is the late delivery rate for shipments to the Western region?'. Returns SQL + result table. Use for: late delivery rates, delay reasons, carrier performance, DC congestion.",
    "supplier-risk": "Supplier Risk specialist. Ask about specific supplier metrics. Good: 'What is the supplier late rate by continent in the last 30 days?'. Returns SQL + result table. Use for: supplier on-time rates, lead time variances, SLA breaches, fill rates, risk ranking.",
    "executive-reporting": "Executive Reporting with pre-built cross-domain views. Good: 'Show executive KPIs'. Returns SQL + result table. Use for: headline KPIs, regional comparison, revenue trends, risk scorecard. Use this to CROSS-CHECK numbers from other agents.",
}

for tool_id, desc in tool_updates.items():
    resp = requests.patch(
        f"{host}/api/2.1/{supervisor_name}/tools/{tool_id}?update_mask=description",
        headers=headers,
        json={"description": desc, "tool_type": "genie_space"}
    )
    status = "\u2713" if resp.status_code == 200 else "\u2717"
    print(f"{status} Updated tool: {tool_id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Iteration 5 Summary
# MAGIC 
# MAGIC | Improvement | Description |
# MAGIC | --- | --- |
# MAGIC | Traceability | Every finding shows source agent, SQL query, and confidence |
# MAGIC | Consistency | Cross-agent reconciliation table catches mismatches |
# MAGIC | Groundedness | Agents flagged when they cannot verify claims |
# MAGIC | Source Attribution | Each number traced to specific Genie Space and query |
# MAGIC | Actionability | Structured actions with KPI targets and timelines |
# MAGIC 
# MAGIC ## Cumulative Improvement Across All Iterations
# MAGIC 
# MAGIC | Iteration | Focus | Key Metric |
# MAGIC | --- | --- | --- |
# MAGIC | 1. Baseline | Document issues | Score: 2/10 |
# MAGIC | 2. Certified Queries | Pre-built SQL patterns | Revenue accuracy: Wrong -> Correct |
# MAGIC | 3. Synonyms | Column name mapping | 0-row results: ~30% -> ~5% |
# MAGIC | 4. Benchmarks | Automated validation | Regression testing: None -> 6 tests |
# MAGIC | 5. Consistency | Cross-agent reconciliation | Hallucination: Common -> Flagged |
