# Databricks notebook source
# MAGIC %md
# MAGIC # Iteration 4: Supervisor Hardening (Consistency and Traceability)
# MAGIC
# MAGIC ## Why This Step Improves Output
# MAGIC
# MAGIC Iterations 2 and 3 fixed the **sub-agents** -- each Genie Space now returns correct, robust numbers.
# MAGIC But the **Supervisor Agent** that orchestrates them can still:
# MAGIC
# MAGIC | Failure Mode | Example | Root Cause |
# MAGIC | --- | --- | --- |
# MAGIC | Vague questions | Supervisor asks "Tell me about Western region" instead of a specific question | No prompting guidance |
# MAGIC | Missing agents | Only queries 3 of 5 spaces for a cross-domain question | No routing mandate |
# MAGIC | No SQL traceability | Reports numbers without showing which query produced them | No format requirement |
# MAGIC | Inconsistent framing | Revenue from Demand says -$1.23M, Executive says -$1.7M, no reconciliation | No cross-check step |
# MAGIC | Disconnected findings | Lists 5 findings but doesn't connect them into a causal chain | No root cause template |
# MAGIC
# MAGIC **The fix**: We update the Supervisor's instructions to enforce:
# MAGIC 1. **Mandatory 6-section format** (Understanding / Plan / Findings / Reconciliation / Root Cause / Actions)
# MAGIC 2. **SQL citation** -- every number must show the SQL that produced it
# MAGIC 3. **Cross-domain reconciliation table** -- forces the supervisor to compare numbers across agents
# MAGIC 4. **Specific prompting** -- tool descriptions tell the supervisor exactly how to phrase questions
# MAGIC
# MAGIC ## How It Drives Consistency
# MAGIC
# MAGIC The reconciliation table is the key innovation. When the Supervisor is forced to put
# MAGIC "Demand says X, Executive says Y, Match?" in a table, it catches its own inconsistencies
# MAGIC **before** presenting to the user. This is self-correction through structured output.
# MAGIC
# MAGIC | Before (No Hardening) | After (Hardened) |
# MAGIC | --- | --- |
# MAGIC | Free-form narrative, numbers buried in prose | Structured 6-section format |
# MAGIC | No SQL shown | SQL cited for every finding |
# MAGIC | Numbers from different agents may contradict | Cross-domain reconciliation table catches mismatches |
# MAGIC | Root causes listed as bullets, not connected | Causal chain connects upstream -> downstream |
# MAGIC | Actions are generic | Actions have specific targets, owners, and timelines |

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
# MAGIC ---
# MAGIC ## Step 1: Find the Supervisor Agent

# COMMAND ----------

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
print(f"\u2713 Found: {supervisor_name} ({supervisor['display_name']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Update Supervisor Instructions
# MAGIC
# MAGIC The instructions define the **output contract**. Every response must follow this 6-section format.
# MAGIC This is not just cosmetic -- it forces the LLM to structure its reasoning, which reduces hallucination
# MAGIC and ensures cross-domain consistency.

# COMMAND ----------

# DBTITLE 1,Update Supervisor Instructions (Hardened)
new_instructions = """You are the Supply Chain Control Tower Supervisor Agent. You orchestrate 5 specialist agents + 1 evaluator.

## MANDATORY RESPONSE FORMAT (7 sections)

### 1. What I Understood
Restate the question and identify relevant domains.

### 2. Investigation Plan
List agents and specific questions for each.

### 3. Findings by Agent
For EACH agent:
#### Agent: [name]
- **Source**: [space name]
- **Questions Asked**: [list all questions sent]
- **SQL Query Used**: ```sql [exact SQL] ```
- **Result Summary**: key numbers with units
- **Confidence**: HIGH/MEDIUM/LOW
- **Data Quality Note**: row counts, completeness

### 4. Cross-Domain Reconciliation
| KPI | Agent Finding | Executive KPI | Ground Truth | Match? |
Include 10+ rows. Flag discrepancies.

### 5. Root Cause Chain
Numbered causal cascade: [1] SUPPLIER CRISIS → [2] INVENTORY COLLAPSE → [3] LOGISTICS BREAKDOWN → [4] DEMAND DESTRUCTION → [5] SERVICE LEVEL FAILURE. Include specific numbers at each stage. End with KEY INSIGHT.

### 6. Ground Truth Comparison Scorecard
| Agent | Metric | Agent Finding | Ground Truth | Match? |
Use: ✅ EXACT (<1%), ⚠️ CLOSE (<5%), ❌ MISS (>5%). ALWAYS include actual calendar month names in the Metric column -- e.g. "Western revenue change (Aug vs Jul 2026 MoM)" not just "Western revenue change". Use DATE_FORMAT to derive month names. End with "Scorecard Summary: X of 10 EXACT (Y%)."

### 7. Conclusion and Actions
**Direct Answer**: 2-3 sentences with key numbers.
**Immediate Actions** (1-2 weeks): table with #, Action, Owner, Target, Timeline
**Medium-Term Actions** (1-3 months): table with #, Action, Target, Timeline
**KPIs to Monitor**: table with KPI, Current, Target, Timeline
**Risk Assessment**: probability of missing targets.

## RULES
1. Query ALL 5 domain agents + evaluator LAST
2. NEVER invent numbers -- every number from agent SQL
3. Show the SQL each agent used
4. USE destination_region for logistics (no 'region' column in shipments)
5. "last month" = August 2026. The dataset reference date is September 1, 2026. Use date filters: >= '2026-08-01' AND < '2026-09-01'
6. ASK SPECIFIC QUESTIONS: "Show revenue by region comparing last month to prior month"
7. Re-ask if agent returns 0 rows
8. Round dollars to nearest dollar, percentages to 1 decimal
9. Product family subtotals must sum to regional total
10. Call evaluator LAST: 'Show all ground truth values'
11. Build Scorecard in Section 6
12. In ALL tables and findings, label time periods with actual month names (e.g. 'Aug 2026') not just 'last month'
13. For EVERY metric the prompt asks about, report a numeric answer OR explicitly state 'NOT_FOUND: [metric name] could not be determined from available data' -- never silently omit a requested metric
14. In Section 3, each agent MUST list metrics it could NOT answer under a 'Gaps' bullet -- e.g. 'Gaps: lead time variance not available in this agent's tables'"""

resp = requests.patch(
    f"{host}/api/2.1/{supervisor_name}?update_mask=instructions",
    headers=headers,
    json={"instructions": new_instructions}
)
status = "\u2713" if resp.status_code == 200 else "\u2717"
print(f"{status} Supervisor instructions updated ({len(new_instructions)} chars)")

# COMMAND ----------

# DBTITLE 1,Tool Description Intro
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Update Tool Descriptions
# MAGIC
# MAGIC Tool descriptions tell the Supervisor **how to phrase questions** for each sub-agent.
# MAGIC This is critical because the same question phrased differently can trigger different SQL.
# MAGIC
# MAGIC Key guidance embedded in descriptions:
# MAGIC - **Logistics**: Always say "destination region" not "region"
# MAGIC - **Demand**: Ask for "last month vs prior month"
# MAGIC - **Executive**: Use for cross-checking, not primary analysis

# COMMAND ----------

# DBTITLE 1,Update Supervisor Tool Descriptions (Hardened)
tool_updates = {
    "demand-analysis": "Demand Analysis specialist. Ask EXACTLY: 'Show revenue by region comparing last month to prior month -- August and July revenue, dollar change, and percentage change. Also show Western region revenue by product family last month vs prior month.' Report NOT_FOUND for any metric you cannot answer.",
    "inventory-management": "Inventory Management specialist. Ask EXACTLY: 'Show positions below safety stock, unique SKUs below safety stock, days of supply for at-risk items, stockout SKUs, and warehouses affected by region.' Report NOT_FOUND for any metric you cannot answer.",
    "logistics-operations": "Logistics Operations specialist. CRITICAL: use 'destination_region' not 'region'. Ask EXACTLY: 'What is the on-time delivery rate, late delivery rate, average delay days, total shipments, late shipments, and wasted freight cost by destination region last month?' Report NOT_FOUND for any metric you cannot answer.",
    "supplier-risk": "Supplier Risk specialist. Ask by continent not region. Ask EXACTLY: 'What is the supplier late rate, total purchase orders, late purchase orders, and average lead time variance by continent last month? Also show total SLA penalties.' Report NOT_FOUND for any metric you cannot answer.",
    "executive-reporting": "Executive Reporting specialist. Ask EXACTLY: 'Show executive KPIs including service level performance and Cost of Disruption by region.' Report NOT_FOUND for any metric you cannot answer.",
    "evaluator": "Evaluator with ground truth. Call LAST after all other agents. Ask EXACTLY: 'Show all ground truth values'",
}

for tool_id, desc in tool_updates.items():
    resp = requests.patch(
        f"{host}/api/2.1/{supervisor_name}/tools/{tool_id}?update_mask=description",
        headers=headers,
        json={"description": desc, "tool_type": "genie_space"}
    )
    status = "\u2713" if resp.status_code == 200 else "\u2717"
    print(f"{status} Tool: {tool_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## The Complete Picture: How All 4 Iterations Work Together
# MAGIC
# MAGIC ```
# MAGIC User Question: "Why did Western revenue drop?"
# MAGIC         |
# MAGIC         v
# MAGIC [Iteration 4: Supervisor Hardening]
# MAGIC   - Supervisor parses question, builds investigation plan
# MAGIC   - Routes to ALL 5 agents with SPECIFIC questions
# MAGIC   - Tool descriptions guide phrasing (e.g., "destination region")
# MAGIC         |
# MAGIC         v
# MAGIC [Iteration 3: Column Synonyms]
# MAGIC   - "revenue" -> total_amount, "western" -> "Western"
# MAGIC   - Natural language maps to correct columns regardless of phrasing
# MAGIC         |
# MAGIC         v
# MAGIC [Iteration 2: Certified Queries]
# MAGIC   - Question matches certified pattern -> use pre-built SQL
# MAGIC   - Correct time windows, correct columns, deterministic results
# MAGIC         |
# MAGIC         v
# MAGIC [Data Layer: 23 tables across 5 schemas]
# MAGIC   - Table/column comments guide Genie even for uncertified questions
# MAGIC         |
# MAGIC         v
# MAGIC [Iteration 4: Reconciliation]
# MAGIC   - Supervisor compares numbers across agents
# MAGIC   - Flags discrepancies, builds root cause chain
# MAGIC   - Presents structured 6-section response
# MAGIC ```
# MAGIC
# MAGIC | Layer | What It Fixes | Without It |
# MAGIC | --- | --- | --- |
# MAGIC | **Comments** (setup) | Genie understands table/column purpose | Genie guesses from column names alone |
# MAGIC | **Certified Queries** (Iter 2) | Correct SQL for known questions | Wrong time windows, wrong columns |
# MAGIC | **Synonyms** (Iter 3) | Business language -> column mapping | "revenue" doesn't match `total_amount` |
# MAGIC | **Supervisor Hardening** (Iter 4) | Structured output, cross-domain consistency | Numbers contradict, no audit trail |
# MAGIC
# MAGIC Each layer addresses a different failure mode. Remove any one and the system degrades:
# MAGIC - Without certified queries: numbers are wrong
# MAGIC - Without synonyms: natural language misses the right columns
# MAGIC - Without hardening: correct sub-agent results are presented inconsistently