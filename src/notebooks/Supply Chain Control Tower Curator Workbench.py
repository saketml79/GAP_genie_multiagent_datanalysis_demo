# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Supply Chain Control Tower — Full Pipeline
# MAGIC %md
# MAGIC # Supply Chain Control Tower — Full Pipeline
# MAGIC
# MAGIC **One-click E2E setup**: Tears down any existing deployment, regenerates all data, creates 5 Genie Agents + Supervisor Agent, then progressively improves accuracy across **3 iterations** using **UC Semantic features** — from a non-deterministic ~62% baseline to a fully deterministic **100% (45/45)**.
# MAGIC
# MAGIC ### Why the data is deterministic
# MAGIC
# MAGIC All data generation scripts use a **fixed reference date** (`base_date = datetime(2026, 9, 1)`) and fixed random seeds (42/43/44/45). This produces **identical data every run**, regardless of when the demo is executed.
# MAGIC
# MAGIC * "Last month" = August 2026 | "Prior month" = July 2026
# MAGIC * All SQL uses `DATE '2026-09-01'` instead of `CURRENT_DATE()`
# MAGIC
# MAGIC ### Key insight: Non-determinism → Determinism
# MAGIC
# MAGIC At baseline, agent answers are **non-deterministic** — the same question may produce different SQL on different runs. The agent guesses table and column mappings from names, picks thresholds from general knowledge, and may change its answer if the question is rephrased. In the recorded baseline run, **28 of 45 tests pass**; before the semantic layer is added, results can still drift across runs.
# MAGIC
# MAGIC Each iteration adds UC Semantic features that make more answers **deterministic**: the agent follows governed metadata (column comments, metric views, SQL functions) rather than guessing. By Iteration 3, all 45 tests pass consistently because every answer is grounded in a UC-governed asset.
# MAGIC
# MAGIC ### Notebook structure
# MAGIC
# MAGIC | Cell | What It Does | Type |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | This overview | Markdown |
# MAGIC | 2 | Parameters (`catalog_name`, `warehouse_id`, `run_mode`) | Python |
# MAGIC | 3 | Setup: paths, API client, evaluation engine, provenance system, agent lifecycle helpers | Python |
# MAGIC | 4 | **Step 1**: Teardown (drop catalog, delete all agents across all iterations) | Python |
# MAGIC | 5 | **Steps 2-8**: Create catalog, generate data (4 scripts), create reporting views, create Baseline Genie Agents + Supervisor | Python |
# MAGIC | 6 | Test suite reference: 45 questions × 5 agents | Markdown |
# MAGIC | 7 | **Curator Benchmarks (Baseline)**: 45-test run on bare schema | Python |
# MAGIC | 8 | VISUAL: Baseline dashboard + provenance analysis | Python |
# MAGIC | 9 | Iteration 1 approach (what and why) | Markdown |
# MAGIC | 10 | **Iteration 1**: Clone Baseline → Iteration 1 agents, add column comments + example SQL + benchmarks | Python |
# MAGIC | 11 | VISUAL: After Iteration 1 | Python |
# MAGIC | 12 | Iteration 2 approach (what and why) | Markdown |
# MAGIC | 13 | **Iteration 2**: Clone Iteration 1 → Iteration 2 agents, add metric views + governed tags + CoD view | Python |
# MAGIC | 14 | VISUAL: After Iteration 2 | Python |
# MAGIC | 15 | Iteration 3 approach (what and why) | Markdown |
# MAGIC | 16 | **Pre-step**: Create `fiscal_targets` reference table | Python |
# MAGIC | 17 | Kernel Recovery (warm kernel / state rebuild — discovers latest iteration agents) | Python |
# MAGIC | 18 | **Manual Step**: Create UC Domain + Pages on the Discover page | Markdown |
# MAGIC | 19 | **Iteration 3 setup**: Clone Iteration 2 → Iteration 3 agents, register fiscal targets + SQL functions | Python |
# MAGIC | 20 | **Iteration 3**: Full 45-test rerun | Python |
# MAGIC | 21 | Reasoning confidence definitions | Markdown |
# MAGIC | 22 | VISUAL: After Iteration 3 (Final) | Python |
# MAGIC | 23 | Deep Reliability Analysis: key terms | Markdown |
# MAGIC | 24 | DEEP RELIABILITY ANALYSIS | Python |
# MAGIC | 25 | Robustness Test: key terms | Markdown |
# MAGIC | 26 | ROBUSTNESS TEST | Python |
# MAGIC | 27 | Comprehensive Prompt Benchmark: key terms | Markdown |
# MAGIC | 28 | **Comprehensive Prompt Benchmark**: LIVE Supervisor call | Python |
# MAGIC | 29 | Current Status and Findings | Markdown |
# MAGIC | 30 | **Agent Cleanup**: Delete all agents except Iteration 3 (keep 5 domain + supervisor = 6) | Python |
# MAGIC | 31 | Supervisor Agent vs Genie One comparison report | Markdown |
# MAGIC | 32 | Manual step: Delete UC Domain (cleanup) | Markdown |
# MAGIC
# MAGIC ### The 45 tests (9 groups)
# MAGIC
# MAGIC | Group | Count | What It Tests |
# MAGIC | --- | --- | --- |
# MAGIC | A: Logistics | 6 | OTD rate, late rate, avg delay, shipment counts, wasted freight |
# MAGIC | B: Demand | 4 | Revenue (Aug vs Jul), dollar change, % change |
# MAGIC | C: Inventory | 5 | Below safety stock, stockouts, days of supply |
# MAGIC | D: Supplier | 7 | PO counts, late %, lead time variance (overall + by continent) |
# MAGIC | E: Cross-domain | 3 | Fill rate, SLA penalties, Cost of Disruption |
# MAGIC | F: Indirect / Ambiguity | 6 | Region synonyms, per-order vs per-vendor, status filters |
# MAGIC | G: Q1 Fiscal | 2 | Q1 service-level target (not in any base table) |
# MAGIC | H: Hard failures | 7 | Wrong table, cross-domain joins, derived ratios |
# MAGIC | P: Critical Thresholds | 5 | Domain-specific "critical" definitions (each domain has a unique threshold) |
# MAGIC
# MAGIC ### Per-iteration agent architecture
# MAGIC
# MAGIC Each iteration creates **5 new agents** (cloned from the previous iteration) rather than modifying the existing ones. This preserves each iteration's state for side-by-side comparison:
# MAGIC
# MAGIC * **Baseline**: `SC - Demand Analysis - Baseline`, `SC - Inventory Management - Baseline`, …
# MAGIC * **Iteration 1**: `SC - Demand Analysis - Iteration 1`, … (cloned from Baseline + column comments + examples)
# MAGIC * **Iteration 2**: `SC - Demand Analysis - Iteration 2`, … (cloned from Iter 1 + metric views + tags)
# MAGIC * **Iteration 3**: `SC - Demand Analysis - Iteration 3`, … (cloned from Iter 2 + SQL functions + fiscal targets)
# MAGIC
# MAGIC At the end, a cleanup cell deletes Baseline + Iteration 1 + Iteration 2 agents (15 agents), keeping only the 5 Iteration 3 domain agents + the Supervisor (6 survivors).
# MAGIC
# MAGIC ### The 3 iterations
# MAGIC
# MAGIC | Iteration | UC Features | Accuracy | What Changes |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Baseline | Bare tables, no comments, no views | 28/45 (non-deterministic) | Agent guesses from column/table names. Answers vary across runs. |
# MAGIC | **1** | Column/Table Comments, Example SQL Queries (Genie Examples tab), Benchmarks | 37/45 | Comments steer agent to correct tables. Example SQL teaches correct patterns. |
# MAGIC | **2** | UC Metric Views (4), Governed Tags, Schema Tags, Open Knowledge View (CoD) | 41/45 | Pre-computed KPIs eliminate formula ambiguity. CoD view enables cross-domain answers. |
# MAGIC | **3** | `fiscal_targets` table, SQL Functions (5) for critical thresholds, plus UC Domain + Pages as human-facing governance | **45/45 (deterministic)** | Every final answer is grounded in a governed asset, reference table, or structured function. |
# MAGIC
# MAGIC ### Provenance and Evaluation System
# MAGIC
# MAGIC After each iteration, the `detect_provenance()` function parses each agent's generated SQL to identify which UC semantic feature it actually used — metric view, SQL function, reference table, column comment guidance, or raw table. Based on the provenance tier, a **Reasoning Confidence** label is derived dynamically (never hardcoded):
# MAGIC
# MAGIC * **DETERMINISTIC** — Agent was steered by a governed UC asset (metric view, SQL function, reference table). Answer cannot drift even if the question is rephrased. Provenance: Iter 2-3 features.
# MAGIC * **HEURISTIC** — Agent was guided by metadata (column comments, example SQL, benchmarks). Likely repeatable, but a sufficiently novel phrasing could bypass the guidance. Provenance: Iter 1 features.
# MAGIC * **INFERRED** — Agent figured it out on its own from raw column/table names — no UC feature involved. Correct today, but the mapping is a probabilistic LLM guess. Ungoverned and fragile. Provenance: Raw Table.
# MAGIC * **GUESSED** — Agent invented thresholds or conditions not provided by any UC feature. May be right by coincidence but is unreliable. Provenance: No UC feature available.
# MAGIC
# MAGIC The progression from INFERRED/GUESSED at baseline to mostly DETERMINISTIC at Iteration 3 is the entire point — accuracy alone doesn't prove governance; confidence proves the answer is **reliably correct**, not just correct today.
# MAGIC
# MAGIC ### Important: UC Pages vs SQL Functions
# MAGIC
# MAGIC **Genie Agents CANNOT access UC Pages** (proven — the agent itself stated: "I don't have direct access to those pages in this context"). UC Pages are consumed by **Genie One** only.
# MAGIC
# MAGIC * **G01/G02** (Q1 target) — Fixed by the `fiscal_targets` TABLE, not by UC Pages
# MAGIC * **P01-P05** (critical thresholds) — Fixed by **SQL Functions** (`get_critical_delay_shipments()`, etc.) added to agent data sources in Iteration 3
# MAGIC * UC Pages remain valuable as **human-facing governance documentation** on the Discover page
# MAGIC
# MAGIC ### Manual step required
# MAGIC
# MAGIC Before Iteration 3, create **6 UC Domains and 7 Pages** on the Discover page (cell 18 has the definitions): 1 overall domain (`Supply Chain Operations` with all 5 schemas) + 5 domain-specific domains, each with its policy Page. Cell 16 creates the `fiscal_targets` table first so it's available as a Related Asset. UC Domains and Pages are UI-only — no API yet. They persist through teardown by design.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
dbutils.widgets.text("warehouse_id", "bf50738cf2819197", "SQL Warehouse ID")
dbutils.widgets.dropdown("run_mode", "full", ["full", "skip_teardown", "iterations_only"], "Run Mode")

CATALOG = dbutils.widgets.get("catalog_name")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
RUN_MODE = dbutils.widgets.get("run_mode")

print(f"Catalog: {CATALOG}")
print(f"Warehouse: {WAREHOUSE_ID}")
print(f"Run Mode: {RUN_MODE}")

# COMMAND ----------

# DBTITLE 1,Setup: Paths + API Client + Verification Functions
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

def run_notebook(name, timeout=0, extra_params=None):
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


# ============================================================
# AGENT LIFECYCLE: clone, discover, cleanup
# Each iteration creates NEW agents (never modifies previous).
# all_spaces tracks every iteration: {"Baseline": {...}, "Iteration 1": {...}, ...}
# ============================================================
all_spaces = {}  # populated by each iteration cell


def clone_agents(source_spaces, iteration_label):
    """Clone 5 domain agents from source iteration into new agents.

    Reads each source agent's full serialized_space, creates a new Genie
    space with the iteration label, PATCHes with the copied config.
    Returns dict: agent_name -> new_space_id
    """
    title_prefix = {
        "demand": "SC - Demand Analysis",
        "inventory": "SC - Inventory Management",
        "logistics": "SC - Logistics Operations",
        "supplier": "SC - Supplier Risk",
        "executive": "SC - Executive Reporting",
    }
    new_spaces = {}
    for agent_name, source_sid in source_spaces.items():
        new_title = f"{title_prefix[agent_name]} - {iteration_label}"
        # Read full config from source
        r = requests.get(
            f"{host}/api/2.0/genie/spaces/{source_sid}?include_serialized_space=true",
            headers=headers,
        )
        source_ss = json.loads(r.json().get("serialized_space", "{}"))
        source_desc = r.json().get("description", f"{agent_name} agent")
        table_ids = [t["identifier"] for t in source_ss.get("data_sources", {}).get("tables", [])]

        # Create new space
        cr = requests.post(
            f"{host}/api/2.0/genie/spaces",
            headers=headers,
            json={
                "title": new_title,
                "description": source_desc,
                "warehouse_id": WAREHOUSE_ID,
                "table_identifiers": table_ids,
                "serialized_space": json.dumps({"version": 2}),
            },
        )
        if cr.status_code not in (200, 201):
            print(f"    \u2717 {agent_name}: create failed {cr.status_code} {cr.text[:150]}")
            continue
        new_sid = cr.json().get("space_id") or cr.json().get("id")

        # PATCH with full config (retry for API race condition)
        ok = False
        for attempt in range(3):
            time.sleep(2 + attempt)
            pr = requests.patch(
                f"{host}/api/2.0/genie/spaces/{new_sid}",
                headers=headers,
                json={"serialized_space": json.dumps(source_ss)},
            )
            if pr.status_code == 200:
                vr = requests.get(
                    f"{host}/api/2.0/genie/spaces/{new_sid}?include_serialized_space=true",
                    headers=headers,
                )
                vss = json.loads(vr.json().get("serialized_space", "{}"))
                if len(vss.get("data_sources", {}).get("tables", [])) > 0:
                    ok = True
                    break
            print(f"    \u26a0 {agent_name}: retry {attempt + 1}...")
        if ok:
            new_spaces[agent_name] = new_sid
            print(f"    \u2713 {agent_name}: {new_title} ({new_sid})")
        else:
            print(f"    \u2717 {agent_name}: failed after 3 attempts")
    return new_spaces


def discover_agents_by_label(label):
    """Discover SC agents matching a specific iteration label.
    Returns dict: agent_name -> space_id
    """
    resp = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers)
    found = {}
    for s in resp.json().get("spaces", []):
        title = s.get("title", "")
        if "SC" not in title or label not in title:
            continue
        sid = s["space_id"]
        if "Demand" in title:
            found["demand"] = sid
        elif "Inventory" in title:
            found["inventory"] = sid
        elif "Logistics" in title:
            found["logistics"] = sid
        elif "Supplier" in title:
            found["supplier"] = sid
        elif "Executive" in title:
            found["executive"] = sid
    return found


def delete_agents_by_label(label):
    """Delete all SC agents matching a specific iteration label."""
    resp = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers)
    deleted = 0
    for s in resp.json().get("spaces", []):
        title = s.get("title", "")
        if "SC" in title and label in title:
            dr = requests.delete(f"{host}/api/2.0/genie/spaces/{s['space_id']}", headers=headers)
            if dr.status_code in (200, 204):
                print(f"    \u2713 Deleted: {title}")
                deleted += 1
            else:
                print(f"    \u2717 Failed to delete: {title} ({dr.status_code})")
    return deleted


def update_supervisor_tools(current_spaces):
    """Update the Supervisor Agent's genie_space tools to point to current iteration's agents.
    Finds the Supply Chain supervisor, deletes old genie_space tools, creates new ones."""
    sup_resp = requests.get(f"{host}/api/2.1/supervisor-agents", headers=headers)
    if sup_resp.status_code != 200:
        print(f"    \u26a0 Could not list supervisors: {sup_resp.status_code}")
        return
    supervisors = sup_resp.json().get("supervisor_agents", [])
    sup = next((a for a in supervisors if "Supply Chain" in a.get("display_name", "")), None)
    if not sup:
        print("    \u26a0 Supply Chain supervisor not found")
        return
    sup_name = sup["name"]
    # List existing tools
    tools_resp = requests.get(f"{host}/api/2.1/{sup_name}/tools", headers=headers)
    if tools_resp.status_code != 200:
        print(f"    \u26a0 Could not list supervisor tools: {tools_resp.status_code}")
        return
    # Delete existing genie_space tools
    for t in tools_resp.json().get("tools", []):
        if t.get("tool_type") == "genie_space":
            tid = t.get("tool_id", "")
            requests.delete(f"{host}/api/2.1/{sup_name}/tools/{tid}", headers=headers)
    # Re-create genie_space tools pointing to current iteration
    tool_map = {
        "demand": ("demand-analysis", "Demand Analysis agent. Answers questions about sales, revenue, and demand forecasts."),
        "inventory": ("inventory-management", "Inventory Management agent. Answers questions about inventory levels, stockouts, and warehouse data."),
        "logistics": ("logistics-operations", "Logistics Operations agent. Answers questions about shipments, deliveries, and transit data."),
        "supplier": ("supplier-risk", "Supplier Risk agent. Answers questions about supplier performance, lead times, and procurement."),
        "executive": ("executive-reporting", "Executive Reporting agent. Answers questions about KPIs, regional performance, and dashboards."),
    }
    for agent_name, space_id in current_spaces.items():
        tid, desc = tool_map.get(agent_name, (agent_name, ""))
        r = requests.post(
            f"{host}/api/2.1/{sup_name}/tools?tool_id={tid}",
            headers=headers,
            json={"tool_type": "genie_space", "description": desc, "genie_space": {"id": space_id}},
        )
        status = "\u2713" if r.status_code in (200, 201) else "\u2717"
        print(f"    {status} {tid} \u2192 {space_id}")


# Single business narrative — covers ALL 10 primary GTs + every metric view measure.
# Deliberately uses business vocabulary that confuses Genie without UC features:
#   "revenue" / "August and July revenue"  → needs synonym: revenue = total_amount (Iter 2)
#   "on-time delivery rate"                → needs synonym + metric view (Iters 2-4)
#   "total shipments" / "how many late"     → needs metric view (Iter 4)
#   "fill rate"                             → needs synonym: fill_rate = service_level_pct (Iter 2)
#   "vendor" / "purchase orders"            → needs synonym: vendor = supplier_* (Iter 2)
#   "lead time variance"                    → needs metric view (Iter 4)
#   "SLA penalties"                         → needs synonym: SLA penalty = penalty_amount (Iter 2)
#   "Cost of Disruption"                    → needs metric view (cross-domain, Iter 4 only)
#   "Q1 service-level targets"              → needs UC Page (Iter 5 — manual)
#   "West region"                           → needs instruction: West = Western
MAIN_PROMPT = (
    "We need a complete supply chain health check for our West region in August 2026. "
    "The CFO wants to understand what drove the revenue decline versus July — "
    "show the actual August and July revenue numbers, the dollar change, and the percentage change — "
    "and which product families are most at fault. "
    "Are our on-time delivery rate and average delay for West region shipments contributing to the problem? "
    "How many total shipments went out and how many were late? "
    "I also need our current fill rate, how many inventory positions are sitting below safety stock "
    "in the West region, how many unique SKUs are affected, what is our days of supply for those at-risk items, "
    "and how many SKUs are completely stocked out. "
    "On the vendor side: what percentage of vendors delivered late in August, "
    "how many purchase orders were late out of total, what is the average lead time variance, "
    "and what are the total vendor SLA penalties we have incurred? "
    "Bring it all together as our total Cost of Disruption by region for last month Aug 26 — "
    "cancelled revenue, at-risk backorder revenue, wasted freight on late shipments, "
    "and supplier penalty exposure in one number per region. "
    "Additionally, how many shipments in August were flagged under the Logistics Risk Standards, how many Western region orders in August "
    "triggered a Demand Anomaly Alert, how many inventory positions are classified as supply-risk under Inventory Standards, "
    "how many supplier orders last month fell below the Procurement Quality Minimum, "
    "and how many suppliers exceeded the Executive Disruption Threshold?"
    "Are we going to miss our Q1 service-level targets, and what are the top actions we should take? "
)


def invoke_supervisor(prompt, max_tokens=2500):
    resp = requests.post(
        f"{host}/serving-endpoints/{endpoint_name}/invocations",
        headers=headers,
        json={
            "input": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": 0,
        },
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Supervisor invocation failed: {resp.status_code} - {resp.text[:400]}")
    return resp.json()


# ============================================================
# EVALUATION ENGINE
# 1. Calls Supervisor Agent with business prompt
# 2. Extracts agent queries + SQL from response
# 3. Sends findings to Evaluator Genie Agent for comparison
# 4. Parses Evaluator assessment (narration + query results)
# ============================================================
import re


def extract_response_text(response_json):
    """Pull all assistant text from the Supervisor response."""
    texts = []
    for item in response_json.get("output", []):
        if item.get("role") != "assistant":
            continue
        for c in item.get("content", []):
            if c.get("type") == "output_text" and c.get("text"):
                texts.append(c["text"])
    return "\n".join(texts)


def extract_domain_text(response_json):
    """Extract ONLY domain agent text, excluding evaluator output.
    This prevents the ground truth table from inflating the Python scorer."""
    texts = []
    skip_next = False
    for item in response_json.get("output", []):
        # Detect evaluator tool call — skip its output
        if "name" in item and "arguments" in item and "role" not in item:
            skip_next = (item["name"] == "evaluator")
            continue
        if item.get("role") != "assistant":
            continue
        for c in item.get("content", []):
            text = c.get("text", "")
            # Skip text that starts with <name>evaluator</name>
            if "<name>evaluator</name>" in text or "<name>SC - Evaluator</name>" in text:
                skip_next = True
                continue
            if skip_next:
                skip_next = False
                continue
            if c.get("type") == "output_text" and text:
                texts.append(text)
    return "\n".join(texts)


def extract_agent_queries(response_json):
    """Extract sub-agent tool calls, SQL blocks, and result columns."""
    tool_calls = []
    for item in response_json.get("output", []):
        if "name" in item and "arguments" in item and "role" not in item:
            agent = item["name"]
            try:
                args = json.loads(item["arguments"]) if isinstance(item["arguments"], str) else item["arguments"]
                query = args.get("genie_query", str(args))
            except Exception:
                query = str(item["arguments"])[:200]
            tool_calls.append({"agent": agent, "query": query})

    full_text = extract_response_text(response_json)
    sql_blocks = re.findall(r'```sql\n(.*?)\n```', full_text, re.DOTALL)

    result_columns = {}
    current_agent = None
    for item in response_json.get("output", []):
        if item.get("role") != "assistant":
            continue
        for c in item.get("content", []):
            text = c.get("text", "")
            if text.startswith("<name>") and text.endswith("</name>"):
                current_agent = text.replace("<name>", "").replace("</name>", "")
            elif text.startswith("||") and current_agent:
                cols = [h.strip() for h in text.split("\n")[0].strip("|").split("|") if h.strip()]
                result_columns[current_agent] = cols

    return {"tool_calls": tool_calls, "sql_blocks": sql_blocks, "result_columns": result_columns}


def extract_tool_results(response_json):
    """Extract tool result content from each agent (text response + any SQL blocks)."""
    results = {}
    pending_agent = None
    for item in response_json.get("output", []):
        if "name" in item and "arguments" in item and "role" not in item:
            pending_agent = item["name"]
        elif item.get("role") == "tool":
            agent = pending_agent or item.get("name", "unknown")
            content = ""
            if isinstance(item.get("content"), str):
                content = item["content"]
            elif isinstance(item.get("content"), list):
                for c in item["content"]:
                    if isinstance(c, dict):
                        content += c.get("text", "")
            if content and agent not in ("evaluator", "unknown"):
                sql_blocks = re.findall(r'```sql\s*\n(.*?)\n\s*```', content, re.DOTALL)
                if not sql_blocks:
                    sql_blocks = [s.strip() for s in re.findall(
                        r'(SELECT\s+[\s\S]{10,}?FROM\s+\S+[\s\S]*?)(?:\n\n|\Z)',
                        content, re.IGNORECASE) if len(s) > 30][:2]
                results[agent] = {"content": content[:3000], "sql_blocks": sql_blocks[:3]}
            pending_agent = None
    return results


def remove_evaluator_from_supervisor():
    """Remove evaluator tool from Supervisor — it must NOT access GT values."""
    try:
        tools_resp = requests.get(f"{host}/api/2.1/{supervisor_name}/tools", headers=headers)
        for tool in tools_resp.json().get("tools", []):
            if "evaluator" in tool.get("name", "").lower():
                del_resp = requests.delete(
                    f"{host}/api/2.1/{supervisor_name}/tools/{tool['tool_id']}",
                    headers=headers)
                print(f"  \u2713 Removed evaluator tool from Supervisor ({del_resp.status_code})")
                return
        print("  (evaluator tool not found on Supervisor \u2014 already removed)")
    except Exception as e:
        print(f"  \u26a0 Could not remove evaluator: {e}")


def find_evaluator_space():
    """Discover the Evaluator Genie Agent space ID."""
    resp = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers)
    for s in resp.json().get("spaces", []):
        if "evaluator" in s.get("title", "").lower():
            return s["space_id"]
    return None


def call_evaluator(evaluator_space_id, prompt, timeout_secs=300):
    """Call the Evaluator Genie Agent via conversation API."""
    conv_resp = requests.post(
        f"{host}/api/2.0/genie/spaces/{evaluator_space_id}/start-conversation",
        headers=headers, json={"content": prompt},
    )
    if conv_resp.status_code != 200:
        return None
    conv_id = conv_resp.json().get("conversation_id")
    msg_id = conv_resp.json().get("message_id")

    for attempt in range(timeout_secs // 5):
        time.sleep(5)
        poll = requests.get(
            f"{host}/api/2.0/genie/spaces/{evaluator_space_id}/conversations/{conv_id}/messages/{msg_id}",
            headers=headers,
        )
        if poll.status_code != 200:
            continue
        msg = poll.json()
        status = msg.get("status", "")
        if status in ["COMPLETED", "FAILED", "CANCELLED"]:
            narration = ""
            for att in msg.get("attachments", []):
                txt = att.get("text", {}).get("content", "")
                if txt:
                    narration += txt
            qr = msg.get("query_result")
            rows = []
            if qr:
                cols = [c.get("name", "") for c in qr.get("columns", [])]
                for row in qr.get("data", {}).get("data_array", []):
                    rows.append(dict(zip(cols, row)))
            return {"status": status, "narration": narration, "rows": rows}
        elif attempt % 6 == 0:
            print(f"    polling... {attempt*5}s ({status})")
    return None


def parse_evaluator_counts(narration, total_metrics):
    """Parse the Evaluator's narration for EXACT/CLOSE/MISS/NOT_FOUND counts.
    PRIMARY source of truth. Handles 'X of the Y EXACT' (note 'the')."""
    clean = re.sub(r'\*\*', '', narration)
    exact = close = miss = nf = 0
    m = re.search(r'(\d+)\s+(?:of|out of)\s+(?:the\s+)?(\d+)\b.*?EXACT', clean)
    if m: exact = int(m.group(1))
    m = re.search(r'(\d+)\s+(?:(?:metrics?\s+)?(?:were|as)\s+)?CLOSE', clean)
    if m and int(m.group(1)) <= total_metrics: close = int(m.group(1))
    m = re.search(r'(\d+)\s+(?:(?:metrics?\s+)?(?:were|as)\s+)?MISS(?!_)', clean)
    if m and int(m.group(1)) <= total_metrics: miss = int(m.group(1))
    m = re.search(r'(\d+)\s+(?:(?:metrics?\s+)?(?:were|as)\s+)?NOT_FOUND', clean)
    if m and int(m.group(1)) <= total_metrics: nf = int(m.group(1))
    if re.search(r'\bno\s+CLOSE', clean): close = 0
    if re.search(r'\bno\s+MISS', clean): miss = 0
    if re.search(r'\bno\s+NOT_FOUND', clean): nf = 0
    if exact > 0 and (close + miss + nf) == 0:
        nf = max(0, total_metrics - exact)
    if exact + close + miss + nf == 0:
        exact = len(re.findall(r':\s*EXACT\b', clean))
        nf = len(re.findall(r':\s*NOT_FOUND\b', clean))
    if exact + close + miss + nf == 0:
        nf = total_metrics
    return {"exact": exact, "close": close, "miss": miss, "not_found": nf}


def score_domain_text(domain_text, gt_rows):
    """Score DOMAIN-ONLY text against ground truth using EXACT 2-decimal matching.
    round(abs(found), 2) == round(abs(expected), 2). No tolerance bands."""
    all_numbers = []
    for m in re.finditer(r'-?[\d,]+\.?\d*', domain_text):
        try: all_numbers.append(float(m.group().replace(',', '')))
        except ValueError: pass
    results = []
    for row in gt_rows:
        metric, gt_val = row["metric"], float(row["ground_truth_value"])
        target = round(abs(gt_val), 2)
        found_val = None
        status = "NOT_FOUND"
        best_diff, closest = float('inf'), None
        for num in all_numbers:
            rounded = round(abs(num), 2)
            if rounded == target:
                found_val = num
                status = "EXACT"
                break
            diff = abs(rounded - target)
            if diff < best_diff:
                best_diff, closest = diff, num
        if status != "EXACT" and closest is not None:
            # Check if closest is within 10% for CLOSE classification
            if target > 0 and abs(round(abs(closest), 2) - target) / target < 0.10:
                status = "CLOSE"
                found_val = closest
            elif target > 0 and abs(round(abs(closest), 2) - target) / target < 0.30:
                status = "MISS"
                found_val = closest
        results.append({"metric": metric, "gt_value": gt_val,
            "found_value": round(found_val, 2) if found_val is not None else None,
            "status": status, "pct_diff": round(best_diff / max(target, 0.001) * 100, 1) if closest else None})
    return results


def score_supervisor_text(supervisor_text, gt_rows):
    """DETERMINISTIC scoring: search Supervisor text for each ground truth value.
    Uses EXACT 2-decimal matching: round(abs(found), 2) == round(abs(expected), 2).
    Returns list of {metric, gt_value, found_value, status, pct_diff}."""
    all_numbers = []
    for m in re.finditer(r'-?[\d,]+\.?\d*', supervisor_text):
        try:
            all_numbers.append(float(m.group().replace(',', '')))
        except ValueError:
            pass
    results = []
    for row in gt_rows:
        metric = row["metric"]
        gt_val = float(row["ground_truth_value"])
        target = round(abs(gt_val), 2)
        found_val = None
        status = "NOT_FOUND"
        best_diff, closest = float('inf'), None
        for num in all_numbers:
            rounded = round(abs(num), 2)
            if rounded == target:
                found_val = num
                status = "EXACT"
                break
            diff = abs(rounded - target)
            if diff < best_diff:
                best_diff, closest = diff, num
        if status != "EXACT" and closest is not None:
            if target > 0 and abs(round(abs(closest), 2) - target) / target < 0.10:
                status = "CLOSE"
                found_val = closest
            elif target > 0 and abs(round(abs(closest), 2) - target) / target < 0.30:
                status = "MISS"
                found_val = closest
        results.append({
            "metric": metric, "gt_value": gt_val,
            "found_value": round(found_val, 2) if found_val is not None else None,
            "status": status, "pct_diff": round(best_diff / max(target, 0.001) * 100, 1) if closest else None,
        })
    return results


def _fmt_box(title):
    """Print a formatted section header."""
    w = 70
    print(f"\n{'\u2550' * w}")
    print(f"  {title}")
    print(f"{'\u2550' * w}")


def _fix_ref_sql_display(sql):
    """Fix reference SQL for display — single quotes stripped during INSERT."""
    if not sql:
        return sql
    # region = Western → region = 'Western'
    sql = re.sub(r"=\s*(?!')(Western|Eastern|Central|Southern)\b", r"= '\1'", sql)
    # DATE 2026-08-01 → DATE '2026-08-01'
    sql = re.sub(r"DATE\s+(?!')(20\d{2}-\d{2}-\d{2})", r"DATE '\1'", sql)
    return sql


def verify_ground_truth(stage_label):
    """Full verification: Supervisor call -> Evaluator comparison -> formatted report."""
    _fmt_box(f"STAGE: {stage_label}")

    gt_table = f"{CATALOG}.reporting.ground_truth_kpis"
    if not table_exists(gt_table):
        print("  \u26a0 ground_truth_kpis not created \u2014 skipping")
        return {"stage": stage_label, "exact": 0, "total": 10}

    # All 9 GT metrics are seeded by 08_setup (including CoD, computed from raw tables).
    # No need to seed here — just load them.
    gt_df = spark.sql(f"SELECT agent, metric, ground_truth_value, reference_sql, uc_feature_needed FROM {gt_table} ORDER BY metric").collect()
    total_metrics = len(gt_df)

    # ---- Display: The prompt being evaluated ----
    import textwrap
    print(f"\n  \u25b6 PROMPT SENT TO SUPERVISOR (covers all {total_metrics} GT metrics):")
    for line in textwrap.wrap(MAIN_PROMPT, width=105):
        print(f"    {line}")

    # ------------------------------------------------------------------ #
    #  STEP 1 \u2014 Call Supervisor Agent (single prompt, all 9 metrics)      #
    # ------------------------------------------------------------------ #
    print("\n  \u25b6 STEP 1: Calling Supervisor Agent")
    main_response = invoke_supervisor(MAIN_PROMPT, max_tokens=4000)
    main_text = extract_response_text(main_response)
    main_queries = extract_agent_queries(main_response)
    tool_results = extract_tool_results(main_response)
    print(f"    Response: {len(main_text):,} chars")

    # Filter out evaluator + sandbox from display (evaluator must not be called)
    all_tc = [tc for tc in main_queries["tool_calls"] if tc["agent"] not in ("evaluator", "sandbox")]
    all_sql = main_queries["sql_blocks"]
    all_cols = {k: v for k, v in main_queries["result_columns"].items() if k != "evaluator"}

    # ---- Display: Agent Routing ----
    print(f"\n  \u25b6 AGENT QUERIES ({len(all_tc)} tool calls)")
    for tc in all_tc:
        print(f'    \u2022 {tc["agent"]:30s} \u2502 "{tc["query"][:90]}"')

    # ---- Display: SQL blocks (if any) ----
    if all_sql:
        print(f"\n  \u25b6 SQL FROM RESPONSE ({len(all_sql)} blocks)")
        for i, sql in enumerate(all_sql[:4]):
            lines = sql.strip().split("\n")
            print(f"    [{i+1}] {lines[0][:120]}")
            for line in lines[1:3]:
                print(f"        {line[:120]}")
            if len(lines) > 3:
                print(f"        ... ({len(lines)} lines total)")

    # ---- Display: Result columns ----
    if all_cols:
        print(f"\n  \u25b6 RESULT SCHEMAS (column names returned)")
        for agent, cols in all_cols.items():
            print(f"    \u2022 {agent:30s} \u2502 {', '.join(cols[:6])}{'...' if len(cols) > 6 else ''}")

    # ------------------------------------------------------------------ #
    #  STEP 2 \u2014 Evaluator Genie Agent compares against ground truth       #
    # ------------------------------------------------------------------ #
    evaluator_id = find_evaluator_space()
    if not evaluator_id:
        print("\n  \u26a0 Evaluator agent not found")
        return {"stage": stage_label, "exact": 0, "total": total_metrics}

    # Build a structured eval prompt that tells the evaluator WHAT NUMBERS to search for.
    # The old prompt asked "compare metrics" — but the supervisor doesn't use GT metric names,
    # so the evaluator couldn't match. Now we include GT values explicitly.
    gt_table_str = "\n".join(
        f"  - {row['metric']}: look for the number {row['ground_truth_value']} (or close to it)"
        for row in gt_df
    )
    eval_prompt = f"""The Supervisor Agent produced this supply chain analysis:

{main_text[:6000]}

Search the report above for EACH of these ground truth values.
For each metric, find the NUMBER (not the name) in the report:
{gt_table_str}

Classify each as:
- EXACT: the number appears in the report (within 2%)
- CLOSE: a similar number appears (within 10%)
- MISS: a number was found but differs by >10%
- NOT_FOUND: no matching number in the report

List each metric with its classification and what number you found (if any).
Format: 'metric_name: CLASSIFICATION (found=X)' on each line."""

    print(f"\n  \u25b6 STEP 2: Evaluator comparing {total_metrics} ground truth metrics...")
    eval_result = call_evaluator(evaluator_id, eval_prompt)

    if not eval_result or eval_result["status"] != "COMPLETED":
        print("    \u26a0 Evaluator did not complete")
        return {"stage": stage_label, "exact": 0, "total": total_metrics}

    # ---- Display: Evaluator narration ----
    narration = eval_result["narration"]
    print(f"\n  \u25b6 EVALUATOR NARRATIVE")
    for line in narration.split("\n")[:20]:
        print(f"    {line[:120]}")
    if narration.count("\n") > 20:
        print(f"    ... ({narration.count(chr(10))} lines total)")

    # ------------------------------------------------------------------ #
    #  STEP 3 \u2014 Scoring (Evaluator narration = primary)                   #
    # ------------------------------------------------------------------ #
    # PRIMARY: Python scorer on domain-only text (evaluator output excluded)
    domain_text = extract_domain_text(main_response)
    per_metric = score_domain_text(domain_text, gt_df)
    exact = sum(1 for r in per_metric if r["status"] == "EXACT")
    close = sum(1 for r in per_metric if r["status"] == "CLOSE")
    miss  = sum(1 for r in per_metric if r["status"] == "MISS")
    nf    = sum(1 for r in per_metric if r["status"] == "NOT_FOUND")
    pct   = round(exact / total_metrics * 100) if total_metrics > 0 else 0

    # Also parse Evaluator narration for context
    eval_counts = parse_evaluator_counts(narration, total_metrics)
    eval_exact = eval_counts["exact"]

    # ---- Per-metric results: value + query analysis ----
    print(f"\n  \u25b6 PER-METRIC DETAIL (value match + query analysis)")
    status_icons = {"EXACT": "\u2705", "CLOSE": "\u26a0\ufe0f", "MISS": "\u274c", "NOT_FOUND": "\u2b1b"}
    for r in per_metric:
        icon = status_icons.get(r["status"], "?")
        found_str = f"found={r['found_value']}" if r["found_value"] is not None else "not in agent output"
        print(f"    {icon} {r['metric'][:55]:55s} {r['status']:10s} (gt={r['gt_value']}, {found_str})")
        # For non-EXACT: show WHY it failed + full reference SQL + what the agent did
        if r["status"] != "EXACT":
            gt_row = next((g for g in gt_df if g["metric"] == r["metric"]), None)
            if gt_row:
                try:
                    uc_feature = gt_row["uc_feature_needed"] or ""
                except Exception:
                    uc_feature = ""
                ref_sql = gt_row["reference_sql"] or ""
                agent_name = gt_row["agent"]
                # WHY: the UC feature that would fix this
                if uc_feature:
                    print(f"        WHY: {uc_feature}")
                # REFERENCE SQL: full, with proper quotes restored for display
                if ref_sql:
                    print(f"        REFERENCE SQL (correct):")
                    for line in _fix_ref_sql_display(ref_sql).strip().split("\n"):
                        print(f"            {line}")
                # AGENT SQL: search ALL tool results for SQL relevant to this metric
                # (don't just use the mapped agent — supervisor may route differently)
                metric_keywords = r['metric'].lower().split()
                best_sql = None
                for ag_name, ag_tr in tool_results.items():
                    for sql_block in ag_tr.get("sql_blocks", []):
                        sql_lower = sql_block.lower()
                        # Score: how many metric keywords appear in the SQL
                        hits = sum(1 for kw in metric_keywords if kw in sql_lower and len(kw) > 3)
                        if hits >= 2 and (best_sql is None or hits > best_sql[1]):
                            best_sql = (sql_block, hits, ag_name)
                if best_sql:
                    print(f"        AGENT SQL (from {best_sql[2]}):")
                    for line in best_sql[0].strip().split("\n")[:6]:
                        print(f"            {line}")
                print()
    if eval_exact != exact:
        print(f"    (Evaluator narration: {eval_exact}/{total_metrics} vs domain-text: {exact}/{total_metrics})")

    # ---- Final score line ----
    score_icon = "\u2705" if exact == total_metrics else "\u26a0\ufe0f" if pct >= 70 else "\u274c"
    parts = [f"{exact}/{total_metrics} EXACT ({pct}%)"]
    if close: parts.append(f"{close} CLOSE")
    if miss:  parts.append(f"{miss} MISS")
    if nf:    parts.append(f"{nf} NOT_FOUND")
    score_line = " | ".join(parts)
    print(f"\n  {score_icon} SCORE: {score_line}")
    print(f"{'\u2500' * 70}")

    return {
        "stage": stage_label, "exact": exact, "close": close,
        "miss": miss, "not_found": nf, "total": total_metrics,
        "per_metric": per_metric, "gt_rows": gt_df,
        "tool_calls": all_tc, "sql_blocks": all_sql, "result_columns": all_cols,
    }


def run_comprehensive_benchmark(label, assumptions_list=None, report_text=None):
    """Run MAIN_PROMPT through the Supervisor and score against ALL 40 GT values.
    If report_text is provided, scores that text directly (static analysis).
    Otherwise discovers the supervisor endpoint and calls it live (~3-5 min).
    Returns dict: label, found, close, missing, total, pct, results[], response_text."""
    print(f"\n{'='*90}")
    print(f"  COMPREHENSIVE PROMPT BENCHMARK: {label}")
    print(f"{'='*90}")
    if assumptions_list is None:
        try:
            assumptions_list = assumptions
        except NameError:
            print("  \u26a0 assumptions list not defined yet")
            return None
    # Get response text (live or static)
    if report_text is None:
        try:
            ep_resp = requests.get(f"{host}/api/2.0/serving-endpoints", headers=headers)
            ep_name = None
            for ep in ep_resp.json().get("endpoints", []):
                if ep.get("name", "").startswith("mas-"):
                    ep_name = ep["name"]
                    break
            if not ep_name:
                print("  \u26a0 Supervisor endpoint not found")
                return None
            print(f"  Supervisor: {ep_name}")
            print(f"  Sending prompt ({len(MAIN_PROMPT)} chars)... ~3-5 min")
            sup_resp = requests.post(
                f"{host}/serving-endpoints/{ep_name}/invocations", headers=headers,
                json={"input": [{"role": "user", "content": MAIN_PROMPT}],
                      "max_tokens": 4000, "temperature": 0}, timeout=600)
            if sup_resp.status_code != 200:
                print(f"  \u26a0 Supervisor failed: {sup_resp.status_code} {sup_resp.text[:200]}")
                return None
            report_text = extract_response_text(sup_resp.json())
        except Exception as e:
            print(f"  \u26a0 Supervisor error: {e}")
            return None
    print(f"  Response: {len(report_text):,} chars")
    # Score every GT metric against the response text
    results = []
    for aid, agent_key, question, expected, desc, claimed_fix in assumptions_list:
        match, found, closest = find_value_in_text(report_text, expected)
        if match:
            verdict = "\u2705 FOUND"
        elif closest is not None and abs(expected) > 0 and abs(abs(closest) - abs(expected)) / abs(expected) < 0.05:
            verdict = "\u26a0 CLOSE"
        else:
            verdict = "\u2b1b NOT_FOUND"
        results.append({"id": aid, "verdict": verdict, "expected": expected,
            "found": found, "closest": closest, "desc": desc,
            "agent": agent_key, "claimed_fix": claimed_fix})
    # Scorecard
    correct_count = sum(1 for r in results if "FOUND" in r["verdict"] and "NOT" not in r["verdict"])
    wrong_count = sum(1 for r in results if "CLOSE" in r["verdict"])
    missing = sum(1 for r in results if "NOT_FOUND" in r["verdict"])
    total = len(results)
    current_group = ""
    gnames = {'A': 'LOGISTICS MV', 'B': 'DEMAND MV', 'C': 'INVENTORY MV',
              'D': 'SUPPLIER MV', 'E': 'CROSS-DOMAIN', 'F': 'INDIRECT GTs',
              'G': 'Q1 FISCAL', 'H': 'HARD FAILURES',
              'P': 'UC PAGES (CRITICAL THRESHOLDS)'}
    for r in results:
        gid = r['id'][:1]
        if gid != current_group:
            current_group = gid
            print(f"\n  \u2500\u2500 {gnames.get(gid, gid)} {'\u2500'*60}")
        val = r.get('found') if r.get('found') is not None else r.get('closest')
        val_str = f"{val:>14,.2f}" if val is not None else f"{'\u2014':>14}"
        print(f"  {r['verdict'][:2]} {r['id']:<5} gt={r['expected']:<14,.2f} {val_str}  {r['desc'][:50]}")
    pct = 100 * correct_count // max(total, 1)
    print(f"\n{'='*90}")
    print(f"  COMPREHENSIVE SCORE ({label}):")
    print(f"    \u2705 Found + Correct: {correct_count}/{total} ({pct}%)")
    if wrong_count:
        print(f"    \u26a0 Found + Wrong:   {wrong_count}/{total}")
    print(f"    \u2b1b Not Found:       {missing}/{total}")
    print(f"{'='*90}")
    return {"label": label, "found": correct_count, "close": wrong_count,
            "missing": missing, "total": total, "pct": pct,
            "results": results, "response_text": report_text}


# Tests known to be fragile ACROSS runs (may flip even with identical setup)
# Key = test ID, value = reason for fragility
FRAGILE_RISK = {
    "A02": "Agent intermittently bypasses metric view, queries base shipments with CURRENT_DATE() and wrong date column",
    "A03": "Agent sometimes adds extra delay_days IS NOT NULL filter, changing the average",
    "A06": "Wasted freight depends on agent filtering is_late on shipments — may guess correctly at baseline",
    "B03": "Revenue change requires precise Aug-Jul subtraction — agent may use different date ranges",
    "F06": "Product family decline is complex MoM — agent may rank/compute differently each run",
    "D03": "75% late rate could match by coincidence if agent counts differently",
    "C05": "avg_days_of_supply=0.96 is very small — agent may round or filter differently",
}

# CLASSIFICATION IS FULLY DYNAMIC — derived from agent's actual SQL.
# NO hardcoded per-test labels. As iterations add UC features,
# the same test gets a DIFFERENT classification based on what
# the agent ACTUALLY used (metric view, SQL function, raw table, etc.).
# Categories (all dynamic):
#   METRIC_VIEW       = agent queried a governed metric view (Iter 2)
#   SQL_FUNCTION      = agent called a threshold SQL function (Iter 3)
#   REFERENCE_TABLE   = agent queried fiscal_targets (Iter 3)
#   GUESSED_THRESHOLD = agent invented a numeric threshold in WHERE clause
#   GUESSED_VALUE     = agent produced correct value but no governed source
#   DERIVED           = agent computed rate/formula (multiple aggs, CASE WHEN)
#   UNAMBIGUOUS       = simple column query on base table
#   NARRATION         = no SQL — answered from narration text only
LLM_REASONING = {}  # intentionally empty — all classification is dynamic
BASELINE_HOW = LLM_REASONING

def _parse_sql_components(sql_text):
    """Extract structural components from a SQL statement for comparison."""
    if not sql_text:
        return {}
    sql = sql_text.lower()
    tables = [t.replace('`','') for t in re.findall(r'(?:from|join)\s+`?([a-z_][a-z0-9_.`]*)`?', sql)]
    # Columns in SELECT, WHERE, GROUP BY, aggregations
    cols = set(re.findall(r'(?:select|where|group\s+by|order\s+by|avg|sum|count|min|max|case\s+when)\s*\(?\s*([a-z_]\w*)', sql))
    cols -= {'when','then','else','end','as','and','or','not','null','true','false','from','case','distinct'}
    aggs = re.findall(r'(avg|sum|count|min|max)\s*\(', sql)
    # WHERE predicates: column op value
    predicates = []
    wm = re.search(r'where\s+(.*?)(?:group\s+by|order\s+by|limit|having|$)', sql, re.DOTALL)
    if wm:
        for m in re.finditer(r'(\w+)\s*([<>=!]+)\s*[\'"]?([\w.\-]+)[\'"]?', wm.group(1)):
            predicates.append((m.group(1), m.group(2), m.group(3)))
    return {"tables": tables, "columns": cols, "aggs": aggs, "predicates": predicates}


def analyze_agent_sql(sql_text, test_id, reference_sql=None, narration=None, question=None):
    """Semantic reasoning analyzer: HOW did the LLM generate this SQL?
    Compares agent SQL to ground truth reference SQL structurally.
    Returns (mechanism, evidence_str, details_dict).
    Evidence includes: table match, column overlap, guessed thresholds, semantic equivalence."""
    if not sql_text or not sql_text.strip():
        return ("NARRATION", "No SQL captured — answered from narration only", {})
    sql_lower = sql_text.lower()
    # Extract tables (FROM/JOIN clauses)
    tables_raw = re.findall(r'(?:from|join)\s+`?([a-z_][a-z0-9_.`]*)`?', sql_lower)
    tables = [t.replace('`', '') for t in tables_raw]
    # Known metric views vs base tables
    MV_KW = ['delivery_performance', 'revenue_comparison', 'inventory_safety',
             'supplier_performance', 'cost_of_disruption']
    BASE_KW = ['shipments', 'sales_orders', 'inventory_ledger', 'supplier_orders',
               'supplier_lead_times', 'executive_kpis', 'vendor_slas',
               'demand_forecasts', 'fiscal_targets']
    metric_views = [t for t in tables if any(mv in t for mv in MV_KW)]
    base_tables = [t for t in tables if any(bt in t for bt in BASE_KW)]
    # Self-explanatory columns used in the SQL
    SCHEMA_COLS = {'is_late', 'delay_days', 'shipping_cost', 'total_amount',
        'below_safety_stock_flag', 'stockout_flag', 'days_of_supply',
        'order_status', 'service_level_pct', 'penalty_amount',
        'supplier_continent', 'destination_region', 'region',
        'forecast_accuracy_pct', 'quality_score', 'late_shipment_count',
        'sku_id', 'order_date', 'ship_date', 'actual_delivery_date'}
    used_cols = {col for col in SCHEMA_COLS if col in sql_lower}
    # Detect hardcoded thresholds in WHERE clause
    thresholds = []
    wm = re.search(r'where\s+(.*?)(?:group\s+by|order\s+by|limit|having|$)', sql_lower, re.DOTALL)
    if wm:
        wc = wm.group(1)
        for m in re.finditer(r'(\w+)\s*([<>=!]+)\s*(\d+\.?\d*)', wc):
            col, op, val = m.group(1), m.group(2), float(m.group(3))
            if col in ('month', 'year', 'day', 'date', 'limit') or val in (0, 1):
                continue
            # These are INVENTED thresholds — no table defines them
            if col in ('forecast_accuracy_pct', 'quality_score', 'late_shipment_count',
                       'delay_days', 'days_of_supply'):
                thresholds.append((col, op, val))
    uses_current_date = 'current_date' in sql_lower
    details = {
        "tables": tables, "metric_views": metric_views, "base_tables": base_tables,
        "used_cols": sorted(used_cols), "thresholds": thresholds,
        "current_date": uses_current_date, "sql": sql_text[:200]
    }
    # --- Semantic comparison with GT reference SQL ---
    ref = _parse_sql_components(reference_sql) if reference_sql else {}
    ref_tables = {t.split('.')[-1] for t in ref.get('tables', [])} if ref else set()
    agent_table_set = {t.split('.')[-1] for t in tables}
    same_tables = agent_table_set == ref_tables if ref_tables else None
    same_aggs = set(re.findall(r'(avg|sum|count)\s*\(', sql_lower)) == set(ref.get('aggs', [])) if ref else None
    sem_note = ""
    if same_tables is True and same_aggs is True:
        sem_note = "SQL EQUIVALENT to GT"
    elif same_tables is True:
        sem_note = "Same tables, different agg"
    elif same_tables is False:
        sem_note = f"Different tables: agent={sorted(agent_table_set)}, gt={sorted(ref_tables)}"
    details["same_tables"] = same_tables
    details["same_aggs"] = same_aggs
    details["sem_note"] = sem_note
    # ── FULLY DYNAMIC CLASSIFICATION (no hardcoded per-test lookup) ──
    # Category is determined SOLELY from the agent's actual SQL.
    # As iterations add UC features, the same test gets different labels.

    # 1. METRIC_VIEW — agent used a governed metric view (Iter 2)
    if metric_views:
        mv_name = metric_views[0].split('.')[-1] if '.' in metric_views[0] else metric_views[0]
        return ("METRIC_VIEW", f"Used metric view: {mv_name}. {sem_note}", details)

    # 2. SQL_FUNCTION — agent called a threshold SQL function (Iter 3)
    iter3_funcs = ['get_critical_delay_shipments', 'get_critical_accuracy_forecasts',
                   'get_critical_supply_positions', 'get_critical_quality_orders',
                   'get_critical_disruption_regions']
    for func in iter3_funcs:
        if func in sql_lower:
            return ("SQL_FUNCTION", f"Called SQL function: {func}(). {sem_note}", details)

    # 3. REFERENCE_TABLE — agent queried fiscal_targets (Iter 3)
    if 'fiscal_targets' in sql_lower:
        return ("REFERENCE_TABLE", f"Queried fiscal_targets reference table. {sem_note}", details)

    # 4. GUESSED_THRESHOLD — agent invented a numeric threshold
    if thresholds:
        t_str = ", ".join(f"{c}{o}{int(v) if v == int(v) else v}" for c, o, v in thresholds)
        return ("GUESSED_THRESHOLD", f"Threshold invented: {t_str}. {sem_note}", details)

    # 5. NARRATION-BASED — agent explains HOW it chose (from its response text)
    narr_lower = (narration or "").lower()
    if narr_lower:
        # Agent explicitly cites a column comment/description
        comment_signals = ['column comment', 'column description', 'described in the column',
                           'comment on the column', 'description indicates',
                           'table description', 'table comment', 'as described in']
        if any(sig in narr_lower for sig in comment_signals):
            return ("COLUMN_COMMENT", f"Agent cited column comment/description in narration. {sem_note}", details)
        # Agent explicitly cites an example/certified query
        example_signals = ['example query', 'example sql', 'sample query', 'certified query',
                           'benchmark query', 'similar query pattern', 'provided example',
                           'reference query', 'following the example']
        if any(sig in narr_lower for sig in example_signals):
            return ("EXAMPLE_GUIDED", f"Agent cited example/certified query in narration. {sem_note}", details)

    # 6. DERIVED — agent computed a rate/formula (multiple aggs, CASE WHEN, division)
    aggs_found = set(re.findall(r'(avg|sum|count)\s*\(', sql_lower))
    has_case = 'case when' in sql_lower or 'case\n' in sql_lower
    has_division = '/ ' in sql_lower or '* 100' in sql_lower or 'round(' in sql_lower
    if len(aggs_found) >= 2 or has_case or (has_division and aggs_found):
        agg_str = '+'.join(sorted(aggs_found)) if aggs_found else 'formula'
        return ("DERIVED", f"Computed: {agg_str}. {sem_note}", details)

    # 7. SYNONYM — question uses a business term mapped to a differently-named column
    #    Detected dynamically by comparing question keywords to SQL column names.
    if question and sql_lower:
        q_lower = question.lower()
        synonym_pairs = [
            ('revenue', 'total_amount', "'revenue' -> total_amount"),
            ('fill rate', 'service_level_pct', "'fill rate' -> service_level_pct"),
            ('sla penalties', 'penalty_amount', "'SLA penalties' -> penalty_amount"),
            ('penalties', 'penalty_amount', "'penalties' -> penalty_amount"),
            ('vendor', 'supplier', "'vendor' -> supplier_*"),
            ('cancelled revenue', 'total_amount', "'cancelled revenue' -> SUM(total_amount) WHERE Cancelled"),
            ('west ', "'western'", "'West' -> region='Western'"),
        ]
        for biz_term, sql_col, mapping_desc in synonym_pairs:
            if biz_term in q_lower and sql_col in sql_lower:
                # Only classify as SYNONYM if the business term itself is NOT in the SQL
                # (if it IS in the SQL, the agent found the term directly = UNAMBIGUOUS)
                if biz_term not in sql_lower:
                    bt_name = base_tables[0].split('.')[-1] if base_tables else (tables[0].split('.')[-1] if tables else '?')
                    return ("SYNONYM", f"{mapping_desc}. {bt_name}. {sem_note}", details)

    # 8. UNAMBIGUOUS — simple column query on a base/known table
    if base_tables:
        bt_name = base_tables[0].split('.')[-1] if '.' in base_tables[0] else base_tables[0]
        cols_str = ', '.join(sorted(used_cols)[:3]) if used_cols else bt_name
        return ("UNAMBIGUOUS", f"{bt_name} -> {cols_str}. {sem_note}", details)
    if tables:
        tbl_name = tables[0].split('.')[-1] if '.' in tables[0] else tables[0]
        return ("UNAMBIGUOUS", f"{tbl_name}. {sem_note}", details)

    return ("NARRATION", "Value in narration text, no SQL", details)


# ============================================================
# PROVENANCE: Agent Self-Assessment System
# Instead of US guessing why a test passes, we ask THE AGENT.
# Two approaches:
# 1. INSTRUCTION: Patch agent instructions to demand provenance
# 2. PROBE: After tests, ask the agent to explain specific answers
# Evidence priority: agent_probe > [PROVENANCE:] tag > SQL parse > hypothesis
# ============================================================

PROVENANCE_INSTRUCTION = """

PROVENANCE REQUIREMENT: After every analytical answer, append exactly one line:
[PROVENANCE: tables={table_names}, key_columns={column_names}, assumed_thresholds={none_or_values}, method={metric_view|sql_function|base_table|computed|assumed_definition}, uc_feature={name_of_view_or_function_if_used}]
If you used a metric view, name it. If you called a SQL function (get_critical_*), name it.
If you assumed any threshold or business definition not in the data, say so explicitly.
"""


def patch_provenance_instructions(spaces_dict):
    """Append provenance requirement to all Genie Agent text_instructions.
    Handles the Genie API nested format: ss['instructions']['text_instructions'][0]['content'][0].
    Call BEFORE running tests so responses include self-assessment."""
    patched = 0
    for name, space_id in spaces_dict.items():
        try:
            resp = requests.get(
                f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true",
                headers=headers)
            if resp.status_code != 200:
                print(f"  \u26a0 {name}: read failed ({resp.status_code})")
                continue
            ss = json.loads(resp.json().get("serialized_space", "{}"))
            instr = ss.get("instructions", {})
            # Genie API stores text as: instructions.text_instructions[0].content[0]
            if isinstance(instr, dict):
                text_instrs = instr.get("text_instructions", [])
                if text_instrs and text_instrs[0].get("content"):
                    current_text = text_instrs[0]["content"][0]
                else:
                    current_text = ""
            elif isinstance(instr, str):
                current_text = instr
            else:
                current_text = ""
            if "[PROVENANCE:" in current_text:
                print(f"  \u2713 {name}: already has provenance requirement")
                patched += 1
                continue
            new_text = current_text + PROVENANCE_INSTRUCTION
            # Write back in correct format
            if isinstance(instr, dict):
                if not instr.get("text_instructions"):
                    instr["text_instructions"] = [{"content": [new_text]}]
                else:
                    instr["text_instructions"][0]["content"] = [new_text]
            else:
                ss["instructions"] = new_text
            pr = requests.patch(
                f"{host}/api/2.0/genie/spaces/{space_id}",
                headers=headers,
                json={"serialized_space": json.dumps(ss)})
            if pr.status_code == 200:
                patched += 1
                print(f"  \u2713 {name}: provenance instructions added")
            else:
                print(f"  \u26a0 {name}: patch failed ({pr.status_code})")
        except Exception as e:
            print(f"  \u26a0 {name}: error {e}")
    return patched


def parse_provenance(text):
    """Extract [PROVENANCE: ...] from agent response text.
    Returns dict with tables, key_columns, assumed_thresholds, method."""
    if not text:
        return None
    m = re.search(r'\[PROVENANCE:\s*(.*?)\]', text, re.DOTALL | re.IGNORECASE)
    if not m:
        return None
    prov = m.group(1)
    result = {}
    for field in ['tables', 'key_columns', 'assumed_thresholds', 'method']:
        fm = re.search(rf'{field}\s*=\s*\{{?([^,}}\]]+)\}}?', prov)
        if fm:
            result[field] = fm.group(1).strip()
    return result if result else None


def probe_agent_reasoning(space_id, question, answer_value, timeout_secs=120):
    """Structured self-assessment: asks the agent POINTED questions about its SQL choices.
    Returns the agent's explanation text, or None on timeout/failure.
    The questions are designed so an evaluator can classify the answer as
    DETERMINISTIC / HEURISTIC / GUESS."""
    probe = (
        f"I asked you: \"{question[:250]}\"\n"
        f"You returned the value {answer_value}.\n\n"
        f"Answer these SPECIFIC questions about how you generated the SQL:\n\n"
        f"TABLE SELECTION:\n"
        f"- Which table(s) did you query? List the exact table name(s).\n"
        f"- Were there OTHER tables in the schema with similar data? "
        f"If yes, why did you pick this one over the alternatives?\n"
        f"- Was there a column comment, table description, or example query that guided your choice, "
        f"or did you choose based only on the table/column names?\n\n"
        f"COLUMN SELECTION:\n"
        f"- Which column(s) were key to computing the answer?\n"
        f"- How did you know this column maps to the business term in the question? "
        f"Was it the column name, a comment, or your own inference?\n\n"
        f"THRESHOLD / DEFINITION:\n"
        f"- Did you ASSUME any threshold, cutoff, or business definition "
        f"(e.g., 'critical' means below some number)? If yes, what value and why?\n\n"
        f"CONFIDENCE:\n"
        f"- If the question were rephrased slightly (e.g., different wording, same intent), "
        f"would you generate the same SQL? Why or why not?\n\n"
        f"Be specific. Do not say 'I used the relevant table'. Name the table and explain WHY that one."
    )
    try:
        conv_resp = requests.post(
            f"{host}/api/2.0/genie/spaces/{space_id}/start-conversation",
            headers=headers, json={"content": probe})
        if conv_resp.status_code != 200:
            return None
        conv_id = conv_resp.json().get("conversation_id")
        msg_id = conv_resp.json().get("message_id")
        for _ in range(timeout_secs // 5):
            time.sleep(5)
            poll = requests.get(
                f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}",
                headers=headers)
            if poll.status_code != 200:
                continue
            msg = poll.json()
            if msg.get("status") in ("COMPLETED", "FAILED", "CANCELLED"):
                narration = ""
                for att in msg.get("attachments", []):
                    txt = att.get("text", {}).get("content", "")
                    if txt:
                        narration += txt
                return narration or None
    except Exception as e:
        print(f" error: {e}")
    return None


def _positive_match(text, signal, window=50):
    """Check if signal appears in text in a POSITIVE (non-negated) context.
    Looks for negation words within `window` chars BEFORE the match.
    Returns True only if signal is found and NOT negated."""
    pos = 0
    neg_words = ['no ', 'not ', "n't ", 'without ', 'never ', 'lack of ',
                 'did not ', 'does not ', 'was not ', 'were not ',
                 "didn't ", "doesn't ", "wasn't ", "weren't ",
                 'absence of ', 'unable to find ']
    while pos < len(text):
        idx = text.find(signal, pos)
        if idx < 0:
            return False
        before = text[max(0, idx - window):idx]
        if not any(neg in before for neg in neg_words):
            return True   # found a non-negated occurrence
        pos = idx + 1     # this occurrence was negated, try next
    return False


def evaluate_agent_explanation(explanation):
    """Score an agent's self-assessment into DETERMINISTIC / HEURISTIC / GUESS.
    Parses the agent's natural-language explanation for key signals.
    Uses negation-aware matching: 'no column comment' does NOT count as DETERMINISTIC.
    Returns (confidence, reasoning_summary, details).

    DETERMINISTIC = agent cites a specific UC feature (comment, metric view, example SQL)
                    OR there is exactly one table/column match with no alternatives.
    HEURISTIC     = agent picked based on column/table name similarity,
                    acknowledges alternatives exist but chose 'best match'.
    GUESS         = agent assumed a threshold, admits uncertainty, or says
                    rephrasing might change the SQL.
    """
    if not explanation:
        return ("UNKNOWN", "No explanation provided", {})
    text = explanation.lower()
    details = {"full_text": explanation[:500]}
    # --- GUESS signals: these INCLUDE negation words by design ---
    # (e.g., 'no comment' means the agent LACKED guidance → GUESS)
    guess_signals = [
        ('assumed', 'Agent admitted assuming a value'),
        ('no definition', 'No definition found for business term'),
        ('common standard', 'Used a common industry standard, not from data'),
        ('typically', 'Applied general knowledge, not schema-specific'),
        ('generally', 'Applied general knowledge'),
        ('i chose', 'Agent made a subjective choice'),
        ('might change', 'Agent admits rephrasing could change SQL'),
        ('could vary', 'Agent admits result could vary'),
        ('not certain', 'Agent expressed uncertainty'),
        ('no comment', 'No column comment guided the choice'),
        ('no description', 'No table description available'),
        ('no column comment', 'No column comment available'),
        ('no table comment', 'No table comment available'),
    ]
    # GUESS signals: simple substring match (they inherently contain negation)
    guess_hits = [(sig, reason) for sig, reason in guess_signals if sig in text]
    # Check for threshold/cutoff only in POSITIVE context (agent defining one)
    if _positive_match(text, 'threshold'):
        guess_hits.append(('threshold', 'Agent mentioned a threshold it may have invented'))
    if _positive_match(text, 'cutoff'):
        guess_hits.append(('cutoff', 'Agent mentioned a cutoff it may have invented'))
    # --- DETERMINISTIC signals: MUST be in positive context ---
    # 'column comment' only counts if NOT preceded by 'no'/'not'
    det_signals = [
        ('column comment', 'Guided by a column comment (UC feature)'),
        ('table comment', 'Guided by a table comment (UC feature)'),
        ('table description', 'Guided by a table description'),
        ('metric view', 'Used a metric view (UC feature)'),
        ('example query', 'Followed an example query (UC feature)'),
        ('example sql', 'Followed an example SQL (UC feature)'),
        ('only table', 'Only one table had the relevant data'),
        ('only column', 'Only one column matched'),
        ('no other table', 'No alternative tables existed'),
        ('boolean column', 'Boolean column with unambiguous semantics'),
        ('flag column', 'Flag column directly answers the question'),
        ('same sql', 'Would generate identical SQL regardless of phrasing'),
        ('would not change', 'SQL would not change with rephrasing'),
    ]
    # Use _positive_match for UC feature signals (first 6) to avoid false positives
    # Structural signals (only table, boolean column, etc.) use simple match
    uc_feature_signals = det_signals[:6]  # column comment through example sql
    structural_signals = det_signals[6:]  # only table through would not change
    det_hits = [(sig, reason) for sig, reason in uc_feature_signals if _positive_match(text, sig)]
    det_hits += [(sig, reason) for sig, reason in structural_signals if sig in text]
    # --- HEURISTIC signals: name-based inference ---
    heur_signals = [
        ('column name', 'Chose based on column name similarity'),
        ('table name', 'Chose based on table name similarity'),
        ('name suggest', 'Column/table name suggested the mapping'),
        ('inferred', 'Agent inferred the mapping'),
        ('similar', 'Acknowledged similar alternatives'),
        ('best match', 'Picked best match among options'),
        ('most relevant', 'Picked most relevant option'),
        ('likely', 'Used probabilistic reasoning'),
        ('other table', 'Acknowledged other tables exist'),
        ('alternative', 'Acknowledged alternatives'),
    ]
    heur_hits = [(sig, reason) for sig, reason in heur_signals if _positive_match(text, sig)]
    details['guess_signals'] = [r for _, r in guess_hits]
    details['deterministic_signals'] = [r for _, r in det_hits]
    details['heuristic_signals'] = [r for _, r in heur_hits]
    # --- Classify ---
    # Strong guess signals override everything
    if len(guess_hits) >= 2 or any(s in text for s in ['assumed', 'might change', 'not certain']):
        reasons = '; '.join(r for _, r in guess_hits[:3])
        return ("GUESS", f"Agent admits uncertainty: {reasons}", details)
    # Strong deterministic signals
    if len(det_hits) >= 2 or any(s in text for s in ['only table', 'boolean column', 'would not change']):
        reasons = '; '.join(r for _, r in det_hits[:3])
        return ("DETERMINISTIC", f"Unambiguous path: {reasons}", details)
    # Heuristic: name-based with some confidence
    if heur_hits:
        reasons = '; '.join(r for _, r in heur_hits[:3])
        return ("HEURISTIC", f"Name-based inference: {reasons}", details)
    # Weak deterministic (at least one signal, no counter-signals)
    if det_hits and not guess_hits:
        reasons = '; '.join(r for _, r in det_hits[:2])
        return ("DETERMINISTIC", f"{reasons}", details)
    # Weak guess
    if guess_hits:
        reasons = '; '.join(r for _, r in guess_hits[:2])
        return ("GUESS", f"{reasons}", details)
    # Default: can't tell
    return ("HEURISTIC", "Agent explained its choice but signals were ambiguous", details)


def run_provenance_probes(stage_results, assumptions_list, spaces_dict, max_probes=15):
    """Probe agents for self-assessment on baseline-passing tests.
    Only probes tests that pass at baseline with claimed_fix != Baseline.
    Returns dict: test_id \u2192 agent's self-assessment text."""
    if "Baseline" not in stage_results:
        print("  No baseline results to probe")
        return {}
    baseline = {r["id"]: r for r in stage_results["Baseline"]}
    probes = {}
    n = 0
    for aid, agent_key, question, expected, desc, claimed_fix in assumptions_list:
        br = baseline.get(aid, {})
        if br.get("verdict") != "PASS" or claimed_fix in ("Baseline", "Direct"):
            continue
        if n >= max_probes:
            print(f"  (stopped at {max_probes} probes \u2014 increase max_probes for more)")
            break
        space_id = spaces_dict.get(agent_key)
        if not space_id:
            continue
        if n > 0:
            time.sleep(2)  # Rate limit guard: avoid 429 RESOURCE_EXHAUSTED
        print(f"  [{n+1}] {aid} ({agent_key}): ", end="", flush=True)
        explanation = probe_agent_reasoning(space_id, question, expected)
        if explanation:
            confidence, eval_summary, eval_details = evaluate_agent_explanation(explanation)
            probes[aid] = {
                "explanation": explanation,
                "confidence": confidence,
                "eval_summary": eval_summary,
                "signals": eval_details,
            }
            icon = {"DETERMINISTIC": "\u2705", "HEURISTIC": "\u26a0\ufe0f", "GUESS": "\u274c"}.get(confidence, "?")
            print(f"{icon} {confidence}: {eval_summary[:70]}")
        else:
            print("\u26a0 no response")
        n += 1
    # Summary
    counts = {"DETERMINISTIC": 0, "HEURISTIC": 0, "GUESS": 0}
    for p in probes.values():
        counts[p["confidence"]] = counts.get(p["confidence"], 0) + 1
    print(f"\n  Probed {n} tests, got {len(probes)} explanations")
    print(f"  \u2705 DETERMINISTIC: {counts['DETERMINISTIC']}  \u26a0\ufe0f HEURISTIC: {counts['HEURISTIC']}  \u274c GUESS: {counts['GUESS']}")
    return probes


def classify_tests(stage_results, assumptions_list, agent_probes=None):
    """Classify all 45 tests into 4 buckets based on actual results across stages.
    Evidence priority for baseline-passing tests:
      1. agent_probes (agent's own explanation via probe_agent_reasoning)
      2. [PROVENANCE:] tag in agent narration (if instructions were patched)
      3. analyze_agent_sql() parsing of actual SQL
      4. BASELINE_HOW hypothesis (our best guess)
    Returns list of dicts with classification per test."""
    stages_ordered = ["Baseline", "After Iteration 1", "After Iteration 2", "After Iteration 3"]
    available = [s for s in stages_ordered if s in stage_results]
    if not available:
        return []
    # Build per-test history
    history = {}
    for stage in available:
        for r in stage_results[stage]:
            history.setdefault(r["id"], {})[stage] = r
    def claimed_to_iter(cf):
        if "Iter 1" in cf: return "After Iteration 1"
        if "Iter 2" in cf: return "After Iteration 2"
        if "Iter 3" in cf: return "After Iteration 3"
        return None
    results = []
    for aid, agent_key, question, expected, desc, claimed_fix in assumptions_list:
        h = history.get(aid, {})
        verdicts = [h.get(s, {}).get("verdict", "?") for s in available]
        baseline_v = verdicts[0] if verdicts else "?"
        latest_v = verdicts[-1] if verdicts else "?"
        latest_r = h.get(available[-1], {})
        # Check for regression (pass then fail)
        has_regression = any(
            verdicts[i] == "PASS" and verdicts[i+1] == "FAIL"
            for i in range(len(verdicts)-1)
        )
        # Find first stage where it passes (after baseline)
        first_fix = None
        for s in available[1:]:
            if h.get(s, {}).get("verdict") == "PASS":
                first_fix = s
                break
        expected_iter = claimed_to_iter(claimed_fix)
        fragile_reason = FRAGILE_RISK.get(aid)
        if has_regression:
            cat = "Fragile/Non-Deterministic"
            expl = f"Regression: {' -> '.join(verdicts)}" + (f" | {fragile_reason}" if fragile_reason else "")
        elif baseline_v == "PASS":
            cat = "Self-Evident from Schema"
            baseline_r = h.get("Baseline", {})
            baseline_sql = baseline_r.get("sql", "")
            narration = baseline_r.get("narration", "")
            # Priority 1: Agent's own explanation (from probe_agent_reasoning)
            agent_says = (agent_probes or {}).get(aid)
            # Priority 2: [PROVENANCE:] tag in agent narration
            prov = parse_provenance(narration) if narration else None
            if agent_says:
                # GOLD STANDARD: agent told us + evaluator scored it
                if isinstance(agent_says, dict):
                    confidence = agent_says.get('confidence', 'UNKNOWN')
                    eval_sum = agent_says.get('eval_summary', '')
                    agent_text = agent_says.get('explanation', '').replace('\n', ' ')[:120]
                    mechanism = confidence  # DETERMINISTIC / HEURISTIC / GUESS
                    expl = f"{confidence} [agent]: {eval_sum}"
                else:
                    # Legacy format (plain string)
                    agent_text = agent_says.replace('\n', ' ')[:150]
                    says_lower = agent_says.lower()
                    if any(kw in says_lower for kw in ['assum', 'no definition', 'common standard', 'typically', 'generally']):
                        mechanism = "GUESS"
                    else:
                        mechanism = "DETERMINISTIC"
                    expl = f"{mechanism} [agent]: {agent_text}"
            elif prov:
                # Agent included [PROVENANCE:] tag in response
                method = prov.get('method', 'unknown')
                tables = prov.get('tables', '?')
                thresh = prov.get('assumed_thresholds', 'none')
                if thresh and thresh.lower() not in ('none', 'n/a', ''):
                    mechanism = "GUESS"
                    expl = f"GUESS [provenance]: assumed {thresh} on {tables}"
                elif method == 'metric_view':
                    mechanism = "METRIC_VIEW"
                    expl = f"METRIC_VIEW [provenance]: {tables} -> {prov.get('key_columns', '?')}"
                else:
                    mechanism = "SCHEMA"
                    expl = f"SCHEMA [provenance]: {tables} -> {prov.get('key_columns', '?')}"
            elif baseline_sql:
                # Priority 3: Parse actual SQL + narration + question for evidence
                baseline_narr = baseline_r.get("narration", "")
                mechanism, evidence, sql_details = analyze_agent_sql(
                    baseline_sql, aid, narration=baseline_narr, question=question)
                expl = f"{mechanism} [sql]: {evidence}"
            else:
                # Priority 4: Our hypothesis (least reliable)
                how = BASELINE_HOW.get(aid)
                mechanism = how[0] if how else "UNKNOWN"
                how_text = how[1] if how else ""
                tag = "hypothesis"
                if mechanism == "GUESS":
                    expl = f"GUESS ({tag}): {how_text}"
                elif claimed_fix in ("Baseline", "Direct"):
                    expl = f"{mechanism} ({tag}): {how_text}" if how else "Designed to pass at baseline"
                elif fragile_reason:
                    expl = f"{mechanism} ({tag}): {how_text}"
                else:
                    expl = f"{mechanism} ({tag}): {how_text}" if how else f"Passes despite '{claimed_fix}'"
        elif first_fix:
            if expected_iter == first_fix:
                cat = "Genuine UC Semantic Layer for Genie Agent Fix"
                expl = f"Failed baseline -> fixed by {claimed_fix}"
            elif expected_iter and first_fix != expected_iter:
                cat = "Collateral Benefit"
                expl = f"Expected '{claimed_fix}' but fixed earlier in {first_fix}"
            else:
                cat = "Genuine UC Semantic Layer for Genie Agent Fix"
                expl = f"Failed baseline -> fixed in {first_fix}"
        else:
            cat = "Genuine UC Semantic Layer for Genie Agent Fix"
            expl = f"Still failing; needs {claimed_fix}"
        # Store evidence for detail display
        sql_snippet = ""
        if baseline_v == "PASS":
            bs_r = h.get("Baseline", {})
            bs_sql = bs_r.get("sql", "")
            if bs_sql:
                sql_snippet = bs_sql[:120].replace('\n', ' ')
        probe_data = (agent_probes or {}).get(aid, {})
        if isinstance(probe_data, dict):
            agent_explanation = probe_data.get('explanation', '')[:200]
            agent_confidence = probe_data.get('confidence', '')
        else:
            agent_explanation = str(probe_data)[:200] if probe_data else ""
            agent_confidence = ""
        results.append({"id": aid, "category": cat, "claimed_fix": claimed_fix,
            "baseline": baseline_v, "current": latest_v,
            "provenance": latest_r.get("provenance", "N/A"),
            "explanation": expl, "trajectory": " -> ".join(verdicts),
            "sql_evidence": sql_snippet,
            "agent_explanation": agent_explanation,
            "agent_confidence": agent_confidence})
    return results


def plot_classification_dashboard(stage_results, assumptions_list, agent_probes=None):
    """Stacked bar chart showing 4-bucket classification progression across stages."""
    import matplotlib.pyplot as plt
    import numpy as np
    stages_ordered = ["Baseline", "After Iteration 1", "After Iteration 2", "After Iteration 3"]
    available = [s for s in stages_ordered if s in stage_results]
    if not available:
        print("No results yet.")
        return
    CAT_NAMES = [
        "Genuine UC Semantic Layer for Genie Agent Fix",
        "Self-Evident from Schema",
        "Collateral Benefit",
        "Fragile/Non-Deterministic",
    ]
    CAT_COLORS = ['#1565c0', '#43a047', '#ff9800', '#e53935']
    CAT_SHORT = ["Genuine UC Fix", "Self-Evident", "Collateral", "Fragile"]
    # Classify progressively at each stage
    stage_data = []
    for i in range(len(available)):
        partial = {s: stage_results[s] for s in available[:i+1]}
        cls = classify_tests(partial, assumptions_list, agent_probes)
        counts = {c: 0 for c in CAT_NAMES}
        for item in cls:
            counts[item["category"]] = counts.get(item["category"], 0) + 1
        stage_data.append(counts)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, max(3.5, len(available)*1.4)),
                                    gridspec_kw={'width_ratios': [2.5, 1]})
    # Panel 1: Stacked horizontal bar
    y = np.arange(len(available))
    left = np.zeros(len(available))
    for cat, color, short in zip(CAT_NAMES, CAT_COLORS, CAT_SHORT):
        vals = [stage_data[i].get(cat, 0) for i in range(len(available))]
        ax1.barh(y, vals, left=left, color=color, edgecolor='white', height=0.6, label=short)
        for j, (v, l) in enumerate(zip(vals, left)):
            if v > 1:
                ax1.text(l + v/2, j, str(v), ha='center', va='center',
                         fontsize=10, fontweight='bold', color='white')
        left += vals
    ax1.set_yticks(y)
    ax1.set_yticklabels(available, fontsize=10)
    ax1.set_xlabel('Number of Tests (45 total)')
    ax1.set_title('Test Classification: Why Each Test Passes or Fails',
                   fontsize=12, fontweight='bold')
    ax1.legend(loc='lower right', fontsize=8, ncol=2)
    ax1.set_xlim(0, 50)
    ax1.axvline(45, color='gray', linestyle='--', alpha=0.3)
    ax1.invert_yaxis()
    # Panel 2: Pie chart for latest stage
    latest_counts = stage_data[-1]
    pie_vals = [(latest_counts.get(c, 0), col, sh) for c, col, sh in zip(CAT_NAMES, CAT_COLORS, CAT_SHORT)]
    non_zero = [(v, c, s) for v, c, s in pie_vals if v > 0]
    if non_zero:
        wedges, texts, autotexts = ax2.pie(
                [x[0] for x in non_zero], colors=[x[1] for x in non_zero],
                labels=[f"{x[2]} ({x[0]})" for x in non_zero],
                autopct='%1.0f%%', startangle=90, textprops={'fontsize': 9})
        for t in autotexts:
            t.set_color('white')
            t.set_fontweight('bold')
            t.set_fontsize(11)
        ax2.set_title(f'Latest: {available[-1]}', fontsize=11, fontweight='bold')
    plt.tight_layout()
    plt.show()
    # Print detail table
    latest_cls = classify_tests(stage_results, assumptions_list, agent_probes)
    print(f"\n  CLASSIFICATION ({available[-1]}):")
    print(f"  {'ID':<5} {'Now':<7} {'Base':<5} {'Expected Fix':<28} {'Provenance (detected)':<38} Explanation")
    print(f"  {'_'*130}")
    prev_cat = ""
    for cl in sorted(latest_cls, key=lambda x: (CAT_NAMES.index(x['category']) if x['category'] in CAT_NAMES else 99, x['id'])):
        if cl['category'] != prev_cat:
            prev_cat = cl['category']
            count = sum(1 for c in latest_cls if c['category'] == prev_cat)
            print(f"\n  -- {cl['category']} ({count}) --")
        icon = {"PASS": "\u2705", "FAIL": "\u274c", "ERROR": "\u26a0\ufe0f"}.get(cl['current'], '?')
        risk = FRAGILE_RISK.get(cl['id'])
        risk_flag = " \u26a0\ufe0f FRAGILE RISK" if risk and cl['category'] != "Fragile/Non-Deterministic" else ""
        prov_short = (cl.get('provenance') or 'N/A')[:36]
        print(f"  {cl['id']:<5} {icon}{cl['current']:<5} {cl['baseline']:<5} {cl.get('claimed_fix',''):<28} {prov_short:<38} {cl['explanation']}{risk_flag}")
        if risk and cl['category'] != "Fragile/Non-Deterministic":
            print(f"        \u2514\u2500 Why fragile: {risk}")
        if cl.get('agent_confidence') and cl['baseline'] == 'PASS':
            conf_icon = {"DETERMINISTIC": "\u2705", "HEURISTIC": "\u26a0\ufe0f", "GUESS": "\u274c"}.get(cl['agent_confidence'], '?')
            print(f"        \u2514\u2500 Dependability: {conf_icon} {cl['agent_confidence']}")
            if cl.get('agent_explanation'):
                print(f"        \u2514\u2500 Agent says: {cl['agent_explanation'][:150]}")
        elif cl.get('sql_evidence') and cl['baseline'] == 'PASS':
            print(f"        \u2514\u2500 SQL: {cl['sql_evidence']}")

# COMMAND ----------

# DBTITLE 1,Step 1: Teardown (clean slate)
# Always run teardown first for idempotency — safe even if nothing exists to tear down
if RUN_MODE != "iterations_only":
    run_notebook("09_teardown")
    print("\n✓ Teardown complete — catalog and agents removed")
    print("  DROP CATALOG CASCADE removes: all tables, views, metric views, tags")
    print("  Agent deletion removes: all instruction modifications, certified queries")
    print("  Persists (by design): 6 UC Domains + 7 Pages (governance layer)")
else:
    print(f"⏭ Skipping teardown (run_mode=iterations_only — agents must already exist)")

# COMMAND ----------

# DBTITLE 1,Steps 2-8: Create Catalog + Generate Data + Setup Agents (NO column comments)
if RUN_MODE != "iterations_only":
    # Create catalog and schemas
    run_notebook("01_create_catalog_schemas")
    
    # Generate deterministic demo data (seeds 42-45, pinned to 1st of month)
    run_notebook("02_generate_demand_data")
    run_notebook("03_generate_inventory_data")
    run_notebook("04_generate_logistics_data")
    run_notebook("05_generate_supplier_data")
    
    # Create reporting views (NO column comments yet — that's Iteration 1)
    run_notebook("06_create_reporting_views")
    # NOTE: 07_add_all_comments is deliberately NOT run here.
    # At baseline, tables have bare column names with no descriptions.
    # Column descriptions are added in Iteration 1 to show their impact.
    
    # Create Genie Agents + Supervisor + Ground Truth
    run_notebook("08_setup_genie_supervisor", extra_params={"warehouse_id": WAREHOUSE_ID})
    
    print("\n✓ Steps 2-8 complete: data layer + agents created")

    # ── Comprehensive Setup Verification ──
    print("\n" + "="*60)
    print("  SETUP VERIFICATION")
    print("="*60)
    _errors = []
    # 1. Verify all schemas
    _expected_schemas = ['demand_analysis', 'inventory_management', 'logistics_operations', 'supplier_procurement', 'reporting']
    _actual_schemas = [r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()]
    for _s in _expected_schemas:
        if _s not in _actual_schemas:
            _errors.append(f"MISSING schema: {CATALOG}.{_s}")
    print(f"  Schemas: {len(_expected_schemas)}/{len(_expected_schemas)} {'✓' if not _errors else '✗'}")

    # 2. Verify all tables/views have rows
    _expected_tables = {
        'demand_analysis': ['products', 'customer_segments', 'sales_orders', 'demand_forecasts', 'pos_data', 'promotions'],
        'inventory_management': ['inventory_ledger', 'warehouse_data', 'store_inventory', 'stock_movements'],
        'logistics_operations': ['shipments', 'carriers', 'distribution_centers', 'transit_data'],
        'supplier_procurement': ['suppliers', 'supplier_orders', 'supplier_lead_times', 'vendor_slas', 'procurement_data'],
        'reporting': ['regional_performance_summary', 'revenue_trend', 'supply_chain_risk_scorecard', 'executive_kpis'],
    }
    _total, _ok = 0, 0
    for _schema, _tables in _expected_tables.items():
        for _t in _tables:
            _total += 1
            _fqn = f"{CATALOG}.{_schema}.{_t}"
            try:
                if not spark.catalog.tableExists(_fqn):
                    _errors.append(f"MISSING: {_fqn}")
                    continue
                _cnt = spark.table(_fqn).count()
                if _cnt == 0:
                    _errors.append(f"EMPTY: {_fqn} (0 rows)")
                    continue
                _ok += 1
            except Exception as _e:
                _errors.append(f"ERROR: {_fqn} — {str(_e)[:80]}")
    print(f"  Tables:  {_ok}/{_total} {'✓' if _ok == _total else '✗'}")

    # 3. Verify Baseline Genie agents via API
    _found_baseline = discover_agents_by_label("Baseline")
    _missing = {'demand', 'inventory', 'logistics', 'supplier', 'executive'} - set(_found_baseline.keys())
    if _missing:
        _errors.append(f"MISSING Baseline agents: {_missing}")
    print(f"  Agents:  {len(_found_baseline)}/5 {'✓' if not _missing else '✗'}")

    if _errors:
        print(f"\n  ✗ {len(_errors)} ERRORS:")
        for _e in _errors:
            print(f"    - {_e}")
        assert False, f"Setup verification failed with {len(_errors)} errors"
    else:
        print(f"\n  ✓ ALL CHECKS PASSED: {len(_expected_schemas)} schemas, {_total} tables, 5 agents")

    # Discover baseline agents and store in all_spaces
    spaces = discover_agents_by_label("Baseline")
    all_spaces["Baseline"] = dict(spaces)
    print(f"\n  Baseline agents discovered: {list(spaces.keys())}")
else:
    print(f"⏭ Skipping data creation (run_mode={RUN_MODE})")

# COMMAND ----------

# DBTITLE 1,Test Suite: 45 Questions × 5 Genie Agents
# MAGIC %md
# MAGIC ## Test Suite: 45 Questions × 5 Genie Agents
# MAGIC
# MAGIC ### The Business Prompt (sent to Supervisor Agent)
# MAGIC
# MAGIC The entire test suite is derived from a single CFO-style prompt. This is what the Supervisor receives:
# MAGIC
# MAGIC > *"We need a complete supply chain health check for our West region in August 2026. The CFO wants to understand what drove the revenue decline versus July — show the actual August and July revenue numbers, the dollar change, and the percentage change — and which product families are most at fault. Are our on-time delivery rate and average delay for West region shipments contributing to the problem? How many total shipments went out and how many were late? I also need our current fill rate, how many inventory positions are sitting below safety stock in the West region, how many unique SKUs are affected, what is our days of supply for those at-risk items, and how many SKUs are completely stocked out. On the vendor side: what percentage of vendors delivered late in August, how many purchase orders were late out of total, what is the average lead time variance, and what are the total vendor SLA penalties we have incurred? Bring it all together as our total Cost of Disruption by region for last month Aug 26 — cancelled revenue, at-risk backorder revenue, wasted freight on late shipments, and supplier penalty exposure in one number per region. Are we going to miss our Q1 service-level targets, and what are the top actions we should take? Additionally, how many shipments in August were flagged under the Logistics Risk Standards, how many Western region orders in August triggered a Demand Anomaly Alert, how many inventory positions are classified as supply-risk under Inventory Standards, how many supplier orders last month fell below the Procurement Quality Minimum, and how many suppliers exceeded the Executive Disruption Threshold?"*
# MAGIC
# MAGIC ### How the Curator Broke It Down into 45 Tests
# MAGIC
# MAGIC The Curator extracted every testable metric from the prompt, then **expanded** with edge cases, failure traps, and cross-domain challenges. Each row below shows which prompt fragment produced which tests — and why additional tests were added beyond the literal ask.
# MAGIC
# MAGIC | Prompt Fragment | Direct Tests | Additional Edge-Case Tests | Why the extras |
# MAGIC | --- | --- | --- | --- |
# MAGIC | *"revenue decline versus July — August and July revenue, dollar change, percentage change"* | B01 (Aug rev), B02 (Jul rev), B03 (\$ change), B04 (% change) | F01 ("West" synonym), F06 (product family decline) | F01 tests if agent maps "West" → "Western". F06 tests if agent can rank families. |
# MAGIC | *"on-time delivery rate and average delay"* | A01 (OTD rate), A03 (avg delay) | A02 (late rate = complement of OTD) | A02 verifies the agent computes the inverse correctly. |
# MAGIC | *"How many total shipments went out and how many were late?"* | A04 (total), A05 (late) | A06 (wasted freight cost) | A06 extends to dollar impact — not asked but derivable. |
# MAGIC | *"current fill rate"* | E01 (fill rate) | — | Synonym test: fill\_rate = service\_level\_pct. |
# MAGIC | *"inventory positions below safety stock, unique SKUs, days of supply, stocked out"* | C01 (positions), C02 (SKUs), C05 (DoS), C03 (stockouts), C04 (stockout SKUs) | — | 5 distinct metrics from one sentence. |
# MAGIC | *"percentage of vendors delivered late, POs late out of total, lead time variance, SLA penalties"* | D03 (late %), D01 (total POs), D02 (late POs), D04 (avg LTV), E02 (SLA penalties) | D05-D07 (Asia breakdown), F02 ("vendors" ambiguity) | D05-D07 test continent drill-down. F02 traps per-vendor vs per-order confusion. |
# MAGIC | *"Cost of Disruption by region"* | E03 (total CoD) | H05 (component sum), H06 (per-SKU ratio), H07 (CoD/revenue ratio) | H05-H07 test cross-domain joins that no single agent owns. |
# MAGIC | *"miss our Q1 service-level targets"* | G01 (will we miss?), G02 (what is the target?) | — | Q1 = fiscal Jul-Sep (current quarter). Target (92%) only in fiscal\_targets table. |
# MAGIC | *"Logistics Risk Standards"* | P01 (flagged shipments = 176) | — | Multi-condition policy: delay >= 5 AND weight > 800. Agent must learn from SQL Function. |
# MAGIC | *"Demand Anomaly Alert"* | P02 (anomaly orders = 77) | — | Multi-condition: qty >= 8 AND price < 30 AND Online. |
# MAGIC | *"supply-risk under Inventory Standards"* | P03 (risk positions = 106) | — | Multi-condition: DoS 1-11 AND below\_ss AND on\_hand > 0. |
# MAGIC | *"Procurement Quality Minimum"* | P04 (below-minimum orders = 11) | — | Multi-condition: score < 75 AND LTV > 12. |
# MAGIC | *"Executive Disruption Threshold"* | P05 (at-risk suppliers = 3) | — | Multi-condition: risk < 55 AND LTV > 8 AND penalty > 80K. |
# MAGIC | *(not in prompt — Curator-added traps)* | — | H01-H02 (wrong-table trap), H03-H04 (status ambiguity), F03-F05 (status filter) | H01-H02: agent picks `supplier_lead_times` instead of `supplier_orders`. H03-H04: `Fulfilled` vs `Partially_Fulfilled` distinction. F03-F05: filtered counts by order status. |
# MAGIC
# MAGIC **Summary**: 27 tests come directly from the prompt. 18 are Curator-added edge cases that expose fragility — agents may get the direct question right by coincidence but fail on variations that require genuine understanding.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Each question below is sent to a **specific domain agent** and the response is compared against a ground truth value at **exact 2-decimal precision**. No tolerance bands — either it matches or it doesn't.
# MAGIC
# MAGIC ### Group A: Logistics (6 tests → `SC Logistics` agent)
# MAGIC
# MAGIC | ID | Question | Ground Truth |
# MAGIC | --- | --- | --- |
# MAGIC | A01 | What is the on-time delivery rate for Western region shipments in August 2026? | 5.43 |
# MAGIC | A02 | What is the delayed shipment rate for Western region shipments last month? | 94.57 |
# MAGIC | A03 | What is the average delay in days for delayed shipments in the Western region last month? | 2.94 |
# MAGIC | A04 | How many total shipments went to the Western region in August? | 1086 |
# MAGIC | A05 | How many late shipments went to the Western region last month? | 1027 |
# MAGIC | A06 | What is the total wasted freight on late shipments in Western region last month? | 2484985.57 |
# MAGIC
# MAGIC ### Group B: Demand (4 tests → `SC Demand Analysis` agent)
# MAGIC
# MAGIC | ID | Question | Ground Truth |
# MAGIC | --- | --- | --- |
# MAGIC | B01 | What is the total revenue for the Western region in August 2026? | 3341062.58 |
# MAGIC | B02 | What was the total revenue for the Western region in July 2026? | 4581392.70 |
# MAGIC | B03 | What is the revenue change in dollars for Western region month-over-month? | -1240330.12 |
# MAGIC | B04 | What is the percentage change in revenue for Western region last month vs prior month? | -27.07 |
# MAGIC
# MAGIC ### Group C: Inventory (5 tests → `SC Inventory Management` agent)
# MAGIC
# MAGIC | ID | Question | Ground Truth |
# MAGIC | --- | --- | --- |
# MAGIC | C01 | How many inventory positions are below safety stock in the Western region? | 109 |
# MAGIC | C02 | How many unique SKUs are below safety stock in the Western region? | 61 |
# MAGIC | C03 | How many stockout positions are there in the Western region? | 35 |
# MAGIC | C04 | How many unique SKUs are completely stocked out in the Western region? | 33 |
# MAGIC | C05 | What is the average days of supply for at-risk items in the Western region? | 0.96 |
# MAGIC
# MAGIC ### Group D: Supplier (7 tests → `SC Supplier Procurement` agent)
# MAGIC
# MAGIC | ID | Question | Ground Truth |
# MAGIC | --- | --- | --- |
# MAGIC | D01 | How many total purchase orders were placed in August 2026? | 48 |
# MAGIC | D02 | How many purchase orders were delayed last month? | 36 |
# MAGIC | D03 | What percentage of purchase orders were delayed last month? | 75.00 |
# MAGIC | D04 | What is the average lead time variance in days for all suppliers last month? | 8.69 |
# MAGIC | D05 | What percentage of purchase orders from Asia suppliers were delayed in August? | 100.00 |
# MAGIC | D06 | What is the average lead time variance for Asia suppliers last month? | 13.67 |
# MAGIC | D07 | How many purchase orders did we place with Asia suppliers in August? | 30 |
# MAGIC
# MAGIC ### Group E: Cross-Domain & Executive (3 tests)
# MAGIC
# MAGIC | ID | Agent | Question | Ground Truth |
# MAGIC | --- | --- | --- | --- |
# MAGIC | E01 | `SC Executive` | What is our current fill rate? | 80.70 |
# MAGIC | E02 | `SC Supplier` | What are the total vendor SLA penalties we incurred? | 1185043.10 |
# MAGIC | E03 | `SC Executive` | What is the total Cost of Disruption for the Western region last month? | 3757298.31 |
# MAGIC
# MAGIC ### Group F: Indirect & Ambiguity (6 tests → `SC Demand Analysis` or `SC Supplier`)
# MAGIC
# MAGIC | ID | Agent | Question | Ground Truth |
# MAGIC | --- | --- | --- | --- |
# MAGIC | F01 | `SC Demand` | Show me the total revenue for the West region last month | 3341062.58 |
# MAGIC | F02 | `SC Supplier` | What percentage of vendors had delayed deliveries last month? | 75.00 |
# MAGIC | F03 | `SC Demand` | How many Western region orders were fulfilled last month? | 1342 |
# MAGIC | F04 | `SC Demand` | What is the total cancelled revenue in Western region in August? | 179419.26 |
# MAGIC | F05 | `SC Demand` | How many orders were backordered in Western region last month? | 275 |
# MAGIC | F06 | `SC Demand` | Which product family had the largest revenue decline in Western region last month vs prior month? | 349062.88 |
# MAGIC
# MAGIC ### Group H: Hard Failures (7 tests — guaranteed baseline misses)
# MAGIC
# MAGIC | ID | Agent | Question | Ground Truth | Failure Pattern |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | H01 | `SC Supplier` | What is the average lead time variance for Europe suppliers last month? | 0.38 | Wrong table trap |
# MAGIC | H02 | `SC Supplier` | What is the average lead time variance for North America suppliers last month? | 0.40 | Wrong table trap |
# MAGIC | H03 | `SC Demand` | What is the order fulfillment rate for Western region last month? | 71.23 | Status ambiguity |
# MAGIC | H04 | `SC Demand` | What percentage of Western region orders were only partially fulfilled last month? | 9.02 | Status value trap |
# MAGIC | H05 | `SC Executive` | What is the total revenue at risk from supply chain disruptions in Western region? | 3138569.66 | Cross-domain |
# MAGIC | H06 | `SC Inventory` | What is the average revenue at risk per stockout SKU in Western region? | 14368.63 | Cross-domain |
# MAGIC | H07 | `SC Executive` | What is our total cost of supply chain disruptions as a ratio of Western region revenue? | 1.12 | Cross-domain |
# MAGIC
# MAGIC ### Group G: Q1 Fiscal Calendar (2 tests → `SC Executive` agent)
# MAGIC
# MAGIC | ID | Question | Ground Truth | Why It's Hard |
# MAGIC | --- | --- | --- | --- |
# MAGIC | G01 | Are we going to miss our Q1 service-level targets? | 92.0 | Q1 = fiscal Jul-Sep (FY starts July). Target only in fiscal_targets table |
# MAGIC | G02 | What is our Q1 service-level target? | 92.0 | Target only in fiscal_targets table (not in any base table) |
# MAGIC
# MAGIC ### Group P: Critical Threshold Policies (5 tests — unguessable multi-condition rules)
# MAGIC
# MAGIC | ID | Agent | Question | Ground Truth | Policy Rule |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | P01 | `SC Logistics` | How many shipments in August were flagged under the Logistics Risk Standards? | 176 | `delay_days >= 5 AND total_weight_kg > 800` |
# MAGIC | P02 | `SC Demand` | How many Western region orders in August triggered a Demand Anomaly Alert? | 77 | `quantity >= 8 AND unit_price < 30 AND channel='Online'` |
# MAGIC | P03 | `SC Inventory` | How many inventory positions are classified as supply-risk under Inventory Standards? | 106 | `days_of_supply BETWEEN 1 AND 11 AND below_safety_stock_flag=true AND on_hand_qty > 0` |
# MAGIC | P04 | `SC Supplier` | How many supplier orders last month fell below the Procurement Quality Minimum? | 11 | `quality_score < 75 AND lead_time_variance_days > 12` |
# MAGIC | P05 | `SC Executive` | How many suppliers exceeded the Executive Disruption Threshold? | 3 | `composite_risk_score < 55 AND lead_time_variance > 8 AND total_penalty_usd > 80000` |

# COMMAND ----------

# DBTITLE 1,CURATOR BENCHMARKS: 45 tests across 9 domain groups (BASELINE)
# ============================================================
# CURATOR BENCHMARKS — COMPREHENSIVE
# Tests EVERY metric from the 4 metric views + CoD view + indirect GTs.
# Groups: (A) Logistics MV, (B) Demand MV, (C) Inventory MV,
#         (D) Supplier MV, (E) Cross-domain/Executive, (F) Indirect GTs.
# Shows FULL raw agent response. All GTs at 2-decimal precision.
# ============================================================
import time, requests, json, re

# --- Discover Baseline agents (label-based, not generic SC matching) ---
if 'spaces' not in dir() or not spaces or len(spaces) < 5:
    print("Discovering Baseline Genie Agent spaces...")
    spaces = discover_agents_by_label("Baseline")
    all_spaces["Baseline"] = dict(spaces)
else:
    print("Reusing existing Genie Agent space IDs...")

for name, sid in spaces.items():
    print(f"  {name:15s} \u2192 {sid}")


def ask_genie(space_id, question, timeout_secs=300, max_retries=4):
    """Send a question to a Genie agent via Agent Mode API.
    Uses POST /api/2.0/genie/agents/{id}/responses (SSE stream).
    Retries on 429 RESOURCE_EXHAUSTED with exponential backoff.
    Returns a dict compatible with extract_from_msg()."""
    payload = {
        "input": [{
            "type": "message", "role": "user",
            "content": [{"type": "input_text", "text": question}]
        }]
    }
    h = dict(headers)
    h["Accept"] = "text/event-stream"
    for attempt in range(max_retries + 1):
        try:
            resp = requests.post(
                f"{host}/api/2.0/genie/agents/{space_id}/responses",
                headers=h, json=payload, stream=False, timeout=timeout_secs
            )
        except requests.exceptions.Timeout:
            return {"error": "timeout"}
        if resp.status_code == 429:
            # Rate limited — exponential backoff: 15s, 30s, 60s, 120s
            wait = 15 * (2 ** attempt)
            print(f" \u23f3 rate-limited, retry {attempt+1}/{max_retries} in {wait}s...", end="", flush=True)
            time.sleep(wait)
            continue
        if resp.status_code != 200:
            return {"error": f"agent-mode failed: {resp.status_code} {resp.text[:500]}"}
        break  # success
    else:
        return {"error": f"agent-mode failed after {max_retries} retries: 429 RESOURCE_EXHAUSTED"}

    # Parse SSE events — line-by-line for robustness
    # Each data: line contains a full JSON object
    all_json = []
    for line in resp.text.split("\n"):
        line = line.strip()
        if line.startswith("data:"):
            json_str = line[5:].strip()
            if json_str:
                try: all_json.append(json.loads(json_str))
                except: pass

    sql_queries, answer, function_outputs = [], "", []

    def _extract_from_item(item):
        nonlocal answer
        itype = item.get("type", "")
        if itype == "function_call":
            try:
                args_raw = item.get("arguments", "{}")
                args = json.loads(args_raw) if isinstance(args_raw, str) else args_raw
                sql = args.get("query", args.get("sql", ""))
                if sql and sql.strip(): sql_queries.append(sql)
            except: pass
        elif itype == "function_call_output":
            out = item.get("output", "")
            if out: function_outputs.append(out)
        elif itype == "message" and item.get("role") == "assistant":
            for c in item.get("content", []):
                if c.get("type") == "output_text" and c.get("text"):
                    answer += c["text"]

    for data in all_json:
        # Individual event items (added/done)
        if "item" in data:
            _extract_from_item(data["item"])
        # response.completed carries ALL output items
        for out_item in data.get("response", {}).get("output", []):
            _extract_from_item(out_item)

    # Dedupe SQL (Agent Mode fires added+done events for each query)
    seen, unique_sqls = set(), []
    for s in sql_queries:
        key = s.strip()
        if key not in seen:
            seen.add(key)
            unique_sqls.append(s)

    # Build response compatible with extract_from_msg()
    attachments = []
    # Put last SQL first (the data query, not the function lookup)
    if unique_sqls:
        attachments.append({"query": {"query": unique_sqls[-1]}})
    # Combine answer + function outputs as narration text
    full_narration = answer
    for out in function_outputs:
        full_narration += "\n" + out
    for sql in unique_sqls:
        full_narration += "\n" + sql
    attachments.append({"text": {"content": full_narration}})
    return {"status": "COMPLETED", "attachments": attachments}


def extract_from_msg(msg):
    """Extract sql, narration, rows from a raw Genie message."""
    narration, sql, rows, cols = "", None, [], []
    for att in msg.get("attachments", []):
        txt = att.get("text", {}).get("content", "")
        if txt:
            narration += txt + "\n"
        q = att.get("query", {})
        if q.get("query") and not sql:
            sql = q["query"]
    qr = msg.get("query_result")
    if qr:
        cols = [c.get("name", "") for c in qr.get("columns", [])]
        for row in qr.get("data", {}).get("data_array", []):
            rows.append(dict(zip(cols, row)))
    return sql, narration.strip(), rows, cols


def find_value_in_text(text, expected):
    """Search text for a number matching expected at EXACT 2-decimal precision.
    Compares round(abs(found), 2) == round(abs(expected), 2).
    No tolerance bands — either it matches or it doesn't.
    Returns (is_match, matched_value, closest_value)."""
    target = round(abs(float(expected)), 2)
    best_diff = float('inf')
    closest = None
    for m in re.finditer(r'-?[\d,]+\.?\d*', text):
        try:
            num = float(m.group().replace(',', ''))
        except ValueError:
            continue
        rounded = round(abs(num), 2)
        if rounded == target:
            return True, num, num
        diff = abs(rounded - target)
        if diff < best_diff:
            best_diff, closest = diff, num
    return False, None, closest


def print_boxed_block(title, content):
    """Print a boxed multi-line block using the notebook's console style."""
    print(f"\n  \u250c\u2500 {title} {'(none)' if not content else ''}\u2500\u2500\u2500")
    if content:
        for line in str(content).strip().split("\n"):
            print(f"  \u2502 {line}")
    print(f"  \u2514{'\u2500'*70}")


print("\n" + "="*90)
print("  CURATOR BENCHMARKS: Comprehensive \u2014 ALL metric view measures + indirect GTs")
print("="*90)

# ============================================================
# COMPLETE TEST SUITE \u2014 every metric view measure + indirect GTs
# Grouped by metric view they map to.
# GT values from live SQL at 2 decimal precision.
#
# DESIGN PRINCIPLES:
#   1. Date phrasing varies: "August 2026" (once per group), then
#      "last month", "August", or no date \u2014 to test date inference.
#   2. "Q1" = fiscal Q1 = Jul-Aug-Sep for this org (FY starts July).
#      Agent needs fiscal_targets table to know Q1 target = 92%.
#   3. "West" vs "Western" \u2014 test region value mapping.
# ============================================================
assumptions = [
    # \u2500\u2500 GROUP A: Logistics MV (delivery_performance_by_region) \u2500\u2500
    ("A01", "logistics",
     "What is the on-time delivery rate for Western region shipments in August 2026?",
     5.43, "MV: on_time_delivery_rate", "Metric View (Iter 2)"),
    ("A02", "logistics",
     "What is the delayed shipment rate for Western region shipments last month?",
     94.57, "MV: late_delivery_rate", "Metric View (Iter 2)"),
    ("A03", "logistics",
     "What is the average delay in days for delayed shipments in the Western region last month?",
     2.94, "MV: avg_delay_days", "Metric View (Iter 2)"),
    ("A04", "logistics",
     "How many total shipments went to the Western region in August?",
     1086, "MV: total_shipments", "Metric View (Iter 2)"),
    ("A05", "logistics",
     "How many late shipments went to the Western region last month?",
     1027, "MV: late_shipments", "Metric View (Iter 2)"),
    ("A06", "logistics",
     "What is the total wasted freight on late shipments in Western region last month?",
     2484985.57, "MV: wasted_freight_cost", "Metric View (Iter 2)"),

    # \u2500\u2500 GROUP B: Demand MV (revenue_comparison_by_region) \u2500\u2500
    ("B01", "demand",
     "What is the total revenue for the Western region in August 2026?",
     3341062.58, "MV: revenue_last_month", "Metric View (Iter 2)"),
    ("B02", "demand",
     "What was the total revenue for the Western region in July 2026?",
     4581392.70, "MV: revenue_prior_month", "Metric View (Iter 2)"),
    ("B03", "demand",
     "What is the revenue change in dollars for Western region month-over-month?",
     -1240330.12, "MV: revenue_change_dollars", "Metric View (Iter 2)"),
    ("B04", "demand",
     "What is the percentage change in revenue for Western region last month vs prior month?",
     -27.07, "MV: revenue_change_pct", "Metric View (Iter 2)"),

    # \u2500\u2500 GROUP C: Inventory MV (inventory_safety_stock_metrics) \u2500\u2500
    ("C01", "inventory",
     "How many inventory positions are below safety stock in the Western region?",
     109, "MV: positions_below_safety_stock", "Metric View (Iter 2)"),
    ("C02", "inventory",
     "How many unique SKUs are below safety stock in the Western region?",
     61, "MV: unique_skus_below_safety", "Metric View (Iter 2)"),
    ("C03", "inventory",
     "How many stockout positions are there in the Western region?",
     35, "MV: stockout_positions", "Metric View (Iter 2)"),
    ("C04", "inventory",
     "How many unique SKUs are completely stocked out in the Western region?",
     33, "MV: unique_skus_in_stockout", "Metric View (Iter 2)"),
    ("C05", "inventory",
     "What is the average days of supply for at-risk items in the Western region?",
     0.96, "MV: avg_days_of_supply", "Metric View (Iter 2)"),

    # \u2500\u2500 GROUP D: Supplier MV (supplier_performance_by_continent) \u2500\u2500
    ("D01", "supplier",
     "How many total purchase orders were placed in August 2026?",
     48, "MV: total_purchase_orders (overall)", "Metric View (Iter 2)"),
    ("D02", "supplier",
     "How many purchase orders were delayed last month?",
     36, "MV: late_purchase_orders (overall)", "Metric View (Iter 2)"),
    ("D03", "supplier",
     "What percentage of purchase orders were delayed last month?",
     75.00, "MV: supplier_late_rate_pct (overall)", "Metric View (Iter 2)"),
    ("D04", "supplier",
     "What is the average lead time variance in days for all suppliers last month?",
     8.69, "MV: avg_lead_time_variance (overall)", "Metric View (Iter 2)"),
    ("D05", "supplier",
     "What percentage of purchase orders from Asia suppliers were delayed in August?",
     100.00, "MV: supplier_late_rate_pct (Asia)", "Metric View (Iter 2)"),
    ("D06", "supplier",
     "What is the average lead time variance for Asia suppliers last month?",
     13.67, "MV: avg_lead_time_variance (Asia)", "Metric View (Iter 2)"),
    ("D07", "supplier",
     "How many purchase orders did we place with Asia suppliers in August?",
     30, "MV: total_purchase_orders (Asia)", "Metric View (Iter 2)"),

    # \u2500\u2500 GROUP E: Cross-domain & Executive \u2500\u2500
    ("E01", "executive",
     "What is our current fill rate?",
     80.70, "fill rate = service_level_pct", "Baseline"),
    ("E02", "supplier",
     "What are the total vendor SLA penalties we incurred?",
     1185043.10, "SLA penalty = SUM(penalty_amount)", "Baseline"),
    ("E03", "executive",
     "What is the total Cost of Disruption for the Western region last month?",
     3757298.31, "Cross-domain metric (no table exists)", "CoD View (Iter 2)"),

    # \u2500\u2500 GROUP F: Indirect GTs & Ambiguity Tests \u2500\u2500
    ("F01", "demand",
     "Show me the total revenue for the West region last month",
     3341062.58, "West \u2192 Western region mapping", "Instruction"),
    ("F02", "supplier",
     "What percentage of vendors had delayed deliveries last month?",
     75.00, "Vendor late % (per-order vs per-vendor ambiguity)", "Certified Query (Iter 1)"),
    ("F03", "demand",
     "How many Western region orders were fulfilled last month?",
     1342, "Fulfilled order count", "Direct"),
    ("F04", "demand",
     "What is the total cancelled revenue in Western region in August?",
     179419.26, "Cancelled revenue", "Direct"),
    ("F05", "demand",
     "How many orders were backordered in Western region last month?",
     275, "Backordered order count", "Direct"),
    ("F06", "demand",
     "Which product family had the largest revenue decline in Western region last month vs prior month?",
     349062.88, "Worst product family decline (Home Goods)", "Certified Query (Iter 1)"),

    # \u2500\u2500 GROUP H: HARD FAILURES \u2014 guaranteed baseline misses \u2500\u2500
    # These exploit proven failure patterns: wrong table, status ambiguity, cross-domain.
    # Agents CANNOT answer these correctly at baseline.
    ("H01", "supplier",
     "What is the average lead time variance for Europe suppliers last month?",
     0.38, "Wrong table: supplier_lead_times vs supplier_orders (Europe)", "Comment (Iter 1) / Metric View (Iter 2)"),
    ("H02", "supplier",
     "What is the average lead time variance for North America suppliers last month?",
     0.40, "Wrong table: supplier_lead_times vs supplier_orders (NA)", "Comment (Iter 1) / Metric View (Iter 2)"),
    ("H03", "demand",
     "What is the order fulfillment rate for Western region last month?",
     71.23, "Status ambiguity: Fulfilled only vs incl Partially_Fulfilled", "Comment (Iter 1) / Certified Query (Iter 1)"),
    ("H04", "demand",
     "What percentage of Western region orders were only partially fulfilled last month?",
     9.02, "Status value: Partially_Fulfilled exact definition", "Comment (Iter 1)"),
    ("H05", "executive",
     "What is the total revenue at risk from supply chain disruptions in Western region including cancelled revenue, backordered revenue, and wasted freight combined?",
     3138569.66, "Cross-domain: demand + logistics (no single agent has both)", "CoD View (Iter 2)"),
    ("H06", "inventory",
     "What is the average revenue at risk per stockout SKU in Western region?",
     14368.63, "Cross-domain: inventory stockouts + demand revenue", "Metric View (Iter 2)"),
    ("H07", "executive",
     "What is our total cost of supply chain disruptions as a ratio of Western region revenue?",
     1.12, "Cross-domain: CoD / revenue ratio", "CoD View (Iter 2)"),

    # \u2500\u2500 GROUP G: Q1 Fiscal Calendar (UC Pages) \u2500\u2500
    ("G01", "executive",
     "Are we going to miss our Q1 service-level targets?",
     92.0, "Q1 target (in fiscal_targets table, Q1=Jul-Sep fiscal)", "Reference Table (Iter 3)"),
    ("G02", "executive",
     "What is our Q1 service-level target?",
     92.0, "Q1 target value (in fiscal_targets table)", "Reference Table (Iter 3)"),

    # ── GROUP P: UC PAGES ONLY — domain-specific "critical" thresholds ──
    # Domain-specific POLICY NAMES — each defined only in its UC Page.
    # Without the UC Page, agent cannot map policy names to column thresholds.
    ("P01", "logistics",
     "How many shipments in August were flagged under the Logistics Risk Standards?",
     176, "UC Page: Logistics Risk = delay >= 5 AND weight > 800", "UC Pages / SQL Function (Iter 3)"),
    ("P02", "demand",
     "How many Western region orders in August triggered a Demand Anomaly Alert?",
     77, "UC Page: Demand Anomaly = qty >= 8 AND price < 30 AND Online", "UC Pages / SQL Function (Iter 3)"),
    ("P03", "inventory",
     "How many inventory positions are classified as supply-risk under Inventory Standards?",
     106, "UC Page: Inventory Risk = dos 1-11 AND below_ss AND on_hand > 0", "UC Pages / SQL Function (Iter 3)"),
    ("P04", "supplier",
     "How many supplier orders last month fell below the Procurement Quality Minimum?",
     11, "UC Page: Procurement Quality = score < 75 AND ltv > 12", "UC Pages / SQL Function (Iter 3)"),
    ("P05", "executive",
     "How many suppliers exceeded the Executive Disruption Threshold?",
     3, "UC Page: Exec Disruption = risk < 55 AND ltv > 8 AND penalty > 80K", "UC Pages / SQL Function (Iter 3)"),
]

GT_QUERIES = {
    # Query 5 in expected_output_reference (A01-A05)
    "A01": f"""-- expected_output_reference / Query 5 (Western row carries A01-A05)
SELECT
  destination_region,
  COUNT(*) AS total_shipments,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_shipments,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(CASE WHEN is_late THEN delay_days ELSE NULL END), 1) AS avg_delay_when_late
FROM {CATALOG}.logistics_operations.shipments
WHERE ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND ship_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY destination_region ORDER BY late_pct DESC""",
    "A02": f"""-- expected_output_reference / Query 5 (Western row carries A01-A05)
SELECT
  destination_region,
  COUNT(*) AS total_shipments,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_shipments,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(CASE WHEN is_late THEN delay_days ELSE NULL END), 1) AS avg_delay_when_late
FROM {CATALOG}.logistics_operations.shipments
WHERE ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND ship_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY destination_region ORDER BY late_pct DESC""",
    "A03": f"""-- expected_output_reference / Query 5 (Western row carries A01-A05)
SELECT
  destination_region,
  COUNT(*) AS total_shipments,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_shipments,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(CASE WHEN is_late THEN delay_days ELSE NULL END), 1) AS avg_delay_when_late
FROM {CATALOG}.logistics_operations.shipments
WHERE ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND ship_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY destination_region ORDER BY late_pct DESC""",
    "A04": f"""-- expected_output_reference / Query 5 (Western row carries A01-A05)
SELECT
  destination_region,
  COUNT(*) AS total_shipments,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_shipments,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(CASE WHEN is_late THEN delay_days ELSE NULL END), 1) AS avg_delay_when_late
FROM {CATALOG}.logistics_operations.shipments
WHERE ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND ship_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY destination_region ORDER BY late_pct DESC""",
    "A05": f"""-- expected_output_reference / Query 5 (Western row carries A01-A05)
SELECT
  destination_region,
  COUNT(*) AS total_shipments,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_shipments,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(CASE WHEN is_late THEN delay_days ELSE NULL END), 1) AS avg_delay_when_late
FROM {CATALOG}.logistics_operations.shipments
WHERE ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND ship_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY destination_region ORDER BY late_pct DESC""",

    # Dedicated GT query in expected_output_reference cell 44
    "A06": f"""-- expected_output_reference / GT Query A06
SELECT
  destination_region,
  ROUND(SUM(freight_cost), 2) AS wasted_freight_cost
FROM {CATALOG}.logistics_operations.shipments
WHERE is_late = true
  AND ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND ship_date <  DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY destination_region
ORDER BY wasted_freight_cost DESC""",

    # Query 1 in expected_output_reference (B01-B04, F01)
    "B01": f"""-- expected_output_reference / Query 1 (Western row carries B01-B04, F01)
SELECT
  region,
  DATE_FORMAT(DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)), 'MMMM yyyy') AS last_month_name,
  DATE_FORMAT(DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)), 'MMMM yyyy') AS prior_month_name,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
                  AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END), 2) AS revenue_last_month,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2))
                  AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 2) AS revenue_prior_month,
  ROUND(
    SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) -
    SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)
  , 2) AS revenue_change,
  ROUND(
    (SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) -
     SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)) /
    NULLIF(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 0) * 100
  , 1) AS pct_change
FROM {CATALOG}.demand_analysis.sales_orders
GROUP BY region ORDER BY revenue_change ASC""",
    "B02": f"""-- expected_output_reference / Query 1 (Western row carries B01-B04, F01)
SELECT
  region,
  DATE_FORMAT(DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)), 'MMMM yyyy') AS last_month_name,
  DATE_FORMAT(DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)), 'MMMM yyyy') AS prior_month_name,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
                  AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END), 2) AS revenue_last_month,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2))
                  AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 2) AS revenue_prior_month,
  ROUND(
    SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) -
    SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)
  , 2) AS revenue_change,
  ROUND(
    (SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) -
     SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)) /
    NULLIF(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 0) * 100
  , 1) AS pct_change
FROM {CATALOG}.demand_analysis.sales_orders
GROUP BY region ORDER BY revenue_change ASC""",
    "B03": f"""-- expected_output_reference / Query 1 (Western row carries B01-B04, F01)
SELECT
  region,
  DATE_FORMAT(DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)), 'MMMM yyyy') AS last_month_name,
  DATE_FORMAT(DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)), 'MMMM yyyy') AS prior_month_name,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
                  AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END), 2) AS revenue_last_month,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2))
                  AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 2) AS revenue_prior_month,
  ROUND(
    SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) -
    SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)
  , 2) AS revenue_change,
  ROUND(
    (SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) -
     SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)) /
    NULLIF(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 0) * 100
  , 1) AS pct_change
FROM {CATALOG}.demand_analysis.sales_orders
GROUP BY region ORDER BY revenue_change ASC""",
    "B04": f"""-- expected_output_reference / Query 1 (Western row carries B01-B04, F01)
SELECT
  region,
  DATE_FORMAT(DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)), 'MMMM yyyy') AS last_month_name,
  DATE_FORMAT(DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)), 'MMMM yyyy') AS prior_month_name,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
                  AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END), 2) AS revenue_last_month,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2))
                  AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 2) AS revenue_prior_month,
  ROUND(
    SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) -
    SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)
  , 2) AS revenue_change,
  ROUND(
    (SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) -
     SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)) /
    NULLIF(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 0) * 100
  , 1) AS pct_change
FROM {CATALOG}.demand_analysis.sales_orders
GROUP BY region ORDER BY revenue_change ASC""",

    # Query 4 in expected_output_reference (C01-C05)
    "C01": f"""-- expected_output_reference / Query 4 (Western row carries C01-C05)
SELECT
  region,
  COUNT(*) AS positions_below_safety_stock,
  COUNT(DISTINCT sku_id) AS unique_skus_below_safety,
  SUM(CASE WHEN stockout_flag = true THEN 1 ELSE 0 END) AS stockout_positions,
  COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS stockout_skus,
  ROUND(AVG(days_of_supply), 2) AS avg_days_of_supply
FROM {CATALOG}.inventory_management.inventory_ledger
WHERE below_safety_stock_flag = true
GROUP BY region
ORDER BY positions_below_safety_stock DESC""",
    "C02": f"""-- expected_output_reference / Query 4 (Western row carries C01-C05)
SELECT
  region,
  COUNT(*) AS positions_below_safety_stock,
  COUNT(DISTINCT sku_id) AS unique_skus_below_safety,
  SUM(CASE WHEN stockout_flag = true THEN 1 ELSE 0 END) AS stockout_positions,
  COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS stockout_skus,
  ROUND(AVG(days_of_supply), 2) AS avg_days_of_supply
FROM {CATALOG}.inventory_management.inventory_ledger
WHERE below_safety_stock_flag = true
GROUP BY region
ORDER BY positions_below_safety_stock DESC""",
    "C03": f"""-- expected_output_reference / Query 4 (Western row carries C01-C05)
SELECT
  region,
  COUNT(*) AS positions_below_safety_stock,
  COUNT(DISTINCT sku_id) AS unique_skus_below_safety,
  SUM(CASE WHEN stockout_flag = true THEN 1 ELSE 0 END) AS stockout_positions,
  COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS stockout_skus,
  ROUND(AVG(days_of_supply), 2) AS avg_days_of_supply
FROM {CATALOG}.inventory_management.inventory_ledger
WHERE below_safety_stock_flag = true
GROUP BY region
ORDER BY positions_below_safety_stock DESC""",
    "C04": f"""-- expected_output_reference / Query 4 (Western row carries C01-C05)
SELECT
  region,
  COUNT(*) AS positions_below_safety_stock,
  COUNT(DISTINCT sku_id) AS unique_skus_below_safety,
  SUM(CASE WHEN stockout_flag = true THEN 1 ELSE 0 END) AS stockout_positions,
  COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS stockout_skus,
  ROUND(AVG(days_of_supply), 2) AS avg_days_of_supply
FROM {CATALOG}.inventory_management.inventory_ledger
WHERE below_safety_stock_flag = true
GROUP BY region
ORDER BY positions_below_safety_stock DESC""",
    "C05": f"""-- expected_output_reference / Query 4 (Western row carries C01-C05)
SELECT
  region,
  COUNT(*) AS positions_below_safety_stock,
  COUNT(DISTINCT sku_id) AS unique_skus_below_safety,
  SUM(CASE WHEN stockout_flag = true THEN 1 ELSE 0 END) AS stockout_positions,
  COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS stockout_skus,
  ROUND(AVG(days_of_supply), 2) AS avg_days_of_supply
FROM {CATALOG}.inventory_management.inventory_ledger
WHERE below_safety_stock_flag = true
GROUP BY region
ORDER BY positions_below_safety_stock DESC""",

    # Query 7 in expected_output_reference (D01-D07, H01, H02, F02)
    "D01": f"""-- expected_output_reference / Query 7 (continent rows + overall rollup in finding)
SELECT
  supplier_continent,
  COUNT(*) AS total_pos,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_pos,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(lead_time_variance_days), 1) AS avg_variance_days
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY supplier_continent ORDER BY late_pct DESC""",
    "D02": f"""-- expected_output_reference / Query 7 (continent rows + overall rollup in finding)
SELECT
  supplier_continent,
  COUNT(*) AS total_pos,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_pos,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(lead_time_variance_days), 1) AS avg_variance_days
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY supplier_continent ORDER BY late_pct DESC""",
    "D03": f"""-- expected_output_reference / Query 7 (continent rows + overall rollup in finding)
SELECT
  supplier_continent,
  COUNT(*) AS total_pos,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_pos,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(lead_time_variance_days), 1) AS avg_variance_days
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY supplier_continent ORDER BY late_pct DESC""",
    "D04": f"""-- expected_output_reference / Query 7 (continent rows + overall rollup in finding)
SELECT
  supplier_continent,
  COUNT(*) AS total_pos,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_pos,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(lead_time_variance_days), 1) AS avg_variance_days
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY supplier_continent ORDER BY late_pct DESC""",
    "D05": f"""-- expected_output_reference / Query 7 (continent rows + overall rollup in finding)
SELECT
  supplier_continent,
  COUNT(*) AS total_pos,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_pos,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(lead_time_variance_days), 1) AS avg_variance_days
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY supplier_continent ORDER BY late_pct DESC""",
    "D06": f"""-- expected_output_reference / Query 7 (continent rows + overall rollup in finding)
SELECT
  supplier_continent,
  COUNT(*) AS total_pos,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_pos,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(lead_time_variance_days), 1) AS avg_variance_days
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY supplier_continent ORDER BY late_pct DESC""",
    "D07": f"""-- expected_output_reference / Query 7 (continent rows + overall rollup in finding)
SELECT
  supplier_continent,
  COUNT(*) AS total_pos,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_pos,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(lead_time_variance_days), 1) AS avg_variance_days
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY supplier_continent ORDER BY late_pct DESC""",

    # Dedicated GT queries in expected_output_reference cells 45-47
    "E01": f"""-- expected_output_reference / GT Query E01
SELECT
  metric_name,
  ROUND(metric_value, 2) AS metric_value
FROM {CATALOG}.reporting.executive_kpis
WHERE metric_name = 'service_level_pct'""",
    "E02": f"""-- expected_output_reference / GT Query E02
SELECT
  ROUND(SUM(penalty_amount), 2) AS total_sla_penalties
FROM {CATALOG}.supplier_procurement.vendor_slas
WHERE penalty_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND penalty_date <  DATE_TRUNC('month', DATE '2026-09-01')""",
    "E03": f"""-- expected_output_reference / GT Query E03
SELECT
  region,
  ROUND(cancelled_revenue, 2)    AS cancelled_revenue,
  ROUND(backordered_revenue, 2)  AS backordered_revenue,
  ROUND(late_shipping_cost, 2)   AS late_shipping_cost,
  ROUND(sla_penalties, 2)        AS sla_penalties,
  ROUND(total_cost_of_disruption, 2) AS total_cost_of_disruption
FROM {CATALOG}.reporting.cost_of_disruption_by_region
ORDER BY total_cost_of_disruption DESC""",

    # Query 1 reused for West -> Western mapping
    "F01": f"""-- expected_output_reference / Query 1 (Western row verifies West -> Western mapping)
SELECT
  region,
  DATE_FORMAT(DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)), 'MMMM yyyy') AS last_month_name,
  DATE_FORMAT(DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)), 'MMMM yyyy') AS prior_month_name,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
                  AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END), 2) AS revenue_last_month,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2))
                  AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 2) AS revenue_prior_month,
  ROUND(
    SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) -
    SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)
  , 2) AS revenue_change,
  ROUND(
    (SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) -
     SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)) /
    NULLIF(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 0) * 100
  , 1) AS pct_change
FROM {CATALOG}.demand_analysis.sales_orders
GROUP BY region ORDER BY revenue_change ASC""",
    "F02": f"""-- expected_output_reference / Query 7 (overall finding defines per-order late rate = 75.00%)
SELECT
  supplier_continent,
  COUNT(*) AS total_pos,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_pos,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(lead_time_variance_days), 1) AS avg_variance_days
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY supplier_continent ORDER BY late_pct DESC""",
    "F03": f"""-- expected_output_reference / Query 3 (Western row status breakdown)
SELECT order_status, COUNT(*) AS order_count,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_orders
FROM {CATALOG}.demand_analysis.sales_orders
WHERE region = 'Western'
  AND order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY order_status ORDER BY order_count DESC""",
    "F04": f"""-- expected_output_reference / Query 3 (Western row status breakdown)
SELECT order_status, COUNT(*) AS order_count,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_orders
FROM {CATALOG}.demand_analysis.sales_orders
WHERE region = 'Western'
  AND order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY order_status ORDER BY order_count DESC""",
    "F05": f"""-- expected_output_reference / Query 3 (Western row status breakdown)
SELECT order_status, COUNT(*) AS order_count,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_orders
FROM {CATALOG}.demand_analysis.sales_orders
WHERE region = 'Western'
  AND order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY order_status ORDER BY order_count DESC""",
    "F06": f"""-- expected_output_reference / Query 2
SELECT
  product_family,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END), 2) AS rev_last_month,
  ROUND(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 2) AS rev_prior_month,
  ROUND(
    SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) -
    SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)
  , 2) AS change_usd,
  ROUND(
    (SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN total_amount ELSE 0 END) -
     SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END)) /
    NULLIF(SUM(CASE WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2)) AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN total_amount ELSE 0 END), 0) * 100
  , 1) AS pct_change
FROM {CATALOG}.demand_analysis.sales_orders
WHERE region = 'Western'
GROUP BY product_family ORDER BY change_usd ASC""",

    # Query 3 reused for H03/H04
    "H01": f"""-- expected_output_reference / Query 7 (Europe row)
SELECT
  supplier_continent,
  COUNT(*) AS total_pos,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_pos,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(lead_time_variance_days), 1) AS avg_variance_days
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY supplier_continent ORDER BY late_pct DESC""",
    "H02": f"""-- expected_output_reference / Query 7 (North America row)
SELECT
  supplier_continent,
  COUNT(*) AS total_pos,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_pos,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(lead_time_variance_days), 1) AS avg_variance_days
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY supplier_continent ORDER BY late_pct DESC""",
    "H03": f"""-- expected_output_reference / Query 3 (Fulfilled row and denominator define H03)
SELECT order_status, COUNT(*) AS order_count,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_orders
FROM {CATALOG}.demand_analysis.sales_orders
WHERE region = 'Western'
  AND order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY order_status ORDER BY order_count DESC""",
    "H04": f"""-- expected_output_reference / Query 3 (Partially_Fulfilled row defines H04)
SELECT order_status, COUNT(*) AS order_count,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_orders
FROM {CATALOG}.demand_analysis.sales_orders
WHERE region = 'Western'
  AND order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY order_status ORDER BY order_count DESC""",
    "H05": f"""-- expected_output_reference / GT Queries H05-H07
WITH western_cancelled AS (
  SELECT ROUND(SUM(total_amount), 2) AS cancelled_revenue
  FROM {CATALOG}.demand_analysis.sales_orders
  WHERE region = 'Western'
    AND order_status = 'Cancelled'
    AND order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND order_date <  DATE_TRUNC('month', DATE '2026-09-01')
),
western_backordered AS (
  SELECT ROUND(SUM(total_amount), 2) AS backordered_revenue
  FROM {CATALOG}.demand_analysis.sales_orders
  WHERE region = 'Western'
    AND order_status = 'Backordered'
    AND order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND order_date <  DATE_TRUNC('month', DATE '2026-09-01')
),
western_freight AS (
  SELECT ROUND(SUM(freight_cost), 2) AS wasted_freight
  FROM {CATALOG}.logistics_operations.shipments
  WHERE destination_region = 'Western'
    AND is_late = true
    AND ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND ship_date <  DATE_TRUNC('month', DATE '2026-09-01')
),
western_stockouts AS (
  SELECT COUNT(DISTINCT sku_id) AS stockout_skus
  FROM {CATALOG}.inventory_management.inventory_ledger
  WHERE region = 'Western' AND stockout_flag = true AND below_safety_stock_flag = true
),
western_revenue AS (
  SELECT ROUND(SUM(total_amount), 2) AS aug_revenue
  FROM {CATALOG}.demand_analysis.sales_orders
  WHERE region = 'Western'
    AND order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND order_date <  DATE_TRUNC('month', DATE '2026-09-01')
),
western_cod AS (
  SELECT ROUND(total_cost_of_disruption, 2) AS cod
  FROM {CATALOG}.reporting.cost_of_disruption_by_region
  WHERE region = 'Western'
)
SELECT
  'H05' AS test_id,
  'Revenue at risk (cancelled + backordered + freight)' AS description,
  ROUND(c.cancelled_revenue + b.backordered_revenue + f.wasted_freight, 2) AS gt_value
FROM western_cancelled c, western_backordered b, western_freight f
UNION ALL
SELECT
  'H06',
  'Avg revenue at risk per stockout SKU (backordered / stockout SKUs)',
  ROUND(b.backordered_revenue / s.stockout_skus, 2)
FROM western_backordered b, western_stockouts s
UNION ALL
SELECT
  'H07',
  'CoD / Western revenue ratio',
  ROUND(cod.cod / r.aug_revenue, 2)
FROM western_cod cod, western_revenue r""",
    "H06": f"""-- expected_output_reference / GT Queries H05-H07
WITH western_cancelled AS (
  SELECT ROUND(SUM(total_amount), 2) AS cancelled_revenue
  FROM {CATALOG}.demand_analysis.sales_orders
  WHERE region = 'Western'
    AND order_status = 'Cancelled'
    AND order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND order_date <  DATE_TRUNC('month', DATE '2026-09-01')
),
western_backordered AS (
  SELECT ROUND(SUM(total_amount), 2) AS backordered_revenue
  FROM {CATALOG}.demand_analysis.sales_orders
  WHERE region = 'Western'
    AND order_status = 'Backordered'
    AND order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND order_date <  DATE_TRUNC('month', DATE '2026-09-01')
),
western_freight AS (
  SELECT ROUND(SUM(freight_cost), 2) AS wasted_freight
  FROM {CATALOG}.logistics_operations.shipments
  WHERE destination_region = 'Western'
    AND is_late = true
    AND ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND ship_date <  DATE_TRUNC('month', DATE '2026-09-01')
),
western_stockouts AS (
  SELECT COUNT(DISTINCT sku_id) AS stockout_skus
  FROM {CATALOG}.inventory_management.inventory_ledger
  WHERE region = 'Western' AND stockout_flag = true AND below_safety_stock_flag = true
),
western_revenue AS (
  SELECT ROUND(SUM(total_amount), 2) AS aug_revenue
  FROM {CATALOG}.demand_analysis.sales_orders
  WHERE region = 'Western'
    AND order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND order_date <  DATE_TRUNC('month', DATE '2026-09-01')
),
western_cod AS (
  SELECT ROUND(total_cost_of_disruption, 2) AS cod
  FROM {CATALOG}.reporting.cost_of_disruption_by_region
  WHERE region = 'Western'
)
SELECT
  'H05' AS test_id,
  'Revenue at risk (cancelled + backordered + freight)' AS description,
  ROUND(c.cancelled_revenue + b.backordered_revenue + f.wasted_freight, 2) AS gt_value
FROM western_cancelled c, western_backordered b, western_freight f
UNION ALL
SELECT
  'H06',
  'Avg revenue at risk per stockout SKU (backordered / stockout SKUs)',
  ROUND(b.backordered_revenue / s.stockout_skus, 2)
FROM western_backordered b, western_stockouts s
UNION ALL
SELECT
  'H07',
  'CoD / Western revenue ratio',
  ROUND(cod.cod / r.aug_revenue, 2)
FROM western_cod cod, western_revenue r""",
    "H07": f"""-- expected_output_reference / GT Queries H05-H07
WITH western_cancelled AS (
  SELECT ROUND(SUM(total_amount), 2) AS cancelled_revenue
  FROM {CATALOG}.demand_analysis.sales_orders
  WHERE region = 'Western'
    AND order_status = 'Cancelled'
    AND order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND order_date <  DATE_TRUNC('month', DATE '2026-09-01')
),
western_backordered AS (
  SELECT ROUND(SUM(total_amount), 2) AS backordered_revenue
  FROM {CATALOG}.demand_analysis.sales_orders
  WHERE region = 'Western'
    AND order_status = 'Backordered'
    AND order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND order_date <  DATE_TRUNC('month', DATE '2026-09-01')
),
western_freight AS (
  SELECT ROUND(SUM(freight_cost), 2) AS wasted_freight
  FROM {CATALOG}.logistics_operations.shipments
  WHERE destination_region = 'Western'
    AND is_late = true
    AND ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND ship_date <  DATE_TRUNC('month', DATE '2026-09-01')
),
western_stockouts AS (
  SELECT COUNT(DISTINCT sku_id) AS stockout_skus
  FROM {CATALOG}.inventory_management.inventory_ledger
  WHERE region = 'Western' AND stockout_flag = true AND below_safety_stock_flag = true
),
western_revenue AS (
  SELECT ROUND(SUM(total_amount), 2) AS aug_revenue
  FROM {CATALOG}.demand_analysis.sales_orders
  WHERE region = 'Western'
    AND order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND order_date <  DATE_TRUNC('month', DATE '2026-09-01')
),
western_cod AS (
  SELECT ROUND(total_cost_of_disruption, 2) AS cod
  FROM {CATALOG}.reporting.cost_of_disruption_by_region
  WHERE region = 'Western'
)
SELECT
  'H05' AS test_id,
  'Revenue at risk (cancelled + backordered + freight)' AS description,
  ROUND(c.cancelled_revenue + b.backordered_revenue + f.wasted_freight, 2) AS gt_value
FROM western_cancelled c, western_backordered b, western_freight f
UNION ALL
SELECT
  'H06',
  'Avg revenue at risk per stockout SKU (backordered / stockout SKUs)',
  ROUND(b.backordered_revenue / s.stockout_skus, 2)
FROM western_backordered b, western_stockouts s
UNION ALL
SELECT
  'H07',
  'CoD / Western revenue ratio',
  ROUND(cod.cod / r.aug_revenue, 2)
FROM western_cod cod, western_revenue r""",

    # UC Page-only values
    "G01": None,
    "G02": None,

    # UC Page-only threshold questions (Group P)
    "P01": f"""-- Logistics Risk Standards: delay_days >= 5 AND total_weight_kg > 800
SELECT COUNT(*) AS flagged_shipments
FROM {CATALOG}.logistics_operations.shipments
WHERE ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'
  AND delay_days >= 5 AND total_weight_kg > 800""",
    "P02": f"""-- Demand Anomaly Alert: quantity >= 8 AND unit_price < 30 AND channel = 'Online'
SELECT COUNT(*) AS anomaly_orders
FROM {CATALOG}.demand_analysis.sales_orders
WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'
  AND region = 'Western'
  AND quantity >= 8 AND unit_price < 30 AND channel = 'Online'""",
    "P03": f"""-- Inventory Standards: dos BETWEEN 1 AND 11, below_safety_stock, on_hand > 0
SELECT COUNT(*) AS supply_risk_positions
FROM {CATALOG}.inventory_management.inventory_ledger
WHERE days_of_supply BETWEEN 1 AND 11 AND below_safety_stock_flag = true AND on_hand_qty > 0""",
    "P04": f"""-- Procurement Quality Minimum: quality_score < 75 AND lead_time_variance_days > 12
SELECT COUNT(*) AS below_quality_minimum
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'
  AND quality_score < 75 AND lead_time_variance_days > 12""",
    "P05": f"""-- Executive Disruption Threshold: composite_risk_score < 55 AND lead_time_variance > 8 AND total_penalty_usd > 80000
SELECT COUNT(*) AS high_risk_suppliers
FROM {CATALOG}.reporting.supply_chain_risk_scorecard
WHERE composite_risk_score < 55 AND lead_time_variance > 8 AND total_penalty_usd > 80000""",
}

GT_QUERY_NOTES = {
    "G01": "No SQL query in expected_output_reference. GT value 92.0 (Q1 target) lives in fiscal_targets table and on UC Page 1 (Fiscal Calendar & Targets) on the Discover page.",
    "G02": "No SQL query in expected_output_reference. GT value 92.0 (Q1 target) lives in fiscal_targets table and on UC Page 1 (Fiscal Calendar & Targets) on the Discover page.",
}


def detect_provenance(sql, aid):
    """Detect which UC semantic feature the agent used based on its SQL.
    Returns (iteration_label, feature_name, explanation)."""
    if not sql:
        return ("N/A", "No SQL", "Agent did not generate SQL")
    sql_lower = sql.lower()
    # --- Iter 2: UC Metric Views ---
    mv_map = {
        'delivery_performance_by_region': {
            'iter': 'Iter 2',
            'measures': ['on_time_delivery_rate', 'late_delivery_rate', 'avg_delay_days',
                         'total_shipments', 'late_shipments', 'wasted_freight_cost'],
        },
        'revenue_comparison_by_region': {
            'iter': 'Iter 2',
            'measures': ['revenue_last_month', 'revenue_prior_month',
                         'revenue_change_dollars', 'revenue_change_pct'],
        },
        'inventory_safety_stock_metrics': {
            'iter': 'Iter 2',
            'measures': ['positions_below_safety_stock', 'unique_skus_below_safety',
                         'stockout_positions', 'unique_skus_in_stockout', 'avg_days_of_supply'],
        },
        'supplier_performance_by_continent': {
            'iter': 'Iter 2',
            'measures': ['total_purchase_orders', 'late_purchase_orders',
                         'supplier_late_rate_pct', 'avg_lead_time_variance'],
        },
    }
    for view, info in mv_map.items():
        if view in sql_lower:
            used = [m for m in info['measures'] if m in sql_lower]
            metric_str = ', '.join(used) if used else '(all measures)'
            return (info['iter'],
                    f"Metric View: {view} \u2192 {metric_str}",
                    f"Agent queried governed metric view, measure(s): {metric_str}")
    # --- Iter 2: Open Knowledge View (CoD) ---
    if 'cost_of_disruption_by_region' in sql_lower:
        return ('Iter 2', 'Open Knowledge View: cost_of_disruption_by_region',
                'Cross-domain CoD join (demand + logistics + supplier + inventory)')
    # --- Iter 3: Reference Table (fiscal_targets) ---
    if 'fiscal_targets' in sql_lower:
        return ('Iter 3', 'Reference Table: fiscal_targets',
                'Agent queries fiscal_targets table added to its data sources')
    # --- Iter 1: Column Comment guidance (correct table choice) ---
    if aid in ('D04', 'D06', 'H01', 'H02') and 'supplier_orders' in sql_lower and 'lead_time_variance' in sql_lower:
        return ('Iter 1', 'Column Comment: supplier_orders.lead_time_variance_days',
                'Comment steered agent from supplier_lead_times to supplier_orders')
    if aid in ('H03', 'H04', 'F03'):
        check = sql_lower.replace('partially_fulfilled', '~~')
        if "'fulfilled'" in check or '"fulfilled"' in check:
            return ('Iter 1', 'Column Comment: order_status disambiguation',
                    'Comment defined Fulfilled vs Partially_Fulfilled distinction')
    # --- Iter 1: Example SQL Queries ---
    if aid == 'F02' and 'supplier_orders' in sql_lower and ('count(*)' in sql_lower or 'sum(case' in sql_lower):
        return ('Iter 1', 'Example SQL: vendor late rate per-order',
                'Example SQL taught per-ORDER rate (not per-vendor COUNT DISTINCT)')
    if aid == 'F06' and 'product_family' in sql_lower:
        return ('Iter 1', 'Example SQL: product family revenue decline',
                'Example SQL demonstrated MoM product family comparison')
    # --- Iter 3: G01/G02 — fiscal_targets table (NOT UC Pages) ---
    if aid in ('G01', 'G02'):
        if '92' in sql:
            return ('Iter 3', 'Reference Table: fiscal_targets',
                    'Agent found 92% Q1 target in fiscal_targets table (not from UC Page)')
    # --- Iter 3: SQL Functions for domain-specific "critical" thresholds ---
    # IMPORTANT: Genie Agents CANNOT read UC Pages (proven — agent confirmed:
    # "I don't have direct access to those pages in this context").
    # Iter 3 creates SQL FUNCTIONS (get_critical_*) as the workaround.
    # If agent calls the function → SQL Function (Iter 3).
    # If agent just hardcodes the threshold → GUESSED (agent invented it).
    # --- Iter 3: SQL Functions — definition-only (agent reads definition, writes own SQL) ---
    # The function name won't appear in the output SQL. Instead, check if ALL
    # conditions from the function definition appear in the agent's query.
    # Each condition group = list of alternative markers (at least one must match).
    sql_func_map = {
        'P01': ('get_critical_delay_shipments', [
            ['delay_days'], ['total_weight_kg'],
            ['>= 5', '>=5'], ['> 800', '>800'],
        ]),
        'P02': ('get_critical_accuracy_forecasts', [
            ['quantity'], ['unit_price'], ['channel'],
            ['>= 8', '>=8'], ['< 30', '<30'],
        ]),
        'P03': ('get_critical_supply_positions', [
            ['days_of_supply'], ['below_safety_stock'],
            ['on_hand_qty', 'on_hand'],
            ['between 1 and 11'],
        ]),
        'P04': ('get_critical_quality_orders', [
            ['quality_score'], ['lead_time_variance'],
            ['< 75', '<75'], ['> 12', '>12'],
        ]),
        'P05': ('get_critical_disruption_regions', [
            ['composite_risk_score'], ['lead_time_variance'],
            ['total_penalty_usd'],
            ['< 55', '<55'], ['> 80000', '>80000'],
        ]),
    }
    if aid in sql_func_map:
        func_name, condition_groups = sql_func_map[aid]
        # Priority 1: Agent called the SQL function directly (unlikely for definition-only)
        if func_name in sql_lower:
            return ('Iter 3', f'SQL Function: {func_name}()',
                    f'Agent called the SQL function directly')
        # Priority 2: Check how many condition groups from the function definition match
        # Definition-only functions: agent reads definition then writes own SQL.
        # ALL groups matching = strong evidence of function usage (API hides internal reasoning).
        groups_matched = sum(
            1 for group in condition_groups
            if any(marker in sql_lower for marker in group)
        )
        total_groups = len(condition_groups)
        if groups_matched == total_groups:
            return ('Iter 3 (inferred)', f'SQL Function (inferred): all {total_groups} conditions match {func_name}',
                    f'Agent SQL matches ALL {total_groups} conditions from {func_name}() definition. '
                    f'API does not expose internal reasoning, but full multi-condition match '
                    f'is strong evidence the agent consulted the function.')
        elif groups_matched > 0:
            return ('GUESSED', f'Partial match ({groups_matched}/{total_groups} conditions vs {func_name})',
                    f'Only {groups_matched} of {total_groups} condition groups match — '
                    f'insufficient evidence of function usage.')
    # --- Fallback: Check for metric view MEASURE names in SQL ---
    # Agent may use metric view knowledge but generate SQL against base tables.
    # If measure names appear in the SQL, it's strong evidence of MV guidance.
    for view, info in mv_map.items():
        used = [m for m in info['measures'] if m in sql_lower]
        if used:
            metric_str = ', '.join(used)
            return (info['iter'] + ' (inferred)',
                    f"Metric View (inferred): {view} -> {metric_str}",
                    f"Agent SQL references measure(s) from {view}: {metric_str}")
    # --- Baseline: Raw table query ---
    return ('Raw Table', 'Direct query (no UC feature detected)',
            'Agent queried base tables directly — no UC semantic feature detected in SQL')


# Global: stores per-iteration test details for flip tracking
iteration_test_details = {}
# Global: stores full 45-test results per stage for graphing
all_stage_results = {}
all_comp_results = {}

test_results = []
for i, (aid, agent_key, question, expected, desc, claimed_fix) in enumerate(assumptions):
    if i > 0:
        time.sleep(2)  # Rate limit guard: avoid 429 RESOURCE_EXHAUSTED
    space_id = spaces.get(agent_key)
    if not space_id:
        print(f"\n\u26a0 {aid}: Agent '{agent_key}' not found")
        test_results.append({"id": aid, "verdict": "SKIP", "desc": desc, "claimed_fix": claimed_fix})
        continue
    print(f"\n{'='*90}")
    print(f"  {aid}: {desc}")
    print(f"  Agent: {agent_key} | Ground truth: {expected} | Claimed fix: {claimed_fix}")
    print(f"  Question: {question}")
    print(f"{'='*90}")
    msg = ask_genie(space_id, question)
    if "error" in msg:
        print(f"  \u274c ERROR: {msg['error']}")
        test_results.append({"id": aid, "verdict": "ERROR", "desc": desc, "claimed_fix": claimed_fix})
        continue
    sql, narration, rows, cols = extract_from_msg(msg)
    gt_sql = GT_QUERIES.get(aid)
    gt_note = GT_QUERY_NOTES.get(aid)
    print_boxed_block("AGENT SQL", sql)
    print_boxed_block("GROUND TRUTH SQL", gt_sql or gt_note)
    print(f"\n  \u250c\u2500 AGENT NARRATION \u2500\u2500\u2500")
    for line in (narration or "(empty)").split("\n"):
        print(f"  \u2502 {line}")
    print(f"  \u2514{'\u2500'*70}")
    if rows:
        print(f"\n  \u250c\u2500 QUERY RESULT ROWS ({len(rows)} rows, cols: {cols}) \u2500\u2500\u2500")
        for i, row in enumerate(rows[:5]):
            print(f"  \u2502 [{i}] {row}")
        if len(rows) > 5:
            print(f"  \u2502 ... ({len(rows)} total rows)")
        print(f"  \u2514{'\u2500'*70}")
    all_text = (narration or "") + " " + (sql or "")
    for row in rows:
        for val in row.values():
            all_text += f" {val}"
    match, found, closest = find_value_in_text(all_text, expected)
    if match:
        print(f"\n  \u2705 EXACT MATCH (2dp): ground_truth={expected}, found={found}")
        print(f"  >> VERDICT: PASS \u2014 agent gets this WITHOUT the claimed UC fix")
        verdict = "PASS"
    else:
        print(f"\n  \u274c NO EXACT MATCH: ground_truth={expected}, closest={closest}")
        print(f"  >> VERDICT: FAIL \u2014 agent NEEDS the fix: '{claimed_fix}'")
        verdict = "FAIL"
    prov_iter, prov_feature, prov_expl = detect_provenance(sql, aid)
    test_results.append({
        "id": aid, "verdict": verdict, "desc": desc, "claimed_fix": claimed_fix,
        "expected": expected, "found": found, "closest": closest,
        "provenance": prov_feature, "prov_iter": prov_iter, "prov_expl": prov_expl, "sql": sql,
        "narration": narration,
    })

# ============================================================
# SUMMARY TABLE \u2014 grouped by metric view
# ============================================================
print(f"\n\n{'='*100}")
print("  CURATOR BENCHMARK SUMMARY \u2014 v3 COMPREHENSIVE")
print(f"{'='*100}")
print(f"  {'ID':<5} {'Result':<12} {'GT':>14} {'Found':>14}  {'Description':<45} {'Expected Fix':<25} {'Agent Used'}")
print(f"  {'\u2500'*5} {'\u2500'*12} {'\u2500'*14} {'\u2500'*14}  {'\u2500'*45} {'\u2500'*25} {'\u2500'*35}")

current_group = ""
disproved = confirmed = errors = 0
for r in test_results:
    gid = r['id'][:1]
    if gid != current_group:
        current_group = gid
        group_names = {'A': 'LOGISTICS MV', 'B': 'DEMAND MV', 'C': 'INVENTORY MV',
                       'D': 'SUPPLIER MV', 'E': 'CROSS-DOMAIN / EXEC', 'F': 'INDIRECT GTs & AMBIGUITY',
                       'G': 'Q1 FISCAL CALENDAR (UC PAGES)',
                       'H': 'HARD FAILURES (GUARANTEED BASELINE MISSES)',
                       'P': 'SQL FUNCTIONS (DOMAIN CRITICAL THRESHOLDS)'}
        print(f"\n  \u2500\u2500 {group_names.get(gid, gid)} {'\u2500'*70}")
    icon = {"PASS": "\u2705", "FAIL": "\u274c", "ERROR": "\u26a0\ufe0f", "SKIP": "\u23ed"}.get(r["verdict"], "?")
    gt = f"{r.get('expected', ''):>14}" if r.get('expected') is not None else f"{'N/A':>14}"
    fd_val = r.get('found') if r.get('found') is not None else r.get('closest')
    fd = f"{fd_val:>14}" if fd_val is not None else f"{'N/A':>14}"
    prov_short = f"[{r.get('prov_iter','?')}] {(r.get('provenance','') or '')[:80]}" if r.get('prov_iter') else ""
    print(f"  {r['id']:<5} {icon} {r['verdict']:<10} {gt} {fd}  {r['desc']:<45} {r.get('claimed_fix', ''):<25} {prov_short}")
    if r["verdict"] == "PASS": disproved += 1
    elif r["verdict"] == "FAIL": confirmed += 1
    else: errors += 1

total = disproved + confirmed + errors
print(f"\n{'='*100}")
print(f"  TOTALS: {total} tests | {disproved} PASS ({100*disproved//max(total,1)}%) | "
      f"{confirmed} FAIL ({100*confirmed//max(total,1)}%) | {errors} errors/skips")
print(f"{'='*100}")
if disproved > 0:
    print(f"  \u26a1 {disproved} metrics work at BASELINE \u2014 agent understands them without UC features")
if confirmed > 0:
    print(f"  FIX {confirmed} metrics NEED UC features \u2014 these are the real gaps the iterations must fix")
if errors > 0:
    print(f"  \u26a0\ufe0f  {errors} tests failed to run \u2014 check agent availability")

print(f"\n  METRIC VIEW COVERAGE:")
for prefix, name, count in [('A', 'Logistics (delivery_performance_by_region)', 6),
                             ('B', 'Demand (revenue_comparison_by_region)', 4),
                             ('C', 'Inventory (inventory_safety_stock_metrics)', 5),
                             ('D', 'Supplier (supplier_performance_by_continent)', 7),
                             ('H', 'Hard Failures (wrong table / status / cross-domain)', 7),
                             ('G', 'Q1 Fiscal Calendar (UC Pages only)', 2),
                             ('P', 'SQL Functions (domain-specific critical thresholds)', 5)]:
    hits = sum(1 for r in test_results if r['id'].startswith(prefix) and r['verdict'] == 'PASS')
    total_g = sum(1 for r in test_results if r['id'].startswith(prefix))
    print(f"    {name}: {hits}/{total_g} work at baseline")

# ============================================================
# DRY: Reusable test function for confirmed-failing tests
# Built DYNAMICALLY from test_results — not hardcoded.
# Called after each iteration to measure progressive improvement.
# ============================================================
confirmed_ids = {r["id"] for r in test_results if r["verdict"] == "FAIL"}
FAILING_TESTS = []
for aid, agent_key, question, expected, desc, claimed_fix in assumptions:
    if aid in confirmed_ids:
        FAILING_TESTS.append((aid, agent_key, question, expected))

# Store baseline results globally
all_stage_results["Baseline"] = test_results

print(f"\n  {len(FAILING_TESTS)} tests FAIL at baseline — these are the iteration targets:")
for aid, agent_key, question, expected in FAILING_TESTS:
    print(f"    {aid}: {agent_key} \u2192 gt={expected}")

def test_all_metrics(label):
    """Run ALL 45 tests and store results globally for graphing.
    Includes 2s delay between API calls to avoid 429 RESOURCE_EXHAUSTED.
    Returns (passed, failed, errors, results_list)."""
    print(f"\n{'='*90}")
    print(f"  FULL 45-TEST SUITE: {label}")
    print(f"{'='*90}")
    results = []
    for i, (aid, agent_key, question, expected, desc, claimed_fix) in enumerate(assumptions):
        if i > 0:
            time.sleep(2)  # Rate limit guard: avoid 429 RESOURCE_EXHAUSTED
        space_id = spaces.get(agent_key)
        if not space_id:
            results.append({"id": aid, "verdict": "SKIP", "desc": desc, "claimed_fix": claimed_fix,
                            "expected": expected, "found": None, "closest": None,
                            "provenance": "N/A", "prov_iter": "N/A", "sql": None})
            continue
        print(f"\n  {'\u2500'*90}")
        print(f"  {aid}: {desc}")
        print(f"  Agent: {agent_key} | GT: {expected} | Claimed Fix: {claimed_fix}")
        print(f"  Q: {question}")
        msg = ask_genie(space_id, question)
        if "error" in msg:
            print(f"  \u274c ERROR: {msg['error']}")
            results.append({"id": aid, "verdict": "ERROR", "desc": desc, "claimed_fix": claimed_fix,
                            "expected": expected, "found": None, "closest": None,
                            "provenance": "N/A", "prov_iter": "N/A", "sql": None})
            continue
        sql, narration, rows, cols = extract_from_msg(msg)
        # Show agent SQL (full — no truncation)
        if sql:
            print(f"  \u250c\u2500 AGENT SQL \u2500\u2500\u2500")
            for line in sql.strip().split("\n"):
                print(f"  \u2502 {line}")
            print(f"  \u2514{'\u2500'*70}")
        else:
            print(f"  \u250c\u2500 AGENT SQL: (none \u2014 answered from narration only) \u2500\u2500\u2500")
        # Show GT reference SQL if available
        gt_sql = GT_QUERIES.get(aid)
        gt_note = GT_QUERY_NOTES.get(aid)
        if gt_sql:
            print(f"  \u250c\u2500 GT REFERENCE SQL \u2500\u2500\u2500")
            for line in gt_sql.strip().split("\n"):
                print(f"  \u2502 {line}")
            print(f"  \u2514{'\u2500'*70}")
        elif gt_note:
            print(f"  GT Note: {gt_note}")
        # Show full narration (no truncation)
        if narration:
            print(f"  \u250c\u2500 NARRATION \u2500\u2500\u2500")
            for line in narration.split("\n"):
                print(f"  \u2502 {line}")
            print(f"  \u2514{'\u2500'*70}")
        # Show result rows
        if rows:
            print(f"  \u250c\u2500 QUERY RESULT ROWS ({len(rows)} rows, cols: {', '.join(cols)}) \u2500\u2500\u2500")
            for i, row in enumerate(rows[:10]):
                print(f"  \u2502 [{i}] {row}")
            if len(rows) > 10:
                print(f"  \u2502 ... ({len(rows)} total rows)")
            print(f"  \u2514{'\u2500'*70}")
        # Value match
        all_text = (narration or "") + " " + (sql or "")
        for row in rows:
            for val in row.values(): all_text += f" {val}"
        match, found, closest = find_value_in_text(all_text, expected)
        prov_iter, prov_feature, prov_expl = detect_provenance(sql, aid)
        verdict = "PASS" if match else "FAIL"
        icon = "\u2705" if match else "\u274c"
        val_str = f"found={found}" if found is not None else f"closest={closest}"
        # Reasoning confidence (derived dynamically from provenance tier)
        if prov_iter in ('Iter 2', 'Iter 3') or prov_iter.startswith('Iter 3'): conf = 'DETERMINISTIC'
        elif prov_iter == 'Iter 1': conf = 'HEURISTIC'
        elif 'Guessed' in str(prov_iter) or prov_iter == 'GUESSED': conf = 'GUESSED'
        elif prov_iter == 'N/A': conf = 'N/A'
        else: conf = 'INFERRED'
        # \u2500\u2500 PROVENANCE & EVALUATOR ASSESSMENT \u2500\u2500
        conf_desc = {
            'DETERMINISTIC': 'Grounded in governed UC asset \u2014 answer is repeatable across runs',
            'HEURISTIC': 'Guided by metadata (comments/examples) \u2014 likely repeatable',
            'INFERRED': 'Agent inferred from raw column/table names \u2014 may vary across runs',
            'GUESSED': 'Agent invented thresholds \u2014 unreliable without UC feature',
            'N/A': 'No SQL generated \u2014 cannot assess',
        }.get(conf, 'Unknown')
        print(f"\n  \u250c\u2500 PROVENANCE & EVALUATOR ASSESSMENT \u2500\u2500\u2500")
        print(f"  \u2502 Verdict:      {icon} {verdict} (gt={expected}, {val_str})")
        print(f"  \u2502 UC Feature:   [{prov_iter}] {prov_feature}")
        print(f"  \u2502 Provenance:   {prov_expl}")
        print(f"  \u2502 Confidence:   {conf}")
        print(f"  \u2502 Assessment:   {conf_desc}")
        print(f"  \u2502 Claimed Fix:  {claimed_fix}")
        print(f"  \u2514{'\u2500'*70}")
        results.append({"id": aid, "verdict": verdict, "desc": desc, "claimed_fix": claimed_fix,
                        "expected": expected, "found": found, "closest": closest,
                        "provenance": prov_feature, "prov_iter": prov_iter, "sql": sql,
                        "narration": narration})
    passed = sum(1 for r in results if r["verdict"] == "PASS")
    failed = sum(1 for r in results if r["verdict"] == "FAIL")
    errs = sum(1 for r in results if r["verdict"] in ("ERROR", "SKIP"))
    print(f"\n{'='*90}")
    print(f"  {label}: {passed}/{len(results)} PASS ({100*passed//max(len(results),1)}%) | {failed} FAIL | {errs} errors")
    print(f"{'='*90}")
    # List FAILED and ERROR tests so the audience sees exactly what needs fixing
    fail_list = [r for r in results if r['verdict'] == 'FAIL']
    err_list = [r for r in results if r['verdict'] in ('ERROR', 'SKIP')]
    if fail_list:
        print(f"\n  \u274c FAILED ({len(fail_list)}):")
        for r in fail_list:
            closest_str = f"closest={r.get('closest')}" if r.get('closest') is not None else "no match"
            print(f"    {r['id']:<5} gt={r['expected']:<14} {closest_str:<24}")
            print(f"           Expected fix: {r.get('claimed_fix','?')}")
            print(f"           Agent used:   [{r.get('prov_iter','?')}] {r.get('provenance','')}")
            if r.get('prov_expl'):
                print(f"           Why:          {r.get('prov_expl','')}")
    if err_list:
        print(f"\n  \u26a0\ufe0f  ERRORS ({len(err_list)}):")
        for r in err_list:
            print(f"    {r['id']:<5} {r['verdict']}")
    if not fail_list and not err_list:
        print(f"\n  ALL {passed} TESTS PASS!")
    all_stage_results[label] = results
    return passed, failed, errs, results


def test_failing_metrics(label=""):
    """Run targeted test on previously-failing metrics.
    Shows full agent response (SQL, narration, result rows) for transparency.
    Returns (passed, failed, errors, details)."""
    print(f"\n{'='*90}")
    print(f"  TARGETED TEST: {len(FAILING_TESTS)} previously-failing metrics{f' \u2014 {label}' if label else ''}")
    print(f"{'='*90}")
    results = []
    for i, (aid, agent_key, question, expected) in enumerate(FAILING_TESTS):
        if i > 0:
            time.sleep(2)  # Rate limit guard: avoid 429 RESOURCE_EXHAUSTED
        space_id = spaces.get(agent_key)
        print(f"\n  {'\u2500'*86}")
        print(f"  {aid}: asking {agent_key}... (gt={expected})")
        print(f"  Q: {question}")
        msg = ask_genie(space_id, question)
        if "error" in msg:
            print(f"  \u274c ERROR: {msg['error']}")
            results.append((aid, "ERROR", expected, None, None, None, "N/A", "N/A"))
            continue
        sql, narration, rows, cols = extract_from_msg(msg)
        gt_sql = GT_QUERIES.get(aid)
        gt_note = GT_QUERY_NOTES.get(aid)
        # Show agent response for full transparency
        print_boxed_block("AGENT SQL", sql)
        print_boxed_block("GROUND TRUTH SQL", gt_sql or gt_note)
        if narration:
            print(f"  \u250c\u2500 NARRATION \u2500\u2500\u2500")
            for line in narration.split("\n"):
                print(f"  \u2502 {line}")
            print(f"  \u2514{'\u2500'*70}")
        if rows:
            print(f"  \u250c\u2500 RESULT ({len(rows)} rows) \u2500\u2500\u2500")
            for i, row in enumerate(rows[:10]):
                print(f"  \u2502 [{i}] {row}")
            if len(rows) > 10:
                print(f"  \u2502 ... ({len(rows)} total)")
            print(f"  \u2514{'\u2500'*70}")
        all_text = (narration or "") + " " + (sql or "")
        for row in rows:
            for val in row.values():
                all_text += f" {val}"
        match, found, closest = find_value_in_text(all_text, expected)
        prov_iter, prov_feature, prov_expl = detect_provenance(sql, aid)
        if match:
            print(f"\n  \u2705 PASS (gt={expected}, found={found})")
            print(f"  \u25b8 Used: [{prov_iter}] {prov_feature}")
            results.append((aid, "PASS", expected, found, None, sql, prov_feature, prov_iter))
        else:
            print(f"\n  \u274c FAIL (gt={expected}, closest={closest})")
            print(f"  \u25b8 Used: [{prov_iter}] {prov_feature}")
            results.append((aid, "FAIL", expected, None, closest, sql, prov_feature, prov_iter))
    passed = sum(1 for r in results if r[1] == "PASS")
    failed = sum(1 for r in results if r[1] == "FAIL")
    errs = sum(1 for r in results if r[1] == "ERROR")
    print(f"\n{'='*90}")
    n = len(FAILING_TESTS)
    print(f"  RESULTS: {passed}/{n} PASS | {failed} FAIL | {errs} ERROR")
    for r in results:
        aid, verdict = r[0], r[1]
        icon = {"PASS": "\u2705", "FAIL": "\u274c", "ERROR": "\u26a0\ufe0f"}[verdict]
        val = r[3] if r[3] is not None else r[4]
        prov_str = f"  [{r[7]}] {r[6]}" if verdict == "PASS" and len(r) > 6 else ""
        print(f"  {icon} {aid:<5} gt={r[2]:<14} {'found='+str(round(val,2)) if val else 'N/A':<24} {verdict}{prov_str}")
    # Store for flip tracking across iterations
    iteration_test_details[label] = results
    # Detect flips from previous iteration
    prev_labels = [k for k in iteration_test_details.keys() if k != label]
    if prev_labels:
        prev_results = iteration_test_details[prev_labels[-1]]
        prev_pass_ids = {r[0] for r in prev_results if r[1] == "PASS"}
        curr_pass_ids = {r[0] for r in results if r[1] == "PASS"}
        new_fixes = curr_pass_ids - prev_pass_ids
        regressions = prev_pass_ids - curr_pass_ids
        if new_fixes:
            print(f"\n  \u2b06 NEWLY FIXED in this iteration ({len(new_fixes)}):")
            for r in results:
                if r[0] in new_fixes and len(r) > 6:
                    print(f"    \u2705 {r[0]}: [{r[7]}] {r[6]}")
        if regressions:
            print(f"\n  \u2b07 REGRESSIONS ({len(regressions)}):")
            for aid in sorted(regressions):
                print(f"    \u274c {aid}: was PASS, now FAIL (non-determinism)")
    print(f"{'='*90}")
    return passed, failed, errs, results

# --- Comprehensive Prompt Benchmark: Baseline ---
comp_baseline = run_comprehensive_benchmark("Baseline (before any UC semantic layer features enhancement)")
all_comp_results["Baseline"] = comp_baseline

# COMMAND ----------

# DBTITLE 1,VISUAL: Test Results Dashboard + Provenance Analysis
# ============================================================
# VISUAL DASHBOARD: Test Results Across Stages
# Reads from all_stage_results dict populated by baseline + iterations.
# Re-run this cell after any stage to see updated charts.
# ============================================================
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

def plot_test_dashboard(stage_results_dict, comp_results_dict=None):
    """Generate a 2-panel dashboard: heatmap + progression bar chart."""
    stages = list(stage_results_dict.keys())
    if not stages:
        print("No results yet.")
        return
    # Build test ID list from first stage
    test_ids = [r["id"] for r in stage_results_dict[stages[0]]]
    groups = ['A','B','C','D','E','F','H','G','P']
    group_names = {'A':'Logistics','B':'Demand','C':'Inventory','D':'Supplier',
                   'E':'Cross-Domain','F':'Indirect','H':'Hard','G':'Fiscal','P':'Critical'}

    # Build matrix: 1=PASS, 0=FAIL, -1=ERROR
    matrix = []
    for stage in stages:
        row = []
        result_map = {r["id"]: r["verdict"] for r in stage_results_dict[stage]}
        for tid in test_ids:
            v = result_map.get(tid, "SKIP")
            row.append(1 if v == "PASS" else (0 if v == "FAIL" else -1))
        matrix.append(row)
    matrix = np.array(matrix)

    fig, axes = plt.subplots(1, 2, figsize=(18, max(4, len(stages)*1.2)),
                             gridspec_kw={'width_ratios': [3, 1]})

    # Panel 1: Heatmap
    ax1 = axes[0]
    from matplotlib.colors import ListedColormap
    cmap = ListedColormap(['#e8e8e8', '#d32f2f', '#2e7d32'])  # ERROR, FAIL, PASS
    ax1.imshow(matrix, aspect='auto', cmap=cmap, vmin=-1, vmax=1, interpolation='nearest')
    ax1.set_yticks(range(len(stages)))
    ax1.set_yticklabels(stages, fontsize=10)
    ax1.set_xticks(range(len(test_ids)))
    ax1.set_xticklabels(test_ids, fontsize=6, rotation=90)
    # Group separators
    prev_g = ''
    for i, tid in enumerate(test_ids):
        g = tid[0]
        if g != prev_g and i > 0:
            ax1.axvline(i - 0.5, color='white', linewidth=2)
        prev_g = g
    ax1.set_title('Per-Test PASS/FAIL Heatmap', fontsize=12, fontweight='bold')
    legend_patches = [mpatches.Patch(color='#2e7d32', label='PASS'),
                      mpatches.Patch(color='#d32f2f', label='FAIL')]
    ax1.legend(handles=legend_patches, loc='upper right', fontsize=8)

    # Panel 2: Bar chart — PASS count per stage
    ax2 = axes[1]
    pass_counts = [sum(1 for r in stage_results_dict[s] if r["verdict"]=="PASS") for s in stages]
    total = len(test_ids)
    colors = ['#1565c0' if i == 0 else '#0d47a1' for i in range(len(stages))]  # Blue baseline, darker blue iterations
    bars = ax2.barh(range(len(stages)), pass_counts, color=colors, edgecolor='white')
    ax2.set_yticks(range(len(stages)))
    ax2.set_yticklabels(stages, fontsize=10)
    ax2.set_xlim(0, total + 2)
    ax2.axvline(total, color='gray', linestyle='--', alpha=0.5)
    for i, (bar, count) in enumerate(zip(bars, pass_counts)):
        pct = 100 * count // total
        ax2.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                 f'{count}/{total} ({pct}%)', va='center', fontsize=10, fontweight='bold')
    ax2.set_title('PASS Count Progression', fontsize=12, fontweight='bold')
    ax2.set_xlabel(f'Tests (of {total})')
    ax2.invert_yaxis()

    plt.tight_layout()
    plt.show()

# Plot whatever stages are available so far
plot_test_dashboard(all_stage_results, all_comp_results if 'all_comp_results' in dir() else None)

# ── Provenance Analysis: Extracted from EXISTING test results ──
# Each test result already contains: provenance, prov_iter, sql
# detect_provenance() was called during the test run (cell 7).
# LLM_REASONING dict provides pre-coded semantic reasoning.
# NO probes, NO instruction patching — just analysis of captured data.

current_stage = list(all_stage_results.keys())[-1] if all_stage_results else "Unknown"
results = all_stage_results.get(current_stage, [])

if results:
    # ── Determine which iterations ACTUALLY ran ──
    # detect_provenance classifies by SQL pattern, not by which iteration was applied.
    # If we're at baseline, an "Iter 1" label means the agent got LUCKY — it wrote SQL
    # matching the Iter 1 pattern without any UC features being applied yet.
    ran_stages = set(all_stage_results.keys())
    iter_actually_ran = {
        'Iter 1': any('iteration 1' in s.lower() or 'iter 1' in s.lower() or 'iter1' in s.lower() for s in ran_stages),
        'Iter 2': any('iteration 2' in s.lower() or 'iter 2' in s.lower() or 'iter2' in s.lower() for s in ran_stages),
        'Iter 3': any('iteration 3' in s.lower() or 'iter 3' in s.lower() or 'iter3' in s.lower() for s in ran_stages),
    }

    def relabel_tier(raw_tier):
        """Relabel tier: distinguish WHAT was guessed.
        'Guessed (Iter N)' = agent happened to write SQL matching an Iter N pattern.
        'Guessed (threshold)' = agent invented a threshold value."""
        if raw_tier == 'GUESSED':
            return 'Guessed (threshold)'
        if raw_tier in iter_actually_ran and not iter_actually_ran[raw_tier]:
            return f'Guessed ({raw_tier})'  # e.g., 'Guessed (Iter 1)'
        return raw_tier

    def _confidence_from_tier(tier):
        """Map provenance tier to reasoning confidence level.
        DETERMINISTIC = governed asset (metric view, SQL function, ref table) — repeatable.
        HEURISTIC = guided by UC comments/examples — likely repeatable.
        INFERRED = raw table query — agent figured it out alone, may vary.
        GUESSED = agent invented the answer — non-deterministic."""
        if tier in ('Iter 2', 'Iter 3') or tier.startswith('Iter 3'):
            return 'DETERMINISTIC'
        if tier == 'Iter 1':
            return 'HEURISTIC'
        if tier.startswith('Guessed'):
            return 'GUESSED'
        if tier == 'N/A':
            return 'N/A'
        return 'INFERRED'

    print("\n" + "="*90)
    print(f"  PROVENANCE ANALYSIS: {current_stage}")
    print(f"  Extracted from test results — no additional API calls")
    print("="*90)

    # Per-test provenance table — NO truncation
    current_group = ""
    group_names = {'A':'LOGISTICS','B':'DEMAND','C':'INVENTORY','D':'SUPPLIER',
                   'E':'CROSS-DOMAIN','F':'INDIRECT','H':'HARD','G':'FISCAL','P':'CRITICAL'}
    for r in results:
        gid = r['id'][:1]
        if gid != current_group:
            current_group = gid
            print(f"\n  ── {group_names.get(gid, gid)} {'─'*80}")
        icon = '✅' if r['verdict'] == 'PASS' else ('❌' if r['verdict'] == 'FAIL' else '⚠️')
        prov = r.get('provenance', 'Unknown')
        tier = relabel_tier(r.get('prov_iter', '?'))
        conf = _confidence_from_tier(tier)
        gt = r.get('expected', '?')
        found = r.get('found') if r.get('found') is not None else r.get('closest')
        found_str = f"{found}" if found is not None else "—"
        print(f"  {icon} {r['id']:<5} {r['verdict']:<5}  gt={gt:<14}  found={found_str:<14}  [{tier}] {prov}")
        print(f"         Confidence: {conf}")

    # Summary by provenance tier (with corrected labels)
    prov_tiers = {}
    for r in results:
        tier = relabel_tier(r.get('prov_iter', 'Unknown'))
        prov_tiers.setdefault(tier, []).append(r)

    print(f"\n{'='*90}")
    print(f"  PROVENANCE SUMMARY BY TIER")
    print(f"{'='*90}")
    tier_order = ['Raw Table', 'Guessed (Iter 1)', 'Guessed (Iter 2)', 'Guessed (Iter 3)', 'Guessed (threshold)', 'Iter 1', 'Iter 2', 'Iter 3', 'Iter 3 (inferred)', 'N/A']
    for tier in tier_order:
        if tier not in prov_tiers:
            continue
        tests = prov_tiers[tier]
        p = sum(1 for t in tests if t['verdict'] == 'PASS')
        f = sum(1 for t in tests if t['verdict'] == 'FAIL')
        pct = 100 * p // max(len(tests), 1)
        ids = ', '.join(t['id'] for t in tests)
        print(f"  {tier:42s}: {len(tests):2d} tests -> {p:2d} PASS ({pct}%), {f:2d} FAIL  [{ids}]")

    # Dynamic reasoning confidence — derived from provenance tiers, not pre-coded
    conf_order = ['DETERMINISTIC', 'HEURISTIC', 'INFERRED', 'GUESSED', 'N/A']
    cat_counts = {}
    for r in results:
        tier = relabel_tier(r.get('prov_iter', 'Unknown'))
        conf = _confidence_from_tier(tier)
        cat_counts.setdefault(conf, {'pass': 0, 'fail': 0})
        if r['verdict'] == 'PASS': cat_counts[conf]['pass'] += 1
        else: cat_counts[conf]['fail'] += 1
    print(f"\n  REASONING CONFIDENCE (derived from provenance):")
    for conf in conf_order:
        if conf in cat_counts:
            c = cat_counts[conf]
            total_c = c['pass'] + c['fail']
            print(f"    {conf:20s}: {total_c:2d} tests ({c['pass']} PASS, {c['fail']} FAIL)")

    # ════════════════════════════════════════════════════════════════════════
    #  VISUAL: Provenance Tier + LLM Reasoning charts (2 panels)
    # ════════════════════════════════════════════════════════════════════════
    fig, (ax_prov, ax_llm) = plt.subplots(1, 2, figsize=(18, 5))

    # ── Panel 1: Provenance Tier (stacked horizontal bars) ──
    # ── Panel 1: Per-Domain Group PASS/FAIL ──
    group_order = ['A','B','C','D','E','F','H','G','P']
    grp_name_map = {'A':'Logistics (A)','B':'Demand (B)','C':'Inventory (C)','D':'Supplier (D)',
                     'E':'Cross-Domain (E)','F':'Indirect (F)','H':'Hard (H)','G':'Fiscal (G)','P':'Critical (P)'}
    grp_pass, grp_fail, grp_labels = [], [], []
    for g in group_order:
        tests = [r for r in results if r['id'].startswith(g)]
        if not tests: continue
        p = sum(1 for t in tests if t['verdict'] == 'PASS')
        f = len(tests) - p
        grp_labels.append(grp_name_map.get(g, g))
        grp_pass.append(p)
        grp_fail.append(f)
    y_pos = range(len(grp_labels))
    ax_prov.barh(y_pos, grp_pass, color='#2e7d32', edgecolor='white', label='PASS')
    ax_prov.barh(y_pos, grp_fail, left=grp_pass, color='#d32f2f', edgecolor='white', label='FAIL')
    ax_prov.set_yticks(y_pos)
    ax_prov.set_yticklabels(grp_labels, fontsize=10)
    ax_prov.set_xlabel('Test Count')
    ax_prov.set_title(f'PASS/FAIL by Domain Group ({current_stage})', fontsize=11, fontweight='bold')
    ax_prov.legend(loc='lower right', fontsize=9)
    ax_prov.invert_yaxis()
    for i, (p, f) in enumerate(zip(grp_pass, grp_fail)):
        total = p + f
        ax_prov.text(total+0.3, i, f'{total} ({p}P/{f}F)', va='center', fontsize=9, fontweight='bold')
    ax_prov.set_xlim(0, max(p+f for p, f in zip(grp_pass, grp_fail)) + 6)

    # ── Panel 2: LLM Reasoning Distribution (horizontal bars, color by reliability) ──
    cat_labels, cat_pass, cat_fail = [], [], []
    cat_colors = {
        'DETERMINISTIC': '#1b5e20', # Governed asset — answer is repeatable
        'HEURISTIC': '#689f38',     # Guided by UC comments/examples
        'INFERRED': '#f9a825',      # Raw table — agent figured it out alone
        'GUESSED': '#b71c1c',       # Agent invented the answer
        'N/A': '#757575',           # No SQL generated
    }
    for cat in conf_order:
        if cat not in cat_counts:
            continue
        c = cat_counts[cat]
        cat_labels.append(cat)
        cat_pass.append(c['pass'])
        cat_fail.append(c['fail'])

    y_pos2 = range(len(cat_labels))
    bar_colors = [cat_colors.get(c, '#757575') for c in cat_labels]
    bars_total = [p + f for p, f in zip(cat_pass, cat_fail)]
    ax_llm.barh(y_pos2, cat_pass, color=bar_colors, edgecolor='white', alpha=0.9, label='PASS')
    ax_llm.barh(y_pos2, cat_fail, left=cat_pass, color=bar_colors, edgecolor='white', alpha=0.3, label='FAIL')
    ax_llm.set_yticks(y_pos2)
    ax_llm.set_yticklabels(cat_labels, fontsize=10)
    ax_llm.set_xlabel('Test Count')
    ax_llm.set_title(f'Reasoning Confidence: Is This Answer Repeatable? ({current_stage})', fontsize=11, fontweight='bold')
    ax_llm.invert_yaxis()
    # Annotate
    for i, (p, f) in enumerate(zip(cat_pass, cat_fail)):
        total_c = p + f
        suffix = '' if f == 0 else f' ({f} FAIL)'
        ax_llm.text(total_c + 0.3, i, f'{total_c}{suffix}', va='center', fontsize=9, fontweight='bold')
    ax_llm.set_xlim(0, max(bars_total) + 5)

    # Reliability legend
    from matplotlib.patches import Patch
    reliability_patches = [
        Patch(facecolor='#1b5e20', label='DETERMINISTIC: governed asset (repeatable)'),
        Patch(facecolor='#689f38', label='HEURISTIC: guided by comments/examples'),
        Patch(facecolor='#f9a825', label='INFERRED: raw table (may vary)'),
        Patch(facecolor='#b71c1c', label='GUESSED: agent invented it'),
    ]
    ax_llm.legend(handles=reliability_patches, loc='lower right', fontsize=8)

    plt.tight_layout()
    plt.show()

# Classification dashboard (uses detect_provenance + LLM_REASONING, no probes needed)
if 'assumptions' in dir():
    plot_classification_dashboard(all_stage_results, assumptions)

# COMMAND ----------

# DBTITLE 1,Iteration 1 Approach: Column Comments + Example SQL Queries + Benchmarks
# MAGIC %md
# MAGIC ## Iteration 1: Column Comments + Example SQL Queries + Benchmarks
# MAGIC
# MAGIC **UC Features Used:** Column/Table Comments (`ALTER COLUMN COMMENT`, `SET TBLPROPERTIES`), Example SQL Queries (Genie Examples tab via `example_question_sqls` API), Benchmark Questions (Genie Benchmarks tab via `benchmarks` API)
# MAGIC
# MAGIC **Goal:** Fix failures caused by table ambiguity and status definition confusion — no new objects, just better metadata + proper Genie teaching patterns. Also stabilize non-deterministic passes by steering agents to governed assets.
# MAGIC
# MAGIC ### What This Fixes
# MAGIC
# MAGIC | Failure | Root Cause | Fix |
# MAGIC | --- | --- | --- |
# MAGIC | D04, D06, H01, H02 | Agent uses `supplier_lead_times` (pre-aggregated monthly) instead of `supplier_orders` (per-order granularity) for lead time variance | Column comment on `supplier_orders.lead_time_variance_days` + warning on `supplier_lead_times` table + Example SQL queries |
# MAGIC | F02 | Agent interprets "% vendors late" as per-distinct-vendor (83.33%) instead of per-order (75%) | Column comment on `supplier_orders.is_late` + Example SQL query with per-order formula |
# MAGIC | F03, H03 | Agent counts `Partially_Fulfilled` as fulfilled | Column comment on `sales_orders.order_status` defining each status value precisely + Example SQL queries |
# MAGIC | A02, A03 | Agent intermittently uses `actual_delivery_date` or `CURRENT_DATE()` instead of `ship_date` | Column comments on `ship_date` (primary), `actual_delivery_date` (warning), `destination_region` (exact values) + strengthened Example SQL usage guidance |
# MAGIC | A04 | Agent uses 2024 instead of 2026 for "August" | Temporal context from certified patterns + instruction emphasis |
# MAGIC
# MAGIC ### Column Comments Added
# MAGIC
# MAGIC * **`supplier_orders.lead_time_variance_days`** — "For ANY lead time variance question, always compute from THIS table at per-purchase-order granularity. Do NOT use `supplier_lead_times`."
# MAGIC * **`supplier_lead_times` (table-level)** — "WARNING: Pre-aggregated monthly summary. Do NOT use for lead time variance calculations."
# MAGIC * **`sales_orders.order_status`** — Defines exact values: `Fulfilled` (100% shipped, only this counts), `Partially_Fulfilled` (NOT fulfilled), `Backordered`, `Cancelled`
# MAGIC * **`supplier_orders.is_late`** — "Vendor late % = COUNT(is_late=true) / COUNT(*) computed per ORDER, not per vendor"
# MAGIC * **`sales_orders.total_amount`** — "This is the revenue column — SUM(total_amount) gives total revenue"
# MAGIC * **`shipments.ship_date`** — "PRIMARY date column for ALL monthly analyses. ALWAYS use ship_date, NEVER actual_delivery_date or CURRENT_DATE()"
# MAGIC * **`shipments.actual_delivery_date`** — "WARNING: Do NOT use for monthly analysis. Use ship_date instead"
# MAGIC * **`shipments.destination_region`** — "Valid values: Western, Eastern, Central, Southern. Use exact match, not ILIKE"
# MAGIC
# MAGIC ### Example SQL Queries (via Genie API → Examples tab)
# MAGIC
# MAGIC Instead of embedding SQL in agent instructions, we use the **proper API mechanism**: `example_question_sqls` in `serialized_space.instructions`. These appear in the Genie Agent "Examples" tab and directly teach the LLM the correct SQL patterns.
# MAGIC
# MAGIC **Supplier Agent (3 examples):**
# MAGIC 1. Lead time variance (overall) → `AVG(lead_time_variance_days) FROM supplier_orders`
# MAGIC 2. Lead time variance (by continent) → same + `WHERE supplier_continent = 'Asia'`
# MAGIC 3. Vendor late % → `SUM(CASE WHEN is_late...) / COUNT(*)` (per-order, not per-vendor)
# MAGIC
# MAGIC **Demand Agent (3 examples):**
# MAGIC 1. Fulfilled count → `WHERE order_status = 'Fulfilled'` only
# MAGIC 2. Fulfillment rate → `COUNT(Fulfilled) / COUNT(*)`
# MAGIC 3. Revenue decline by product family → CTE comparing Aug vs Jul
# MAGIC
# MAGIC ### Benchmarks (via Genie API → Benchmarks tab)
# MAGIC
# MAGIC Ground-truth Q&A pairs for **evaluating** agent accuracy. Added to supplier, demand, and logistics agents. Each benchmark has a SQL answer that Genie compares result sets against during benchmark runs.

# COMMAND ----------

# DBTITLE 1,ITERATION 1: Column Comments + Example SQL Queries + Benchmarks
# ============================================================
# ITERATION 1: Column Comments + Example SQL Queries + Benchmarks
# UC Features: Column/Table Comments (ALTER ... COMMENT),
#              Example SQL Queries (Genie Examples tab via API),
#              Benchmark Questions (Genie Benchmarks tab via API)
# Targets: D04, D06 (wrong table), F02 (vendor late ambiguity),
#          F03, F05 (status definition), H03 (fulfillment rate)
# ============================================================
import json, uuid, time

print("="*80)
print("  ITERATION 1: Column Comments + Example SQL Queries + Benchmarks")
print("="*80)
CAT = CATALOG

# --- Clone Baseline agents into new Iteration 1 agents ---
# Each iteration creates FRESH agents (never modifies previous iteration).
print("\n  Cloning Baseline agents → Iteration 1...")
if 'all_spaces' not in dir() or 'Baseline' not in all_spaces:
    # Discover baseline if not already in all_spaces
    all_spaces["Baseline"] = discover_agents_by_label("Baseline")
source = all_spaces["Baseline"]
assert len(source) >= 5, f"Baseline has only {len(source)} agents — need 5"
spaces = clone_agents(source, "Iteration 1")
all_spaces["Iteration 1"] = dict(spaces)
assert len(spaces) >= 5, f"Iteration 1 clone produced only {len(spaces)} agents"
for name, sid in spaces.items():
    print(f"    {name:15s} → {sid}")
print("\n  Updating Supervisor to point to Iteration 1 agents...")
update_supervisor_tools(spaces)

# --- Step 1: Column & Table Comments ---
print("\n  Step 1: Adding column and table comments...")

table_comments = {
    f"{CAT}.demand_analysis.sales_orders": "Customer sales orders",
    f"{CAT}.inventory_management.inventory_ledger": "Inventory position snapshots",
    f"{CAT}.logistics_operations.shipments": "Outbound shipment records",
    f"{CAT}.reporting.executive_kpis": "Executive KPI summary",
    f"{CAT}.supplier_procurement.supplier_lead_times": "Monthly supplier lead times",
    f"{CAT}.supplier_procurement.supplier_orders": "Supplier purchase orders",
    f"{CAT}.supplier_procurement.vendor_slas": "Supplier SLA results",
}

column_comments = {
    f"{CAT}.demand_analysis.sales_orders": {
        "channel": "Sales channel",
        "customer_id": "Customer identifier",
        "fulfillment_warehouse": "Fulfillment warehouse",
        "order_date": "Order date",
        "order_id": "Order identifier",
        "order_status": "Order fulfillment status",
        "product_category": "Product category",
        "product_family": "Product family",
        "quantity": "Units ordered",
        "region": "Sales region",
        "sku_id": "SKU identifier",
        "state": "State code",
        "total_amount": "Total order value",
        "unit_price": "Unit selling price",
    },
    f"{CAT}.inventory_management.inventory_ledger": {
        "allocated_qty": "Allocated units",
        "available_qty": "Available units",
        "below_safety_stock_flag": "Below safety stock",
        "days_of_supply": "Days of supply",
        "ledger_id": "Ledger identifier",
        "on_hand_qty": "On hand units",
        "product_category": "Product category",
        "product_family": "Product family",
        "region": "Inventory region",
        "reorder_point": "Reorder point units",
        "safety_stock_level": "Safety stock units",
        "sku_id": "SKU identifier",
        "snapshot_date": "Snapshot date",
        "stockout_flag": "Stockout status",
        "warehouse_id": "Warehouse identifier",
    },
    f"{CAT}.logistics_operations.shipments": {
        "actual_delivery_date": "Actual delivery date",
        "actual_transit_days": "Actual transit days",
        "carrier_id": "Carrier identifier",
        "delay_days": "Delay days",
        "delay_reason": "Delay reason",
        "destination_region": "Destination region",
        "destination_state": "Destination state",
        "expected_delivery_date": "Expected delivery date",
        "is_late": "Late shipment flag",
        "order_count": "Orders in shipment",
        "origin_region": "Origin region",
        "origin_warehouse": "Origin warehouse",
        "planned_transit_days": "Planned transit days",
        "ship_date": "Shipment date",
        "shipment_id": "Shipment identifier",
        "shipment_status": "Shipment status",
        "shipment_type": "Shipment type",
        "shipping_cost": "Shipping cost",
        "total_pallets": "Total pallets",
        "total_weight_kg": "Total weight kilograms",
    },
    f"{CAT}.reporting.executive_kpis": {
        "revenue_last_month": "Last month revenue",
        "revenue_prior_month": "Prior month revenue",
        "total_stockout_skus": "Stockout SKU count",
        "avg_days_of_supply": "Average days supply",
        "late_delivery_pct_last_month": "Late delivery percent",
        "avg_delay_days_last_month": "Average delay days",
        "supplier_late_pct_last_month": "Supplier late percent",
        "total_sla_breaches": "SLA breach count",
        "service_level_pct": "Service level percent",
    },
    f"{CAT}.supplier_procurement.supplier_lead_times": {
        "avg_actual_lead_time_days": "Average actual lead time",
        "contracted_lead_time_days": "Contracted lead time",
        "defect_rate_pct": "Defect rate percent",
        "lead_time_id": "Lead time identifier",
        "lead_time_variance_days": "Lead time variance days",
        "month": "Reporting month",
        "on_time_delivery_pct": "On time percent",
        "order_count": "Order count",
        "supplier_continent": "Supplier continent",
        "supplier_country": "Supplier country",
        "supplier_id": "Supplier identifier",
        "supplier_name": "Supplier name",
    },
    f"{CAT}.supplier_procurement.supplier_orders": {
        "actual_delivery_date": "Actual delivery date",
        "actual_lead_time_days": "Actual lead time",
        "contracted_lead_time_days": "Contracted lead time",
        "delay_reason": "Delay reason",
        "expected_delivery_date": "Expected delivery date",
        "is_late": "Late order flag",
        "lead_time_variance_days": "Lead time variance days",
        "order_date": "Purchase order date",
        "po_id": "Purchase order ID",
        "po_status": "Purchase order status",
        "product_family": "Product family",
        "quality_score": "Quality score",
        "quantity_ordered": "Units ordered",
        "quantity_received": "Units received",
        "supplier_continent": "Supplier continent",
        "supplier_country": "Supplier country",
        "supplier_id": "Supplier identifier",
        "supplier_name": "Supplier name",
        "total_cost": "Total order cost",
        "unit_cost": "Unit purchase cost",
    },
    f"{CAT}.supplier_procurement.vendor_slas": {
        "actual_pct": "Actual SLA percent",
        "is_breached": "SLA breach flag",
        "last_updated": "Last update date",
        "penalty_amount": "Penalty amount",
        "review_period": "Review period",
        "sla_id": "SLA identifier",
        "sla_metric": "SLA metric name",
        "supplier_id": "Supplier identifier",
        "supplier_name": "Supplier name",
        "target_pct": "Target SLA percent",
        "variance_pct": "SLA variance percent",
    },
}

for table_name, table_comment in table_comments.items():
    try:
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('comment' = '{table_comment}')")
        print(f"    \u2713 {table_name.split('.')[-1]} (table)")
    except Exception as e:
        print(f"    \u2717 {table_name.split('.')[-1]} table: {str(e)[:120]}")

for table_name, cols in column_comments.items():
    success_count = 0
    for col_name, col_comment in cols.items():
        try:
            spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {col_name} COMMENT '{col_comment}'")
            success_count += 1
        except Exception as e:
            print(f"    \u2717 {table_name.split('.')[-1]}.{col_name}: {str(e)[:100]}")
    print(f"    \u2713 {table_name.split('.')[-1]} columns: {success_count}/{len(cols)} commented")

# --- Step 1b: Augment agent instructions with robust safeguards ---
# RULE: append only — never remove existing instruction items.
print("\n  Step 1b: Augmenting agent instructions with safeguards...")

common_safeguard = (
    "SAFEGUARDS:\n"
    "- If a question is outside your domain, say 'This question is outside my area of expertise' "
    "and suggest which domain agent (logistics, demand, inventory, supplier, or executive) might help.\n"
    "- Do not guess or fabricate data. If you cannot find the answer in your tables, say so clearly.\n"
    "- Date context: current reference date is September 1, 2026. "
    "'Last month' = August 2026. 'Prior month' = July 2026.\n"
    "- Always use explicit date literals (e.g. DATE '2026-08-01'). Never use CURRENT_DATE().\n"
    "- Region values: Western, Eastern, Central, Southern. 'West' maps to 'Western'."
)

agent_table_context = {
    "demand": (
        "YOUR TABLES: sales_orders (primary), demand_forecasts, customer_segments, "
        "pos_data, products, promotions.\n"
        "- total_amount is the revenue column. Use order_date for date filtering.\n"
        "- order_status values: Fulfilled, Partially_Fulfilled, Backordered, Cancelled."
    ),
    "inventory": (
        "YOUR TABLES: inventory_ledger (primary), stock_movements, store_inventory, warehouse_data.\n"
        "- below_safety_stock_flag and stockout_flag are boolean columns.\n"
        "- Use region column for regional inventory questions."
    ),
    "logistics": (
        "YOUR TABLES: shipments (primary), carriers, distribution_centers, transit_data.\n"
        "- Use ship_date for date filtering, not actual_delivery_date.\n"
        "- Use destination_region for delivery region, not origin_region.\n"
        "- is_late = true means delayed or late shipment."
    ),
    "supplier": (
        "YOUR TABLES: supplier_orders (primary), supplier_lead_times, suppliers, "
        "vendor_slas, procurement_data.\n"
        "- Use supplier_orders for per-order metrics like lead time variance.\n"
        "- supplier_lead_times is pre-aggregated monthly data.\n"
        "- Use order_date for date filtering."
    ),
    "executive": (
        "YOUR TABLES: executive_kpis, supply_chain_risk_scorecard, "
        "regional_performance_summary, revenue_trend.\n"
        "- service_level_pct is the current service level.\n"
        "- Use pre-built views for cross-domain questions when available."
    ),
}

SAFEGUARD_MARKER = "SAFEGUARDS:"

for agent_name, space_id in spaces.items():
    resp = requests.get(f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true", headers=headers)
    ss = json.loads(resp.json().get("serialized_space", "{}"))
    instrs = ss.get("instructions", {})
    if not isinstance(instrs, dict):
        instrs = {"text_instructions": [{"content": [str(instrs)]}]}
    text_instrs = instrs.get("text_instructions", [])
    if not text_instrs:
        text_instrs = [{"content": []}]
    current_text = str(text_instrs[0].get("content", []))
    if SAFEGUARD_MARKER in current_text:
        print(f"    (skip) {agent_name}: already has safeguards")
        continue
    # Append safeguards + table context (never replace)
    text_instrs[0]["content"].append(common_safeguard)
    specific = agent_table_context.get(agent_name, "")
    if specific:
        text_instrs[0]["content"].append(specific)
    instrs["text_instructions"] = text_instrs
    ss["instructions"] = instrs
    # Note: no ensure_sorted_payload here — we only touch text_instructions,
    # not example_question_sqls or benchmarks.
    patch_resp = requests.patch(
        f"{host}/api/2.0/genie/spaces/{space_id}",
        headers=headers,
        json={"serialized_space": json.dumps(ss)}
    )
    if patch_resp.status_code == 200:
        final_count = len(text_instrs[0]["content"])
        print(f"    \u2713 {agent_name}: safeguards + table context added ({final_count} instruction items)")
    else:
        print(f"    \u2717 {agent_name}: PATCH failed {patch_resp.status_code} {patch_resp.text[:200]}")

# --- Helper: ensure sortable arrays are sorted before any PATCH ---
def ensure_sorted_payload(ss):
    """Genie API rejects PATCHes if example_question_sqls or benchmarks aren't sorted by id."""
    if "instructions" in ss and "example_question_sqls" in ss.get("instructions", {}):
        ss["instructions"]["example_question_sqls"].sort(key=lambda x: str(x.get("id", "")))
    if "benchmarks" in ss and "questions" in ss.get("benchmarks", {}):
        ss["benchmarks"]["questions"].sort(key=lambda x: str(x.get("id", "")))
    return ss

# --- Step 2: Add Example SQL Queries via Genie API (Examples tab) ---
# The RIGHT way to teach Genie: use example_question_sqls in serialized_space.
# These appear in the Genie Agent "Examples" tab and directly teach the LLM
# how to write correct SQL for common question patterns.
# This replaces the old approach of embedding SQL snippets in text_instructions.
print("\n  Step 2: Adding Example SQL Queries via Genie API (Examples tab)...")

example_sqls = {
    "supplier": [
        {
            "id": uuid.uuid4().hex,
            "question": ["What is the average lead time variance in days for all suppliers last month?"],
            "sql": [f"SELECT ROUND(AVG(lead_time_variance_days), 2) as avg_lead_time_variance FROM {CAT}.supplier_procurement.supplier_orders WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'"],
            "usage_guidance": ["ALWAYS use supplier_orders for lead time variance, NEVER supplier_lead_times (pre-aggregated monthly summary gives different averages)."]
        },
        {
            "id": uuid.uuid4().hex,
            "question": ["What is the average lead time variance for Asia suppliers last month?"],
            "sql": [f"SELECT ROUND(AVG(lead_time_variance_days), 2) as avg_lead_time_variance FROM {CAT}.supplier_procurement.supplier_orders WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01' AND supplier_continent = 'Asia'"],
            "usage_guidance": ["Filter by supplier_continent for continent-specific lead time variance. Valid values: Asia, Europe, North America."]
        },
        {
            "id": uuid.uuid4().hex,
            "question": ["What percentage of vendors had delayed deliveries last month?", "What percentage of purchase orders were delayed last month?", "What percentage of purchase orders were late last month?"],
            "sql": [f"SELECT ROUND(100.0 * SUM(CASE WHEN is_late THEN 1 ELSE 0 END) / COUNT(*), 2) as vendor_late_pct FROM {CAT}.supplier_procurement.supplier_orders WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'"],
            "usage_guidance": ["Vendor delayed delivery % is computed per ORDER (count of delayed/late orders / total orders), not per distinct vendor. Treat delayed as the same concept as is_late = true."]
        },
    ],
    "demand": [
        {
            "id": uuid.uuid4().hex,
            "question": ["How many Western region orders were fulfilled last month?"],
            "sql": [f"SELECT COUNT(*) as fulfilled_orders FROM {CAT}.demand_analysis.sales_orders WHERE order_status = 'Fulfilled' AND region = 'Western' AND order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'"],
            "usage_guidance": ["Fulfilled = ONLY order_status = 'Fulfilled'. Do NOT include Partially_Fulfilled."]
        },
        {
            "id": uuid.uuid4().hex,
            "question": ["What is the order fulfillment rate for Western region last month?"],
            "sql": [f"SELECT ROUND(100.0 * SUM(CASE WHEN order_status = 'Fulfilled' THEN 1 ELSE 0 END) / COUNT(*), 2) as fulfillment_rate FROM {CAT}.demand_analysis.sales_orders WHERE region = 'Western' AND order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'"],
            "usage_guidance": ["Fulfillment rate = Fulfilled / total orders. Partially_Fulfilled is NOT fulfilled."]
        },
        {
            "id": uuid.uuid4().hex,
            "question": ["Which product family had the largest revenue decline in Western region last month vs prior month?"],
            "sql": [
                f"WITH monthly AS (SELECT product_family, ",
                f"SUM(CASE WHEN order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01' THEN total_amount ELSE 0 END) as aug_rev, ",
                f"SUM(CASE WHEN order_date >= DATE '2026-07-01' AND order_date < DATE '2026-08-01' THEN total_amount ELSE 0 END) as jul_rev ",
                f"FROM {CAT}.demand_analysis.sales_orders WHERE region = 'Western' GROUP BY product_family) ",
                f"SELECT product_family, ROUND(aug_rev - jul_rev, 2) as revenue_change FROM monthly ORDER BY revenue_change ASC LIMIT 1"
            ],
            "usage_guidance": ["Compare Aug 2026 vs Jul 2026 revenue per product_family. Most negative = largest decline."]
        },
    ],
    "logistics": [
        {
            "id": uuid.uuid4().hex,
            "question": ["What is the delayed shipment rate for Western region shipments last month?", "What percentage of Western region shipments were delayed last month?", "What is the late delivery rate for Western region shipments last month?"],
            "sql": [f"SELECT ROUND(AVG(CASE WHEN is_late = true THEN 1.0 ELSE 0.0 END) * 100, 2) as late_delivery_rate FROM {CAT}.logistics_operations.shipments WHERE destination_region = 'Western' AND ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'"],
            "usage_guidance": ["ALWAYS filter by ship_date (not actual_delivery_date) for monthly shipment counts and rates. Use destination_region (not origin_region) for region filtering. Treat delayed shipment as the same concept as is_late = true. NEVER use CURRENT_DATE() \u2014 always use explicit date literals like DATE '2026-08-01'. NEVER use ILIKE patterns for region \u2014 use exact match (destination_region = 'Western')."]
        },
        {
            "id": uuid.uuid4().hex,
            "question": ["What is the average delay in days for delayed shipments in the Western region last month?", "What is the average delay days for delayed deliveries in the West region?", "What is the average delay days for late deliveries in the West region?"],
            "sql": [f"SELECT ROUND(AVG(CASE WHEN is_late THEN delay_days END), 2) as avg_delay_days FROM {CAT}.logistics_operations.shipments WHERE destination_region = 'Western' AND ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'"],
            "usage_guidance": ["Average delay for delayed/late shipments = AVG(delay_days) for late/delayed shipments only (CASE WHEN is_late THEN delay_days END). Do NOT add a separate delay_days IS NOT NULL filter \u2014 the CASE WHEN handles NULLs automatically. ALWAYS filter by ship_date (NEVER actual_delivery_date). NEVER use CURRENT_DATE() \u2014 use explicit date literals."]
        },
        {
            "id": uuid.uuid4().hex,
            "question": ["What is the on-time delivery rate for Western region last month?", "What is the OTD rate for the West?"],
            "sql": [f"SELECT ROUND(AVG(CASE WHEN is_late = false THEN 1.0 ELSE 0.0 END) * 100, 2) as otd_rate FROM {CAT}.logistics_operations.shipments WHERE destination_region = 'Western' AND ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'"],
            "usage_guidance": ["On-time delivery rate = percentage of shipments where is_late = false. Use ship_date for date filtering, destination_region for region."]
        },
    ]
}

for agent_name, examples in example_sqls.items():
    space_id = spaces.get(agent_name)
    if not space_id:
        print(f"    \u2717 Agent '{agent_name}' not found")
        continue
    resp = requests.get(f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true", headers=headers)
    if resp.status_code != 200:
        print(f"    \u2717 GET {agent_name} failed: {resp.status_code}")
        continue
    ss = json.loads(resp.json().get("serialized_space", "{}"))
    if "instructions" not in ss:
        ss["instructions"] = {}
    existing = ss["instructions"].get("example_question_sqls", [])
    existing.extend(examples)
    existing.sort(key=lambda x: str(x.get("id", "")))
    ss["instructions"]["example_question_sqls"] = existing
    ensure_sorted_payload(ss)
    patch_resp = requests.patch(f"{host}/api/2.0/genie/spaces/{space_id}", headers=headers,
                                json={"serialized_space": json.dumps(ss)})
    if patch_resp.status_code == 200:
        print(f"    \u2713 {agent_name}: {len(examples)} example SQL queries added (Examples tab)")
    else:
        print(f"    \u2717 {agent_name}: PATCH failed {patch_resp.status_code} {patch_resp.text[:200]}")

# --- Step 3: Add Benchmarks via Genie API (Benchmarks tab) ---
# Benchmarks are ground-truth Q&A pairs for evaluating agent accuracy.
# They appear in the Genie Agent "Benchmarks" tab and can be run to assess quality.
# Each question has a SQL answer that Genie compares result sets against.
print("\n  Step 3: Adding Benchmark Questions via Genie API (Benchmarks tab)...")

benchmark_questions = {
    "supplier": [
        {
            "id": uuid.uuid4().hex,
            "question": ["What is the average lead time variance in days for all suppliers last month?"],
            "answer": [{"format": "SQL", "content": [f"SELECT ROUND(AVG(lead_time_variance_days), 2) as avg_lead_time_variance FROM {CAT}.supplier_procurement.supplier_orders WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'"]}]
        },
        {
            "id": uuid.uuid4().hex,
            "question": ["What percentage of purchase orders were delayed last month?", "What percentage of purchase orders were late last month?"],
            "answer": [{"format": "SQL", "content": [f"SELECT ROUND(100.0 * SUM(CASE WHEN is_late THEN 1 ELSE 0 END) / COUNT(*), 2) as vendor_late_pct FROM {CAT}.supplier_procurement.supplier_orders WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'"]}]
        },
        {
            "id": uuid.uuid4().hex,
            "question": ["What is the average lead time variance for Europe suppliers last month?"],
            "answer": [{"format": "SQL", "content": [f"SELECT ROUND(AVG(lead_time_variance_days), 2) as avg_lead_time_variance FROM {CAT}.supplier_procurement.supplier_orders WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01' AND supplier_continent = 'Europe'"]}]
        },
    ],
    "demand": [
        {
            "id": uuid.uuid4().hex,
            "question": ["How many Western region orders were fulfilled last month?"],
            "answer": [{"format": "SQL", "content": [f"SELECT COUNT(*) as fulfilled_orders FROM {CAT}.demand_analysis.sales_orders WHERE order_status = 'Fulfilled' AND region = 'Western' AND order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'"]}]
        },
        {
            "id": uuid.uuid4().hex,
            "question": ["What is the order fulfillment rate for Western region last month?"],
            "answer": [{"format": "SQL", "content": [f"SELECT ROUND(100.0 * SUM(CASE WHEN order_status = 'Fulfilled' THEN 1 ELSE 0 END) / COUNT(*), 2) as fulfillment_rate FROM {CAT}.demand_analysis.sales_orders WHERE region = 'Western' AND order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'"]}]
        },
    ],
    "logistics": [
        {
            "id": uuid.uuid4().hex,
            "question": ["What is the delayed shipment rate for Western region shipments last month?", "What is the late delivery rate for Western region shipments last month?"],
            "answer": [{"format": "SQL", "content": [f"SELECT ROUND(AVG(CASE WHEN is_late = true THEN 1.0 ELSE 0.0 END) * 100, 2) as late_delivery_rate FROM {CAT}.logistics_operations.shipments WHERE destination_region = 'Western' AND ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'"]}]
        },
        {
            "id": uuid.uuid4().hex,
            "question": ["What is the average delay in days for delayed shipments in the Western region last month?", "What is the average delay in days for late shipments in the Western region last month?"],
            "answer": [{"format": "SQL", "content": [f"SELECT ROUND(AVG(CASE WHEN is_late THEN delay_days END), 2) as avg_delay_days FROM {CAT}.logistics_operations.shipments WHERE destination_region = 'Western' AND ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'"]}]
        },
    ]
}

for agent_name, benchmarks in benchmark_questions.items():
    space_id = spaces.get(agent_name)
    if not space_id:
        continue
    resp = requests.get(f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true", headers=headers)
    if resp.status_code != 200:
        continue
    ss = json.loads(resp.json().get("serialized_space", "{}"))
    existing_bm = ss.get("benchmarks", {}).get("questions", [])
    existing_bm.extend(benchmarks)
    existing_bm.sort(key=lambda x: str(x.get("id", "")))
    ss["benchmarks"] = {"questions": existing_bm}
    ensure_sorted_payload(ss)  # also re-sort example_question_sqls in case server reordered them
    patch_resp = requests.patch(f"{host}/api/2.0/genie/spaces/{space_id}", headers=headers,
                                json={"serialized_space": json.dumps(ss)})
    if patch_resp.status_code == 200:
        print(f"    \u2713 {agent_name}: {len(benchmarks)} benchmarks added (Benchmarks tab)")
    else:
        print(f"    \u2717 {agent_name}: PATCH failed {patch_resp.status_code} {patch_resp.text[:200]}")

print("\n\u2705 Iteration 1 applied \u2014 column comments + example SQL queries + benchmarks")
iter1_passed, iter1_failed, iter1_errors, iter1_details = test_all_metrics("After Iteration 1")

# --- Comprehensive Prompt Benchmark: After Iteration 1 ---
comp_iter1 = run_comprehensive_benchmark("After Iteration 1 (comments + examples + benchmarks)")
all_comp_results["After Iteration 1"] = comp_iter1

# COMMAND ----------

# DBTITLE 1,VISUAL: After Iteration 1
# Re-plot dashboard with latest results (including Iteration 1)
plot_test_dashboard(all_stage_results, all_comp_results if 'all_comp_results' in dir() else None)

# ── Provenance Analysis: Extracted from EXISTING test results (no API calls) ──
current_stage = list(all_stage_results.keys())[-1] if all_stage_results else "Unknown"
results = all_stage_results.get(current_stage, [])

if results:
    ran_stages = set(all_stage_results.keys())
    iter_actually_ran = {
        'Iter 1': any('iteration 1' in s.lower() or 'iter 1' in s.lower() or 'iter1' in s.lower() for s in ran_stages),
        'Iter 2': any('iteration 2' in s.lower() or 'iter 2' in s.lower() or 'iter2' in s.lower() for s in ran_stages),
        'Iter 3': any('iteration 3' in s.lower() or 'iter 3' in s.lower() or 'iter3' in s.lower() for s in ran_stages),
    }
    def relabel_tier(raw_tier):
        if raw_tier == 'GUESSED':
            return 'Guessed (threshold)'
        if raw_tier in iter_actually_ran and not iter_actually_ran[raw_tier]:
            return f'Guessed ({raw_tier})'
        return raw_tier
    def _confidence_from_tier(tier):
        if tier in ('Iter 2', 'Iter 3') or tier.startswith('Iter 3'): return 'DETERMINISTIC'
        if tier == 'Iter 1': return 'HEURISTIC'
        if tier.startswith('Guessed'): return 'GUESSED'
        if tier == 'N/A': return 'N/A'
        return 'INFERRED'

    print("\n" + "="*90)
    print(f"  PROVENANCE ANALYSIS: {current_stage}")
    print(f"  Extracted from test results — no additional API calls")
    print("="*90)

    current_group = ""
    group_names = {'A':'LOGISTICS','B':'DEMAND','C':'INVENTORY','D':'SUPPLIER',
                   'E':'CROSS-DOMAIN','F':'INDIRECT','H':'HARD','G':'FISCAL','P':'CRITICAL'}
    for r in results:
        gid = r['id'][:1]
        if gid != current_group:
            current_group = gid
            print(f"\n  ── {group_names.get(gid, gid)} {'─'*80}")
        icon = '✅' if r['verdict'] == 'PASS' else ('❌' if r['verdict'] == 'FAIL' else '⚠️')
        prov = r.get('provenance', 'Unknown')
        tier = relabel_tier(r.get('prov_iter', '?'))
        conf = _confidence_from_tier(tier)
        gt = r.get('expected', '?')
        found = r.get('found') if r.get('found') is not None else r.get('closest')
        found_str = f"{found}" if found is not None else "—"
        print(f"  {icon} {r['id']:<5} {r['verdict']:<5}  gt={gt:<14}  found={found_str:<14}  [{tier}] {prov}")
        print(f"         Confidence: {conf}")

    prov_tiers = {}
    for r in results:
        tier = relabel_tier(r.get('prov_iter', 'Unknown'))
        prov_tiers.setdefault(tier, []).append(r)

    print(f"\n{'='*90}")
    print(f"  PROVENANCE SUMMARY BY TIER")
    print(f"{'='*90}")
    tier_order = ['Raw Table', 'Guessed (Iter 1)', 'Guessed (Iter 2)', 'Guessed (Iter 3)',
                  'Guessed (threshold)', 'Iter 1', 'Iter 2', 'Iter 3', 'Iter 3 (inferred)', 'N/A']
    for tier in tier_order:
        if tier not in prov_tiers: continue
        tests = prov_tiers[tier]
        p = sum(1 for t in tests if t['verdict'] == 'PASS')
        f = sum(1 for t in tests if t['verdict'] == 'FAIL')
        pct = 100 * p // max(len(tests), 1)
        ids = ', '.join(t['id'] for t in tests)
        print(f"  {tier:42s}: {len(tests):2d} tests -> {p:2d} PASS ({pct}%), {f:2d} FAIL  [{ids}]")

    conf_order = ['DETERMINISTIC', 'HEURISTIC', 'INFERRED', 'GUESSED', 'N/A']
    conf_counts = {}
    for r in results:
        tier = relabel_tier(r.get('prov_iter', 'Unknown'))
        conf = _confidence_from_tier(tier)
        conf_counts.setdefault(conf, {'pass': 0, 'fail': 0})
        if r['verdict'] == 'PASS': conf_counts[conf]['pass'] += 1
        else: conf_counts[conf]['fail'] += 1
    print(f"\n  REASONING CONFIDENCE DISTRIBUTION:")
    for conf in conf_order:
        if conf in conf_counts:
            c = conf_counts[conf]
            print(f"    {conf:20s}: {c['pass']+c['fail']:2d} tests ({c['pass']} PASS, {c['fail']} FAIL)")

    # ══ VISUAL: Provenance Tier + LLM Reasoning charts ══
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch
    fig, (ax_prov, ax_llm) = plt.subplots(1, 2, figsize=(18, 5))

    # ── Panel 1: Per-Domain Group PASS/FAIL ──
    group_order = ['A','B','C','D','E','F','H','G','P']
    grp_name_map = {'A':'Logistics (A)','B':'Demand (B)','C':'Inventory (C)','D':'Supplier (D)',
                     'E':'Cross-Domain (E)','F':'Indirect (F)','H':'Hard (H)','G':'Fiscal (G)','P':'Critical (P)'}
    grp_pass, grp_fail, grp_labels = [], [], []
    for g in group_order:
        tests = [r for r in results if r['id'].startswith(g)]
        if not tests: continue
        p = sum(1 for t in tests if t['verdict'] == 'PASS')
        f = len(tests) - p
        grp_labels.append(grp_name_map.get(g, g))
        grp_pass.append(p)
        grp_fail.append(f)
    y_pos = range(len(grp_labels))
    ax_prov.barh(y_pos, grp_pass, color='#2e7d32', edgecolor='white', label='PASS')
    ax_prov.barh(y_pos, grp_fail, left=grp_pass, color='#d32f2f', edgecolor='white', label='FAIL')
    ax_prov.set_yticks(y_pos)
    ax_prov.set_yticklabels(grp_labels, fontsize=10)
    ax_prov.set_xlabel('Test Count')
    ax_prov.set_title(f'PASS/FAIL by Domain Group ({current_stage})', fontsize=11, fontweight='bold')
    ax_prov.legend(loc='lower right', fontsize=9)
    ax_prov.invert_yaxis()
    for i, (p, f) in enumerate(zip(grp_pass, grp_fail)):
        total = p + f
        ax_prov.text(total+0.3, i, f'{total} ({p}P/{f}F)', va='center', fontsize=9, fontweight='bold')
    ax_prov.set_xlim(0, max(p+f for p, f in zip(grp_pass, grp_fail)) + 6)

    cat_labels, cat_pass, cat_fail = [], [], []
    cat_colors = {'DETERMINISTIC':'#1b5e20','HEURISTIC':'#689f38','INFERRED':'#f9a825',
                  'GUESSED':'#b71c1c','N/A':'#757575'}
    for conf in conf_order:
        if conf not in conf_counts: continue
        c = conf_counts[conf]
        cat_labels.append(conf)
        cat_pass.append(c['pass'])
        cat_fail.append(c['fail'])
    y_pos2 = range(len(cat_labels))
    bar_colors = [cat_colors.get(c, '#757575') for c in cat_labels]
    bars_total = [p+f for p, f in zip(cat_pass, cat_fail)]
    ax_llm.barh(y_pos2, cat_pass, color=bar_colors, edgecolor='white', alpha=0.9)
    ax_llm.barh(y_pos2, cat_fail, left=cat_pass, color=bar_colors, edgecolor='white', alpha=0.3)
    ax_llm.set_yticks(y_pos2)
    ax_llm.set_yticklabels(cat_labels, fontsize=10)
    ax_llm.set_xlabel('Test Count')
    ax_llm.set_title(f'Reasoning Confidence: Is This Answer Repeatable? ({current_stage})', fontsize=11, fontweight='bold')
    ax_llm.invert_yaxis()
    for i, (p, f) in enumerate(zip(cat_pass, cat_fail)):
        suffix = '' if f == 0 else f' ({f}F)'
        ax_llm.text(p+f+0.3, i, f'{p+f}{suffix}', va='center', fontsize=9, fontweight='bold')
    ax_llm.set_xlim(0, max(bars_total) + 5 if bars_total else 10)
    ax_llm.legend(handles=[Patch(facecolor='#1b5e20', label='DETERMINISTIC'),
        Patch(facecolor='#689f38', label='HEURISTIC'),
        Patch(facecolor='#f9a825', label='INFERRED'),
        Patch(facecolor='#b71c1c', label='GUESSED')],
        loc='lower right', fontsize=7)
    plt.tight_layout()
    plt.show()

if 'assumptions' in dir():
    plot_classification_dashboard(all_stage_results, assumptions)

# COMMAND ----------

# DBTITLE 1,Iteration 2 Approach: UC Metric Views + Governed Tags + Open Knowledge
# MAGIC %md
# MAGIC ## Iteration 2: UC Metric Views + Governed Tags + Open Knowledge View
# MAGIC
# MAGIC **UC Features Used:** Metric Views (`CREATE VIEW ... WITH METRICS LANGUAGE YAML`), Governed Tags (`ALTER TABLE SET TAGS`), Schema Domain Tags (`ALTER SCHEMA SET TAGS`), Open Knowledge View (cross-domain governed join)
# MAGIC
# MAGIC **Goal:** Give each agent pre-computed, governed metrics so it doesn't have to infer SQL logic. Fix cross-domain failures that no single agent can answer alone. Stabilize more non-deterministic passes.
# MAGIC
# MAGIC ### What This Fixes
# MAGIC
# MAGIC | Failure | Root Cause | Fix |
# MAGIC | --- | --- | --- |
# MAGIC | E03 | Cost of Disruption requires joining 4 domain schemas — no single agent has all tables | Open Knowledge View: `cost_of_disruption_by_region` |
# MAGIC | H05 | Revenue at risk spans demand + logistics data | Same CoD view: `revenue_at_risk_from_disruptions` column |
# MAGIC | H06 | Revenue per stockout SKU needs inventory + demand | Same CoD view: `revenue_at_risk_per_stockout_sku` column |
# MAGIC | H07 | CoD-to-revenue ratio is cross-domain | Same CoD view: `disruption_to_revenue_ratio` column |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4 UC Metric Views (WITH METRICS LANGUAGE YAML)
# MAGIC
# MAGIC Metric Views define **dimensions** (slicing axes) and **measures** (aggregation formulas) declaratively. The Genie agent can query them directly without inferring SQL.
# MAGIC
# MAGIC #### 1. `delivery_performance_by_region` (Logistics)
# MAGIC * **Source:** `logistics_operations.shipments` (Aug 2026)
# MAGIC * **Dimension:** `region` = `destination_region` (Western, Eastern, Central, Southern)
# MAGIC * **Measures:**
# MAGIC   * `on_time_delivery_rate` — `AVG(CASE WHEN NOT is_late THEN 1.0 ELSE 0.0 END) * 100`
# MAGIC   * `late_delivery_rate` — `AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100`
# MAGIC   * `avg_delay_days` — `AVG(delay_days)` for late shipments only
# MAGIC   * `total_shipments` — `COUNT(*)`
# MAGIC   * `late_shipments` — `SUM(CASE WHEN is_late THEN 1 END)`
# MAGIC   * `wasted_freight_cost` — `SUM(shipping_cost)` for late deliveries
# MAGIC
# MAGIC #### 2. `revenue_comparison_by_region` (Demand)
# MAGIC * **Source:** `demand_analysis.sales_orders` (full history, date-filtered in measures)
# MAGIC * **Dimensions:** `region`, `product_family`
# MAGIC * **Measures:**
# MAGIC   * `revenue_last_month` — SUM(total_amount) for Aug 2026
# MAGIC   * `revenue_prior_month` — SUM(total_amount) for Jul 2026
# MAGIC   * `revenue_change_dollars` — Aug minus Jul (negative = decline)
# MAGIC   * `revenue_change_pct` — Percentage change Aug vs Jul
# MAGIC
# MAGIC #### 3. `inventory_safety_stock_metrics` (Inventory)
# MAGIC * **Source:** `inventory_management.inventory_ledger` (pre-filtered: `below_safety_stock_flag = true`)
# MAGIC * **Dimension:** `region`
# MAGIC * **Measures:**
# MAGIC   * `positions_below_safety_stock` — `COUNT(*)` (SKU-warehouse pairs)
# MAGIC   * `unique_skus_below_safety` — `COUNT(DISTINCT sku_id)` (deduplicated)
# MAGIC   * `stockout_positions` / `unique_skus_in_stockout` — stockout counts
# MAGIC   * `avg_days_of_supply` — average for at-risk items
# MAGIC
# MAGIC #### 4. `supplier_performance_by_continent` (Supplier)
# MAGIC * **Source:** `supplier_procurement.supplier_orders` (Aug 2026)
# MAGIC * **Dimension:** `supplier_continent` (Asia, Europe, North America)
# MAGIC * **Measures:**
# MAGIC   * `total_purchase_orders` / `late_purchase_orders` — PO counts
# MAGIC   * `supplier_late_rate_pct` — `AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100`
# MAGIC   * `avg_lead_time_variance` — `AVG(lead_time_variance_days)`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Open Knowledge View: `cost_of_disruption_by_region`
# MAGIC
# MAGIC This is a **regular SQL view** (not a Metric View) because it joins across 4 domain schemas. No single Genie agent has visibility into all 4, so this governed view makes the cross-domain calculation authoritative and directly queryable.
# MAGIC
# MAGIC **Cross-domain join logic (5 CTEs):**
# MAGIC
# MAGIC | CTE | Schema | Logic |
# MAGIC | --- | --- | --- |
# MAGIC | `demand_impact` | `demand_analysis.sales_orders` | Cancelled revenue + backordered revenue + total revenue per region |
# MAGIC | `logistics_impact` | `logistics_operations.shipments` | Wasted freight (late shipping cost) + late shipment count per region |
# MAGIC | `supplier_penalties` | `supplier_procurement.vendor_slas` | Total SLA penalties (breached only) — global, then allocated |
# MAGIC | `all_late_shipments` | `logistics_operations.shipments` | Total late shipments across ALL regions (denominator for allocation) |
# MAGIC | `inventory_stockouts` | `inventory_management.inventory_ledger` | Unique stockout SKUs per region |
# MAGIC
# MAGIC **Join:** `demand_impact` JOIN `logistics_impact` ON region, CROSS JOIN `supplier_penalties` and `all_late_shipments`, LEFT JOIN `inventory_stockouts`
# MAGIC
# MAGIC **SLA penalty allocation:** Penalties are allocated to each region proportionally by its share of late shipments: `total_penalties * (region_late / total_late)`
# MAGIC
# MAGIC **Output columns:** `total_cost_of_disruption`, `revenue_at_risk_from_disruptions`, `revenue_at_risk_per_stockout_sku`, `disruption_to_revenue_ratio`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### UC Governed Tags
# MAGIC
# MAGIC Tags applied to each view for discoverability and governance:
# MAGIC * `domain` — logistics / demand / inventory / supplier / financial_impact
# MAGIC * `metric_type` — delivery / revenue / safety_stock / performance / composite_cross_domain
# MAGIC * `data_quality` = `authoritative` on key views
# MAGIC
# MAGIC Schema-level domain tags on all 5 schemas (`ALTER SCHEMA SET TAGS`).

# COMMAND ----------

# DBTITLE 1,ITERATION 2: UC Metric Views + Governed Tags + Open Knowledge View
# ============================================================
# ITERATION 2: UC Metric Views + Governed Tags + Cross-Domain CoD
# UC Features: Metric Views (WITH METRICS LANGUAGE YAML),
#              Governed Tags (ALTER TABLE SET TAGS),
#              Schema Domain Tags (ALTER SCHEMA SET TAGS),
#              Open Knowledge View (cross-domain CoD join)
# Targets: E03 (CoD), H05 (revenue at risk), H06 (rev/stockout), H07 (CoD ratio)
# ============================================================
import json, time

print("="*80)
print("  ITERATION 2: UC Metric Views + Governed Tags + Open Knowledge")
print("="*80)
CAT = CATALOG

# --- Clone Iteration 1 agents into new Iteration 2 agents ---
print("\n  Cloning Iteration 1 agents → Iteration 2...")
if 'all_spaces' not in dir() or 'Iteration 1' not in all_spaces:
    all_spaces["Iteration 1"] = discover_agents_by_label("Iteration 1")
source = all_spaces["Iteration 1"]
assert len(source) >= 5, f"Iteration 1 has only {len(source)} agents — need 5"
spaces = clone_agents(source, "Iteration 2")
all_spaces["Iteration 2"] = dict(spaces)
assert len(spaces) >= 5, f"Iteration 2 clone produced only {len(spaces)} agents"
for name, sid in spaces.items():
    print(f"    {name:15s} → {sid}")
print("\n  Updating Supervisor to point to Iteration 2 agents...")
update_supervisor_tools(spaces)

# ── Step 1: Create 4 UC Metric Views (WITH METRICS LANGUAGE YAML) ──
print("\n  Step 1: Creating UC Metric Views...")

metric_view_sqls = [
    # 1. Delivery Performance
    f"""CREATE OR REPLACE VIEW {CAT}.logistics_operations.delivery_performance_by_region
WITH METRICS LANGUAGE YAML AS $$
  version: 1.1
  source: >
    SELECT shipment_id, destination_region, is_late, delay_days, shipping_cost, ship_date
    FROM {CAT}.logistics_operations.shipments
    WHERE ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'
  dimensions:
    - name: region
      expr: destination_region
      comment: Destination region (Western, Eastern, Central, Southern)
      synonyms: [destination region, delivery region]
  measures:
    - name: on_time_delivery_rate
      expr: ROUND(AVG(CASE WHEN is_late = false THEN 1.0 ELSE 0.0 END) * 100, 2)
      comment: Percentage of shipments delivered on time
      synonyms: [OTD rate, on-time rate]
    - name: late_delivery_rate
      expr: ROUND(AVG(CASE WHEN is_late = true THEN 1.0 ELSE 0.0 END) * 100, 2)
      comment: Percentage of shipments that were late
      synonyms: [late rate]
    - name: avg_delay_days
      expr: ROUND(AVG(CASE WHEN is_late THEN delay_days END), 2)
      comment: Average delay in days for late shipments only
    - name: total_shipments
      expr: COUNT(*)
      comment: Total number of shipments
    - name: late_shipments
      expr: SUM(CASE WHEN is_late THEN 1 ELSE 0 END)
      comment: Number of late shipments
    - name: wasted_freight_cost
      expr: ROUND(SUM(CASE WHEN is_late THEN shipping_cost ELSE 0 END), 2)
      comment: Total shipping cost for late deliveries
      synonyms: [wasted freight]
$$""",
    # 2. Revenue Comparison
    f"""CREATE OR REPLACE VIEW {CAT}.demand_analysis.revenue_comparison_by_region
WITH METRICS LANGUAGE YAML AS $$
  version: 1.1
  source: >
    SELECT order_id, region, order_date, total_amount, order_status, product_family
    FROM {CAT}.demand_analysis.sales_orders
  dimensions:
    - name: region
      expr: region
      comment: Geographic region (Western, Eastern, Central, Southern)
    - name: product_family
      expr: product_family
  measures:
    - name: revenue_last_month
      expr: ROUND(SUM(CASE WHEN order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01' THEN total_amount ELSE 0 END), 2)
      comment: Total revenue for August 2026
      synonyms: [august revenue, last month revenue]
    - name: revenue_prior_month
      expr: ROUND(SUM(CASE WHEN order_date >= DATE '2026-07-01' AND order_date < DATE '2026-08-01' THEN total_amount ELSE 0 END), 2)
      comment: Total revenue for July 2026
    - name: revenue_change_dollars
      expr: |-
        ROUND(SUM(CASE WHEN order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01' THEN total_amount ELSE 0 END) -
        SUM(CASE WHEN order_date >= DATE '2026-07-01' AND order_date < DATE '2026-08-01' THEN total_amount ELSE 0 END), 2)
      comment: Dollar change in revenue Aug vs Jul. Negative = decline.
      synonyms: [revenue change, MoM change]
    - name: revenue_change_pct
      expr: |-
        ROUND((SUM(CASE WHEN order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01' THEN total_amount ELSE 0 END) -
        SUM(CASE WHEN order_date >= DATE '2026-07-01' AND order_date < DATE '2026-08-01' THEN total_amount ELSE 0 END)) /
        NULLIF(SUM(CASE WHEN order_date >= DATE '2026-07-01' AND order_date < DATE '2026-08-01' THEN total_amount ELSE 0 END), 0) * 100, 2)
      comment: Percentage change in revenue Aug vs Jul
$$""",
    # 3. Inventory Safety Stock
    f"""CREATE OR REPLACE VIEW {CAT}.inventory_management.inventory_safety_stock_metrics
WITH METRICS LANGUAGE YAML AS $$
  version: 1.1
  source: >
    SELECT sku_id, warehouse_id, region, below_safety_stock_flag, stockout_flag, days_of_supply
    FROM {CAT}.inventory_management.inventory_ledger
    WHERE below_safety_stock_flag = true
  dimensions:
    - name: region
      expr: region
      comment: Geographic region
      synonyms: [inventory region, warehouse region]
  measures:
    - name: positions_below_safety_stock
      expr: COUNT(*)
      comment: SKU-warehouse positions below safety stock (authoritative count)
      synonyms: [below safety stock count, items below safety stock]
    - name: unique_skus_below_safety
      expr: COUNT(DISTINCT sku_id)
      comment: Distinct SKUs below safety stock (deduplicated across warehouses)
    - name: stockout_positions
      expr: SUM(CASE WHEN stockout_flag = true THEN 1 ELSE 0 END)
      comment: SKU-warehouse positions completely stocked out
    - name: unique_skus_in_stockout
      expr: COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END)
      comment: Distinct SKUs completely stocked out
      synonyms: [unique stockout SKUs]
    - name: avg_days_of_supply
      expr: ROUND(AVG(days_of_supply), 2)
      comment: Average days of supply for at-risk items
$$""",
    # 4. Supplier Performance
    f"""CREATE OR REPLACE VIEW {CAT}.supplier_procurement.supplier_performance_by_continent
WITH METRICS LANGUAGE YAML AS $$
  version: 1.1
  source: >
    SELECT po_id, supplier_continent, supplier_name, is_late,
           lead_time_variance_days, order_date, total_cost, quality_score
    FROM {CAT}.supplier_procurement.supplier_orders
    WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'
  dimensions:
    - name: supplier_continent
      expr: supplier_continent
      comment: Continent where supplier is located
      synonyms: [vendor continent, supplier region]
  measures:
    - name: total_purchase_orders
      expr: COUNT(*)
      comment: Total purchase orders last month
    - name: late_purchase_orders
      expr: SUM(CASE WHEN is_late THEN 1 ELSE 0 END)
      comment: Number of late purchase orders
    - name: supplier_late_rate_pct
      expr: ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 2)
      comment: Percentage of purchase orders (POs) that were late. When asked percentage of vendors delivered late, this is late POs / total POs per ORDER, not per distinct vendor.
      synonyms: [vendor late rate, vendor late delivery percentage, percentage of vendors delivered late, vendor late pct]
    - name: avg_lead_time_variance
      expr: ROUND(AVG(lead_time_variance_days), 2)
      comment: Average lead time variance in days
      synonyms: [lead time variance]
$$""",
]

for sql in metric_view_sqls:
    name = sql.split("VIEW ")[1].split("\n")[0].strip()
    try:
        spark.sql(sql)
        print(f"    \u2713 {name.split('.')[-1]}")
    except Exception as e:
        print(f"    \u2717 {name.split('.')[-1]}: {str(e)[:150]}")

# ── Step 2: Open Knowledge View — Cross-Domain Cost of Disruption ──
# Open Knowledge = a governed view that bridges data siloed across multiple
# domain schemas. Unlike Metric Views (single-domain, governed measures),
# Open Knowledge views make cross-domain joins queryable by any authorized
# agent — without requiring the Supervisor to orchestrate multiple agents.
print("\n  Step 2: Open Knowledge View — Cost of Disruption (cross-domain)...")
print("    ┌─ WHY THIS REQUIRES A VIEW (Open Knowledge category) ──────────────")
print("    │ CoD joins data from 4 domain schemas no single agent can see:")
print("    │   demand_analysis       → cancelled + backordered revenue")
print("    │   logistics_operations  → wasted freight on late shipments")
print("    │   supplier_procurement  → SLA penalties (allocated by region)")
print("    │   inventory_management  → stockout SKU counts for rev-at-risk")
print("    │")
print("    │ Without this view, answering 'What is our Cost of Disruption?'")
print("    │ requires the Supervisor to orchestrate 4 agents — slow, fragile,")
print("    │ and ungoverned. As Open Knowledge, it becomes a governed,")
print("    │ authoritative asset any authorized agent queries directly.")
print("    └─────────────────────────────────────────────────────────────────")
print("    Creating view...")

spark.sql(f"""CREATE OR REPLACE VIEW {CAT}.reporting.cost_of_disruption_by_region AS
WITH demand_impact AS (
    SELECT region,
        SUM(CASE WHEN order_status = 'Cancelled' THEN total_amount ELSE 0 END) AS cancelled_revenue,
        SUM(CASE WHEN order_status = 'Backordered' THEN total_amount ELSE 0 END) AS backordered_at_risk_revenue,
        SUM(total_amount) AS total_region_revenue
    FROM {CAT}.demand_analysis.sales_orders
    WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'
    GROUP BY region
),
logistics_impact AS (
    SELECT destination_region AS region,
        SUM(CASE WHEN is_late THEN shipping_cost ELSE 0 END) AS wasted_logistics_spend,
        SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_shipment_count
    FROM {CAT}.logistics_operations.shipments
    WHERE ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'
    GROUP BY destination_region
),
supplier_penalties AS (
    SELECT SUM(CASE WHEN is_breached THEN penalty_amount ELSE 0 END) AS total_sla_penalties
    FROM {CAT}.supplier_procurement.vendor_slas
),
all_late_shipments AS (
    SELECT SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS total_late_all_regions
    FROM {CAT}.logistics_operations.shipments
    WHERE ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'
),
inventory_stockouts AS (
    SELECT region, COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS unique_stockout_skus
    FROM {CAT}.inventory_management.inventory_ledger WHERE region IS NOT NULL GROUP BY region
)
SELECT d.region, d.cancelled_revenue, d.backordered_at_risk_revenue, l.wasted_logistics_spend,
    l.late_shipment_count,
    ROUND(sp.total_sla_penalties * (l.late_shipment_count * 1.0 / als.total_late_all_regions), 2) AS allocated_supplier_penalties,
    ROUND(d.cancelled_revenue + d.backordered_at_risk_revenue + l.wasted_logistics_spend
        + sp.total_sla_penalties * (l.late_shipment_count * 1.0 / als.total_late_all_regions), 2) AS total_cost_of_disruption,
    ROUND(d.cancelled_revenue + d.backordered_at_risk_revenue + l.wasted_logistics_spend, 2) AS revenue_at_risk_from_disruptions,
    d.total_region_revenue, i.unique_stockout_skus,
    ROUND((d.backordered_at_risk_revenue) / NULLIF(i.unique_stockout_skus, 0), 2) AS revenue_at_risk_per_stockout_sku,
    ROUND((d.cancelled_revenue + d.backordered_at_risk_revenue + l.wasted_logistics_spend
         + sp.total_sla_penalties * (l.late_shipment_count * 1.0 / als.total_late_all_regions))
        / NULLIF(d.total_region_revenue, 0), 2) AS disruption_to_revenue_ratio
FROM demand_impact d
JOIN logistics_impact l ON d.region = l.region
CROSS JOIN supplier_penalties sp CROSS JOIN all_late_shipments als
LEFT JOIN inventory_stockouts i ON d.region = i.region
""")
row = spark.sql(f"SELECT * FROM {CAT}.reporting.cost_of_disruption_by_region WHERE region = 'Western'").collect()[0]
print(f"    \u2713 CoD={row['total_cost_of_disruption']} RevAtRisk={row['revenue_at_risk_from_disruptions']} Ratio={row['disruption_to_revenue_ratio']}")

# ── Step 3: UC Governed Tags on all views ──
print("\n  Step 3: UC Governed Tags...")
tag_targets = [
    (f"{CAT}.logistics_operations.delivery_performance_by_region",
     {"domain": "logistics", "metric_type": "delivery", "data_quality": "authoritative"}),
    (f"{CAT}.demand_analysis.revenue_comparison_by_region",
     {"domain": "demand", "metric_type": "revenue", "time_granularity": "calendar_month"}),
    (f"{CAT}.inventory_management.inventory_safety_stock_metrics",
     {"domain": "inventory", "metric_type": "safety_stock", "data_quality": "authoritative"}),
    (f"{CAT}.supplier_procurement.supplier_performance_by_continent",
     {"domain": "supplier", "metric_type": "performance"}),
    (f"{CAT}.reporting.cost_of_disruption_by_region",
     {"domain": "financial_impact", "metric_type": "composite_cross_domain", "data_quality": "authoritative"}),
]
for table, tags in tag_targets:
    tag_pairs = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
    try:
        spark.sql(f"ALTER TABLE {table} SET TAGS ({tag_pairs})")
        print(f"    \u2713 {table.split('.')[-1]}")
    except Exception as e:
        print(f"    \u2717 {table.split('.')[-1]}: {str(e)[:100]}")

# Schema-level domain tags
print("\n  Step 4: Schema-level domain tags...")
for schema, domain in [(f"{CAT}.demand_analysis", "demand"), (f"{CAT}.inventory_management", "inventory"),
                        (f"{CAT}.logistics_operations", "logistics"), (f"{CAT}.supplier_procurement", "supplier"),
                        (f"{CAT}.reporting", "executive")]:
    try:
        spark.sql(f"ALTER SCHEMA {schema} SET TAGS ('domain' = '{domain}', 'business_unit' = 'supply_chain')")
        print(f"    \u2713 {schema.split('.')[-1]} -> {domain}")
    except Exception as e:
        print(f"    \u2717 {schema.split('.')[-1]}: {str(e)[:80]}")

# ── Step 5: Add metric views + CoD to Genie Agents ──
print("\n  Step 5: Adding metric views + CoD to Genie Agents...")
agent_tables = {
    "logistics":  [f"{CAT}.logistics_operations.delivery_performance_by_region"],
    "demand":     [f"{CAT}.demand_analysis.revenue_comparison_by_region"],
    "inventory":  [f"{CAT}.inventory_management.inventory_safety_stock_metrics",
                   f"{CAT}.reporting.cost_of_disruption_by_region"],
    "supplier":   [f"{CAT}.supplier_procurement.supplier_performance_by_continent"],
    "executive":  [f"{CAT}.reporting.cost_of_disruption_by_region"],
}
cod_instr = """\n\ncost_of_disruption_by_region view has pre-computed cross-domain metrics by region:\n- total_cost_of_disruption, revenue_at_risk_from_disruptions, revenue_at_risk_per_stockout_sku, disruption_to_revenue_ratio\nUse this for ANY question about Cost of Disruption, revenue at risk, or disruption ratios."""

for agent_name, new_tables in agent_tables.items():
    space_id = spaces.get(agent_name)
    if not space_id: continue
    resp = requests.get(f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true", headers=headers)
    ss = json.loads(resp.json().get("serialized_space", "{}"))
    tables = ss.get("data_sources", {}).get("tables", [])
    existing = {t["identifier"] for t in tables}
    added = [t for t in new_tables if t not in existing]
    if added:
        for t in added: tables.append({"identifier": t})
        tables.sort(key=lambda t: t["identifier"])
        ss["data_sources"]["tables"] = tables
        if agent_name in ("executive", "inventory"):
            instrs = ss.get("instructions", {}).get("text_instructions", [])
            if instrs:
                instrs[0]["content"].append(cod_instr)
                ss["instructions"] = {"text_instructions": instrs}
        ensure_sorted_payload(ss)  # ensure example_question_sqls stay sorted
        requests.patch(f"{host}/api/2.0/genie/spaces/{space_id}", headers=headers,
                       json={"serialized_space": json.dumps(ss)})
        print(f"    \u2713 {agent_name}: +{', '.join(t.split('.')[-1] for t in added)}")
    else:
        print(f"    (skip) {agent_name}: already present")

print("\n\u2705 Iteration 2: 4 UC Metric Views + Governed Tags + 1 Open Knowledge View")
iter2_passed, iter2_failed, iter2_errors, iter2_details = test_all_metrics("After Iteration 2")

# --- Comprehensive Prompt Benchmark: After Iteration 2 ---
comp_iter2 = run_comprehensive_benchmark("After Iteration 2 (metric views + CoD)")
all_comp_results["After Iteration 2"] = comp_iter2

# COMMAND ----------

# DBTITLE 1,VISUAL: After Iteration 2
# Re-plot dashboard with latest results (including Iteration 2)
plot_test_dashboard(all_stage_results, all_comp_results if 'all_comp_results' in dir() else None)

# ── Provenance Analysis: Extracted from EXISTING test results (no API calls) ──
current_stage = list(all_stage_results.keys())[-1] if all_stage_results else "Unknown"
results = all_stage_results.get(current_stage, [])

if results:
    ran_stages = set(all_stage_results.keys())
    iter_actually_ran = {
        'Iter 1': any('iteration 1' in s.lower() or 'iter 1' in s.lower() or 'iter1' in s.lower() for s in ran_stages),
        'Iter 2': any('iteration 2' in s.lower() or 'iter 2' in s.lower() or 'iter2' in s.lower() for s in ran_stages),
        'Iter 3': any('iteration 3' in s.lower() or 'iter 3' in s.lower() or 'iter3' in s.lower() for s in ran_stages),
    }
    def relabel_tier(raw_tier):
        if raw_tier == 'GUESSED':
            return 'Guessed (threshold)'
        if raw_tier in iter_actually_ran and not iter_actually_ran[raw_tier]:
            return f'Guessed ({raw_tier})'
        return raw_tier
    def _confidence_from_tier(tier):
        if tier in ('Iter 2', 'Iter 3') or tier.startswith('Iter 3'): return 'DETERMINISTIC'
        if tier == 'Iter 1': return 'HEURISTIC'
        if tier.startswith('Guessed'): return 'GUESSED'
        if tier == 'N/A': return 'N/A'
        return 'INFERRED'

    print("\n" + "="*90)
    print(f"  PROVENANCE ANALYSIS: {current_stage}")
    print(f"  Extracted from test results — no additional API calls")
    print("="*90)

    current_group = ""
    group_names = {'A':'LOGISTICS','B':'DEMAND','C':'INVENTORY','D':'SUPPLIER',
                   'E':'CROSS-DOMAIN','F':'INDIRECT','H':'HARD','G':'FISCAL','P':'CRITICAL'}
    for r in results:
        gid = r['id'][:1]
        if gid != current_group:
            current_group = gid
            print(f"\n  ── {group_names.get(gid, gid)} {'─'*80}")
        icon = '✅' if r['verdict'] == 'PASS' else ('❌' if r['verdict'] == 'FAIL' else '⚠️')
        prov = r.get('provenance', 'Unknown')
        tier = relabel_tier(r.get('prov_iter', '?'))
        conf = _confidence_from_tier(tier)
        gt = r.get('expected', '?')
        found = r.get('found') if r.get('found') is not None else r.get('closest')
        found_str = f"{found}" if found is not None else "—"
        print(f"  {icon} {r['id']:<5} {r['verdict']:<5}  gt={gt:<14}  found={found_str:<14}  [{tier}] {prov}")
        print(f"         Confidence: {conf}")

    prov_tiers = {}
    for r in results:
        tier = relabel_tier(r.get('prov_iter', 'Unknown'))
        prov_tiers.setdefault(tier, []).append(r)

    print(f"\n{'='*90}")
    print(f"  PROVENANCE SUMMARY BY TIER")
    print(f"{'='*90}")
    tier_order = ['Raw Table', 'Guessed (Iter 1)', 'Guessed (Iter 2)', 'Guessed (Iter 3)',
                  'Guessed (threshold)', 'Iter 1', 'Iter 2', 'Iter 3', 'Iter 3 (inferred)', 'N/A']
    for tier in tier_order:
        if tier not in prov_tiers: continue
        tests = prov_tiers[tier]
        p = sum(1 for t in tests if t['verdict'] == 'PASS')
        f = sum(1 for t in tests if t['verdict'] == 'FAIL')
        pct = 100 * p // max(len(tests), 1)
        ids = ', '.join(t['id'] for t in tests)
        print(f"  {tier:42s}: {len(tests):2d} tests -> {p:2d} PASS ({pct}%), {f:2d} FAIL  [{ids}]")

    conf_order = ['DETERMINISTIC', 'HEURISTIC', 'INFERRED', 'GUESSED', 'N/A']
    conf_counts = {}
    for r in results:
        tier = relabel_tier(r.get('prov_iter', 'Unknown'))
        conf = _confidence_from_tier(tier)
        conf_counts.setdefault(conf, {'pass': 0, 'fail': 0})
        if r['verdict'] == 'PASS': conf_counts[conf]['pass'] += 1
        else: conf_counts[conf]['fail'] += 1
    print(f"\n  REASONING CONFIDENCE DISTRIBUTION:")
    for conf in conf_order:
        if conf in conf_counts:
            c = conf_counts[conf]
            print(f"    {conf:20s}: {c['pass']+c['fail']:2d} tests ({c['pass']} PASS, {c['fail']} FAIL)")

    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch
    fig, (ax_prov, ax_llm) = plt.subplots(1, 2, figsize=(18, 5))

    # ── Panel 1: Per-Domain Group PASS/FAIL ──
    group_order = ['A','B','C','D','E','F','H','G','P']
    grp_name_map = {'A':'Logistics (A)','B':'Demand (B)','C':'Inventory (C)','D':'Supplier (D)',
                     'E':'Cross-Domain (E)','F':'Indirect (F)','H':'Hard (H)','G':'Fiscal (G)','P':'Critical (P)'}
    grp_pass, grp_fail, grp_labels = [], [], []
    for g in group_order:
        tests = [r for r in results if r['id'].startswith(g)]
        if not tests: continue
        p = sum(1 for t in tests if t['verdict'] == 'PASS')
        f = len(tests) - p
        grp_labels.append(grp_name_map.get(g, g))
        grp_pass.append(p)
        grp_fail.append(f)
    y_pos = range(len(grp_labels))
    ax_prov.barh(y_pos, grp_pass, color='#2e7d32', edgecolor='white', label='PASS')
    ax_prov.barh(y_pos, grp_fail, left=grp_pass, color='#d32f2f', edgecolor='white', label='FAIL')
    ax_prov.set_yticks(y_pos)
    ax_prov.set_yticklabels(grp_labels, fontsize=10)
    ax_prov.set_xlabel('Test Count')
    ax_prov.set_title(f'PASS/FAIL by Domain Group ({current_stage})', fontsize=11, fontweight='bold')
    ax_prov.legend(loc='lower right', fontsize=9)
    ax_prov.invert_yaxis()
    for i, (p, f) in enumerate(zip(grp_pass, grp_fail)):
        total = p + f
        ax_prov.text(total+0.3, i, f'{total} ({p}P/{f}F)', va='center', fontsize=9, fontweight='bold')
    ax_prov.set_xlim(0, max(p+f for p, f in zip(grp_pass, grp_fail)) + 6)

    cat_labels, cat_pass, cat_fail = [], [], []
    cat_colors = {'DETERMINISTIC':'#1b5e20','HEURISTIC':'#689f38','INFERRED':'#f9a825',
                  'GUESSED':'#b71c1c','N/A':'#757575'}
    for conf in conf_order:
        if conf not in conf_counts: continue
        c = conf_counts[conf]
        cat_labels.append(conf)
        cat_pass.append(c['pass'])
        cat_fail.append(c['fail'])
    y_pos2 = range(len(cat_labels))
    bar_colors = [cat_colors.get(c, '#757575') for c in cat_labels]
    bars_total = [p+f for p, f in zip(cat_pass, cat_fail)]
    ax_llm.barh(y_pos2, cat_pass, color=bar_colors, edgecolor='white', alpha=0.9)
    ax_llm.barh(y_pos2, cat_fail, left=cat_pass, color=bar_colors, edgecolor='white', alpha=0.3)
    ax_llm.set_yticks(y_pos2)
    ax_llm.set_yticklabels(cat_labels, fontsize=10)
    ax_llm.set_xlabel('Test Count')
    ax_llm.set_title(f'Reasoning Confidence: Is This Answer Repeatable? ({current_stage})', fontsize=11, fontweight='bold')
    ax_llm.invert_yaxis()
    for i, (p, f) in enumerate(zip(cat_pass, cat_fail)):
        suffix = '' if f == 0 else f' ({f}F)'
        ax_llm.text(p+f+0.3, i, f'{p+f}{suffix}', va='center', fontsize=9, fontweight='bold')
    ax_llm.set_xlim(0, max(bars_total) + 5 if bars_total else 10)
    ax_llm.legend(handles=[Patch(facecolor='#1b5e20', label='DETERMINISTIC'),
        Patch(facecolor='#689f38', label='HEURISTIC'),
        Patch(facecolor='#f9a825', label='INFERRED'),
        Patch(facecolor='#b71c1c', label='GUESSED')],
        loc='lower right', fontsize=7)
    plt.tight_layout()
    plt.show()

if 'assumptions' in dir():
    plot_classification_dashboard(all_stage_results, assumptions)

# COMMAND ----------

# DBTITLE 1,Iteration 3 Approach: fiscal_targets + SQL Functions + UC Pages (Governance)
# MAGIC %md
# MAGIC ## Iteration 3: fiscal_targets + SQL Functions + UC Pages (Governance Layer)
# MAGIC
# MAGIC **UC Features Used:** Reference Table (`fiscal_targets`), SQL Functions (5 threshold functions), UC Domain + UC Pages (Discover page governance)
# MAGIC
# MAGIC **Goal:** Make ALL 45 tests fully **deterministic**. Fix the remaining failures caused by missing business governance context (fiscal calendar, critical thresholds) and stabilize any non-deterministic baseline passes.
# MAGIC
# MAGIC ### What This Fixes
# MAGIC
# MAGIC | Failure | Root Cause | Fix (Actual Mechanism) |
# MAGIC | --- | --- | --- |
# MAGIC | G01, G02 | Q1 target = 92% — not in any base table; agent assumes calendar quarters | **`fiscal_targets` TABLE** added to Executive agent's data sources |
# MAGIC | P01 | Logistics Risk — agent can't guess multi-condition rule | **SQL Function**: `get_critical_delay_shipments()` returns `delay_days >= 5 AND total_weight_kg > 800` (gt=176) |
# MAGIC | P02 | Demand Anomaly — agent can't guess 3-condition rule | **SQL Function**: `get_critical_accuracy_forecasts()` returns `quantity >= 8 AND unit_price < 30 AND channel='Online'` (gt=77) |
# MAGIC | P03 | Inventory Risk — agent can't guess triple-band rule | **SQL Function**: `get_critical_supply_positions()` returns `days_of_supply BETWEEN 1 AND 11 AND below_ss=true AND on_hand > 0` (gt=106) |
# MAGIC | P04 | Procurement Quality — needs BOTH score < 75 AND ltv > 12 (neither alone gives 11) | **SQL Function**: `get_critical_quality_orders()` returns `quality_score < 75 AND lead_time_variance_days > 12` (gt=11) |
# MAGIC | P05 | Executive Disruption — needs ALL 3 conditions (any pair gives wrong answer) | **SQL Function**: `get_critical_disruption_regions()` returns `composite_risk_score < 55 AND ltv > 8 AND penalty > 80K` (gt=3) |
# MAGIC
# MAGIC ### PROVEN FINDING: Genie Agents CANNOT Access UC Pages
# MAGIC
# MAGIC This is the most important finding of Iteration 3:
# MAGIC
# MAGIC * **Genie One** (the standalone chat) **CAN** read UC Pages. Test confirmed: asked about "critical delay", Genie One found the Page, read the threshold, returned correct answer with citation.
# MAGIC * **Genie Agents** (used via API / Supervisor) **CANNOT** read UC Pages. The agent explicitly said: *"I don't have direct access to those pages in this context."*
# MAGIC * **Supervisor Agent** has no tool type for UC Pages. No `page` or `domain` tool exists in the Supervisor tool registry.
# MAGIC
# MAGIC Therefore:
# MAGIC * **G01/G02** are fixed by the `fiscal_targets` TABLE — not by UC Pages
# MAGIC * **P01-P05** are fixed by **SQL Functions** added to agent data sources — not by UC Pages
# MAGIC * UC Pages serve as **human-facing governance documentation** on the Discover page
# MAGIC
# MAGIC ### SQL Functions (Workaround for UC Pages)
# MAGIC
# MAGIC Since agents cannot read Pages, we create **table-valued SQL Functions** that encode the same thresholds. Each function returns the concept, threshold column, operator, value, definition, and the pre-computed result count.
# MAGIC
# MAGIC When added to an agent's data sources, the agent can call these functions to get the governed threshold instead of guessing.
# MAGIC
# MAGIC ### `fiscal_targets` Reference Table
# MAGIC
# MAGIC This table provides **queryable data** for the Executive agent. It encodes the fiscal calendar and service-level targets.
# MAGIC
# MAGIC | fiscal_quarter | fiscal_year | calendar_months | service_level_target_pct |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Q1 | 2027 | Jul 2026, Aug 2026, Sep 2026 | 92.0 |
# MAGIC | Q2 | 2027 | Oct 2026, Nov 2026, Dec 2026 | 93.0 |
# MAGIC | Q3 | 2027 | Jan 2027, Feb 2027, Mar 2027 | **95.0** |
# MAGIC | Q4 | 2027 | Apr 2027, May 2027, Jun 2027 | 94.0 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### UC Domain + Pages (Created on the Discover page)
# MAGIC
# MAGIC These persist through teardown (governance layer, not data layer):
# MAGIC
# MAGIC * **Domain:** "Supply Chain Operations" — groups all 5 schemas
# MAGIC * **Page 1 — Fiscal Calendar & Targets:** July FY start. Q1=Jul-Sep (current quarter), Q3=Jan-Mar. Current quarter (Q1) service-level target = 92.0%. Reference date: Sept 1, 2026. Last month = August 2026.
# MAGIC * **Page 2 — Cross-Domain Metric Definitions:** Vendor Late Rate = late POs / total POs per ORDER (75.0%), NEVER per distinct vendor (83.33%). OTD rate (94.57%) ≠ supplier late rate (75%). CoD formula definition.

# COMMAND ----------

# DBTITLE 1,PRE-STEP: Create fiscal_targets reference table (needed for UC Page related assets)
# ============================================================
# PRE-STEP: Create fiscal_targets reference table
# Must exist BEFORE the manual step so the Discover page can
# link it as a Related Asset on UC Page 1.
# ============================================================
import json

print("="*80)
print("  PRE-STEP: Create fiscal_targets reference table")
print("="*80)
CAT = CATALOG

# Drop and recreate with GENERIC comments — no fiscal calendar explanation.
# The UC Page 'Fiscal Calendar & Targets' should be the ONLY source of
# fiscal calendar context. Column comments must NOT spoon-feed the agent.
spark.sql(f"DROP TABLE IF EXISTS {CAT}.reporting.fiscal_targets")
spark.sql(f"""CREATE TABLE {CAT}.reporting.fiscal_targets (
    fiscal_quarter STRING COMMENT 'Fiscal quarter identifier (Q1-Q4)',
    fiscal_year INT COMMENT 'Fiscal year',
    calendar_months STRING COMMENT 'Calendar months covered by this quarter',
    service_level_target_pct DOUBLE COMMENT 'Target service level percentage for this quarter',
    on_time_delivery_target_pct DOUBLE COMMENT 'Target on-time delivery rate for this quarter',
    fill_rate_target_pct DOUBLE COMMENT 'Target fill rate for this quarter',
    description STRING COMMENT 'Quarter description'
) USING DELTA
COMMENT 'Quarterly performance targets by fiscal quarter and year.'
""")
spark.sql(f"""INSERT INTO {CAT}.reporting.fiscal_targets VALUES
    ('Q1', 2027, 'Jul 2026, Aug 2026, Sep 2026', 92.0, 85.0, 90.0, 'Ramp-up quarter'),
    ('Q2', 2027, 'Oct 2026, Nov 2026, Dec 2026', 93.0, 88.0, 92.0, 'Holiday season quarter'),
    ('Q3', 2027, 'Jan 2027, Feb 2027, Mar 2027', 95.0, 92.0, 95.0, 'Peak performance quarter'),
    ('Q4', 2027, 'Apr 2027, May 2027, Jun 2027', 94.0, 90.0, 93.0, 'Wind-down quarter')
""")
result = spark.sql(f"SELECT fiscal_quarter, service_level_target_pct, calendar_months FROM {CAT}.reporting.fiscal_targets WHERE fiscal_quarter = 'Q1'").collect()
print(f"  \u2713 fiscal_targets created: Q1 = {result[0]['service_level_target_pct']}% ({result[0]['calendar_months']})")
print(f"  \u2713 Table now available as Related Asset for UC Page 1 on Discover page")

display(spark.sql(f"SELECT * FROM {CAT}.reporting.fiscal_targets ORDER BY fiscal_quarter"))

# COMMAND ----------

# DBTITLE 1,KERNEL RECOVERY: Repopulate spaces + functions after kernel restart
# ============================================================
# KERNEL RECOVERY — run this if the kernel restarted mid-session.
# Re-establishes: spaces, ask_genie, extract_from_msg,
# find_value_in_text, FAILING_TESTS, test_failing_metrics,
# assumptions, without re-running the 30-min baseline.
# Safe to skip if the kernel is warm (cell 7 already ran).
# ============================================================

# ── WARM-KERNEL GUARD ─────────────────────────────────────────────────────
# If cell 7 already ran, this cell would DOWNGRADE ask_genie
# (to old start-conversation API) and overwrite the assumptions list.
# The guard preserves cell 7's state when the kernel is warm.
_KERNEL_WARM = (
    'assumptions' in dir() and len(assumptions) >= 45
    and 'spaces' in dir() and isinstance(spaces, dict) and len(spaces) >= 5
    and 'ask_genie' in dir() and 'test_all_metrics' in dir()
    and 'detect_provenance' in dir()
)
if _KERNEL_WARM:
    print("  \u23ed KERNEL IS WARM — skipping recovery (cell 7 already loaded 45 tests + all functions)")
    print(f"    assumptions: {len(assumptions)} tests | spaces: {list(spaces.keys())}")
    print(f"    Functions: ask_genie, detect_provenance, test_all_metrics — all present")
# Save cell 7's ask_genie (uses Agent Mode API) before recovery might overwrite it
_saved_ask_genie = ask_genie if 'ask_genie' in dir() and _KERNEL_WARM else None

import time, requests, json, re

# --- Re-discover Genie spaces (find the LATEST iteration's agents) ---
if 'spaces' not in dir() or not spaces or len(spaces) < 5:
    print("  Discovering Genie Agent spaces (latest iteration first)...")
    if 'all_spaces' not in dir():
        all_spaces = {}
    for _label in ["Iteration 3", "Iteration 2", "Iteration 1", "Baseline"]:
        _found = discover_agents_by_label(_label)
        if len(_found) >= 5:
            spaces = _found
            all_spaces[_label] = dict(_found)
            print(f"  Found {_label} agents:")
            for name, sid in spaces.items():
                print(f"    {name:15s} -> {sid}")
            break
    else:
        print("  ⚠ No complete set of SC agents found")
else:
    print("  spaces already in kernel — skipping discovery")

# --- Re-define core functions (same as cell 7) ---
def ask_genie(space_id, question, timeout_secs=120):
    conv = requests.post(f"{host}/api/2.0/genie/spaces/{space_id}/start-conversation",
                         headers=headers, json={"content": question})
    if conv.status_code != 200:
        return {"error": f"start failed: {conv.status_code} {conv.text[:200]}"}
    conv_id = conv.json().get("conversation_id")
    msg_id = conv.json().get("message_id")
    for _ in range(timeout_secs // 5):
        time.sleep(5)
        poll = requests.get(f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}", headers=headers)
        if poll.status_code != 200: continue
        msg = poll.json()
        if msg.get("status") in ["COMPLETED", "FAILED", "CANCELLED"]: return msg
    return {"error": "timeout"}

def extract_from_msg(msg):
    narration, sql, rows, cols = "", None, [], []
    for att in msg.get("attachments", []):
        txt = att.get("text", {}).get("content", "")
        if txt: narration += txt + "\n"
        q = att.get("query", {})
        if q.get("query") and not sql: sql = q["query"]
    qr = msg.get("query_result")
    if qr:
        cols = [c.get("name", "") for c in qr.get("columns", [])]
        for row in qr.get("data", {}).get("data_array", []):
            rows.append(dict(zip(cols, row)))
    return sql, narration.strip(), rows, cols

def find_value_in_text(text, expected):
    target = round(abs(float(expected)), 2)
    best_diff, closest = float('inf'), None
    for m in re.finditer(r'-?[\d,]+\.?\d*', text):
        try: num = float(m.group().replace(',', ''))
        except ValueError: continue
        rounded = round(abs(num), 2)
        if rounded == target: return True, num, num
        diff = abs(rounded - target)
        if diff < best_diff: best_diff, closest = diff, num
    return False, None, closest

# Restore cell 7's ask_genie if kernel was warm (don't downgrade to old API)
if _saved_ask_genie is not None:
    ask_genie = _saved_ask_genie
    print("  \u2705 Restored cell 7's ask_genie (Agent Mode API — not overwritten by recovery)")

# --- Full assumptions list (45 tests — same as cell 7) ---
# GUARD: Only overwrite if kernel is cold
_recovery_assumptions = [
    ("A01","logistics","What is the on-time delivery rate for Western region shipments in August 2026?",5.43,"MV: on_time_delivery_rate","Metric View (Iter 2)"),
    ("A02","logistics","What is the late delivery rate for Western region shipments last month?",94.57,"MV: late_delivery_rate","Metric View (Iter 2)"),
    ("A03","logistics","What is the average delay in days for late shipments in the Western region last month?",2.94,"MV: avg_delay_days","Metric View (Iter 2)"),
    ("A04","logistics","How many total shipments went to the Western region in August?",1086,"MV: total_shipments","Metric View (Iter 2)"),
    ("A05","logistics","How many late shipments went to the Western region last month?",1027,"MV: late_shipments","Metric View (Iter 2)"),
    ("A06","logistics","What is the total wasted freight on late shipments in Western region last month?",2484985.57,"MV: wasted_freight_cost","Metric View (Iter 2)"),
    ("B01","demand","What is the total revenue for the Western region in August 2026?",3341062.58,"MV: revenue_last_month","Metric View (Iter 2)"),
    ("B02","demand","What was the total revenue for the Western region in July 2026?",4581392.70,"MV: revenue_prior_month","Metric View (Iter 2)"),
    ("B03","demand","What is the revenue change in dollars for Western region month-over-month?",  -1240330.12,"MV: revenue_change_dollars","Metric View (Iter 2)"),
    ("B04","demand","What is the percentage change in revenue for Western region last month vs prior month?",-27.07,"MV: revenue_change_pct","Metric View (Iter 2)"),
    ("C01","inventory","How many inventory positions are below safety stock in the Western region?",109,"MV: positions_below_safety_stock","Metric View (Iter 2)"),
    ("C02","inventory","How many unique SKUs are below safety stock in the Western region?",61,"MV: unique_skus_below_safety","Metric View (Iter 2)"),
    ("C03","inventory","How many stockout positions are there in the Western region?",35,"MV: stockout_positions","Metric View (Iter 2)"),
    ("C04","inventory","How many unique SKUs are completely stocked out in the Western region?",33,"MV: unique_skus_in_stockout","Metric View (Iter 2)"),
    ("C05","inventory","What is the average days of supply for at-risk items in the Western region?",0.96,"MV: avg_days_of_supply","Metric View (Iter 2)"),
    ("D01","supplier","How many total purchase orders were placed in August 2026?",48,"MV: total_purchase_orders (overall)","Metric View (Iter 2)"),
    ("D02","supplier","How many purchase orders were late last month?",36,"MV: late_purchase_orders (overall)","Metric View (Iter 2)"),
    ("D03","supplier","What percentage of purchase orders were late last month?",75.00,"MV: supplier_late_rate_pct (overall)","Metric View (Iter 2)"),
    ("D04","supplier","What is the average lead time variance in days for all suppliers last month?",8.69,"MV: avg_lead_time_variance (overall)","Metric View (Iter 2)"),
    ("D05","supplier","What percentage of purchase orders from Asia suppliers were late in August?",100.00,"MV: supplier_late_rate_pct (Asia)","Metric View (Iter 2)"),
    ("D06","supplier","What is the average lead time variance for Asia suppliers last month?",13.67,"MV: avg_lead_time_variance (Asia)","Metric View (Iter 2)"),
    ("D07","supplier","How many purchase orders did we place with Asia suppliers in August?",30,"MV: total_purchase_orders (Asia)","Metric View (Iter 2)"),
    ("E01","executive","What is our current fill rate?",80.70,"fill rate = service_level_pct","Baseline"),
    ("E02","supplier","What are the total vendor SLA penalties we incurred?",1185043.10,"SLA penalty = SUM(penalty_amount)","Baseline"),
    ("E03","executive","What is the total Cost of Disruption for the Western region last month?",3757298.31,"Cross-domain metric (no table exists)","CoD View (Iter 2)"),
    ("F01","demand","Show me the total revenue for the West region last month",3341062.58,"West -> Western region mapping","Instruction"),
    ("F02","supplier","What percentage of vendors delivered late last month?",75.00,"Vendor late % (per-order vs per-vendor ambiguity)","Certified Query (Iter 1)"),
    ("F03","demand","How many Western region orders were fulfilled last month?",1342,"Fulfilled order count","Direct"),
    ("F04","demand","What is the total cancelled revenue in Western region in August?",179419.26,"Cancelled revenue","Direct"),
    ("F05","demand","How many orders were backordered in Western region last month?",275,"Backordered order count","Direct"),
    ("F06","demand","Which product family had the largest revenue decline in Western region last month vs prior month?",349062.88,"Worst product family decline (Home Goods)","Certified Query (Iter 1)"),
    ("H01","supplier","What is the average lead time variance for Europe suppliers last month?",0.38,"Wrong table: supplier_lead_times vs supplier_orders (Europe)","Comment (Iter 1) / Metric View (Iter 2)"),
    ("H02","supplier","What is the average lead time variance for North America suppliers last month?",0.40,"Wrong table: supplier_lead_times vs supplier_orders (NA)","Comment (Iter 1) / Metric View (Iter 2)"),
    ("H03","demand","What is the order fulfillment rate for Western region last month?",71.23,"Status ambiguity: Fulfilled only vs incl Partially_Fulfilled","Comment (Iter 1) / Certified Query (Iter 1)"),
    ("H04","demand","What percentage of Western region orders were only partially fulfilled last month?",9.02,"Status value: Partially_Fulfilled exact definition","Comment (Iter 1)"),
    ("H05","executive","What is the total revenue at risk from supply chain disruptions in Western region including cancelled revenue, backordered revenue, and wasted freight combined?",3138569.66,"Cross-domain: demand + logistics (no single agent has both)","CoD View (Iter 2)"),
    ("H06","inventory","What is the average revenue at risk per stockout SKU in Western region?",14368.63,"Cross-domain: inventory stockouts + demand revenue","Metric View (Iter 2)"),
    ("H07","executive","What is our total cost of supply chain disruptions as a ratio of Western region revenue?",1.12,"Cross-domain: CoD / revenue ratio","CoD View (Iter 2)"),
    ("G01","executive","Are we going to miss our Q1 service-level targets?",92.0,"Q1 target (in fiscal_targets table, Q1=Jul-Sep fiscal)","Reference Table (Iter 3)"),
    ("G02","executive","What is our Q1 service-level target?",92.0,"Q1 target value (in fiscal_targets table)","Reference Table (Iter 3)"),
    ("P01","logistics","How many shipments in August were flagged under the Logistics Risk Standards?",176,"Multi-condition: delay>=5 AND weight>800","UC Pages / SQL Function (Iter 3)"),
    ("P02","demand","How many Western region orders in August triggered a Demand Anomaly Alert?",77,"Multi-condition: qty>=8 AND price<30 AND Online","UC Pages / SQL Function (Iter 3)"),
    ("P03","inventory","How many inventory positions are classified as supply-risk under Inventory Standards?",106,"Multi-condition: dos 1-11 AND below_ss AND on_hand>0","UC Pages / SQL Function (Iter 3)"),
    ("P04","supplier","How many supplier orders last month fell below the Procurement Quality Minimum?",11,"Multi-condition: quality<75 AND ltv>12","UC Pages / SQL Function (Iter 3)"),
    ("P05","executive","How many suppliers exceeded the Executive Disruption Threshold?",3,"Multi-condition: risk<55 AND ltv>8 AND penalty>80K","UC Pages / SQL Function (Iter 3)"),
]

# --- FAILING_TESTS from known baseline results ---
# Baseline: 29/45 PASS, these 16 FAIL (from Sept 21 fresh build)
baseline_failing_ids = {"A03","A06","B04","D04","D06","E03","F02","H01","H02","H05","H06","H07","G02","P01","P02","P03"}
if not _KERNEL_WARM:
    assumptions = _recovery_assumptions
    FAILING_TESTS = [(aid, ak, q, exp) for aid, ak, q, exp, _, _ in assumptions if aid in baseline_failing_ids]
    print(f"  assumptions: reloaded ({len(assumptions)} tests)")
    print(f"  FAILING_TESTS: {len(FAILING_TESTS)} tests from baseline")
else:
    print(f"  \u23ed assumptions preserved ({len(assumptions)} tests) — not overwritten")
    print(f"  \u23ed FAILING_TESTS preserved ({len(FAILING_TESTS)} tests) — not overwritten")

# --- test_failing_metrics (same as cell 7 — with GT SQL + provenance + 4 return values) ---
def test_failing_metrics(label=""):
    """Run targeted test on previously-failing metrics.
    Shows full agent response (SQL, narration, result rows) for transparency.
    Returns (passed, failed, errors, details)."""
    print(f"\n{'='*90}")
    print(f"  TARGETED TEST: {len(FAILING_TESTS)} previously-failing metrics{f' \u2014 {label}' if label else ''}")
    print(f"{'='*90}")
    results = []
    for aid, agent_key, question, expected in FAILING_TESTS:
        space_id = spaces.get(agent_key)
        print(f"\n  {'\u2500'*86}")
        print(f"  {aid}: asking {agent_key}... (gt={expected})")
        print(f"  Q: {question}")
        msg = ask_genie(space_id, question)
        if "error" in msg:
            print(f"  \u274c ERROR: {msg['error']}")
            results.append((aid, "ERROR", expected, None, None, None, "N/A", "N/A"))
            continue
        sql, narration, rows, cols = extract_from_msg(msg)
        gt_sql = GT_QUERIES.get(aid)
        gt_note = GT_QUERY_NOTES.get(aid)
        # Show agent response for full transparency
        print_boxed_block("AGENT SQL", sql)
        print_boxed_block("GROUND TRUTH SQL", gt_sql or gt_note)
        if narration:
            print(f"  \u250c\u2500 NARRATION \u2500\u2500\u2500")
            for line in narration.split("\n"):
                print(f"  \u2502 {line}")
            print(f"  \u2514{'\u2500'*70}")
        if rows:
            print(f"  \u250c\u2500 RESULT ({len(rows)} rows) \u2500\u2500\u2500")
            for i, row in enumerate(rows[:10]):
                print(f"  \u2502 [{i}] {row}")
            if len(rows) > 10:
                print(f"  \u2502 ... ({len(rows)} total)")
            print(f"  \u2514{'\u2500'*70}")
        all_text = (narration or "") + " " + (sql or "")
        for row in rows:
            for val in row.values():
                all_text += f" {val}"
        match, found, closest = find_value_in_text(all_text, expected)
        prov_iter, prov_feature, prov_expl = detect_provenance(sql, aid)
        if match:
            print(f"\n  \u2705 PASS (gt={expected}, found={found})")
            print(f"  \u25b8 Used: [{prov_iter}] {prov_feature}")
            results.append((aid, "PASS", expected, found, None, sql, prov_feature, prov_iter))
        else:
            print(f"\n  \u274c FAIL (gt={expected}, closest={closest})")
            print(f"  \u25b8 Used: [{prov_iter}] {prov_feature}")
            results.append((aid, "FAIL", expected, None, closest, sql, prov_feature, prov_iter))
    passed = sum(1 for r in results if r[1] == "PASS")
    failed = sum(1 for r in results if r[1] == "FAIL")
    errs = sum(1 for r in results if r[1] == "ERROR")
    print(f"\n{'='*90}")
    n = len(FAILING_TESTS)
    print(f"  RESULTS: {passed}/{n} PASS | {failed} FAIL | {errs} ERROR")
    for r in results:
        aid, verdict = r[0], r[1]
        icon = {"PASS": "\u2705", "FAIL": "\u274c", "ERROR": "\u26a0\ufe0f"}[verdict]
        val = r[3] if r[3] is not None else r[4]
        prov_str = f"  [{r[7]}] {r[6]}" if verdict == "PASS" and len(r) > 6 else ""
        print(f"  {icon} {aid:<5} gt={r[2]:<14} {'found='+str(round(val,2)) if val else 'N/A':<24} {verdict}{prov_str}")
    # Store for flip tracking across iterations
    iteration_test_details[label] = results
    # Detect flips from previous iteration
    prev_labels = [k for k in iteration_test_details.keys() if k != label]
    if prev_labels:
        prev_results = iteration_test_details[prev_labels[-1]]
        prev_pass_ids = {r[0] for r in prev_results if r[1] == "PASS"}
        curr_pass_ids = {r[0] for r in results if r[1] == "PASS"}
        new_fixes = curr_pass_ids - prev_pass_ids
        regressions = prev_pass_ids - curr_pass_ids
        if new_fixes:
            print(f"\n  \u2b06 NEWLY FIXED in this iteration ({len(new_fixes)}):")
            for r in results:
                if r[0] in new_fixes and len(r) > 6:
                    print(f"    \u2705 {r[0]}: [{r[7]}] {r[6]}")
        if regressions:
            print(f"\n  \u2b07 REGRESSIONS ({len(regressions)}):")
            for aid in sorted(regressions):
                print(f"    \u274c {aid}: was PASS, now FAIL (non-determinism)")
    print(f"{'='*90}")
    return passed, failed, errs, results

# --- detect_provenance (reloaded — covers all 45 tests: MVs, CoD, fiscal_targets, P01-P05 SQL funcs) ---
def detect_provenance(sql, aid):
    """Detect which UC semantic feature the agent used based on its SQL.
    Returns (iteration_label, feature_name, explanation)."""
    if not sql:
        return ('N/A', 'No SQL', 'Agent did not generate SQL')
    sql_lower = sql.lower()
    # --- Iter 2: UC Metric Views ---
    mv_map = {
        'delivery_performance_by_region': ('Iter 2', 'Metric View: delivery_performance_by_region',
            'Pre-aggregated delivery metrics with governed MEASURE() semantics'),
        'revenue_comparison_by_region': ('Iter 2', 'Metric View: revenue_comparison_by_region',
            'Pre-computed MoM revenue comparison with governed measures'),
        'inventory_safety_stock_metrics': ('Iter 2', 'Metric View: inventory_safety_stock_metrics',
            'Governed safety stock aggregations (positions, SKUs, days-of-supply)'),
        'supplier_performance_by_continent': ('Iter 2', 'Metric View: supplier_performance_by_continent',
            'Governed supplier KPIs by continent with correct per-order semantics'),
    }
    for view, (il, feat, expl) in mv_map.items():
        if view in sql_lower:
            return (il, feat, expl)
    # --- Iter 2: Open Knowledge View (CoD) ---
    if 'cost_of_disruption_by_region' in sql_lower:
        return ('Iter 2', 'Open Knowledge View: cost_of_disruption_by_region',
                'Cross-domain CoD join (demand + logistics + supplier + inventory)')
    # --- Iter 3: Reference Table (fiscal_targets) ---
    if 'fiscal_targets' in sql_lower:
        return ('Iter 3', 'Reference Table: fiscal_targets',
                'Agent queries fiscal_targets table added to its data sources')
    # --- Iter 1: Column Comment guidance ---
    if aid in ('D04', 'D06', 'H01', 'H02') and 'supplier_orders' in sql_lower and 'lead_time_variance' in sql_lower:
        return ('Iter 1', 'Column Comment: supplier_orders.lead_time_variance_days',
                'Comment steered agent from supplier_lead_times to supplier_orders')
    if aid in ('H03', 'H04', 'F03'):
        check = sql_lower.replace('partially_fulfilled', '~~')
        if "'fulfilled'" in check or '"fulfilled"' in check:
            return ('Iter 1', 'Column Comment: order_status disambiguation',
                    'Comment defined Fulfilled vs Partially_Fulfilled distinction')
    # --- Iter 1: Example SQL Queries ---
    if aid == 'F02' and 'supplier_orders' in sql_lower and ('count(*)' in sql_lower or 'sum(case' in sql_lower):
        return ('Iter 1', 'Example SQL: vendor late rate per-order',
                'Example SQL taught per-ORDER rate (not per-vendor COUNT DISTINCT)')
    if aid == 'F06' and 'product_family' in sql_lower:
        return ('Iter 1', 'Example SQL: product family revenue decline',
                'Example SQL demonstrated MoM product family comparison')
    # --- Iter 3: G01/G02 — fiscal_targets table (NOT UC Pages) ---
    if aid in ('G01', 'G02'):
        if '92' in sql:
            return ('Iter 3', 'Reference Table: fiscal_targets',
                    'Agent found 92% Q1 target in fiscal_targets table (not from UC Page)')
    # --- Iter 3: SQL Functions — domain-specific policy thresholds ---
    # --- Iter 3: SQL Functions — definition-only (agent reads definition, writes own SQL) ---
    sql_func_map = {
        'P01': ('get_critical_delay_shipments', [
            ['delay_days'], ['total_weight_kg'],
            ['>= 5', '>=5'], ['> 800', '>800'],
        ]),
        'P02': ('get_critical_accuracy_forecasts', [
            ['quantity'], ['unit_price'], ['channel'],
            ['>= 8', '>=8'], ['< 30', '<30'],
        ]),
        'P03': ('get_critical_supply_positions', [
            ['days_of_supply'], ['below_safety_stock'],
            ['on_hand_qty', 'on_hand'],
            ['between 1 and 11'],
        ]),
        'P04': ('get_critical_quality_orders', [
            ['quality_score'], ['lead_time_variance'],
            ['< 75', '<75'], ['> 12', '>12'],
        ]),
        'P05': ('get_critical_disruption_regions', [
            ['composite_risk_score'], ['lead_time_variance'],
            ['total_penalty_usd'],
            ['< 55', '<55'], ['> 80000', '>80000'],
        ]),
    }
    if aid in sql_func_map:
        func_name, condition_groups = sql_func_map[aid]
        if func_name in sql_lower:
            return ('Iter 3', f'SQL Function: {func_name}()',
                    f'Agent called the SQL function directly')
        groups_matched = sum(
            1 for group in condition_groups
            if any(marker in sql_lower for marker in group)
        )
        total_groups = len(condition_groups)
        if groups_matched == total_groups:
            return ('Iter 3 (inferred)', f'SQL Function (inferred): all {total_groups} conditions match {func_name}',
                    f'Agent SQL matches ALL {total_groups} conditions from {func_name}() definition. '
                    f'API does not expose internal reasoning, but full multi-condition match '
                    f'is strong evidence the agent consulted the function.')
        elif groups_matched > 0:
            return ('GUESSED', f'Partial match ({groups_matched}/{total_groups} conditions vs {func_name})',
                    f'Only {groups_matched} of {total_groups} condition groups match.')
        return ('GUESSED', f'No condition match for {aid} vs {func_name}',
                f'Agent did not use any conditions from {func_name}() definition.')
    # --- Baseline: Raw table query ---
    return ('Raw Table', 'Direct query (no UC feature needed)',
            'Agent answered correctly from raw tables alone — no UC semantic feature required')

# Global: stores per-iteration test details for flip tracking
if 'iteration_test_details' not in dir():
    iteration_test_details = {}

if not _KERNEL_WARM:
    # Placeholders for iteration tracking (not available without full baseline rerun)
    iter1_passed = 8; iter2_passed = 12  # known from earlier session
    print("\n  \u2705 Kernel recovery complete — 45 tests + detect_provenance + all core functions reloaded")
else:
    print("\n  \u23ed Recovery skipped — kernel already warm with all 45 tests + Agent Mode API")

# COMMAND ----------

# DBTITLE 1,MANUAL STEP: Create UC Domain + Pages (before Iteration 3)
# MAGIC %md
# MAGIC ## ⏸️ MANUAL STEP: Create UC Domain + Pages
# MAGIC
# MAGIC > **Pause here.** Before running Iteration 3, create the following in the **Discover** page (Beta — UI only, no API yet).
# MAGIC >
# MAGIC > **Note:** The `fiscal_targets` table was created in the previous cell so it's available as a Related Asset.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Domain: `Supply Chain Operations`
# MAGIC
# MAGIC **Add all 5 schemas** from the `GAP_Demo_Dev` catalog:
# MAGIC * `demand_analysis`
# MAGIC * `inventory_management`
# MAGIC * `logistics_operations`
# MAGIC * `supplier_procurement`
# MAGIC * `reporting`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Page 1: `Fiscal Calendar & Targets`
# MAGIC
# MAGIC | Field | Value |
# MAGIC | --- | --- |
# MAGIC | **Synonyms** | Fiscal, Fiscal Year |
# MAGIC | **Description** | Fiscal Calendar for this domain of Supply chain |
# MAGIC | **Definition** | This organization uses a **July fiscal year start** (not January). Q1=Jul-Sep, Q2=Oct-Dec, **Q3=Jan-Mar** (NOT calendar Jul-Sep!), Q4=Apr-Jun. Current fiscal year: FY2027 (Jul 2026 – Jun 2027). |
# MAGIC | **Business Use** | Q1 service-level target = **92.0%**. The current reference date is **September 1, 2026**. "Last month" = **August 2026**. "Prior month" = **July 2026**. When a question mentions "August" without a year, ALWAYS use 2026. |
# MAGIC | **Related Assets** | `fiscal_targets`, `executive_kpis` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Page 2: `Cross-Domain Metric Definitions`
# MAGIC
# MAGIC | Field | Value |
# MAGIC | --- | --- |
# MAGIC | **Synonyms** | *(none)* |
# MAGIC | **Description** | Cross-Domain Metric Definitions |
# MAGIC | **Definition** | Resolves ambiguity between same-named metrics across domains. **Vendor Late Rate** = late POs / total POs computed **per ORDER** (= 75.0%). NEVER use COUNT(DISTINCT supplier_id) which gives per-vendor = 83.33%. "Percentage of vendors delivered late" is a BUSINESS TERM that means per-order, not per-distinct-vendor. |
# MAGIC | **Business Use** | CoD = wasted freight + cancelled rev + backordered rev + SLA penalties. OTD rate (94.57%) ≠ supplier late rate (75%). Fulfillment rate = only `order_status = 'Fulfilled'` (NOT `Partially_Fulfilled`). |
# MAGIC | **Related Assets** | `delivery_performance_by_region`, `cost_of_disruption_by_region`, `supplier_performance_by_continent`, `revenue_comparison_by_region` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 5 Domain-Specific Policy Definitions (NEW)
# MAGIC
# MAGIC > **Unguessable multi-condition rules.** Each domain defines a named POLICY with 2–3 conditions that no LLM can infer from general knowledge. The Genie agent for each domain can ONLY know the rule from its UC Page (or SQL function). Tests P01–P05 validate this.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Domain 2: `Logistics Operations` (schema: `logistics_operations`)
# MAGIC
# MAGIC **Page 3: `Logistics Risk Standards`**
# MAGIC
# MAGIC | Field | Value |
# MAGIC | --- | --- |
# MAGIC | **Synonyms** | logistics risk flag, flagged shipment |
# MAGIC | **Definition** | A shipment is **flagged under Logistics Risk Standards** when `delay_days >= 5` **AND** `total_weight_kg > 800`. Both conditions must be met — heavy shipments with significant delay. Minor delays or lightweight shipments do not trigger the flag. |
# MAGIC | **Business Use** | Flagged shipments require escalation to the VP of Logistics. Use `delay_days >= 5 AND total_weight_kg > 800` to identify flagged shipments. Neither condition alone is sufficient. |
# MAGIC | **Related Assets** | `shipments` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Domain 3: `Demand Analysis` (schema: `demand_analysis`)
# MAGIC
# MAGIC **Page 4: `Demand Quality Standards`**
# MAGIC
# MAGIC | Field | Value |
# MAGIC | --- | --- |
# MAGIC | **Synonyms** | demand anomaly, anomaly alert |
# MAGIC | **Definition** | A Western region order **triggers a Demand Anomaly Alert** when `quantity >= 8` **AND** `unit_price < 30` **AND** `channel = 'Online'`. This identifies high-volume, low-price online orders that signal unusual demand patterns. |
# MAGIC | **Business Use** | Anomaly orders trigger demand planning review. Use `quantity >= 8 AND unit_price < 30 AND channel = 'Online'` on `sales_orders` filtered to Western region. All three conditions must be met. |
# MAGIC | **Related Assets** | `sales_orders` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Domain 4: `Inventory Management` (schema: `inventory_management`)
# MAGIC
# MAGIC **Page 5: `Inventory Risk Classification`**
# MAGIC
# MAGIC | Field | Value |
# MAGIC | --- | --- |
# MAGIC | **Synonyms** | supply risk, inventory risk |
# MAGIC | **Definition** | An inventory position is **classified as supply-risk under Inventory Standards** when `days_of_supply` is **BETWEEN 1 AND 11** AND `below_safety_stock_flag = true` AND `on_hand_qty > 0`. This captures items that are running low but not yet stocked out. |
# MAGIC | **Business Use** | Supply-risk positions require emergency replenishment. Use `days_of_supply BETWEEN 1 AND 11 AND below_safety_stock_flag = true AND on_hand_qty > 0`. All three conditions define the risk band. |
# MAGIC | **Related Assets** | `inventory_ledger` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Domain 5: `Supplier Procurement` (schema: `supplier_procurement`)
# MAGIC
# MAGIC **Page 6: `Supplier Quality Standards`**
# MAGIC
# MAGIC | Field | Value |
# MAGIC | --- | --- |
# MAGIC | **Synonyms** | quality minimum, procurement quality |
# MAGIC | **Definition** | A supplier order **falls below the Procurement Quality Minimum** when `quality_score < 75` **AND** `lead_time_variance_days > 12`. Both marginal quality AND significant delivery variance must co-occur. The quality threshold 75 alone gives 19 orders; adding ltv > 12 narrows it to 11. Neither condition alone produces the correct count. |
# MAGIC | **Business Use** | Below-minimum orders trigger corrective action. Use `quality_score < 75 AND lead_time_variance_days > 12`. Quality < 75 alone = 19 (wrong). ltv > 12 alone = wrong. Both needed → 11. |
# MAGIC | **Related Assets** | `supplier_orders` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Domain 6: `Executive Reporting` (schema: `reporting`)
# MAGIC
# MAGIC **Page 7: `Executive Alert Thresholds`**
# MAGIC
# MAGIC | Field | Value |
# MAGIC | --- | --- |
# MAGIC | **Synonyms** | disruption threshold, executive alert |
# MAGIC | **Definition** | A supplier **exceeds the Executive Disruption Threshold** when `composite_risk_score < 55` **AND** `lead_time_variance > 8` **AND** `total_penalty_usd > 80000`. This triple condition identifies suppliers with compounding risk factors. Each pair of conditions gives a different wrong answer (score+ltv=4, score+penalty=5, score alone=6). |
# MAGIC | **Business Use** | Threshold-exceeding suppliers are flagged for board-level review. Use `composite_risk_score < 55 AND lead_time_variance > 8 AND total_penalty_usd > 80000` from `supply_chain_risk_scorecard`. All 3 conditions needed → 3. |
# MAGIC | **Related Assets** | `supply_chain_risk_scorecard` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > **🔑 PROVEN FINDING: Genie One vs Genie Agents vs Supervisor Agent**
# MAGIC >
# MAGIC > UC Pages are part of **Unity Catalog Semantics** and the **Genie Ontology**.
# MAGIC >
# MAGIC > **Genie One ✅ READS UC Pages.** Manual test confirmed: asking *"How many shipments in August were flagged under the Logistics Risk Standards?"* in Genie One produced:
# MAGIC > * Genie One found the glossary page **"Logistics Risk Standards"**
# MAGIC > * Read the definition: `delay_days >= 5 AND total_weight_kg > 800`
# MAGIC > * Executed both conditions → **176** (correct)
# MAGIC > * Cited the Page as source: **"Genie Ontology (1) → Logistics Risk Standards"**
# MAGIC >
# MAGIC > **Genie Agents ❌ CANNOT read UC Pages.** The agent explicitly stated:
# MAGIC > *"I need to consult the UC Pages under the 'Logistics Operations' domain... However, since I don't have direct access to those pages in this context, I'll provide counts for different delay thresholds..."*
# MAGIC >
# MAGIC > Genie Agents document their SQL-generation inputs as: datasets, column descriptions, synonyms, example SQL, instructions, and SQL expressions. **Pages are not a Genie Agent input.**
# MAGIC >
# MAGIC > **Supervisor Agent ❌ NO tool type for UC Pages.** Supported tools: `genie_space`, `dashboard`, `uc_function`, `table`, `knowledge_assistant`, `serving_endpoint`, `vector_search_index`, `volume`, `app`, `uc_connection`, `uc_mcp`, `databricks_web_search`, `function`. No `page` or `domain` tool exists. There is also **no public REST API** for reading UC Page content programmatically (Pages is Beta, UI-only on the Discover page).
# MAGIC >
# MAGIC > **SQL Function as Agent Data Source ✅ WORKS via both Agent UI and Agent Mode API.** Created `get_critical_delay_shipments()` returning multi-condition rule. Added to logistics agent via UI (Sources > SQL function).
# MAGIC > * **Agent UI**: Agent found the function, called it, read the definition (`delay_days >= 5 AND total_weight_kg > 800`), applied both conditions → **176** (correct!). Cited the function in its response.
# MAGIC > * **Agent Mode API** (`/api/2.0/genie/agents/{id}/responses`): Also calls function correctly → **176**. Both channels work.
# MAGIC > * The `serialized_space` API only exposes `tables` in `data_sources` — SQL functions can only be **added** via UI, not API. But once added, both UI and API can use them.
# MAGIC >
# MAGIC > **Workarounds** (all duplicate governance, defeating the single-source-of-truth purpose):
# MAGIC > * Knowledge Assistant with Page content → add as Supervisor tool
# MAGIC > * Delta table with Page content → add as Supervisor `table` tool (like `fiscal_targets`)
# MAGIC > * UC Function returning definitions → add as Supervisor `uc_function` tool
# MAGIC > * **SQL Function as Genie Agent source → WORKS via UI, fails via API** (best workaround!)
# MAGIC >
# MAGIC > **Bottom line:**
# MAGIC > * **Genie One** is the only channel that natively consumes **UC Pages**.
# MAGIC > * **SQL Functions** added as agent data sources work via **both Agent UI and Agent Mode API** (the best workaround for bridging Page definitions to agents). They can only be **added** via UI, but once added, both channels use them.
# MAGIC >
# MAGIC > **What we test:**
# MAGIC > * **G01/G02** → Fixed by adding the `fiscal_targets` TABLE to the Executive agent's data sources (cell 15, Step 2). The agent queries the table directly. UC Pages play no role — agents cannot read them.
# MAGIC > * **P01–P05** → Demonstrate the Genie One vs Genie Agent gap. These tests FAIL via the Agent API but P01 PASSES via Genie One (176, with citation to the Page). Multi-condition rules make guessing impossible.
# MAGIC >
# MAGIC > **Ensure Pages are Published** (not Draft) — Draft Pages are only visible to the owner in Genie One.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔧 MANUAL STEP 2: Add SQL Functions to Agent Data Sources
# MAGIC
# MAGIC > **After running cell 18** (which creates the SQL functions in the catalog), add each function to its agent via the UI.
# MAGIC >
# MAGIC > **KNOWN LIMITATION:** The `serialized_space` API does NOT support SQL functions in `data_sources`. Functions can ONLY be added via the Agent UI.
# MAGIC
# MAGIC For each agent, go to: **Agent Settings → Sources → SQL function → Add function**
# MAGIC
# MAGIC | Agent | SQL Function to Add |
# MAGIC | --- | --- |
# MAGIC | **SC Logistics** | `GAP_Demo_Dev.logistics_operations.get_critical_delay_shipments` |
# MAGIC | **SC Demand** | `GAP_Demo_Dev.demand_analysis.get_critical_accuracy_forecasts` |
# MAGIC | **SC Inventory** | `GAP_Demo_Dev.inventory_management.get_critical_supply_positions` |
# MAGIC | **SC Supplier** | `GAP_Demo_Dev.supplier_procurement.get_critical_quality_orders` |
# MAGIC | **SC Executive** | `GAP_Demo_Dev.reporting.get_critical_disruption_regions` |
# MAGIC
# MAGIC > **Expected behavior after adding:**
# MAGIC > * ✅ Agent UI queries: agent calls function, returns correct answer
# MAGIC > * ✅ Agent Mode API: also calls function correctly
# MAGIC > * Once added as source, P01–P05 should pass in both channels.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > **▶ Continue to cell 19 (Iteration 3 visual) after completing both manual steps.**

# COMMAND ----------

# DBTITLE 1,ITERATION 3: UC Pages (Governance Layer) + SQL Functions + fiscal_targets — SETUP
# ============================================================
# ITERATION 3: UC Domain + UC Pages (Governance Layer)
# UC Features: UC Domain (Discover page), UC Pages (glossary),
#              Reference Table (fiscal_targets — queryable data
#              backing the UC Pages governance definitions)
# Targets: F02 (vendor late rate ambiguity — resolved by Page 2),
#          G01, G02 (Q1 target = 92% — resolved by Page 1)
#
# KEY FINDING (PROVEN):
# UC Pages work via Genie One (402, cited 'Logistics Risk Standards')
# but NOT via Genie Agents (agent stated: 'I don't have direct
# access to those pages in this context').
# Supervisor Agent has NO tool type for UC Pages (no 'page' tool,
# no public REST API for Page content).
# Genie One is currently the ONLY channel that consumes UC Pages.
# G01/G02 are fixed by the fiscal_targets TABLE, not by Pages.
# P01-P05 document the Genie One vs Agent gap.
# ============================================================
import json

# Helper: ensure sortable arrays are sorted before any PATCH
def ensure_sorted_payload(ss):
    """Genie API rejects PATCHes if example_question_sqls or benchmarks aren't sorted by id."""
    if "instructions" in ss and "example_question_sqls" in ss.get("instructions", {}):
        ss["instructions"]["example_question_sqls"].sort(key=lambda x: str(x.get("id", "")))
    if "benchmarks" in ss and "questions" in ss.get("benchmarks", {}):
        ss["benchmarks"]["questions"].sort(key=lambda x: str(x.get("id", "")))
    return ss

print("="*80)
print("  ITERATION 3: UC Domain + UC Pages (Governance Layer)")
print("="*80)
CAT = CATALOG

# ── Step 0: Clone Iteration 2 agents into new Iteration 3 agents ──
print("\n  Step 0: Cloning Iteration 2 agents → Iteration 3...")
if 'all_spaces' not in dir() or 'Iteration 2' not in all_spaces:
    all_spaces["Iteration 2"] = discover_agents_by_label("Iteration 2")
source = all_spaces["Iteration 2"]
assert len(source) >= 5, f"Iteration 2 has only {len(source)} agents — need 5"
spaces = clone_agents(source, "Iteration 3")
all_spaces["Iteration 3"] = dict(spaces)
assert len(spaces) >= 5, f"Iteration 3 clone produced only {len(spaces)} agents"
for name, sid in spaces.items():
    print(f"    {name:15s} → {sid}")
print("\n  Updating Supervisor to point to Iteration 3 agents...")
update_supervisor_tools(spaces)

# ── Step 1: Add fiscal_targets table to Executive agent's data sources ──
# fiscal_targets was created in the PRE-STEP cell (before the manual step)
# so it could be linked as a Related Asset on UC Page 1.
# Here we just add it to the Executive agent's data sources.
# We add the TABLE so the agent can query it — but we do NOT inject
# instructions. The UC Page provides the governance context that tells
# the agent WHAT Q1 means and WHERE to look.
print("\n  Step 2: Adding fiscal_targets table to Executive agent...")

exec_space_id = spaces["executive"]
resp = requests.get(f"{host}/api/2.0/genie/spaces/{exec_space_id}?include_serialized_space=true", headers=headers)
ss = json.loads(resp.json().get("serialized_space", "{}"))
tables = ss.get("data_sources", {}).get("tables", [])
target_table = f"{CAT}.reporting.fiscal_targets"
if target_table not in [t["identifier"] for t in tables]:
    tables.append({"identifier": target_table})
    tables.sort(key=lambda t: t["identifier"])
    ss.setdefault("data_sources", {})["tables"] = tables
    ensure_sorted_payload(ss)  # ensure example_question_sqls stay sorted
    patch_resp = requests.patch(f"{host}/api/2.0/genie/spaces/{exec_space_id}", headers=headers,
                                json={"serialized_space": json.dumps(ss)})
    if patch_resp.status_code == 200:
        print(f"    \u2713 fiscal_targets added to Executive agent's data sources")
    else:
        print(f"    \u2717 PATCH failed: {patch_resp.status_code} {patch_resp.text[:200]}")
else:
    print(f"    (skip) fiscal_targets already in Executive agent")

# ── Step 2a: Cleanup — remove any prior threshold/pointer injections ──
# PROVEN: Genie Agents cannot read UC Pages (agent confirmed:
# "I don't have direct access to those pages in this context").
# UC Pages work only via Genie One (confirmed: 402 with citation).
# Clean up any residual threshold or UC Domain pointer text from agents.
print("\n  Step 2a: Cleanup — removing any prior threshold/UC pointer injections from agents...")

# Patterns that indicate explicit threshold duplication (must be removed)
THRESHOLD_PATTERNS = [
    "CRITICAL THRESHOLD", "delay_days > 3", "delay_days >3",
    "forecast_accuracy_pct < 85", "forecast_accuracy_pct <85",
    "days_of_supply < 14", "days_of_supply <14",
    "quality_score < 70", "quality_score <70",
    "late_shipment_count > 500", "late_shipment_count >500",
    "Do NOT use is_late alone", "Do NOT search delay_reason",
    "Do NOT use below_safety_stock_flag", "Do NOT use is_late for quality",
]

def has_threshold_text(item):
    """Return True if instruction item contains ANY explicit threshold."""
    return any(pat in item for pat in THRESHOLD_PATTERNS)

for agent_name in ["logistics", "demand", "inventory", "supplier", "executive"]:
    space_id = spaces.get(agent_name)
    if not space_id:
        print(f"    (skip) {agent_name}: space not found")
        continue
    resp = requests.get(f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true", headers=headers)
    ss = json.loads(resp.json().get("serialized_space", "{}"))
    instrs = ss.get("instructions", {}).get("text_instructions", [])
    if not instrs:
        print(f"    (skip) {agent_name}: no instructions found")
        continue

    # --- REMOVAL: Strip ALL explicit threshold & UC Domain pointer items ---
    old_content = instrs[0]["content"]
    clean_content = []
    removed_count = 0
    for item in old_content:
        if has_threshold_text(item) or "UC DOMAIN REFERENCE" in item:
            removed_count += 1
        else:
            clean_content.append(item)

    if removed_count == 0:
        print(f"    (skip) {agent_name}: already clean")
        continue

    instrs[0]["content"] = clean_content
    ss["instructions"]["text_instructions"] = instrs
    ensure_sorted_payload(ss)
    patch_resp = requests.patch(f"{host}/api/2.0/genie/spaces/{space_id}", headers=headers,
                                json={"serialized_space": json.dumps(ss)})
    if patch_resp.status_code != 200:
        print(f"    \u2717 {agent_name}: PATCH failed {patch_resp.status_code} {patch_resp.text[:200]}")
        continue

    # --- VERIFY ---
    verify_resp = requests.get(f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true", headers=headers)
    verify_ss = json.loads(verify_resp.json().get("serialized_space", "{}"))
    verify_text = str(verify_ss.get("instructions", {}))
    residual = [pat for pat in THRESHOLD_PATTERNS if pat in verify_text]
    has_pointer = "UC DOMAIN REFERENCE" in verify_text
    if residual or has_pointer:
        print(f"    \u2717 {agent_name}: residual found after cleanup: {residual}")
    else:
        print(f"    \u2713 {agent_name}: {removed_count} injections removed, verified clean")

# ── FALLBACK: Agent instruction injection (COMMENTED OUT) ──────────────────
# If UC Pages alone don't fix G01/G02/F02, uncomment Steps 2b and 2c below.
# These inject temporal context and fiscal calendar directly into agent
# instructions — they WORK, but the demo narrative is stronger if Pages
# alone do the job. Test without first; uncomment only if needed.
# ───────────────────────────────────────────────────────────────────────────

# # ── Step 2b: Add temporal context to ALL agents (fixes date ambiguity) ──
# print("\n  Step 2b: Adding temporal context to all agents...")
# temporal_instruction = """\n\nTEMPORAL CONTEXT (CRITICAL): The current reference date is September 1, 2026.
# - 'Last month' = August 2026 (the entire calendar month)
# - 'Prior month' = July 2026
# - When a question mentions 'August' without a year, ALWAYS use 2026.
# - When a question mentions 'last month' without a year, ALWAYS use August 2026.
# - Do NOT use 2023, 2024, or 2025 for any 'last month' or 'August' queries."""
# for agent_name in ["logistics", "demand", "inventory", "supplier", "executive"]:
#     space_id = spaces.get(agent_name)
#     if not space_id: continue
#     resp = requests.get(f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true", headers=headers)
#     ss = json.loads(resp.json().get("serialized_space", "{}"))
#     instrs = ss.get("instructions", {}).get("text_instructions", [])
#     if instrs and "TEMPORAL CONTEXT" not in str(instrs):
#         instrs[0]["content"].append(temporal_instruction)
#         ss["instructions"] = {"text_instructions": instrs}
#         requests.patch(f"{host}/api/2.0/genie/spaces/{space_id}", headers=headers,
#                        json={"serialized_space": json.dumps(ss)})
#         print(f"    ✓ {agent_name}: temporal context added")
#     else:
#         print(f"    (skip) {agent_name}: already has temporal context")

# # ── Step 2c: Add fiscal calendar instructions to Executive agent ──
# print("\n  Step 2c: Adding fiscal calendar to Executive agent...")
# fiscal_instruction = f"""\n\nFISCAL CALENDAR (UC Pages equivalent):
# This company uses a JULY fiscal year start (not January).
# - Q1 = Jul, Aug, Sep
# - Q2 = Oct, Nov, Dec
# - Q3 = Jan, Feb, Mar (NOT calendar Jul-Sep!)
# - Q4 = Apr, May, Jun
# Current fiscal year: FY2027 (Jul 2026 - Jun 2027).
# The fiscal_targets table has service_level_target_pct for each quarter.
# Q1 service-level target = 92.0%.
# When asked about Q1 targets, ALWAYS query fiscal_targets WHERE fiscal_quarter = 'Q1'.
# When asked if we will miss Q1 targets, compare executive_kpis.service_level_pct against fiscal_targets.service_level_target_pct."""
# exec_resp = requests.get(f"{host}/api/2.0/genie/spaces/{exec_space_id}?include_serialized_space=true", headers=headers)
# exec_ss = json.loads(exec_resp.json().get("serialized_space", "{}"))
# instrs = exec_ss.get("instructions", {}).get("text_instructions", [])
# if instrs and "FISCAL CALENDAR" not in str(instrs):
#     instrs[0]["content"].append(fiscal_instruction)
#     exec_ss["instructions"] = {"text_instructions": instrs}
#     patch_resp = requests.patch(f"{host}/api/2.0/genie/spaces/{exec_space_id}", headers=headers,
#                                 json={"serialized_space": json.dumps(exec_ss)})
#     if patch_resp.status_code == 200:
#         print(f"    ✓ Added fiscal calendar instructions to Executive agent")
#     else:
#         print(f"    ✗ PATCH failed: {patch_resp.status_code} {patch_resp.text[:200]}")

# ── Step 3: Document UC Domain + Pages (governance layer — LIVE in Discover) ──
# These were created manually on the Discover page BEFORE this iteration.
# The domain and pages feed directly into Genie's ontology.
# NO agent instruction injection needed — the Pages ARE the fix.
print("\n  Step 3: UC Domain + Pages (LIVE — created on Discover page)")
print("    \u250c\u2500 PROVEN: GENIE ONE vs GENIE AGENTS \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
print("    \u2502")
print("    \u2502 GENIE ONE \u2705 READS UC PAGES:")
print("    \u2502   Asked: 'How many shipments in August had critical delay?'")
print("    \u2502   Found glossary page 'Logistics Risk Standards'")
print("    \u2502   Read definition: delay_days >= 5 AND total_weight_kg > 800")
print("    \u2502   Result: 176 (correct). Cited: Genie Ontology \u2192 Page.")
print("    \u2502")
print("    \u2502 GENIE AGENTS \u274c CANNOT READ UC PAGES:")
print("    \u2502   Agent stated: 'I don't have direct access to those")
print("    \u2502   pages in this context'")
print("    \u2502   Fell back to guessing thresholds (60, 70, 50) instead")
print("    \u2502   of reading the Page-defined value.")
print("    \u2502")
print("    \u2502 SUPERVISOR AGENT \u274c NO TOOL TYPE FOR UC PAGES:")
print("    \u2502   Supported: genie_space, dashboard, uc_function, table,")
print("    \u2502   knowledge_assistant, volume, vector_search_index, etc.")
print("    \u2502   No 'page' or 'domain' tool type exists.")
print("    \u2502   No public REST API for Page content.")
print("    \u2502")
print("    \u2502 SQL FUNCTION AS AGENT SOURCE \u2705:")
print("    \u2502   Agent UI: Called get_critical_delay_shipments(),")
print("    \u2502   read delay >= 5 AND weight > 800, returned 176. WORKS!")
print("    \u2502   Agent Mode API: Also calls function correctly. WORKS!")
print("    \u2502   Both channels use the function when added as a source.")
print("    \u2502")
print("    \u2502 CONCLUSION:")
print("    \u2502   UC Pages \u2192 Genie One only (agents cannot read Pages).")
print("    \u2502   SQL Functions \u2192 Both Agent UI and Agent Mode API.")
print("    \u2502   G01/G02 pass via fiscal_targets TABLE.")
print("    \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
print("")
print("    Domain: Supply Chain Operations \u2713")
print("    Schemas: demand_analysis, inventory_management, logistics_operations,")
print("             supplier_procurement, reporting")
print("")
print("    \u2500\u2500 PAGE 1: 'Fiscal Calendar & Targets' \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
print("    Note:           G01/G02 fixed by fiscal_targets TABLE,")
print("                    NOT by this Page (agents can't read Pages)")
print("    Synonyms:       Fiscal, Fiscal Year")
print("    Definition:     July FY start. Q1=Jul-Sep, Q2=Oct-Dec,")
print("                    Q3=Jan-Mar (NOT calendar!), Q4=Apr-Jun")
print("    Business Use:   Q1 target = 92%. Reference date: Sept 1, 2026.")
print("                    Last month = August 2026, prior = July 2026.")
print("    Related Assets: fiscal_targets, executive_kpis")
print("")
print("    \u2500\u2500 PAGE 2: 'Cross-Domain Metric Definitions' \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
print("    Fixes:          F02 (vendor late rate = per-order, not per-vendor)")
print("    Definition:     Vendor Late Rate = late POs / total POs per ORDER")
print("                    = 75.0%. NEVER COUNT(DISTINCT supplier_id) = 83.33%.")
print("                    OTD rate (94.57%) \u2260 supplier late rate (75%).")
print("    Business Use:   CoD formula, cross-domain metric disambiguation")
print("    Related Assets: delivery_performance_by_region,")
print("                    cost_of_disruption_by_region,")
print("                    supplier_performance_by_continent")
print("")
print("    \u2500\u2500 5 DOMAIN-SPECIFIC 'CRITICAL' DEFINITIONS \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
print("    Domain-specific POLICY NAMES, each defined in its own UC Page.")
print("    Tests P01\u2013P05: each domain defines a unique threshold.")
print("    Without its UC Page, the agent has NO threshold to apply.")
print("")
print("    Domain 2: Logistics Operations (logistics_operations)")
print("    PAGE 3: 'Logistics Risk Standards'")
print("    Fixes: P01 (Logistics Risk = delay >= 5 AND weight > 800 \u2192 176 shipments)")
print("")
print("    Domain 3: Demand Analysis (demand_analysis)")
print("    PAGE 4: 'Demand Quality Standards'")
print("    Fixes: P02 (Demand Anomaly = qty >= 8 AND price < 30 AND Online \u2192 77 orders)")
print("")
print("    Domain 4: Inventory Management (inventory_management)")
print("    PAGE 5: 'Inventory Risk Classification'")
print("    Fixes: P03 (Inventory Risk = dos 1-11 AND below_ss AND on_hand > 0 \u2192 106 positions)")
print("")
print("    Domain 5: Supplier Procurement (supplier_procurement)")
print("    PAGE 6: 'Supplier Quality Standards'")
print("    Fixes: P04 (Procurement Quality = score < 75 AND ltv > 12 \u2192 11 orders)")
print("")
print("    Domain 6: Executive Reporting (reporting)")
print("    PAGE 7: 'Executive Alert Thresholds'")
print("    Fixes: P05 (Exec Disruption = risk < 55 AND ltv > 8 AND penalty > 80K \u2192 3 suppliers)")

# ── Step 4: Create SQL Functions for domain-specific "critical" thresholds ──
# Since Genie Agents CANNOT read UC Pages, we create TABLE-valued SQL functions
# that encode the same thresholds. When added to an agent's data sources,
# the agent CAN call these functions (proven with get_critical_threshold).
print("\n  Step 4: Creating SQL threshold functions for P01-P05...")

threshold_functions = [
    # P01: Logistics critical delay — definition only, agent must query shipments table itself
    (f"{CAT}.logistics_operations", "get_critical_delay_shipments", f"""CREATE OR REPLACE FUNCTION {CAT}.logistics_operations.get_critical_delay_shipments()
RETURNS TABLE (concept STRING, source_table STRING, threshold_columns STRING, conditions STRING, definition STRING)
RETURN
  SELECT
    'Logistics Risk Standards' AS concept,
    '{CAT}.logistics_operations.shipments' AS source_table,
    'delay_days, total_weight_kg' AS threshold_columns,
    'delay_days >= 5 AND total_weight_kg > 800' AS conditions,
    'A shipment is flagged under Logistics Risk Standards when delay_days >= 5 AND total_weight_kg > 800. Query the shipments table with ship_date for the relevant time period and apply these conditions. Source: UC Page Logistics Risk Standards.' AS definition"""),
    # P03: Inventory critical supply — definition only, agent must query inventory_ledger itself
    (f"{CAT}.inventory_management", "get_critical_supply_positions", f"""CREATE OR REPLACE FUNCTION {CAT}.inventory_management.get_critical_supply_positions()
RETURNS TABLE (concept STRING, source_table STRING, threshold_columns STRING, conditions STRING, definition STRING)
RETURN
  SELECT
    'Inventory Risk Classification' AS concept,
    '{CAT}.inventory_management.inventory_ledger' AS source_table,
    'days_of_supply, below_safety_stock_flag, on_hand_qty' AS threshold_columns,
    'days_of_supply BETWEEN 1 AND 11 AND below_safety_stock_flag = true AND on_hand_qty > 0' AS conditions,
    'An inventory position is classified as supply-risk under Inventory Standards when days_of_supply BETWEEN 1 AND 11 AND below_safety_stock_flag = true AND on_hand_qty > 0. Query the inventory_ledger table and apply these conditions. Source: UC Page Inventory Risk Classification.' AS definition"""),
    # P02: Demand critical accuracy — definition only, agent must query sales_orders itself
    (f"{CAT}.demand_analysis", "get_critical_accuracy_forecasts", f"""CREATE OR REPLACE FUNCTION {CAT}.demand_analysis.get_critical_accuracy_forecasts()
RETURNS TABLE (concept STRING, source_table STRING, threshold_columns STRING, conditions STRING, definition STRING)
RETURN
  SELECT
    'Demand Anomaly Alert' AS concept,
    '{CAT}.demand_analysis.sales_orders' AS source_table,
    'quantity, unit_price, channel' AS threshold_columns,
    'quantity >= 8 AND unit_price < 30 AND channel = ''Online''' AS conditions,
    'A Western region order triggers a Demand Anomaly Alert when quantity >= 8 AND unit_price < 30 AND channel = Online. Query the sales_orders table with order_date for the relevant time period and region, then apply these conditions. Source: UC Page Demand Quality Standards.' AS definition"""),
    # P04: Supplier critical quality — definition only, agent must query supplier_orders itself
    (f"{CAT}.supplier_procurement", "get_critical_quality_orders", f"""CREATE OR REPLACE FUNCTION {CAT}.supplier_procurement.get_critical_quality_orders()
RETURNS TABLE (concept STRING, source_table STRING, threshold_columns STRING, conditions STRING, definition STRING)
RETURN
  SELECT
    'Procurement Quality Minimum' AS concept,
    '{CAT}.supplier_procurement.supplier_orders' AS source_table,
    'quality_score, lead_time_variance_days' AS threshold_columns,
    'quality_score < 75 AND lead_time_variance_days > 12' AS conditions,
    'A supplier order falls below Procurement Quality Minimum when quality_score < 75 AND lead_time_variance_days > 12. Query the supplier_orders table with order_date for the relevant time period and apply these conditions. Source: UC Page Supplier Quality Standards.' AS definition"""),
    # P05: Executive critical disruption — definition only, agent must query risk_scorecard itself
    (f"{CAT}.reporting", "get_critical_disruption_regions", f"""CREATE OR REPLACE FUNCTION {CAT}.reporting.get_critical_disruption_regions()
RETURNS TABLE (concept STRING, source_table STRING, threshold_columns STRING, conditions STRING, definition STRING)
RETURN
  SELECT
    'Executive Disruption Threshold' AS concept,
    '{CAT}.reporting.supply_chain_risk_scorecard' AS source_table,
    'composite_risk_score, lead_time_variance, total_penalty_usd' AS threshold_columns,
    'composite_risk_score < 55 AND lead_time_variance > 8 AND total_penalty_usd > 80000' AS conditions,
    'A supplier exceeds the Executive Disruption Threshold when composite_risk_score < 55 AND lead_time_variance > 8 AND total_penalty_usd > 80000. Query the supply_chain_risk_scorecard table and apply these conditions. Source: UC Page Executive Alert Thresholds.' AS definition"""),
]

for schema, fname, sql in threshold_functions:
    try:
        spark.sql(sql)
        print(f"    \u2713 {schema.split('.')[-1]}.{fname}()")
    except Exception as e:
        print(f"    \u2717 {fname}: {str(e)[:120]}")

# ── Step 5: Add SQL functions to agent data sources (MANUAL — UI only to ADD) ──
# KNOWN LIMITATION: The serialized_space API does NOT support adding
# SQL functions to data_sources. The PATCH silently succeeds but
# functions are NOT actually added. Functions can ONLY be added via
# the Agent UI: Agent Settings > Sources > SQL function > Add function.
# Once added, both Agent UI AND Agent Mode API can call the function.
print("\n  Step 5: SQL Functions — \u26a0 MANUAL STEP REQUIRED")
print("    \u250c\u2500 KNOWN LIMITATION: serialized_space API does NOT support functions \u2500\u2500\u2500")
print("    \u2502")
print("    \u2502  Functions were CREATED in the catalog (Step 4 above).")
print("    \u2502  But they must be ADDED to each agent's data sources")
print("    \u2502  via the Agent UI (API cannot do this).")
print("    \u2502")
print("    \u2502  For each agent below, open the agent in the UI and go to:")
print("    \u2502    Agent Settings \u2192 Sources \u2192 SQL function \u2192 Add function")
print("    \u2502")
print(f"    \u2502  logistics  \u2192 {CAT}.logistics_operations.get_critical_delay_shipments")
print(f"    \u2502  demand     \u2192 {CAT}.demand_analysis.get_critical_accuracy_forecasts")
print(f"    \u2502  inventory  \u2192 {CAT}.inventory_management.get_critical_supply_positions")
print(f"    \u2502  supplier   \u2192 {CAT}.supplier_procurement.get_critical_quality_orders")
print(f"    \u2502  executive  \u2192 {CAT}.reporting.get_critical_disruption_regions")
print("    \u2502")
print("    \u2502  EXPECTED BEHAVIOR AFTER ADDING:")
print("    \u2502  \u2705 Agent UI queries: agent calls function, returns correct answer")
print("    \u2502  \u2705 Agent Mode API: also calls function correctly")
print("    \u2502  Once added as source, P01-P05 should pass in both channels.")
print("    \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")

# ── Step 6: Enhance Supervisor instructions for Iteration 3 ──
# Appends a brief capability-awareness block to the supervisor's instructions.
# Keeps it general — agents discover answers through their own semantic context.
print("\n  Step 6: Enhancing Supervisor agent with Iteration 3 capabilities...")


def _append_once(existing_text, addition_text, marker):
    existing_text = existing_text or ""
    if marker in existing_text:
        return existing_text, False
    sep = "\n\n" if existing_text.strip() else ""
    return existing_text.rstrip() + sep + addition_text.strip(), True


# Find the current supervisor agent created by 08_setup
sup_list_resp = requests.get(f"{host}/api/2.1/supervisor-agents", headers=headers)
if sup_list_resp.status_code != 200:
    print(f"    \u2717 Could not list supervisor agents: {sup_list_resp.status_code} {sup_list_resp.text[:200]}")
else:
    supervisors = sup_list_resp.json().get("supervisor_agents", [])
    supervisor = next((a for a in supervisors if "Supply Chain" in a.get("display_name", "")), None)
    if not supervisor:
        print("    \u2717 Supply Chain supervisor not found")
    else:
        supervisor_name = supervisor["name"]
        print(f"    \u2713 Found supervisor: {supervisor_name}")

        # Read current instructions so we ENHANCE them instead of replacing them.
        sup_detail_resp = requests.get(f"{host}/api/2.1/{supervisor_name}", headers=headers)
        sup_detail = sup_detail_resp.json() if sup_detail_resp.status_code == 200 else supervisor
        current_instructions = sup_detail.get("instructions", supervisor.get("instructions", "")) or ""

        iter3_supervisor_block = """
ITERATION 3 CAPABILITIES
- Your sub-agents have been enhanced with UC semantic features including metric views, SQL functions, certified queries, column comments, and governed tags. They discover and use these features autonomously to answer questions about business terms, policy thresholds, and cross-domain metrics.
- For cross-domain questions spanning multiple schemas (e.g. Cost of Disruption, revenue at risk), route to executive-reporting first — it has pre-built cross-domain views.
- For domain-specific policy terms (e.g. Logistics Risk Standards, Demand Anomaly Alert, Procurement Quality Minimum), route to the owning domain agent — it has the context to resolve the term.
- This organization uses a July fiscal year start. Q1 = Jul-Sep (current quarter). For Q1 target questions, route to executive-reporting.
"""

        enhanced_instructions, changed = _append_once(
            current_instructions,
            iter3_supervisor_block,
            "ITERATION 3 CAPABILITIES"
        )

        if changed:
            patch_resp = requests.patch(
                f"{host}/api/2.1/{supervisor_name}?update_mask=instructions",
                headers=headers,
                json={"instructions": enhanced_instructions}
            )
            if patch_resp.status_code == 200:
                print(f"    \u2713 Supervisor instructions enhanced ({len(current_instructions)} -> {len(enhanced_instructions)} chars)")
            else:
                print(f"    \u2717 Supervisor instruction PATCH failed: {patch_resp.status_code} {patch_resp.text[:200]}")
        else:
            print("    (skip) Supervisor instructions already include Iteration 3 block")

# ── Step 7: Add SQL Functions as Supervisor Agent tools ──
# The supervisor needs direct access to these functions so it can
# answer policy threshold questions without routing to sub-agents.
# Tool type = 'uc_function' (proven supported by Supervisor Agent API).
print("\n  Step 7: Adding 5 SQL threshold functions as Supervisor tools...")

sql_function_tools = [
    {"name": f"{CAT}.logistics_operations.get_critical_delay_shipments",
     "description": "Returns the business definition of Logistics Risk Standards — which columns and conditions define a flagged shipment. Does NOT return data; agent must query the source table itself."},
    {"name": f"{CAT}.demand_analysis.get_critical_accuracy_forecasts",
     "description": "Returns the business definition of Demand Anomaly Alert — which columns and conditions define an anomalous order. Does NOT return data; agent must query the source table itself."},
    {"name": f"{CAT}.inventory_management.get_critical_supply_positions",
     "description": "Returns the business definition of Inventory Risk Classification — which columns and conditions define a supply-risk position. Does NOT return data; agent must query the source table itself."},
    {"name": f"{CAT}.supplier_procurement.get_critical_quality_orders",
     "description": "Returns the business definition of Procurement Quality Minimum — which columns and conditions define a below-minimum order. Does NOT return data; agent must query the source table itself."},
    {"name": f"{CAT}.reporting.get_critical_disruption_regions",
     "description": "Returns the business definition of Executive Disruption Threshold — which columns and conditions define an at-risk supplier. Does NOT return data; agent must query the source table itself."},
]

if supervisor:
    # List existing tools to avoid duplicates
    existing_tools_resp = requests.get(f"{host}/api/2.1/{supervisor_name}/tools", headers=headers)
    existing_tool_names = set()
    if existing_tools_resp.status_code == 200:
        for t in existing_tools_resp.json().get("tools", []):
            existing_tool_names.add(t.get("uc_function", {}).get("name", ""))
    
    added, skipped = 0, 0
    for func in sql_function_tools:
        if func["name"] in existing_tool_names:
            print(f"    (skip) {func['name'].split('.')[-1]}: already registered")
            skipped += 1
            continue
        # tool_id must be passed as URL query parameter (not in JSON body)
        func_slug = func["name"].split(".")[-1].replace("_", "-")
        add_resp = requests.post(
            f"{host}/api/2.1/{supervisor_name}/tools",
            headers=headers,
            params={"tool_id": func_slug},
            json={
                "tool_type": "uc_function",
                "uc_function": {"name": func["name"]},
                "description": func["description"],
            }
        )
        if add_resp.status_code == 200:
            print(f"    \u2713 {func['name'].split('.')[-1]} (tool_id: {func_slug})")
            added += 1
        else:
            print(f"    \u2717 {func['name'].split('.')[-1]}: {add_resp.status_code} {add_resp.text[:150]}")
    print(f"    Added: {added}, Skipped: {skipped}")

    # ── Step 7b: Enhance Supervisor instructions to use SQL functions ──
    print("\n  Step 7b: Enhancing Supervisor instructions to reference SQL functions...")
    sup_detail_resp2 = requests.get(f"{host}/api/2.1/{supervisor_name}", headers=headers)
    current_instr2 = sup_detail_resp2.json().get("instructions", "") if sup_detail_resp2.status_code == 200 else ""

    sql_func_instruction_block = """
SQL THRESHOLD FUNCTIONS (ITERATION 3)
You have 5 SQL threshold functions available as direct tools. Each returns the BUSINESS DEFINITION of a domain-specific policy standard — the concept name, source table, threshold columns, and conditions. They do NOT return data or counts.
When asked about policy standards or "critical" thresholds, call the relevant function to learn the definition, then query the source table with the conditions it describes:
- get_critical_delay_shipments: Defines what Logistics Risk Standards means
- get_critical_accuracy_forecasts: Defines what Demand Anomaly Alert means
- get_critical_supply_positions: Defines what Inventory Risk Classification means
- get_critical_quality_orders: Defines what Procurement Quality Minimum means
- get_critical_disruption_regions: Defines what Executive Disruption Threshold means
Do NOT guess thresholds — always call the function first to learn the correct conditions.
"""

    enhanced2, changed2 = _append_once(
        current_instr2,
        sql_func_instruction_block,
        "SQL THRESHOLD FUNCTIONS (ITERATION 3)"
    )
    if changed2:
        patch_resp2 = requests.patch(
            f"{host}/api/2.1/{supervisor_name}?update_mask=instructions",
            headers=headers,
            json={"instructions": enhanced2}
        )
        if patch_resp2.status_code == 200:
            print(f"    \u2713 Supervisor instructions enhanced with SQL function guidance ({len(current_instr2)} -> {len(enhanced2)} chars)")
        else:
            print(f"    \u2717 PATCH failed: {patch_resp2.status_code} {patch_resp2.text[:200]}")
    else:
        print("    (skip) SQL function block already in supervisor instructions")
else:
    print("    \u2717 Supervisor not found — cannot add tools")

# ── Step 7c: Enhance sub-agent instructions with provenance reporting ──
# Genie agents may or may not honor this, but it's a lightweight addition
# that helps the narration include what UC feature was consulted.
print("\n  Step 7c: Adding provenance reporting instruction to sub-agents...")

prov_instruction = """
PROVENANCE: When answering, briefly note which data source informed your answer:
- If you called a SQL function (e.g. get_critical_delay_shipments), say 'Used SQL function: [name]'
- If you queried a metric view (e.g. delivery_performance_by_region), say 'Used metric view: [name]'
- If column comments or examples guided your query, note which ones
This helps track which governance features are actively used.
"""

for agent_name, agent_sid in spaces.items():
    try:
        r = requests.get(f"{host}/api/2.0/genie/spaces/{agent_sid}?include_serialized_space=true", headers=headers)
        if r.status_code != 200:
            print(f"    \u26a0 {agent_name}: read failed")
            continue
        ss = json.loads(r.json().get("serialized_space", "{}"))
        instr = ss.get("instructions", {})
        if isinstance(instr, dict):
            text_instrs = instr.get("text_instructions", [])
            current_text = text_instrs[0]["content"][0] if text_instrs and text_instrs[0].get("content") else ""
        elif isinstance(instr, str):
            current_text = instr
        else:
            current_text = ""
        if "PROVENANCE:" in current_text:
            print(f"    (skip) {agent_name}: already has provenance instruction")
            continue
        new_text = current_text.rstrip() + "\n" + prov_instruction.strip()
        if isinstance(instr, dict):
            if not instr.get("text_instructions"):
                instr["text_instructions"] = [{"content": [new_text]}]
            else:
                instr["text_instructions"][0]["content"] = [new_text]
        else:
            ss["instructions"] = new_text
        ensure_sorted_payload(ss)
        pr = requests.patch(f"{host}/api/2.0/genie/spaces/{agent_sid}",
                            headers=headers, json={"serialized_space": json.dumps(ss)})
        if pr.status_code == 200:
            print(f"    \u2713 {agent_name}: provenance instruction added")
        else:
            print(f"    \u26a0 {agent_name}: patch failed ({pr.status_code})")
    except Exception as e:
        print(f"    \u26a0 {agent_name}: error {e}")

print("\n\u2705 Iteration 3 SETUP COMPLETE:")
print("   \u2713 fiscal_targets table added to Executive agent (fixes G01/G02)")
print("   \u2713 5 SQL threshold functions created in catalog")
print("   \u2713 UC Pages on Discover page (work via Genie One, not Genie Agents)")
print("   \u2713 Supervisor enhanced with Iteration 3 capability awareness")
print("   \u2713 5 SQL functions registered as Supervisor tools (direct access)")
print("   \u2713 Supervisor instructions enhanced with SQL function guidance")
print("   \u2713 Sub-agent provenance reporting instructions added")
print("")
print("  \u26d4 STOP HERE \u2014 Add SQL functions to each agent via the UI before continuing.")
print("     See the table above (Step 5) for which function goes to which agent.")
print("     Then run the NEXT CELL to execute the Iteration 3 test suite.")

# COMMAND ----------

# DBTITLE 1,STEP 8: ai_forecast Capability + Supervisor Enhancement
# ============================================================
# STEP 8: ai_forecast Forecasting Capability
# Adds ai_forecast instruction to all 5 domain agents so they
# can project time-series metrics forward when asked about
# future outlook or end-of-quarter projections.
# Also enhances Supervisor agent descriptions and adds forecast awareness.
# This is a supplementary capability — does NOT affect existing
# test results. Forecasted values are always labeled as projections.
# ============================================================
import json

print("="*80)
print("  STEP 8: ai_forecast Forecasting Capability")
print("="*80)

# Guard: ensure helper is available (defined in Iter 1 / Iter 3 cells)
if 'ensure_sorted_payload' not in dir():
    def ensure_sorted_payload(ss):
        if "instructions" in ss and "example_question_sqls" in ss.get("instructions", {}):
            ss["instructions"]["example_question_sqls"].sort(key=lambda x: str(x.get("id", "")))
        if "benchmarks" in ss and "questions" in ss.get("benchmarks", {}):
            ss["benchmarks"]["questions"].sort(key=lambda x: str(x.get("id", "")))
        return ss

# ── Step 8a: Add ai_forecast instruction to ALL domain agents ──
# Generic capability — no test IDs, no specific metrics, no hints.
# Agents decide WHEN and HOW to forecast based on the question asked.
print("\n  Step 8a: Adding ai_forecast instruction to domain agents...")

forecast_instruction = (
    "FORECASTING CAPABILITY:\n"
    "When asked about future outlook, end-of-quarter projections, or whether current "
    "trends will hold, you may use the ai_forecast() SQL function to project time-series "
    "metrics forward. Pattern:\n"
    "  WITH monthly AS (\n"
    "    SELECT DATE_TRUNC('month', <date_col>) AS ds, <AGG>(<metric>) AS val\n"
    "    FROM <table> WHERE <filters> GROUP BY 1\n"
    "  )\n"
    "  SELECT * FROM ai_forecast(TABLE(monthly), horizon => DATE '<end_date>', "
    "time_col => 'ds', value_col => 'val', version => '1')\n"
    "After answering the factual question with current data, add a separate FORECAST "
    "section showing projected values through the target date. Clearly label forecasts "
    "as projections, not confirmed facts. Include upper/lower prediction intervals "
    "when available."
)

FORECAST_MARKER = "FORECASTING CAPABILITY:"
for agent_name, space_id in spaces.items():
    try:
        resp = requests.get(
            f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true",
            headers=headers
        )
        ss = json.loads(resp.json().get("serialized_space", "{}"))
        instrs = ss.get("instructions", {}).get("text_instructions", [])
        if not instrs or not instrs[0].get("content"):
            print(f"    (skip) {agent_name}: no instructions found")
            continue
        if FORECAST_MARKER in str(instrs):
            print(f"    (skip) {agent_name}: already has forecast instruction")
            continue
        instrs[0]["content"].append(forecast_instruction)
        ss["instructions"]["text_instructions"] = instrs
        ensure_sorted_payload(ss)
        patch_resp = requests.patch(
            f"{host}/api/2.0/genie/spaces/{space_id}", headers=headers,
            json={"serialized_space": json.dumps(ss)}
        )
        if patch_resp.status_code == 200:
            print(f"    \u2713 {agent_name}: ai_forecast capability added")
        else:
            print(f"    \u2717 {agent_name}: PATCH failed {patch_resp.status_code} {patch_resp.text[:200]}")
    except Exception as e:
        print(f"    \u26a0 {agent_name}: error {e}")

# ── Step 8b: Enhance Supervisor base descriptions + add FORECASTING block ──
# Make agent descriptions slightly more specific (key tables) so the Supervisor
# routes more precisely. Also add FORECASTING awareness block.
print("\n  Step 8b: Enhancing Supervisor instructions...")

if 'supervisor_name' not in dir() or not supervisor_name:
    sup_list = requests.get(f"{host}/api/2.1/supervisor-agents", headers=headers)
    if sup_list.status_code == 200:
        for sa in sup_list.json().get("supervisor_agents", []):
            if "Supply Chain" in sa.get("display_name", ""):
                supervisor_name = sa["name"]
                break

sup_resp = requests.get(f"{host}/api/2.1/{supervisor_name}", headers=headers)
if sup_resp.status_code == 200:
    instr = sup_resp.json().get("instructions", "")
    changed = False

    # Enhance base agent descriptions (replace minimal with table-aware)
    OLD_AGENTS = """Available agents:
- demand-analysis: Revenue and sales data
- inventory-management: Inventory and stockout data
- logistics-operations: Shipment and delivery data
- supplier-risk: Supplier performance data
- executive-reporting: KPI summaries and dashboards"""

    NEW_AGENTS = """Available agents:
- demand-analysis: Revenue, sales orders, demand forecasts, product performance. Key: sales_orders, demand_forecasts, products.
- inventory-management: Inventory levels, safety stock, stockouts, days of supply. Key: inventory_ledger, warehouse_data.
- logistics-operations: Shipment tracking, delivery performance, freight costs. Key: shipments, carriers.
- supplier-risk: Supplier performance, purchase orders, lead times, SLA penalties. Key: supplier_orders, vendor_slas.
- executive-reporting: Company-wide KPIs, regional summaries, revenue trends, fiscal targets. Key: executive_kpis, revenue_trend, fiscal_targets, cost_of_disruption_by_region."""

    if OLD_AGENTS in instr:
        instr = instr.replace(OLD_AGENTS, NEW_AGENTS)
        changed = True
        print("    \u2713 Enhanced agent descriptions with key tables")
    elif "Key: sales_orders" in instr:
        print("    (skip) Agent descriptions already enhanced")
    else:
        print("    (skip) Agent descriptions format not recognized")

    # Add FORECASTING block if missing
    forecast_block = (
        "\n\nFORECASTING (ai_forecast):\n"
        "Your domain agents can use ai_forecast() to project time-series metrics forward "
        "when asked about future outlook or end-of-quarter projections. When an agent "
        "provides forecasted values alongside actuals, present both clearly: label "
        "projections as 'PROJECTED' and actuals as 'ACTUAL'. Forecasts are supplementary "
        "context, not confirmed facts."
    )
    if "FORECASTING (ai_forecast)" not in instr:
        instr = instr.rstrip() + forecast_block
        changed = True
        print("    \u2713 Added FORECASTING block")
    else:
        print("    (skip) FORECASTING block already present")

    if changed:
        patch_resp = requests.patch(
            f"{host}/api/2.1/{supervisor_name}?update_mask=instructions",
            headers=headers,
            json={"instructions": instr}
        )
        if patch_resp.status_code == 200:
            print(f"    \u2713 Supervisor patched ({len(instr)} chars)")
        else:
            print(f"    \u2717 PATCH failed: {patch_resp.status_code} {patch_resp.text[:200]}")
    else:
        print(f"    (no changes needed, {len(instr)} chars)")
else:
    print(f"    \u2717 Supervisor not found: {sup_resp.status_code}")

print("\n\u2705 Step 8 complete:")
print("   \u2713 ai_forecast capability added to all 5 domain agents")
print("   \u2713 Supervisor agent descriptions enhanced with key tables")
print("   \u2713 Supervisor FORECASTING awareness block present")
print("   Existing test results are NOT affected — forecasting is supplementary output.")

# COMMAND ----------

# DBTITLE 1,ITERATION 3: Run 45-test suite (after SQL functions added to agents)
# ============================================================
# ITERATION 3: Run test suite
# Prerequisites:
#   1. Cell above ran successfully (fiscal_targets + SQL functions created)
#   2. SQL functions added to each agent's data sources via the UI:
#      Agent Settings → Sources → SQL function → Add function
#      (see Step 5 output in the cell above for the exact function names)
# ============================================================
print("="*80)
print("  ITERATION 3: Running 45-test suite")
print("="*80)
print("  Prerequisite: SQL functions added to agents via UI")
print(f"  Functions: get_critical_delay_shipments, get_critical_accuracy_forecasts,")
print(f"            get_critical_supply_positions, get_critical_quality_orders,")
print(f"            get_critical_disruption_regions")
print("")

iter3_passed, iter3_failed, iter3_errors, iter3_details = test_all_metrics("After Iteration 3")

# --- Comprehensive Prompt Benchmark: After Iteration 3 ---
comp_iter3 = run_comprehensive_benchmark("After Iteration 3 (UC Pages + SQL functions + fiscal_targets)")
all_comp_results["After Iteration 3"] = comp_iter3

# COMMAND ----------

# DBTITLE 1,Reasoning Confidence: How to Read the Evaluator Assessment
# MAGIC %md
# MAGIC ## Reasoning Confidence: How to Read the Evaluator Assessment
# MAGIC
# MAGIC Every test result carries a **Provenance Tier** (which UC feature the agent used) and a **Reasoning Confidence** label (how repeatable the answer is). The evaluator derives the confidence label dynamically from the provenance tier — nothing is hardcoded.
# MAGIC
# MAGIC | Confidence | What It Means | Provenance Tiers | Repeatable? |
# MAGIC | --- | --- | --- | --- |
# MAGIC | **DETERMINISTIC** | The agent was **steered** to the correct answer by a governed UC asset. Even if the question is rephrased, the governed column name or function catches all phrasings. The answer **cannot drift**. | Iter 2 (Metric Views), Iter 3 (SQL Functions, Reference Tables) | Always |
# MAGIC | **HEURISTIC** | The agent was **guided** by metadata — column comments, example SQL, or certified queries taught it the right pattern. Likely repeatable, but a sufficiently novel phrasing could bypass the guidance. | Iter 1 (Column Comments, Example SQL, Benchmarks) | Usually |
# MAGIC | **INFERRED** | The agent **figured it out on its own** from raw column and table names — no UC feature was involved. Correct today, but the mapping is a probabilistic LLM guess. A model update, rephrase, or vocabulary change could break it. These tests pass because the agent is capable, but they are **ungoverned**. | Raw Table (no UC feature used) | Today, not guaranteed |
# MAGIC | **GUESSED** | The agent **invented** thresholds or conditions that weren't provided by any UC feature. The answer may be right by coincidence but is unreliable. Multi-condition rules (P01-P05) are truly unguessable without the SQL function definition. | Guessed thresholds, no UC feature available | No |
# MAGIC
# MAGIC **Key insight**: The progression from INFERRED → DETERMINISTIC is the entire point of the UC semantic layer. Accuracy alone doesn't prove governance — a lucky guess scores the same as a governed answer. Confidence proves that the answer is **reliably correct**, not just correct today.

# COMMAND ----------

# DBTITLE 1,VISUAL: After Iteration 3 (Final)
# Re-plot dashboard with latest results (including Iteration 3)
plot_test_dashboard(all_stage_results, all_comp_results if 'all_comp_results' in dir() else None)

# ── Provenance Analysis: Extracted from EXISTING test results (no API calls) ──
current_stage = list(all_stage_results.keys())[-1] if all_stage_results else "Unknown"
results = all_stage_results.get(current_stage, [])

if results:
    ran_stages = set(all_stage_results.keys())
    iter_actually_ran = {
        'Iter 1': any('iteration 1' in s.lower() or 'iter 1' in s.lower() or 'iter1' in s.lower() for s in ran_stages),
        'Iter 2': any('iteration 2' in s.lower() or 'iter 2' in s.lower() or 'iter2' in s.lower() for s in ran_stages),
        'Iter 3': any('iteration 3' in s.lower() or 'iter 3' in s.lower() or 'iter3' in s.lower() for s in ran_stages),
    }
    def relabel_tier(raw_tier):
        if raw_tier == 'GUESSED':
            return 'Guessed (threshold)'
        if raw_tier in iter_actually_ran and not iter_actually_ran[raw_tier]:
            return f'Guessed ({raw_tier})'
        return raw_tier
    def _confidence_from_tier(tier):
        if tier in ('Iter 2', 'Iter 3') or tier.startswith('Iter 3'): return 'DETERMINISTIC'
        if tier == 'Iter 1': return 'HEURISTIC'
        if tier.startswith('Guessed'): return 'GUESSED'
        if tier == 'N/A': return 'N/A'
        return 'INFERRED'

    print("\n" + "="*90)
    print(f"  PROVENANCE ANALYSIS: {current_stage}")
    print(f"  Extracted from test results — no additional API calls")
    print("="*90)

    current_group = ""
    group_names = {'A':'LOGISTICS','B':'DEMAND','C':'INVENTORY','D':'SUPPLIER',
                   'E':'CROSS-DOMAIN','F':'INDIRECT','H':'HARD','G':'FISCAL','P':'CRITICAL'}
    for r in results:
        gid = r['id'][:1]
        if gid != current_group:
            current_group = gid
            print(f"\n  ── {group_names.get(gid, gid)} {'─'*80}")
        icon = '✅' if r['verdict'] == 'PASS' else ('❌' if r['verdict'] == 'FAIL' else '⚠️')
        prov = r.get('provenance', 'Unknown')
        tier = relabel_tier(r.get('prov_iter', '?'))
        conf = _confidence_from_tier(tier)
        gt = r.get('expected', '?')
        found = r.get('found') if r.get('found') is not None else r.get('closest')
        found_str = f"{found}" if found is not None else "—"
        print(f"  {icon} {r['id']:<5} {r['verdict']:<5}  gt={gt:<14}  found={found_str:<14}  [{tier}] {prov}")
        print(f"         Confidence: {conf}")

    prov_tiers = {}
    for r in results:
        tier = relabel_tier(r.get('prov_iter', 'Unknown'))
        prov_tiers.setdefault(tier, []).append(r)

    print(f"\n{'='*90}")
    print(f"  PROVENANCE SUMMARY BY TIER")
    print(f"{'='*90}")
    tier_order = ['Raw Table', 'Guessed (Iter 1)', 'Guessed (Iter 2)', 'Guessed (Iter 3)',
                  'Guessed (threshold)', 'Iter 1', 'Iter 2', 'Iter 3', 'Iter 3 (inferred)', 'N/A']
    for tier in tier_order:
        if tier not in prov_tiers: continue
        tests = prov_tiers[tier]
        p = sum(1 for t in tests if t['verdict'] == 'PASS')
        f = sum(1 for t in tests if t['verdict'] == 'FAIL')
        pct = 100 * p // max(len(tests), 1)
        ids = ', '.join(t['id'] for t in tests)
        print(f"  {tier:42s}: {len(tests):2d} tests -> {p:2d} PASS ({pct}%), {f:2d} FAIL  [{ids}]")

    conf_order = ['DETERMINISTIC', 'HEURISTIC', 'INFERRED', 'GUESSED', 'N/A']
    conf_counts = {}
    for r in results:
        tier = relabel_tier(r.get('prov_iter', 'Unknown'))
        conf = _confidence_from_tier(tier)
        conf_counts.setdefault(conf, {'pass': 0, 'fail': 0})
        if r['verdict'] == 'PASS': conf_counts[conf]['pass'] += 1
        else: conf_counts[conf]['fail'] += 1
    print(f"\n  REASONING CONFIDENCE DISTRIBUTION:")
    for conf in conf_order:
        if conf in conf_counts:
            c = conf_counts[conf]
            print(f"    {conf:20s}: {c['pass']+c['fail']:2d} tests ({c['pass']} PASS, {c['fail']} FAIL)")

    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch
    fig, (ax_prov, ax_llm) = plt.subplots(1, 2, figsize=(18, 5))

    # ── Panel 1: Per-Domain Group PASS/FAIL ──
    group_order = ['A','B','C','D','E','F','H','G','P']
    grp_name_map = {'A':'Logistics (A)','B':'Demand (B)','C':'Inventory (C)','D':'Supplier (D)',
                     'E':'Cross-Domain (E)','F':'Indirect (F)','H':'Hard (H)','G':'Fiscal (G)','P':'Critical (P)'}
    grp_pass, grp_fail, grp_labels = [], [], []
    for g in group_order:
        tests = [r for r in results if r['id'].startswith(g)]
        if not tests: continue
        p = sum(1 for t in tests if t['verdict'] == 'PASS')
        f = len(tests) - p
        grp_labels.append(grp_name_map.get(g, g))
        grp_pass.append(p)
        grp_fail.append(f)
    y_pos = range(len(grp_labels))
    ax_prov.barh(y_pos, grp_pass, color='#2e7d32', edgecolor='white', label='PASS')
    ax_prov.barh(y_pos, grp_fail, left=grp_pass, color='#d32f2f', edgecolor='white', label='FAIL')
    ax_prov.set_yticks(y_pos)
    ax_prov.set_yticklabels(grp_labels, fontsize=10)
    ax_prov.set_xlabel('Test Count')
    ax_prov.set_title(f'PASS/FAIL by Domain Group ({current_stage})', fontsize=11, fontweight='bold')
    ax_prov.legend(loc='lower right', fontsize=9)
    ax_prov.invert_yaxis()
    for i, (p, f) in enumerate(zip(grp_pass, grp_fail)):
        total = p + f
        ax_prov.text(total+0.3, i, f'{total} ({p}P/{f}F)', va='center', fontsize=9, fontweight='bold')
    ax_prov.set_xlim(0, max(p+f for p, f in zip(grp_pass, grp_fail)) + 6)

    cat_labels, cat_pass, cat_fail = [], [], []
    cat_colors = {'DETERMINISTIC':'#1b5e20','HEURISTIC':'#689f38','INFERRED':'#f9a825',
                  'GUESSED':'#b71c1c','N/A':'#757575'}
    for conf in conf_order:
        if conf not in conf_counts: continue
        c = conf_counts[conf]
        cat_labels.append(conf)
        cat_pass.append(c['pass'])
        cat_fail.append(c['fail'])
    y_pos2 = range(len(cat_labels))
    bar_colors = [cat_colors.get(c, '#757575') for c in cat_labels]
    bars_total = [p+f for p, f in zip(cat_pass, cat_fail)]
    ax_llm.barh(y_pos2, cat_pass, color=bar_colors, edgecolor='white', alpha=0.9)
    ax_llm.barh(y_pos2, cat_fail, left=cat_pass, color=bar_colors, edgecolor='white', alpha=0.3)
    ax_llm.set_yticks(y_pos2)
    ax_llm.set_yticklabels(cat_labels, fontsize=10)
    ax_llm.set_xlabel('Test Count')
    ax_llm.set_title(f'Reasoning Confidence: Is This Answer Repeatable? ({current_stage})', fontsize=11, fontweight='bold')
    ax_llm.invert_yaxis()
    for i, (p, f) in enumerate(zip(cat_pass, cat_fail)):
        suffix = '' if f == 0 else f' ({f}F)'
        ax_llm.text(p+f+0.3, i, f'{p+f}{suffix}', va='center', fontsize=9, fontweight='bold')
    ax_llm.set_xlim(0, max(bars_total) + 5 if bars_total else 10)
    ax_llm.legend(handles=[Patch(facecolor='#1b5e20', label='DETERMINISTIC'),
        Patch(facecolor='#689f38', label='HEURISTIC'),
        Patch(facecolor='#f9a825', label='INFERRED'),
        Patch(facecolor='#b71c1c', label='GUESSED')],
        loc='lower right', fontsize=7)
    plt.tight_layout()
    plt.show()

if 'assumptions' in dir():
    plot_classification_dashboard(all_stage_results, assumptions)

# Also show comprehensive prompt progression
if all_comp_results:
    print("\n" + "="*90)
    print("  COMPREHENSIVE PROMPT BENCHMARK PROGRESSION")
    print("="*90)
    for label, cr in all_comp_results.items():
        if cr:
            found = cr.get('found', 0)
            total = cr.get('total', 45)
            close = cr.get('close', 0)
            missing = cr.get('missing', 0)
            pct = cr.get('pct', 0)
            print(f"  {label:55s} \u2705 {found}/{total} Found + Correct ({pct}%) | \u26a0 {close} Found + Wrong | \u2b1b {missing} Not Found")
    print("="*90)

# COMMAND ----------

# DBTITLE 1,Deep Reliability Analysis: Key Terms
# MAGIC %md
# MAGIC ## Deep Reliability Analysis: Key Terms
# MAGIC
# MAGIC The cell below analyzes **consistency across iterations** using data already collected (zero API calls). It answers: *"Can we trust these agent-generated metrics in production?"*
# MAGIC
# MAGIC ### SQL Pattern Stability
# MAGIC
# MAGIC Compares the normalized SQL signature (table + aggregation + sorted WHERE conditions) across all 4 iterations. SQL **should change** between iterations — that’s the demo improving the agent. What matters is whether it **stabilizes**.
# MAGIC
# MAGIC | Category | Icon | Meaning |
# MAGIC | --- | --- | --- |
# MAGIC | **STABLE** | 🟢 | Same SQL across ALL iterations — agent locked from the start |
# MAGIC | **STABILIZED** | 🔵 | SQL changed early (improvement), then locked in the last 2 iterations |
# MAGIC | **CONVERGING** | 🟡 | Same table + aggregation approach, but WHERE clause details still differ |
# MAGIC | **VOLATILE** | 🔴 | SQL still changing in the latest iteration — not yet locked |
# MAGIC
# MAGIC ### Other Sections
# MAGIC
# MAGIC * **Provenance Stability** — Did the agent converge from raw base tables to governed UC assets (metric views, SQL functions) over iterations? A shift from `Raw Table` → `Iter 2` → `Iter 3` is the intended progression.
# MAGIC * **Value Reliability** — Did the returned numeric value stabilize? A test that returns 94.57 in 3 of 4 iterations is more reliable than one that returns a different number each time.
# MAGIC * **Flip Analysis** — Which tests changed verdict (PASS↔FAIL) between iterations? Flips from FAIL→PASS are expected improvements. Flips from PASS→FAIL are regressions that need investigation.
# MAGIC * **Reliability Matrix** — Per-confidence-tier production readiness: are DETERMINISTIC tests more reliable than INFERRED ones?

# COMMAND ----------

# DBTITLE 1,Robustness Test: Key Terms
# MAGIC %md
# MAGIC ## Robustness Test: Key Terms
# MAGIC
# MAGIC The cell below sends **10 hardest questions × 3 genuine rephrased variations = 30 live API calls** to test whether the agent gives consistent answers regardless of how the question is worded.
# MAGIC
# MAGIC ### Reliability vs Dependability
# MAGIC
# MAGIC | Metric | Question It Answers | How It’s Measured |
# MAGIC | --- | --- | --- |
# MAGIC | **Reliability** | Did the agent get the **right answer**? | % of variations that matched ground truth |
# MAGIC | **Dependability** | Did the agent use the **same SQL approach**? | SQL signature consistency across variations |
# MAGIC
# MAGIC A test can be **high reliability / low dependability** — it got the right answer every time, but via different SQL each time. That’s *lucky*, not *governed*. It may break tomorrow.
# MAGIC
# MAGIC ### Dependability Score Levels
# MAGIC
# MAGIC | Score | Label | Meaning |
# MAGIC | --- | --- | --- |
# MAGIC | **100%** | Identical SQL | Same normalized query across all 3 variations |
# MAGIC | **85%** | Same structure, different filters | Same table + aggregation, slightly different WHERE |
# MAGIC | **60%** | Mixed approaches | 2 of 3 variations used the same structural approach |
# MAGIC | **33%** | Genuinely different SQL | Each variation chose a completely different strategy |
# MAGIC
# MAGIC ### Functional Equivalence
# MAGIC
# MAGIC When the SQL differs but ALL variations still produce the correct answer, the test is marked **Functionally Equivalent** (✅). The agent took different paths to the same result — reliable today, but not locked to a single governed approach.
# MAGIC
# MAGIC ### Anti-Bias Design
# MAGIC
# MAGIC Variations use genuinely different vocabulary ("late" ↔ "delayed", "vendors" ↔ "suppliers"), date phrasings ("last month" ↔ "August 2026"), and sentence structures. No variation adds hints or removes context.

# COMMAND ----------

# DBTITLE 1,Comprehensive Prompt Benchmark: Key Terms
# MAGIC %md
# MAGIC ## Comprehensive Prompt Benchmark: Key Terms
# MAGIC
# MAGIC The cell below sends a **single large executive prompt** (the same CFO-style prompt used at baseline) to the Supervisor Agent and scores how many of the 45 ground truth values appear in the response.
# MAGIC
# MAGIC ### What This Measures
# MAGIC
# MAGIC * **Coverage Score** — How many of 45 GT values the Supervisor surfaces in one comprehensive response. Not all 45 are expected — the prompt is broad and the agent must prioritize.
# MAGIC * **Indirect Improvement** — UC features added for individual tests (metric views, SQL functions, column comments) may also improve the Supervisor’s ability to surface those metrics in a single report. This measures that transfer effect.
# MAGIC * **Progression** — Coverage at Baseline vs After Iteration 1 vs After Iteration 2 vs After Iteration 3. If coverage grows, the UC semantic layer is helping even for unstructured comprehensive queries.
# MAGIC
# MAGIC ### Scope: 27 of 45 Tests
# MAGIC
# MAGIC Only **27 tests** come directly from the CFO prompt (see cell 6 mapping table). The other **18 are Curator-added edge cases** — wrong-table traps (H01-H02), status ambiguity (H03-H04), continent drill-downs (D05-D07), indirect derivations (F01-F06), and complements (A02, A06). These require targeted questions, not a broad ask.
# MAGIC
# MAGIC Scoring the Supervisor on all 45 inflates the "missing" count unfairly. The correct denominator is **27**, not 45.
# MAGIC
# MAGIC ### Scoring Categories (in-scope tests only)
# MAGIC
# MAGIC | Category | What It Means |
# MAGIC | --- | --- |
# MAGIC | **Found + Correct** | Supervisor surfaced this metric AND the value matched ground truth |
# MAGIC | **Not Found** | Supervisor didn’t surface this metric in the response |
# MAGIC | **Bonus Coverage** | Curator-added edge cases that the Supervisor found anyway (not expected) |
# MAGIC
# MAGIC ### Important Caveats
# MAGIC
# MAGIC * This benchmark sends **1 API call** to the Supervisor (not 45 individual ones). The Supervisor routes to sub-agents and synthesizes.
# MAGIC * A metric that the Supervisor doesn’t mention is a **prompt-coverage gap** (the Supervisor didn’t ask about it), not an accuracy failure.
# MAGIC * Out-of-scope metrics found are "bonus" — the Supervisor happened to surface them, but they weren’t part of the original prompt.

# COMMAND ----------

# DBTITLE 1,DEEP RELIABILITY ANALYSIS: How Consistent Are Agent Answers?
# ============================================================
# DEEP RELIABILITY ANALYSIS: How Consistent Are Agent Answers?
# ============================================================
# Uses data from all_stage_results (all iterations already run).
# ZERO API calls — pure analysis of existing results.
#
# Answers: "Can we TRUST these agent-generated metrics in production?"
#
# 6 sections:
# 1. SQL Pattern Consistency — same SQL structure across iterations?
# 2. Provenance Stability — does the agent converge to governed assets?
# 3. Value Reliability — does the returned value stabilize?
# 4. Reliability Matrix — per-confidence-tier production readiness
# 5. Flip Analysis — which tests changed verdict between iterations?
# 6. Overall Verdict — can we deploy this?
# ============================================================
import re
from collections import defaultdict, Counter

print("="*110)
print("  DEEP RELIABILITY ANALYSIS: How Consistent Are Agent-Generated Metrics?")
print("="*110)

stages = list(all_stage_results.keys())
print(f"\n  Iterations analyzed: {len(stages)}")
for s in stages:
    p = sum(1 for r in all_stage_results[s] if r.get('verdict') == 'PASS')
    print(f"    {s:35s}: {p}/{len(all_stage_results[s])} PASS")

# Build per-test history across all iterations
test_history = defaultdict(list)
for stage in stages:
    for r in all_stage_results[stage]:
        aid = r['id']
        val = r.get('found') if r.get('found') is not None else r.get('closest')
        test_history[aid].append({
            'stage': stage, 'verdict': r.get('verdict', '?'),
            'value': val, 'expected': r.get('expected'),
            'sql': r.get('sql', ''), 'prov_iter': r.get('prov_iter', '?'),
            'provenance': r.get('provenance', '?'),
        })

# ── 1. SQL PATTERN CONSISTENCY ──────────────────────────────────────────────
print(f"\n{'='*110}")
print(f"  1. SQL PATTERN CONSISTENCY: Does the agent write the same SQL each time?")
print(f"{'='*110}")

def extract_sql_signature(sql):
    """Normalize SQL into a comparable signature: table + aggregation + sorted WHERE conditions.
    Normalizations: (1) collapse whitespace, (2) sort AND conditions,
    (3) strip DATE keyword from date literals, (4) generous cutoff."""
    if not sql: return "(no SQL)"
    # Collapse ALL whitespace (newlines, tabs, multiple spaces) to single space
    sql_lower = ' '.join(sql.lower().replace('`', '').replace('"', '').split())
    # Normalize date literals: DATE '2026-08-01' → '2026-08-01'
    sql_lower = re.sub(r"date\s+'", "'", sql_lower)
    tables = re.findall(r'from\s+(\w+(?:\.\w+)*)', sql_lower)
    aggs = sorted(set(re.findall(r'(count|sum|avg|min|max)\s*\(', sql_lower)))
    where = re.search(r'where\s+(.+?)(?:\s+group\s|\s+order\s|\s+limit\s|\s+having\s|$)', sql_lower)
    if where:
        # Sort AND-separated conditions so clause order doesn't matter
        conditions = sorted(c.strip() for c in re.split(r'\s+and\s+', where.group(1).strip()) if c.strip())
        where_sketch = ' AND '.join(conditions)[:200]
    else:
        where_sketch = "(no WHERE)"
    return f"{tables[0] if tables else '?'} | {'+'.join(aggs) if aggs else 'select'} | {where_sketch}"

def extract_structural_sig(sql):
    """Coarse signature: table + agg functions only (ignores WHERE details).
    Two queries with the same structural sig used the same approach."""
    if not sql: return "(no SQL)"
    sql_lower = ' '.join(sql.lower().replace('`', '').replace('"', '').split())
    tables = re.findall(r'from\s+(\w+(?:\.\w+)*)', sql_lower)
    aggs = sorted(set(re.findall(r'(count|sum|avg|min|max)\s*\(', sql_lower)))
    return f"{tables[0] if tables else '?'} | {'+'.join(aggs) if aggs else 'select'}"

# ── Categorize each test's SQL evolution ──
# SQL SHOULD change across iterations (that's the demo!) — what matters is STABILIZATION.
# Categories:
#   \U0001f7e2 STABLE      = same signature ALL iterations
#   \U0001f535 STABILIZED  = changed early, but last 2 iterations match
#   \U0001f7e1 CONVERGING  = structural approach (table+agg) stabilized, WHERE details differ
#   \U0001f534 VOLATILE    = SQL still changing in the latest iteration

sql_categories = {'STABLE': [], 'STABILIZED': [], 'CONVERGING': [], 'VOLATILE': []}
sql_details = []

for aid in sorted(test_history.keys()):
    hist = test_history[aid]
    sigs = [(extract_sql_signature(h['sql']), extract_structural_sig(h['sql']), h) for h in hist if h['sql']]
    full_sigs = [s[0] for s in sigs]
    struct_sigs = [s[1] for s in sigs]

    if len(full_sigs) <= 1:
        cat = 'STABLE'
    elif len(set(full_sigs)) == 1:
        cat = 'STABLE'
    elif len(full_sigs) >= 2 and full_sigs[-1] == full_sigs[-2]:
        cat = 'STABILIZED'
    elif len(struct_sigs) >= 2 and struct_sigs[-1] == struct_sigs[-2]:
        cat = 'CONVERGING'
    else:
        cat = 'VOLATILE'

    sql_categories[cat].append(aid)
    desc = next((a[4] for a in assumptions if a[0] == aid), "")[:55]
    sql_details.append((aid, cat, desc, sigs))

total_with_sql = sum(1 for aid in test_history if any(h['sql'] for h in test_history[aid]))
stable_n = len(sql_categories['STABLE'])
stabilized_n = len(sql_categories['STABILIZED'])
converging_n = len(sql_categories['CONVERGING'])
volatile_n = len(sql_categories['VOLATILE'])
locked_n = stable_n + stabilized_n  # SQL is locked in the last 2 iterations
locked_pct = 100 * locked_n // max(total_with_sql, 1)

print(f"\n  SQL stabilized in last 2 iterations: {locked_n}/{total_with_sql} ({locked_pct}%)")
print(f"")
print(f"  \U0001f7e2 STABLE      (same SQL ALL iterations):          {stable_n}")
print(f"  \U0001f535 STABILIZED  (changed early, locked in last 2):   {stabilized_n}")
print(f"  \U0001f7e1 CONVERGING  (same table+agg, WHERE differs):     {converging_n}")
print(f"  \U0001f534 VOLATILE    (still changing in latest iteration): {volatile_n}")

# ── Show ALL 45 tests with full (untruncated) signatures ──
cat_icons = {'STABLE': '\U0001f7e2', 'STABILIZED': '\U0001f535', 'CONVERGING': '\U0001f7e1', 'VOLATILE': '\U0001f534'}
print(f"\n  \u2500\u2500 All {len(sql_details)} Tests: SQL Evolution Across Iterations \u2500\u2500")
for aid, cat, desc, sigs in sql_details:
    print(f"\n  {cat_icons[cat]} {aid} [{cat}]: {desc}")
    for full_sig, struct_sig, h in sigs:
        icon = '\u2705' if h['verdict'] == 'PASS' else '\u274c'
        print(f"    {icon} {h['stage']:30s} \u2192 {full_sig}")

# Update sql_consistent_count for downstream sections (pie chart etc.)
sql_consistent_count = locked_n
sql_inconsistent = [(aid, [s[0] for s in sigs], [s[2] for s in sigs]) for aid, cat, desc, sigs in sql_details if cat in ('VOLATILE', 'CONVERGING')]

# ── 2. PROVENANCE STABILITY ─────────────────────────────────────────────────
print(f"\n{'='*110}")
print(f"  2. PROVENANCE STABILITY: Does the agent converge to governed assets?")
print(f"{'='*110}")

def _conf(tier):
    """Map provenance tier to confidence level."""
    if tier in ('Iter 2', 'Iter 3') or tier.startswith('Iter 3'): return 'DETERMINISTIC'
    if tier == 'Iter 1': return 'HEURISTIC'
    if 'Guessed' in str(tier) or tier == 'GUESSED': return 'GUESSED'
    if tier == 'N/A': return 'N/A'
    return 'INFERRED'

converged, stayed_baseline, stayed_det, regressed_prov = [], [], [], []
for aid in sorted(test_history.keys()):
    hist = test_history[aid]
    if len(hist) < 2: continue
    first_c, last_c = _conf(hist[0]['prov_iter']), _conf(hist[-1]['prov_iter'])
    if first_c == 'INFERRED' and last_c == 'DETERMINISTIC': converged.append(aid)
    elif first_c == last_c == 'INFERRED': stayed_baseline.append(aid)
    elif first_c == last_c == 'DETERMINISTIC': stayed_det.append(aid)
    elif first_c in ('DETERMINISTIC','HEURISTIC') and last_c in ('INFERRED','GUESSED'): regressed_prov.append(aid)

print(f"\n  \u2705 Converged to DETERMINISTIC: {len(converged)} tests (raw table \u2192 governed asset)")
if converged: print(f"     {', '.join(converged)}")
print(f"  \u2705 Always DETERMINISTIC: {len(stayed_det)} tests")
if stayed_det: print(f"     {', '.join(stayed_det)}")
print(f"  \u26a0  Always INFERRED (raw table): {len(stayed_baseline)} tests (never used a governed asset)")
if stayed_baseline: print(f"     {', '.join(stayed_baseline)}")
if regressed_prov:
    print(f"  \u274c Regressed: {len(regressed_prov)} tests (governed \u2192 raw)")
    print(f"     {', '.join(regressed_prov)}")

# ── 3. VALUE RELIABILITY ────────────────────────────────────────────────────
print(f"\n{'='*110}")
print(f"  3. VALUE RELIABILITY: Does the returned value stabilize or fluctuate?")
print(f"{'='*110}")

val_stable, val_fluctuating = 0, []
for aid in sorted(test_history.keys()):
    hist = test_history[aid]
    vals = []
    for h in hist:
        v = h.get('value')
        if v is not None:
            try: vals.append(round(float(v), 2))
            except (ValueError, TypeError): pass
    if not vals: continue
    if len(set(vals)) <= 1:
        val_stable += 1
    else:
        val_fluctuating.append((aid, vals, hist[0].get('expected')))

print(f"\n  Stable value: {val_stable}/{len(test_history)} tests returned the SAME value every time")
print(f"  Fluctuating: {len(val_fluctuating)} tests returned DIFFERENT values")
if val_fluctuating:
    print(f"\n  \u2500\u2500 Value Changes \u2500\u2500")
    for aid, vals, gt in val_fluctuating[:12]:
        verdicts = '\u2192'.join(h['verdict'][0] for h in test_history[aid])
        print(f"  {aid}: gt={gt}  values={vals}  verdict_chain={verdicts}")

# ── 4. RELIABILITY MATRIX ───────────────────────────────────────────────────
print(f"\n{'='*110}")
print(f"  4. RELIABILITY MATRIX: Per-Tier Production Readiness")
print(f"{'='*110}")

tier_stats = defaultdict(lambda: {'n':0, 'passes':0, 'runs':0, 'sql_ok':0, 'val_ok':0})
for aid in sorted(test_history.keys()):
    hist = test_history[aid]
    if not hist: continue
    conf = _conf(hist[-1]['prov_iter'])
    s = tier_stats[conf]
    s['n'] += 1
    s['passes'] += sum(1 for h in hist if h['verdict'] == 'PASS')
    s['runs'] += len(hist)
    sigs = [extract_sql_signature(h['sql']) for h in hist if h['sql']]
    if len(set(sigs)) <= 1: s['sql_ok'] += 1
    vals = []
    for h in hist:
        v = h.get('value')
        if v is not None:
            try: vals.append(round(float(v), 2))
            except: pass
    if len(set(vals)) <= 1: s['val_ok'] += 1

print(f"\n  {'Confidence':<16} {'Tests':>6} {'Pass Rate':>10} {'SQL Stable':>12} {'Value Stable':>14} {'Prod Ready?':>13}")
print(f"  {'\u2500'*16} {'\u2500'*6} {'\u2500'*10} {'\u2500'*12} {'\u2500'*14} {'\u2500'*13}")
for conf in ['DETERMINISTIC','HEURISTIC','INFERRED','GUESSED','N/A']:
    s = tier_stats.get(conf)
    if not s or s['n'] == 0: continue
    pr = 100*s['passes']/max(s['runs'],1)
    sq = 100*s['sql_ok']/s['n']
    vl = 100*s['val_ok']/s['n']
    ready = "\u2705 YES" if pr >= 95 and sq >= 80 else ("\u26a0 CONDITIONAL" if pr >= 80 else "\u274c NO")
    print(f"  {conf:<16} {s['n']:>6} {pr:>9.1f}% {sq:>11.0f}% {vl:>13.0f}% {ready:>13}")

# ── 5. FLIP ANALYSIS ────────────────────────────────────────────────────────
print(f"\n{'='*110}")
print(f"  5. FLIP ANALYSIS: Which tests changed verdict between iterations?")
print(f"{'='*110}")

flips = []
for aid in sorted(test_history.keys()):
    hist = test_history[aid]
    for i in range(1, len(hist)):
        if hist[i-1]['verdict'] != hist[i]['verdict']:
            flips.append((aid, hist[i-1]['stage'], hist[i-1]['verdict'],
                          hist[i]['stage'], hist[i]['verdict']))
if flips:
    fixes = sum(1 for f in flips if f[4] == 'PASS')
    breaks = sum(1 for f in flips if f[4] == 'FAIL')
    print(f"\n  Total flips: {len(flips)} ({fixes} fixes FAIL\u2192PASS, {breaks} regressions PASS\u2192FAIL)")
    print(f"\n  {'Test':<6} {'From':<30} {'Was':<6} {'To':<30} {'Now':<6}")
    print(f"  {'\u2500'*6} {'\u2500'*30} {'\u2500'*6} {'\u2500'*30} {'\u2500'*6}")
    for aid, fs, fv, ts, tv in flips:
        icon = '\U0001f53c' if tv == 'PASS' else '\U0001f53d'
        print(f"  {icon} {aid:<5} {fs:<30} {fv:<6} {ts:<30} {tv}")
else:
    print(f"\n  No flips \u2014 all tests had consistent verdicts across all iterations.")

# ── 6. OVERALL VERDICT ──────────────────────────────────────────────────────
print(f"\n{'='*110}")
print(f"  6. OVERALL VERDICT: Can We Deploy This?")
print(f"{'='*110}")

last_stage = stages[-1] if stages else "?"
last_pass = sum(1 for r in all_stage_results.get(last_stage, []) if r.get('verdict') == 'PASS')
total = len(assumptions)
det_n = tier_stats.get('DETERMINISTIC', {}).get('n', 0)

print(f"\n  Latest pass rate ({last_stage}): {last_pass}/{total} ({100*last_pass//total}%)")
print(f"  DETERMINISTIC coverage: {det_n}/{total} ({100*det_n//total}%) tests backed by governed assets")
print(f"  SQL stabilized (locked in last 2 iterations): {locked_n}/{total_with_sql} ({locked_pct}%)")
print(f"  Value stability: {val_stable}/{total} ({100*val_stable//total}%)")
print(f"\n  \u250c{'\u2500'*72}\u2510")
print(f"  \u2502  KEY FINDING: UC semantic features improve RELIABILITY, not just     \u2502")
print(f"  \u2502  accuracy. Tests backed by Metric Views and SQL Functions produce    \u2502")
print(f"  \u2502  consistent SQL and stable values across runs. Tests relying on     \u2502")
print(f"  \u2502  raw table interpretation are probabilistic \u2014 they work today but   \u2502")
print(f"  \u2502  may break with a rephrased question or model update.               \u2502")
print(f"  \u2514{'\u2500'*72}\u2518")
print(f"{'='*110}")

# ── Visual: Reliability Dashboard ──
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
fig, axes = plt.subplots(1, 3, figsize=(20, 5))

# Panel 1: Pass rate progression
ax1 = axes[0]
stage_labels = [s.replace('After ','').replace('Iteration ','Iter ')[:22] for s in stages]
pass_counts = [sum(1 for r in all_stage_results[s] if r.get('verdict')=='PASS') for s in stages]
colors = ['#1565c0'] + ['#0d47a1']*(len(stages)-1)
ax1.bar(range(len(stages)), pass_counts, color=colors, edgecolor='white')
ax1.set_xticks(range(len(stages)))
ax1.set_xticklabels(stage_labels, rotation=30, ha='right', fontsize=9)
ax1.set_ylabel('Tests Passing')
ax1.set_title('Accuracy Progression', fontweight='bold')
ax1.set_ylim(0, total + 5)
ax1.axhline(y=total, color='#2e7d32', linestyle='--', alpha=0.5, label=f'Target ({total})')
for i, v in enumerate(pass_counts):
    ax1.text(i, v+0.5, str(v), ha='center', fontweight='bold')
ax1.legend(fontsize=8)

# Panel 2: Reliability by confidence tier
ax2 = axes[1]
tier_color_map = {'DETERMINISTIC':'#1b5e20','HEURISTIC':'#689f38','INFERRED':'#f9a825','GUESSED':'#b71c1c'}
conf_labels, conf_pass_rates, conf_bar_colors = [], [], []
for conf in ['DETERMINISTIC','HEURISTIC','INFERRED','GUESSED']:
    s = tier_stats.get(conf)
    if not s or s['n'] == 0: continue
    conf_labels.append(conf)
    conf_pass_rates.append(100*s['passes']/max(s['runs'],1))
    conf_bar_colors.append(tier_color_map.get(conf, '#757575'))
y_pos = range(len(conf_labels))
ax2.barh(y_pos, conf_pass_rates, color=conf_bar_colors, edgecolor='white')
ax2.set_yticks(y_pos)
ax2.set_yticklabels(conf_labels, fontsize=10)
ax2.set_xlabel('Pass Rate (%)')
ax2.set_title('Reliability by Confidence Tier', fontweight='bold')
ax2.set_xlim(0, 115)
ax2.axvline(x=92, color='#2e7d32', linestyle='--', alpha=0.5, label='92% Q1 target')
ax2.axvline(x=80, color='#f9a825', linestyle='--', alpha=0.3)
for i, v in enumerate(conf_pass_rates):
    ax2.text(v+1, i, f'{v:.0f}%', va='center', fontweight='bold', fontsize=9)
ax2.invert_yaxis()
ax2.legend(fontsize=8)

# Panel 3: SQL stability pie (4 categories)
ax3 = axes[2]
sizes_pie = [stable_n, stabilized_n, converging_n, volatile_n]
labels_pie = [f'\U0001f7e2 Stable\n({stable_n})', f'\U0001f535 Stabilized\n({stabilized_n})',
              f'\U0001f7e1 Converging\n({converging_n})', f'\U0001f534 Volatile\n({volatile_n})']
colors_pie = ['#1b5e20', '#1565c0', '#f9a825', '#c62828']
# Remove zero-sized slices
filt = [(s, l, c) for s, l, c in zip(sizes_pie, labels_pie, colors_pie) if s > 0]
if filt:
    ax3.pie([f[0] for f in filt], labels=[f[1] for f in filt], colors=[f[2] for f in filt],
            autopct='%1.0f%%', startangle=90, textprops={'fontsize': 10, 'fontweight': 'bold'})
ax3.set_title(f'SQL Stabilization ({locked_n}/{total_with_sql} locked)', fontweight='bold')

plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,ROBUSTNESS TEST: Question Variation Consistency (10 hardest x 3 variations)
# ============================================================
# ROBUSTNESS TEST: Question Variation Consistency
# ============================================================
# Tests whether agent answers are ROBUST to rephrasing.
# 10 hardest questions \u00d7 3 genuine variations each = 30 API calls.
# Same ground truth must be matched regardless of how the question
# is worded.
#
# ANTI-BIAS DESIGN PRINCIPLES:
# - Variations use genuinely different vocabulary, not trivial rewrites
# - Some use formal language, some casual, some restructured
# - Synonym swaps: "late" <-> "delayed", "vendors" <-> "suppliers"
# - Date phrasings: "last month" <-> "August 2026" <-> "in Aug"
# - Region naming: "Western region" <-> "Western territory"
# - No variation adds hints that make the question easier
# - No variation removes context that makes it unanswerable
# - Questions selected for diversity: known flakers, wrong-table
#   traps, multi-condition policies, cross-domain joins
# ============================================================
import time

print("="*110)
print("  ROBUSTNESS TEST: Do Agents Give Consistent Answers to Rephrased Questions?")
print("="*110)
print("  10 hardest questions \u00d7 3 variations each = 30 API calls")
print("  Same ground truth must match every variation.")
print("  Estimated time: ~15 minutes (with rate limiting)\n")

ROBUSTNESS_TESTS = [
    {
        "id": "A02", "agent": "logistics", "gt": 94.57,
        "desc": "Late delivery rate (known non-deterministic flaker)",
        "variations": [
            "What percentage of August 2026 shipments to the Western region were delayed?",
            "For Western deliveries last month, what share arrived late?",
            "Calculate the delayed shipment rate for the Western territory in August",
        ]
    },
    {
        "id": "A03", "agent": "logistics", "gt": 2.94,
        "desc": "Avg delay days for late shipments (filter sensitivity)",
        "variations": [
            "How many days on average were Western region delayed shipments late in August?",
            "What is the mean delay duration for delayed Western deliveries last month?",
            "Average number of days late for delayed Western shipments in Aug 2026",
        ]
    },
    {
        "id": "D04", "agent": "supplier", "gt": 8.69,
        "desc": "Avg lead time variance (wrong-table trap: supplier_lead_times vs supplier_orders)",
        "variations": [
            "What was the mean lead time deviation across all vendors in August 2026?",
            "How many days on average did supplier deliveries deviate from planned lead times last month?",
            "Across all purchase orders in August, what is the average lead time variance?",
        ]
    },
    {
        "id": "F02", "agent": "supplier", "gt": 75.00,
        "desc": "Vendor late rate (per-order vs per-distinct-vendor trap)",
        "variations": [
            "What is the vendor late delivery rate for August 2026?",
            "What share of purchase orders had late deliveries last month?",
            "Late delivery percentage across all supplier orders in August",
        ]
    },
    {
        "id": "H03", "agent": "demand", "gt": 71.23,
        "desc": "Fulfillment rate (Fulfilled vs Partially_Fulfilled status trap)",
        "variations": [
            "What percentage of Western orders were fully fulfilled in August 2026?",
            "For the Western region, what is the fulfilled order rate last month?",
            "How many Western region orders last month had status Fulfilled as a percentage of total?",
        ]
    },
    {
        "id": "H05", "agent": "executive", "gt": 3138569.66,
        "desc": "Revenue at risk (cross-domain: demand + logistics aggregation)",
        "variations": [
            "How much revenue is at risk in the Western region from cancellations, backorders, and late shipment costs combined?",
            "Calculate the total financial exposure from Western supply chain disruptions including cancelled orders, backordered revenue, and wasted freight",
            "What is the combined revenue impact of cancellations, backorders, and freight waste in the Western region?",
        ]
    },
    {
        "id": "G01", "agent": "executive", "gt": 92.0,
        "desc": "Q1 service-level target (fiscal calendar trap: Q1=Jul-Sep, current quarter)",
        "variations": [
            "What is our Q1 service level target and are we at risk of missing it?",
            "Will we hit our first quarter service-level goals?",
            "How do our current service levels compare to the Q1 target?",
        ]
    },
    {
        "id": "P01", "agent": "logistics", "gt": 176,
        "desc": "Logistics Risk Standards (multi-condition: delay>=5 AND weight>800)",
        "variations": [
            "Count of August deliveries that triggered the logistics risk policy",
            "How many shipments last month violated the Logistics Risk Standards?",
            "Total shipments flagged by logistics risk criteria in August 2026",
        ]
    },
    {
        "id": "P04", "agent": "supplier", "gt": 11,
        "desc": "Procurement Quality Minimum (needs BOTH quality<75 AND ltv>12)",
        "variations": [
            "How many August purchase orders failed to meet the procurement quality threshold?",
            "Count of vendor orders last month that were below the quality minimum standard",
            "Number of supplier POs in August that didn't meet Procurement Quality Minimum",
        ]
    },
    {
        "id": "P02", "agent": "demand", "gt": 77,
        "desc": "Demand Anomaly Alert (3-condition: qty>=8 AND price<30 AND Online)",
        "variations": [
            "Count of Western August orders that set off a demand anomaly flag",
            "How many orders in the Western territory last month were flagged as demand anomalies?",
            "Total demand anomaly alerts for Western region orders in August 2026",
        ]
    },
]

# Confidence helper (in case cell 20 didn't run)
def _conf_robust(tier):
    if tier in ('Iter 2', 'Iter 3') or tier.startswith('Iter 3'): return 'DETERMINISTIC'
    if tier == 'Iter 1': return 'HEURISTIC'
    if 'Guessed' in str(tier) or tier == 'GUESSED': return 'GUESSED'
    if tier == 'N/A': return 'N/A'
    return 'INFERRED'

# ── Run all variations ──
robustness_results = []
total_calls = sum(len(t["variations"]) for t in ROBUSTNESS_TESTS)
call_num = 0

for test in ROBUSTNESS_TESTS:
    aid, agent, gt = test["id"], test["agent"], test["gt"]
    space_id = spaces.get(agent)
    print(f"\n  {'\u2500'*100}")
    print(f"  {aid}: {test['desc']} (gt={gt}, agent={agent})")
    print(f"  {'\u2500'*100}")

    if not space_id:
        print(f"  \u26a0 Agent '{agent}' not found \u2014 skipping")
        continue

    for vi, question in enumerate(test["variations"]):
        call_num += 1
        if call_num > 1:
            time.sleep(2)  # Rate limit: avoid 429 RESOURCE_EXHAUSTED
        print(f"\n    V{vi+1} [{call_num}/{total_calls}]: {question}")
        msg = ask_genie(space_id, question)

        if "error" in msg:
            err = msg.get("error", "unknown")
            print(f"    \u274c ERROR: {err}")
            robustness_results.append({"id": aid, "vi": vi+1, "question": question,
                "gt": gt, "verdict": "ERROR", "found": None, "sql": None, "prov_iter": "N/A"})
            continue

        sql, narration, rows, cols = extract_from_msg(msg)
        all_text = (narration or "") + " " + (sql or "")
        for row in rows:
            for val in row.values(): all_text += f" {val}"

        match, found, closest = find_value_in_text(all_text, gt)
        prov_iter, prov_feat, _ = detect_provenance(sql, aid)
        verdict = "PASS" if match else "FAIL"
        icon = "\u2705" if match else "\u274c"
        val_str = f"found={found}" if found is not None else f"closest={closest}"
        print(f"    {icon} {verdict} ({val_str})  [{prov_iter}] {prov_feat}")

        if not match and sql:
            for line in sql.strip().split("\n")[:4]:
                print(f"       \u2502 {line}")

        robustness_results.append({"id": aid, "vi": vi+1, "question": question,
            "gt": gt, "verdict": verdict, "found": found, "closest": closest,
            "sql": sql, "prov_iter": prov_iter, "provenance": prov_feat})

# ── Per-Question Summary ──
print(f"\n{'='*110}")
print(f"  ROBUSTNESS SUMMARY: Per-Question Consistency")
print(f"{'='*110}")

print(f"\n  {'Test':<6} {'GT':<14} {'V1':<6} {'V2':<6} {'V3':<6} {'Score':<8} {'Robust?':<10} {'Description'}")
print(f"  {'\u2500'*6} {'\u2500'*14} {'\u2500'*6} {'\u2500'*6} {'\u2500'*6} {'\u2500'*8} {'\u2500'*10} {'\u2500'*40}")

per_test_scores = {}
for test in ROBUSTNESS_TESTS:
    aid = test["id"]
    test_res = [r for r in robustness_results if r["id"] == aid]
    verdicts = [r["verdict"] for r in test_res]
    v_icons = ['\u2705' if v == 'PASS' else ('\u274c' if v == 'FAIL' else '\u26a0') for v in verdicts]
    while len(v_icons) < 3: v_icons.append('\u2500')
    passes = sum(1 for v in verdicts if v == 'PASS')
    total_v = len(verdicts)
    score = f"{passes}/{total_v}"
    robust = "\u2705 YES" if passes == total_v else ("\u26a0 PARTIAL" if passes > 0 else "\u274c NO")
    per_test_scores[aid] = (passes, total_v)
    print(f"  {aid:<6} {test['gt']:<14} {v_icons[0]:<6} {v_icons[1]:<6} {v_icons[2]:<6} {score:<8} {robust:<10} {test['desc'][:45]}")

total_pass = sum(v[0] for v in per_test_scores.values())
total_run = sum(v[1] for v in per_test_scores.values())
fully_robust = sum(1 for p, t in per_test_scores.values() if p == t)
robust_pct = 100 * total_pass // max(total_run, 1)

print(f"\n  {'\u2500'*110}")
print(f"  OVERALL: {total_pass}/{total_run} variations PASS ({robust_pct}%)")
print(f"  Fully robust (3/3): {fully_robust}/{len(ROBUSTNESS_TESTS)} questions")
print(f"  Partially robust:   {sum(1 for p,t in per_test_scores.values() if 0 < p < t)}/{len(ROBUSTNESS_TESTS)}")
print(f"  Fragile (0/3):      {sum(1 for p,t in per_test_scores.values() if p == 0)}/{len(ROBUSTNESS_TESTS)}")

# ── Robustness by Provenance Tier ──
print(f"\n  ROBUSTNESS BY GOVERNANCE TIER:")
tier_robust = defaultdict(lambda: {'pass': 0, 'total': 0})
for r in robustness_results:
    conf = _conf_robust(r.get('prov_iter', 'Unknown'))
    tier_robust[conf]['total'] += 1
    if r['verdict'] == 'PASS': tier_robust[conf]['pass'] += 1

for conf in ['DETERMINISTIC', 'HEURISTIC', 'INFERRED', 'GUESSED']:
    s = tier_robust.get(conf)
    if not s or s['total'] == 0: continue
    rpct = 100 * s['pass'] // s['total']
    print(f"    {conf:<18}: {s['pass']}/{s['total']} ({rpct}%)")

print(f"\n  \u250c{'\u2500'*72}\u2510")
print(f"  \u2502  KEY FINDING: Questions backed by DETERMINISTIC governance          \u2502")
print(f"  \u2502  (Metric Views, SQL Functions) should be robust to rephrasing.      \u2502")
print(f"  \u2502  INFERRED-tier questions may fail when vocabulary changes because the    \u2502")
print(f"  \u2502  agent relies on column-name matching, which is fragile.            \u2502")
print(f"  \u2514{'\u2500'*72}\u2518")
print(f"{'='*110}")

# ══════════════════════════════════════════════════════════════════════════════
# LARGE VISUAL: Reliability & Dependability Scorecard
# ══════════════════════════════════════════════════════════════════════════════
# RELIABILITY  = Did the agent return the CORRECT answer? (accuracy)
# DEPENDABILITY = Did the agent use the SAME SQL approach? (consistency)
# A high-reliability, low-dependability test is LUCKY — it got the right
# answer but via different SQL each time (fragile, may break tomorrow).
# ══════════════════════════════════════════════════════════════════════════════
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.patches import Patch, FancyBboxPatch
from matplotlib.colors import ListedColormap
import numpy as np
from collections import defaultdict

# ── Compute per-test RELIABILITY & DEPENDABILITY scores ──
def _extract_sig(sql):
    """Normalized SQL signature: collapses whitespace, sorts AND conditions.
    Returns (structural_sig, full_sig) tuple:
      structural = table + agg functions only (coarse — catches same-approach-different-filters)
      full       = table + agg + sorted WHERE conditions (fine — catches exact SQL match)"""
    if not sql: return ("(no SQL)", "(no SQL)")
    # Collapse ALL whitespace to single spaces
    sl = ' '.join(sql.lower().replace('`', '').replace('"', '').split())
    # Normalize date literals: DATE '2026-08-01' → '2026-08-01'
    sl = re.sub(r"date\s+'", "'", sl)
    tables = re.findall(r'from\s+(\w+(?:\.\w+)*)', sl)
    aggs = sorted(set(re.findall(r'(count|sum|avg|min|max)\s*\(', sl)))
    table_str = tables[0] if tables else '?'
    agg_str = '+'.join(aggs) if aggs else 'sel'
    where = re.search(r'where\s+(.+?)(?:\s+group\s|\s+order\s|\s+limit\s|\s+having\s|$)', sl)
    if where:
        conditions = sorted(c.strip() for c in re.split(r'\s+and\s+', where.group(1).strip()) if c.strip())
        where_sketch = ' AND '.join(conditions)[:150]
    else:
        where_sketch = "(none)"
    structural_sig = f"{table_str}|{agg_str}"
    full_sig = f"{table_str}|{agg_str}|{where_sketch}"
    return (structural_sig, full_sig)

test_scores = []
for test in ROBUSTNESS_TESTS:
    aid = test["id"]
    test_res = [r for r in robustness_results if r["id"] == aid]
    passes = sum(1 for r in test_res if r["verdict"] == "PASS")
    total_v = len(test_res) if test_res else 3
    reliability = 100 * passes / max(total_v, 1)

    # DEPENDABILITY: multi-level SQL consistency across variations
    # Level 1 (structural): same table + same agg functions — structurally equivalent
    # Level 2 (full): same table + same agg + same sorted WHERE — identical SQL
    sig_pairs = [_extract_sig(r.get("sql")) for r in test_res if r.get("sql")]
    structural_sigs = [s[0] for s in sig_pairs]
    full_sigs = [s[1] for s in sig_pairs]
    if len(sig_pairs) >= 2:
        unique_full = len(set(full_sigs))
        unique_structural = len(set(structural_sigs))
        if unique_full == 1:
            dependability = 100
            dep_label = "Identical SQL"
        elif unique_structural == 1:
            # Same table + same agg, different WHERE details — structurally equivalent
            dependability = 85
            dep_label = "Same structure, different filters"
        elif unique_structural == 2:
            dependability = 60
            dep_label = "Mixed approaches"
        else:
            dependability = 33
            dep_label = "Genuinely different SQL"
    elif len(sig_pairs) == 1:
        dependability = 50
        dep_label = "Only 1 SQL available"
    else:
        dependability = 0
        dep_label = "No SQL generated"

    tiers = [_conf_robust(r.get('prov_iter', 'Unknown')) for r in test_res]

    # Functional equivalence: different SQL but same correct result
    if dependability < 100 and reliability == 100:
        func_equiv = True  # all PASS despite different SQL
    else:
        func_equiv = False

    test_scores.append({
        'id': aid, 'desc': test['desc'][:42],
        'reliability': round(reliability, 1),
        'dependability': min(100, round(dependability, 1)),
        'dep_label': dep_label,
        'func_equiv': func_equiv,
        'passes': passes, 'total': total_v,
        'verdicts': [r.get('verdict', '?') for r in test_res],
        'tiers': tiers,
        'structural_sigs': structural_sigs,
        'full_sigs': full_sigs,
    })

# ── Print scores table ──
print(f"\n{'='*110}")
print(f"  RELIABILITY & DEPENDABILITY SCORES (per test)")
print(f"{'='*110}")
print(f"\n  {'Test':<6} {'Reliability':>12} {'Dependability':>14} {'SQL Assessment':<30} {'Equiv?':<8} {'Tier':<16} {'Description'}")
print(f"  {'\u2500'*6} {'\u2500'*12} {'\u2500'*14} {'\u2500'*30} {'\u2500'*8} {'\u2500'*16} {'\u2500'*40}")
for s in test_scores:
    tier_str = '/'.join(set(s['tiers'])) if s['tiers'] else '?'
    eq_str = '\u2705 Yes' if s['func_equiv'] else '\u2500'
    print(f"  {s['id']:<6} {s['reliability']:>11.0f}% {s['dependability']:>13.0f}% {s['dep_label']:<30} {eq_str:<8} {tier_str:<16} {s['desc']}")

avg_rel = sum(s['reliability'] for s in test_scores) / len(test_scores)
avg_dep = sum(s['dependability'] for s in test_scores) / len(test_scores)
func_equiv_count = sum(1 for s in test_scores if s['func_equiv'])
print(f"\n  AVERAGES: Reliability={avg_rel:.0f}%  Dependability={avg_dep:.0f}%")
print(f"  Functionally equivalent (different SQL, same correct result): {func_equiv_count}/{len(test_scores)}")

print(f"\n  \u250c{'\u2500'*80}\u2510")
print(f"  \u2502  WHAT THE SCORES MEAN:                                                         \u2502")
print(f"  \u2502  100%  Identical SQL      = same normalized query every time \u2014 fully locked      \u2502")
print(f"  \u2502   85%  Same structure      = same table + agg, slightly different WHERE filters   \u2502")
print(f"  \u2502   60%  Mixed approaches    = 2 of 3 used the same table/agg                      \u2502")
print(f"  \u2502   33%  Genuinely different = each variation chose a different SQL strategy         \u2502")
print(f"  \u2502                                                                                   \u2502")
print(f"  \u2502  FUNCTIONAL EQUIVALENCE (\u2705): SQL is different but ALL variations                 \u2502")
print(f"  \u2502  produced the correct answer. The agent took different paths to the same           \u2502")
print(f"  \u2502  result \u2014 reliable today, but not locked to a single approach.                    \u2502")
print(f"  \u2514{'\u2500'*80}\u2518")
print(f"{'='*110}")

# ══════════════════════════════════════════════════════════════════════════════
# FIGURE: 4-Panel Robustness Scorecard (22" x 14")
# ══════════════════════════════════════════════════════════════════════════════
fig = plt.figure(figsize=(22, 14))
fig.patch.set_facecolor('#fafafa')
gs = gridspec.GridSpec(2, 2, height_ratios=[1, 1.1], hspace=0.35, wspace=0.28)

test_ids = [s['id'] for s in test_scores]
rel_scores = [s['reliability'] for s in test_scores]
dep_scores = [s['dependability'] for s in test_scores]

# ══ Panel 1 (top-left): RELIABILITY SCORE per test ══
ax1 = fig.add_subplot(gs[0, 0])
rel_colors = ['#1b5e20' if r == 100 else '#f9a825' if r > 0 else '#b71c1c' for r in rel_scores]
ax1.bar(range(len(test_ids)), rel_scores, color=rel_colors, edgecolor='white', width=0.7)
ax1.set_xticks(range(len(test_ids)))
ax1.set_xticklabels(test_ids, fontsize=11, fontweight='bold')
ax1.set_ylabel('Reliability Score (%)', fontsize=12)
ax1.set_title('RELIABILITY SCORE\nDoes the agent get the RIGHT answer regardless of phrasing?',
              fontsize=13, fontweight='bold', pad=15)
ax1.set_ylim(0, 118)
ax1.axhline(y=100, color='#1b5e20', linestyle='--', alpha=0.3)
ax1.axhline(y=67, color='#f9a825', linestyle='--', alpha=0.15)
for i, (v, s) in enumerate(zip(rel_scores, test_scores)):
    ax1.text(i, v + 2, f'{v:.0f}%', ha='center', fontsize=10, fontweight='bold')
    ax1.text(i, -7, f'{s["passes"]}/{s["total"]}', ha='center', fontsize=8, color='#555')
ax1.legend(handles=[
    Patch(facecolor='#1b5e20', label='100% \u2014 Fully reliable'),
    Patch(facecolor='#f9a825', label='33-67% \u2014 Partial'),
    Patch(facecolor='#b71c1c', label='0% \u2014 Unreliable')],
    loc='upper right', fontsize=9, framealpha=0.9)

# ══ Panel 2 (top-right): DEPENDABILITY SCORE per test ══
ax2 = fig.add_subplot(gs[0, 1])
dep_colors = ['#0d47a1' if d >= 100 else '#1976d2' if d >= 85 else '#42a5f5' if d >= 60 else '#ef6c00' if d > 0 else '#b71c1c' for d in dep_scores]
ax2.bar(range(len(test_ids)), dep_scores, color=dep_colors, edgecolor='white', width=0.7)
ax2.set_xticks(range(len(test_ids)))
ax2.set_xticklabels(test_ids, fontsize=11, fontweight='bold')
ax2.set_ylabel('Dependability Score (%)', fontsize=12)
ax2.set_title('DEPENDABILITY SCORE\nDoes the agent use the SAME SQL approach regardless of phrasing?',
              fontsize=13, fontweight='bold', pad=15)
ax2.set_ylim(0, 118)
ax2.axhline(y=100, color='#0d47a1', linestyle='--', alpha=0.3)
ax2.axhline(y=67, color='#42a5f5', linestyle='--', alpha=0.15)
for i, v in enumerate(dep_scores):
    ax2.text(i, v + 2, f'{v:.0f}%', ha='center', fontsize=10, fontweight='bold')
ax2.legend(handles=[
    Patch(facecolor='#0d47a1', label='100% \u2014 Identical SQL'),
    Patch(facecolor='#1976d2', label='85% \u2014 Same structure, different filters'),
    Patch(facecolor='#42a5f5', label='60% \u2014 Mixed approaches'),
    Patch(facecolor='#ef6c00', label='33% \u2014 Genuinely different SQL'),
    Patch(facecolor='#b71c1c', label='0% \u2014 No SQL / all errors')],
    loc='upper right', fontsize=9, framealpha=0.9)

# ══ Panel 3 (bottom-left): VARIATION HEATMAP (10 tests \u00d7 3 variations) ══
ax3 = fig.add_subplot(gs[1, 0])
heatmap_data = np.full((len(test_scores), 3), np.nan)
for i, s in enumerate(test_scores):
    for j, v in enumerate(s['verdicts'][:3]):
        heatmap_data[i, j] = 1 if v == 'PASS' else (-1 if v == 'ERROR' else 0)
cmap = ListedColormap(['#c62828', '#9e9e9e', '#2e7d32'])  # FAIL=red, ERROR=gray, PASS=green
im = ax3.imshow(heatmap_data, cmap=cmap, aspect='auto', vmin=-1, vmax=1)
ax3.set_xticks([0, 1, 2])
ax3.set_xticklabels(['Variation 1', 'Variation 2', 'Variation 3'], fontsize=12, fontweight='bold')
ax3.set_yticks(range(len(test_scores)))
ax3.set_yticklabels([f"{s['id']}" for s in test_scores], fontsize=12, fontweight='bold')
ax3.set_title('VARIATION HEATMAP\nGreen = PASS  |  Red = FAIL  |  Gray = ERROR',
              fontsize=13, fontweight='bold', pad=15)
for i in range(len(test_scores)):
    for j in range(min(3, len(test_scores[i]['verdicts']))):
        v = test_scores[i]['verdicts'][j]
        color = 'white' if v in ('PASS', 'FAIL') else 'black'
        ax3.text(j, i, v, ha='center', va='center', fontsize=11, fontweight='bold', color=color)
for i in range(len(test_scores) + 1):
    ax3.axhline(y=i-0.5, color='white', linewidth=2.5)
for j in range(4):
    ax3.axvline(x=j-0.5, color='white', linewidth=2.5)

# ══ Panel 4 (bottom-right): OVERALL SCORECARD ══
ax4 = fig.add_subplot(gs[1, 1])
ax4.set_xlim(0, 10)
ax4.set_ylim(0, 10)
ax4.axis('off')

# Compute overall scores
overall_reliability = sum(s['reliability'] for s in test_scores) / len(test_scores)
overall_dependability = sum(s['dependability'] for s in test_scores) / len(test_scores)
fully_robust_n = sum(1 for s in test_scores if s['reliability'] == 100)
fully_depend_n = sum(1 for s in test_scores if s['dependability'] >= 100)

# Big RELIABILITY box
rel_color = '#1b5e20' if overall_reliability >= 80 else '#f9a825' if overall_reliability >= 50 else '#b71c1c'
rect1 = FancyBboxPatch((0.2, 5.8), 4.3, 3.5, boxstyle="round,pad=0.3",
                         facecolor=rel_color, alpha=0.12, edgecolor=rel_color, linewidth=3)
ax4.add_patch(rect1)
ax4.text(2.35, 8.5, 'RELIABILITY', ha='center', fontsize=15, fontweight='bold', color=rel_color)
ax4.text(2.35, 7.0, f'{overall_reliability:.0f}%', ha='center', fontsize=48, fontweight='bold', color=rel_color)
ax4.text(2.35, 6.15, f'{fully_robust_n}/10 fully robust', ha='center', fontsize=11, color='#555')

# Big DEPENDABILITY box
dep_color = '#0d47a1' if overall_dependability >= 80 else '#42a5f5' if overall_dependability >= 50 else '#ef6c00'
rect2 = FancyBboxPatch((5.5, 5.8), 4.3, 3.5, boxstyle="round,pad=0.3",
                         facecolor=dep_color, alpha=0.12, edgecolor=dep_color, linewidth=3)
ax4.add_patch(rect2)
ax4.text(7.65, 8.5, 'DEPENDABILITY', ha='center', fontsize=15, fontweight='bold', color=dep_color)
ax4.text(7.65, 7.0, f'{overall_dependability:.0f}%', ha='center', fontsize=48, fontweight='bold', color=dep_color)
ax4.text(7.65, 6.15, f'{fully_depend_n}/10 fully consistent', ha='center', fontsize=11, color='#555')

# Interpretation section
ax4.text(5, 5.0, '\u2500' * 50, ha='center', fontsize=10, color='#ccc')
ax4.text(5, 4.4, f'{total_pass}/{total_run} total variations passed ({robust_pct}%)',
         ha='center', fontsize=12, fontweight='bold', color='#333')

# Governance comparison
gov_pass = tier_robust.get('DETERMINISTIC', {}).get('pass', 0)
gov_total = tier_robust.get('DETERMINISTIC', {}).get('total', 0)
base_pass = tier_robust.get('INFERRED', {}).get('pass', 0)
base_total = tier_robust.get('INFERRED', {}).get('total', 0)
guess_pass = tier_robust.get('GUESSED', {}).get('pass', 0)
guess_total = tier_robust.get('GUESSED', {}).get('total', 0)

y_pos = 3.5
if gov_total > 0:
    ax4.text(5, y_pos, f'\u2705 Governed (DETERMINISTIC): {gov_pass}/{gov_total} ({100*gov_pass//max(gov_total,1)}%)',
             ha='center', fontsize=12, color='#1b5e20', fontweight='bold')
    y_pos -= 0.65
if base_total > 0:
    ax4.text(5, y_pos, f'\u26a0  Ungoverned (INFERRED): {base_pass}/{base_total} ({100*base_pass//max(base_total,1)}%)',
             ha='center', fontsize=12, color='#e65100', fontweight='bold')
    y_pos -= 0.65
if guess_total > 0:
    ax4.text(5, y_pos, f'\u274c Guessed (no UC feature): {guess_pass}/{guess_total} ({100*guess_pass//max(guess_total,1)}%)',
             ha='center', fontsize=12, color='#b71c1c', fontweight='bold')
    y_pos -= 0.65

# Key insight
ax4.text(5, 1.2, 'Governed assets produce reliable, dependable answers.',
         ha='center', fontsize=12, fontstyle='italic', color='#1b5e20', fontweight='bold')
ax4.text(5, 0.5, 'Ungoverned answers break when questions are rephrased.',
         ha='center', fontsize=12, fontstyle='italic', color='#b71c1c', fontweight='bold')

fig.suptitle('ROBUSTNESS SCORECARD: Reliability & Dependability Under Question Variation',
             fontsize=17, fontweight='bold', y=0.99, color='#1a237e')
plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.show()

# COMMAND ----------

# DBTITLE 1,COMPREHENSIVE PROMPT BENCHMARK: LIVE Supervisor Call + Consistency Analysis
# ============================================================
# COMPREHENSIVE PROMPT BENCHMARK: LIVE Supervisor Call
#
# Sends the SAME large executive prompt to the Supervisor Agent
# and scores how many GT values appear in the response.
#
# SCOPE: The large prompt directly asks about 27 of the 45 tests.
# The other 18 are Curator-added edge cases (wrong-table traps,
# status ambiguity, continent drill-downs, indirect derivations)
# that the prompt never mentions. Scoring the Supervisor on those
# 18 is unfair — they require targeted questions, not a broad ask.
#
# SCORING (in-scope tests only):
#   1. FOUND + CORRECT  — Supervisor surfaced it, value matched GT
#   2. FOUND + WRONG    — Supervisor mentioned it, but value was off
#   3. NOT FOUND        — Supervisor didn’t surface this metric
#
# Out-of-scope tests are shown separately as "bonus coverage."
# ============================================================

# ── Define prompt scope from cell 6 mapping ──
# These 27 tests map directly to a fragment of the CFO prompt.
# The other 18 are Curator-added edge cases (A02,A06,D05-D07,F01-F06,H01-H07).
COMP_PROMPT_DIRECT = {
    # "revenue decline vs July — August and July revenue, dollar change, pct change"
    'B01', 'B02', 'B03', 'B04',
    # "on-time delivery rate and average delay"
    'A01', 'A03',
    # "How many total shipments went out and how many were late?"
    'A04', 'A05',
    # "current fill rate"
    'E01',
    # "inventory positions below safety stock, unique SKUs, days of supply, stocked out"
    'C01', 'C02', 'C03', 'C04', 'C05',
    # "percentage of vendors delivered late, POs late, lead time variance, SLA penalties"
    'D01', 'D02', 'D03', 'D04', 'E02',
    # "Cost of Disruption by region"
    'E03',
    # "miss our Q1 service-level targets"
    'G01', 'G02',
    # "Logistics Risk Standards / Demand Anomaly / Inventory Standards / Quality Min / Disruption Threshold"
    'P01', 'P02', 'P03', 'P04', 'P05',
}

print("="*90)
print("  COMPREHENSIVE PROMPT BENCHMARK: After Iteration 3 (LIVE)")
print("="*90)
print(f"  Scope: {len(COMP_PROMPT_DIRECT)} tests directly from the CFO prompt")
print(f"  Out of scope: {45 - len(COMP_PROMPT_DIRECT)} Curator-added edge cases (shown separately)")
print("  Sending the full executive prompt to the Supervisor Agent...\n")

# Use stored results if available from iteration cells, otherwise make live call
comp_iter3 = all_comp_results.get("After Iteration 3")
if not comp_iter3:
    comp_iter3 = run_comprehensive_benchmark("After Iteration 3 (all 13 UC features)")
    all_comp_results["After Iteration 3"] = comp_iter3

# ── Progression table ──
print(f"\n{'='*90}")
print("  COMPREHENSIVE PROMPT PROGRESSION")
print(f"{'='*90}")

stages = []
try:
    if comp_baseline:
        stages.append(("Baseline (no UC features)", comp_baseline))
except NameError:
    pass
try:
    if comp_iter1:
        stages.append(("After Iter 1 (comments + examples)", comp_iter1))
except NameError:
    pass
if comp_iter3:
    stages.append(("After Iter 3 (all UC features)", comp_iter3))

if stages:
    print(f"\n  {'Stage':<50} {'All 45':>8} {'In-Scope':>10} {'Coverage':>10}")
    print(f"  {'\u2500'*50} {'\u2500'*8} {'\u2500'*10} {'\u2500'*10}")
    for label, data in stages:
        all_found = {r['id'] for r in data['results'] if 'FOUND' in r['verdict'] and 'NOT' not in r['verdict']}
        in_scope_n = len(all_found & COMP_PROMPT_DIRECT)
        in_pct = 100 * in_scope_n // max(len(COMP_PROMPT_DIRECT), 1)
        print(f"  {label:<50} {data['found']:>3}/45   {in_scope_n:>4}/{len(COMP_PROMPT_DIRECT):<4} {in_pct:>9}%")
else:
    print("  (No benchmark data \u2014 run a full E2E to populate comp_baseline)")

# ── Per-metric consistency analysis ──
# Compare the FIRST stage vs the LAST stage to see what changed
if len(stages) >= 2 and comp_iter3:
    first_label, first_data = stages[0]
    last_label, last_data = stages[-1]

    first_found = {r["id"] for r in first_data["results"] if "FOUND" in r["verdict"] and "NOT" not in r["verdict"]}
    last_found  = {r["id"] for r in last_data["results"] if "FOUND" in r["verdict"] and "NOT" not in r["verdict"]}

    consistent_hit  = sorted(first_found & last_found)         # always found
    newly_found     = sorted(last_found - first_found)         # gained after UC features
    regressed       = sorted(first_found - last_found)         # lost (non-determinism?)
    consistent_miss = sorted(
        r["id"] for r in first_data["results"]
        if "NOT_FOUND" in r["verdict"] and r["id"] not in last_found
    )

    def get_desc(results, mid):
        return next((r.get("desc","")[:65] for r in results if r["id"] == mid), "")

    # \u2500\u2500 3-CATEGORY SCORING: In-Scope Tests Only \u2500\u2500
    # Split results into in-scope (directly from prompt) vs out-of-scope (Curator-added)
    in_scope_found = sorted(last_found & COMP_PROMPT_DIRECT)
    in_scope_missing = sorted(COMP_PROMPT_DIRECT - last_found)
    out_scope_found = sorted(last_found - COMP_PROMPT_DIRECT)
    out_scope_missing = sorted((set(r['id'] for r in last_data['results']) - last_found) - COMP_PROMPT_DIRECT)

    print(f"\n{'='*90}")
    print(f"  IN-SCOPE SCORING ({len(COMP_PROMPT_DIRECT)} tests directly from the CFO prompt)")
    print(f"  Stage: {last_label}")
    print(f"{'='*90}")

    print(f"\n  \u2705 FOUND + CORRECT ({len(in_scope_found)}/{len(COMP_PROMPT_DIRECT)}):")
    for mid in in_scope_found:
        print(f"     {mid:<6} {get_desc(last_data['results'], mid)}")

    print(f"\n  \u2b1b NOT FOUND ({len(in_scope_missing)}/{len(COMP_PROMPT_DIRECT)}):")
    for mid in in_scope_missing:
        print(f"     {mid:<6} {get_desc(last_data['results'], mid)}")

    in_scope_pct = 100 * len(in_scope_found) // max(len(COMP_PROMPT_DIRECT), 1)
    print(f"\n  IN-SCOPE COVERAGE: {len(in_scope_found)}/{len(COMP_PROMPT_DIRECT)} ({in_scope_pct}%)")

    # \u2500\u2500 Bonus: Out-of-scope tests the Supervisor found anyway \u2500\u2500
    if out_scope_found:
        print(f"\n  \U0001f31f BONUS COVERAGE ({len(out_scope_found)} Curator-added tests the Supervisor found anyway):")
        for mid in out_scope_found:
            print(f"     {mid:<6} {get_desc(last_data['results'], mid)}")
    if out_scope_missing:
        print(f"\n  \u2500 OUT OF SCOPE ({len(out_scope_missing)} Curator-added tests \u2014 not expected from this prompt):")
        for mid in out_scope_missing[:8]:
            print(f"     {mid:<6} {get_desc(last_data['results'], mid)}")
        if len(out_scope_missing) > 8:
            print(f"     ... and {len(out_scope_missing) - 8} more")

    # \u2500\u2500 Progression: in-scope only \u2500\u2500
    first_in_scope_found = len(first_found & COMP_PROMPT_DIRECT)
    last_in_scope_found = len(in_scope_found)
    print(f"\n  PROGRESSION (in-scope only): {first_in_scope_found}/{len(COMP_PROMPT_DIRECT)} \u2192 {last_in_scope_found}/{len(COMP_PROMPT_DIRECT)}")

    # ── Coverage by group (in-scope only) ──
    print(f"\n  {'\u2500'*80}")
    print(f"  COVERAGE BY METRIC GROUP (in-scope tests only):")
    print(f"  {'Group':<22} {'Scope':>6} {'Baseline':>10} {'After Iter 3':>14} {'Delta':>8}")
    print(f"  {'\u2500'*22} {'\u2500'*6} {'\u2500'*10} {'\u2500'*14} {'\u2500'*8}")
    for prefix, name in [('A','Logistics'), ('B','Revenue'), ('C','Inventory'),
                         ('D','Supplier'), ('E','Cross-domain'), ('F','Indirect'),
                         ('G','Q1 Fiscal'), ('H','Hard derivations'),
                         ('P','Critical Thresholds')]:
        grp_ids = [r["id"] for r in last_data["results"] if r["id"].startswith(prefix)]
        in_scope_ids = [mid for mid in grp_ids if mid in COMP_PROMPT_DIRECT]
        base_hits = sum(1 for mid in in_scope_ids if mid in first_found)
        iter3_hits = sum(1 for mid in in_scope_ids if mid in last_found)
        delta = iter3_hits - base_hits
        delta_str = f"+{delta}" if delta > 0 else str(delta) if delta < 0 else "\u2014"
        scope_str = f"{len(in_scope_ids)}/{len(grp_ids)}"
        if not in_scope_ids:
            print(f"  {name:<22} {scope_str:>6} {'\u2014':>10} {'\u2014':>14} {'\u2014':>8}  (all Curator-added)")
        else:
            print(f"  {name:<22} {scope_str:>6} {base_hits:>4}/{len(in_scope_ids):<5} {iter3_hits:>8}/{len(in_scope_ids):<5} {delta_str:>8}")

    # ── Verdict ──
    delta_scope = last_in_scope_found - first_in_scope_found
    print(f"\n{'='*90}")
    print(f"  VERDICT (in-scope only): {first_in_scope_found}/{len(COMP_PROMPT_DIRECT)} \u2192 {last_in_scope_found}/{len(COMP_PROMPT_DIRECT)}")
    print(f"  Bonus (Curator-added edge cases found): {len(out_scope_found)}")
    if delta_scope > 0:
        print(f"  \u2705 YES \u2014 UC semantic improvements INDIRECTLY improved coverage by +{delta_scope} metrics.")
        print(f"     The semantic layer helps even when the Supervisor decomposes one broad question.")
    elif delta_scope == 0:
        print(f"  \u2796 NEUTRAL \u2014 same in-scope coverage. Improvements helped targeted queries")
        print(f"     more than the broad comprehensive prompt.")
    else:
        print(f"  \u26a0\ufe0f REGRESSED by {abs(delta_scope)} in-scope metrics. Agent non-determinism likely.")

    print(f"\n  KEY INSIGHT:")
    print(f"  Only {len(COMP_PROMPT_DIRECT)} of 45 tests come directly from the CFO prompt.")
    print(f"  The other {45 - len(COMP_PROMPT_DIRECT)} are Curator-added edge cases that need targeted questions.")
    print(f"  Judging the Supervisor on all 45 inflates the 'missing' count unfairly.")
    print(f"  The correct denominator is {len(COMP_PROMPT_DIRECT)}, not 45.")

print(f"\n{'='*90}")

# COMMAND ----------

# DBTITLE 1,Current Status and Findings
# MAGIC %md
# MAGIC ## Current Status and Findings
# MAGIC
# MAGIC ### Evaluation System (BUILT)
# MAGIC
# MAGIC The notebook includes a full **provenance and evaluation system** answering: HOW did the agent arrive at each answer, and can we depend on it?
# MAGIC
# MAGIC **Per-test output** (in `test_all_metrics`, cell 7):
# MAGIC * **Agent SQL** — the full SQL the agent generated (no truncation)
# MAGIC * **GT Reference SQL** — the ground truth SQL for comparison (no truncation)
# MAGIC * **Agent Narration** — the full natural language response (no truncation)
# MAGIC * **Query Result Rows** — up to 10 rows with all columns
# MAGIC * **Provenance & Evaluator Assessment** — structured block showing:
# MAGIC   * **Verdict**: PASS/FAIL with ground truth and found value
# MAGIC   * **UC Feature**: which governed asset the agent used (or raw table if none)
# MAGIC   * **Provenance**: explanation of how the detection was made
# MAGIC   * **Confidence**: dynamically derived label (see below)
# MAGIC   * **Assessment**: human-readable explanation of what the confidence label means
# MAGIC   * **Claimed Fix**: which iteration was expected to fix this test
# MAGIC
# MAGIC ### Reasoning Confidence Labels
# MAGIC
# MAGIC Every test carries a **Reasoning Confidence** label derived dynamically from provenance tier — nothing is hardcoded.
# MAGIC
# MAGIC | Confidence | What It Means | Provenance Tiers |
# MAGIC | --- | --- | --- |
# MAGIC | **DETERMINISTIC** | Agent steered by a governed UC asset — answer cannot drift | Iter 2 (Metric Views), Iter 3 (SQL Functions, Reference Tables) |
# MAGIC | **HEURISTIC** | Agent guided by metadata — likely repeatable | Iter 1 (Column Comments, Example SQL, Benchmarks) |
# MAGIC | **INFERRED** | Agent figured it out from raw column names — correct but ungoverned, fragile to rephrasing | Raw Table (no UC feature used) |
# MAGIC | **GUESSED** | Agent invented thresholds — unreliable | No UC feature available |
# MAGIC
# MAGIC **Key insight**: Accuracy alone doesn’t prove governance. A lucky guess scores the same as a governed answer. Confidence proves the answer is **reliably correct**, not just correct today.
# MAGIC
# MAGIC ### SQL Pattern Stability (Deep Reliability Analysis, cell 24)
# MAGIC
# MAGIC SQL **should change** between iterations — that’s the demo improving the agent. What matters is whether it **stabilizes**.
# MAGIC
# MAGIC | Category | Icon | Meaning |
# MAGIC | --- | --- | --- |
# MAGIC | **STABLE** | 🟢 | Same SQL across ALL iterations |
# MAGIC | **STABILIZED** | 🔵 | Changed early (improvement), locked in last 2 iterations |
# MAGIC | **CONVERGING** | 🟡 | Same table + aggregation approach, WHERE details still differ |
# MAGIC | **VOLATILE** | 🔴 | SQL still changing in the latest iteration |
# MAGIC
# MAGIC SQL signature normalization: collapses whitespace, sorts AND conditions alphabetically, strips `DATE` keyword from date literals.
# MAGIC
# MAGIC ### Robustness: Reliability vs Dependability (cell 26)
# MAGIC
# MAGIC | Metric | Question It Answers |
# MAGIC | --- | --- |
# MAGIC | **Reliability** | Did the agent get the **right answer**? (accuracy across rephrased variations) |
# MAGIC | **Dependability** | Did the agent use the **same SQL approach**? (consistency across rephrased variations) |
# MAGIC
# MAGIC Dependability scored at 4 levels: 100% (Identical SQL), 85% (Same structure, different filters), 60% (Mixed approaches), 33% (Genuinely different SQL). **Functional Equivalence** flag marks tests where SQL differs but all variations still produce the correct answer.
# MAGIC
# MAGIC ### Key Finding: Genie Agents Cannot Access UC Pages
# MAGIC
# MAGIC * **Genie One** (standalone chat): CAN read UC Pages. Returns correct answers with citations.
# MAGIC * **Genie Agents** (API / Supervisor): CANNOT read UC Pages. Agent stated: *"I don’t have direct access to those pages."*
# MAGIC * **Workaround**: SQL Functions encode the same thresholds. When added to agent data sources, the agent calls these functions.
# MAGIC * UC Pages remain valuable as human-facing governance documentation on the Discover page.
# MAGIC
# MAGIC ### Supervisor Agent Configuration
# MAGIC
# MAGIC * **10 registered tools**: 5 Genie Agent sub-agents + 5 UC SQL Functions
# MAGIC * **SQL Functions** return DEFINITIONS (concept, source_table, threshold_columns, conditions, definition) — not data. Agent must write its own SQL.
# MAGIC * **Provenance instructions** patched onto all 5 sub-agents asking them to state which UC feature they used.
# MAGIC
# MAGIC ### Baseline Non-Determinism (28/45)
# MAGIC
# MAGIC At baseline, agent answers are non-deterministic — the same question may produce different SQL across runs. The recorded baseline run passed 28/45. Most baseline passes are classified as INFERRED confidence.
# MAGIC
# MAGIC The 5 P-tests (P01–P05) use multi-condition rules that are truly unguessable — no LLM can infer "delay_days >= 5 AND total_weight_kg > 800" from column names alone. These tests definitively prove whether the agent used the SQL function or guessed.
# MAGIC
# MAGIC ### Iteration Results
# MAGIC
# MAGIC | Stage | PASS | Confidence Profile |
# MAGIC | --- | --- | --- |
# MAGIC | Baseline | 28/45 | Mostly INFERRED |
# MAGIC | After Iteration 1 | 37/45 | +HEURISTIC (comments/examples) |
# MAGIC | After Iteration 2 | 41/45 | +DETERMINISTIC (metric views) |
# MAGIC | After Iteration 3 | **45/45** | Mostly DETERMINISTIC (SQL functions, reference tables) |
# MAGIC
# MAGIC ### Notebook Structure (33 cells)
# MAGIC
# MAGIC * Cells 1-5: Title, Parameters, Setup, Teardown, Build (data + agents)
# MAGIC * Cell 6: Test Suite definitions (markdown) → Cell 7: BASELINE (45 tests)
# MAGIC * Cell 8: VISUAL Dashboard
# MAGIC * Cell 9: Iteration 1 approach (markdown) → Cell 10: ITERATION 1 → Cell 11: VISUAL After Iter 1
# MAGIC * Cell 12: Iteration 2 approach (markdown) → Cell 13: ITERATION 2 → Cell 14: VISUAL After Iter 2
# MAGIC * Cell 15: Iteration 3 approach (markdown) → Cell 16: PRE-STEP fiscal_targets → Cell 17: Kernel Recovery → Cell 18: Manual Step (markdown) → Cell 19: ITERATION 3 SETUP
# MAGIC * Cell 20: STEP 8 ai_forecast capability → Cell 21: ITERATION 3 test suite run
# MAGIC * Cell 22: **Reasoning Confidence definitions** (markdown) → Cell 23: VISUAL After Iter 3 (Final)
# MAGIC * Cell 24: **Deep Reliability Analysis definitions** (markdown)
# MAGIC * Cell 25: **Robustness Test definitions** (markdown)
# MAGIC * Cell 26: **Comprehensive Prompt Benchmark definitions** (markdown)
# MAGIC * Cell 27: DEEP RELIABILITY ANALYSIS → Cell 28: ROBUSTNESS TEST → Cell 29: COMPREHENSIVE PROMPT BENCHMARK
# MAGIC * Cell 30: Current Status and Findings (this cell)
# MAGIC * Cell 31: Teardown (delete non-Iter-3 agents)
# MAGIC * Cell 32: **Supervisor Agent vs Genie One** comparison report (markdown)
# MAGIC * Cell 33: MANUAL STEP Delete UC Domain (markdown)

# COMMAND ----------

# DBTITLE 1,Teardown: Delete all agents EXCEPT Iteration 3 (keep 5 domain + supervisor = 6)
# ============================================================
# TEARDOWN: Delete all agents EXCEPT Iteration 3
# Keeps: 5 Iteration 3 domain agents + 1 Supervisor = 6 survivors
# Deletes: Baseline (5), Iteration 1 (5), Iteration 2 (5),
#          Evaluator (1) = 16 agents removed
# ============================================================
print("="*80)
print("  AGENT CLEANUP: Keep only Iteration 3 + Supervisor")
print("="*80)

# Discover ALL SC agents
_all_resp = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers)
_all_sc = []
for _s in _all_resp.json().get("spaces", []):
    _t = _s.get("title", "")
    if "SC" in _t:
        _all_sc.append((_t, _s["space_id"]))

# Identify keepers: only "Iteration 3" agents survive
_keep_ids = set()
_keep_names = []
for _t, _sid in _all_sc:
    if "Iteration 3" in _t:
        _keep_ids.add(_sid)
        _keep_names.append(_t)

print(f"\n  Total SC agents found: {len(_all_sc)}")
print(f"  Keeping (Iteration 3): {len(_keep_ids)}")
for _n in sorted(_keep_names):
    print(f"    \u2713 {_n}")

# Delete everything else
_deleted = 0
print(f"\n  Deleting non-Iteration-3 agents:")
for _t, _sid in _all_sc:
    if _sid in _keep_ids:
        continue
    dr = requests.delete(f"{host}/api/2.0/genie/spaces/{_sid}", headers=headers)
    if dr.status_code in (200, 204):
        print(f"    \u2713 Deleted: {_t}")
        _deleted += 1
    else:
        print(f"    \u2717 Failed: {_t} ({dr.status_code})")

# Supervisor check
_sup_resp = requests.get(f"{host}/api/2.1/supervisor-agents", headers=headers)
_sup_name = None
if _sup_resp.status_code == 200:
    for _a in _sup_resp.json().get("supervisor_agents", []):
        if "Supply Chain" in _a.get("display_name", ""):
            _sup_name = _a["name"]
            break

print(f"\n  Summary:")
print(f"    Deleted: {_deleted} agents")
print(f"    Surviving: {len(_keep_ids)} domain agents (Iteration 3)")
print(f"    Supervisor: {_sup_name or 'NOT FOUND'} (kept)")
print(f"    Total alive: {len(_keep_ids) + (1 if _sup_name else 0)}")

# COMMAND ----------

# DBTITLE 1,Supervisor Agent vs Genie One: Side-by-Side Comparison Report
# MAGIC %md
# MAGIC ## Supervisor Agent vs Genie One: Side-by-Side Comparison Report
# MAGIC
# MAGIC ### Experimental Setup
# MAGIC
# MAGIC Both systems answered the same West-region August 2026 CFO-style prompt over the same deterministic dataset in [GAP_Demo_Dev](#catalog).
# MAGIC
# MAGIC * **Supervisor Agent**: 5 domain Genie Agents + 5 SQL threshold functions (tools on the Supervisor endpoint)
# MAGIC * **Genie One**: standalone Databricks chat with Discover-page knowledge, UC Pages, knowledge snippets, links, and visual output
# MAGIC
# MAGIC Note: teardown had removed the UC Page related-table attachments before the Genie One run, but the page prose and threshold text were still intact.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Metrics Where Both Systems Are Correct
# MAGIC
# MAGIC | Test | Metric | Supervisor | Genie One | Ground Truth |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | B01 | August revenue | $3,341,062.58 | $3,341,062.58 | 3341062.58 |
# MAGIC | B02 | July revenue | $4,581,392.70 | $4,581,392.70 | 4581392.70 |
# MAGIC | B03 | Revenue change ($) | -$1,240,330.12 | -$1,240,330.12 | -1240330.12 |
# MAGIC | B04 | Revenue change (%) | -27.07% | -27.07% | -27.07 |
# MAGIC | F06 | Worst product family decline | Home Goods -$349,063 | Home Goods -$349,063 | 349062.88 |
# MAGIC | A01 | On-time delivery rate | 5.43% | 5.43% | 5.43 |
# MAGIC | A03 | Avg delay days | 2.94 | 2.94 | 2.94 |
# MAGIC | A04 | Total shipments | 1,086 | 1,086 | 1086 |
# MAGIC | A05 | Late shipments | 1,027 | 1,027 | 1027 |
# MAGIC | A06 | Wasted freight cost | $2,484,986 | $2,484,986 | 2484985.57 |
# MAGIC | C01 | Below safety stock positions | 109 | 109 | 109 |
# MAGIC | C02 | Unique SKUs below SS | 61 | 61 | 61 |
# MAGIC | C04 | Unique SKUs stocked out | 33 | 33 | 33 |
# MAGIC | C05 | Avg days of supply | 0.96 | 0.96 | 0.96 |
# MAGIC | D01 | Total purchase orders | 48 | 48 | 48 |
# MAGIC | D02 | Late purchase orders | 36 | 36 | 36 |
# MAGIC | D03 | Vendor late rate (%) | 75% | 75% | 75.00 |
# MAGIC | D04 | Avg lead time variance | 8.69 days | 8.69 days | 8.69 |
# MAGIC | E03 | Cost of Disruption (Western) | $3,757,298 | $3,757,298 | 3757298.31 |
# MAGIC | F01 | West = Western mapping | Correct | Correct | 3341062.58 |
# MAGIC | F04 | Cancelled revenue | $179,419 | $179,419 | 179419.26 |
# MAGIC | G01 | Q1 target (miss?) | Yes, 92% | Yes, 92% | 92.00 |
# MAGIC | G02 | Q1 target value | 92.0% | 92.0% | 92.00 |
# MAGIC | P01 | Logistics Risk Standards | **176** | **176** | 176 |
# MAGIC | P02 | Demand Anomaly Alert | **77** | **77** | 77 |
# MAGIC | P04 | Procurement Quality Minimum | **11** | **11** | 11 |
# MAGIC | P05 | Executive Disruption Threshold | **3** | **3** | 3 |
# MAGIC
# MAGIC Both systems correctly resolved all 4 revenue metrics, all 5 logistics metrics, all 4 inventory metrics, all 4 supplier metrics, the full Cost of Disruption breakdown, both fiscal target tests, and 4 of 5 multi-condition policy thresholds.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Metrics Where They Differ
# MAGIC
# MAGIC | Metric | Supervisor | Genie One | Ground Truth | Better |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | Fill Rate / Service Level (E01) | 94.1% (inventory agent) | **80.70%** (governed KPI) | 80.70 | Genie One |
# MAGIC | Total SLA penalties (E02) | $0 | **$1,185,043.10** | 1185043.10 | Genie One |
# MAGIC | Inventory risk count (P03) | **106** | 27 (West-scoped) | 106 | Supervisor |
# MAGIC | Late delivery rate (A02) | Not surfaced explicitly | **94.57%** | 94.57 | Genie One |
# MAGIC | Disruption-to-revenue ratio (H07) | Not surfaced | **1.12** | 1.12 | Genie One |
# MAGIC | Q1 fill-rate actual vs target | Not available | **80.7% vs 90%** | 80.70 | Genie One |
# MAGIC | Stockout positions (C03) | 35 | **35** | 35 | Both correct |
# MAGIC
# MAGIC The Supervisor's two most significant misses:
# MAGIC
# MAGIC * **SLA penalties = $0** — the supplier agent couldn't locate the `penalty_amount` sum from `vendor_slas`. Genie One returned the correct $1.185M.
# MAGIC * **Fill rate = 94.1%** — the inventory agent returned a raw ratio instead of the governed `service_level_pct` (80.70%) from `executive_kpis`. Genie One referenced the KPI correctly.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Policy Threshold Tests (P01-P05)
# MAGIC
# MAGIC These are the most important tests because the multi-condition rules are not guessable from column names alone.
# MAGIC
# MAGIC | Policy Test | Supervisor | Genie One | Ground Truth | Notes |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | P01 Logistics Risk | **176** | **176** | 176 | Both applied `delay_days >= 5 AND total_weight_kg > 800` |
# MAGIC | P02 Demand Anomaly | **77** | **77** | 77 | Both applied `qty >= 8 AND price < 30 AND channel='Online'` |
# MAGIC | P03 Inventory Risk | **106** | 27 | 106 | Supervisor: all regions. Genie One: added Western filter |
# MAGIC | P04 Procurement Quality | **11** | **11** | 11 | Both applied `score < 75 AND ltv > 12` |
# MAGIC | P05 Exec Disruption | **3** | **3** | 3 | Both applied `risk < 55 AND ltv > 8 AND penalty > 80K` |
# MAGIC
# MAGIC **P-test score**: Supervisor **5/5**, Genie One **4/5**.
# MAGIC
# MAGIC Genie One correctly retrieved and applied the multi-condition definitions from UC Page prose for 4 of 5 tests. The one miss (P03) is a scoping issue — it added a West-region filter that the benchmark does not expect — not a definition failure.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### How Each System Accesses Governance
# MAGIC
# MAGIC | Capability | Supervisor Agent | Genie One |
# MAGIC | --- | --- | --- |
# MAGIC | UC Pages (Discover) | Cannot access | Reads and cites |
# MAGIC | Knowledge Snippets | Not available | Retrieves and cites |
# MAGIC | SQL threshold functions | Calls as tools | Not available |
# MAGIC | Metric views | Agents query directly | Queries directly |
# MAGIC | Visual output | Text/tables only | Cards, charts, links, embedded queries |
# MAGIC | UC Page links in response | No | Yes (linked to Discover pages) |
# MAGIC | Genie Agent room links | No | Yes (linked to source agents) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Scorecard
# MAGIC
# MAGIC | Category | Supervisor | Genie One |
# MAGIC | --- | --- | --- |
# MAGIC | Revenue metrics (B01-B04) | 4/4 | 4/4 |
# MAGIC | Logistics metrics (A01-A06) | 5/6 (A02 not surfaced) | 6/6 |
# MAGIC | Inventory metrics (C01-C05) | 5/5 | 5/5 |
# MAGIC | Supplier metrics (D01-D04) | 4/4 | 4/4 |
# MAGIC | Cross-domain (E01-E03) | 1/3 (fill rate, penalties wrong) | 3/3 |
# MAGIC | Fiscal targets (G01-G02) | 2/2 | 2/2 |
# MAGIC | P-tests (P01-P05) | **5/5** | **4/5** |
# MAGIC | CoD breakdown | Complete | Complete + ratio |
# MAGIC | Report quality | Structured text | Rich cards, links, snippets, visuals |
# MAGIC | Governance testability | **High** (SQL functions, benchmark-scorable) | Medium (prose-based, harder to automate) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Conclusion
# MAGIC
# MAGIC **Genie One** produced the better executive report: richer formatting (cards, embedded visualizations, UC Page links, knowledge snippet citations), complete coverage of derived metrics (late rate, disruption ratio, fill-rate gap), and correct SLA penalty totals.
# MAGIC
# MAGIC **The Supervisor Agent** is the more testable and controllable system: SQL functions expose exact threshold conditions that can be scored by the 45-test benchmark, and in this run it achieved a perfect **5/5** on the policy tests that matter most to the demo.
# MAGIC
# MAGIC Both systems correctly resolved the vast majority of governed metrics — revenue, logistics, inventory, supplier, Cost of Disruption, and fiscal targets all matched ground truth.
# MAGIC
# MAGIC The core finding:
# MAGIC
# MAGIC * **Genie One wins on executive-quality reporting and breadth of surfaced insight**
# MAGIC * **Supervisor wins on controlled, machine-testable policy execution**
# MAGIC * **Testable structured governance (SQL functions) is easier to harden, score, and prove correct than prose-first governance (UC Pages) — even when both systems are quite capable**

# COMMAND ----------

# DBTITLE 1,MANUAL STEP: Delete UC Domain (cleanup)
# MAGIC %md
# MAGIC ## 🧹 MANUAL STEP: Delete UC Domain
# MAGIC
# MAGIC > **After reviewing results above**, delete all 6 UC Domains to complete teardown.
# MAGIC
# MAGIC ### Steps:
# MAGIC 1. Go to the **Discover** page
# MAGIC 2. Delete each of the **6 domains**:
# MAGIC    * `Supply Chain Operations` (overall domain — holds Fiscal Calendar & Targets + Cross-Domain Metric Definitions)
# MAGIC    * `Logistics Operations` (holds Logistics Risk Standards)
# MAGIC    * `Demand Analysis` (holds Demand Quality Standards)
# MAGIC    * `Inventory Management` (holds Inventory Risk Classification)
# MAGIC    * `Supplier Procurement` (holds Supplier Quality Standards)
# MAGIC    * `Executive Reporting` (holds Executive Alert Thresholds)
# MAGIC
# MAGIC > Deleting the domains removes all 6 domains and their 7 pages. Schemas and tables are **not** affected — they belong to the catalog, not the domain.
# MAGIC >
# MAGIC > This ensures the next E2E run starts clean. The domains and pages must be **recreated fresh each time** (matching the teardown + rebuild pattern of the rest of the notebook).