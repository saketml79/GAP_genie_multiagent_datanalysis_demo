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


# Single business narrative — 9 metrics, all explicitly asked using business terms only.
# Deliberate term choices that confuse Genie agents without UC features:
#   "revenue"               → needs synonym: revenue = total_amount          (Iter 2)
#   "on-time delivery rate" → needs synonym + metric view: inverse of is_late (Iters 2-4)
#   "fill rate"             → needs synonym: fill_rate = service_level_pct   (Iter 2)
#   "vendor"                → needs synonym: vendor = supplier_* tables       (Iter 2)
#   "SLA penalties"         → needs synonym: SLA penalty = penalty_amount     (Iter 2)
#   "freight"               → needs synonym: freight = shipping_cost           (Iter 2)
#   "Cost of Disruption"    → needs metric view (cross-domain, Iter 4 only)
#   "West region"           → needs instruction: West = Western              (value mapping)
MAIN_PROMPT = (
    "We need a complete supply chain health check for our West region in August 2026. "
    "The CFO wants to understand what drove the revenue decline versus July, which product families "
    "are most at fault, and whether our on-time delivery rate and average delay for West region shipments "
    "are contributing to the problem. "
    "I also need our current fill rate, how many inventory positions are sitting below safety stock "
    "in the West region, and how many SKUs are completely stocked out. "
    "On the vendor side: what percentage of vendors delivered late in August, "
    "and what are the total vendor SLA penalties we have incurred? "
    "Bring it all together as our total Cost of Disruption by region for August 2026 — "
    "cancelled revenue, at-risk backorder revenue, wasted freight on late shipments, "
    "and supplier penalty exposure in one number per region. "
    "Are we going to miss our Q3 service-level targets, and what are the top actions we should take?"
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
    """BACKUP scorer on DOMAIN-ONLY text (evaluator output excluded)."""
    all_numbers = []
    for m in re.finditer(r'-?[\d,]+\.?\d*', domain_text):
        try: all_numbers.append(float(m.group().replace(',', '')))
        except ValueError: pass
    abs_numbers = [abs(n) for n in all_numbers]
    results = []
    for row in gt_rows:
        metric, gt_val = row["metric"], float(row["ground_truth_value"])
        gt_abs = abs(gt_val)
        best_s, best_f, best_p = "NOT_FOUND", None, float('inf')
        for num in all_numbers + abs_numbers:
            pct = abs(num - gt_abs) / max(gt_abs, 0.001)
            if pct < best_p: best_p, best_f = pct, num
        if best_p < 1e-9: best_s = "EXACT"        # exact match (1e-9 for IEEE 754 rounding only)
        elif best_p < 0.10: best_s = "CLOSE"      # < 10% — approximately right
        elif best_p < 0.30: best_s = "MISS"       # < 30% — wrong but in the neighborhood
        results.append({"metric": metric, "gt_value": gt_val,
            "found_value": round(best_f, 2) if best_f and best_s != "NOT_FOUND" else None,
            "status": best_s, "pct_diff": round(best_p * 100, 1)})
    return results


def score_supervisor_text(supervisor_text, gt_rows):
    """DETERMINISTIC scoring: search Supervisor text for each ground truth value.
    No AI narration parsing — pure numeric comparison.
    Returns list of {metric, gt_value, found_value, status, pct_diff}."""
    # Pre-extract all numbers from the Supervisor response
    all_numbers = []
    for m in re.finditer(r'-?[\d,]+\.?\d*', supervisor_text):
        try:
            val = float(m.group().replace(',', ''))
            all_numbers.append(val)
        except ValueError:
            pass
    # Also collect absolute values (revenue change may appear as "1,240,330" without minus)
    abs_numbers = [abs(n) for n in all_numbers]

    results = []
    for row in gt_rows:
        metric = row["metric"]
        gt_val = float(row["ground_truth_value"])
        gt_abs = abs(gt_val)

        best_status = "NOT_FOUND"
        best_found = None
        best_pct = float('inf')

        # Check both raw numbers and their absolute values
        for num in all_numbers + abs_numbers:
            if gt_abs < 0.001:
                pct = abs(num)
            else:
                pct = abs(num - gt_abs) / gt_abs  # compare absolute values
            if pct < best_pct:
                best_pct = pct
                best_found = num

        if best_pct < 1e-9:
            best_status = "EXACT"     # exact match (1e-9 for IEEE 754 only)
        elif best_pct < 0.10:
            best_status = "CLOSE"     # < 10%
        elif best_pct < 0.30:
            best_status = "MISS"      # < 30%
        # else remains NOT_FOUND

        results.append({
            "metric": metric, "gt_value": gt_val,
            "found_value": round(best_found, 2) if best_found and best_status != "NOT_FOUND" else None,
            "status": best_status, "pct_diff": round(best_pct * 100, 1),
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

# COMMAND ----------

# DBTITLE 1,Step 1: Teardown (clean slate)
# Always run teardown first for idempotency — safe even if nothing exists to tear down
if RUN_MODE != "iterations_only":
    run_notebook("09_teardown")
    print("\n✓ Teardown complete — catalog and agents removed")
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
else:
    print(f"⏭ Skipping data creation (run_mode={RUN_MODE})")

# COMMAND ----------

# DBTITLE 1,Step 9: Discover Supervisor Endpoint + Baseline Verification
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

# Remove evaluator from Supervisor tools — Supervisor must NOT see ground truth values
remove_evaluator_from_supervisor()

verification_history = []
verification_history.append(verify_ground_truth("Baseline (bare tables, no semantic enrichment)"))

# COMMAND ----------

# DBTITLE 1,Demo Design: Business Term Confusion Map + Iteration Plan
# ============================================================
# DEMO DESIGN: Proving UC Features Fix Agent Hallucinations
#
# The prompt uses BUSINESS terms that don't match column names.
# Without UC features, agents guess wrong. With them, they get
# correct contextual answers. Each iteration adds one UC feature.
# ============================================================

design = """
══════════════════════════════════════════════════════════════════════
  BUSINESS TERM CONFUSION MAP — 10 Primary Ground Truth Metrics
  (What the prompt says vs what the database has)
══════════════════════════════════════════════════════════════════════

  #  | Prompt Term               | Agent Guesses          | Correct Column/Table               | UC Fix
  ---+---------------------------+------------------------+------------------------------------+---------------------------------
  1  | "revenue decline"         | looks for revenue col  | total_amount in sales_orders       | Synonym (Iter 2)
  2  | "on-time delivery rate"   | computes OTD wrong     | inverse of is_late in shipments    | Synonym + Metric View (Iter 2+4)
  3  | "fill rate"               | looks for fill_rate    | service_level_pct in executive_kpis| Synonym (Iter 2)
  4  | "inventory positions"     | COUNT(DISTINCT sku_id) | COUNT(*) per SKU-warehouse row     | Comment (Iter 1)
  5  | "SKUs stocked out"        | COUNT(*)               | COUNT(DISTINCT sku_id)             | Comment (Iter 1)
  6  | "vendor SLA penalties"    | looks in wrong schema  | penalty_amount in vendor_slas      | Synonym: vendor→supplier (Iter 2)
  7  | "vendors delivered late"  | wrong schema           | supplier_orders.is_late            | Synonym: vendor→supplier (Iter 2)
  8  | "average delay"           | usually finds it       | delay_days in shipments            | Direct (baseline findable)
  9  | "Cost of Disruption"      | no such table/column   | cost_of_disruption_by_region view  | Metric View (Iter 4 only)
  10 | "Q3 service-level targets"| guesses 90% or 95%     | 95.0% (business policy, NOT data)  | UC Page (Iter 5 — manual from UI)

══════════════════════════════════════════════════════════════════════
  INDIRECT METRICS (from metric views — also tracked)
══════════════════════════════════════════════════════════════════════

  #  | Metric                         | Source Metric View                      | Related Primary GT
  ---+--------------------------------+-----------------------------------------+-------------------
  11 | Western Aug revenue             | revenue_comparison_by_region            | #1 (MoM change)
  12 | Western Jul revenue             | revenue_comparison_by_region            | #1 (MoM change)
  13 | Western revenue % change        | revenue_comparison_by_region            | #1 (MoM change)
  14 | Western late delivery rate      | delivery_performance_by_region          | #2 (OTD = inverse)
  15 | Western total shipments (Aug)   | delivery_performance_by_region          | #2
  16 | Western late shipments (Aug)    | delivery_performance_by_region          | #2
  17 | Western wasted freight cost     | delivery_performance_by_region          | #9 (CoD component)
  18 | Western unique SKUs below safety| inventory_safety_stock_metrics          | #4 (positions vs SKUs)
  19 | Western avg days of supply      | inventory_safety_stock_metrics          | #4
  20 | Asia supplier late rate         | supplier_performance_by_continent       | #7
  21 | Asia avg lead time variance     | supplier_performance_by_continent       | #7

══════════════════════════════════════════════════════════════════════
  ITERATION PLAN — Each adds ONE UC Semantics feature
══════════════════════════════════════════════════════════════════════

  Iter | UC Feature                | Metrics Fixed                                 | Target Score
  -----+---------------------------+-----------------------------------------------+-------------
  0    | (Baseline - bare tables)  | #8 (avg delay)                                | ~1-3/10
  1    | Column/Table COMMENTS     | #4 (safety stock), #5 (stockout SKUs)         | ~3-5/10
  2    | Column SYNONYMS           | #1 (revenue), #3 (fill rate), #6, #7 (vendor) | ~5-7/10
  3    | Certified SQL + Hardening | #1 (MoM formula), #2 (OTD calc)               | ~7-8/10
  4    | Metric Views + CoD + Tags | #2 (OTD view), #4 (authoritative), #9 (CoD)   | ~9-10/10
  5    | UC Pages + Domains (UI)   | #10 (Q3 target = 95%, from UC Page)            | 10/10

  Key: Each iteration adds ONE type of UC feature.
  Progression proves: data governance → better AI answers.
  See README.md for full metric view inventory + UC Pages spec.
"""

for line in design.strip().split("\n"):
    print(line)

print("\n\u2713 Evaluator is called EXTERNALLY by verify_ground_truth (NOT a Supervisor tool)")
print("  Scoring: Python domain-text scorer (deterministic) + Evaluator narration (context)")

# COMMAND ----------

# DBTITLE 1,Iteration 1: Column/Table Comments (UC COMMENT)
# ====================================================================
# ITERATION 1: Column and Table COMMENTS
# UC Feature: ALTER TABLE/COLUMN SET COMMENT
# Impact: Agents learn what columns MEAN:
#   - total_amount described as "Total order revenue in USD"
#   - below_safety_stock_flag described as "per SKU-warehouse position"
#   - is_late described as "TRUE when actual_delivery > expected_delivery"
# Without comments, agents guess from column names alone.
# ====================================================================
try:
    run_notebook("07_add_all_comments")  # 180+ column descriptions
except Exception as e:
    print(f"\u26a0 Iteration 1 error: {str(e)[:200]}")
verification_history.append(verify_ground_truth("Iter 1: Column/Table Comments"))

# COMMAND ----------

# DBTITLE 1,Iteration 2: Column Synonyms (UC Genie synonyms)
# ====================================================================
# ITERATION 2: Column Synonyms + Enhanced Instructions
# UC Feature: Genie column_configs.synonyms + text_instructions
# Impact: Business terms map to technical column names:
#   - "revenue" -> total_amount (sales_orders)
#   - "fill rate" -> service_level_pct (executive_kpis)
#   - "OTD" / "on-time delivery" -> is_late (shipments, inverted)
#   - "vendor" -> supplier_* (supplier tables)
#   - "freight cost" -> shipping_cost (shipments)
#   - "SLA penalty" -> penalty_amount (vendor_slas)
# Without synonyms, agents can't translate business jargon to SQL.
# ====================================================================
try:
    run_notebook("improvements/iteration_03_column_synonyms")
except Exception as e:
    print(f"\u26a0 Iteration 2 error: {str(e)[:200]}")
verification_history.append(verify_ground_truth("Iter 2: Column Synonyms"))

# COMMAND ----------

# DBTITLE 1,Iteration 3: Certified Queries + Supervisor Hardening
# ====================================================================
# ITERATION 3: Certified Queries + Supervisor Hardening
# UC Feature: Genie example_question_sqls + supervisor instructions
# Impact: Agents get SQL templates for complex calculations:
#   - MoM revenue change: SUM(Aug) - SUM(Jul) using total_amount
#   - OTD rate: ROUND(AVG(CASE WHEN NOT is_late...) * 100, 1)
#   - SLA penalty totals: SUM(penalty_amount) WHERE is_breached
# Supervisor gets structured routing and output format rules.
# Without certified queries, agents write wrong SQL for derived metrics.
# ====================================================================
try:
    run_notebook("improvements/iteration_02_certified_queries")
    run_notebook("improvements/iteration_04_supervisor_hardening")
except Exception as e:
    print(f"\u26a0 Iteration 3 error: {str(e)[:200]}")
verification_history.append(verify_ground_truth("Iter 3: Certified Queries + Hardening"))

# COMMAND ----------

# DBTITLE 1,Iteration 4: Metric Views + Cost of Disruption
# ====================================================================
# ITERATION 4: Metric Views + Cost of Disruption + UC Tags
# UC Feature: CREATE VIEW + SET TAG + system.certification_status
# Impact: Pre-computed business metrics agents can query directly:
#   - delivery_performance_by_region: OTD rate pre-computed (no inverse calc)
#   - revenue_comparison_by_region: MoM change pre-computed
#   - inventory_safety_stock_metrics: SKU-warehouse positions resolved
#   - cost_of_disruption_by_region: cross-domain join (4 tables)
# Also updates 9th GT metric (Western CoD computed dynamically from view).
# Without metric views, cross-domain questions are unanswerable.
# ====================================================================
try:
    run_notebook("improvements/iteration_05_metric_views_glossary")
    run_notebook("improvements/iteration_06_cost_of_disruption")
    # Certify all tables and views
    for schema in ["demand_analysis", "inventory_management", "logistics_operations", "supplier_procurement", "reporting"]:
        for tbl in [r.tableName for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()]:
            try:
                spark.sql(f"SET TAG ON TABLE {CATALOG}.{schema}.{tbl} `system`.`certification_status` = 'certified'")
            except Exception:
                pass
    print("\u2713 All tables/views certified")
except Exception as e:
    print(f"\u26a0 Iteration 4 error: {str(e)[:200]}")
verification_history.append(verify_ground_truth("Iter 4: Metric Views + CoD"))

# COMMAND ----------

# DBTITLE 1,Final Scorecard: Progression + SQL Evolution
# ================================================================== #
#                    FINAL SCORECARD                                  #
# ================================================================== #

final = verification_history[-1] if verification_history else {"stage": "none", "exact": 0, "total": 0}

# ---- 1. Accuracy Progression Table ----
print("\n" + "\u2550" * 70)
print("  ACCURACY PROGRESSION \u2014 Ground Truth Match Rate per Iteration")
print("\u2550" * 70)
print(f"  {'Stage':<55s} {'Score':>12s}")
print(f"  {'\u2500' * 55} {'\u2500' * 12}")
for item in verification_history:
    total = item.get("total", 0)
    exact = item.get("exact", 0)
    close = item.get("close", 0)
    miss  = item.get("miss", 0)
    nf    = item.get("not_found", 0)
    pct   = round(exact / total * 100) if total > 0 else 0
    icon  = "\u2705" if exact == total and total > 0 else "\u26a0\ufe0f" if pct >= 70 else "\u274c"
    extra = []
    if close: extra.append(f"{close} CLOSE")
    if miss:  extra.append(f"{miss} MISS")
    if nf:    extra.append(f"{nf} NOT_FOUND")
    detail = f" ({', '.join(extra)})" if extra else ""
    print(f"  {icon} {item['stage']:<53s} {exact}/{total} ({pct}%){detail}")

fe, ft = final.get("exact", 0), final.get("total", 0)
print(f"\n  {'\u2705' if fe == ft and ft > 0 else '\u26a0\ufe0f'} FINAL: {fe}/{ft} EXACT")

# ---- 1b. Per-Metric Heatmap across iterations ----
all_metrics = []
for item in verification_history:
    for pm in item.get("per_metric", []):
        if pm["metric"] not in all_metrics:
            all_metrics.append(pm["metric"])

if all_metrics:
    print(f"\n  {'Metric':<55s}", end="")
    for item in verification_history:
        label = item.get("stage", "?").replace("After ", "")[:15]
        print(f" {label:>15s}", end="")
    print()
    print(f"  {'\u2500' * 55}", end="")
    for _ in verification_history:
        print(f" {'\u2500' * 15}", end="")
    print()
    status_icons = {"EXACT": "\u2705", "CLOSE": "\u26a0\ufe0f", "MISS": "\u274c", "NOT_FOUND": "\u2b1b"}
    for metric in all_metrics:
        print(f"  {metric[:55]:<55s}", end="")
        for item in verification_history:
            pm_list = item.get("per_metric", [])
            status = next((p["status"] for p in pm_list if p["metric"] == metric), "--")
            icon = status_icons.get(status, "  ")
            print(f" {icon:>15s}", end="")
        print()

# ---- 2. SQL Evolution: How agent queries changed ----
print("\n" + "\u2550" * 70)
print("  SQL EVOLUTION \u2014 Supervisor \u2192 Agent Queries at Each Stage")
print("\u2550" * 70)

agent_query_history = {}
for item in verification_history:
    stage_short = item.get("stage", "?").replace("After ", "")[:45]
    for tc in item.get("tool_calls", []):
        agent = tc["agent"]
        if agent not in agent_query_history:
            agent_query_history[agent] = []
        agent_query_history[agent].append((stage_short, tc["query"]))

for agent, history in sorted(agent_query_history.items()):
    print(f"\n  \u25b6 {agent}")
    prev_q = None
    for stage, query in history:
        tag = "\u2728 CHANGED" if query != prev_q and prev_q is not None else "  initial" if prev_q is None else "  (same)"
        # Wrap long queries
        q_display = query[:120] + ("..." if len(query) > 120 else "")
        print(f"    [{stage[:35]:35s}] {tag}: {q_display}")
        prev_q = query

# ---- 3. Result Schema Evolution ----
print("\n" + "\u2550" * 70)
print("  RESULT SCHEMAS \u2014 Column Names Returned by Each Agent")
print("\u2550" * 70)

agent_col_history = {}
for item in verification_history:
    stage_short = item.get("stage", "?").replace("After ", "")[:45]
    for agent, cols in item.get("result_columns", {}).items():
        if agent not in agent_col_history:
            agent_col_history[agent] = []
        agent_col_history[agent].append((stage_short, cols))

for agent, history in sorted(agent_col_history.items()):
    print(f"\n  \u25b6 {agent}")
    prev_cols = None
    for stage, cols in history:
        col_str = ", ".join(cols[:7]) + (" ..." if len(cols) > 7 else "")
        tag = "\u2728" if cols != prev_cols else "  "
        print(f"    {tag} [{stage[:35]:35s}]: {col_str}")
        prev_cols = cols

print("\n" + "\u2550" * 70)
print("  END OF SCORECARD")
print("\u2550" * 70)

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

# DBTITLE 1,Summary
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
# MAGIC | Ground truth | 9/9 EXACT match target |
# MAGIC
# MAGIC ### To invoke the Supervisor manually:
# MAGIC ```python
# MAGIC resp = requests.post(
# MAGIC     f"{host}/serving-endpoints/{endpoint_name}/invocations",
# MAGIC     headers=headers,
# MAGIC     json={"input": [{"role": "user", "content": "Why did revenue drop in the Western Region last month?"}]}
# MAGIC )
# MAGIC ```