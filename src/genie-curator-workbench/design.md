# Design — Genie Curator Workbench

## Architecture

```
User (Browser)
  → Databricks App (Dash/Flask, port 8000, OBO authentication)
    → OBO Token (X-Forwarded-Access-Token from logged-in user)
      → Databricks REST APIs:
        ├─ GET  /api/2.0/genie/spaces                    (discover agents)
        ├─ POST /api/2.0/genie/agents/{id}/responses      (chat — agent mode SSE)
        ├─ GET  /api/2.0/serving-endpoints                (discover supervisors)
        ├─ POST /serving-endpoints/{ep}/invocations       (invoke supervisor)
        ├─ POST /api/2.0/sql/statements/                  (catalog/data queries)
        ├─ GET  /api/2.0/sql/warehouses/{id}              (warehouse status)
        ├─ POST /api/2.0/sql/warehouses/{id}/start        (warehouse start)
        ├─ POST /api/2.1/jobs/update                     (set run_as to calling user — via OBO)
        ├─ POST /api/2.1/jobs/run-now                     (trigger job — via SP)
        └─ GET  /api/2.1/jobs/runs/get                    (poll job status — via SP)
```

## Authentication: OBO + SP (Split by API)

Most API calls use the logged-in user's OBO token. The **Jobs API** uses the app's
**Service Principal** credentials (via `w.config.authenticate()`) because there is NO
`jobs` OBO scope. The SP has `CAN_MANAGE_RUN` on all per-user jobs via App resources.

**Per-User Jobs (DAB Deploy)**: Each user runs a DAB deploy from the GitHub repo to
their Databricks profile, which creates 5 jobs prefixed with `[dev <alias>]`:
- `[dev ssuman] Curator Workbench: Full Setup`
- `[dev ssuman] Curator Workbench: Teardown`
- `[dev ssuman] Curator Workbench: Iteration 1/2/3 Supervisor`

Each user's jobs have `run_as` set to that user naturally (they deployed them).
The app discovers all per-user jobs at first request via `GET /api/2.1/jobs/list`,
matches the `[dev <alias>]` or `[<alias>]` naming convention, and routes each user
to their own jobs. No runtime `run_as` changes needed.

| Auth Method | APIs                                               |
|-------------|----------------------------------------------------|
| OBO Token   | genie, sql, serving-endpoints, catalog, files, workspace |
| SP (App resources) | Jobs API: trigger (run-now) + poll (runs/get) + list (discover per-user jobs) |

### Token Extraction Priority
1. `X-Forwarded-Access-Token` (primary — injected by Databricks App proxy)
2. `X-Databricks-Token`
3. `Authorization`

### Required OBO Scopes (user_api_scopes)
Managed ONLY via the App UI (Settings > Authentication > User authorization).
NEVER via CLI or API.

| Scope                    | Used For                                      |
|--------------------------|-----------------------------------------------|
| sql                      | SQL Statement API (catalog checks, queries)   |
| genie                    | Genie Spaces API (discover, CRUD agents)      |
| model-serving            | Serving Endpoints (discover supervisors, chat) |
| catalog.tables:read      | UC table metadata                             |
| catalog.catalogs:read    | UC catalog metadata                           |
| catalog.schemas:read     | UC schema metadata                            |
| catalog.connections      | UC connections                                |
| files                    | Workspace file access                         |
| workspace.workspace      | Workspace operations                          |
| ai-gateway               | AI Gateway access                             |
| ai-functions             | AI functions                                  |
| mcp.functions            | MCP function tools                            |
| mcp.external             | External MCP tools                            |
| vector-search            | Vector search                                 |
| postgres                 | Lakebase access                               |
| sql:restricted-query     | Restricted SQL                                |

## Main Notebook Structure (41 cells — READ ONLY)

The main workflow notebook defines the canonical flow. The app mirrors this.

| Cell | Title                                                    | Stage      |
|------|----------------------------------------------------------|------------|
| 1    | Title (markdown)                                         | -          |
| 2    | Parameters (catalog, warehouse, run_mode)                | -          |
| 3    | Setup: Paths + API Client + Verification Functions       | -          |
| 4    | Step 1: Teardown (clean slate) — run_notebook("09_teardown") | Teardown |
| 5    | Steps 2-8: Create Catalog + Data + Agents (NO comments)  | Setup      |
| 6    | Test Suite: 45 Questions (markdown)                      | -          |
| 7    | BASELINE BENCHMARK: 45 tests (discover_agents_by_label)  | Baseline   |
| 8    | VISUAL: Dashboard + Provenance                           | Baseline   |
| 9    | Iteration 1 Approach (markdown)                          | -          |
| 10   | ITERATION 1: Clone Baseline → Iter 1 + comments + SQL   | Iter 1     |
| 11   | VISUAL: After Iteration 1                                | Iter 1     |
| 12   | Iteration 2 Approach (markdown)                          | -          |
| 13   | ITERATION 2: Clone Iter1 → Iter2 + metric views + tags  | Iter 2     |
| 14   | VISUAL: After Iteration 2                                | Iter 2     |
| 15   | Iteration 3 Approach (markdown)                          | -          |
| 16   | PRE-STEP: Create fiscal_targets table                    | Iter 3     |
| 17   | KERNEL RECOVERY (repopulate after restart)                | -          |
| 18   | MANUAL STEP: Create UC Domain + Pages (markdown)         | Iter 3     |
| 19   | ITERATION 3: UC Pages + SQL Functions + fiscal_targets   | Iter 3     |
| 20   | STEP 8: ai_forecast Capability                           | Iter 3     |
| 21   | STEP 9: Entity Matching on String Columns                | Iter 3     |
| 22   | ITERATION 3: Run 45-test suite                           | Iter 3     |
| 23   | Reasoning Confidence (markdown)                          | -          |
| 24   | VISUAL: After Iteration 3 (Final)                        | Iter 3     |
| 25-27| Analysis markdown cells                                  | -          |
| 28   | DEEP RELIABILITY ANALYSIS                                | Analysis   |
| 29   | ROBUSTNESS TEST (10 × 3 variations)                      | Analysis   |
| 30   | COMPREHENSIVE PROMPT BENCHMARK                           | Analysis   |
| 31   | Status & Findings (markdown)                             | -          |
| 32   | CLEANUP: Delete non-Iter-3 agents (keep 5 Iter3 + sups) | Cleanup    |
| 33-34| Supervisor vs Genie One comparison                       | Comparison |
| 35-38| Supervisor 45-Test Benchmark                             | Benchmark  |
| 39-40| MCP Supervisor Comparison                                | Comparison |
| 41   | MANUAL STEP: Delete UC Domain                            | Cleanup    |

## Agent Discovery Design

### By Label (notebook pattern — canonical)
```python
discover_agents_by_label("Baseline")   # → 5 agents with "SC" AND "Baseline" in title
discover_agents_by_label("Iteration 1") # → 5 agents with "SC" AND "Iteration 1" in title
```

### Agent Naming Convention
- `SC - Demand Analysis - Baseline`
- `SC - Demand Analysis - Iteration 1`
- `SC - Inventory Management - Iteration 2`
- `SC - Executive Reporting - Iteration 3`
- `SC - Evaluator` (no iteration label — one only, created at setup)

### Supervisor Discovery
The supervisor-agents API (/api/2.1/supervisor-agents) requires a scope NOT in the standard
OBO token set. Use serving-endpoints API (/api/2.0/serving-endpoints) filtering for `mas-*-endpoint`
pattern. This is the working approach, not a "fallback" — log it cleanly, no error language.

**Per-user alias filtering**: Both discovery paths filter by `(alias)` in the display_name.
If a user's alias is `ssuman_bpcs_com`, only supervisors with `(ssuman_bpcs_com)` in their
display_name are returned. This prevents cross-user contamination — ssuman never sees
mchaudri's supervisors, and vice versa. Same pattern applies to `discover_agents()`
via `_group_agent_versions()` and the fallback domain-keyword loop.

## Agent Cloning Pattern (from notebook)

Each iteration creates FRESH agents (never modifies previous iteration):
1. `discover_agents_by_label(prev_label)` → get 5 source agents
2. For each: `GET /api/2.0/genie/spaces/{id}?include_serialized_space=true`
3. Create new space: `POST /api/2.0/genie/spaces` with new title containing iteration label
4. PATCH with copied config from serialized_space
5. Tables must be sorted alphabetically in `data_sources.tables`
6. `example_question_sqls` and `benchmarks.questions` must be sorted by `str(id)`
7. Verify: retry GET after PATCH to confirm `data_sources` populated (2-4s race condition)

## Iteration 3: Manual Step Required

Between Iter 2 and Iter 3, the user must manually create UC Domain + Pages on the Discover page.
This is a UI-only step — no API available yet. The `fiscal_targets` table must exist FIRST
(created in the PRE-STEP) so the Discover page can link it as a Related Asset on UC Page 1.

## Setup & Teardown via Jobs (MANDATORY)

### Setup Job (SETUP_JOB_ID = 98136556715691)
- Triggered via `POST /api/2.1/jobs/run-now`
- MUST pass `notebook_params={"agent_label": "Baseline"}` — without this, defaults to "Iteration 3"
- Executes: 01_create_catalog → 02-05_generate_data → 06_reporting_views → 08_setup_genie_supervisor
- NOTE: Iteration 1 applies table/column comments via SQL Statement API (no external notebooks)

### Teardown Job (TEARDOWN_JOB_ID = 154776446737504)
- Triggered via `POST /api/2.1/jobs/run-now`
- Deletes all supervisor agents, all SC Genie agents, DROP CATALOG CASCADE
- Preserves: UC Domains + Pages (governance layer persists by design)

## SQL Statement API Constraints
- `wait_timeout`: must be 0 (disable) or 5-50 seconds. NEVER 60+.
- Cap: `wait_s = min(max(timeout, 5), 50)`
- Warehouse must be RUNNING before SQL calls; app auto-starts if STOPPED.

## Supervisor Invocation
- Endpoint: `POST /serving-endpoints/{endpoint_name}/invocations`
- Hard 290-second timeout
- Tool descriptions must be directive ("Ask EXACTLY: ...")
- Instructions must be under ~2200 chars
- Returns text/tables only (no charts — unlike standalone Genie Agent UI)

## Data Characteristics
- All data is fully deterministic: `base_date = datetime(2026, 9, 1)` + fixed seeds (42/43/44/45)
- "Last month" = August 2026, "Prior month" = July 2026
- All SQL uses `DATE '2026-09-01'` instead of `CURRENT_DATE()`
- 55 tables across 6 schemas + information_schema + default
- Catalog: GAP_Demo_Dev, Warehouse: e3a0fc2c08db05bb

## Chat API (Agent Mode)
- `POST /api/2.0/genie/agents/{space_id}/responses` (SSE stream)
- NOT the legacy conversations API
- Retries on 429 RESOURCE_EXHAUSTED with exponential backoff

## State Management
Global `ST` dict (in-memory, per-process, **shared across all concurrent users**):
- agents, supervisors, benchmarks, catalog_exists, table_count
- warehouse_state, warehouse_name, _wh_starting (fire-and-forget flag)
- setup_running, setup_step, setup_log, setup_tasks

**CRITICAL: `ST["user_alias"]` and `ST["user_catalog"]` are last-writer-wins.**
Multiple users share the same ST. Any code that needs the CURRENT user's alias
or catalog for job parameters MUST derive it from per-request OBO headers
(`_get_user_alias(obo_headers)`, `user_catalog(obo_headers)`), NOT from ST.
ST reads are only safe for display/telemetry (heartbeat chip, UI cards).
- td_running, td_log (teardown background thread progress)
- iterations_done (set), iter_running (None or "it1"/"it2"/"it3")
- iter_setup_phase (bool), iter_setup_log (list) — live progress during iteration feature setup
- bench_running, bench_results, bench_elapsed, bench_total_tokens
- bench_last_ts, bench_last_user — last benchmark run timestamp + user (from OBO JWT)
- iter_bench_active, iter_it1_results, iter_it2_results, iter_it3_results

## Async Patterns

### Setup (background thread + polling)
- `btn-setup` click → captures OBO headers → launches `_run_setup_bg(obo_headers)` in daemon thread
- `_run_setup_bg` uses SP for Jobs API polling, OBO for post-setup SQL catalog check
- `setup-poll` Interval (2s) updates `setup-out` from `ST["setup_log"]` + step rows from `ST["setup_tasks"]`
- Steps get pulsating animation (CSS `.step-row.running`) as each task starts

### Teardown (background thread + polling)
- `td-everything` click → captures OBO headers → launches `_run_teardown_bg(obo_headers)` in daemon thread
- `_run_teardown_bg` derives alias/catalog from OBO headers (NOT from ST) for job params
- Uses SP for Jobs API trigger + polling
- `td-poll` Interval (1.5s) updates `td-out` from `ST["td_log"]`
- Shows progress: Triggering → Job starting → PENDING → RUNNING → TERMINATED → COMPLETE

### Warehouse Startup (non-blocking)
- `_build_state_cards()` NEVER calls `ensure_warehouse_running()` (that blocks 3 min)
- Instead: quick `check_warehouse()` (GET), if STOPPED → fire-and-forget `POST .../start`
- `state-refresh` Interval (10s) re-checks; card updates when RUNNING
- `global-poll` Interval (8s) updates dynamic WH chip in header (name + color status)
- `_discover_all()` also checks/starts warehouse on ANY page load (not just Setup)
- Header chip: checkmark name (teal) = RUNNING, arrow name (indigo) = STARTING, x name (rose) = STOPPED
- While starting: warehouse card pulsates (CSS `.state-card.starting`)
- Dependent cards (catalog, agents, sups, schemas) show "Waiting for warehouse..." (`.state-card.waiting`)

### Dynamic Button State
- `auto_refresh_state` callback (10s) updates BOTH state cards AND setup button:
  - disabled/className/style/hint text all reactive to `ST["agents"]` and `ST["setup_running"]`
  - After teardown clears `ST["agents"]`, next refresh cycle enables the setup button
- `poll_teardown` ALSO outputs btn-setup.disabled + className + hint (allow_duplicate=True)
  - Enables setup button IMMEDIATELY when teardown completes (no 10s lag)

### Global Warehouse Banner (global-wh-banner)
- Lives OUTSIDE `html.Div(id="page")` in app.layout, between make_nav() and page div
- Updated every 8s by `update_wh_chip` callback which returns (chip, banner) tuple
- Per-page banners (in page_overview, page_setup, etc.) provide initial render state only
- Fixes stale banner bug: previously, banner rendered once at page load and never updated
  when warehouse transitioned STARTING → RUNNING

### Tab Transition Guards (Dash Race Condition Fixes)
1. **loading_refresh one-shot guard**: ST["_loading_done"] flag ensures loading_refresh
   renders page exactly once when app_ready flips True. Subsequent queued interval callbacks
   are no-ops. Without this, loading_refresh could overwrite page.children with stale
   State("tab") value, causing nav to show one tab while page shows another.
2. **nav_click phantom trigger guard**: Pattern-matching callbacks with ALL fire when new
   matching components appear in DOM (e.g. qchat buttons created by page re-render).
   nav_click checks ctx.triggered[0]["value"] — if 0 or None (component creation, not
   real click), returns no_update. Without this, app auto-switched from Overview to Agents
   page on every launch.

### Iteration Setup (background thread + polling)
- btn-itN click captures OBO headers, launches _run_iter_setup_bg(obo_headers) in daemon thread
- Success callbacks (_it1_success, _it2_success, _it3_success) receive obo_h from the bg thread
  and forward it to _run_supervisor_refresh_job(obo_headers=obo_h) for per-user alias/catalog
- Phase 1: run_iteration_N() runs in background with log_fn callback for live progress
- Column/table comments applied via SQL Statement API (mirrors notebook cell 10 Step 1)
- No external notebooks called — all iteration logic is direct API (Genie + SQL)
- iter_setup_log updated at each step; poll_iter_bench renders _build_iter_status() panel
- Phase 2: after features applied, _start_iter_bench() launches parallel 45-test benchmark
- UI transitions: status panel with step dots -> test chip grid with live scoring -> charts

### Benchmark (baseline + iterations)
- _run_bench_bg() runs 45 tests with ThreadPoolExecutor(max_workers=8) in parallel
- Each test calls ask_genie() (OBO) and accumulates token usage (thread-safe _token_lock)
- On completion: stores bench_last_ts, bench_last_user, bench_total_tokens in ST
- Last-run info displayed: "Last run: ts by user | elapsed | tokens"

### Token Tracking
- ask_genie() returns usage dict from SSE response events (response.usage)
- call_supervisor() returns (text, elapsed, usage_dict) from invocation response
- Chat page shows "Supervisor responded in Ns, N tokens" when usage available

## Color Architecture (CSS Theme Adaptation)

Two color dictionaries serve different purposes:
- HEX dict: raw hex values for Plotly charts ONLY (Plotly cannot resolve CSS variables)
- C dict: CSS variable refs (var(--ink), etc.) for ALL inline styles
- Plotly template PT: paper_bgcolor="rgba(0,0,0,0)" (transparent), font color="#888" (neutral)

## UI Layout

### Setup & Teardown Page
- Environment State: 5-card grid (WAREHOUSE, CATALOG, DOMAIN AGENTS, SUPERVISORS, SCHEMAS)
- Teardown section: 60/40 flex (buttons left, output right)
- Setup section: 60/40 flex (steps + button left, output right)
- Output panels: `.scr-body` with max-height:420px, overflow:scroll, styled scrollbars

### Comparison (streaming, Oct 11)
- Background thread `_run_cmp_bg` -> two parallel `call_supervisor_stream` calls with OBO headers captured in the callback.
- SSE events used: `response.output_item.done` (function_call, function_call_output, message incl. `<name>X</name>` routing markers) and `response.output_text.delta`. Final answer = the supervisor's last streamed message (sub-agent messages never scored).
- UI refresh: `cmp-poll` (3s) drives `handle_cmp` (grids/status/charts) and `render_cmp_live` (live panels). Poll re-enables on page return while a run is in progress.
