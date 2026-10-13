# Genie Code Daily Startup — READ THIS FIRST

## Step 0: Read the 4 Design Documents

Before taking ANY action, read all 4 files in this directory:

1. **product.md** — What we're building (goals, pages, user stories, iteration progression)
2. **design.md** — How it works (auth split, APIs, state management, async patterns, color architecture)
3. **rules.md** — 43 constraints that MUST NOT be broken (auth, UI, coding, deployment)
4. **test.md** — 45 ground truth tests + comprehensive regression checklist

These files are the source of truth. If your memory contradicts them, the files win.

## Step 1: Understand the Auth Split (THE #1 SOURCE OF REGRESSIONS)

| Auth Method | Use For | Function |
|---|---|---|
| OBO (get_headers) | EVERYTHING except Jobs API: genie, sql, warehouse, serving-endpoints, catalog, files | get_headers() |
| SP (get_sp_headers) | Jobs API ONLY: trigger_job(), poll_run(), submit_notebook_run() | get_sp_headers() |

**NEVER use get_sp_headers() for:**
- check_warehouse()
- warehouse start (POST .../start)
- update_wh_chip()
- _discover_all()
- _build_state_cards()
- ask_genie()
- call_supervisor()
- discover_agents()
- discover_supervisors()
- run_sql() / run_sql_query()

If you find yourself typing `get_sp_headers` for ANY of the above, STOP. You are about to cause a regression.

## Step 2: Background Thread Rule

Background threads have NO Flask request context. `get_headers()` will fail inside them.

**Pattern (ALWAYS follow):**
```python
# IN the Dash callback (HAS Flask context):
obo_h = get_headers()  # Capture BEFORE thread start

# THEN start the thread:
threading.Thread(target=_my_bg_fn, args=(obo_h,), daemon=True).start()

# IN the thread function:
def _my_bg_fn(obo_headers):
    # Use obo_headers, NEVER call get_headers() here
```

## Step 3: Color Architecture

| Dict | Purpose | Used By |
|---|---|---|
| HEX["xxx"] | Raw hex values | Plotly charts ONLY (marker_color, font color, etc.) |
| C["xxx"] | CSS variable refs (var(--ink)) | ALL inline style={} dicts |

**NEVER** put C["xxx"] in Plotly code. **NEVER** put HEX["xxx"] in style dicts.
Plotly template PT uses paper_bgcolor="rgba(0,0,0,0)" (transparent) and font color="#888".

## Step 4: Pre-Deploy Checklist (RUN EVERY TIME)

```python
import ast, re

# 1. Syntax
ast.parse(content)

# 2. Duplicate outputs
output_map = {}
for m in re.finditer(r'Output\("([^"]+)"\s*,\s*"([^"]+)"(?:\s*,\s*allow_duplicate\s*=\s*True)?\)', c):
    key = m.group(1) + "." + m.group(2)
    has_dup = 'allow_duplicate=True' in m.group(0)
    output_map.setdefault(key, []).append(has_dup)
conflicts = [k for k, v in output_map.items() if len(v) > 1 and sum(1 for d in v if not d) > 1]
assert not conflicts, f"DUPLICATE OUTPUTS: {conflicts}"

# 3. Clientside callback IDs (ONLY these 3 exist)
# setup-out, td-out, log-body — NOTHING ELSE

# 4. No SP in warehouse code
wh_section = c[c.index('def check_warehouse'):c.index('def ensure_warehouse_running')]
assert 'get_sp_headers' not in wh_section

# 5. No C[] in Plotly charts
for fn in ['_build_bench_charts', '_build_cmp_charts']:
    idx = c.index(f'def {fn}(')
    end = c.index('\ndef ', idx + 10)
    assert 'C["' not in c[idx:end] or all(x in ('ink','muted') for x in re.findall(r'C\["(\w+)"\]', c[idx:end]))
```

## Step 5: Core Design Principles

1. **UI MUST always show what is happening.** Never let the user stare at a static screen.
   All long-running ops → background thread → polling interval → live progress display.

2. **Never show fake data.** Progression chart, scores, benchmarks — only actual results.
   Unrun stages show ghost bars with "Not run", not hardcoded expected values.

3. **Never modify notebooks** other than main.py and iterations.py.

4. **Setup job MUST pass** `notebook_params={"agent_label": "Baseline"}`.

5. **SQL wait_timeout**: 5-50 seconds. Never 60+.

## Step 6: Current App State (update this after each session)

### Files
- main.py (ID: 3996267606404343) — single-file Dash app, ~140K chars
- iterations.py (ID: 3996267606404392) — iteration logic (clone, patch, example SQL, benchmarks)
- app.yaml (ID: 3902498041032651) — env vars: CATALOG_NAME, WAREHOUSE_ID, SETUP_JOB_ID, TEARDOWN_JOB_ID

### Pages (7 tabs)
1. Overview — agent/supervisor table, quick chat buttons
2. Setup & Teardown — 5-card state grid, async setup/teardown via jobs
3. Baseline — 45-test benchmark, parallel (8 workers), progress bar, charts
4. Iterations — progression chart, 3 iteration cards with UC features, live setup progress
5. Comparison — side-by-side supervisor scoring with CFO prompt
6. Agents (Chat) — dropdown + chat interface
7. Logs — activity log

### Key Architecture
- Header: dynamic WH chip (id="wh-chip", global-poll 8s) shows name + color status
- Benchmark: last-run timestamp + user + elapsed + tokens displayed
- Iteration 1: non-blocking background thread with log_fn for live step progress
- Iterations 2 & 3: stub logic ("wiring in progress")

### Session Oct 11, 2026 (update)
- Pages now include a version picker for agents (Overview + Chat). DOMAIN AGENTS count = all versions.
- `_genie_agent_url(space_id)` builds absolute workspace Genie links -- never use relative `/#genie/...` hrefs (they resolve to the app's own URL).
- View Setup Log button: disabled via `_has_setup_log(key)`; `sync_setuplog_buttons` re-enables on iter-bench-poll.
- 09_teardown: list ALL pages first, then delete (never delete while paging).
- Open: E01 fill-rate regression in Iter 3 (fiscal_targets.fill_rate_target_pct collision) -- see product.md. Robustness/Reliability page missing -- see product.md.
- Stale invariant in .code_integrity_manifest.json: `def _run_iter1_full_bg(obo_headers):` (function renamed to `_run_iter_setup_bg`).

- Comparison page: nav tabs now freeze while `cmp_running` (added to the global `_app_busy` rule + `handle_cmp` locks nav immediately on Run and releases on finish). Each supervisor side shows its own state (finished side = final score; pending side = "Waiting for supervisor response..."). cmp-poll resumes after a page reload.
- Supervisor names show as `mas-xxxx-endpoint` because the app's OBO token has no supervisor-agents scope -> `discover_supervisors` falls back to serving endpoints. Real identities (verified with user creds Oct 13): mas-f541ae00 = "Supply Chain Control Tower (ssuman_bpcs_com)" (5 genie_space + 5 uc_function tools), mas-06bde9a3 = "Supply Chain MCP Supervisor (ssuman_bpcs_com)" (1 UC MCP tool system.ai.genie_one_mcp). Both supervisors are user-scoped with `({alias})` suffix. Scopes are user-managed -- never change them.
- Supervisor endpoints support `"stream": true` (SSE): `response.output_item.done` with item.type=function_call (tool name + genie_query args), message items (sub-agent output), and `response.output_text.delta` (final answer). Use this for live comparison steps.

- Comparison page (Oct 13): supervisor cards show what each supervisor IS (badge MULTI-AGENT / GENIE ONE MCP, description, tools -> agent version). Source order: supervisor-agents API + `/tools` (if OBO token can read it) -> tile-based fallback via serving endpoint tile_id -> learned from the live stream (`ST["sup_learned"]`) -> description-based inference ("mcp"/"genie one" in desc -> MCP, "multi-agent"/"coordinates" -> MULTI). Architecture is always resolved — "ARCHITECTURE UNKNOWN" badge only appears when no description is available. 403 on supervisor-agents API is expected (logged once per session) — serving-endpoints fallback always works.
- Comparison runs use `call_supervisor_stream` (stream=True). Live steps per side in `ST["cmp_live_a/b"]` (call / route / result / progress / error) + streamed output; rendered by `render_cmp_live` on cmp-poll into `cmp-live-a/b` (two scrollable boxes per side). `call_supervisor` (non-stream) is unchanged for the Chat page. Empty stream -> falls back to the classic call.
- CFO prompt box (`_build_prompt_box`) shows the exact MAIN_PROMPT with the workshop HTML highlight styles (metric view / CoD view / policy function / UC Page / business-word-different-column).

- Supervisor DISPLAY NAMES (never show `mas-xxxx-endpoint`): `discover_supervisors` reads `tile_endpoint_metadata.tile_id` from the serving endpoint, then `_resolve_supervisor_from_tile` tries `/api/2.0/multi-agent-supervisors/{tile_id}` -> `/api/2.0/tiles/{tile_id}` -> `/api/2.1/supervisor-agents/{tile_id}` for name, description and agents. Every UI spot uses `_sup_title(sup, "Supervisor A")`; `_is_system_name` blocks code-like names; live steps use `_pretty_tool`. Chart/tile names are no longer truncated.

- Supervisor naming (Oct 11): NEVER show `mas-xxxx-endpoint`. Resolution order: supervisor-agents API -> tile lookup via endpoint `tile_endpoint_metadata.tile_id` -> serving-endpoint tags `display_name` / `supervisor_description` (written by `iterations.tag_supervisor_endpoints`, run at the end of every Iteration Supervisor job as the user) -> name learned from the live stream -> neutral "Supervisor A/B". Long names wrap (grid headers, cards, tiles).
- 290 s limit: the Agent Bricks supervisor endpoint has a fixed ~290 s budget per request (platform-side). It is NOT the app's timeout (app client timeout is 600 s). When exceeded the supervisor returns a "reached the time limit ... Continue / Wrap up" message; the app now marks it ERROR:TIME_LIMIT and never scores it.

- Nav now has 8 tabs (`NAV_TAB_COUNT`); every nav-disabled list must use it. Busy rule (`_app_busy`, `_app_busy_now`, nav_click) covers bench, setup, teardown, iteration, comparison and robustness runs.
- Reliability page: `ROBUSTNESS_TESTS`, `_rob_sig` / `_rob_scores` (same scoring as notebook cell 28), `_run_rob_bg`, `page_robustness`, `handle_rob` (rob-poll 3s). Results in-memory only (`ST["rob_*"]`).

- CHAT PAGE (rebuilt Oct 11): old code POSTed to non-existent `/genie/spaces/{id}/conversations` (404) and called supervisors synchronously inside the Dash callback (request died, browser retried -> repeated 'Chat:' log lines, no reply). Now: `_chat_bg` thread + `chat-poll` Interval; Genie agents via `_chat_genie_stream` (agents/{id}/responses, stream, follow-ups pass `conversation_id`); supervisors via `call_supervisor_stream` with the full history list; per-target sessions in `ST["chat_sessions"]`; live steps panel; Markdown rendering; New conversation button; 290 s supervisor limit shown as a clear message. Verified live: Logistics Iter3 'hi' -> greeting, Western late rate 94.57, follow-up avg delay 2.94 (context kept); MCP supervisor 'hi' -> reply.

- 290 s ROOT CAUSE (corrected Oct 11): the app's supervisor call did not enable long-running mode. Without `"databricks_options": {"long_task": true}` the supervisor stops at its ~290 s request window with "Continue / Wrap up". With it, it emits a `task_continue_request`; the client resends input + output items + that item + `{"type":"task_continue_response","continue_request_id": id}` and it resumes. Verified live: Control Tower + MAIN_PROMPT = 278 s + 182 s (1 checkpoint), full 8,278-char brief. `call_supervisor_stream` now does this automatically (max `SUP_MAX_CONTINUATIONS` = 12).

### Deploy Command
```
apps deploy genie-curator-workbench --source-code-path /Workspace/Users/ssuman@bpcs.com/GAP_genie_multiagent_datanalysis_demo/src/genie-curator-workbench
```

## Regressions I Have Caused (LEARN FROM THESE)

1. **SP in warehouse code** — Added get_sp_headers() fallback to check_warehouse(). Broke warehouse detection. OBO is the ONLY auth for warehouse. REVERTED.

2. **Fake progression data** — Showed hardcoded expected scores (28, 35, 40, 45) for stages not yet run. Users thought these were real results. FIXED: ghost bars with "Not run".

3. **Blocking iteration callback** — run_iteration_1() ran synchronously in a Dash callback. User stared at static screen for minutes. FIXED: background thread + live progress.

4. **OBO in background threads** — Called get_headers() inside thread (no Flask context). All 45 benchmark tests failed instantly (0/45 in 4s). FIXED: capture obo_h before thread start.

5. **Clientside callback ghost IDs** — Removed Screen components but left their IDs in the clientside_callback loop. Dash crashed. FIXED: only setup-out, td-out, log-body.

6. **Duplicate callback outputs** — Two callbacks claiming same Output without allow_duplicate=True. Killed entire app. FIXED: always scan for duplicates before deploy.

7. **HEX["ink"] in PT font** — Plotly template font was #E8ECF7 (light blue). Invisible in light mode. FIXED: neutral #888.

8. **C dict with hardcoded hex** — C["ink"] was "#E8ECF7" instead of "var(--ink)". All inline styles were dark-mode only. FIXED: C uses CSS vars, HEX for Plotly.

9. **Warehouse banner stale after page render** — _build_warehouse_banner() called once at page load, became static HTML. Chip (update_wh_chip) updated every 8s but banner stayed frozen on "Starting". FIXED: added global-wh-banner div to layout, update_wh_chip returns (chip, banner) tuple.

10. **Tab transition desync (nav shows X, page shows Y)** — loading_refresh (1.5s interval) captures State("tab") when interval fires, not when callback executes. User clicks tab between fire and execution → page overwritten with old tab's content while nav shows new tab. FIXED: one-shot guard ST["_loading_done"].

11. **App auto-switches from Overview to Agents on launch** — qchat buttons (pattern-matching ALL) created during page re-render fire nav_click with n_clicks=0. nav_click didn't check click value, blindly switched to "agents" tab. FIXED: guard `if not ctx.triggered[0]["value"]: return no_update`.

12. **Teardown completes but Setup button stays disabled 0-10s** — poll_teardown didn't output to btn-setup. Setup button depended on auto_refresh_state (10s interval). FIXED: added btn-setup outputs to poll_teardown with allow_duplicate=True.

13. **clone_agents non-idempotent** — Created duplicate agents on re-run. FIXED: pre-scans existing agents by label, reuses if found.

14. **Iteration proceeds to benchmark after step failure** — run_iteration_1 returned string on failure, _run_iter1_full_bg treated it as success (iter_setup_done=True), benchmark ran on garbage. FIXED: string return → ABORT (iter_setup_error=True).

15. **Iteration badge stuck on "Running" after completion** — `poll_iter_bench` (2s) picks up completion and disables itself + updates buttons, but badges are only rendered by `page_iterations()` via `render()`, which requires a `page-refresh` bump. Previously only the 8s global poll could bump `page-refresh`, causing up to 8s stale badge + race condition where global poll sees intermediate state (benchmarks changed but iter_running not yet cleared). FIXED: added `Output("page-refresh","data", allow_duplicate=True)` as 18th output of `poll_iter_bench`; bumps `-int(n or 1)` on completion, forcing immediate full page re-render within 2 seconds.

16. **Cross-user alias contamination** — `_run_supervisor_refresh_job` read `ST.get("user_alias")` for job parameters. Since ST is global across all users, user B's alias could end up in user A's iteration supervisor job, causing `Missing Iteration N agents for alias=B`. Same bug in `_run_teardown_bg` (could delete wrong user's agents). FIXED: both functions now accept `obo_headers` parameter, derive alias/catalog via `_get_user_alias(obo_headers)` / `user_catalog(obo_headers)`. Success callbacks (`_it1_success`, `_it2_success`, `_it3_success`) pass `obo_h` through from the bg thread. Teardown captures `get_headers()` before `threading.Thread().start()`. Rule 36b added.

17. **Stop Iteration did not cancel supervisor job** — `_iter_stop_now` only invalidated the token and reset UI. If a supervisor refresh job was running, the bg thread's polling loop would cancel it at its next checkpoint (line 979). But there was an edge case: if the user stopped between `trigger_job()` returning and the first poll iteration, the job was orphaned. FIXED: (a) `_run_supervisor_refresh_job` stores `ST["_iter_sup_rid"] = rid` immediately after trigger; (b) `_iter_stop_now` pops and cancels that run ID directly; (c) `_log_step` after trigger is wrapped in try/except InterruptedError to cancel on immediate stop. All three paths clear `_iter_sup_rid` on completion.

18. **Domain agents card formatting** — `_agent_count_summary` produced plain text like `"Baseline 5 · Iteration 15"` (count ran into name). FIXED: returns styled `html.Span` components with bold version names, regular-weight counts, and styled `·` separators.

19. **Token tracking across all pages** — Token usage was only displayed on the benchmark page (total tokens in the last-run row). FIXED: Token counts are now captured and displayed on:
    - **Comparison page**: `call_supervisor_stream` returns `usage` dict; stored in `ST["cmp_usage_a"]` / `ST["cmp_usage_b"]`; shown in `_build_cmp_live` status bar, `_cmp_status_line` live stats, `_cmp_done_status` summary, and activity log.
    - **Chat page**: `_chat_bg` stores `run["usage"]` from `call_supervisor_stream`; shown in `_build_chat_view` step summary header ("Steps of the last reply (Xs • N tokens)") and `chat_poll` status line ("Replied in Xs • N tokens").
    - **Robustness page**: Each `ask_genie` call's `usage.total_tokens` accumulated in `_rob_tokens[0]` (thread-safe mutable), stored in `ST["rob_total_tokens"]`; shown live in `handle_rob` status ("Running... • N tokens so far") and after completion ("Complete • N tokens").
    - Token helper pattern: `_tok.get("total_tokens") or ((_tok.get("input_tokens") or 0) + (_tok.get("output_tokens") or 0))` — handles both `total_tokens` and split fields.

20. **Model info in supervisor cards** — `_fetch_endpoint_model(ep)` queries `/api/2.0/serving-endpoints/{ep}` config, extracts `served_entities[*].entity_name` + version, caches in `ST[f"_ep_model_{ep}"]`. Called from `_sup_profile`, returned as `prof["model"]`. Displayed in `sup_card` as "Model: {name}" below the Created line.

21. **Robustness page layout fix** — CSS rule `.stage.rob-chart{{margin-bottom:0}}` added to eliminate bottom gap between chart cards in the flex container. `_build_stability_section` H3 no longer has `marginTop: 28px` inline style (using card padding instead). `page_robustness` outer div has `paddingBottom: 60px` for page-end breathing room.

**Pattern**: Most regressions come from (a) mixing up OBO/SP, (b) touching working code without understanding why it works, (c) not running the pre-deploy checklist, (d) Dash callback race conditions with pattern-matching inputs and interval State captures, (e) removing per-page elements without adding a global replacement, (f) reading shared ST state for per-user parameters in multi-user scenarios.
