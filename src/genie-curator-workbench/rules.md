# Rules — Genie Curator Workbench

## ABSOLUTE Rules (never break)

1. **NEVER modify any notebook** other than main.py and iterations.py.
   The main workflow notebook (ID: 1022572831310835) is READ ONLY.

2. **NEVER modify app scopes via CLI or API.**
   OBO scopes (user_api_scopes) are managed ONLY by the user in the App UI
   (Settings > Authentication > User authorization). A previous CLI attempt
   (`apps create-update` with `update_mask`) wiped all scopes — this must never happen again.

3. **NEVER create tables** without explicit user permission.

4. **NEVER deviate from the job-based design** for setup and teardown.
   Setup triggers SETUP_JOB_ID. Teardown triggers TEARDOWN_JOB_ID.
   No "quick alternatives" like direct API teardown or SQL-based setup.

5. **OBO for all discovery/SQL/genie/serving calls.** SP credentials ONLY for Jobs API
   (trigger_job + poll_run use SP via App resources CAN_MANAGE_RUN; job has run_as=user).

6. **Setup job MUST pass `notebook_params={"agent_label": "Baseline"}`.**
   Without this param, the 08_setup notebook defaults to "Iteration 3" which is wrong.

## API Rules

7. **SQL Statement API `wait_timeout`**: Must be 0 or 5-50 seconds.
   Cap: `wait_s = min(max(timeout, 5), 50)`. NEVER send 60s.

8. **Supervisor discovery**: Use serving-endpoints API (`GET /api/2.0/serving-endpoints`)
   filtering for `mas-*-endpoint` pattern. The supervisor-agents API returns 403 with
   standard OBO scopes. This is the DESIGNED approach, not a fallback — log it cleanly
   without error/warning language like "falling back".

9. **Agent discovery must filter by label**, not just "SC" keyword.
   - At baseline: `discover_agents_by_label("Baseline")` → exactly 5
   - After Iter N: filter by "Iteration N" in title
   - The generic `discover_agents()` should NOT log "20 total agents" as if that's normal.
   - Log only the count relevant to the current stage.

10. **Tables sorted alphabetically** in `serialized_space.data_sources.tables`.

11. **`example_question_sqls` and `benchmarks.questions` sorted by `str(id)`.**

12. **Agent PATCH race condition**: After PATCH, retry GET to verify `data_sources`
    populated (2-4 second delay before data becomes available).

## UI/UX Rules

13. **No notebook references** in the app UI.
    No "Mirror of the notebook", "Run setup notebooks", "Steps 1-6, 8", etc.
    The app is the product — it doesn't reference its source.

14. **Buttons inline with each row**, not in separate sections at the bottom.

15. **Sequential iteration gating**: Only the next applicable iteration is enabled.
    All others are disabled. While one is running, all are disabled.

16. **Setup disabled when baseline exists**; only Teardown is active.
    After teardown completes, Setup re-enables.

17. **Consistent button styling**: Domain agents and supervisors use the same button
    class. No different colors for different agent types unless there's a clear UX reason.

## Coding Standards

18. **Every `except` block must log** — no bare `except: pass`.
    At minimum: `except Exception as e: log(f"context: {e}")`

19. **Preserve user comments** and existing structure when editing files.

20. **Python syntax check after every edit**: `import ast; ast.parse(content)`

21. **Test edits before deploying**: Verify the old text exists exactly once
    with `assert c.count(old) == 1` before replacing.

22. **`executeCode` file writes DO persist** to the .py file on disk.
    The notebook editor may show stale content — this is OK.
    Deploy reads the file, not the editor view.

## Async & UX Rules

 30. **Teardown (Full) must be async**: Background thread + td-poll interval.
    Never block the page during job execution — the page must remain responsive.

31. **Setup must be async**: Background thread + setup-poll interval.
    Step rows pulsate with CSS animation as each task starts/completes.

32. **Warehouse startup is non-blocking**: `_build_state_cards()` must NEVER call
    `ensure_warehouse_running()`. Fire-and-forget `POST .../start`, let the 10s
    auto-refresh interval pick up RUNNING state.

33. **Dependent cards show "Waiting for warehouse..."** when WH is STARTING.
    Use `.state-card.waiting` CSS class (dashed border, dimmed opacity).

34. **Setup button state is dynamic**: Updated every 10s by `auto_refresh_state`
    callback based on `ST["agents"]` and `ST["setup_running"]`.

35. **Output panels** (setup + teardown) are on the RIGHT side of their sections
    in a 60/40 flex layout. Both have visible scrollbars (V + H).

## Idempotency Rules

44. **All iteration operations must be idempotent.** Re-running setup, iterations, or
    baseline must NOT throw errors or create duplicates.
    - clone_agents: pre-scans existing agents by iteration label; reuses if found
    - COMMENT ON TABLE: idempotent (overwrites existing comment)
    - PATCH instructions/example SQL/benchmarks: idempotent (overwrites)
    - _save_benchmark_run: append-only (INSERT); _load_persisted_state takes LATEST per stage

45. **Error gating: if any step fails, STOP the iteration.**
    - run_iteration_1 returns tuple (text, spaces) on success, string on failure
    - _run_iter1_full_bg must treat string return as ABORT (set iter_setup_error=True)
    - Steps 3-5 check success count; if ZERO succeed, abort with error message

## Dash Callback Rules

46. **Pattern-matching callbacks with ALL fire on component creation.**
    When loading_refresh re-renders a page and creates new pattern-matching components
    (e.g. qchat buttons), Dash fires the callback with n_clicks=0. ALWAYS guard with
    click value check: `if not ctx.triggered[0]["value"]: return no_update`

47. **Interval callbacks capture State() at fire time, not execution time.**
    loading_refresh reads State("tab") when the interval fires. If the user clicks a
    tab between fire and execution, it overwrites page.children with the old tab's content.
    Use one-shot guards (ST["_loading_done"]) for any callback that writes to page.children.

48. **Three callbacks write to page.children — treat as mutually exclusive:**
    - render (primary, Input: tab.data) — ALSO updates nav classes
    - loading_refresh (one-shot, Input: loading-poll interval) — does NOT update nav
    - refresh_overview (Input: btn-refresh-overview click) — always renders overview
    Never add a new page.children writer without understanding all three.

49. **poll_teardown must immediately enable btn-setup.**
    Don't rely on auto_refresh_state (10s lag). Add btn-setup as output to poll_teardown.

50. **Global elements for live-updating state.**
    The warehouse banner (global-wh-banner) lives OUTSIDE html.Div(id="page") so it
    updates every 8s via update_wh_chip regardless of which page is active.
    Per-page banners provide initial render state only.

## Deployment

23. **Deploy command**: Always use:
    ```
    apps deploy genie-curator-workbench --source-code-path /Workspace/Users/ssuman@bpcs.com/GAP_genie_multiagent_datanalysis_demo/src/genie-curator-workbench
    ```

24. **After scope changes**, user must open app in incognito/private window
    to get a fresh OBO token with the new scopes.

## Background Thread Rules

36. **NEVER call get_headers() inside a background thread.**
    Flask request context is thread-local. Always capture `obo_h = get_headers()`
    BEFORE `threading.Thread(...).start()` and pass as parameter.

36b. **NEVER read ST['user_alias'] or ST['user_catalog'] for job parameters.**
     ST is a single global dict shared by ALL concurrent users. If user A sets
     `ST['user_alias'] = 'a'` and user B triggers a job, it picks up 'a'.
     All job-triggering code MUST derive alias/catalog from per-request OBO headers:
     `_alias = _get_user_alias(obo_headers)`, `_cat = user_catalog(obo_headers)`.
     ST reads are ONLY acceptable for display/telemetry (heartbeat, UI chips).

37. **NEVER block a Dash callback** for multi-minute operations.
    All long-running work (setup, teardown, benchmarks, iteration features) MUST
    go in background threads with polling intervals for live progress.

38. **UI MUST always show what is happening.** User should never stare at a static
    screen. Every background operation needs: status text, progress indicator
    (shimmer/dots), and live updates via polling.

## Color Rules

39. **Plotly charts MUST use HEX["xxx"]** for marker_color, colors, font colors.
    Plotly cannot resolve CSS variables.

40. **Inline styles MUST use C["xxx"]** (CSS variable refs) for theme adaptation.
    Never hardcode hex values in style dicts.

41. **NEVER show fake/expected data.** Progression chart, score tiles, and test
    grids must show ONLY actual benchmark results. Unrun stages show ghost bars
    with "Not run" text, not hardcoded expected scores.

## Warehouse Rules

42. **Warehouse check and start use OBO ONLY.** `check_warehouse()` and warehouse
    start POST both use `get_headers()`. NEVER use `get_sp_headers()` for warehouse.
    SP is ONLY for Jobs API (trigger_job + poll_run).

43. **Header WH chip is dynamic** (`id="wh-chip"`, `global-poll` 8s interval).
    Shows warehouse NAME (not ID) with color: teal=RUNNING, indigo=STARTING, rose=STOPPED.

## Data Rules

25. **All data is deterministic**: `base_date = datetime(2026, 9, 1)` + seeds 42-45.
    All SQL uses `DATE '2026-09-01'` not `CURRENT_DATE()`.

26. **"Last month" = August 2026**, "Prior month" = July 2026.

27. **Q1 = Fiscal Q1 = Jul-Aug-Sep** for this organization (FY starts July).
    Calendar Q1 (Jan-Mar) is WRONG.

28. **59 vs 107**: `inventory_ledger` has multiple rows per SKU (one per warehouse).
    `COUNT(*) = 107`, `COUNT(DISTINCT sku_id) = 59`.

## Read-First Protocol

29. **At the start of every new chat session**, read all 4 MD files before taking
    any action:
    - `product.md` — what are we building
    - `design.md` — how are we building it
    - `rules.md` — what constraints to obey
    - `test.md` — how do we know it's correct
