# Test Strategy — Genie Curator Workbench

## 45 Ground Truth Assumptions

Each test has: ID, domain agent, metric name, expected value, description, iteration that fixes it.

### Group A: Logistics (6 tests)
| ID  | Metric               | Expected    | Fix   |
|-----|----------------------|-------------|-------|
| A01 | on-time delivery rate| 5.43        | Iter2 |
| A02 | late delivery rate   | 94.57       | Iter2 |
| A03 | avg delay days       | 2.94        | Iter2 |
| A04 | total shipments      | 1086        | Iter2 |
| A05 | late shipments       | 1027        | Iter2 |
| A06 | wasted freight       | 2484985.57  | Iter2 |

### Group B: Demand (4 tests)
| ID  | Metric               | Expected     | Fix   |
|-----|----------------------|--------------|-------|
| B01 | August revenue       | 3341062.58   | Iter2 |
| B02 | July revenue         | 4581392.70   | Iter2 |
| B03 | revenue change $     | -1240330.12  | Iter2 |
| B04 | revenue change %     | -27.07       | Iter2 |

### Group C: Inventory (5 tests)
| ID  | Metric                  | Expected | Fix   |
|-----|-------------------------|----------|-------|
| C01 | positions below SS      | 109      | Iter2 |
| C02 | unique SKUs below SS    | 61       | Iter2 |
| C03 | stockout positions      | 35       | Iter2 |
| C04 | stocked-out SKUs        | 33       | Iter2 |
| C05 | days of supply          | 0.96     | Iter2 |

### Group D: Supplier (7 tests)
| ID  | Metric            | Expected | Fix   |
|-----|-------------------|----------|-------|
| D01 | total POs         | 48       | Iter2 |
| D02 | late POs          | 36       | Iter2 |
| D03 | vendor late %     | 75.0     | Iter2 |
| D04 | avg LTV           | 8.69     | Iter1 |
| D05 | Asia late %       | 100.0    | Iter1 |
| D06 | Asia LTV          | 13.67    | Iter1 |
| D07 | Asia POs          | 30       | Iter2 |

### Group E: Cross-Domain (3 tests)
| ID  | Metric        | Expected    | Fix   |
|-----|---------------|-------------|-------|
| E01 | fill rate     | 80.70       | Iter2 |
| E02 | SLA penalties | 1185043.10  | Iter2 |
| E03 | total CoD     | 3757298.31  | Iter2 |

### Group F: Indirect (6 tests)
| ID  | Metric             | Expected    | Fix   |
|-----|--------------------|-------------|-------|
| F01 | West revenue       | 3341062.58  | Iter1 |
| F02 | vendor late %      | 75.0        | Iter1 |
| F03 | fulfilled orders   | 1342        | Iter2 |
| F04 | cancelled revenue  | 179419.26   | Iter2 |
| F05 | backordered count  | 275         | Iter2 |
| F06 | Home Goods decline | 349062.88   | Iter1 |

### Group H: Hard (7 tests)
| ID  | Metric                | Expected    | Fix   |
|-----|-----------------------|-------------|-------|
| H01 | Europe LTV            | 0.38        | Iter1 |
| H02 | NA LTV                | 0.40        | Iter1 |
| H03 | order fulfillment rate| 71.23       | Iter1 |
| H04 | partial fulfillment   | 9.02        | Iter2 |
| H05 | revenue at risk       | 3138569.66  | Iter2 |
| H06 | per-SKU risk          | 14368.63    | Iter2 |
| H07 | CoD/revenue ratio     | 1.12        | Iter2 |

### Group G: Fiscal (2 tests)
| ID  | Metric           | Expected | Fix   |
|-----|------------------|----------|-------|
| G01 | Q1 SL target     | 92.0     | Iter3 |
| G02 | Q1 SL target val | 92.0     | Iter3 |

### Group P: Policy/Threshold (5 tests)
| ID  | Metric                 | Expected | Fix   |
|-----|------------------------|----------|-------|
| P01 | logistics risk         | 176      | Iter3 |
| P02 | demand anomaly         | 77       | Iter3 |
| P03 | inventory risk         | 106      | Iter3 |
| P04 | quality minimum        | 11       | Iter3 |
| P05 | disruption threshold   | 3        | Iter3 |

## Expected Pass Counts Per Stage

| Stage    | A  | B  | C  | D  | E  | F  | G  | H  | P  | Total |
|----------|----|----|----|----|----|----|----|----|----| ------|
| Baseline | ~3 | ~3 | ~3 | ~3 | ~1 | ~3 | 0  | ~3 | 0  | 28/45 |
| Iter 1   | ~3 | ~3 | ~3 | ~5 | ~1 | ~6 | 0  | ~5 | 0  | 35/45 |
| Iter 2   | 6  | 4  | 5  | 7  | 3  | 6  | 0  | 7  | 0  | 40/45 (est) |
| Iter 3   | 6  | 4  | 5  | 7  | 3  | 6  | 2  | 7  | 5  | 45/45 |

Note: Baseline scores are non-deterministic (~). After Iter 2+, scores are deterministic.

## Acceptance Tests (App UI)

### Setup & Teardown
1. Open app → Environment State cards show warehouse (RUNNING), catalog, agents, schemas.
2. Click "Run Full Setup" → async: triggers job in background, shows live per-task progress with pulsating steps, cards update when complete.
3. Click "Full Teardown" → async: triggers job in background, shows live state progress (PENDING → RUNNING → TERMINATED → COMPLETE), cards reset to empty.
4. Setup button disabled when agents exist; re-enables after teardown.

### Overview
1. Agent table shows 5 domain agents with inline Chat buttons.
2. Supervisor table shows 2 supervisors with inline Chat buttons.
3. Click Chat → navigates to Agents tab with target pre-selected.

### Iterations
1. Only Iter 1 button enabled when baseline exists (Iter 2, 3 disabled).
2. After Iter 1 completes → Iter 2 enables, Iter 1 disables.
3. While an iteration runs → all iteration buttons disabled.
4. After all 3 complete → Iter 3 shows "Re-run Iteration 3" (enabled), Iter 1/2 show "Re-run" (disabled).
5. Badges update to "Completed" within 2 seconds of iteration finishing (poll_iter_bench bumps page-refresh).
6. Supervisor refresh job passes the CURRENT user's alias (from OBO headers), not a stale ST value.

### Agents (Chat)
1. Select agent → send question → response appears.
2. Uses agent mode API: POST /api/2.0/genie/agents/{id}/responses

### Comparison
1. Select two supervisors → click "Run Comparison" → side-by-side results.

## Regression Checklist (MUST CHECK BEFORE EVERY DEPLOY)

### Auth (Rule 5: OBO for everything except Jobs API)
- [ ] check_warehouse() uses OBO only (get_headers). NO get_sp_headers.
- [ ] update_wh_chip() uses OBO only. NO get_sp_headers.
- [ ] _discover_all() warehouse start uses OBO only. NO get_sp_headers.
- [ ] _build_state_cards() warehouse start uses OBO only. NO get_sp_headers.
- [ ] ask_genie() uses OBO (passed headers or get_headers).
- [ ] call_supervisor() uses OBO (get_headers).
- [ ] discover_agents() uses OBO. discover_supervisors() uses OBO.
- [ ] ONLY trigger_job() and poll_run() use get_sp_headers() (Jobs API).
- [ ] submit_notebook_run() uses get_sp_headers() (Jobs API runs/submit).

### Dash Callback Safety
- [ ] ast.parse(content) passes (syntax check)
- [ ] No duplicate Output IDs without allow_duplicate=True
- [ ] Clientside callback loop IDs match existing Screens ONLY: setup-out, td-out, log-body
- [ ] All callback Output IDs have matching components in layout
- [ ] nav_click guards against phantom triggers (n_clicks=0 from component creation)
- [ ] loading_refresh has one-shot guard (ST["_loading_done"]) — never overwrites user's tab
- [ ] Pattern-matching callback inputs (ALL) don't fire phantom triggers on page re-render
- [ ] poll_teardown outputs btn-setup.disabled + btn-setup.className + setup-btn-hint.children
- [ ] update_wh_chip returns (chip, banner) tuple — global-wh-banner updates with chip

### Color Architecture
- [ ] No C["xxx"] in Plotly chart code (marker_color, colors, etc.) — must use HEX["xxx"]
- [ ] No HEX["xxx"] in inline style dicts — must use C["xxx"] (CSS vars)
- [ ] Plotly template PT uses paper_bgcolor="rgba(0,0,0,0)" and font color="#888"

### Background Threads (OBO captured BEFORE thread start)
- [ ] _run_setup_bg receives obo_headers as parameter (captured in callback)
- [ ] _run_bench_bg receives obo_headers as parameter
- [ ] _run_iter_setup_bg receives obo_h as parameter (captured in _iter_button_click)
- [ ] _run_iter_bench_bg receives obo_headers as parameter
- [ ] _run_cmp_bg receives obo_headers as parameter
- [ ] _run_teardown_bg receives obo_headers as parameter (captured in handle_teardown)
- [ ] _run_supervisor_refresh_job receives obo_headers parameter (from success callbacks)
- [ ] _it1/_it2/_it3_success callbacks receive obo_h from bg thread, forward to supervisor job
- [ ] NEVER read ST['user_alias'] or ST['user_catalog'] for job parameters (multi-user safety)
- [ ] _iter_stop_now cancels supervisor job via ST['_iter_sup_rid'] (stored on trigger, popped on stop)
- [ ] _run_supervisor_refresh_job wraps post-trigger _log_step in try/except InterruptedError → cancel

### Data Integrity
- [ ] Progression chart shows ONLY actual benchmark results (no hardcoded expected scores)
- [ ] Unrun stages show ghost bars with "Not run" text
- [ ] Setup passes notebook_params={"agent_label": "Baseline"}
- [ ] wait_timeout capped 5-50s (never 60+)

### UI/UX
- [ ] Warehouse header chip shows NAME (not ID) with color status
- [ ] "Stage" label (not "Label") on baseline page
- [ ] Last-run info shows timestamp + user below benchmark progress bar
- [ ] Iteration setup shows live step progress (not blocking, not static)
- [ ] All long-running ops in background threads with polling

## Additional Acceptance Tests (Added Sept 28)

### Async Behavior
1. Click "Full Teardown" → page remains responsive (no "Updating..." in browser tab).
2. Click "Run Full Setup" → page remains responsive, step rows pulsate as tasks run.
3. Warehouse starting → warehouse card pulsates indigo, dependent cards show "Waiting for warehouse..."
4. After teardown → Setup button re-enables within 10 seconds (dynamic refresh).
5. Output panels show both vertical and horizontal scrollbars.

### Layout
1. Teardown section: buttons on left (60%), output panel on right (40%).
2. Setup section: steps + button on left (60%), output panel on right (40%).
3. Output panels constrained to max-height:420px with visible styled scrollbars.
4. Hint text next to Setup button truncates with ellipsis (no word wrapping).

## Performance Targets

- Environment State cards: < 5 sec to populate
- Agent discovery: < 2 sec
- Supervisor discovery: < 3 sec
- Setup job: ~5-10 min (7 steps)
- Teardown job: ~2-3 min
- Single benchmark test (ask_genie): < 30 sec per question
- Full 45-test suite: ~15-20 min
