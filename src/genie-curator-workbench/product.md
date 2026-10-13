# Product Requirements — Genie Curator Workbench

## What This Demo Proves

Enterprise data warehouses contain the answers to complex business questions — but AI agents
often **hallucinate**, **misinterpret column names**, or **generate incorrect SQL** when querying
that data. This demo shows how to build AI-powered data analysis agents on Databricks that are
**grounded to business truth** and **consistently generate correct queries** across a multi-domain
supply chain data warehouse — without hallucination.

The last mile of accuracy requires **data governance** (formal business definitions, Unity Catalog
tags, metric views) — not just better prompts.

## The Business Scenario

A multi-regional retail company sells 100 SKUs across 5 product families (Apparel, Accessories,
Footwear, Home Goods, Electronics) through 4 US regions (Western, Eastern, Central, Southern).
Products are sourced from 12 international suppliers across Asia, Europe, North America, stored
in 12 warehouses, and shipped via 8 carriers through 7 distribution centers.

The company's data warehouse is organized in Unity Catalog with 5 domain schemas — each owned by
a different business team — plus a shared `reporting` schema for cross-domain executive views.

## The Single Canonical Prompt

> "We need a complete supply chain health check for our West region in August 2026. The CFO wants
> to understand what drove the revenue decline versus July — show the actual August and July revenue
> numbers, the dollar change, and the percentage change — and which product families are most at
> fault. Are our on-time delivery rate and average delay for West region shipments contributing to
> the problem? How many total shipments went out and how many were late? I also need our current
> fill rate, how many inventory positions are sitting below safety stock in the West region, how
> many unique SKUs are affected, what is our days of supply for those at-risk items, and how many
> SKUs are completely stocked out. On the vendor side: what percentage of vendors delivered late in
> August, how many purchase orders were late out of total, what is the average lead time variance,
> and what are the total vendor SLA penalties we have incurred? Bring it all together as our total
> Cost of Disruption by region for August 2026 — cancelled revenue, at-risk backorder revenue,
> wasted freight on late shipments, and supplier penalty exposure in one number per region. Are we
> going to miss our Q1 service-level targets, and what are the top actions we should take?"

This prompt is deliberately designed so that every measure in every UC Metric View is an expected
metric the agent should return. It uses business vocabulary that does NOT match column names.

## App Goal

The **Genie Curator Workbench** is the visual control plane for this workshop. It lets users
provision, iterate, benchmark, and compare Genie Agent configurations — progressing from a
non-deterministic ~62% baseline to a fully deterministic 100% (45/45) through 3 iterations
of UC Semantic improvements.

## User Stories

### Overview Page
- See tile counts: domain agents, supervisors, benchmark runs, latest score.
- See each domain agent row (name, space ID, status) with an inline Chat button.
- See each supervisor row (name, endpoint, description) with an inline Chat button.
- Chat buttons navigate to the Agents tab with the selected agent pre-loaded.

### Setup & Teardown Page
- See live environment state: 5-card grid (warehouse, catalog, agents, supervisors, schemas).
- Warehouse card pulsates (indigo glow) when starting; dependent cards show "Waiting for warehouse..."
- 60/40 layout: controls on left, scrollable output panel on right (both setup and teardown).
- Run Full Setup (async, non-blocking): triggers SETUP_JOB_ID, shows live per-task progress with pulsating step rows.
- Run Full Teardown (async, non-blocking): triggers TEARDOWN_JOB_ID, shows live job state progress.
- Setup button dynamically disables when agents exist; re-enables within 10s after teardown.
- Selective teardown: "Delete Non-Iter-3 Only", "Delete ALL SC", "Delete Supervisors Only".

### Baseline Page
- Run the 45-test benchmark against baseline agents.
- See per-test PASS/FAIL grouped by domain. Expected: ~28/45 (62%), non-deterministic.

### Iterations Page
- Apply Iteration 1, 2, or 3 sequentially.
- Strictly sequential: Iter 1 needs baseline, Iter 2 needs Iter 1, Iter 3 needs Iter 2.
- Only the next applicable iteration button is enabled; all others disabled.
- While an iteration runs, all iteration buttons are disabled.
- Each iteration: clones previous agents, applies UC semantic features, updates supervisors, runs benchmarks.

### Comparison Page
- Select two supervisor endpoints; send same CFO prompt to both; see side-by-side scoring.

### Agents (Chat) Page
- Chat with any discovered domain agent or supervisor via Genie Agent Mode API.

### Logs Page
- Live timestamped application log.

## Iteration Progression (13 UC Semantic Features)

### Iteration 1 — Metadata Enrichment (Baseline ~67% → ~78%)
| # | Feature              | Mechanism                                    |
|---|----------------------|----------------------------------------------|
| 1 | Column Comments      | ALTER TABLE ALTER COLUMN COMMENT              |
| 2 | Table Comments       | ALTER TABLE SET TBLPROPERTIES (comment)       |
| 3 | Example SQL Queries  | example_question_sqls in serialized_space API |
| 4 | Benchmark Questions  | benchmarks.questions in serialized_space API  |

### Iteration 2 — Business Semantics (~78% → ~87%)
| # | Feature              | Mechanism                                    |
|---|----------------------|----------------------------------------------|
| 5 | UC Metric Views (4)  | CREATE VIEW ... WITH METRICS LANGUAGE YAML   |
| 6 | Open Knowledge View  | Cross-domain governed join (CoD by region)   |
| 7 | UC Governed Tags     | ALTER TABLE SET TAGS (domain, metric_type)   |
| 8 | Schema Domain Tags   | ALTER SCHEMA SET TAGS (domain, business_unit)|

### Iteration 3 — Governance Layer (~87% → 100%)
| # | Feature              | Mechanism                                    |
|---|----------------------|----------------------------------------------|
| 9 | UC Domain            | Discover page (UI only, manual step)         |
| 10| UC Pages (7 pages)   | Discover page (UI only, manual step)         |
| 11| Reference Table      | CREATE TABLE fiscal_targets                  |
| 12| SQL Functions (5)    | CREATE FUNCTION get_critical_*()             |
| 13| Entity Matching      | serialized_space API (string column matching)|

Additional Iter 3 features: ai_forecast capability added to all 5 domain agents.

## Agent Counts At Each Stage

| Stage             | Domain Agents | Evaluator | Supervisors | Total SC Agents |
|-------------------|---------------|-----------|-------------|-----------------|
| After Setup       | 5 (Baseline)  | 1         | 2           | 8               |
| After Iter 1      | +5 (Iter 1)   | 1         | 2           | 13              |
| After Iter 2      | +5 (Iter 2)   | 1         | 2           | 18              |
| After Iter 3      | +5 (Iter 3)   | 1         | 2           | 23              |
| After Cleanup     | 5 (Iter 3)    | 0         | 2           | 7               |

## 5 Domain Agents (per iteration)

| Domain           | Schema                   | Key Fact Table    |
|------------------|--------------------------|-------------------|
| Demand Analysis  | demand_analysis          | sales_orders      |
| Inventory Mgmt   | inventory_management     | inventory_ledger  |
| Logistics Ops    | logistics_operations     | shipments         |
| Supplier Risk    | supplier_procurement     | supplier_orders   |
| Executive Report | reporting                | (cross-domain)    |

## 2 Supervisors

| Name                       | Type             | Description                              |
|----------------------------|------------------|------------------------------------------|
| Supply Chain Control Tower ({alias}) | Supervisor Agent | Routes CFO questions to 5 domain agents + 5 policy SQL functions |
| Supply Chain MCP Supervisor ({alias}) | MCP Supervisor   | Genie One MCP — alternative single-tool architecture |

## Token Economics (from README measurement)

| Metric                  | Without UC Semantics | With UC Semantics | Reduction |
|-------------------------|----------------------|-------------------|-----------|
| Tokens per call         | 7,286                | ~100-200          | 36-73×    |
| Daily (500 calls)       | 3,643,000            | 50,000-100,000    | 36-73×    |
| Per Supervisor question | 36,430               | 500-1,000         | 36-73×    |

UC Semantics doesn't just improve accuracy — it fundamentally changes the cost structure.

## Out of Scope
- Multi-user collaboration / concurrent sessions
- Modifying source notebooks from the app
- Creating agents outside the SC domain
- Direct SQL/API teardown or setup — always via pre-created Jobs
- Agent memory or conversation persistence across sessions

## Current Status (Oct 6, 2026)

**Baseline + Iteration 1 working.** Setup, teardown, baseline benchmark (34/45), and
Iteration 1 (clone, comments, instructions, example SQL, benchmarks) all run successfully.
Persistent storage in Delta table. All Dash callback race conditions fixed.

### What Works Today
- Full Setup (async, with per-task progress): triggers job, polls via SP, shows live output
- Full Teardown (async, with live progress, prominent report, immediate Setup button enable)
- Baseline benchmark: 45-test suite, parallel 8 workers, persisted to Delta
- Iteration 1: clone agents, apply comments/instructions/example SQL/benchmarks, benchmark
- Idempotent re-runs: clone_agents reuses existing agents, COMMENT ON TABLE overwrites
- Error gating: if any iteration step fails, STOP (no benchmark on garbage)
- Environment state cards: 5-card grid with pulsating warehouse + waiting dependents
- Dynamic button enable/disable (setup enables IMMEDIATELY after teardown)
- Global warehouse banner: updates every 8s alongside header chip (no stale state)
- Tab transitions: instant, no desync, no phantom auto-navigation
- Persistent state: benchmark runs stored in GAP_Demo_Dev.workbench.benchmark_runs
- Stage progression: 4-stage dots with scores loaded from Delta on startup
- OBO auth for all discovery/SQL/genie + SP auth for Jobs API

### Next: Iterations 2 & 3
- Iteration 2: metric views + governed tags + CoD view
- Iteration 3: fiscal_targets + SQL functions + UC Pages (manual step)
- Comparison page: side-by-side supervisor scoring

## Status Update (Oct 11, 2026)

### Changes this session (local, not yet deployed unless noted)
- Teardown (09_teardown) now pages through ALL Genie agents before deleting (was first page of 20 only -> leftovers -> Genie added timestamp suffixes to new titles). Live for the next Teardown run (job runs the notebook directly).
- DOMAIN AGENTS count card / tile = total across Baseline + Iterations 1-3 (20 after Iter 3) with per-version breakdown.
- Overview + Chat: agent version picker (Baseline / Iteration 1 / 2 / 3).
- Iteration cards: View Setup Log button (disabled until a log exists) + 16px gap above the results area.
- Iter 3 manual-step agent links now open the workspace Genie page (`https://<workspace>/genie/rooms/<id>`), not the app.

### Known Issue: E01 "What is our current fill rate?" regresses in Iteration 3
- Ground truth 80.7 = `reporting.executive_kpis.service_level_pct` (fill rate and service level are the same metric in this demo).
- Baseline/Iter 1/Iter 2: Executive agent has no column literally named "fill rate", so it maps the question to service_level_pct -> PASS.
- Iter 3 adds `reporting.fiscal_targets` with `fill_rate_target_pct` (comment "Target fill rate", Q1 FY2027 = 90.0). The agent now sees an explicit fill-rate column, treats service_level_pct as a *different* metric, and answers with the 90.0 target -> MISS.
- This is a semantic collision introduced by Iter 3, not agent randomness. Candidate semantic-layer fixes (awaiting user decision): (a) comment service_level_pct as the ACTUAL fill rate / service level and fill_rate_target_pct as TARGET only; (b) add a fill-rate synonym rule to the Executive agent instructions in Iter 3.

### Gap: Reliability & Dependability (Robustness) page does not exist in the app
- The Curator Workbench notebook has it: Deep Reliability Analysis (SQL pattern consistency, provenance stability, value reliability, reliability matrix, flip analysis, verdict) and Robustness Test (10 hardest tests x 3 rephrased variations = 30 Genie calls; Reliability = % of variations matching ground truth, Dependability = SQL-signature consistency at 100/85/60/33/50/0 levels).
- Tests in the notebook robustness set: A02, A03, D04, F02, H03, H05, G01, P01, P04, P02.
- The app has no equivalent page yet (no robustness / reliability / dependability code in main.py).

### Comparison page upgrade (Oct 11, local -- not yet deployed)
- Supervisor cards: name, endpoint, created time, architecture badge, description and tool list (which Genie agents / MCP tool, and which agent version).
- Live view per supervisor: scrollable "Live steps" box (tool calls with the exact sub-question, sub-agent results, Genie One progress, routing) and a scrollable "Supervisor output" box that streams the answer as it is written. Status line shows both sides' current step.
- The CFO prompt sent to both supervisors is shown in full, formatted like the workshop HTML prompt box with colour-coded business phrases and a legend.
- Live check against both live supervisors passed (Control Tower: 1 tool call, 34s; Genie One MCP: 4 tool calls, 46s).

### Oct 11 (later): E01 fix + Reliability page (local -- not yet deployed)
- E01 fix (Option A, user-approved) in iterations.py Iteration 3: `executive_kpis.service_level_pct` commented as the ACTUAL current fill rate (= service level); `fiscal_targets.fill_rate_target_pct` commented as the TARGET only, pointing to the actual column. Takes effect the next time Iteration 3 is applied.
- New "Reliability" tab (Reliability & Dependability): pick an agent version, run the notebook's robustness test (10 hardest tests x 3 rephrasings = 30 Genie calls, 3 in parallel, background thread, live log). Shows overall Reliability / Dependability, per-test scores with each variation's question, value and SQL, a variation heatmap, a reliability-vs-dependability chart, and a cross-iteration stability section (fixed / regressed / flaky tests, stable values, deploy verdict) from the benchmark runs held in this app session.
