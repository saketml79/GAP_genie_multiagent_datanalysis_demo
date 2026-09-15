# Supply Chain Control Tower — Multi-Agent Genie Workshop

## What This Demo Proves

Enterprise data warehouses and data marts contain the answers to complex business questions — but AI agents often **hallucinate**, **misinterpret column names**, or **generate incorrect SQL** when querying that data. This demo shows how to build AI-powered data analysis agents on Databricks that are **grounded to business truth** and **consistently generate correct queries** across a multi-domain supply chain data warehouse — without hallucination.

Using a realistic supply chain scenario with 23 tables across 5 business domains, this workshop:

- Builds a **Supervisor Agent** that orchestrates 5 domain-specific **Genie Agents** to answer complex cross-domain business questions
- Starts at **30% accuracy** with a minimal baseline and progressively improves to **100% accuracy** across 6 iterations
- Demonstrates **why each improvement matters**: certified queries, column synonyms, agent instructions, supervisor hardening, metric views, and business definitions
- Shows that the last mile of accuracy requires **data governance** (formal business definitions, Unity Catalog tags, metric views) — not just better prompts

### What Are Genie Agents and Supervisor Agents?

**[Genie Agents](https://docs.databricks.com/en/genie/index.html)** (formerly called Genie Spaces) are AI-powered data analysis agents in Databricks. You give them access to specific tables, and they translate natural-language questions into SQL, execute the query, and return results. They can be enhanced with certified queries, column synonyms, and instructions to improve accuracy.

**[Supervisor Agents](https://docs.databricks.com/en/generative-ai/agent-framework/build-supervisor-agent.html)** orchestrate multiple sub-agents (including Genie Agents) as tools. The Supervisor receives a complex question, decides which sub-agents to call and in what order, synthesizes their results, and produces a unified answer. This enables cross-domain analysis that no single agent could perform alone.

##### Ground Truth: Demo Proxy for Real-World Feedback

> In this demo, we use a **ground truth table** — 10 pre-computed KPIs with known correct values — to objectively measure whether the agents are generating the right SQL and returning the right numbers. After each improvement iteration, we compare the Supervisor's output against ground truth and score it (EXACT / CLOSE / MISS).

> **In production, there is no ground truth table.** Instead, accuracy improves through an iterative **user feedback loop**:

1. A business user asks a question via a Genie Agent or Supervisor Agent
2. The agent generates SQL and returns a result
3. The user reviews the result and provides feedback: 👍 (correct) or 👎 (wrong)
4. A data team member reviews the feedback, identifies the root cause (wrong column, wrong filter, ambiguous metric), and applies a fix — exactly the same kinds of fixes shown in this demo (certified queries, synonyms, instructions, metric views)
5. Over time, the agent gets better and better at answering questions correctly

> **This demo compresses months of user-feedback-driven improvement into 6 scripted iterations**, so you can see the full journey in a single workshop session. Every fix we apply (certified queries, synonyms, metric views, business definitions) is the same fix a data team would apply in response to real user feedback.

---

## The Business Scenario

A **multi-regional retail company** sells 100 SKUs across 5 product families (Apparel, Accessories, Footwear, Home Goods, Electronics) through 4 US regions (Western, Eastern, Central, Southern). Products are sourced from 12 international suppliers across Asia, Europe, and North America, stored in 12 warehouses, and shipped via 8 carriers through 7 distribution centers.

The company's data warehouse is organized in [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) with 5 domain schemas — each owned by a different business team — plus a shared `reporting` schema for cross-domain executive views.

### Data Model (Final State — after all 6 iterations)

The demo generates 23 base tables (~130K total rows) and progressively adds 5 metric views + 1 cross-domain view through the improvement iterations.

```mermaid
graph TD
    CAT["GAP_Demo_Dev\n(Unity Catalog)"]

    CAT --> DA["demand_analysis\n6 tables + 1 metric view"]
    CAT --> IM["inventory_management\n4 tables + 1 metric view"]
    CAT --> LO["logistics_operations\n4 tables + 1 metric view"]
    CAT --> SP["supplier_procurement\n5 tables + 1 metric view"]
    CAT --> RP["reporting\n5 views + 1 table"]

    DA --- DA_T["products \u00b7 customer_segments \u00b7 sales_orders\ndemand_forecasts \u00b7 pos_data \u00b7 promotions"]
    DA --- DA_V["\ud83d\udcca revenue_comparison_by_region"]

    IM --- IM_T["warehouse_data \u00b7 inventory_ledger\nstore_inventory \u00b7 stock_movements"]
    IM --- IM_V["\ud83d\udcca inventory_safety_stock_metrics"]

    LO --- LO_T["carriers \u00b7 distribution_centers\nshipments \u00b7 transit_data"]
    LO --- LO_V["\ud83d\udcca delivery_performance_by_region"]

    SP --- SP_T["suppliers \u00b7 supplier_orders\nsupplier_lead_times \u00b7 vendor_slas \u00b7 procurement_data"]
    SP --- SP_V["\ud83d\udcca supplier_performance_by_continent"]

    RP --- RP_V["executive_kpis \u00b7 regional_performance_summary\nrevenue_trend \u00b7 supply_chain_risk_scorecard\ncost_of_disruption_by_region \u00b7 ground_truth_kpis"]

    style DA_V fill:#e1f5fe,stroke:#0288d1
    style IM_V fill:#e1f5fe,stroke:#0288d1
    style LO_V fill:#e1f5fe,stroke:#0288d1
    style SP_V fill:#e1f5fe,stroke:#0288d1
```

**Metric views** (blue) are pre-computed summaries added in iterations 5–6. They encode the exact business definition in the column name (e.g., `sku_warehouse_positions_below_safety_stock` instead of an ambiguous `COUNT(*)`) — this is what eliminates the last sources of agent error.

| Schema | Key Tables (bold = primary fact table) | Rows | Business Domain |
|--------|---------------------------------------|------|----------------|
| `demand_analysis` | products, customer_segments, **sales_orders**, demand_forecasts, pos_data, promotions | ~74K | Revenue, orders, demand forecasting |
| `inventory_management` | warehouse_data, **inventory_ledger**, store_inventory, stock_movements | ~15K | Stock levels, stockouts, safety stock |
| `logistics_operations` | carriers, distribution_centers, **shipments**, transit_data | ~38K | Delivery performance, delay tracking |
| `supplier_procurement` | suppliers, **supplier_orders**, supplier_lead_times, vendor_slas, procurement_data | ~1.7K | Supplier reliability, SLA penalties |
| `reporting` | executive_kpis, regional_performance_summary, revenue_trend, supply_chain_risk_scorecard, cost_of_disruption_by_region, ground_truth_kpis | Views | Cross-domain executive dashboards |

All data is generated with a **fixed reference date** of September 1, 2026 (`base_date = datetime(2026, 9, 1)`). "Last month" = August 2026, "Prior month" = July 2026. The demo produces identical results every run.

---

## The Two Test Prompts

### Prompt 1 — The Main Business Question (used for iterations 1–5)

> **"Why did revenue drop in the Western Region last month, are we going to miss our quarterly service-level targets, and what immediate actions should we take?"**

This question requires investigation across all 5 domains (demand, inventory, logistics, suppliers, executive KPIs). The Supervisor must call multiple Genie Agents, reconcile their findings, and produce a structured executive brief. After 5 iterations, the system achieves 100% accuracy on 10 ground truth KPIs.

### Prompt 2 — The Follow-Up That Breaks the System (used for iteration 6)

> **"What is our total Cost of Disruption by region last month — combining lost revenue from cancellations, at-risk backorder revenue, supplier SLA penalties, and wasted logistics spend on late shipments?"**

After reaching 100%, this new question **cannot be answered** because:
- "Cost of Disruption" is undefined in any schema, comment, synonym, or certified query
- It requires joining 3 schemas (`demand_analysis` + `logistics_operations` + `supplier_procurement`)
- No single Genie Agent has visibility across all three domains
- The Supervisor collects text answers — it cannot JOIN or SUM across agents

**Iteration 6** fixes this by creating a formal business definition (UC comments + tags), a cross-domain metric view, and a certified query — demonstrating that **data governance is the answer, not better prompts**.

## Architecture

```
Supervisor Agent ("Supply Chain Control Tower")
    ├─ Demand Analysis Agent        → Genie Agent (6 tables + 1 metric view)
    ├─ Inventory Management Agent   → Genie Agent (4 tables + 1 metric view)
    ├─ Logistics Operations Agent   → Genie Agent (4 tables + 1 metric view)
    ├─ Supplier Risk Agent          → Genie Agent (5 tables + 1 metric view)
    ├─ Executive Reporting Agent    → Genie Agent (4 views + 1 metric view)
    └─ Evaluator Agent              → Genie Agent (1 table: ground_truth_kpis)
```

## The Demo Story

A cascading supply chain failure:

1. **Asian suppliers** are 100% late (30/30 POs, +13.7 day average lead-time variance)
2. **Western inventory** collapses (33 stockouts, 109 SKU-warehouse positions below safety stock)
3. **Western logistics** breaks down (94.6% late delivery rate, 1,027 of 1,086 shipments late)
4. **Western revenue** drops ~$1.24M (-27.1%) as customers get backordered, partially fulfilled, or cancelled orders
5. **Company service level** falls to 70.4% — quarterly 95% target at CRITICAL RISK

The Supervisor Agent investigates all 5 domains and produces a structured executive brief with root cause analysis, cross-domain reconciliation, and a prioritized action plan.

---

## Quick Start

### Option A: One-Click Pipeline (recommended)

Run the master orchestrator notebook. It handles everything: teardown, data generation, agent creation, all 6 iterations, and verification.

| Notebook | Description |
|----------|-------------|
| `src/notebooks/00_run_all.py` | Runs teardown → create → generate → setup → all iterations → verify. Discovers the supervisor endpoint dynamically. |

Widgets:
- `catalog_name` (default: `GAP_Demo_Dev`)
- `warehouse_id` — set this to your SQL Warehouse ID
- `run_mode`: `full` (teardown + rebuild), `skip_teardown`, or `iterations_only`

The notebook runs `verify_ground_truth()` after each stage so you can see the accuracy progression live.

### Option B: Step-by-Step (for workshop presentation)

#### Prerequisites

- Databricks workspace with [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) enabled
- A running [SQL Warehouse](https://docs.databricks.com/en/compute/sql-warehouse/index.html) (note the warehouse ID from the SQL Warehouses page)
- Permission to create catalogs, schemas, and tables
- Each script takes a `catalog_name` widget (default: `GAP_Demo_Dev`)
- `08_setup_genie_supervisor.py` also takes a `warehouse_id` widget

#### Step 1: Build the Data Layer (run once)

Open each notebook in the Databricks UI and **Run All**, in order:

| # | Script | What It Does | Time |
|---|--------|-------------|------|
| 1 | `src/01_create_catalog_schemas.py` | Creates catalog + 5 schemas | ~10s |
| 2 | `src/02_generate_demand_data.py` | 6 tables: products, customers, sales_orders, forecasts, POS, promotions | ~30s |
| 3 | `src/03_generate_inventory_data.py` | 4 tables: warehouses, inventory_ledger, store_inventory, stock_movements | ~20s |
| 4 | `src/04_generate_logistics_data.py` | 4 tables: carriers, distribution_centers, shipments, transit_data | ~20s |
| 5 | `src/05_generate_supplier_data.py` | 5 tables: suppliers, supplier_orders, lead_times, SLAs, procurement | ~20s |
| 6 | `src/06_create_reporting_views.py` | 4 views: executive_kpis, regional_performance, revenue_trend, risk_scorecard | ~10s |
| 7 | `src/07_add_all_comments.py` | Adds table + column comments (180+ columns) for Genie accuracy | ~30s |

**Result**: 19 tables + 4 views across 5 schemas in `GAP_Demo_Dev`.

#### Step 2: Create the Baseline Agents

| # | Script | What It Does |
|---|--------|-------------|
| 8 | `src/08_setup_genie_supervisor.py` | Creates 5 domain Genie Agents + 1 Evaluator Agent + ground truth table + Supervisor Agent with 6 tools |

Set the `warehouse_id` widget to your SQL Warehouse ID before running.

This creates a **deliberately minimal** baseline:
- Genie Agents have tables but **no** certified queries, synonyms, or enhanced instructions
- Supervisor has basic instructions — no structured format, no specific question phrasings
- Expected accuracy: **~30%** (3 of 10 ground truth metrics match)

**Test it now** — go to the [Agents playground](https://docs.databricks.com/en/large-language-models/llm-serving-intro.html) and send **Prompt 1** (the main business question from above). The Supervisor will try but produce inconsistent, partially incorrect results.

#### Step 3: Baseline Assessment (optional)

| # | Script | What It Does |
|---|--------|-------------|
| - | `src/improvements/iteration_01_baseline_assessment.py` | Runs ground truth SQL, documents correct values, shows what the baseline gets wrong |

This is read-only — it doesn't change any agents. It establishes the scoreboard.

#### Step 4: Progressive Improvement (the core demo)

Run each iteration notebook **in order**. After each one, invoke the Supervisor with **Prompt 1** to see the improvement.

##### Iteration 2: Certified Queries → ~20% accuracy (WORSE)

| # | Script | What It Does |
|---|--------|-------------|
| - | `src/improvements/iteration_02_certified_queries.py` | Adds 20 certified SQL queries across 5 domain Genie Agents |

**What changed**: Each Genie Agent now has pre-built SQL patterns with correct columns and calendar-month time windows.

**Why it got WORSE**: Certified queries only fire when the question matches the pattern. The Supervisor still asks vague questions like "Tell me about Western revenue" which don't trigger the certified SQL. The certified patterns may even confuse Genie when non-matching questions arrive.

**Key insight**: Optimizing sub-agents without optimizing the orchestrator is counterproductive.

##### Iteration 3: Enhanced Instructions + Synonyms → ~50% accuracy

| # | Script | What It Does |
|---|--------|-------------|
| - | `src/improvements/iteration_03_column_synonyms.py` | Adds enhanced domain instructions + 107 column synonyms to all 5 Genie Agents |

**What changed**:
- Enhanced instructions tell each agent: "last month = calendar month boundaries (DATE_TRUNC)"
- Schema notes: "shipments has destination_region, NOT region"
- 107 column synonyms: "revenue" → total_amount, "late" → is_late, etc.
- Logistics synonyms: "region" → destination_region

**Why it jumped to 50%**: Agents now understand time periods and correct columns. But the Supervisor still phrases some questions poorly.

##### Iteration 4: Supervisor Hardening → ~90% accuracy (9/10)

| # | Script | What It Does |
|---|--------|-------------|
| - | `src/improvements/iteration_04_supervisor_hardening.py` | Updates Supervisor instructions (7-section mandatory format) + tool descriptions with exact question phrasings |

**What changed**:
- Mandatory 7-section output format (Findings, Reconciliation, Root Cause Chain, Scorecard, Actions)
- Tool descriptions now mandate EXACT questions: e.g., "Show revenue by region comparing last month to prior month"
- Evaluator agent called LAST to validate all findings against ground truth
- Structured action tables with owners, targets, and timelines

**Why 90% (not 100%)**: The Supervisor asks the right questions and 9 of 10 metrics match exactly. However, "Western below safety stock" still misses: the Genie Agent returns 61 (COUNT DISTINCT sku_id) while ground truth is 109 (COUNT of all SKU-warehouse positions). This is a **metric definition ambiguity** that certified queries and synonyms alone cannot resolve.

##### Iteration 5: Metric Views + Business Glossary → 100% accuracy

| # | Script | What It Does |
|---|--------|-------------|
| - | `src/improvements/iteration_05_metric_views_glossary.py` | Creates 4 metric views, adds them to Genie Agents with certified queries, UC tags, and column-level examples |

**What changed**:
- 4 **metric views** with unambiguous column names (e.g., `sku_warehouse_positions_below_safety_stock` = 109)
- **Column comments with format examples** (e.g., "Example value: 109")
- **Table comments with business definitions** ("Use sku_warehouse_positions, not unique_skus")
- **UC Tags** for governance (`domain=inventory`, `metric_type=safety_stock`, `data_quality=authoritative`)
- Certified queries that reference metric views instead of base tables
- Updated inventory agent instructions to prefer the metric view
- Updated supervisor tool description to ask about "SKU-warehouse positions"

**Why 100%**: The metric view eliminates the ambiguity entirely. The column name IS the metric definition — there's no way for the agent to misinterpret `sku_warehouse_positions_below_safety_stock`.

**Key insight**: When a metric has multiple valid interpretations (COUNT vs COUNT DISTINCT), the only reliable fix is a **pre-computed metric view** that encodes the exact business definition in the column name itself.

##### Iteration 6: Cost of Disruption → 100% accuracy (11/11)

| # | Script | What It Does |
|---|--------|-------------|
| - | `src/improvements/iteration_06_cost_of_disruption.py` | Creates cross-domain CoD view + UC tags + rich business definitions + certified query + 11th ground truth |

**Now test Prompt 2** (the Cost of Disruption follow-up question from above). Before iteration 6, the system fails completely. After iteration 6, it returns the correct cross-domain metric.

**Key insight**: Even a 100%-accurate system breaks when asked a question involving an undefined business concept. The fix is governance — formal business definitions, cross-domain metric views, and UC documentation — not better SQL or smarter instructions.

#### Step 5: Run the Visual Demo

| # | Script | What It Does |
|---|--------|-------------|
| - | `src/10_demo_runner.py` | Generates plotly charts from live data + invokes the Supervisor + scores against ground truth |

Or invoke the Supervisor directly from the [Agents playground](https://docs.databricks.com/en/large-language-models/llm-serving-intro.html) with **Prompt 1** or **Prompt 2** from above.

> **Note on visualizations**: Genie Agents produce interactive charts when used standalone in the Genie UI. When called as tools by a Supervisor Agent via API, they return structured data tables. The `10_demo_runner.py` generates plotly charts from the same underlying data to provide the visual layer.

#### Step 6: Teardown (when done)

| # | Script | What It Does |
|---|--------|-------------|
| 9 | `src/09_teardown.py` | Deletes all Genie Agents, Supervisor Agent, and drops the entire catalog |

---

## Progressive Improvement Summary

| Stage | Script | What Changed | Accuracy |
|-------|--------|-------------|----------|
| **Baseline** | `08_setup` | Minimal instructions, no CQs, no synonyms | **~30%** (3/10) |
| **+ Certified Queries** | `iteration_02` | 20 SQL patterns added to Genie Agents | **~20%** (2/10) — worse! |
| **+ Synonyms + Instructions** | `iteration_03` | 107 synonyms + calendar-month rules | **~50%** (5/10) |
| **+ Supervisor Hardening** | `iteration_04` | 7-section format + exact question phrasings | **~90%** (9/10) |
| **+ Metric Views + Glossary** | `iteration_05` | Pre-computed views with examples + UC tags | **100%** (10/10) |
| **+ Cost of Disruption** | `iteration_06` | Cross-domain view + UC tags + business definitions | **100%** (11/11) |

## Expected Output (after iteration 5)

The Supervisor produces a structured 7-section executive brief:

1. **What I Understood** — restates the business question
2. **Investigation Plan** — lists which agents to query and exact questions
3. **Findings by Agent** — for each of 5 agents: questions asked, SQL used, result summary, confidence
4. **Cross-Domain Reconciliation** — table comparing metrics across agents + ground truth
5. **Root Cause Chain** — numbered causal cascade: Supplier → Inventory → Logistics → Revenue → Service Level
6. **Ground Truth Comparison Scorecard** — 10-row table: Agent Finding vs Ground Truth vs Match
7. **Conclusion and Actions** — direct answer + immediate actions (table) + medium-term actions (table) + KPIs to monitor + risk assessment

## Ground Truth KPIs (10 + 1)

These are dynamically computed from the live data using calendar-month boundaries:

| Agent | Metric | Source |
|-------|--------|--------|
| demand-analysis | Western revenue change (MoM with dynamic month names) | sales_orders |
| inventory-management | Western stockout SKUs | inventory_ledger |
| inventory-management | Western below safety stock (COUNT of SKU-warehouse positions) | inventory_ledger |
| logistics-operations | Western late delivery rate (dynamic month name) | shipments (destination_region) |
| logistics-operations | Western avg delay days (dynamic month name) | shipments |
| supplier-risk | Asia supplier late rate (dynamic month name) | supplier_orders |
| supplier-risk | Asia avg lead time variance (dynamic month name) | supplier_orders |
| executive-reporting | Service level pct | executive_kpis view |
| executive-reporting | Supplier late pct (dynamic month name) | executive_kpis view |
| executive-reporting | Total stockout SKUs | executive_kpis view |
| executive-reporting | Western Cost of Disruption *(added by iteration 6)* | cost_of_disruption_by_region view |

> **Note**: All time-based metrics include actual month names (e.g., "Aug 2026 vs Jul 2026 MoM") generated dynamically via `DATE_FORMAT`. The demo works regardless of when it is run.

## File Structure

```
├── databricks.yml                               # DAB config (catalog, warehouse per env)
├── README.md                                    # This file
├── resources/
│   └── supply_chain_job.yml                     # Job definition (optional DAB deployment)
└── src/
    ├── 01_create_catalog_schemas.py              # Catalog + 5 schemas
    ├── 02_generate_demand_data.py                # 6 demand tables (~74K rows)
    ├── 03_generate_inventory_data.py             # 4 inventory tables (~15K rows)
    ├── 04_generate_logistics_data.py             # 4 logistics tables (~38K rows)
    ├── 05_generate_supplier_data.py              # 5 supplier tables (~1.7K rows)
    ├── 06_create_reporting_views.py              # 4 cross-domain views
    ├── 07_add_all_comments.py                   # 180+ column comments
    ├── 08_setup_genie_supervisor.py              # Raw baseline: 6 Genie Agents + Supervisor
    ├── 09_teardown.py                            # Full cleanup
    ├── 10_demo_runner.py                         # Charts + supervisor invocation + scoring
    ├── improvements/
    │   ├── iteration_01_baseline_assessment.py    # Ground truth + error documentation
    │   ├── iteration_02_certified_queries.py      # 20 certified SQL patterns
    │   ├── iteration_03_column_synonyms.py        # 107 synonyms + enhanced instructions
    │   ├── iteration_04_supervisor_hardening.py   # 7-section format + exact phrasings
    │   ├── iteration_05_metric_views_glossary.py  # Metric views + UC tags + examples
    │   └── iteration_06_cost_of_disruption.py     # Cross-domain CoD view + UC governance
    └── notebooks/
        ├── 00_run_all.py                          # One-click full pipeline orchestrator
        └── expected_output_reference.py           # Reference output for validation
```

## Data Determinism

All data generation scripts use a **fixed reference date** (`base_date = datetime(2026, 9, 1)`) and fixed random seeds (42/43/44/45), following the same approach as standard Databricks training demos. This produces **identical data every run**, regardless of when the demo is executed.

"Last month" = August 2026, "Prior month" = July 2026. All SQL views, certified queries, Genie Agent instructions, and ground truth use `DATE '2026-09-01'` instead of `CURRENT_DATE()`, so the demo is fully self-contained and never needs data regeneration.

---

## Configuration

Edit `databricks.yml` to set per-environment values:

- `catalog_name`: Unity Catalog name (default: `GAP_Demo_Dev`)
- `warehouse_id`: SQL Warehouse ID for Genie Agents

## Technical Notes

- **"Last month" = calendar month**: All SQL uses `DATE_TRUNC('month', ADD_MONTHS(CURRENT_DATE(), -1))` for last month and `-2` for prior month. Never rolling 30-day windows.
- **Genie API race condition**: After creating a Genie Agent, tables may not persist on the first PATCH. Scripts include retry logic.
- **Metric ambiguity**: When base tables have multiple rows per entity (e.g., one SKU in 3 warehouses), `COUNT(*)` and `COUNT(DISTINCT)` give different answers. Metric views resolve this by encoding the exact definition in the column name.
- **Tables must be sorted alphabetically** in `serialized_space.data_sources.tables`.
- **Supervisor requires a `description` field** or invocation fails.
- **Endpoint name format**: `mas-{short-uuid}-endpoint` (first 8 chars of the supervisor UUID).
- **Shipments table**: Has `destination_region` and `origin_region` but NO `region` column — this is the #1 trap.
- **Supplier data**: Organized by `supplier_continent` (Asia/Europe/North America), NOT by domestic region.

---

## Learn More

- **[Genie Agents](https://docs.databricks.com/en/genie/index.html)** — AI-powered data analysis agents that translate natural-language questions into SQL
- **[Supervisor Agents](https://docs.databricks.com/en/generative-ai/agent-framework/build-supervisor-agent.html)** — Multi-agent orchestrators that coordinate multiple sub-agents as tools
- **[Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)** — Unified governance for data and AI assets on Databricks
- **[SQL Warehouses](https://docs.databricks.com/en/compute/sql-warehouse/index.html)** — Serverless or provisioned compute for running SQL queries
- **[Databricks Agent Framework](https://docs.databricks.com/en/generative-ai/agent-framework/index.html)** — End-to-end tools for building, deploying, and monitoring AI agents
- **[Certified Queries](https://docs.databricks.com/en/genie/certified-queries.html)** — Pre-built SQL patterns that ground Genie Agent responses
