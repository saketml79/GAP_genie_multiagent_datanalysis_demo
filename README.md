# Supply Chain Control Tower — Multi-Agent Genie Workshop

## What This Demo Proves

Enterprise data warehouses and data marts contain the answers to complex business questions — but AI agents often **hallucinate**, **misinterpret column names**, or **generate incorrect SQL** when querying that data. This demo shows how to build AI-powered data analysis agents on Databricks that are **grounded to business truth** and **consistently generate correct queries** across a multi-domain supply chain data warehouse — without hallucination.

Using a realistic supply chain scenario with 23 tables across 5 business domains, this workshop:

- Builds a **Supervisor Agent** that orchestrates 5 domain-specific **Genie Agents** to answer complex cross-domain business questions
- Starts at **low accuracy** with a minimal baseline and progressively improves to **100% accuracy** across 3 iterations
- Demonstrates **why each improvement matters**: column comments + certified queries, UC metric views + governed tags + Open Knowledge views, and UC Pages + Domains
- Shows that the last mile of accuracy requires **data governance** (formal business definitions, Unity Catalog tags, metric views) — not just better prompts

### What Are Genie Agents and Supervisor Agents?

**[Genie Agents](https://docs.databricks.com/en/genie/index.html)** (formerly called Genie Spaces) are AI-powered data analysis agents in Databricks. You give them access to specific tables, and they translate natural-language questions into SQL, execute the query, and return results. They can be enhanced with certified queries, column synonyms, and instructions to improve accuracy.

**[Supervisor Agents](https://docs.databricks.com/en/generative-ai/agent-framework/build-supervisor-agent.html)** orchestrate multiple sub-agents (including Genie Agents) as tools. The Supervisor receives a complex question, decides which sub-agents to call and in what order, synthesizes their results, and produces a unified answer. This enables cross-domain analysis that no single agent could perform alone.

##### Ground Truth: Demo Proxy for Real-World Feedback

> In this demo, we use a **benchmark ground-truth framework** — **45 metrics** across 9 groups — to objectively measure whether the agents are generating the right SQL and returning the right numbers. Each metric is tested individually by sending a targeted question to the relevant domain agent and comparing the response against a known ground-truth value at exact 2-decimal precision.
>
> | Group | Count | What It Tests |
> | --- | --- | --- |
> | A: Logistics | 6 | OTD rate, late rate, avg delay, shipment counts, wasted freight |
> | B: Demand | 4 | Revenue (Aug vs Jul), dollar change, % change |
> | C: Inventory | 5 | Below safety stock, stockouts, days of supply |
> | D: Supplier | 7 | PO counts, late %, lead time variance (overall + by continent) |
> | E: Cross-domain | 3 | Fill rate, SLA penalties, Cost of Disruption |
> | F: Indirect / Ambiguity | 6 | Region synonyms, per-order vs per-vendor, status filters |
> | G: Q1 Fiscal | 2 | Q1 service-level target (not in any base table) |
> | H: Hard failures | 7 | Wrong table, cross-domain joins, derived ratios |
> | P: Critical Thresholds | 5 | Domain-specific "critical" definitions (each domain has a unique threshold) |
>
> At baseline, agent answers are **non-deterministic** — approximately 30-31 of 45 tests pass (~67-69%), but the exact count fluctuates across runs because the agent guesses from column/table names. After each improvement iteration, more answers become **deterministic** (grounded in UC features rather than guessing): Baseline (~30-31/45) → Iteration 1 (~35-37/45) → Iteration 2 (~38-40/45) → Iteration 3 (45/45, fully deterministic). A separate **comprehensive prompt benchmark** sends one broad executive question to the Supervisor Agent and scores how many of the 45 values appear in its unified report — testing whether UC improvements *indirectly* improve the Supervisor's coverage.
>
> **Important**: Genie Agents **cannot** access UC Pages (proven — the agent stated: "I don't have direct access to those pages in this context"). G01/G02 are fixed by the `fiscal_targets` TABLE. P01-P05 are fixed by **SQL Functions** that encode the same thresholds UC Pages define for humans.

> **In production, there is no ground truth table.** Instead, accuracy improves through an iterative **user feedback loop**:

1. A business user asks a question via a Genie Agent or Supervisor Agent
2. The agent generates SQL and returns a result
3. The user reviews the result and provides feedback: 👍 (correct) or 👎 (wrong)
4. A data team member reviews the feedback, identifies the root cause (wrong column, wrong filter, ambiguous metric), and applies a fix — exactly the same kinds of fixes shown in this demo (certified queries, synonyms, instructions, metric views)
5. Over time, the agent gets better and better at answering questions correctly

> **This demo compresses months of user-feedback-driven improvement into 3 scripted iterations**, so you can see the full journey in a single workshop session. Every fix we apply (comments, synonyms, certified queries, metric views, UC Pages) is the same fix a data team would apply in response to real user feedback.

---

## The Business Scenario

A **multi-regional retail company** sells 100 SKUs across 5 product families (Apparel, Accessories, Footwear, Home Goods, Electronics) through 4 US regions (Western, Eastern, Central, Southern). Products are sourced from 12 international suppliers across Asia, Europe, and North America, stored in 12 warehouses, and shipped via 8 carriers through 7 distribution centers.

The company's data warehouse is organized in [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) with 5 domain schemas — each owned by a different business team — plus a shared `reporting` schema for cross-domain executive views.

### Data Model (Final State)

The demo starts with **19 base tables + 4 reporting views**, then layers on **4 UC Metric Views**, **1 cross-domain governed executive view**, and the benchmark table used for evaluation. The diagram below shows the final semantic surface area exposed to Genie.

```mermaid
flowchart TD
    C["GAP_Demo_Dev catalog"]

    C --> D["demand_analysis schema"]
    C --> I["inventory_management schema"]
    C --> L["logistics_operations schema"]
    C --> S["supplier_procurement schema"]
    C --> R["reporting schema"]

    D --> D1["Base tables: products, customer_segments, sales_orders, demand_forecasts, pos_data, promotions"]
    D --> D2["UC Metric View: revenue_comparison_by_region"]

    I --> I1["Base tables: warehouse_data, inventory_ledger, store_inventory, stock_movements"]
    I --> I2["UC Metric View: inventory_safety_stock_metrics"]

    L --> L1["Base tables: carriers, distribution_centers, shipments, transit_data"]
    L --> L2["UC Metric View: delivery_performance_by_region"]

    S --> S1["Base tables: suppliers, supplier_orders, supplier_lead_times, vendor_slas, procurement_data"]
    S --> S2["UC Metric View: supplier_performance_by_continent"]

    R --> R1["Reporting views: executive_kpis, regional_performance_summary, revenue_trend, supply_chain_risk_scorecard"]
    R --> R2["Governed executive view: cost_of_disruption_by_region"]
    R --> R3["Benchmark table: ground_truth_kpis"]

    style D2 fill:#e8f4fd,stroke:#1f77b4
    style I2 fill:#e8f4fd,stroke:#1f77b4
    style L2 fill:#e8f4fd,stroke:#1f77b4
    style S2 fill:#e8f4fd,stroke:#1f77b4
    style R2 fill:#fff4e5,stroke:#f39c12
    style R3 fill:#f4e9ff,stroke:#7e57c2
```

**Why this diagram matters**: Genie gets SQL right only when each layer exposes a clear grain and a governed semantic object. Base tables answer raw questions. UC Metric Views answer certified KPI questions. UC Pages answer policy and definition questions that are intentionally not in SQL tables at all.

| Schema | Key Tables (bold = primary fact table) | Rows | Business Domain |
|--------|---------------------------------------|------|----------------|
| `demand_analysis` | products, customer_segments, **sales_orders**, demand_forecasts, pos_data, promotions | ~74K | Revenue, orders, demand forecasting |
| `inventory_management` | warehouse_data, **inventory_ledger**, store_inventory, stock_movements | ~15K | Stock levels, stockouts, safety stock |
| `logistics_operations` | carriers, distribution_centers, **shipments**, transit_data | ~38K | Delivery performance, delay tracking |
| `supplier_procurement` | suppliers, **supplier_orders**, supplier_lead_times, vendor_slas, procurement_data | ~1.7K | Supplier reliability, SLA penalties |
| `reporting` | executive_kpis, regional_performance_summary, revenue_trend, supply_chain_risk_scorecard, cost_of_disruption_by_region, ground_truth_kpis | Views | Cross-domain executive dashboards |

All data is generated with a **fixed reference date** of September 1, 2026 (`base_date = datetime(2026, 9, 1)`). "Last month" = August 2026, "Prior month" = July 2026. The demo produces identical results every run.

---

## The Single Canonical Prompt

A single comprehensive prompt covers all 10 primary ground truth metrics **and** every measure in every metric view across all 5 domains — including Cost of Disruption and Q1 targets:

> **"We need a complete supply chain health check for our West region in August 2026. The CFO wants to understand what drove the revenue decline versus July — show the actual August and July revenue numbers, the dollar change, and the percentage change — and which product families are most at fault. Are our on-time delivery rate and average delay for West region shipments contributing to the problem? How many total shipments went out and how many were late? I also need our current fill rate, how many inventory positions are sitting below safety stock in the West region, how many unique SKUs are affected, what is our days of supply for those at-risk items, and how many SKUs are completely stocked out. On the vendor side: what percentage of vendors delivered late in August, how many purchase orders were late out of total, what is the average lead time variance, and what are the total vendor SLA penalties we have incurred? Bring it all together as our total Cost of Disruption by region for August 2026 — cancelled revenue, at-risk backorder revenue, wasted freight on late shipments, and supplier penalty exposure in one number per region. Are we going to miss our Q1 service-level targets, and what are the top actions we should take?"**

This prompt is **deliberately designed** so that every measure in every UC Metric View is an expected metric the agent should return. It uses **business vocabulary** that does NOT match the database column names — "revenue" (not `total_amount`), "fill rate" (not `service_level_pct`), "vendor" (not `supplier`), "Cost of Disruption" (not in any table). Each iteration adds one UC Semantics feature to close the gap between business language and database reality.

**Prompt-to-measure coverage** (every metric view measure is explicitly asked):

| Prompt phrase | Metric view measure it expects | View |
| --- | --- | --- |
| "actual August and July revenue numbers" | `revenue_last_month`, `revenue_prior_month` | `revenue_comparison_by_region` |
| "the dollar change" | `revenue_change_dollars` | `revenue_comparison_by_region` |
| "the percentage change" | `revenue_change_pct` | `revenue_comparison_by_region` |
| "on-time delivery rate" | `on_time_delivery_rate` | `delivery_performance_by_region` |
| "average delay" | `avg_delay_days` | `delivery_performance_by_region` |
| "how many total shipments went out" | `total_shipments` | `delivery_performance_by_region` |
| "how many were late" | `late_shipments` | `delivery_performance_by_region` |
| "wasted freight on late shipments" | `wasted_freight_cost` | `delivery_performance_by_region` |
| "inventory positions below safety stock" | `positions_below_safety_stock` | `inventory_safety_stock_metrics` |
| "how many unique SKUs are affected" | `unique_skus_below_safety` | `inventory_safety_stock_metrics` |
| "days of supply for those at-risk items" | `avg_days_of_supply` | `inventory_safety_stock_metrics` |
| "SKUs are completely stocked out" | `unique_skus_in_stockout` | `inventory_safety_stock_metrics` |
| "percentage of vendors delivered late" | `supplier_late_rate_pct` | `supplier_performance_by_continent` |
| "purchase orders were late out of total" | `total_purchase_orders`, `late_purchase_orders` | `supplier_performance_by_continent` |
| "average lead time variance" | `avg_lead_time_variance` | `supplier_performance_by_continent` |
| "Cost of Disruption" (components) | `total_cost_of_disruption` + components | `cost_of_disruption_by_region` |
| "Q1 service-level targets" | 92.0% from UC Page | UC Page (not a metric view) |

If the Supervisor or any domain agent **cannot find** a metric the prompt asks for, it must explicitly state that the metric was NOT_FOUND or NOT_UNDERSTOOD in its response. This makes the ground-truth scorer’s job unambiguous: a missing number is always a miss, never a silent gap.

## The Genie Ontology and UC Semantics Stack

> **Source**: [Databricks UC Semantics documentation](https://docs.databricks.com/en/uc-semantics/index.html)

### Image 1: UC Semantics Architecture

![UC Semantics Architecture](docs/images/1.png)

The diagram above shows how the **Genie Ontology** sits between AI consumers (Genie, MCP/SQL/API, Dashboards) and the underlying data sources (Delta Lake, Iceberg, federated sources, business apps, workspace assets). The Ontology is a **continuously-learned enterprise context** layer — it gets smarter every time a user interacts with data.

Under the Ontology, **Unity Catalog Semantics** provides the **user-defined** semantic objects that feed the Ontology:

* **Domains**: Business-aligned organization of data assets. In this demo: a single "Supply Chain Operations" domain that groups all 5 schemas. Domains help Genie route questions to the right schema and help humans discover related assets on the Discover page.
* **Glossary** (UC Pages): Governed business concept definitions — policies, formulas, terminology — that Genie references authoritatively. In this demo: 7 Pages — "Fiscal Calendar & Targets" (Q1=Jul-Sep, target=92%), "Cross-Domain Metric Definitions" (CoD formula, OTD vs supplier late rate), and 5 domain-specific critical threshold definitions (P01-P05). **Important**: Genie One reads UC Pages; Genie Agents cannot. SQL Functions bridge the gap.
* **Metrics** (Metric Views): Reusable KPI definitions with `MEASURE()` syntax. In this demo: `delivery_performance_by_region`, `revenue_comparison_by_region`, `inventory_safety_stock_metrics`, `supplier_performance_by_continent`. Metric Views eliminate formula ambiguity by encoding the exact business definition in the column name itself.

The key insight: data alone is not enough. The UC Semantics layer teaches Genie **what the data means**, not just what it contains.

### Image 2: The Six Foundation Layers

![Six Foundation Layers](docs/images/2.png)

This diagram shows the six layers that make the Genie Ontology progressively more accurate. Each layer builds on the previous one. The feedback arrow on the right shows that evaluation at Layer 5 drives improvements back into every earlier layer.

| Layer | What It Does | This Demo’s Implementation |
| --- | --- | --- |
| **0. Get the data model right** | Model gold for agents (star schemas, clear grain). Resolve entities into golden records. | 23 base tables across 5 domain schemas with clear fact tables (`sales_orders`, `shipments`, `inventory_ledger`, `supplier_orders`) and dimension tables. Fixed monthly grain. |
| **1. Enrich metadata** | Table and column descriptions. Governed tags and classification. `dbxmetagen` for scale. | **Iter 1**: 180+ column/table comments via `ALTER TABLE SET COMMENT`. **Iter 2**: governed tags (`domain`, `certification_status`). `dbxmetagen` mentioned as scale path but excluded from workshop. |
| **2. Model business semantics** | Metric Views for certified KPIs. Declared relationships (PK/FK). Domains. Pages. | **Iter 2**: 4 UC Metric Views + cross-domain Open Knowledge view. **Iter 3** (manual): UC Domain + UC Pages. PK/FK relationships documented for future implementation. |
| **3. Curate context-rich assets** | Certify trusted assets. Classify and quality-check. Instructions, definitions, examples. | **Iter 1**: Certified SQL examples in agent instructions + column/table comments. **Iter 2**: Governed tags on all metric views and schemas. |
| **4. Govern access** | UC access controls. Row-level security and column masking. ABAC. Unity AI Gateway. | Standard catalog/schema grants. Row-level security, column masking, and ABAC documented as extension opportunities. |
| **5. Evaluate & improve** | Benchmark answers. Observe and trace. Capture feedback. Watch for drift. | `ground_truth_kpis` benchmark table (10 primary + 14 indirect metrics). Python scorer + Evaluator Genie Agent. Verification after every iteration shows progressive improvement. |

The practical rule: **build one domain at a time, one layer at a time.** This workshop compresses the full journey into 3 scripted iterations so you can see the progression in a single session.

> **Note on images**: The two reference images above are from [Databricks UC Semantics documentation](https://docs.databricks.com/en/uc-semantics/index.html).

### Why Ontology Snippets Are a Massive Token Cost Reduction

A critical but often overlooked advantage of the UC Semantics approach is **token economics**. Consider the two fundamental architectures for giving an AI agent business context:

#### Architecture A — Runtime Context Injection (Naïve)

Every time an agent receives a question, stuff the full business glossary, metric definitions, policy thresholds, and domain context into the prompt. We measured the actual token counts from this demo's content using `tiktoken` (cl100k_base tokenizer):

| Component | Tokens | Source |
| --- | --- | --- |
| UC Page definitions (7 pages) | 828 | Cell 18 definitions: fiscal calendar, cross-domain metrics, 5 policy thresholds |
| Column comments (180 across 5 schemas) | ~5,015 | Iteration 1: disambiguation rules, table warnings, valid values |
| Table comments (13 key tables) | 348 | Fact table descriptions, metric view descriptions, warning labels |
| SQL Function definitions (5 functions) | 184 | P01–P05 threshold rules with conditions and source tables |
| Agent instructions (5 agents) | 911 | Domain routing, table access, date references, region mapping |
| **Total context per call** | **~7,286** | Measured from this demo's actual UC Semantics content |

At 500 agent calls/day, that's **3.64M tokens/day** just for context. With a Supervisor Agent calling 5 sub-agents per executive question, each sub-agent independently consumes the full 7,286 tokens: **36,430 tokens per Supervisor question** on context alone.

#### Architecture B — UC Semantics + Ontology Snippets (This Demo)

Genie and Genie Agents consume UC Semantics metadata **once during ontology indexing** — column comments, table descriptions, UC Page definitions, metric view schemas, governed tags, and synonyms are all pre-processed into the **Genie Ontology**. At query time, the ontology performs **selective retrieval**: only the relevant snippet (the matching definition + table schema) is injected into the LLM context. The full glossary is never sent.

Measured snippet sizes from this demo:

| Question type | Snippet content | Tokens |
| --- | --- | --- |
| P01: "Flagged under Logistics Risk Standards" | Threshold definition + shipments schema | 64 |
| Metric: "On-time delivery rate" | `delivery_performance_by_region` view schema | 33 |
| Revenue: "August revenue for Western region" | `revenue_comparison_by_region` view schema | 26 |
| **Realistic estimate per call** (definition + schema + column context) | | **~100–200** |

The raw snippet averages ~41 tokens, but including table schema context, the realistic per-call retrieval is **~100–200 tokens**.

#### Measured Reduction

| Metric | Architecture A | Architecture B | Reduction |
| --- | --- | --- | --- |
| Tokens per call | 7,286 | ~100–200 | **36–73×** |
| Daily (500 calls) | 3,643,000 | 50,000–100,000 | **36–73×** |
| Per Supervisor question (5 agents) | 36,430 | 500–1,000 | **36–73×** |

These are **measured from this demo's actual content** — not estimates. A real enterprise with hundreds of tables, thousands of columns, and dozens of policy documents would see an even larger gap.

#### How This Works in Practice (This Demo's Example)

* **Without UC Semantics**: To answer *"How many shipments were flagged under Logistics Risk Standards?"*, the agent needs all 7 Page definitions (828 tokens), all 180 column comments (~5,015 tokens), and all table descriptions (348 tokens) injected into the prompt — because the system can't know in advance which definition or column is relevant. Total: **~7,286 tokens** of context per call.
* **With UC Semantics**: The ontology matches the question to the `Logistics Risk Standards` snippet and the `shipments` table schema. Only **64 tokens** reach the LLM. The fiscal calendar, supplier quality thresholds, inventory risk definitions, and 175 irrelevant column comments are never loaded.

#### The Compounding Effect for Supervisor Agents

A Supervisor Agent that orchestrates 5 sub-agents multiplies the savings. Under Architecture A, each sub-agent call carries the full context (5 × 7,286 = **36,430 tokens**). Under Architecture B, each sub-agent receives only its domain-relevant snippet (5 × 150 = **750 tokens**). The Supervisor's own orchestration prompt stays lean because it delegates domain knowledge to the ontology rather than carrying it in instructions.

#### The Governance–Economics Connection

UC Semantics doesn't just improve accuracy — it fundamentally changes the cost structure of AI agents at scale. The semantic layer is consumed once, indexed once, and retrieved selectively. This is why **enterprise governance and enterprise token economics are the same problem**: the more your metadata is structured and pre-indexed, the less you pay per query. Organizations that invest in UC Semantics (column comments, metric views, UC Pages, governed tags) get two returns on the same investment:

1. **Accuracy**: Agents produce correct, deterministic answers grounded in governed definitions
2. **Cost**: Each query consumes only the relevant semantic context, not the entire knowledge base

The demo's progression from 28/45 (62%) to 45/45 (100%) isn't just an accuracy story — it's also a token efficiency story. Every UC feature we add is a snippet the ontology can selectively retrieve, replacing the need to inject the full context document on every call.

### UC Semantic Features Actually Used in This Demo

The table below lists every UC semantic layer feature this demo uses to fix Genie agent accuracy, grouped by iteration. This is the complete set — no other mechanisms (prompt hacks, fine-tuning, custom models) are involved. Every fix is a standard Unity Catalog or Genie API capability.

#### Iteration 1 — Metadata Enrichment (Baseline ~67% → ~78%)

| # | Feature | API / SQL | What It Does in This Demo | Tests Fixed |
| --- | --- | --- | --- | --- |
| 1 | **Column Comments** | `ALTER TABLE ... ALTER COLUMN ... COMMENT '...'` | Disambiguation text on individual columns. Tells agent which table to use for lead time variance, defines exact order status values, clarifies vendor late % is per-order. | D04, D06, H01, H02, F03, H03 |
| 2 | **Table Comments** | `ALTER TABLE ... SET TBLPROPERTIES ('comment' = '...')` | Table-level warnings. Marks `supplier_lead_times` as a pre-aggregated summary that should NOT be used for variance calculations. | D04, D06, H01, H02 |
| 3 | **Example SQL Queries** | `example_question_sqls` in Genie `serialized_space.instructions` (REST API PATCH) | Certified SQL patterns that appear in the Genie Agent **Examples tab**. Teaches the agent the exact SQL shape for lead time variance, vendor late %, fulfilled order count, and product family decline. 6 examples across supplier + demand agents. | A04, D04, D06, F02, F03, H01, H02, H03 |
| 4 | **Benchmark Questions** | `benchmarks.questions` in Genie `serialized_space` (REST API PATCH) | Ground-truth Q&A pairs that appear in the Genie Agent **Benchmarks tab**. Used for evaluation — each benchmark has a SQL answer that Genie compares result sets against during benchmark runs. 7 benchmarks across supplier + demand + logistics agents. | (evaluation, not direct fix) |

#### Iteration 2 — Business Semantics (~78% → ~87%)

| # | Feature | API / SQL | What It Does in This Demo | Tests Fixed |
| --- | --- | --- | --- | --- |
| 5 | **UC Metric Views** | `CREATE VIEW ... WITH METRICS LANGUAGE YAML` | Declarative dimension + measure definitions. 4 Metric Views define the exact business formula for every KPI the agent needs: `delivery_performance_by_region` (6 measures), `revenue_comparison_by_region` (4 measures), `inventory_safety_stock_metrics` (5 measures), `supplier_performance_by_continent` (4 measures). Agent queries the view directly instead of inferring SQL. | Stabilizes A01-A06, B01-B04, C01-C05, D01-D07 |
| 6 | **Open Knowledge View** | `CREATE VIEW ... AS` (cross-domain governed join) | A governed view that joins data from 4 domain schemas no single agent can see. `cost_of_disruption_by_region` combines cancelled revenue (demand), wasted freight (logistics), SLA penalties (supplier), and stockout counts (inventory) into one queryable table. | E03, H05, H06, H07 |
| 7 | **UC Governed Tags** | `ALTER TABLE SET TAGS ('key' = 'value')` | Classification tags on all metric views and the CoD view: `domain`, `metric_type`, `data_quality`, `time_granularity`. Helps Genie prefer the certified, authoritative asset over a plausible but wrong table. | (ranking / preference) |
| 8 | **Schema Domain Tags** | `ALTER SCHEMA SET TAGS ('domain' = '...', 'business_unit' = '...')` | Schema-level business domain classification. All 5 domain schemas tagged with their business function. Routes questions to the right schema and signals organizational ownership. | (routing) |

#### Iteration 3 — Governance Layer + SQL Functions (~87% → 100%)

| # | Feature | API / SQL | What It Does in This Demo | Tests Fixed |
| --- | --- | --- | --- | --- |
| 9 | **UC Domain** | Created on the **Discover page** (UI) | Business-aligned organization of data assets. A single "Supply Chain Operations" domain groups all 5 schemas. Domains help Genie route questions to the right schema and help humans discover related assets. Persists through teardown — it IS the governance layer. | (routing + discovery) |
| 10 | **UC Pages (Glossary)** | Created on the **Discover page** (UI), attached to the Domain | Governed business concept definitions for **humans and Genie One** — policies, formulas, terminology. 7 Pages: Fiscal Calendar & Targets, Cross-Domain Metric Definitions, and 5 domain-specific critical threshold definitions (Logistics Risk Standards, Demand Quality Standards, Inventory Risk Classification, Supplier Quality Standards, Executive Alert Thresholds). **Important**: Genie Agents cannot access UC Pages (proven — agent stated it has no access). Genie One CAN read them (confirmed with citation). Pages serve as governance documentation; agent-facing thresholds are delivered via SQL Functions (#12) and reference tables (#11). | (human governance + Genie One) |
| 11 | **Reference Table** | `CREATE TABLE ... fiscal_targets` + added to Executive agent data sources | Queryable data encoding fiscal calendar and service-level targets. The agent queries this table directly for Q1 target and fiscal quarter definitions. | G01, G02 |
| 12 | **SQL Functions** (5) | `CREATE FUNCTION get_critical_*()` + added to agent data sources via UI | Table-valued functions encoding domain-specific multi-condition "critical" thresholds: `get_critical_delay_shipments()` (delay\_days >= 5 AND total\_weight\_kg > 800 → 176), `get_critical_accuracy_forecasts()` (quantity >= 8 AND unit\_price < 30 AND channel='Online' → 77), `get_critical_supply_positions()` (days\_of\_supply BETWEEN 1 AND 11 AND below\_safety\_stock\_flag=true AND on\_hand\_qty > 0 → 106), `get_critical_quality_orders()` (quality\_score < 75 AND lead\_time\_variance\_days > 12 → 11), `get_critical_disruption_regions()` (composite\_risk\_score < 55 AND lead\_time\_variance > 8 AND total\_penalty\_usd > 80000 → 3). Each function is designed to be **unguessable** — no single condition produces the correct count. SQL Functions work via both Agent UI and Agent Mode API once added as a data source. | P01, P02, P03, P04, P05 |

#### Supporting Features (used throughout)

| # | Feature | API / SQL | What It Does in This Demo |
| --- | --- | --- | --- |
| 13 | **Genie Agent Instructions** | `text_instructions` in `serialized_space` (REST API PATCH) | Natural language guidance for each agent: table descriptions, key columns, disambiguation rules, date references, out-of-scope handling. All 5 agents have multi-paragraph instructions. |
| 14 | **Genie Agent Data Sources** | `data_sources.tables` in `serialized_space` (REST API PATCH) | Explicit table access lists per agent. Tables sorted alphabetically (API requirement). Views, reference tables, and SQL functions added incrementally in each iteration. Controls what each agent can see and query. |

> **Key insight**: Features 1-8 are the **repeatable, scriptable** part of the semantic layer — they can be applied via SQL and API in CI/CD. Features 9-10 are the **governance** part — they require human judgment about business definitions, and they are created in the Discover page UI. Features 11-12 bridge the two: scriptable tables and SQL functions that encode governance decisions so agents can access them (since agents cannot read UC Pages directly). This split mirrors how real teams work: data engineers script the metadata enrichment; data stewards define the business glossary; SQL functions make those definitions agent-accessible.

---

## How Genie Gets the SQL Right

Genie does **not** become accurate because of one magic prompt. It becomes accurate because each semantic layer removes one specific failure mode in SQL generation.

| What Genie needs to do | What fixes it | Why it matters |
| --- | --- | --- |
| Map business words to physical columns | **Synonyms** + comments | "revenue" must resolve to `total_amount`; "vendor" must resolve to supplier assets |
| Pick the right table and grain | **Comments** + governed tags + domains | Helps Genie choose shipments vs sales vs supplier orders, and avoid row-grain mistakes |
| Write the right formula | **Metric Views** + certified SQL examples | Derived KPIs like MoM revenue change, OTD, and CoD should not be recomputed ad hoc |
| Join along the right business path | **Declared relationships** + trusted views | Prevents bad joins across domains and helps route to the correct asset |
| Distinguish policy from data | **UC Pages** | Fiscal targets are business policy, not table data |
| Prefer the trusted asset | **Certification** + examples + instructions | Steers ranking toward the vetted source instead of a plausible but wrong table |
| Keep improving over time | **Benchmark scoring, traces, feedback** | Catches regressions when the model or ontology changes |

For this demo specifically:

* **Synonyms** fix vocabulary mapping.
* **Comments** fix ambiguous columns and row grain.
* **Metric Views** fix derived KPI SQL.
* **UC Pages** fix non-SQL business facts.
* **Certified queries, examples, and supervisor instructions** fix routing and exact SQL shape.
* **Certification, governed tags, and domains** make Genie prefer the right governed asset.
* **The scorer and evaluator** prove whether the SQL grounding actually improved.

## Architecture

```
Supervisor Agent ("Supply Chain Control Tower")
    ├─ Demand Analysis Agent        → Genie Agent (6 tables + 1 metric view)
    ├─ Inventory Management Agent   → Genie Agent (4 tables + 1 metric view)
    ├─ Logistics Operations Agent   → Genie Agent (4 tables + 1 metric view)
    ├─ Supplier Risk Agent          → Genie Agent (5 tables + 1 metric view)
    └─ Executive Reporting Agent    → Genie Agent (4 views + 1 metric view)

Evaluator Agent (external, NOT a supervisor tool)
    └─ Genie Agent (1 table: ground_truth_kpis) — called by Python scorer only
```

### How the Supervisor Orchestrates a Query

When a user asks a business question, the Supervisor Agent breaks it down, routes sub-questions to domain-specific Genie Agents, collects their SQL-grounded answers, and synthesizes a unified executive brief.

**Nothing is hardcoded.** The Supervisor is an LLM with tool-calling capability. Each Genie Agent is registered as a "tool" on the Supervisor endpoint. The LLM autonomously decides:

1. **Which agents to call** — it reads the tool descriptions and picks the relevant domain agents
2. **What to ask each agent** — it generates the sub-query text dynamically (the exact wording varies run to run)
3. **How to synthesize** — it combines the responses into a unified report

This means there are **two layers of LLM non-determinism**: the Supervisor's phrasing of the sub-query, and the Genie Agent's SQL generation from that sub-query. For example, the Supervisor might ask the logistics agent "Show late delivery rate for Western region" in one run and "What is the percentage of late shipments to Western in August 2026?" in the next — both valid, but the Genie Agent may generate different SQL for each.

The only things we control are:

* **Supervisor instructions** — the system prompt (~3000 chars) defining output format, routing rules, and behavior
* **Tool descriptions** — short descriptions of each Genie Agent that help the Supervisor decide which one to call
* **UC semantic features** — comments, metric views, example SQL, UC Pages that help the Genie Agent write correct SQL regardless of how the sub-query is phrased

This is exactly why the semantic layer matters: **we cannot control what the Supervisor asks, but we CAN ensure that however the Genie Agent interprets it, the governed asset steers it to the correct SQL path.**

The sequence diagram below shows one representative orchestration flow (actual sub-queries vary per run):

```mermaid
sequenceDiagram
    actor User
    participant S as Supervisor Agent
    participant DA as Demand Analysis
    participant IM as Inventory Mgmt
    participant LO as Logistics Ops
    participant SR as Supplier Risk
    participant ER as Executive Reporting
    User->>S: "Complete supply chain health check<br/>for West region in August 2026:<br/>revenue decline, OTD, fill rate,<br/>stockouts, vendor penalties, CoD,<br/>Q1 targets, and actions"

    Note over S: Decomposes question into<br/>domain-specific sub-queries

    S->>DA: Show revenue by region<br/>comparing Aug 2026 to Jul 2026
    DA-->>S: Western revenue dropped ~$1.24M (−27.1%)

    S->>IM: Show Western region stockouts<br/>and SKU-warehouse positions below safety stock
    IM-->>S: 33 stockout SKUs, 109 positions below safety stock

    S->>LO: Show late delivery rate<br/>for Western region in Aug 2026
    LO-->>S: 94.6% late rate, avg 3.2 day delay

    S->>SR: Show Asia supplier on-time rate<br/>and lead time variance in Aug 2026
    SR-->>S: 100% late (30/30 POs), +13.7 day variance

    S->>ER: Show executive KPIs<br/>including service level
    ER-->>S: Service level 70.4%, quarterly 92% target at risk

    Note over S: Cross-domain reconciliation:<br/>Supplier delays → Inventory gaps →<br/>Logistics failures → Revenue loss

    S-->>User: 7-Section Executive Brief<br/>Root Cause Chain · Ground Truth Scorecard · Action Plan
```

### How Genie One Answers the Same Question

Genie One is the standalone Databricks chat experience. Unlike the Supervisor (which calls domain agents as tools), Genie One has direct access to the **Genie Ontology** — the continuously-learned enterprise context layer that indexes UC Pages, knowledge snippets, metric view schemas, column comments, governed tags, and table metadata.

When a user asks the same CFO prompt in Genie One, the system retrieves relevant ontology snippets, routes internally to the domain Genie Agents, queries Delta tables directly, and cites UC Pages and knowledge snippets in its response. The screenshot below shows the "12 sources" and "Genie Ontology (9)" badges from a live Genie One response to the CFO prompt.

```mermaid
flowchart TB
    User(["User: CFO supply chain<br/>health check prompt"])

    subgraph GenieOne ["Genie One (standalone chat)"]
        direction TB
        Orchestrator["Genie One<br/>Orchestrator"]
    end

    subgraph Ontology ["Genie Ontology (continuously learned)"]
        direction TB
        Pages["UC Pages (7)\nFiscal Calendar & Targets\nCross-Domain Metrics\nLogistics Risk Standards\nDemand Quality Standards\nInventory Risk Classification\nSupplier Quality Standards\nExecutive Alert Thresholds"]
        Snippets["Knowledge Snippets\nBelow-safety-stock vs zero-on-hand\nCoD vs revenue at risk\nCoD view is the only authoritative cross-domain source\nCoD, revenue at risk, and disruption-to-revenue ratio"]
        MVSchemas["Metric View Schemas\ndelivery_performance_by_region\nrevenue_comparison_by_region\ninventory_safety_stock_metrics\nsupplier_performance_by_continent"]
        ColMeta["Column Comments + Tags\n180+ column descriptions\nGoverned tags (domain, certification)\nSchema domain tags"]
    end

    subgraph Agents ["Domain Genie Agents (routed internally)"]
        direction LR
        DA2["Demand\nAnalysis"]
        IM2["Inventory\nMgmt"]
        LO2["Logistics\nOps"]
        SR2["Supplier\nRisk"]
        ER2["Executive\nReporting"]
    end

    subgraph Data ["Delta Tables (Unity Catalog)"]
        direction LR
        DT["23 base tables\n4 metric views\n1 cross-domain view\n1 reference table\n5 SQL functions"]
    end

    User --> Orchestrator
    Orchestrator -- "retrieves relevant\nsnippets & definitions" --> Ontology
    Orchestrator -- "routes domain\nquestions" --> Agents
    Agents -- "SQL queries" --> Data
    Data -- "results" --> Agents
    Agents -- "answers + SQL" --> Orchestrator
    Ontology -- "cited in response as\nUC Page links &\nknowledge snippets" --> Orchestrator
    Orchestrator -- "Rich report: cards,\nvisualizations, links,\ncitations, 12 sources" --> User

    style GenieOne fill:#f0f4ff,stroke:#4a90d9
    style Ontology fill:#fff8e1,stroke:#f5a623
    style Agents fill:#e8f5e9,stroke:#4caf50
    style Data fill:#fce4ec,stroke:#e91e63
```

**Key difference from the Supervisor architecture:**

| Capability | Supervisor Agent | Genie One |
|---|---|---|
| Orchestration | Explicit tool calls to 5 agents + 5 SQL functions | Internal routing via Genie Ontology |
| UC Pages | Cannot access | Reads, interprets, and **cites with links** |
| Knowledge Snippets | Not available | Retrieved and cited (9 snippets in live run) |
| SQL Functions | Calls as registered tools, gets structured definitions | Not available as tools |
| Output format | Text + tables | Cards, charts, embedded query links, UC Page links |
| Source attribution | Lists agents queried | Shows "12 sources" badge + "Genie Ontology (9)" |

Genie One's access to UC Pages and knowledge snippets is what allows it to correctly apply multi-condition policy thresholds (P01-P05) without SQL Functions — it reads the page prose directly. The Supervisor cannot read UC Pages, so it relies on SQL Functions to bridge that gap.

## The Demo Story

A cascading supply chain failure:

1. **Asian suppliers** are 100% late (30/30 POs, +13.7 day average lead-time variance)
2. **Western inventory** collapses (33 stockouts, 109 SKU-warehouse positions below safety stock)
3. **Western logistics** breaks down (94.6% late delivery rate, 1,027 of 1,086 shipments late)
4. **Western revenue** drops ~$1.24M (-27.1%) as customers get backordered, partially fulfilled, or cancelled orders
5. **Company service level** falls to 70.4% — quarterly 92% target at CRITICAL RISK

The Supervisor Agent investigates all 5 domains and produces a structured executive brief with root cause analysis, cross-domain reconciliation, and a prioritized action plan.

---

## Baseline Test Results (45-Test Curator Benchmarks)

Before any UC Semantics features are applied, the 5 Genie Agents are tested with **45 individual questions** using **exact 2-decimal precision matching** (`round(abs(found), 2) == round(abs(expected), 2)`). No tolerance bands — either it matches or it doesn't.

**Baseline score: ~30-31/45 PASS (~67-69%), non-deterministic**

> Baseline scores fluctuate across runs. The agent guesses from column/table names — those guesses are probabilistic. Some tests pass one run and fail the next.
>
> "PASS" means the agent answers correctly at baseline WITHOUT any UC feature (but may not be reliable).
> "FAIL" means the agent genuinely needs the UC feature to answer correctly.

| Group | Tests | Typical Pass | Typical Fail | Coverage |
| --- | --- | --- | --- | --- |
| A: Logistics | 6 | 4-5 | 1-2 | A02/A03 flaky (non-deterministic); A04 fails: date ambiguity |
| B: Demand | 4 | 3-4 | 0-1 | B03 occasionally flaky |
| C: Inventory | 5 | 4-5 | 0-1 | C05 occasionally flaky |
| D: Supplier | 7 | 5-6 | 1-2 | Wrong table (supplier_lead_times vs supplier_orders) |
| E: Cross-domain | 3 | 2 | 1 | CoD requires cross-domain Open Knowledge view |
| F: Indirect/Ambiguity | 6 | 4 | 2 | Per-vendor vs per-order ambiguity, status filter |
| G: Q1 Fiscal | 2 | 0 | 2 | Target not in any table |
| H: Hard failures | 7 | 1 | 6 | Cross-domain queries impossible for single agent |
| P: Critical Thresholds | 5 | 2-3 | 2-3 | Domain-specific "critical" definitions — agent guesses thresholds |

### Structural Failures and Their Fix Plan (3 Iterations)

These are the tests that consistently fail at baseline. Additional tests may fail intermittently due to non-determinism (A02, A03, B03, C05, F06).

| ID | Metric | GT | Failure Pattern | Fix Iteration |
| --- | --- | --- | --- | --- |
| D04 | Avg lead time variance (overall) | 8.69 | Wrong table: agent queries supplier_lead_times instead of supplier_orders | **Iter 1** |
| D06 | Avg lead time variance (Asia) | 13.67 | Same wrong-table issue | **Iter 1** |
| F02 | Vendor late % (per-order) | 75.00 | Per-vendor (83.33%) vs per-order (75%) ambiguity | **Iter 1** |
| F03 | Fulfilled order count | 1342 | Includes Partially_Fulfilled (1512) | **Iter 1** |
| H01-H02 | Lead time variance (Europe/NA) | varies | Wrong table (same as D04) | **Iter 1** |
| H03 | Order fulfillment rate | 71.23 | Includes Partially_Fulfilled (80.25%) | **Iter 1** |
| E03 | Cost of Disruption (Western) | 3757298.31 | Cross-domain: no single agent has demand + logistics + supplier data | **Iter 2** |
| H05 | Revenue at risk from disruptions | 3138569.66 | Cross-domain: demand + logistics | **Iter 2** |
| H06 | Revenue at risk per stockout SKU | 14368.63 | Cross-domain: inventory + demand | **Iter 2** |
| H07 | Disruption cost / revenue ratio | 1.12 | Cross-domain: CoD / revenue | **Iter 2** |
| G01 | Q1 service-level target (miss?) | 92.0 | Target not in any table; Q1 = fiscal Jul-Sep | **Iter 3** |
| G02 | Q1 service-level target value | 92.0 | Target not in any table | **Iter 3** |

### Iteration Plan

| Iteration | UC Feature Class | What It Does | Targets | Expected Outcome |
| --- | --- | --- | --- | --- |
| **1. Column Comments + Example SQL + Benchmarks** | Enterprise Context (Layer 1) | Table/column comments for disambiguation + Example SQL Queries (via Genie Examples tab) + Benchmark questions | A04, D04, D06, F02, F03, H01, H02, H03 | ~35-37/45 → fixes wrong-table, status ambiguity, date inference |
| **2. UC Metric Views + Governed Tags + Open Knowledge** | Business Semantics (Layer 2) | 4 domain metric views (YAML), schema domain tags, 1 cross-domain Open Knowledge view (CoD) | E03, H05, H06, H07 | ~38-40/45 → fixes cross-domain queries, stabilizes more |
| **3. fiscal_targets + SQL Functions + UC Pages** | Governance (Layer 2+3) | `fiscal_targets` table, 5 SQL Functions for critical thresholds, UC Domain + Pages (UI) | G01, G02, P01-P05 | **45/45 (fully deterministic)** |

### Key Insight

Genie Agents are remarkably capable at baseline — they correctly map business terms to column names ("revenue" → `total_amount`, "fill rate" → `service_level_pct`) and handle region inference ("West" → `ILIKE '%Western%'`) without any synonyms, comments, or instructions. But these baseline answers are **non-deterministic** — the same question can produce different SQL across runs. The progression from ~67% non-deterministic to 100% deterministic proves that UC Semantic features don't just improve accuracy — they make answers **reliable and reproducible**.

---

## Quick Start

### Option A: One-Click Pipeline (recommended)

Run the master orchestrator notebook. It handles everything: teardown, data generation, agent creation, all iterations, and verification.

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

> **Note:** `07_add_all_comments.py` is deliberately **not** run at this stage. At baseline, tables have bare column names with no descriptions. Column and table comments are added inline during **Iteration 1** (via `ALTER COLUMN COMMENT` / `SET TBLPROPERTIES`) to demonstrate their impact on Genie accuracy.

**Result**: 19 tables + 4 views across 5 schemas in `GAP_Demo_Dev`.

#### Step 2: Create the Baseline Agents

| # | Script | What It Does |
|---|--------|-------------|
| 8 | `src/08_setup_genie_supervisor.py` | Creates 5 domain Genie Agents + Evaluator (external scorer) + ground truth table + Supervisor Agent with 5 domain tools |

Set the `warehouse_id` widget to your SQL Warehouse ID before running.

This creates a **deliberately minimal** baseline:
- Genie Agents have tables but **no** certified queries, synonyms, or enhanced instructions
- Supervisor has basic instructions — no structured format, no specific question phrasings
- Expected accuracy: **~67-69%** (~30-31 of 45 metric tests pass at baseline, non-deterministic)

**Test it now** — go to the Agents playground and send the canonical prompt. The Supervisor will try but produce inconsistent, partially incorrect results.

#### Step 3: Progressive Improvement (the core demo)

All 3 improvement iterations run **inline** in the Curator Workbench notebook (cells 10, 13, 19). Run the full notebook end-to-end — do NOT run iteration cells in isolation (they depend on the baseline agents created in earlier cells).

After each iteration cell, the notebook automatically runs `test_failing_metrics()` to show progress.

##### Iteration 1: Column Comments + Example SQL Queries + Benchmarks (cell 10)

**UC Features**: `ALTER TABLE SET COMMENT`, Example SQL Queries (`example_question_sqls` API → Genie Examples tab), Benchmarks (`benchmarks.questions` API → Genie Benchmarks tab)

**What it fixes**: Table/column comments resolve table disambiguation (agent picks `supplier_orders` instead of `supplier_lead_times` for lead time variance). Example SQL Queries teach Genie correct SQL patterns for common questions via the structured Examples tab (stronger than embedding SQL in text instructions). Benchmark questions provide ground-truth Q&A pairs for evaluating accuracy via the Benchmarks tab. Fixes status-filter ambiguity ("Fulfilled" excludes "Partially_Fulfilled") and per-order vs per-vendor aggregation.

**Targets**: A04, D04, D06, F02, F03, H01, H02, H03 → **~35-37/45 (~78%)**

##### Iteration 2: UC Metric Views + Governed Tags + Open Knowledge View (cell 13)

**UC Features**: `CREATE VIEW WITH METRICS LANGUAGE YAML`, `ALTER TABLE SET TAGS`, `ALTER SCHEMA SET TAGS`

**What it fixes**: 4 UC Metric Views (`delivery_performance_by_region`, `revenue_comparison_by_region`, `inventory_safety_stock_metrics`, `supplier_performance_by_continent`) encode exact KPI formulas in governed column names. 1 Open Knowledge View (`cost_of_disruption_by_region`) bridges data from 4 domain schemas that no single agent can access alone. Governed tags and schema domain tags improve asset discovery.

**Targets**: E03, H05, H06, H07 → **~38-40/45 (~87%)**

**Key insight**: Open Knowledge is a governed view that crosses domain boundaries — it exists because some business questions (like Cost of Disruption) require data from multiple schemas.

##### Iteration 3: fiscal_targets + SQL Functions + UC Pages (cells 19-21)

**UC Features**: Reference Table (`fiscal_targets`), 5 SQL Functions for critical thresholds, UC Domain + UC Pages (Discover page, human governance)

**What it fixes**: `fiscal_targets` TABLE provides queryable fiscal calendar and Q1 target (92%). SQL Functions (`get_critical_delay_shipments()`, etc.) encode domain-specific "critical" thresholds that agents would otherwise guess. UC Pages provide human-facing governance documentation on the Discover page. **Important**: Genie Agents cannot access UC Pages directly (proven — the agent stated it has no access). G01/G02 are fixed by the `fiscal_targets` table. P01-P05 are fixed by SQL Functions.

**Targets**: G01, G02, P01-P05 → **45/45 (100%, fully deterministic)**

**Cell split**: Cell 19 runs setup (creates SQL functions, adds fiscal_targets to agent, cleanup) then **stops** with a manual step prompt. Cell 21 runs the 45-test suite after the user has added SQL Functions to agents via the UI and created UC Pages. Cell 20 adds ai_forecast capability to domain agents.

**Key insight**: The last mile of accuracy requires grounded governance assets (tables and SQL functions) that agents can actually query. UC Pages are valuable for human documentation but agents need SQL-queryable equivalents.

##### Prerequisite: UC Domain and Pages (manual — from UI)

Before running Iteration 3, create these on the **Discover** page:

* **Domain**: "Supply Chain Operations" — assign all 5 schemas
* **Page 1**: "Fiscal Calendar & Targets" — Synonyms: Fiscal, Fiscal Year. Definition: July FY start, Q1=Jul-Sep, Q2=Oct-Dec, Q3=Jan-Mar, Q4=Apr-Jun. Business Use: Q1 service-level target = 92.0%. Reference date: Sept 1, 2026. Last month = August 2026, prior month = July 2026.

  ![UC Page: Fiscal Calendar & Targets](docs/images/3.png)

* **Page 2**: "Cross-Domain Metric Definitions" — Definition: Vendor Late Rate = late POs / total POs per ORDER (75.0%), NEVER per distinct vendor (83.33%). OTD rate (94.57%) ≠ supplier late rate (75%). CoD formula. Fulfillment rate = only `Fulfilled` status, not `Partially_Fulfilled`.

  ![UC Page: Cross-Domain Metric Definitions](docs/images/4.png)

* **Pages 3-7**: Domain-specific critical threshold definitions (see [UC Pages section](#uc-pages-create-from-ui--within-the-domain) for full details):
  * Page 3: "Logistics Risk Standards" — `delay_days >= 5 AND total_weight_kg > 800` → 176 shipments
  * Page 4: "Demand Quality Standards" — `quantity >= 8 AND unit_price < 30 AND channel='Online'` → 77 orders
  * Page 5: "Inventory Risk Classification" — `days_of_supply BETWEEN 1 AND 11 AND below_safety_stock_flag=true AND on_hand_qty > 0` → 106 positions
  * Page 6: "Supplier Quality Standards" — `quality_score < 75 AND lead_time_variance_days > 12` → 11 orders
  * Page 7: "Executive Alert Thresholds" — `composite_risk_score < 55 AND lead_time_variance > 8 AND total_penalty_usd > 80000` → 3 suppliers

#### Step 4: Teardown (when done)

| # | Script | What It Does |
|---|--------|-------------|
| 9 | `src/09_teardown.py` | Deletes all Genie Agents, Supervisor Agent, and drops the entire catalog. UC Domain + Pages persist by design (governance layer). |

---

## Progressive Improvement Summary

| Stage | What Changed | UC Feature | Score |
|-------|-------------|-----------|-------|
| **Baseline** | Bare tables + basic agent instructions, no semantic enrichment | None | **~30-31/45 (~67%, non-deterministic)** |
| **+ Iter 1: Comments + Example SQL + Benchmarks** | Column/table comments + Example SQL Queries + Benchmarks | `ALTER TABLE SET COMMENT` + `example_question_sqls` API + `benchmarks` API | **~35-37/45 (~78%)** |
| **+ Iter 2: Metric Views + Tags + Open Knowledge** | 4 UC Metric Views (YAML) + governed tags + CoD cross-domain view | `CREATE VIEW WITH METRICS LANGUAGE YAML` + `ALTER TABLE SET TAGS` | **~38-40/45 (~87%)** |
| **+ Iter 3: fiscal_targets + SQL Functions + UC Pages** | `fiscal_targets` table + 5 SQL Functions + UC Domain & Pages (UI) | Reference tables + SQL Functions + UC Pages + Domains | **45/45 (100%, fully deterministic)** |

## Expected Output (after all 3 iterations)

The Supervisor produces a structured 7-section executive brief:

1. **What I Understood** — restates the business question
2. **Investigation Plan** — lists which agents to query and exact questions
3. **Findings by Agent** — for each of 5 agents: questions asked, SQL used, result summary, confidence
4. **Cross-Domain Reconciliation** — table comparing metrics across agents and governed sources
5. **Root Cause Chain** — numbered causal cascade: Supplier → Inventory → Logistics → Revenue → Service Level
6. **Ground Truth Scorecard** — primary and indirect tracked metrics scored separately (EXACT / CLOSE / MISS / NOT_FOUND)
7. **Conclusion and Actions** — direct answer + action tables + KPIs to monitor + risk assessment

## Complete Metric & UC Feature Matrix

This is the **authoritative reference** for every metric the demo tracks, why the agent fails without UC features, and which feature fixes it.

### The Core Problem: Business Terms ≠ Database Columns

When business users ask questions, they use **business vocabulary** ("revenue", "fill rate", "vendor penalties"). The database has **technical column names** (`total_amount`, `service_level_pct`, `penalty_amount`). Without UC Semantics, AI agents guess — and guess wrong.

| # | Business Term (in prompt) | What Agent Guesses | Correct Column/Table | Why It Fails | UC Feature Fix | Iteration |
|---|---|---|---|---|---|---|
| 1 | "revenue decline" | Looks for `revenue` column | `total_amount` in sales_orders | No column named revenue | Column Comment + Certified Query | Iter 1 |
| 2 | "on-time delivery rate" | Computes OTD incorrectly | Inverse of `is_late` in shipments | Must compute `NOT is_late` as % | Metric View | Iter 2 |
| 3 | "fill rate" | Looks for `fill_rate` column | `service_level_pct` in executive_kpis | Different name entirely | Column Comment + Certified Query | Iter 1 |
| 4 | "inventory positions below safety stock" | `COUNT(DISTINCT sku_id)` = 61 | `COUNT(*)` per SKU-warehouse row = 109 | Multiple rows per SKU (one per warehouse) | Column Comment (explains granularity) | Iter 1 |
| 5 | "SKUs stocked out" | `COUNT(*)` = 56 | `COUNT(DISTINCT sku_id)` = 33 | Opposite ambiguity to #4 | Column Comment (explains DISTINCT) | Iter 1 |
| 6 | "vendor SLA penalties" | Looks in wrong schema | `penalty_amount` in vendor_slas | "vendor" not in column/table names | Certified Query | Iter 1 |
| 7 | "vendors delivered late" | Looks in wrong schema | `supplier_orders.is_late` | "vendor" not in supplier schema | Certified Query | Iter 1 |
| 8 | "average delay" | Usually finds it | `delay_days` in shipments | Relatively direct mapping | Direct (baseline findable) | Baseline |
| 9 | "Cost of Disruption" | No such table/column exists | Cross-domain view joining 4 tables | Concept undefined anywhere in schema | Open Knowledge view | Iter 2 |
| 10 | "Q1 service-level targets" | Guesses 90% or 95% | **92.0%** — defined in UC Page only | Business policy, not in any table | UC Page | Iter 3 |

### Primary Ground Truth Metrics (10 metrics — explicitly asked in prompt)

These are dynamically computed from live data and stored in `reporting.ground_truth_kpis`.

| # | Agent | Metric | Example GT Value | Source Table/View | Reference SQL | UC Feature Needed |
|---|---|---|---|---|---|---|
| 1 | demand-analysis | Western revenue MoM change (Aug vs Jul 2026) | -1240330.12 | demand_analysis.sales_orders | `SUM(total_amount) for Aug - SUM for Jul WHERE region='Western'` | Comment + Certified Query (Iter 1) |
| 2 | logistics-operations | Western on-time delivery rate (Aug 2026) | 5.4 | logistics_operations.shipments | `ROUND(AVG(CASE WHEN NOT is_late THEN 1.0 ELSE 0.0 END)*100, 1)` | Metric View (Iter 2) |
| 3 | executive-reporting | Fill rate | 70.4 | reporting.executive_kpis | `SELECT service_level_pct` | Direct (baseline findable) |
| 4 | inventory-management | Western below safety stock positions | 109 | inventory_management.inventory_ledger | `COUNT(*) WHERE below_safety_stock_flag AND region='Western'` | Comment (SKU-warehouse granularity) |
| 5 | inventory-management | Western stockout SKUs | 33 | inventory_management.inventory_ledger | `COUNT(DISTINCT sku_id) WHERE stockout_flag AND region='Western'` | Comment (COUNT DISTINCT) |
| 6 | supplier-risk | Total vendor SLA penalties (Aug 2026) | 1185043.1 | supplier_procurement.vendor_slas | `SUM(penalty_amount) WHERE is_breached=true` | Certified Query (Iter 1) |
| 7 | supplier-risk | Vendor late delivery pct (Aug 2026) | 75.0 | supplier_procurement.supplier_orders | `AVG(CASE WHEN is_late...) * 100` | Certified Query (Iter 1) |
| 8 | logistics-operations | Western avg delay days (Aug 2026) | 2.9 | logistics_operations.shipments | `AVG(delay_days) WHERE is_late AND dest_region='Western'` | Direct (baseline findable) |
| 9 | executive-reporting | Western Cost of Disruption | 3757298.31 | reporting.cost_of_disruption_by_region | Cross-domain join: cancelled rev + backorder rev + late freight + SLA penalties | Open Knowledge view (Iter 2) |
| 10 | executive-reporting | Q1 service-level target | 92.0 | **UC Page** (not in any table) | Business policy defined in UC Page | **UC Page** (Iter 3) |

> **Note**: Values are examples from current data. All are computed dynamically — no hardcoding.

### Indirect Ground Truth Metrics (derived from metric views and analysis)

These are NOT explicitly asked in the prompt, but appear in the agent's analysis as supporting data. Tracking them validates that the agent's SQL is correct beyond just the headline number.

| # | Category | Metric | Example Value | Source | Related Primary GT |
|---|---|---|---|---|---|
| 11 | Revenue | Western Aug revenue | 3341062.58 | sales_orders | #1 (MoM change) |
| 12 | Revenue | Western Jul revenue | 4581392.70 | sales_orders | #1 (MoM change) |
| 13 | Revenue | Western revenue % change | -27.1 | Derived | #1 (MoM change) |
| 14 | Logistics | Western late delivery rate | 94.6 | delivery_performance_by_region | #2 (OTD = inverse) |
| 15 | Logistics | Western total shipments (Aug) | 1086 | delivery_performance_by_region | #2 |
| 16 | Logistics | Western late shipments (Aug) | 1027 | delivery_performance_by_region | #2 |
| 17 | Logistics | Western wasted freight cost | (from view) | delivery_performance_by_region | #9 (CoD component) |
| 18 | Inventory | Western unique SKUs below safety | 61 | inventory_safety_stock_metrics | #4 (positions vs SKUs) |
| 19 | Inventory | Western avg days of supply (at-risk) | 1.0 | inventory_safety_stock_metrics | #4 |
| 20 | Supplier | Asia supplier late rate | 100.0 | supplier_performance_by_continent | #7 |
| 21 | Supplier | Asia avg lead time variance | 13.7 | supplier_performance_by_continent | #7 |
| 22 | Orders | Western fulfilled order % | 71.2 | sales_orders | Context |
| 23 | Orders | Western backordered orders | 275 | sales_orders | Context |
| 24 | Orders | Western cancelled revenue | 179419.26 | sales_orders | #9 (CoD component) |

---

## Tracked Ground Truth

The full test suite consists of **45 individual metric tests** organized into 9 groups (A-H, P). Each test sends a natural-language question to a specific Genie Agent and compares the returned value against a ground truth at exact 2-decimal precision.

See the Curator Workbench cell 7 (Curator Benchmarks) for the complete test definitions and ground truth values.

Ground truth SQL patterns are embedded in the Curator Workbench Benchmarks (cell 7) and the metric view definitions in cell 13 (Iteration 2). They do not need to be maintained separately in this README.

---

## Metric View Inventory


Each metric view is a `CREATE VIEW ... WITH METRICS LANGUAGE YAML` object. In this demo, metric views are the authoritative semantic layer for KPIs that agents otherwise compute inconsistently from raw tables.

### 1. `logistics_operations.delivery_performance_by_region`

**Purpose**: Resolves the classic OTD error. Agents often confuse late rate and on-time rate, or they filter on `region` instead of `destination_region`. This object fixes both.

| Measure / Field | Meaning | Why it exists | Maps to GT # | Tracked? |
| --- | --- | --- | --- | --- |
| `on_time_delivery_rate` | Percent of shipments with `is_late = false` | Gives the prompt's OTD answer directly | **#2** (Primary) | ✅ |
| `late_delivery_rate` | Percent of shipments with `is_late = true` in Aug 2026 | Supporting inverse of OTD | **#14** (Indirect) | ✅ |
| `avg_delay_days` | Average `delay_days` for late shipments only | Avoids averaging on-time shipments as zero | **#8** (Primary) | ✅ |
| `total_shipments` | Shipment count in Aug 2026 | Gives denominator for delivery metrics | **#15** (Indirect) | ✅ |
| `late_shipments` | Late-shipment count in Aug 2026 | Gives numerator for late-rate metrics | **#16** (Indirect) | ✅ |
| `wasted_freight_cost` | Shipping cost spent on late shipments | CoD component | **#17** (Indirect) | ✅ |

**Dimension / synonym design**:
* Uses the shipment destination slice for West-region delivery questions.
* Includes region synonyms such as "destination region", "ship to region", and "delivery region".
* Encodes the August 2026 calendar-month window in the metric object itself.

### 2. `demand_analysis.revenue_comparison_by_region`

**Purpose**: Resolves both vocabulary mapping (`revenue` → `total_amount`) and formula correctness (Aug vs Jul month-over-month comparison).

| Measure / Field | Meaning | Why it exists | Maps to GT # | Tracked? |
| --- | --- | --- | --- | --- |
| `revenue_last_month` | August 2026 order value | Removes ambiguity around "last month" | **#11** (Indirect) | ✅ |
| `revenue_prior_month` | July 2026 order value | Required comparison baseline | **#12** (Indirect) | ✅ |
| `revenue_change_dollars` | Aug minus Jul | Primary prompt KPI | **#1** (Primary) | ✅ |
| `revenue_change_pct` | Percent change vs July | Supports severity framing | **#13** (Indirect) | ✅ |

**Dimensions**:
* `region`
* `product_family`

This lets the same governed object answer both the headline decline and the product-family contribution analysis.

### 3. `inventory_management.inventory_safety_stock_metrics`

**Purpose**: Resolves the hardest grain ambiguity in the demo: `COUNT(*)` per SKU-warehouse position versus `COUNT(DISTINCT sku_id)` per unique SKU.

| Measure / Field | Meaning | Why it exists | Maps to GT # | Tracked? |
| --- | --- | --- | --- | --- |
| `positions_below_safety_stock` | Count of at-risk SKU-warehouse positions | Authoritative answer to "how many inventory positions are below safety stock" | **#4** (Primary) | ✅ |
| `unique_skus_below_safety` | Count of unique at-risk SKUs | Supporting context, not the primary prompt metric | **#18** (Indirect) | ✅ |
| `stockout_positions` | Count of stockout positions | Extra inventory stress context | Context | ✅ |
| `unique_skus_in_stockout` | Count of unique stockout SKUs | Prompt asks for stocked-out SKUs | **#5** (Primary) | ✅ |
| `avg_days_of_supply` | Average days of supply for below-safety items | Signals urgency of replenishment risk | **#19** (Indirect) | ✅ |

**Dimension / definition design**:
* `region` is the governing slice.
* The metric-view comment explicitly tells Genie to use `positions_below_safety_stock` when the prompt says "positions", and not to substitute the unique-SKU metric.

### 4. `supplier_procurement.supplier_performance_by_continent`

**Purpose**: Resolves `vendor` versus `supplier` terminology and captures supplier-performance KPIs at a governed regional grouping.

| Measure / Field | Meaning | Why it exists | Maps to GT # | Tracked? |
| --- | --- | --- | --- | --- |
| `total_purchase_orders` | PO count in Aug 2026 | Provides denominator | Context | ✅ |
| `late_purchase_orders` | Late PO count in Aug 2026 | Provides numerator | Context | ✅ |
| `supplier_late_rate_pct` | Late PO rate by continent | Governs vendor/supplier lateness questions | **#7**, **#20** | ✅ |
| `avg_lead_time_variance` | Mean lateness vs expected lead time | Root-cause supplier stress metric | **#21** (Indirect) | ✅ |

**Dimension / synonym design**:
* `supplier_continent` is the governed business grouping.
* Synonyms include vendor-oriented language so Genie can map "vendor late rate" into supplier assets.

### 5. `reporting.cost_of_disruption_by_region` (cross-domain governed executive view)

**Purpose**: Gives the CFO a single governed financial-impact number that no single domain table can answer.

| Measure / Field | Meaning | Maps to GT # |
| --- | --- | --- |
| `cancelled_revenue` | Lost revenue from cancelled orders | **#24** (Indirect) |
| `backordered_at_risk_revenue` | Revenue currently at risk in backorders | Context |
| `wasted_logistics_spend` | Freight spent on late shipments | **#17** (shared CoD component) |
| `allocated_supplier_penalties` | Regionally allocated share of total supplier SLA penalties | Context |
| `total_cost_of_disruption` | Sum of all CoD components | **#9** (Primary) |

This is not a metric view in the YAML sense; it is a governed cross-domain executive view. It exists because CoD is fundamentally a business concept that spans demand, logistics, and supplier data.

---

## UC Semantics Implementation Plan

The demo uses all 4 pillars of [Unity Catalog Semantics](https://learn.microsoft.com/en-us/azure/databricks/uc-semantics/) to ground AI agent responses:

| UC Feature | What It Does | How It's Created | Iteration |
|---|---|---|---|
| **Column Comments** | Tells agents what columns mean ("total_amount = total order revenue in USD") | `ALTER TABLE ... SET COMMENT` (automated) | Iter 1 |
| **Certified Queries** | Pre-built SQL examples for disambiguation | Genie Agent instruction text (automated) | Iter 1 |
| **Metric Views** | Pre-computed KPIs with `MEASURE()` syntax + Open Knowledge view | `CREATE VIEW WITH METRICS LANGUAGE YAML` (automated) | Iter 2 |
| **Governed Tags** | Assigns assets to domains + adds certification metadata | `ALTER TABLE/SCHEMA SET TAGS` (automated) | Iter 2 |
| **Domains** | Business-aligned grouping of data assets on the Discover page | Created from Databricks UI (manual) | Iter 3 |
| **UC Pages** | Governed business definitions (policies, targets, formulas) that Genie One references authoritatively | Created from Databricks UI (manual) | Iter 3 |
| **Certification** | Marks assets as trusted — steers Genie toward vetted sources | `SET TAG ... certification_status = 'certified'` (automated) | Iter 2 |
| **UC Domain** | Business-aligned grouping of data assets | Created from Databricks UI (manual) | Iter 3 |

### Domain (create from UI — Discover page)

Create a single domain on the **Discover** page that encompasses all supply chain schemas.

| Domain Name | Description | Schemas to Assign |
|---|---|---|
| **Supply Chain Operations** | End-to-end supply chain data across all business domains | All 5 schemas: `demand_analysis`, `inventory_management`, `logistics_operations`, `supplier_procurement`, `reporting` |

**Governed tags** are applied automatically by Iteration 2 (cell 13) to all 5 schemas. The single domain groups all supply chain assets together on the Discover page, making it easy for Genie to find related assets across domains.

### UC Pages (create from UI — within the Domain)

Create **7 Pages** within the "Supply Chain Operations" domain: 2 cross-domain (Fiscal Calendar, Cross-Domain Metrics) + 5 domain-specific critical threshold definitions (one per domain). The 2 cross-domain pages resolve ambiguity for humans and Genie One. The 5 domain-specific pages define multi-condition "critical" policies that are unguessable without the definition — these are bridged to agents via SQL Functions.

#### Page 1: "Fiscal Calendar & Targets"

| Field | Value |
|---|---|
| **Domain** | Supply Chain Operations |
| **Synonyms** | Fiscal and Fiscal Year |
| **Description** | Fiscal Calendar for this domain of Supply chain |
| **Definition** | This organization uses a **July fiscal year start**. **Q1=Jul-Sep** (current quarter), Q2=Oct-Dec, Q3=Jan-Mar, Q4=Apr-Jun. Current FY: FY2027 (Jul 2026 – Jun 2027). |
| **Business Use** | Q1 service-level target = 92.0%. Reference date: September 1, 2026. "Last month" = August 2026. "Prior month" = July 2026. When a question mentions "August" without a year, ALWAYS use 2026. |
| **Related Assets** | `fiscal_targets`, `executive_kpis` |

> **This Page is required for GT tests G01/G02.** Without it, the agent cannot authoritatively answer "Are we going to miss our Q1 targets?" — it can compute the current service level from data, but the 92% target is a business policy, and Q1 = Jul-Sep (fiscal, current quarter).

#### Page 2: "Cross-Domain Metric Definitions"

| Field | Value |
|---|---|
| **Domain** | Supply Chain Operations |
| **Description** | Cross-Domain Metric Definitions |
| **Definition** | Resolves ambiguity between same-named metrics across domains. **Vendor Late Rate** = late POs / total POs per ORDER (= 75.0%). NEVER use COUNT(DISTINCT supplier_id) which gives per-vendor = 83.33%. "Percentage of vendors delivered late" is a BUSINESS TERM meaning per-order, not per-distinct-vendor. |
| **Business Use** | CoD formula. OTD rate (94.57%) ≠ supplier late rate (75%). Fulfillment rate = only `Fulfilled` status (not `Partially_Fulfilled`). |
| **Related Assets** | `delivery_performance_by_region`, `cost_of_disruption_by_region`, `supplier_performance_by_continent`, `revenue_comparison_by_region` |

> **Note**: UC Pages is Beta (UI-only, no API). Pages are created manually on the Discover page. The notebook documents their existence but does not create them programmatically. UC Pages feed directly into Genie's ontology — they are the governance layer that resolves business ambiguity (fiscal calendar definition, vendor late rate semantics) without requiring agent instruction injection. The `fiscal_targets` reference table provides queryable data backing Page 1.
>
> **This Page is required for GT test F02.** Without it, the agent interprets "percentage of vendors delivered late" literally (per-vendor = 83.33%) instead of using the governed business definition (per-order = 75.0%).

#### Pages 3-7: Domain-Specific Critical Threshold Definitions

Each domain defines a **named policy** with 2-3 conditions that no LLM can infer from general knowledge. Tests P01-P05 validate these. Without the Page (or its SQL Function equivalent), the agent must guess the threshold — and multi-condition rules are unguessable.

#### Page 3: "Logistics Risk Standards" (schema: `logistics_operations`)

| Field | Value |
|---|---|
| **Domain** | Supply Chain Operations |
| **Synonyms** | logistics risk flag, flagged shipment |
| **Definition** | A shipment is **flagged under Logistics Risk Standards** when `delay_days >= 5` **AND** `total_weight_kg > 800`. Both conditions must be met — heavy shipments with significant delay. Minor delays or lightweight shipments do not trigger the flag. |
| **Business Use** | Flagged shipments require escalation. Use `delay_days >= 5 AND total_weight_kg > 800`. Neither condition alone is sufficient. |
| **Related Assets** | `shipments` |

> **Tests P01**: Without this definition, agent guesses single-condition thresholds (e.g., delay > 3, or delay > 7). Correct count = **176**.

#### Page 4: "Demand Quality Standards" (schema: `demand_analysis`)

| Field | Value |
|---|---|
| **Domain** | Supply Chain Operations |
| **Synonyms** | demand anomaly, anomaly alert |
| **Definition** | A Western region order **triggers a Demand Anomaly Alert** when `quantity >= 8` **AND** `unit_price < 30` **AND** `channel = 'Online'`. Identifies high-volume, low-price online orders signaling unusual demand patterns. |
| **Business Use** | Use `quantity >= 8 AND unit_price < 30 AND channel = 'Online'` on `sales_orders` filtered to Western region, August 2026. All three conditions must be met. |
| **Related Assets** | `sales_orders` |

> **Tests P02**: Three conditions make guessing impossible. Correct count = **77**.

#### Page 5: "Inventory Risk Classification" (schema: `inventory_management`)

| Field | Value |
|---|---|
| **Domain** | Supply Chain Operations |
| **Synonyms** | supply risk, inventory risk |
| **Definition** | An inventory position is **classified as supply-risk** when `days_of_supply` is **BETWEEN 1 AND 11** AND `below_safety_stock_flag = true` AND `on_hand_qty > 0`. Captures items running low but not yet stocked out. |
| **Business Use** | Use `days_of_supply BETWEEN 1 AND 11 AND below_safety_stock_flag = true AND on_hand_qty > 0`. All three conditions define the risk band. |
| **Related Assets** | `inventory_ledger` |

> **Tests P03**: Correct count = **106**.

#### Page 6: "Supplier Quality Standards" (schema: `supplier_procurement`)

| Field | Value |
|---|---|
| **Domain** | Supply Chain Operations |
| **Synonyms** | quality minimum, procurement quality |
| **Definition** | A supplier order **falls below the Procurement Quality Minimum** when `quality_score < 75` **AND** `lead_time_variance_days > 12`. Both marginal quality AND significant delivery variance must co-occur. Quality < 75 alone = 19 orders; adding ltv > 12 narrows to 11. |
| **Business Use** | Use `quality_score < 75 AND lead_time_variance_days > 12`. Quality < 75 alone = 19 (wrong). ltv > 12 alone = wrong. Both needed → 11. |
| **Related Assets** | `supplier_orders` |

> **Tests P04**: Correct count = **11**.

#### Page 7: "Executive Alert Thresholds" (schema: `reporting`)

| Field | Value |
|---|---|
| **Domain** | Supply Chain Operations |
| **Synonyms** | disruption threshold, executive alert |
| **Definition** | A supplier **exceeds the Executive Disruption Threshold** when `composite_risk_score < 55` **AND** `lead_time_variance > 8` **AND** `total_penalty_usd > 80000`. Triple condition identifies suppliers with compounding risk. Each pair gives a different wrong answer (score+ltv=4, score+penalty=5, score alone=6). |
| **Business Use** | Use `composite_risk_score < 55 AND lead_time_variance > 8 AND total_penalty_usd > 80000` from `supply_chain_risk_scorecard`. All 3 needed → 3. |
| **Related Assets** | `supply_chain_risk_scorecard` |

> **Tests P05**: Correct count = **3**.

### Certification (automated via SQL)

All metric views, schemas, and the Open Knowledge view are tagged with governed tags in Iteration 2 (cell 13). This steers Genie toward these assets when resolving ambiguous questions.

---

## The Six Foundation Layers Mapped to This Demo

The image is the right mental model. To make this workshop complete, every layer except `dbxmetagen` should have a concrete artifact in the demo.

| Layer | Component from the foundation model | Why it matters to SQL grounding | How this demo uses it | Status |
| --- | --- | --- | --- | --- |
| 0. Get the data model right | Gold model for agents, clear grain, golden records | Bad grain creates bad SQL even with perfect prompts | Domain schemas, fixed monthly grain, clear base facts (`sales_orders`, `shipments`, `inventory_ledger`, `supplier_orders`) | In use |
| 1. Enrich metadata | Table comments, column comments | Explains what columns actually mean | Iteration 1 comments across tables/columns | In use |
| 1. Enrich metadata | Governed tags | Improves discovery and domain organization | Domain tags and certification tags on schemas/views | In use |
| 1. Enrich metadata | Classification | Lets governance policies reason over sensitive/business-critical assets | Add classification tags such as `financial_metric`, `operational_kpi`, `internal_only` on executive and supplier assets | Should add explicitly |
| 1. Enrich metadata | dbxmetagen | Metadata-at-scale accelerator | Out of scope for this workshop; mention only as scale path | Explicitly excluded |
| 2. Model business semantics | Metric Views for certified KPIs | Removes formula ambiguity | 4 UC Metric Views for revenue, delivery, inventory, supplier performance | In use |
| 2. Model business semantics | Declared relationships (PK/FK) | Improves join-path selection | Add and document key relationships: `sales_orders.product_id → products.product_id`, `shipments.carrier_id → carriers.carrier_id`, `supplier_orders.supplier_id → suppliers.supplier_id`, etc. | Should add explicitly |
| 2. Model business semantics | Domains | Organizes business assets the way users think | 1 domain: "Supply Chain Operations" grouping all 5 schemas | Planned manual UI step |
| 2. Model business semantics | Pages | Supplies policy and glossary facts not stored in tables | 7 pages: Fiscal Calendar & Targets, Cross-Domain Metric Definitions, and 5 domain-specific critical threshold definitions (P01-P05) | Planned manual UI step |
| 3. Curate context-rich assets | Certification / trusted assets | Steers Genie to governed sources first | Certification tags on metric views, CoD view, and vetted reporting assets | In use |
| 3. Curate context-rich assets | Quality-checked assets | Gives a gold answer set | `expected_output_reference` + benchmark SQL inventory + benchmark table | In use |
| 3. Curate context-rich assets | Instructions | Fixes decomposition and wording | Supervisor hardening and domain instructions | In use |
| 3. Curate context-rich assets | Definitions | Makes business meaning explicit | UC Pages + rich comments | In use / planned |
| 3. Curate context-rich assets | Examples | Gives Genie exact SQL patterns | Certified queries and example question-SQL pairs | In use |
| 4. Govern access | UC access controls | Ensures answers only use permitted assets | Standard grants on schemas/tables in the demo catalog | In use |
| 4. Govern access | Row-level security | Proves governed answers remain filtered | Add one lightweight row-filter example on a non-critical reporting view if you want an explicit workshop step | Should add explicitly |
| 4. Govern access | Column masking | Proves Genie respects protected fields | Add one mask on a supplier-sensitive column or contract field without affecting the main KPI prompt | Should add explicitly |
| 4. Govern access | ABAC | Shows policy-by-tag, not per-table hardcoding | Reuse governed tags/classification for one simple tag-based policy | Should add explicitly |
| 4. Govern access | Unity AI Gateway | Governance, logging, guardrails for model calls | Optional wrapper around the supervisor endpoint for traces/guardrails if workspace setup allows it | Useful extension |
| 5. Evaluate & improve | Benchmark answers | Objective scoring of answer quality | `ground_truth_kpis`, direct/indirect GTs, Python scorer, evaluator agent | In use |
| 5. Evaluate & improve | Observe & trace | Lets us see what the system actually did | Capture supervisor/evaluator outputs and, if enabled, model traces | Partially in use |
| 5. Evaluate & improve | Capture feedback | Turns mistakes into ontology improvements | Workshop loop: wrong answer → comments/synonyms/certified SQL/metric views/pages | In use conceptually |
| 5. Evaluate & improve | Watch for drift | Detects regressions over time | Persist benchmark results by iteration/run and compare against prior runs | Should add explicitly |

### Deeper definitions of the semantic components we rely on most

| Component | What it is in this demo | What it fixes |
| --- | --- | --- |
| **Synonyms** | Business-term aliases attached to Genie columns and metric-view fields | Vocabulary mismatch: revenue, fill rate, vendor, SLA penalty, West/Western |
| **Metric Views** | YAML-defined governed KPI objects with explicit dimensions and measures | Formula ambiguity and grain ambiguity |
| **UC Pages** | Human-authored governed markdown pages in Discover | Business policy and glossary facts that should not be reverse-engineered from SQL |
| **Domains** | Business-aligned grouping of tables, views, metric views, and pages | Discovery/ranking and better business routing |
| **Certification** | Trusted-source signal on assets | Source ranking under ambiguity |
| **Instructions + examples** | Supervisor and Genie guidance plus vetted query templates | Better question decomposition and exact SQL shapes |

### What should make Genie write the correct SQL in this workshop

For this specific demo, the SQL is correct only when all of the following line up:

1. The question routes to the right domain agent.
2. Business words map to the right governed field via synonyms or page definitions.
3. The agent prefers the governed metric view or certified executive view instead of raw-table guessing.
4. The examples/certified queries show the exact calendar-month filter and formula shape.
5. Certification and governed tags bias retrieval toward the trusted asset.
6. The benchmark scorer catches any regression immediately.

If any one of those is missing, Genie can still produce plausible SQL, but not necessarily the right SQL.

## Iteration Plan (Curator Workbench)

The master orchestrator notebook ("Supply Chain Control Tower Curator Workbench", 33 cells) runs **3 inline iterations** (cells 10, 13, 19-21), each adding a distinct category of UC Semantics feature. The 45-test Curator Benchmarks suite runs after each iteration to measure progress. After the final iteration, cells 27-29 provide deep analysis of reliability, robustness, and provenance.

| Stage | Cell | UC Feature Added | Targets Fixed | Score |
|---|---|---|---|---|
| **Baseline** | Cell 7 | None — bare tables + lean agent instructions | — | **~29-31/45 (~67%, non-deterministic)** |
| **Iter 1** | Cell 10 | Column/table **comments** + **Example SQL Queries** + **Benchmarks** | A04, D04, D06, F02, F03, H01, H02, H03 | **~35-37/45 (~78%)** |
| **Iter 2** | Cell 13 | 4 UC **Metric Views** (YAML) + **governed tags** + 1 **Open Knowledge** view (CoD) | E03, H05, H06, H07 | **~38-40/45 (~87%)** |
| **Iter 3 Setup** | Cell 19 | `fiscal_targets` table + 5 **SQL Functions** + UC **Domain & Pages** (UI) | G01, G02, P01-P05 | (setup only — stops for manual step) |
| **ai_forecast** | Cell 20 | ai_forecast capability added to all 5 domain agents | — | (supplementary, no test impact) |
| **Iter 3 Test** | Cell 21 | Full 45-test rerun with SQL Functions active | — | **45/45 (100%, deterministic)** |
| **Visual** | Cell 23 | Provenance analysis + confidence distribution charts | — | Dashboard |
| **Deep Reliability** | Cell 27 | SQL consistency, value stability, flip analysis, production readiness | — | Analysis |
| **Robustness Test** | Cell 28 | 10 hardest questions × 3 rephrased variations = 30 API calls | — | Reliability + Dependability scores |
| **Comp Benchmark** | Cell 29 | Single executive prompt to Supervisor — scores coverage | — | Indirect improvement tracking |

**Key design principle**: Each iteration adds ONE category of UC feature. The progression proves that **data governance → better AI answers**.

### What Each Iteration Does NOT Fix

| Stage | What Still Fails | Why |
|---|---|---|
| Baseline | ~14-15 structural failures + non-deterministic flakes (A02/A03/B03/C05/F06) | No semantic context beyond column names; agent guesses are probabilistic |
| After Iter 1 (Comments + Example SQL) | Cross-domain queries (CoD, revenue at risk), Q1 targets, P-group thresholds | Comments fix single-agent issues but can't span domains or define thresholds |
| After Iter 2 (Metric Views + Open Knowledge) | Q1 target (92%), P-group critical thresholds, remaining non-determinism | Metric views fix formula + cross-domain, but policies and thresholds aren't in any table |
| After Iter 3 (fiscal_targets + SQL Functions) | Nothing — 45/45, fully deterministic | All failure modes resolved; every answer grounded in governed asset |

> **Note**: All time-based metrics use a fixed reference date of September 1, 2026. "Last month" = August 2026, "prior month" = July 2026. The demo produces identical results every run.

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
    ├── 07_add_all_comments.py                   # 180+ column comments (NOT run in pipeline — comments added inline in Iter 1)
    ├── 08_setup_genie_supervisor.py              # Raw baseline: 5 domain Genie Agents + Evaluator + Supervisor
    ├── 09_teardown.py                            # Full cleanup
    ├── app/
    │   ├── app.yaml                               # Databricks App config (Streamlit)
    │   └── action_tracker_app.py                  # Action Intelligence Tracker Streamlit app
    ├── archive/                                  # Old iteration notebooks (no longer used)
    │   ├── iteration_01_baseline_assessment.py
    │   ├── iteration_02_certified_queries.py
    │   ├── iteration_03_column_synonyms.py
    │   ├── iteration_04_supervisor_hardening.py
    │   ├── iteration_05_metric_views_glossary.py
    │   ├── iteration_06_cost_of_disruption.py
    │   └── 10_demo_runner.py
    └── notebooks/
        ├── Supply Chain Control Tower Curator Workbench  # Master orchestrator (33 cells): teardown → build → 3 iterations → verify
        ├── report_agent_functions.py              # Executive Report Agent: charts, HTML/PDF, email/Slack delivery
        └── expected_output_reference.py           # Reference output for validation
```

All iteration logic now runs **inline** in the Curator Workbench notebook (cells 10, 13, 19). The old standalone iteration notebooks have been moved to `src/archive/` for reference only — they are not executed.

## Data Determinism

All data generation scripts use a **fixed reference date** (`base_date = datetime(2026, 9, 1)`) and fixed random seeds (42/43/44/45), following the same approach as standard Databricks training demos. This produces **identical data every run**, regardless of when the demo is executed.

"Last month" = August 2026, "Prior month" = July 2026. All SQL views, certified queries, Genie Agent instructions, and ground truth use `DATE '2026-09-01'` instead of `CURRENT_DATE()`, so the demo is fully self-contained and never needs data regeneration.

---

## Configuration

Edit `databricks.yml` to set per-environment values:

- `catalog_name`: Unity Catalog name (default: `GAP_Demo_Dev`)
- `warehouse_id`: SQL Warehouse ID for Genie Agents

## Provenance & Classification System

Every test result includes **provenance** — which UC semantic feature the agent actually used to get its answer. This makes the demo's thesis visible: accuracy improved BECAUSE the agent used the governed asset we provided.

### Dynamic Classification

The `analyze_agent_sql()` function parses agent SQL and narration to classify HOW the agent arrived at each answer. Classification is **fully dynamic** — no per-test hardcoding. Priority cascade:

| Priority | Label | Detection | Meaning |
|---|---|---|---|
| 1 | METRIC_VIEW | SQL references a governed metric view table | Agent used a UC Metric View (Iter 2) |
| 2 | SQL_FUNCTION | SQL references `get_critical_*()` function | Agent used a SQL Function (Iter 3) |
| 3 | REFERENCE_TABLE | SQL references `fiscal_targets` | Agent used the reference table (Iter 3) |
| 4 | GUESSED_THRESHOLD | WHERE clause has invented numeric cutoff | Agent guessed a threshold not from any UC feature |
| 5 | COLUMN_COMMENT | Narration says "column comment" / "description" | Agent cited column metadata (Iter 1) |
| 6 | EXAMPLE_GUIDED | Narration says "example query" / "certified query" | Agent followed an Example SQL (Iter 1) |
| 7 | DERIVED | SQL has multiple aggregations / CASE WHEN / division | Agent derived a complex formula from raw tables |
| 8 | SYNONYM | Question says "revenue" but SQL uses `total_amount` | Agent resolved a vocabulary synonym |
| 9 | UNAMBIGUOUS | Simple column query on base table | Direct column lookup, no UC feature needed |
| 10 | NARRATION | No SQL returned | Agent answered from text only |

### Reasoning Confidence Labels

Each provenance tier maps to a **confidence label** indicating reproducibility:

| Confidence | Meaning | Provenance Tiers |
|---|---|---|
| **DETERMINISTIC** | Grounded in governed asset — repeatable across runs | Iter 2 (Metric Views), Iter 3 (SQL Functions, Reference Tables) |
| **HEURISTIC** | Guided by metadata — likely repeatable | Iter 1 (Column Comments, Example SQL) |
| **INFERRED** | Agent inferred answer from raw column/table names — may vary across runs | Baseline (no UC feature used) |
| **GUESSED** | Agent invented the answer — unreliable | Guessed thresholds, no UC feature available |

Visual cells (8, 11, 14, 23) display two-panel charts: (1) Provenance tier distribution, (2) Reasoning Confidence distribution.

### Reliability & Robustness Analysis (Cells 27-28)

**Cell 27 (Deep Reliability)** analyzes existing test results across all iterations (zero API calls):
* SQL pattern consistency — same SQL structure across iterations?
* Provenance stability — does the agent converge to governed assets?
* Value reliability — does the returned value stabilize?
* Per-tier production readiness matrix
* Flip analysis — which tests changed verdict between iterations?

**Cell 28 (Robustness Test)** sends 10 hardest questions × 3 rephrased variations (30 API calls):
* Tests whether agent answers are robust to vocabulary changes
* Computes per-test **Reliability** (correct answer regardless of phrasing) and **Dependability** (same SQL approach regardless of phrasing)
* Key finding: DETERMINISTIC-tier tests are robust to rephrasing; INFERRED-tier tests break when vocabulary changes

## Technical Notes

- **Agent Mode API**: The test harness uses `POST /api/2.0/genie/agents/{space_id}/responses` with SSE streaming (not the older `start-conversation` API). Returns SQL queries, function call outputs, and narration text in a single streamed response.
- **Genie One vs Genie Agents (PROVEN)**: UC Pages are consumed by **Genie One** (confirmed: 402 response with citation to "Logistics Risk Standards" Page). Genie Agents **cannot** read UC Pages (agent stated: "I don't have direct access to those pages in this context"). Supervisor Agent has no `page` or `domain` tool type. There is no public REST API for reading UC Page content. This is why SQL Functions (feature #12) exist — they bridge Page-defined thresholds to agents.
- **SQL Functions work via both channels**: Once added as a data source via the Agent UI, SQL Functions work via both the Agent UI and the Agent Mode API. The `serialized_space` API cannot add SQL functions programmatically — UI-only for adding, but both channels can call them.
- **"Last month" = fixed August 2026 calendar month in this workshop**: The demo is deterministic. Data generation, views, certified queries, and benchmark SQL use the fixed anchor `DATE '2026-09-01'` to represent last month = August 2026 and prior month = July 2026. Never rolling 30-day windows.
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
- **[Genie Agent Mode API](https://docs.databricks.com/api/genie/v1/agent-mode-create-response)** — REST API for programmatic SSE streaming queries to Genie Agents (used by the Supervisor and test harness in this demo)
- **[UC Semantics](https://docs.databricks.com/en/uc-semantics/index.html)** — Unity Catalog semantic layer: Domains, Pages (Glossary), Metric Views, governed tags, and the Genie Ontology
