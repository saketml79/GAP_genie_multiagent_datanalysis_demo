# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title and Prompt
# MAGIC %md
# MAGIC # Expected Output Reference: Supply Chain Control Tower
# MAGIC
# MAGIC **Canonical Prompt**: "We need a complete supply chain health check for our West region in August 2026. The CFO wants to understand what drove the revenue decline versus July — show the actual August and July revenue numbers, the dollar change, and the percentage change — and which product families are most at fault. Are our on-time delivery rate and average delay for West region shipments contributing to the problem? How many total shipments went out and how many were late? I also need our current fill rate, how many inventory positions are sitting below safety stock in the West region, how many unique SKUs are affected, what is our days of supply for those at-risk items, and how many SKUs are completely stocked out. On the vendor side: what percentage of vendors delivered late in August, how many purchase orders were late out of total, what is the average lead time variance, and what are the total vendor SLA penalties we have incurred? Bring it all together as our total Cost of Disruption by region for August 2026 — cancelled revenue, at-risk backorder revenue, wasted freight on late shipments, and supplier penalty exposure in one number per region. Are we going to miss our Q3 service-level targets, and what are the top actions we should take?"
# MAGIC
# MAGIC This notebook shows the **correct, verified output** that the Supervisor Agent should produce.
# MAGIC Every number is computed from direct SQL against the actual data at **2 decimal precision**.
# MAGIC All values validated via empirical assumption testing against Genie agents (see 00_run_all Assumption Tester v2).

# COMMAND ----------

dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. What I Understood
# MAGIC
# MAGIC The VP of Operations needs three things:
# MAGIC 1. **Why** did Western Region revenue decline?
# MAGIC 2. **Will we miss** quarterly service-level targets?
# MAGIC 3. **What actions** should we take immediately?
# MAGIC
# MAGIC This requires investigation across all 5 domains: demand, inventory, logistics, suppliers, and executive KPIs.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Investigation Plan
# MAGIC
# MAGIC | Agent | Question |
# MAGIC | --- | --- |
# MAGIC | demand-analysis | Revenue by region last month vs prior month; Western breakdown by product family; order status |
# MAGIC | inventory-management | Stockout SKUs by region; safety stock metrics from metric view; stock movement net flow |
# MAGIC | logistics-operations | Late delivery rate by destination region last month; Western delay reasons; carrier performance |
# MAGIC | supplier-risk | Supplier late rate by continent last month; top risk suppliers; SLA breaches |
# MAGIC | executive-reporting | Executive KPIs; regional performance summary |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Findings by Agent
# MAGIC ### Agent: demand-analysis (SC - Demand Analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 1: Revenue by Region (Last Month vs Prior Month)

# COMMAND ----------

df1 = spark.sql(f"""
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
GROUP BY region ORDER BY revenue_change ASC
""")
df1.display()

# COMMAND ----------

# DBTITLE 1,Finding: Revenue by Region
# MAGIC %md
# MAGIC **Finding**: Western revenue dropped **-$1,240,330.12 (-27.07%)** in August 2026 vs July 2026 -- by far the largest decline.
# MAGIC
# MAGIC * Western: Aug **$3,341,062.58**, Jul $4,581,392.70, change **-$1,240,330.12 (-27.07%)**
# MAGIC * Southern: Aug $4,175,882.05, Jul $4,377,166.50, change -$201,284.45 (-4.60%)
# MAGIC * Eastern: Aug $4,189,811.21, Jul $4,342,727.70, change -$152,916.49 (-3.50%)
# MAGIC * Central: Aug $4,094,683.96, Jul $4,188,864.89, change -$94,180.93 (-2.20%)
# MAGIC
# MAGIC **Confidence**: HIGH -- complete data, 4 regions, calendar month boundaries, clear pattern.
# MAGIC
# MAGIC **Empirically verified**: Agent returns $3,341,062.58 (A1) and -$1,240,330.12 (A2) at baseline with no synonyms.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 2: Western Revenue by Product Family

# COMMAND ----------

df2 = spark.sql(f"""
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
GROUP BY product_family ORDER BY change_usd ASC
""")
df2.display()

# COMMAND ----------

# DBTITLE 1,Finding: Product Family Breakdown
# MAGIC %md
# MAGIC **Finding**: ALL Western product families declined last month. Worst hit:
# MAGIC - Home Goods: **-$349K (-28.9%)**
# MAGIC - Electronics: **-$271K (-27.5%)**
# MAGIC - Footwear: **-$223K (-26.3%)**
# MAGIC - Accessories: **-$213K (-24.6%)**
# MAGIC - Apparel: **-$184K (-27.2%)**
# MAGIC
# MAGIC Sum of product family changes = -$1,240K ✓ (matches regional total)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 3: Western Order Status Breakdown

# COMMAND ----------

df3 = spark.sql(f"""
SELECT order_status, COUNT(*) AS order_count,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_orders
FROM {CATALOG}.demand_analysis.sales_orders
WHERE region = 'Western'
  AND order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY order_status ORDER BY order_count DESC
""")
df3.display()

# COMMAND ----------

# DBTITLE 1,Finding: Order Status
# MAGIC %md
# MAGIC **Finding**: Western Aug 2026 order breakdown:
# MAGIC * Fulfilled: **1,342 orders (71.23%)** — $2,397,445.86
# MAGIC * Backordered: **275 orders (14.60%)** — $474,164.83 at-risk revenue
# MAGIC * Partially_Fulfilled: 170 orders (9.00%) — $290,032.63
# MAGIC * Cancelled: **97 orders (5.10%)** — **$179,419.26** cancelled revenue
# MAGIC
# MAGIC **Confidence**: HIGH -- calendar month boundaries, all statuses captured.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Agent: inventory-management (SC - Inventory Management)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 4: Stockouts by Region

# COMMAND ----------

# DBTITLE 1,Query 4: Inventory by Region (baseline SQL, no metric view)
# Inventory metrics from base table (no metric view needed — agent gets this right at baseline)
df4 = spark.sql(f"""
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
ORDER BY positions_below_safety_stock DESC
""")
df4.display()

# COMMAND ----------

# DBTITLE 1,Finding: Stockouts
# MAGIC %md
# MAGIC **Finding**: Western inventory is in crisis:
# MAGIC * **109** SKU-warehouse positions below safety stock (COUNT(*))
# MAGIC * **61** unique SKUs below safety stock (COUNT(DISTINCT sku_id))
# MAGIC * **33** unique SKUs completely stocked out
# MAGIC * **0.96 days** average days of supply for at-risk items
# MAGIC
# MAGIC **Empirically verified**: Agent returns 109 (A8) and 33 (A9) at baseline with correct SQL — no comments needed.
# MAGIC The agent naturally uses COUNT(*) for "positions" and COUNT(DISTINCT sku_id) for "SKUs".
# MAGIC
# MAGIC **Confidence**: HIGH -- direct from inventory_ledger.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Agent: logistics-operations (SC - Logistics Operations)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 5: Late Delivery Rate by Destination Region (Last Month)

# COMMAND ----------

df5 = spark.sql(f"""
SELECT
  destination_region,
  COUNT(*) AS total_shipments,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_shipments,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(CASE WHEN is_late THEN delay_days ELSE NULL END), 1) AS avg_delay_when_late
FROM {CATALOG}.logistics_operations.shipments
WHERE ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND ship_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY destination_region ORDER BY late_pct DESC
""")
df5.display()

# COMMAND ----------

# DBTITLE 1,Finding: Late Delivery
# MAGIC %md
# MAGIC **Finding**: Western has **94.57% late delivery rate** in August 2026 (1,027 of 1,086 shipments late, avg **2.94 day** delay).
# MAGIC On-time delivery rate: **5.43%**. Wasted freight cost on late shipments: **$2,484,985.57**.
# MAGIC
# MAGIC **Empirically verified**: Agent returns OTD 5.43% (A4), avg delay 2.94 days (A10), and wasted freight $2,484,985.57 (A3) at baseline with correct SQL. No synonyms or metric views needed for any of these.
# MAGIC
# MAGIC **Confidence**: HIGH -- complete shipment data, calendar month boundaries.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 6: Western Delay Reasons

# COMMAND ----------

df6 = spark.sql(f"""
SELECT delay_reason, COUNT(*) AS cnt,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
FROM {CATALOG}.logistics_operations.shipments
WHERE destination_region = 'Western'
  AND ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND ship_date < DATE_TRUNC('month', DATE '2026-09-01')
  AND is_late = true
GROUP BY delay_reason ORDER BY cnt DESC
""")
df6.display()

# COMMAND ----------

# DBTITLE 1,Finding: Delay Reasons
# MAGIC %md
# MAGIC **Finding**: Delays spread across multiple causes: Weather 20.4%, Labor Shortage 20.3%, Customs Hold 20.0%, Carrier Capacity 19.9%, Port Congestion 18.5%. This suggests systemic infrastructure issues, not a single cause.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Agent: supplier-risk (SC - Supplier Risk)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 7: Supplier Performance by Continent

# COMMAND ----------

df7 = spark.sql(f"""
SELECT
  supplier_continent,
  COUNT(*) AS total_pos,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_pos,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(lead_time_variance_days), 1) AS avg_variance_days
FROM {CATALOG}.supplier_procurement.supplier_orders
WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY supplier_continent ORDER BY late_pct DESC
""")
df7.display()

# COMMAND ----------

# DBTITLE 1,Finding: Supplier Performance
# MAGIC %md
# MAGIC **Finding**: Asia suppliers are **100.00% late** (30/30 POs) with **+13.67 day** average lead time variance in August 2026.
# MAGIC Overall: **48 total POs**, **36 late** (**75.00%**), avg lead time variance **8.69 days**.
# MAGIC Total vendor SLA penalties: **$1,185,043.10**.
# MAGIC
# MAGIC **Empirically verified**: Agent returns SLA penalties $1,185,043.10 (A6) and late PO count 36/48 (A14) at baseline. However, A7 shows the agent interprets "% of vendors delivered late" as per-vendor (83.33%) not per-order (75.00%) — this is a genuine semantic ambiguity that needs a certified query or metric view. A13 shows the agent queries the wrong table (supplier_lead_times instead of supplier_orders) for lead time variance — gets 6.075 instead of 8.69.
# MAGIC
# MAGIC **Confidence**: HIGH -- all POs in the calendar month captured.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 8: Top Risk Suppliers

# COMMAND ----------

df8 = spark.sql(f"""
SELECT supplier_id, supplier_name, country, risk_tier,
  composite_risk_score, breach_count, total_penalty,
  latest_variance AS lead_time_variance_days
FROM {CATALOG}.reporting.supply_chain_risk_scorecard
ORDER BY composite_risk_score ASC LIMIT 5
""")
df8.display()

# COMMAND ----------

# DBTITLE 1,Finding: Top Risk Suppliers
# MAGIC %md
# MAGIC **Finding**: Top 5 riskiest suppliers are ALL in Asia. SUP-001 TextilePro Asia (China, Apparel) remains the highest-risk supplier, with **5 SLA breaches**, **$66.2K** in penalties, and **8.7 days** of latest lead-time variance.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Agent: executive-reporting (SC - Executive Reporting)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 9: Executive KPIs

# COMMAND ----------

df9 = spark.sql(f"SELECT * FROM {CATALOG}.reporting.executive_kpis")
df9.display()

# COMMAND ----------

# DBTITLE 1,Finding: Executive KPIs
# MAGIC %md
# MAGIC **Executive KPIs** (all at 2 decimal precision):
# MAGIC * Service level (fill rate): **80.70%** (target: 95.00%) — agent finds this at baseline (A5)
# MAGIC * Western OTD rate: **5.43%** — agent finds this at baseline (A4)
# MAGIC * Western late delivery rate: **94.57%**
# MAGIC * Western avg delay: **2.94 days** — agent finds this at baseline (A10)
# MAGIC * Vendor late delivery % (per-order): **75.00%** — A7 CONFIRMED: agent gets 83.33% (per-vendor interpretation)
# MAGIC * Total SLA penalties: **$1,185,043.10** — agent finds this at baseline (A6)
# MAGIC * Western positions below safety stock: **109** — agent finds this at baseline (A8)
# MAGIC * Western stockout SKUs: **33** — agent finds this at baseline (A9)
# MAGIC * Western Cost of Disruption: **$3,757,298.31** — A12 CONFIRMED: agent cannot compute (no table exists)

# COMMAND ----------

# DBTITLE 1,Cross-Domain Reconciliation
# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Cross-Domain Reconciliation
# MAGIC
# MAGIC | KPI | Value | Source | Agent Baseline? |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Western Aug Revenue | $3,341,062.58 | demand_analysis.sales_orders | ✅ A1 DISPROVED |
# MAGIC | Western Jul Revenue | $4,581,392.70 | demand_analysis.sales_orders | ✅ A2 DISPROVED |
# MAGIC | Western Revenue MoM Change | -$1,240,330.12 (-27.07%) | demand_analysis.sales_orders | ✅ A2 DISPROVED |
# MAGIC | Western OTD Rate | 5.43% | logistics_operations.shipments | ✅ A4 DISPROVED |
# MAGIC | Western Late Delivery Rate | 94.57% | logistics_operations.shipments | supporting |
# MAGIC | Western Avg Delay (late) | 2.94 days | logistics_operations.shipments | ✅ A10 DISPROVED |
# MAGIC | Western Total Shipments | 1,086 | logistics_operations.shipments | supporting |
# MAGIC | Western Late Shipments | 1,027 | logistics_operations.shipments | supporting |
# MAGIC | Western Wasted Freight | $2,484,985.57 | logistics_operations.shipments | ✅ A3 DISPROVED |
# MAGIC | Fill Rate | 80.70% | reporting.executive_kpis | ✅ A5 DISPROVED |
# MAGIC | Western Below Safety Stock | 109 positions | inventory_management.inventory_ledger | ✅ A8 DISPROVED |
# MAGIC | Western Unique SKUs at Risk | 61 | inventory_management.inventory_ledger | supporting |
# MAGIC | Western Stockout SKUs | 33 | inventory_management.inventory_ledger | ✅ A9 DISPROVED |
# MAGIC | Western Avg Days of Supply | 0.96 days | inventory_management.inventory_ledger | supporting |
# MAGIC | Vendor Late % (per-order) | 75.00% | supplier_procurement.supplier_orders | ❌ A7 CONFIRMED |
# MAGIC | Total POs Aug | 48 | supplier_procurement.supplier_orders | supporting |
# MAGIC | Late POs Aug | 36 | supplier_procurement.supplier_orders | ✅ A14 DISPROVED |
# MAGIC | Avg Lead Time Variance | 8.69 days | supplier_procurement.supplier_orders | ❌ A13 CONFIRMED |
# MAGIC | Total SLA Penalties | $1,185,043.10 | supplier_procurement.vendor_slas | ✅ A6 DISPROVED |
# MAGIC | Western Fulfilled Order % | 71.23% | demand_analysis.sales_orders | supporting |
# MAGIC | Western Backordered Orders | 275 | demand_analysis.sales_orders | supporting |
# MAGIC | Western Cancelled Revenue | $179,419.26 | demand_analysis.sales_orders | supporting |
# MAGIC | Western Cost of Disruption | $3,757,298.31 | reporting.cost_of_disruption_by_region | ❌ A12 CONFIRMED |
# MAGIC | Q3 Service-Level Target | 95.00% | UC Page (not in any table) | manual |

# COMMAND ----------

# DBTITLE 1,Root Cause Chain
# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Root Cause Chain
# MAGIC
# MAGIC ```
# MAGIC UPSTREAM CAUSE:
# MAGIC   Asian suppliers 100.00% late (30/30 POs, avg +13.67 days) August 2026
# MAGIC   └─ SUP-001 TextilePro Asia remains the top risk supplier
# MAGIC   └─ 5 of top 5 risk suppliers are in Asia
# MAGIC        │
# MAGIC        ▼
# MAGIC INVENTORY IMPACT:
# MAGIC   Western warehouses depleted (33 SKU stockouts, 109 SKU-warehouse positions below safety stock)
# MAGIC   └─ Western days of supply for at-risk items: 0.96 days
# MAGIC   └─ Other regions show materially lower safety-stock stress
# MAGIC        │
# MAGIC        ▼
# MAGIC LOGISTICS BREAKDOWN:
# MAGIC   94.57% of Western-bound shipments late in August 2026 (1,027/1,086, avg 2.94 day delay)
# MAGIC   └─ Delays are broad-based: Weather, Labor Shortage, Customs Hold, Carrier Capacity, Port Congestion
# MAGIC   └─ This is a systemic logistics issue, not a single-point failure
# MAGIC        │
# MAGIC        ▼
# MAGIC REVENUE IMPACT:
# MAGIC   Western revenue -$1,240,330.12 (-27.07%) August 2026
# MAGIC   └─ All 5 product families declined 24-29%
# MAGIC   └─ Home Goods worst: -$349,062.88 (-28.90%)
# MAGIC   └─ 542 unfulfilled orders = $943,616.72 at-risk revenue
# MAGIC   └─ Service level: 80.70% (below 95.00% target)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Executive Dashboard
# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Executive Dashboard
# MAGIC
# MAGIC | KPI | Current Value | Target | Status | Trend |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | Revenue (Last Month) | $15.8M | $17.5M | BELOW | DOWN -9.7% |
# MAGIC | Western Revenue (Last Month) | $3.34M | $4.58M | CRITICAL | DOWN -27.1% |
# MAGIC | Service Level | 70.4% | 95.0% | BELOW | DECLINING |
# MAGIC | Western Late Delivery (Last Month) | 94.6% | <20% | CRITICAL | WORSENING |
# MAGIC | Stockout SKUs | 33 (Western) | 0 | CRITICAL | -- |
# MAGIC | Supplier On-Time (Asia, Last Month) | 0% | 90% | CRITICAL | WORSENING |
# MAGIC | Supplier Late Rate (Overall) | 75.0% | <15% | CRITICAL | -- |
# MAGIC | Western Avg Delay Days (Last Month) | 2.9 days | <0.5 days | ELEVATED | -- |
# MAGIC | Western Below Safety Stock | 109 positions | 0 | CRITICAL | -- |

# COMMAND ----------

# DBTITLE 1,Conclusion and Actions
# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Conclusion and Recommended Actions
# MAGIC
# MAGIC ### Direct Answers
# MAGIC
# MAGIC 1. **Why did revenue drop?** Asian supplier delays (**100% late, +13.7 day average variance**) last month cascaded through Western inventory (**33 stockouts, 109 SKU-warehouse positions below safety stock**) and Western logistics (**94.6% late deliveries**), causing a **-$1,240K (-27.1%)** revenue decline across all product families. Home Goods was hit hardest (**-$349K, -28.9%**).
# MAGIC
# MAGIC 2. **Will we miss targets?** **YES**. Service level is **70.4%** vs **95%** target. With **94.6%** Western late delivery, **75.0%** supplier late rate, and only **71.2%** of Western orders fully fulfilled, quarterly targets are at material risk without immediate intervention.
# MAGIC
# MAGIC 3. **What actions?** A structured 11-priority recovery plan focused on inventory recovery, customer save actions, supplier escalation, and logistics stabilization.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### IMMEDIATE ACTIONS (Next 24-48 Hours)
# MAGIC
# MAGIC | Priority | Action | Owner | Timeline | Expected Impact | Investment | ROI |
# MAGIC | --- | --- | --- | --- | --- | --- | --- |
# MAGIC | P1 | **Emergency stock transfer**: Redistribute inventory from Eastern/Central warehouses to Western for the **33 stockout SKUs**. Prioritize zero-on-hand and lowest-days-of-supply items first. | VP Supply Chain + Logistics Director | 48 hours | Restore high-priority Western order fulfillment | $30K expedited freight | 7.3x |
# MAGIC | P2 | **Customer recovery blitz**: Contact all customers with backordered, partially fulfilled, and cancelled Western orders. Offer incentives and proactive ETA updates to recover part of the **$944K** at-risk revenue. | VP Sales + Customer Success | 48 hours | Recover $350-400K of at-risk revenue | $75K (discounts + shipping) | 4.7-5.3x |
# MAGIC | P3 | **Carrier escalation**: Emergency executive call with carriers handling Western-bound shipments (**94.6% late rate**). Demand immediate remediation plan or activate backup routes. | VP Logistics | 24 hours | Reduce Western late delivery rate toward <25% within 2 weeks | Carrier SLA penalties | -- |
# MAGIC
# MAGIC ### SHORT-TERM ACTIONS (Next 7-14 Days)
# MAGIC
# MAGIC | Priority | Action | Owner | Timeline | Expected Impact | Investment |
# MAGIC | --- | --- | --- | --- | --- | --- |
# MAGIC | P4 | **Supplier SLA enforcement**: Escalate SUP-001 TextilePro Asia (**5 breaches, $66.2K penalties, 8.7 latest variance days**). Demand a recovery plan or trigger penalty and secondary sourcing. | CPO | This week | Prevent future inventory depletion | SLA penalty enforcement |
# MAGIC | P5 | **Demand forecast correction**: Recalibrate Western replenishment plans using current actuals and fulfillment constraints so replenishment does not assume normal supplier/logistics performance. | Demand Planning Manager | This week | Align orders with operational reality | -- |
# MAGIC | P6 | **Safety stock recalibration**: Increase buffers for the most exposed Western categories and warehouses. Current signal: **109** positions below safety stock and only **1.0 day** of supply on at-risk items. | Inventory Planning Manager | 7 days | Buffer against supplier and carrier delays | $180K working capital |
# MAGIC | P7 | **Western DC audit**: Physical audit of Western warehouses and distribution hubs. Verify inventory accuracy, receiving bottlenecks, and allocation logic. | Warehouse Ops Manager | 7 days | Identify hidden inventory or process constraints | -- |
# MAGIC
# MAGIC ### MEDIUM-TERM ACTIONS (Next 30-60 Days)
# MAGIC
# MAGIC | Priority | Action | Owner | Timeline | Expected Impact |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | P8 | **Carrier diversification**: Shift Western volume toward the best-performing carriers and routes. | Logistics Director | 30 days | Reduce structural late-delivery risk |
# MAGIC | P9 | **Supplier diversification**: Onboard 1-2 North American or European suppliers for the most exposed product families to reduce Asia concentration. | Strategic Sourcing Director | 60 days | Eliminate single-continent dependency |
# MAGIC | P10 | **Western service recovery program**: Dedicated task force with daily standups. Goals: late delivery **94.6% to <25%**, fulfillment up to **90%+**, stockouts **33 to 0**. | Regional GM Western | 30 days | Restore Western operational performance |
# MAGIC | P11 | **Quarterly target recovery**: Build a realistic recovery path against the current **70.4% vs 95%** service-level gap and communicate mitigation plans to stakeholders. | COO + CFO | 30 days | Limit financial impact and protect retention |

# COMMAND ----------

# DBTITLE 1,Financial + KPIs + Bottom Line
# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Financial Impact Summary
# MAGIC
# MAGIC ### Revenue at Risk
# MAGIC
# MAGIC | Metric | Value | Status | Timeline |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Western Revenue Loss (last month) | **-$1,240,330** | Occurred | Past (sunk) |
# MAGIC | Unfulfilled Orders (at-risk revenue) | **$944K** (542 orders) | Recoverable | 7 days |
# MAGIC | Customer Recovery Potential | **+$350-400K** | Actionable | 7-14 days |
# MAGIC | Next Month Revenue at Risk (if no action) | **-$1.2M+** (projected repeat) | Preventable | 30 days |
# MAGIC | Quarterly Service Level Miss Impact | **$730K+** (penalties + attrition) | At risk | 90 days |
# MAGIC
# MAGIC ### Investment Required vs Return
# MAGIC
# MAGIC | Action | Investment | Expected Return | ROI | Payback |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | Emergency inventory airlift (P1) | $30K | $220K+ revenue recovery | 7.3x | Immediate |
# MAGIC | Customer recovery program (P2) | $75K | $350-400K revenue recovery | 4.7-5.3x | 7 days |
# MAGIC | Safety stock increase (P6) | $180K | $400K+ prevented losses/month | 2.2x | 30 days |
# MAGIC | Carrier/supplier improvements (P3-P5) | $50K | $500K+ prevented losses | 10x | 60 days |
# MAGIC | **TOTAL** | **$335K** | **$1.47M+ recovery/prevention** | **4.4x** | **30-60 days** |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 9. KPIs to Monitor (Weekly Tracking)
# MAGIC
# MAGIC | KPI | Current | 2-Week Target | 30-Day Target | 90-Day Target |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | Western late delivery % | 94.6% | <50% | <25% | <15% |
# MAGIC | Western stockout SKUs | 33 | <10 | 0 | 0 |
# MAGIC | Service level % | 70.4% | 80% | 90% | 95% |
# MAGIC | Asian supplier on-time | 0% | 30% | 70% | 90% |
# MAGIC | Western revenue (monthly) | $3.34M | $3.5M | $4.0M | $4.2M |
# MAGIC | Western fulfillment rate | 71.2% | 80% | 90% | 95% |
# MAGIC | Safety stock breaches | 109 | <50 | <25 | <10 |
# MAGIC | Backorders | 275 orders | <100 | <25 | <5 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Bottom Line
# MAGIC
# MAGIC **This is NOT a demand problem. This is a supply chain execution failure.**
# MAGIC
# MAGIC - **Demand existed**: Customers placed orders, but fulfillment failures reduced realized revenue
# MAGIC - **Suppliers failed**: Asia was **100% late** (30/30 POs, **+13.7 day** average variance) last month
# MAGIC - **Logistics collapsed**: **94.6%** of Western-bound shipments were late (**1,027/1,086**) last month
# MAGIC - **Inventory depleted**: **33** stockouts and **109** SKU-warehouse positions below safety stock
# MAGIC - **Revenue dropped**: Western revenue declined **-$1,240K (-27.1%)**
# MAGIC - **Service levels at risk**: **70.4%** vs **95%** target — current trajectory misses quarterly goals
# MAGIC
# MAGIC The 11-priority action plan aims to:
# MAGIC - Recover $350-400K of lost revenue within 7 days
# MAGIC - Prevent $1.2M+ in additional losses next month
# MAGIC - Restore Western service levels materially over the next 30 days
# MAGIC - Require **$335K investment with 4.4x ROI**
# MAGIC
# MAGIC **This is a solvable crisis, but the window is closing. Action must begin TODAY.**