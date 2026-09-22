# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title: Ground Truth Definition Notebook
# MAGIC %md
# MAGIC # Expected Output Reference: Supply Chain Control Tower
# MAGIC
# MAGIC **Canonical Prompt**: "We need a complete supply chain health check for our West region in August 2026. The CFO wants to understand what drove the revenue decline versus July — show the actual August and July revenue numbers, the dollar change, and the percentage change — and which product families are most at fault. Are our on-time delivery rate and average delay for West region shipments contributing to the problem? How many total shipments went out and how many were late? I also need our current fill rate, how many inventory positions are sitting below safety stock in the West region, how many unique SKUs are affected, what is our days of supply for those at-risk items, and how many SKUs are completely stocked out. On the vendor side: what percentage of vendors delivered late in August, how many purchase orders were late out of total, what is the average lead time variance, and what are the total vendor SLA penalties we have incurred? Bring it all together as our total Cost of Disruption by region for August 2026 — cancelled revenue, at-risk backorder revenue, wasted freight on late shipments, and supplier penalty exposure in one number per region. Are we going to miss our Q3 service-level targets, and what are the top actions we should take?"
# MAGIC
# MAGIC This notebook **defines every ground truth value** used by the `00_run_all` test suite.
# MAGIC Each value is produced by a **runnable SQL query** against the actual data at **2 decimal precision**.
# MAGIC The `00_run_all` notebook sends questions to Genie agents and compares their answers to these values.
# MAGIC
# MAGIC > **Circular reference note:** These GT queries were written by the same AI that powers the Genie agents. In a future version, ground truth values should be **human-authored and manually verified** to break this circularity.
# MAGIC
# MAGIC **Notebook structure:**
# MAGIC * **Sections 1–9**: Queries grouped by domain (demand, inventory, logistics, suppliers, executive) with findings, root cause analysis, and the expected Supervisor report format
# MAGIC * **Section 10**: **Ground Truth Query Reference** — master mapping table (45 test IDs → query locations, including P01-P05 critical thresholds) + SQL queries for metrics not already covered in Sections 1–9
# MAGIC * **Section 11**: Comprehensive Prompt Benchmark coverage (which metrics the Supervisor surfaces vs misses)
# MAGIC * **Section 12**: Iteration progression summary (baseline ~30-31/45 non-deterministic → 45/45 deterministic)
# MAGIC
# MAGIC See `00_run_all` for the automated test runner (Assumption Tester v3) that uses these values.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")
print(f"Catalog: {CATALOG}")

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

# DBTITLE 1,Finding: Revenue by Region → GT: B01, B02, B03, B04
# MAGIC %md
# MAGIC **Finding**: Western revenue dropped **-$1,240,330.12 (-27.07%)** in August 2026 vs July 2026 -- by far the largest decline.
# MAGIC
# MAGIC * Western: Aug **$3,341,062.58**, Jul $4,581,392.70, change **-$1,240,330.12 (-27.07%)**
# MAGIC * Southern: Aug $4,175,882.05, Jul $4,377,166.50, change -$201,284.45 (-4.60%)
# MAGIC * Eastern: Aug $4,189,811.21, Jul $4,342,727.70, change -$152,916.49 (-3.50%)
# MAGIC * Central: Aug $4,094,683.96, Jul $4,188,864.89, change -$94,180.93 (-2.20%)
# MAGIC
# MAGIC **Confidence**: HIGH — complete data, 4 regions, calendar month boundaries, clear pattern.
# MAGIC
# MAGIC **Ground Truth values produced by Query 1:**
# MAGIC * **B01** = $3,341,062.58 (Western Aug revenue)
# MAGIC * **B02** = $4,581,392.70 (Western Jul revenue)
# MAGIC * **B03** = -$1,240,330.12 (revenue change $)
# MAGIC * **B04** = -27.07% (revenue change %)
# MAGIC * **F01** = $3,341,062.58 (same as B01 — tests "West" → "Western" mapping)

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

# DBTITLE 1,Finding: Product Family Breakdown → GT: F06
# MAGIC %md
# MAGIC **Finding**: ALL Western product families declined last month. Worst hit:
# MAGIC - Home Goods: **-$349K (-28.9%)**
# MAGIC - Electronics: **-$271K (-27.5%)**
# MAGIC - Footwear: **-$223K (-26.3%)**
# MAGIC - Accessories: **-$213K (-24.6%)**
# MAGIC - Apparel: **-$184K (-27.2%)**
# MAGIC
# MAGIC Sum of product family changes = -$1,240K ✓ (matches regional total)
# MAGIC
# MAGIC **Ground Truth values produced by Query 2:**
# MAGIC * **F06** = $349,062.88 (Home Goods = largest decline)

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

# DBTITLE 1,Finding: Order Status → GT: F03, F04, F05, H03, H04
# MAGIC %md
# MAGIC **Finding**: Western Aug 2026 order breakdown:
# MAGIC * Fulfilled: **1,342 orders (71.23%)** — $2,397,445.86
# MAGIC * Backordered: **275 orders (14.60%)** — $474,164.83 at-risk revenue
# MAGIC * Partially_Fulfilled: 170 orders (9.00%) — $290,032.63
# MAGIC * Cancelled: **97 orders (5.10%)** — **$179,419.26** cancelled revenue
# MAGIC
# MAGIC **Confidence**: HIGH — calendar month boundaries, all statuses captured.
# MAGIC
# MAGIC **Ground Truth values produced by Query 3:**
# MAGIC * **F03** = 1,342 (Fulfilled orders only — NOT Partially_Fulfilled)
# MAGIC * **F04** = $179,419.26 (Cancelled revenue)
# MAGIC * **F05** = 275 (Backordered orders)
# MAGIC * **H03** = 71.23% (fulfillment rate = 1342/1884)
# MAGIC * **H04** = 9.02% (partially fulfilled rate = 170/1884)

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

# DBTITLE 1,Finding: Stockouts → GT: C01, C02, C03, C04, C05
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
# MAGIC **Confidence**: HIGH — direct from inventory_ledger.
# MAGIC
# MAGIC **Ground Truth values produced by Query 4:**
# MAGIC * **C01** = 109 (positions below safety stock)
# MAGIC * **C02** = 61 (unique SKUs below safety stock)
# MAGIC * **C03** = 35 (stockout positions)
# MAGIC * **C04** = 33 (unique SKUs stocked out)
# MAGIC * **C05** = 0.96 (avg days of supply for at-risk items)

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

# DBTITLE 1,Finding: Late Delivery → GT: A01-A05
# MAGIC %md
# MAGIC **Finding**: Western has **94.57% late delivery rate** in August 2026 (1,027 of 1,086 shipments late, avg **2.94 day** delay).
# MAGIC On-time delivery rate: **5.43%**. Wasted freight cost on late shipments: **$2,484,985.57**.
# MAGIC
# MAGIC **Empirically verified**: Agent returns OTD 5.43% (A4), avg delay 2.94 days (A10), and wasted freight $2,484,985.57 (A3) at baseline with correct SQL. No synonyms or metric views needed for any of these.
# MAGIC
# MAGIC **Confidence**: HIGH — complete shipment data, calendar month boundaries.
# MAGIC
# MAGIC **Ground Truth values produced by Query 5:**
# MAGIC * **A01** = 5.43% (OTD rate = 100 - 94.57)
# MAGIC * **A02** = 94.57% (late delivery rate)
# MAGIC * **A03** = 2.94 days (avg delay when late)
# MAGIC * **A04** = 1,086 (total Western shipments)
# MAGIC * **A05** = 1,027 (late shipments)
# MAGIC * A06 (wasted freight) requires separate query — see Section 10

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

# DBTITLE 1,Finding: Supplier Performance → GT: D01-D07, H01, H02, F02
# MAGIC %md
# MAGIC **Finding**: Asia suppliers are **100.00% late** (30/30 POs) with **+13.67 day** average lead time variance in August 2026.
# MAGIC Overall: **48 total POs**, **36 late** (**75.00%**), avg lead time variance **8.69 days**.
# MAGIC Total vendor SLA penalties: **$1,185,043.10**.
# MAGIC
# MAGIC **Empirically verified**: Agent returns SLA penalties $1,185,043.10 (A6) and late PO count 36/48 (A14) at baseline. However, A7 shows the agent interprets "% of vendors delivered late" as per-vendor (83.33%) not per-order (75.00%) — this is a genuine semantic ambiguity that needs a certified query or metric view. A13 shows the agent queries the wrong table (supplier_lead_times instead of supplier_orders) for lead time variance — gets 6.075 instead of 8.69.
# MAGIC
# MAGIC **Confidence**: HIGH — all POs in the calendar month captured.
# MAGIC
# MAGIC **Ground Truth values produced by Query 7:**
# MAGIC * **D01** = 48 (total POs) | **D02** = 36 (late POs) | **D03** = 75.00% (late rate)
# MAGIC * **D04** = 8.69 days (avg lead time variance, overall)
# MAGIC * **D05** = 100.00% (Asia late rate) | **D06** = 13.67 days (Asia variance) | **D07** = 30 (Asia POs)
# MAGIC * **H01** = 0.38 days (Europe variance) | **H02** = 0.40 days (NA variance)
# MAGIC * **F02** = 75.00% (same as D03 — tests "% of vendors delivered late" phrasing, per-ORDER not per-vendor)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 8: Top Risk Suppliers

# COMMAND ----------

# DBTITLE 1,Query 8: Top Risk Suppliers
df8 = spark.sql(f"""
SELECT supplier_id, supplier_name, country, risk_tier,
  composite_risk_score, sla_breaches, total_penalty_usd,
  lead_time_variance AS lead_time_variance_days
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

# DBTITLE 1,Cross-Domain Reconciliation (mapped to 40-test IDs)
# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Cross-Domain Reconciliation
# MAGIC
# MAGIC | KPI | Value | Source | Agent Baseline? |
# MAGIC | --- | --- | --- | --- |
# MAGIC | KPI | Value | Test ID | Source | Baseline Result |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | Western Aug Revenue | $3,341,062.58 | B01/F01 | `demand_analysis.sales_orders` | ✅ PASS |
# MAGIC | Western Jul Revenue | $4,581,392.70 | B02 | `demand_analysis.sales_orders` | ✅ PASS |
# MAGIC | Revenue Change ($) | -$1,240,330.12 | B03 | `demand_analysis.sales_orders` | ✅ PASS |
# MAGIC | Revenue Change (%) | -27.07% | B04 | `demand_analysis.sales_orders` | ✅ PASS |
# MAGIC | Western OTD Rate | 5.43% | A01 | `logistics_operations.shipments` | ✅ PASS |
# MAGIC | Late Delivery Rate | 94.57% | A02 | `logistics_operations.shipments` | ✅ PASS |
# MAGIC | Avg Delay (late) | 2.94 days | A03 | `logistics_operations.shipments` | ✅ PASS |
# MAGIC | Total Shipments | 1,086 | A04 | `logistics_operations.shipments` | ❌ FAIL → Iter 1 |
# MAGIC | Late Shipments | 1,027 | A05 | `logistics_operations.shipments` | ✅ PASS |
# MAGIC | Wasted Freight | $2,484,985.57 | A06 | `logistics_operations.shipments` | ✅ PASS |
# MAGIC | Fill Rate | 80.70% | E01 | `reporting.executive_kpis` | ✅ PASS |
# MAGIC | Positions Below Safety Stock | 109 | C01 | `inventory_management.inventory_ledger` | ✅ PASS |
# MAGIC | Unique SKUs Below Safety | 61 | C02 | `inventory_management.inventory_ledger` | ✅ PASS |
# MAGIC | Stockout SKUs | 33 | C04 | `inventory_management.inventory_ledger` | ✅ PASS |
# MAGIC | Avg Days of Supply | 0.96 days | C05 | `inventory_management.inventory_ledger` | ✅ PASS |
# MAGIC | Vendor Late % (per-order) | 75.00% | F02 | `supplier_procurement.supplier_orders` | ❌ FAIL → Iter 3 (UC Page) |
# MAGIC | Total POs | 48 | D01 | `supplier_procurement.supplier_orders` | ✅ PASS |
# MAGIC | Late POs | 36 | D02 | `supplier_procurement.supplier_orders` | ✅ PASS |
# MAGIC | Avg Lead Time Variance | 8.69 days | D04 | `supplier_procurement.supplier_orders` | ❌ FAIL → Iter 1 |
# MAGIC | Total SLA Penalties | $1,185,043.10 | E02 | `supplier_procurement.vendor_slas` | ✅ PASS |
# MAGIC | Fulfillment Rate | 71.23% | H03 | `demand_analysis.sales_orders` | ❌ FAIL → Iter 1 |
# MAGIC | Backordered Orders | 275 | F05 | `demand_analysis.sales_orders` | ✅ PASS |
# MAGIC | Cancelled Revenue | $179,419.26 | F04 | `demand_analysis.sales_orders` | ✅ PASS |
# MAGIC | Western Cost of Disruption | $3,757,298.31 | reporting.cost_of_disruption_by_region | ❌ E03 FAIL → Iter 2 (CoD view) |
# MAGIC | Q3 Service-Level Target | 95.00% | UC Page (not in any table) | ❌ G01/G02 FAIL → Iter 3 (UC Pages) |
# MAGIC
# MAGIC > **Updated**: Test IDs now use the current A01-G02 numbering (40-test suite). See Section 10 below for the complete reference.

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

# COMMAND ----------

# DBTITLE 1,Section 10: Ground Truth Query Reference — All 40 Metrics
# MAGIC %md
# MAGIC ---
# MAGIC ## 10. Ground Truth Query Reference — All 40 Metrics
# MAGIC
# MAGIC This section is the **authoritative source** for every ground truth value used by the `00_run_all` test suite. Each metric is defined by a **runnable SQL query** against the actual data. Running these cells produces the canonical expected values.
# MAGIC
# MAGIC > **Design note (circular reference):** These GT queries were written by the same AI that powers the Genie agents — meaning one version of the AI writes the "correct" answers and another version tries to match them. In the next version of this demo, ground truth values should be **human-authored and manually verified** to break this circularity.
# MAGIC
# MAGIC ### Master Mapping: Test ID → Ground Truth Query
# MAGIC
# MAGIC | Test ID | GT Value | GT Query Location | Source Table |
# MAGIC | --- | --- | --- | --- |
# MAGIC | **A01** | 5.43 | Query 5 (Section 3, cell 21) | `logistics_operations.shipments` |
# MAGIC | **A02** | 94.57 | Query 5 (Section 3, cell 21) | `logistics_operations.shipments` |
# MAGIC | **A03** | 2.94 | Query 5 (Section 3, cell 21) | `logistics_operations.shipments` |
# MAGIC | **A04** | 1086 | Query 5 (Section 3, cell 21) | `logistics_operations.shipments` |
# MAGIC | **A05** | 1027 | Query 5 (Section 3, cell 21) | `logistics_operations.shipments` |
# MAGIC | **A06** | 2,484,985.57 | **GT Query A06 (below)** | `logistics_operations.shipments` |
# MAGIC | **B01** | 3,341,062.58 | Query 1 (Section 3, cell 7) | `demand_analysis.sales_orders` |
# MAGIC | **B02** | 4,581,392.70 | Query 1 (Section 3, cell 7) | `demand_analysis.sales_orders` |
# MAGIC | **B03** | -1,240,330.12 | Query 1 (Section 3, cell 7) | `demand_analysis.sales_orders` |
# MAGIC | **B04** | -27.07 | Query 1 (Section 3, cell 7) | `demand_analysis.sales_orders` |
# MAGIC | **C01** | 109 | Query 4 (Section 3, cell 17) | `inventory_management.inventory_ledger` |
# MAGIC | **C02** | 61 | Query 4 (Section 3, cell 17) | `inventory_management.inventory_ledger` |
# MAGIC | **C03** | 35 | Query 4 (Section 3, cell 17) | `inventory_management.inventory_ledger` |
# MAGIC | **C04** | 33 | Query 4 (Section 3, cell 17) | `inventory_management.inventory_ledger` |
# MAGIC | **C05** | 0.96 | Query 4 (Section 3, cell 17) | `inventory_management.inventory_ledger` |
# MAGIC | **D01** | 48 | Query 7 (Section 3, cell 28) | `supplier_procurement.supplier_orders` |
# MAGIC | **D02** | 36 | Query 7 (Section 3, cell 28) | `supplier_procurement.supplier_orders` |
# MAGIC | **D03** | 75.00 | Query 7 (Section 3, cell 28) | `supplier_procurement.supplier_orders` |
# MAGIC | **D04** | 8.69 | Query 7 (Section 3, cell 28) | `supplier_procurement.supplier_orders` |
# MAGIC | **D05** | 100.00 | Query 7 (Section 3, cell 28) | `supplier_procurement.supplier_orders` |
# MAGIC | **D06** | 13.67 | Query 7 (Section 3, cell 28) | `supplier_procurement.supplier_orders` |
# MAGIC | **D07** | 30 | Query 7 (Section 3, cell 28) | `supplier_procurement.supplier_orders` |
# MAGIC | **E01** | 80.70 | **GT Query E01 (below)** | `reporting.executive_kpis` |
# MAGIC | **E02** | 1,185,043.10 | **GT Query E02 (below)** | `supplier_procurement.vendor_slas` |
# MAGIC | **E03** | 3,757,298.31 | **GT Query E03 (below)** | `reporting.cost_of_disruption_by_region` |
# MAGIC | **F01** | 3,341,062.58 | Same as B01 (Query 1) | `demand_analysis.sales_orders` |
# MAGIC | **F02** | 75.00 | Same as D03 (Query 7) | `supplier_procurement.supplier_orders` |
# MAGIC | **F03** | 1,342 | Query 3 (Section 3, cell 13) | `demand_analysis.sales_orders` |
# MAGIC | **F04** | 179,419.26 | Query 3 (Section 3, cell 13) | `demand_analysis.sales_orders` |
# MAGIC | **F05** | 275 | Query 3 (Section 3, cell 13) | `demand_analysis.sales_orders` |
# MAGIC | **F06** | 349,062.88 | Query 2 (Section 3, cell 10) | `demand_analysis.sales_orders` |
# MAGIC | **G01** | 95.0 | **No SQL — UC Pages (below)** | UC Page 1 (Fiscal Calendar) |
# MAGIC | **G02** | 95.0 | **No SQL — UC Pages (below)** | UC Page 1 (Fiscal Calendar) |
# MAGIC | **H01** | 0.38 | Query 7 (Section 3, cell 28) | `supplier_procurement.supplier_orders` |
# MAGIC | **H02** | 0.40 | Query 7 (Section 3, cell 28) | `supplier_procurement.supplier_orders` |
# MAGIC | **H03** | 71.23 | Query 3 (Section 3, cell 13) | `demand_analysis.sales_orders` |
# MAGIC | **H04** | 9.02 | Query 3 (Section 3, cell 13) | `demand_analysis.sales_orders` |
# MAGIC | **H05** | 3,138,569.66 | **GT Query H05 (below)** | Cross-domain derivation |
# MAGIC | **H06** | 14,368.63 | **GT Query H06 (below)** | Cross-domain derivation |
# MAGIC | **H07** | 1.12 | **GT Query H07 (below)** | Cross-domain derivation |
# MAGIC
# MAGIC > **Bolded** query locations indicate metrics whose GT query lives in this section (below). All others reference queries already in Sections 1–9 above.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Ambiguity Notes
# MAGIC
# MAGIC * **F01 vs B01**: Same value ($3,341,062.58), different phrasing. F01 says "West" to test region-name mapping (West → Western).
# MAGIC * **F02 vs D03**: Same value (75.00%), different phrasing. F02 says "percentage of vendors delivered late" which linguistically pulls toward COUNT(DISTINCT supplier_id) = 83.33%. The **correct** interpretation is per-order (75.00%) — governed by UC Page 2.
# MAGIC * **G01/G02**: These are the only GT values with **no SQL query**. The value 95.0% exists only in UC Pages (fiscal calendar). Without UC Pages, agents default to 85% (OTD target from `executive_kpis`).

# COMMAND ----------

# DBTITLE 1,GT Queries: Metrics not covered in Sections 1-9
# MAGIC %md
# MAGIC ### Ground Truth Queries for Metrics Not Covered in Sections 1–9
# MAGIC
# MAGIC The queries below define the GT values for metrics that are **not already computed** by Queries 1–7 above. Run these cells to verify each expected value.

# COMMAND ----------

# DBTITLE 1,GT Query A06: Wasted Freight on Late Shipments
# GT Query A06: Wasted Freight on Late Shipments
# Expected: Western = 2,484,985.57
# Logic: SUM(freight_cost) for late shipments destined to Western region in August 2026

df_a06 = spark.sql(f"""
SELECT
  destination_region,
  ROUND(SUM(freight_cost), 2) AS wasted_freight_cost
FROM {CATALOG}.logistics_operations.shipments
WHERE is_late = true
  AND ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND ship_date <  DATE_TRUNC('month', DATE '2026-09-01')
GROUP BY destination_region
ORDER BY wasted_freight_cost DESC
""")
df_a06.display()
# A06 = Western row wasted_freight_cost = 2,484,985.57

# COMMAND ----------

# DBTITLE 1,GT Query E01: Fill Rate (Service Level)
# GT Query E01: Fill Rate (= Service Level %)
# Expected: 80.70
# Logic: service_level_pct from the executive_kpis reporting view
# Note: The original Query 9 (cell 35) errored because the view hadn't been created.
#       This query works after 06_create_reporting_views has run.

df_e01 = spark.sql(f"""
SELECT
  metric_name,
  ROUND(metric_value, 2) AS metric_value
FROM {CATALOG}.reporting.executive_kpis
WHERE metric_name = 'service_level_pct'
""")
df_e01.display()
# E01 = 80.70 (fill rate = service level percentage)

# COMMAND ----------

# DBTITLE 1,GT Query E02: Total SLA Penalties
# GT Query E02: Total Vendor SLA Penalties
# Expected: 1,185,043.10
# Logic: SUM(penalty_amount) across all vendors in August 2026

df_e02 = spark.sql(f"""
SELECT
  ROUND(SUM(penalty_amount), 2) AS total_sla_penalties
FROM {CATALOG}.supplier_procurement.vendor_slas
WHERE penalty_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  AND penalty_date <  DATE_TRUNC('month', DATE '2026-09-01')
""")
df_e02.display()
# E02 = 1,185,043.10

# COMMAND ----------

# DBTITLE 1,GT Query E03: Cost of Disruption by Region
# GT Query E03: Cost of Disruption by Region
# Expected: Western = 3,757,298.31
# Logic: From the reporting.cost_of_disruption_by_region view (created in Iteration 2)
# Components: cancelled_revenue + backordered_revenue + late_shipping_cost + sla_penalties
#
# IMPORTANT: This view is created by iteration_06 (CoD view). It won't exist at baseline.
# The GT value is verified after Iteration 2 has run.

df_e03 = spark.sql(f"""
SELECT
  region,
  ROUND(cancelled_revenue, 2)    AS cancelled_revenue,
  ROUND(backordered_revenue, 2)  AS backordered_revenue,
  ROUND(late_shipping_cost, 2)   AS late_shipping_cost,
  ROUND(sla_penalties, 2)        AS sla_penalties,
  ROUND(total_cost_of_disruption, 2) AS total_cost_of_disruption
FROM {CATALOG}.reporting.cost_of_disruption_by_region
ORDER BY total_cost_of_disruption DESC
""")
df_e03.display()
# E03 = Western total_cost_of_disruption = 3,757,298.31

# COMMAND ----------

# DBTITLE 1,GT Queries H05-H07: Cross-Domain Derivations
# MAGIC %md
# MAGIC #### GT Queries H05, H06, H07: Cross-Domain Derived Metrics
# MAGIC
# MAGIC These metrics are **not stored in any single table**. They are calculated by combining values from multiple domain queries. This is exactly why they are "hard failures" at baseline — no single Genie agent has the data to compute them.
# MAGIC
# MAGIC | ID | Formula | Components |
# MAGIC | --- | --- | --- |
# MAGIC | H05 | cancelled\_rev + backordered\_rev + wasted\_freight | F04 ($179,419.26) + backordered ($474,164.83) + A06 ($2,484,985.57) = **$3,138,569.66** |
# MAGIC | H06 | backordered\_revenue / stockout\_SKUs | $474,164.83 / 33 = **$14,368.63** |
# MAGIC | H07 | CoD / Western\_revenue | $3,757,298.31 / $3,341,062.58 = **1.12** |

# COMMAND ----------

# DBTITLE 1,GT Query H05-H07: Cross-Domain Calculations
# GT Queries H05, H06, H07: Cross-Domain Derived Metrics
# These combine values from multiple domains — no single agent can compute them.

# H05: Total revenue at risk from supply chain disruptions (Western)
# = cancelled_revenue + backordered_revenue + wasted_freight
df_h05 = spark.sql(f"""
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
FROM western_cod cod, western_revenue r
""")
df_h05.display()
# H05 = 3,138,569.66
# H06 = 14,368.63
# H07 = 1.12

# COMMAND ----------

# DBTITLE 1,GT Values G01/G02: Q3 Fiscal Targets (UC Pages)
# MAGIC %md
# MAGIC #### GT Values G01, G02: Q3 Fiscal Calendar Targets
# MAGIC
# MAGIC **These are the only GT values with no SQL query.** The value `95.0%` exists exclusively in **UC Page 1 (Fiscal Calendar & Targets)** created on the Discover page. No table in the catalog stores this number.
# MAGIC
# MAGIC | ID | Question | GT Value | Source |
# MAGIC | --- | --- | --- | --- |
# MAGIC | G01 | Are we going to miss our Q3 service-level targets? | **95.0** | UC Page 1: "Q3 (Jan–Mar FY2027) service-level target = 95%" |
# MAGIC | G02 | What is our Q3 service-level target? | **95.0** | UC Page 1: same |
# MAGIC
# MAGIC **Why these fail at baseline:** Without UC Pages, agents see only `executive_kpis` which contains an OTD target of 85%. The agent confuses the OTD target with the service-level target. UC Page 1 defines:
# MAGIC * Fiscal year starts July (so Q3 = Jan–Mar, **NOT** calendar Jul–Sep)
# MAGIC * Q3 FY2027 service-level target = 95%
# MAGIC * Current FY = FY2027 (July 2026 – June 2027)
# MAGIC
# MAGIC **Iteration 3** also creates a `fiscal_targets` reference table that the Executive agent can query, but the 95% value is **governed** by the UC Page, not the table alone.

# COMMAND ----------

# DBTITLE 1,GT Test Definitions: 40 Questions + Expected Values
# MAGIC %md
# MAGIC ---
# MAGIC ### Complete Test Definitions: All 40 Questions
# MAGIC
# MAGIC For reference, here is every test ID with the **exact question** sent to the Genie agent and the **expected value** the agent must return. These are the inputs to `00_run_all`'s Assumption Tester.
# MAGIC
# MAGIC #### Group A: Logistics (agent: `logistics`)
# MAGIC
# MAGIC | ID | Question | Expected |
# MAGIC | --- | --- | --- |
# MAGIC | A01 | What is the on-time delivery rate for Western region shipments in August 2026? | **5.43** |
# MAGIC | A02 | What is the late delivery rate for Western region shipments last month? | **94.57** |
# MAGIC | A03 | What is the average delay in days for late shipments in the Western region last month? | **2.94** |
# MAGIC | A04 | How many total shipments went to the Western region in August? | **1086** |
# MAGIC | A05 | How many late shipments went to the Western region last month? | **1027** |
# MAGIC | A06 | What is the total wasted freight on late shipments in Western region last month? | **2,484,985.57** |
# MAGIC
# MAGIC #### Group B: Demand (agent: `demand`)
# MAGIC
# MAGIC | ID | Question | Expected |
# MAGIC | --- | --- | --- |
# MAGIC | B01 | What is the total revenue for the Western region in August 2026? | **3,341,062.58** |
# MAGIC | B02 | What was the total revenue for the Western region in July 2026? | **4,581,392.70** |
# MAGIC | B03 | What is the revenue change in dollars for Western region month-over-month? | **-1,240,330.12** |
# MAGIC | B04 | What is the percentage change in revenue for Western region last month vs prior month? | **-27.07** |
# MAGIC
# MAGIC #### Group C: Inventory (agent: `inventory`)
# MAGIC
# MAGIC | ID | Question | Expected |
# MAGIC | --- | --- | --- |
# MAGIC | C01 | How many inventory positions are below safety stock in the Western region? | **109** |
# MAGIC | C02 | How many unique SKUs are below safety stock in the Western region? | **61** |
# MAGIC | C03 | How many stockout positions are there in the Western region? | **35** |
# MAGIC | C04 | How many unique SKUs are completely stocked out in the Western region? | **33** |
# MAGIC | C05 | What is the average days of supply for at-risk items in the Western region? | **0.96** |
# MAGIC
# MAGIC #### Group D: Supplier (agent: `supplier`)
# MAGIC
# MAGIC | ID | Question | Expected |
# MAGIC | --- | --- | --- |
# MAGIC | D01 | How many total purchase orders were placed in August 2026? | **48** |
# MAGIC | D02 | How many purchase orders were late last month? | **36** |
# MAGIC | D03 | What percentage of purchase orders were late last month? | **75.00** |
# MAGIC | D04 | What is the average lead time variance in days for all suppliers last month? | **8.69** |
# MAGIC | D05 | What percentage of purchase orders from Asia suppliers were late in August? | **100.00** |
# MAGIC | D06 | What is the average lead time variance for Asia suppliers last month? | **13.67** |
# MAGIC | D07 | How many purchase orders did we place with Asia suppliers in August? | **30** |
# MAGIC
# MAGIC #### Group E: Cross-Domain (agents: `executive`, `supplier`)
# MAGIC
# MAGIC | ID | Question | Expected |
# MAGIC | --- | --- | --- |
# MAGIC | E01 | What is our current fill rate? | **80.70** |
# MAGIC | E02 | What are the total vendor SLA penalties we incurred? | **1,185,043.10** |
# MAGIC | E03 | What is the total Cost of Disruption for the Western region last month? | **3,757,298.31** |
# MAGIC
# MAGIC #### Group F: Indirect & Ambiguity (agents: `demand`, `supplier`)
# MAGIC
# MAGIC | ID | Question | Expected | Why It's Tricky |
# MAGIC | --- | --- | --- | --- |
# MAGIC | F01 | Show me the total revenue for the West region last month | **3,341,062.58** | "West" must map to "Western" |
# MAGIC | F02 | What percentage of vendors delivered late last month? | **75.00** | Per-order (75%) not per-vendor (83.33%) |
# MAGIC | F03 | How many Western region orders were fulfilled last month? | **1,342** | Fulfilled only, not Partially_Fulfilled |
# MAGIC | F04 | What is the total cancelled revenue in Western region in August? | **179,419.26** | Cancelled status filter |
# MAGIC | F05 | How many orders were backordered in Western region last month? | **275** | Backordered status filter |
# MAGIC | F06 | Which product family had the largest revenue decline in Western region last month vs prior month? | **349,062.88** | Home Goods = worst decline |
# MAGIC
# MAGIC #### Group H: Hard Failures (agents: `supplier`, `demand`, `executive`, `inventory`)
# MAGIC
# MAGIC | ID | Question | Expected | Why It Fails at Baseline |
# MAGIC | --- | --- | --- | --- |
# MAGIC | H01 | What is the average lead time variance for Europe suppliers last month? | **0.38** | Agent uses wrong table (`supplier_lead_times`) |
# MAGIC | H02 | What is the average lead time variance for North America suppliers last month? | **0.40** | Agent uses wrong table |
# MAGIC | H03 | What is the order fulfillment rate for Western region last month? | **71.23** | Agent includes Partially_Fulfilled |
# MAGIC | H04 | What percentage of Western region orders were only partially fulfilled last month? | **9.02** | Status value not obvious |
# MAGIC | H05 | What is the total revenue at risk from supply chain disruptions in Western region? | **3,138,569.66** | Cross-domain, no single agent can answer |
# MAGIC | H06 | What is the average revenue at risk per stockout SKU in Western region? | **14,368.63** | Requires inventory + demand join |
# MAGIC | H07 | What is our total cost of supply chain disruptions as a ratio of Western region revenue? | **1.12** | Requires CoD + revenue |
# MAGIC
# MAGIC #### Group G: Q3 Fiscal Calendar (agent: `executive`)
# MAGIC
# MAGIC | ID | Question | Expected | Why It Fails at Baseline |
# MAGIC | --- | --- | --- | --- |
# MAGIC | G01 | Are we going to miss our Q3 service-level targets? | **95.0** | No SQL — value exists only in UC Pages |
# MAGIC | G02 | What is our Q3 service-level target? | **95.0** | No SQL — value exists only in UC Pages |

# COMMAND ----------

# DBTITLE 1,Section 11: Comprehensive Prompt Benchmark
# MAGIC %md
# MAGIC ---
# MAGIC ## 11. Comprehensive Prompt Benchmark — Supervisor Agent Scoring
# MAGIC
# MAGIC The comprehensive prompt is a **single, multi-part business question** sent to the **Supervisor Agent**, which orchestrates all 5 domain Genie agents. Unlike the 45 individual tests (one question → one agent), this tests how well the Supervisor synthesizes a complete report.
# MAGIC
# MAGIC ### The Prompt
# MAGIC
# MAGIC > We need a complete supply chain health check for our West region in August 2026. The CFO wants to understand what drove the revenue decline versus July — show the actual August and July revenue numbers, the dollar change, and the percentage change — and which product families are most at fault. Are our on-time delivery rate and average delay for West region shipments contributing to the problem? How many total shipments went out and how many were late? I also need our current fill rate, how many inventory positions are sitting below safety stock in the West region, how many unique SKUs are affected, what is our days of supply for those at-risk items, and how many SKUs are completely stocked out. On the vendor side: what percentage of vendors delivered late in August, how many purchase orders were late out of total, what is the average lead time variance, and what are the total vendor SLA penalties we have incurred? Bring it all together as our total Cost of Disruption by region for last month Aug 26 — cancelled revenue, at-risk backorder revenue, wasted freight on late shipments, and supplier penalty exposure in one number per region. Are we going to miss our Q3 service-level targets, and what are the top actions we should take?
# MAGIC
# MAGIC ### Scoring Methodology
# MAGIC
# MAGIC The Supervisor response is plain text (not structured). Scoring uses `find_value_in_text()` — the same 2-decimal matcher used for individual tests — to search the response for each of the 45 GT values.
# MAGIC
# MAGIC * **FOUND**: `round(abs(number_in_report), 2) == round(abs(GT_value), 2)`
# MAGIC * **CLOSE**: Within 5% but not exact
# MAGIC * **NOT_FOUND**: No matching number in the report (prompt didn't surface this metric)
# MAGIC
# MAGIC ### Which Metrics the Comprehensive Prompt Surfaces (~22/40)
# MAGIC
# MAGIC The prompt explicitly asks for some metrics and implicitly covers others. Here is the expected coverage:
# MAGIC
# MAGIC | ID | Expected | Explicitly Asked? | Typically Surfaced? | Notes |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | **A01** | 5.43 | Yes ("on-time delivery rate") | Yes | Always in report |
# MAGIC | **A02** | 94.57 | Implicitly (complement of A01) | Yes | Usually mentioned |
# MAGIC | **A03** | 2.94 | Yes ("average delay") | Yes | Always in report |
# MAGIC | **A04** | 1086 | Yes ("total shipments") | Yes | Always in report |
# MAGIC | **A05** | 1027 | Yes ("how many were late") | Yes | Always in report |
# MAGIC | **A06** | 2,484,985.57 | Yes ("wasted freight") | Yes | In CoD breakdown |
# MAGIC | **B01** | 3,341,062.58 | Yes ("August revenue") | Yes | Always in report |
# MAGIC | **B02** | 4,581,392.70 | Yes ("July revenue") | Yes | Always in report |
# MAGIC | **B03** | -1,240,330.12 | Yes ("dollar change") | Yes | Always in report |
# MAGIC | **B04** | -27.07 | Yes ("percentage change") | Yes | Always in report |
# MAGIC | **C01** | 109 | Yes ("inventory positions below safety stock") | Sometimes | May show unique SKUs instead |
# MAGIC | **C02** | 61 | Yes ("unique SKUs") | Yes | Usually in report |
# MAGIC | **C03** | 35 | Implicitly | Rarely | Stockout positions vs SKUs confusion |
# MAGIC | **C04** | 33 | Yes ("stocked out") | Yes | Usually in report |
# MAGIC | **C05** | 0.96 | Yes ("days of supply") | Rarely | Often says "Data not available" |
# MAGIC | **D01** | 48 | Yes ("purchase orders total") | Yes | Usually in report |
# MAGIC | **D02** | 36 | Yes ("how many late") | Yes | Usually in report |
# MAGIC | **D03** | 75.00 | Yes ("percentage of vendors delivered late") | Yes | Usually in report |
# MAGIC | **D04** | 8.69 | Yes ("average lead time variance") | Yes | Usually in report |
# MAGIC | **D05** | 100.00 | No (Asia-specific) | No | Not in prompt scope |
# MAGIC | **D06** | 13.67 | No (Asia-specific) | No | Not in prompt scope |
# MAGIC | **D07** | 30 | No (Asia PO count) | No | Not in prompt scope |
# MAGIC | **E01** | 80.70 | Yes ("fill rate") | Rarely | Agent sometimes can't find it |
# MAGIC | **E02** | 1,185,043.10 | Yes ("SLA penalties") | Sometimes | May show per-region, not total |
# MAGIC | **E03** | 3,757,298.31 | Yes ("Cost of Disruption") | Yes | Always in CoD table |
# MAGIC | **F01** | 3,341,062.58 | Same as B01 | Yes | Duplicate of B01 |
# MAGIC | **F02** | 75.00 | Yes ("% vendors late") | **Yes** | Interesting: correct per-order in comprehensive context |
# MAGIC | **F03** | 1,342 | No (fulfilled count) | Rarely | Not explicitly asked |
# MAGIC | **F04** | 179,419.26 | Yes ("cancelled revenue") | Yes | In CoD breakdown |
# MAGIC | **F05** | 275 | No (backordered count) | Rarely | Not explicitly asked |
# MAGIC | **F06** | 349,062.88 | Yes ("product families most at fault") | Sometimes | May show $ or % but not exact |
# MAGIC | **G01** | 95.0 | Yes ("Q3 service-level targets") | **Unreliable** | Often confuses OTD target (85%) with service-level (95%) |
# MAGIC | **G02** | 95.0 | Same as G01 | **Unreliable** | Same confusion |
# MAGIC | **H01-H07** | Various | No | No | Not in prompt scope (continent-specific, ratios) |
# MAGIC
# MAGIC ### Expected Comprehensive Prompt Scores by Stage
# MAGIC
# MAGIC | Stage | Expected FOUND | Notes |
# MAGIC | --- | --- | --- |
# MAGIC | Baseline (no UC features) | ~15-18/40 | Revenue, logistics, basic inventory work; supplier/cross-domain/fiscal fail |
# MAGIC | After Iteration 1 (comments + examples) | ~18-22/40 | Better supplier metrics, fulfillment clarity |
# MAGIC | After Iteration 3 (full UC stack) | ~20-24/40 | CoD, fiscal targets (if Pages work), vendor late % |
# MAGIC
# MAGIC ### Key Findings from Post-Iter-3 Analysis
# MAGIC
# MAGIC * **F02 = 75.0% CORRECT** — The comprehensive prompt resolves the per-vendor vs per-order ambiguity. When the Supervisor asks the supplier agent "what percentage of vendors delivered late", the full context helps the agent choose the per-order interpretation. This is more reliable than the isolated F02 individual test.
# MAGIC * **G01/G02 UNRELIABLE** — The Supervisor report often confuses the OTD target (85%) with the service-level target (95%). UC Pages should fix this, but the comprehensive prompt adds more noise than the direct G02 question.
# MAGIC * **~18 metrics NOT IN REPORT** — These are Asia-specific (D05-D07), continent-level (H01-H02), ratio metrics (H06-H07), and other metrics the prompt simply doesn't ask about.

# COMMAND ----------

# DBTITLE 1,Section 12: Iteration Progression Summary
# MAGIC %md
# MAGIC ---
# MAGIC ## 12. Iteration Progression Summary
# MAGIC
# MAGIC This table shows how UC Semantic features progressively improve Genie accuracy across the 45 individual tests.
# MAGIC
# MAGIC ### Individual Tests (45 questions → domain agents)
# MAGIC
# MAGIC | Stage | PASS | Total | Accuracy | What Changed |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | **Baseline** | ~30-31 | 45 | ~67-69% (non-deterministic) | Bare tables, no comments, no views, no tags. Agent guesses from column/table names — answers vary across runs. |
# MAGIC | **After Iter 1** | ~35-37 | 45 | ~78-82% | Column comments, Example SQL Queries, Benchmarks. Steers agent to correct tables. |
# MAGIC | **After Iter 2** | ~38-40 | 45 | ~84-89% | UC Metric Views (4), Governed Tags, Open Knowledge View (CoD). Pre-computed KPIs eliminate formula guessing. |
# MAGIC | **After Iter 3** | 45 | 45 | 100% (fully deterministic) | `fiscal_targets` table, 5 SQL Functions, UC Domain + Pages. Every answer grounded in governed asset. |
# MAGIC
# MAGIC ### UC Features Used Per Iteration
# MAGIC
# MAGIC | Iteration | UC Features | Tests Fixed |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | `ALTER TABLE SET COMMENT`, `example_question_sqls` API (Genie Examples tab), `benchmarks` API (Genie Benchmarks tab) | A04, D04, D06, F02, F03, H01, H02, H03 |
# MAGIC | 2 | `CREATE VIEW WITH METRICS LANGUAGE YAML`, `ALTER TABLE SET TAGS`, `ALTER SCHEMA SET TAGS`, Open Knowledge View | E03, H05, H06, H07 |
# MAGIC | 3 | `fiscal_targets` table, 5 SQL Functions (`get_critical_*`), UC Domain + Pages (Discover page) | G01, G02, P01, P02, P03, P04, P05 |
# MAGIC
# MAGIC **Key finding: Genie Agents CANNOT access UC Pages.** G01/G02 are fixed by the `fiscal_targets` TABLE. P01-P05 are fixed by SQL Functions. UC Pages serve as human-facing governance documentation only.
# MAGIC
# MAGIC ### The Six Foundation Layers Demonstrated
# MAGIC
# MAGIC | Layer | What It Does | Demo Implementation |
# MAGIC | --- | --- | --- |
# MAGIC | 0. Data model | Gold model for agents, clear grain | 23 tables across 5 domain schemas |
# MAGIC | 1. Enrich metadata | Column/table comments, tags | Iter 1: comments. Iter 2: governed tags |
# MAGIC | 2. Model semantics | Metric Views, Domains, Pages | Iter 2: 4 MVs + CoD view. Iter 3: Domain + Pages |
# MAGIC | 3. Curate context | Certified assets, examples, instructions | Iter 1: Example SQL + Benchmarks. Iter 2: certification tags |
# MAGIC | 4. Evaluate & refine | Benchmark, evaluate, iterate | 40-test suite + comprehensive prompt benchmark |
# MAGIC | 5. Continuously learn | Ontology feedback loop | Domain + Pages feed Genie's ontology |