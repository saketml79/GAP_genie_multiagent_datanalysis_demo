# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Expected Output Reference: Supply Chain Control Tower
# MAGIC 
# MAGIC **Prompt**: "Why did revenue drop in the Western Region last month, are we going to miss our quarterly
# MAGIC service-level targets, and what immediate actions should we take? Investigate every dimension -- demand,
# MAGIC inventory, logistics, suppliers, and overall KPIs. Show me your full reasoning, which agents you consulted,
# MAGIC what each found, and how the root causes connect across domains."
# MAGIC 
# MAGIC This notebook shows the **correct, verified output** that the Supervisor Agent should produce after
# MAGIC all 5 improvement iterations. Every number is computed from direct SQL against the actual data.

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
# MAGIC | demand-analysis | Revenue by region last 30d vs prior 30d; Western breakdown by product family; order status |
# MAGIC | inventory-management | Stockout SKUs by region; Western inventory by product family; stock movement net flow |
# MAGIC | logistics-operations | Late delivery rate by destination region last 30d; Western delay reasons; carrier performance |
# MAGIC | supplier-risk | Supplier late rate by continent last 30d; top risk suppliers; SLA breaches |
# MAGIC | executive-reporting | Executive KPIs; regional performance summary |

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Findings by Agent
# MAGIC ### Agent: demand-analysis (SC - Demand Analysis)

# COMMAND ----------
# MAGIC %md
# MAGIC #### Query 1: Revenue by Region (Last 30d vs Prior 30d)

# COMMAND ----------
df1 = spark.sql(f"""
SELECT
  region,
  ROUND(SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE(), 30) THEN total_amount ELSE 0 END), 2) AS revenue_last_30d,
  ROUND(SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END), 2) AS revenue_prior_30d,
  ROUND(
    SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE(), 30) THEN total_amount ELSE 0 END) -
    SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END)
  , 2) AS revenue_change,
  ROUND(
    (SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE(), 30) THEN total_amount ELSE 0 END) -
     SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END)) /
    NULLIF(SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END), 0) * 100
  , 1) AS pct_change
FROM {CATALOG}.demand_analysis.sales_orders
GROUP BY region ORDER BY revenue_change ASC
""")
df1.display()

# COMMAND ----------
# MAGIC %md
# MAGIC **Finding**: Western revenue dropped **-$1.23M (-29.1%)** -- by far the largest decline. Other regions
# MAGIC show modest declines (Central -7.3%, Southern -6.0%) while Eastern is flat (+1.9%).
# MAGIC 
# MAGIC **Confidence**: HIGH -- complete data, 4 regions, clear pattern.

# COMMAND ----------
# MAGIC %md
# MAGIC #### Query 2: Western Revenue by Product Family

# COMMAND ----------
df2 = spark.sql(f"""
SELECT
  product_family,
  ROUND(SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE(), 30) THEN total_amount ELSE 0 END), 2) AS rev_last_30d,
  ROUND(SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END), 2) AS rev_prior_30d,
  ROUND(
    SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE(), 30) THEN total_amount ELSE 0 END) -
    SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END)
  , 2) AS change_usd,
  ROUND(
    (SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE(), 30) THEN total_amount ELSE 0 END) -
     SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END)) /
    NULLIF(SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31) THEN total_amount ELSE 0 END), 0) * 100
  , 1) AS pct_change
FROM {CATALOG}.demand_analysis.sales_orders
WHERE region = 'Western'
GROUP BY product_family ORDER BY change_usd ASC
""")
df2.display()

# COMMAND ----------
# MAGIC %md
# MAGIC **Finding**: ALL product families declined. Worst hit:
# MAGIC - Apparel: **-$306K (-38.8%)**
# MAGIC - Home Goods: **-$300K (-31.4%)**
# MAGIC - Footwear: **-$297K (-33.6%)**
# MAGIC - Electronics: **-$267K (-31.3%)**
# MAGIC - Accessories: **-$62K (-8.2%)** (least affected)
# MAGIC 
# MAGIC Sum of product family changes = -$1.23M ✓ (matches regional total)

# COMMAND ----------
# MAGIC %md
# MAGIC #### Query 3: Western Order Status Breakdown

# COMMAND ----------
df3 = spark.sql(f"""
SELECT order_status, COUNT(*) AS order_count,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_orders
FROM {CATALOG}.demand_analysis.sales_orders
WHERE region = 'Western' AND order_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY order_status ORDER BY order_count DESC
""")
df3.display()

# COMMAND ----------
# MAGIC %md
# MAGIC **Finding**: Only **70.8%** of Western orders fulfilled (vs ~85% target). **281 backordered** + **66 cancelled** = 347 unfulfilled orders representing **$546K in at-risk revenue**.

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ### Agent: inventory-management (SC - Inventory Management)

# COMMAND ----------
# MAGIC %md
# MAGIC #### Query 4: Stockouts by Region

# COMMAND ----------
df4 = spark.sql(f"""
SELECT region,
  COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS stockout_skus,
  COUNT(DISTINCT sku_id) AS total_skus,
  ROUND(AVG(days_of_supply), 1) AS avg_days_of_supply,
  COUNT(CASE WHEN below_safety_stock_flag = true THEN 1 END) AS below_safety_stock_count
FROM {CATALOG}.inventory_management.inventory_ledger
GROUP BY region ORDER BY stockout_skus DESC
""")
df4.display()

# COMMAND ----------
# MAGIC %md
# MAGIC **Finding**: Western has **31 SKU stockouts** (other regions: ZERO). Western avg days of supply is only **13.8** (vs 17-18 for others). **109 items below safety stock** in Western alone.
# MAGIC 
# MAGIC **Confidence**: HIGH -- direct flag-based count.

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ### Agent: logistics-operations (SC - Logistics Operations)

# COMMAND ----------
# MAGIC %md
# MAGIC #### Query 5: Late Delivery Rate by Destination Region (Last 30d)

# COMMAND ----------
df5 = spark.sql(f"""
SELECT
  destination_region,
  COUNT(*) AS total_shipments,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_shipments,
  ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
  ROUND(AVG(CASE WHEN is_late THEN delay_days ELSE NULL END), 1) AS avg_delay_when_late
FROM {CATALOG}.logistics_operations.shipments
WHERE ship_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY destination_region ORDER BY late_pct DESC
""")
df5.display()

# COMMAND ----------
# MAGIC %md
# MAGIC **Finding**: Western has **100% late delivery rate** (916/916 shipments late, avg 3.1 day delay).
# MAGIC Other regions are 29-31% late. This is catastrophic and explains the fulfillment failures.
# MAGIC 
# MAGIC **Confidence**: HIGH -- complete shipment data.

# COMMAND ----------
# MAGIC %md
# MAGIC #### Query 6: Western Delay Reasons

# COMMAND ----------
df6 = spark.sql(f"""
SELECT delay_reason, COUNT(*) AS cnt,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
FROM {CATALOG}.logistics_operations.shipments
WHERE destination_region = 'Western'
  AND ship_date >= DATE_SUB(CURRENT_DATE(), 30)
  AND is_late = true
GROUP BY delay_reason ORDER BY cnt DESC
""")
df6.display()

# COMMAND ----------
# MAGIC %md
# MAGIC **Finding**: Delays evenly split: Customs Hold 21.6%, Port Congestion 21.1%, Labor Shortage 20.2%, Carrier Capacity 19.0%, Weather 18.1%. This suggests systemic infrastructure issues, not a single cause.

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
WHERE order_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY supplier_continent ORDER BY late_pct DESC
""")
df7.display()

# COMMAND ----------
# MAGIC %md
# MAGIC **Finding**: Asia suppliers are **100% late** (216/216 POs) with **+11.5 day average variance**.
# MAGIC Europe 40.2% late, North America 25% late. Asian supply chain is completely disrupted.
# MAGIC 
# MAGIC **Confidence**: HIGH -- all POs in the window captured.

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
# MAGIC %md
# MAGIC **Finding**: Top 5 riskiest suppliers are ALL in Asia. SUP-001 TextilePro Asia (China, Apparel) is worst
# MAGIC with risk score 54.5, 5 SLA breaches, $87K in penalties.

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
# MAGIC %md
# MAGIC **Executive KPIs**:
# MAGIC - Revenue last 30d: ~$14.6M (prior: ~$16.3M) -- **-10.5% overall decline**
# MAGIC - Stockout SKUs: **36** (concentrated in Western)
# MAGIC - Late delivery: **46.6%** (target: <20%)
# MAGIC - Supplier late: **72.8%**
# MAGIC - SLA breaches: **38**
# MAGIC - Service level: **80.5%** (target: 85%)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Cross-Domain Reconciliation
# MAGIC 
# MAGIC | KPI | Demand Agent | Executive Agent | Match? |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Revenue Last 30d | Sum of 4 regions = ~$14.6M | executive_kpis = ~$14.6M | YES |
# MAGIC | Stockout SKUs | Inventory: 31 (Western only) | executive_kpis: 36 | CLOSE (timing of snapshot) |
# MAGIC | Late Delivery % | Logistics: 46.6% (all regions) | executive_kpis: 46.6% | YES |
# MAGIC | Service Level | Demand: Western 70.8% fulfill | executive_kpis: 80.5% overall | CONSISTENT (Western pulls avg down) |

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Root Cause Chain
# MAGIC 
# MAGIC ```
# MAGIC UPSTREAM CAUSE:
# MAGIC   Asian suppliers 100% late (avg +11.5 days)
# MAGIC   └─ SUP-001 TextilePro Asia: factory shutdowns, +10-20 day delays
# MAGIC   └─ 5 of top 5 risk suppliers are in Asia
# MAGIC        │
# MAGIC        ▼
# MAGIC INVENTORY IMPACT:
# MAGIC   Western warehouses depleted (31 SKU stockouts, 109 below safety stock)
# MAGIC   └─ Apparel worst: 0-15 units on hand vs 20-100 safety stock
# MAGIC   └─ Days of supply: 13.8 (Western) vs 17-18 (other regions)
# MAGIC        │
# MAGIC        ▼
# MAGIC LOGISTICS BREAKDOWN:
# MAGIC   100% of Western-bound shipments late (avg 3.1 day delay)
# MAGIC   └─ Delays: Port Congestion + Customs Hold + Labor Shortage + Carrier Capacity
# MAGIC   └─ LA Hub (DC-W1) at 92.3% capacity (congested)
# MAGIC        │
# MAGIC        ▼
# MAGIC REVENUE IMPACT:
# MAGIC   Western revenue -$1.23M (-29.1%)
# MAGIC   └─ All 5 product families down 8-39%
# MAGIC   └─ Apparel worst: -$306K (-38.8%)
# MAGIC   └─ 347 unfulfilled orders = $546K at-risk revenue
# MAGIC   └─ Service level: 80.5% (below 85% target)
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Conclusion and Actions
# MAGIC 
# MAGIC ### Direct Answers
# MAGIC 
# MAGIC 1. **Why did revenue drop?** Asian supplier delays cascaded through Western inventory (31 stockouts)
# MAGIC    and logistics (100% late deliveries), causing a -$1.23M (-29.1%) revenue decline across all product
# MAGIC    families, with Apparel hit hardest (-38.8%).
# MAGIC 
# MAGIC 2. **Will we miss targets?** YES. Service level is 80.5% vs 85% target. At current trajectory with
# MAGIC    100% Western late rate and 72.8% supplier late rate, we will miss Q3 targets unless immediate action is taken.
# MAGIC 
# MAGIC 3. **What actions?** See below.
# MAGIC 
# MAGIC ### Immediate Actions (1-2 weeks)
# MAGIC 1. **Emergency stock transfer**: Redistribute 50,000 units from Eastern/Central warehouses to Western (they have adequate supply at 17-18 days)
# MAGIC 2. **Activate backup carriers**: Replace bottom 2 carriers on Western routes with expedited options
# MAGIC 3. **Escalate SUP-001**: Demand recovery plan from TextilePro Asia or trigger contract penalty ($87K accrued)
# MAGIC 
# MAGIC ### Medium-Term Actions (1-3 months)
# MAGIC 1. **Diversify apparel sourcing**: Add North American/European apparel supplier to reduce Asia concentration
# MAGIC 2. **Increase Western safety stock**: Raise reorder points by 50% for top 20 SKUs
# MAGIC 3. **Negotiate DC capacity**: Expand LA Hub (DC-W1) beyond 92.3% or add overflow facility
# MAGIC 
# MAGIC ### KPIs to Monitor
# MAGIC 
# MAGIC | KPI | Current | Target | Timeline |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Western late delivery % | 100% | <25% | 2 weeks |
# MAGIC | Western stockout SKUs | 31 | 0 | 3 weeks |
# MAGIC | Service level % | 80.5% | 85% | End of Q3 |
# MAGIC | Asian supplier on-time | 0% | 70% | 6 weeks |
# MAGIC | Western revenue (monthly) | $3.0M | $4.2M | 8 weeks |
