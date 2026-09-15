# Databricks notebook source
# MAGIC %md
# MAGIC # Step 6: Create Reporting Views
# MAGIC Creates 4 cross-domain views in the reporting schema: regional_performance_summary, revenue_trend, supply_chain_risk_scorecard, executive_kpis

# COMMAND ----------

dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# ---- View 1: Regional Performance Summary ----
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.reporting.regional_performance_summary AS
WITH orders AS (
  SELECT region,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(AVG(total_amount), 2) AS avg_order_value,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(SUM(CASE WHEN order_status = 'Fulfilled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS fulfillment_rate
  FROM {CATALOG}.demand_analysis.sales_orders
  WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
  GROUP BY region
),
inv AS (
  SELECT region, COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS stockout_skus
  FROM {CATALOG}.inventory_management.inventory_ledger GROUP BY region
),
ship AS (
  SELECT destination_region AS region,
    ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_shipment_pct
  FROM {CATALOG}.logistics_operations.shipments
  WHERE ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
    AND ship_date < DATE_TRUNC('month', DATE '2026-09-01')
  GROUP BY destination_region
)
SELECT o.region, o.total_orders, o.avg_order_value, o.total_revenue, o.fulfillment_rate,
  COALESCE(i.stockout_skus, 0) AS stockout_skus,
  COALESCE(s.late_shipment_pct, 0) AS late_shipment_pct
FROM orders o
LEFT JOIN inv i ON o.region = i.region
LEFT JOIN ship s ON o.region = s.region
""")
print("✓ regional_performance_summary")

# COMMAND ----------

# ---- View 2: Revenue Trend ----
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.reporting.revenue_trend AS
SELECT
  order_date, region, product_family,
  SUM(total_amount) AS daily_revenue,
  COUNT(DISTINCT order_id) AS order_count,
  CASE
    WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
         AND order_date < DATE_TRUNC('month', DATE '2026-09-01') THEN 'Last_Month'
    WHEN order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2))
         AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1)) THEN 'Prior_Month'
    ELSE 'Older'
  END AS period
FROM {CATALOG}.demand_analysis.sales_orders
GROUP BY order_date, region, product_family
""")
print("✓ revenue_trend")

# COMMAND ----------

# ---- View 3: Supply Chain Risk Scorecard ----
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.reporting.supply_chain_risk_scorecard AS
SELECT
  s.supplier_id, s.supplier_name, s.country, s.continent,
  s.risk_tier, s.reliability_score,
  COALESCE(lt.avg_actual_lead_time_days, 0) AS latest_lead_time_days,
  COALESCE(lt.lead_time_variance_days, 0) AS lead_time_variance,
  COALESCE(lt.on_time_delivery_pct, 0) AS on_time_pct,
  COALESCE(sla.breach_count, 0) AS sla_breaches,
  COALESCE(sla.total_penalties, 0) AS total_penalty_usd,
  ROUND(
    (COALESCE(lt.on_time_delivery_pct, 50) * 0.4) +
    (s.reliability_score * 10 * 0.3) +
    ((100 - LEAST(COALESCE(lt.lead_time_variance_days, 0) * 5, 100)) * 0.3)
  , 1) AS composite_risk_score
FROM {CATALOG}.supplier_procurement.suppliers s
LEFT JOIN (
  SELECT supplier_id, avg_actual_lead_time_days, lead_time_variance_days, on_time_delivery_pct
  FROM {CATALOG}.supplier_procurement.supplier_lead_times
  WHERE month = (
    SELECT MAX(month) FROM {CATALOG}.supplier_procurement.supplier_lead_times
  )
) lt ON s.supplier_id = lt.supplier_id
LEFT JOIN (
  SELECT supplier_id, COUNT(*) AS breach_count, ROUND(SUM(penalty_amount), 2) AS total_penalties
  FROM {CATALOG}.supplier_procurement.vendor_slas
  WHERE is_breached = true
  GROUP BY supplier_id
) sla ON s.supplier_id = sla.supplier_id
""")
print("✓ supply_chain_risk_scorecard")

# COMMAND ----------

# ---- View 4: Executive KPIs ----
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.reporting.executive_kpis AS
SELECT
  (
    SELECT ROUND(SUM(total_amount), 0)
    FROM {CATALOG}.demand_analysis.sales_orders
    WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
      AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
  ) AS revenue_last_month,
  (
    SELECT ROUND(SUM(total_amount), 0)
    FROM {CATALOG}.demand_analysis.sales_orders
    WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -2))
      AND order_date < DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
  ) AS revenue_prior_month,
  (
    SELECT COUNT(DISTINCT sku_id)
    FROM {CATALOG}.inventory_management.inventory_ledger
    WHERE stockout_flag = true
  ) AS total_stockout_skus,
  (
    SELECT ROUND(AVG(days_of_supply), 1)
    FROM {CATALOG}.inventory_management.inventory_ledger
  ) AS avg_days_of_supply,
  (
    SELECT ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1)
    FROM {CATALOG}.logistics_operations.shipments
    WHERE ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
      AND ship_date < DATE_TRUNC('month', DATE '2026-09-01')
  ) AS late_delivery_pct_last_month,
  (
    SELECT ROUND(AVG(delay_days), 1)
    FROM {CATALOG}.logistics_operations.shipments
    WHERE ship_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
      AND ship_date < DATE_TRUNC('month', DATE '2026-09-01') AND is_late = true
  ) AS avg_delay_days_last_month,
  (
    SELECT ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1)
    FROM {CATALOG}.supplier_procurement.supplier_orders
    WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
      AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
  ) AS supplier_late_pct_last_month,
  (
    SELECT COUNT(*)
    FROM {CATALOG}.supplier_procurement.vendor_slas
    WHERE is_breached = true
  ) AS total_sla_breaches,
  (
    SELECT ROUND(
      SUM(CASE WHEN order_status = 'Fulfilled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
    )
    FROM {CATALOG}.demand_analysis.sales_orders
    WHERE order_date >= DATE_TRUNC('month', ADD_MONTHS(DATE '2026-09-01', -1))
      AND order_date < DATE_TRUNC('month', DATE '2026-09-01')
  ) AS service_level_pct
""")
print("✓ executive_kpis")

print(f"\n✓ All reporting views created in {CATALOG}.reporting")