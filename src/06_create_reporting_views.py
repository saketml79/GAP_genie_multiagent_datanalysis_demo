# Databricks notebook source
# COMMAND ----------
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
SELECT
  so.region,
  COUNT(DISTINCT so.order_id) AS total_orders,
  ROUND(AVG(so.total_amount), 2) AS avg_order_value,
  ROUND(SUM(so.total_amount), 2) AS total_revenue,
  ROUND(
    SUM(CASE WHEN so.order_status = 'Fulfilled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
  ) AS fulfillment_rate,
  (
    SELECT COUNT(DISTINCT il.sku_id)
    FROM {CATALOG}.inventory_management.inventory_ledger il
    WHERE il.region = so.region AND il.stockout_flag = true
  ) AS stockout_skus,
  (
    SELECT ROUND(AVG(CASE WHEN sh.is_late THEN 1.0 ELSE 0.0 END) * 100, 1)
    FROM {CATALOG}.logistics_operations.shipments sh
    WHERE sh.destination_region = so.region
      AND sh.ship_date >= DATE_SUB(CURRENT_DATE(), 30)
  ) AS late_shipment_pct,
  (
    SELECT ROUND(AVG(CASE WHEN spo.is_late THEN 1.0 ELSE 0.0 END) * 100, 1)
    FROM {CATALOG}.supplier_procurement.supplier_orders spo
    WHERE spo.order_date >= DATE_SUB(CURRENT_DATE(), 30)
  ) AS supplier_late_pct,
  (
    SELECT ROUND(AVG(spo.lead_time_variance_days), 1)
    FROM {CATALOG}.supplier_procurement.supplier_orders spo
    WHERE spo.is_late = true AND spo.order_date >= DATE_SUB(CURRENT_DATE(), 30)
  ) AS avg_supplier_delay_days
FROM {CATALOG}.demand_analysis.sales_orders so
WHERE so.order_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY so.region
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
    WHEN DATEDIFF(CURRENT_DATE(), order_date) <= 30 THEN 'Last_30_Days'
    WHEN DATEDIFF(CURRENT_DATE(), order_date) <= 60 THEN 'Prior_30_Days'
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
    WHERE order_date >= DATE_SUB(CURRENT_DATE(), 30)
  ) AS revenue_last_30d,
  (
    SELECT ROUND(SUM(total_amount), 0)
    FROM {CATALOG}.demand_analysis.sales_orders
    WHERE order_date BETWEEN DATE_SUB(CURRENT_DATE(), 60) AND DATE_SUB(CURRENT_DATE(), 31)
  ) AS revenue_prior_30d,
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
    WHERE ship_date >= DATE_SUB(CURRENT_DATE(), 30)
  ) AS late_delivery_pct_30d,
  (
    SELECT ROUND(AVG(delay_days), 1)
    FROM {CATALOG}.logistics_operations.shipments
    WHERE ship_date >= DATE_SUB(CURRENT_DATE(), 30) AND is_late = true
  ) AS avg_delay_days_30d,
  (
    SELECT ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1)
    FROM {CATALOG}.supplier_procurement.supplier_orders
    WHERE order_date >= DATE_SUB(CURRENT_DATE(), 30)
  ) AS supplier_late_pct_30d,
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
    WHERE order_date >= DATE_SUB(CURRENT_DATE(), 30)
  ) AS service_level_pct
""")
print("✓ executive_kpis")

print(f"\n✓ All reporting views created in {CATALOG}.reporting")
