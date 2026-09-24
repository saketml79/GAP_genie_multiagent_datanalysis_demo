# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5: Generate Supplier Procurement Data
# MAGIC Creates: suppliers, supplier_orders, supplier_lead_times, vendor_slas, procurement_data
# MAGIC
# MAGIC **Demo Story**: Asian suppliers with +5-18 day delays; SUP-001 TextilePro Asia worst (+10-20 days, factory shutdowns).

# COMMAND ----------

dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")
SCHEMA = f"{CATALOG}.supplier_procurement"

# COMMAND ----------

import random
from datetime import datetime, timedelta

random.seed(45)  # Fixed seed for deterministic data generation
base_date = datetime(2026, 9, 1)  # Fixed reference date — data is always identical
product_families = ['Apparel', 'Accessories', 'Footwear', 'Home Goods', 'Electronics']

# ---- Suppliers (12) ----
suppliers_data = [
    {'supplier_id':'SUP-001','supplier_name':'TextilePro Asia','country':'China','continent':'Asia','primary_product_family':'Apparel','risk_tier':'High','annual_contract_value':8500000,'reliability_score':4.2},
    {'supplier_id':'SUP-002','supplier_name':'VietStitch Corp','country':'Vietnam','continent':'Asia','primary_product_family':'Apparel','risk_tier':'High','annual_contract_value':6200000,'reliability_score':5.8},
    {'supplier_id':'SUP-003','supplier_name':'EuroLeather GmbH','country':'Germany','continent':'Europe','primary_product_family':'Accessories','risk_tier':'Low','annual_contract_value':3400000,'reliability_score':9.1},
    {'supplier_id':'SUP-004','supplier_name':'IndoFootwear Ltd','country':'Indonesia','continent':'Asia','primary_product_family':'Footwear','risk_tier':'Medium','annual_contract_value':4800000,'reliability_score':6.5},
    {'supplier_id':'SUP-005','supplier_name':'HomeStyle Italy','country':'Italy','continent':'Europe','primary_product_family':'Home Goods','risk_tier':'Low','annual_contract_value':2900000,'reliability_score':8.7},
    {'supplier_id':'SUP-006','supplier_name':'BangladeshTextiles','country':'Bangladesh','continent':'Asia','primary_product_family':'Apparel','risk_tier':'High','annual_contract_value':5100000,'reliability_score':4.9},
    {'supplier_id':'SUP-007','supplier_name':'TechParts Korea','country':'South Korea','continent':'Asia','primary_product_family':'Electronics','risk_tier':'Medium','annual_contract_value':7200000,'reliability_score':7.3},
    {'supplier_id':'SUP-008','supplier_name':'US Manufacturing Co','country':'United States','continent':'North America','primary_product_family':'Home Goods','risk_tier':'Low','annual_contract_value':4100000,'reliability_score':9.4},
    {'supplier_id':'SUP-009','supplier_name':'ThaiSilk Industries','country':'Thailand','continent':'Asia','primary_product_family':'Accessories','risk_tier':'Medium','annual_contract_value':3600000,'reliability_score':6.8},
    {'supplier_id':'SUP-010','supplier_name':'MexiTextil SA','country':'Mexico','continent':'North America','primary_product_family':'Apparel','risk_tier':'Low','annual_contract_value':3800000,'reliability_score':8.1},
    {'supplier_id':'SUP-011','supplier_name':'ScandiDesign AB','country':'Sweden','continent':'Europe','primary_product_family':'Home Goods','risk_tier':'Low','annual_contract_value':2100000,'reliability_score':9.2},
    {'supplier_id':'SUP-012','supplier_name':'VietFootwear Co','country':'Vietnam','continent':'Asia','primary_product_family':'Footwear','risk_tier':'High','annual_contract_value':4500000,'reliability_score':5.1},
]
spark.createDataFrame(suppliers_data).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.suppliers")
print(f"✓ suppliers: {len(suppliers_data)}")

# COMMAND ----------

# ---- Supplier Orders (120 days, ~1400 POs) ----
# STORY: Asian suppliers +5-18 day delays last 45 days; SUP-001 +10-20 days last 60 days
supplier_orders = []
for day_offset in range(120):
    order_date = base_date - timedelta(days=day_offset)
    for supplier in suppliers_data:
        if random.random() < (0.15 if supplier['continent'] == 'Asia' else 0.08):
            contracted_lead = random.randint(14, 45)
            # Determine delay based on story
            if supplier['supplier_id'] == 'SUP-001' and day_offset < 60:
                actual_lead = contracted_lead + random.randint(10, 20)
                delay_reason = random.choice(['Factory_Shutdown', 'Raw_Material_Shortage', 'Port_Strike'])
            elif supplier['continent'] == 'Asia' and day_offset < 45:
                actual_lead = contracted_lead + random.randint(5, 18)
                delay_reason = random.choice(['Port_Strike', 'Raw_Material_Shortage', 'Shipping_Congestion', 'Customs_Delay'])
            else:
                extra = random.choices([0, 0, 1, 2, 3], weights=[40, 20, 15, 15, 10])[0]
                actual_lead = contracted_lead + extra
                delay_reason = None if extra == 0 else random.choice(['Minor_Delay', 'Shipping_Congestion'])
            qty_ordered = random.randint(100, 5000)
            qty_received = int(qty_ordered * random.uniform(0.85, 1.0)) if actual_lead <= contracted_lead + 3 else int(qty_ordered * random.uniform(0.70, 0.95))
            supplier_orders.append({
                'po_id': f'PO-{len(supplier_orders)+1:06d}',
                'order_date': order_date.strftime('%Y-%m-%d'),
                'supplier_id': supplier['supplier_id'],
                'supplier_name': supplier['supplier_name'],
                'supplier_country': supplier['country'],
                'supplier_continent': supplier['continent'],
                'product_family': supplier['primary_product_family'],
                'quantity_ordered': qty_ordered,
                'quantity_received': qty_received,
                'unit_cost': round(random.uniform(5, 80), 2),
                'total_cost': round(qty_ordered * random.uniform(5, 80), 2),
                'contracted_lead_time_days': contracted_lead,
                'actual_lead_time_days': actual_lead,
                'lead_time_variance_days': actual_lead - contracted_lead,
                'expected_delivery_date': (order_date + timedelta(days=contracted_lead)).strftime('%Y-%m-%d'),
                'actual_delivery_date': (order_date + timedelta(days=actual_lead)).strftime('%Y-%m-%d'),
                'is_late': actual_lead > contracted_lead,
                'delay_reason': delay_reason,
                'po_status': random.choice(['Received', 'In_Transit', 'Partially_Received', 'Cancelled']),
                'quality_score': round(random.uniform(70, 100), 1) if actual_lead <= contracted_lead + 5 else round(random.uniform(60, 85), 1)
            })
spark.createDataFrame(supplier_orders).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.supplier_orders")
print(f"✓ supplier_orders: {len(supplier_orders)}")

# COMMAND ----------

# ---- Supplier Lead Times (12 months x 12 suppliers) ----
lead_times = []
for month_offset in range(12):
    month_date = (base_date - timedelta(days=30*month_offset)).replace(day=1)
    for supplier in suppliers_data:
        contracted = random.randint(14, 45)
        if supplier['continent'] == 'Asia' and month_offset < 3:
            actual_avg = contracted + random.uniform(5, 15)
        elif supplier['supplier_id'] == 'SUP-001' and month_offset < 4:
            actual_avg = contracted + random.uniform(10, 20)
        else:
            actual_avg = contracted + random.uniform(-1, 3)
        lead_times.append({
            'lead_time_id': f'LT-{len(lead_times)+1:05d}',
            'month': month_date.strftime('%Y-%m-%d'),
            'supplier_id': supplier['supplier_id'],
            'supplier_name': supplier['supplier_name'],
            'supplier_country': supplier['country'],
            'supplier_continent': supplier['continent'],
            'contracted_lead_time_days': contracted,
            'avg_actual_lead_time_days': round(actual_avg, 1),
            'lead_time_variance_days': round(actual_avg - contracted, 1),
            'on_time_delivery_pct': round(max(30.0, 100.0 - (actual_avg - contracted) * 8), 1),
            'order_count': random.randint(5, 30),
            'defect_rate_pct': round(random.uniform(0.5, 8.0), 1)
        })
spark.createDataFrame(lead_times).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.supplier_lead_times")
print(f"✓ supplier_lead_times: {len(lead_times)}")

# COMMAND ----------

# ---- Vendor SLAs (5 metrics x 12 suppliers) ----
sla_metrics = ['On_Time_Delivery', 'Quality_Acceptance', 'Fill_Rate', 'Response_Time', 'Documentation_Accuracy']
vendor_slas = []
for supplier in suppliers_data:
    for metric in sla_metrics:
        target = round(random.uniform(90, 99), 1)
        if supplier['continent'] == 'Asia' and metric == 'On_Time_Delivery':
            actual = round(target - random.uniform(5, 25), 1)
        elif supplier['risk_tier'] == 'High':
            actual = round(target - random.uniform(2, 15), 1)
        else:
            actual = round(target + random.uniform(-3, 3), 1)
        vendor_slas.append({
            'sla_id': f'SLA-{len(vendor_slas)+1:04d}',
            'supplier_id': supplier['supplier_id'],
            'supplier_name': supplier['supplier_name'],
            'sla_metric': metric,
            'target_pct': target, 'actual_pct': actual,
            'variance_pct': round(actual - target, 1),
            'is_breached': actual < target,
            'penalty_amount': round(random.uniform(5000, 50000), 2) if actual < target else 0.0,
            'review_period': 'Q3_2026',
            'last_updated': base_date.strftime('%Y-%m-%d')
        })
spark.createDataFrame(vendor_slas).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.vendor_slas")
print(f"✓ vendor_slas: {len(vendor_slas)}")

# COMMAND ----------

# ---- Procurement Data (12 months x 5 families) ----
procurement = []
for month_offset in range(12):
    month_date = (base_date - timedelta(days=30*month_offset)).replace(day=1)
    for fam in product_families:
        budgeted = random.randint(200000, 800000)
        actual = int(budgeted * random.uniform(0.90, 1.15))
        procurement.append({
            'procurement_id': f'PROC-{len(procurement)+1:04d}',
            'month': month_date.strftime('%Y-%m-%d'),
            'product_family': fam,
            'total_spend_usd': actual,
            'budgeted_spend_usd': budgeted,
            'variance_pct': round((actual - budgeted) / budgeted * 100, 1),
            'num_suppliers': random.randint(2, 6),
            'num_pos': random.randint(10, 50),
            'avg_unit_cost': round(random.uniform(10, 60), 2),
            'cost_change_mom_pct': round(random.uniform(-5, 8), 1),
            'geographic_concentration_risk': 'High' if fam in ['Apparel', 'Electronics', 'Footwear'] else 'Low'
        })
spark.createDataFrame(procurement).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.procurement_data")
print(f"✓ procurement_data: {len(procurement)}")

print(f"\n✓ All supplier_procurement tables created in {SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Verify: supplier tables exist with rows
# ── Assertions ──
expected = ['suppliers', 'supplier_orders', 'supplier_lead_times', 'vendor_slas', 'procurement_data']
for t in expected:
    fqn = f"{SCHEMA}.{t}"
    assert spark.catalog.tableExists(fqn), f"MISSING table: {fqn}"
    cnt = spark.table(fqn).count()
    assert cnt > 0, f"EMPTY table: {fqn} (0 rows)"
print(f"✓ ASSERT PASS: all {len(expected)} supplier_procurement tables exist with data")