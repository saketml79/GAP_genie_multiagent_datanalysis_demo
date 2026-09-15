# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2: Generate Demand Analysis Data
# MAGIC Creates: products, customer_segments, sales_orders, demand_forecasts, pos_data, promotions
# MAGIC
# MAGIC **Demo Story**: Western Region revenue drops ~30% in last 30 days, Apparel family hit hardest.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")
SCHEMA = f"{CATALOG}.demand_analysis"
print(f"Target: {SCHEMA}")

# COMMAND ----------

from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

random.seed(42)  # Fixed seed for deterministic data generation
base_date = datetime(2026, 9, 1)  # Fixed reference date — data is always identical

regions = ['Western', 'Eastern', 'Central', 'Southern']
states_by_region = {
    'Western': ['CA', 'WA', 'OR', 'NV', 'AZ'],
    'Eastern': ['NY', 'NJ', 'PA', 'MA', 'CT'],
    'Central': ['IL', 'OH', 'MI', 'IN', 'WI'],
    'Southern': ['TX', 'FL', 'GA', 'NC', 'VA']
}
product_families = ['Apparel', 'Accessories', 'Footwear', 'Home Goods', 'Electronics']
product_categories = {
    'Apparel': ['T-Shirts', 'Jeans', 'Jackets', 'Dresses', 'Sweaters'],
    'Accessories': ['Bags', 'Belts', 'Watches', 'Sunglasses', 'Hats'],
    'Footwear': ['Sneakers', 'Boots', 'Sandals', 'Loafers', 'Heels'],
    'Home Goods': ['Bedding', 'Towels', 'Candles', 'Frames', 'Rugs'],
    'Electronics': ['Chargers', 'Headphones', 'Speakers', 'Cables', 'Cases']
}

# ---- Products (100 SKUs) ----
skus = []
sku_id = 1000
for fam in product_families:
    for cat in product_categories[fam]:
        for variant in range(4):
            skus.append({
                'sku_id': f'SKU-{sku_id}', 'product_name': f'{cat} - Style {chr(65+variant)}',
                'product_family': fam, 'product_category': cat,
                'unit_cost': round(random.uniform(5, 80), 2),
                'unit_price': round(random.uniform(15, 200), 2),
                'weight_kg': round(random.uniform(0.1, 5.0), 2)
            })
            sku_id += 1

df_products = spark.createDataFrame(skus)
df_products.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.products")
print(f"✓ products: {df_products.count()}")

# COMMAND ----------

# ---- Customer Segments (500 customers) ----
segment_types = ['Premium', 'Standard', 'Value', 'Wholesale']
customer_segments = []
for i in range(500):
    region = random.choice(regions)
    customer_segments.append({
        'customer_id': f'CUST-{10000+i}', 'customer_name': f'Customer_{i}',
        'segment': random.choice(segment_types), 'region': region,
        'state': random.choice(states_by_region[region]),
        'city': f'City_{random.randint(1,50)}',
        'annual_revenue_tier': random.choice(['Tier1_Above1M', 'Tier2_500K_1M', 'Tier3_100K_500K', 'Tier4_Below100K']),
        'loyalty_score': round(random.uniform(1, 10), 1),
        'first_purchase_date': (datetime(2020,1,1) + timedelta(days=random.randint(0,1500))).strftime('%Y-%m-%d'),
        'is_active': random.choice([True, True, True, False])
    })

df_customers = spark.createDataFrame(customer_segments)
df_customers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.customer_segments")
print(f"✓ customer_segments: {df_customers.count()}")

# COMMAND ----------

# ---- Sales Orders (90 days, ~20K orders) ----
# STORY: Western region + Apparel drops 30% in last 30 days
sales_orders = []
order_id = 100000
for day_offset in range(90):
    order_date = base_date - timedelta(days=day_offset)
    for _ in range(random.randint(150, 300)):
        cust = random.choice(customer_segments)
        sku = random.choice(skus)
        qty = random.randint(1, 50)
        revenue_multiplier = 1.0
        if cust['region'] == 'Western' and day_offset < 30:
            revenue_multiplier = 0.70
        if sku['product_family'] == 'Apparel' and day_offset < 30:
            revenue_multiplier *= 0.85
        unit_price = sku['unit_price'] * revenue_multiplier
        status = random.choices(
            ['Fulfilled', 'Partially_Fulfilled', 'Backordered', 'Cancelled'],
            weights=[70, 10, 15, 5] if (cust['region'] == 'Western' and day_offset < 30) else [85, 5, 5, 5]
        )[0]
        sales_orders.append({
            'order_id': f'ORD-{order_id}', 'order_date': order_date.strftime('%Y-%m-%d'),
            'customer_id': cust['customer_id'], 'region': cust['region'], 'state': cust['state'],
            'sku_id': sku['sku_id'], 'product_family': sku['product_family'],
            'product_category': sku['product_category'], 'quantity': qty,
            'unit_price': round(unit_price, 2), 'total_amount': round(qty * unit_price, 2),
            'order_status': status,
            'channel': random.choice(['Online', 'Retail', 'Wholesale', 'Marketplace']),
            'fulfillment_warehouse': f'WH-{random.randint(1,12):03d}'
        })
        order_id += 1

df_sales = spark.createDataFrame(sales_orders)
df_sales.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.sales_orders")
print(f"✓ sales_orders: {df_sales.count()}")

# COMMAND ----------

# ---- Demand Forecasts (6 months) ----
# STORY: Western forecast missed badly (over-forecast by 35%)
forecasts = []
for month_offset in range(6):
    forecast_month = (base_date - timedelta(days=30*month_offset)).replace(day=1)
    for region in regions:
        for fam in product_families:
            actual = random.randint(5000, 50000)
            if region == 'Western' and month_offset == 0:
                forecast = int(actual * 1.35)
            elif month_offset == 0:
                forecast = int(actual * random.uniform(0.95, 1.10))
            else:
                forecast = int(actual * random.uniform(0.92, 1.08))
            accuracy = round(min(actual, forecast) / max(actual, forecast) * 100, 1)
            forecasts.append({
                'forecast_id': f'FC-{len(forecasts)+1:05d}',
                'forecast_month': forecast_month.strftime('%Y-%m-%d'),
                'region': region, 'product_family': fam,
                'forecast_demand_units': forecast, 'actual_demand_units': actual,
                'forecast_accuracy_pct': accuracy,
                'forecast_model': random.choice(['ARIMA', 'Prophet', 'ML_Ensemble']),
                'bias': round((forecast - actual) / actual * 100, 1)
            })

df_forecasts = spark.createDataFrame(forecasts)
df_forecasts.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.demand_forecasts")
print(f"✓ demand_forecasts: {df_forecasts.count()}")

# COMMAND ----------

# ---- POS Data (60 days, ~50K transactions) ----
stores = [f'STORE-{i:03d}' for i in range(1, 81)]
store_regions = {s: random.choice(regions) for s in stores}
pos_data = []
for day_offset in range(60):
    sale_date = base_date - timedelta(days=day_offset)
    for store in random.sample(stores, 60):
        for _ in range(random.randint(5, 25)):
            sku = random.choice(skus)
            qty = random.randint(1, 5)
            price = sku['unit_price'] * (0.80 if store_regions[store] == 'Western' and day_offset < 30 else 1.0)
            pos_data.append({
                'transaction_id': f'POS-{len(pos_data)+1:08d}',
                'transaction_date': sale_date.strftime('%Y-%m-%d'),
                'store_id': store, 'region': store_regions[store],
                'sku_id': sku['sku_id'], 'product_family': sku['product_family'],
                'quantity_sold': qty, 'unit_price': round(price, 2),
                'total_sales': round(qty * price, 2),
                'payment_method': random.choice(['Credit', 'Debit', 'Cash', 'Digital_Wallet']),
                'return_flag': random.choices([True, False], weights=[8, 92])[0]
            })

df_pos = spark.createDataFrame(pos_data)
df_pos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.pos_data")
print(f"✓ pos_data: {df_pos.count()}")

# COMMAND ----------

# ---- Promotions (30 campaigns) ----
promo_types = ['Clearance', 'Seasonal_Sale', 'Flash_Sale', 'Loyalty_Discount', 'Bundle_Deal']
promotions = []
for i in range(30):
    start = base_date - timedelta(days=random.randint(5, 80))
    promotions.append({
        'promo_id': f'PROMO-{i+1:03d}',
        'promo_name': f'{random.choice(promo_types)}_{random.choice(product_families)}',
        'promo_type': random.choice(promo_types),
        'start_date': start.strftime('%Y-%m-%d'),
        'end_date': (start + timedelta(days=random.randint(5, 21))).strftime('%Y-%m-%d'),
        'discount_pct': random.choice([10, 15, 20, 25, 30]),
        'target_region': random.choice(regions + ['All']),
        'target_product_family': random.choice(product_families + ['All']),
        'budget_usd': round(random.uniform(5000, 100000), 2),
        'estimated_lift_pct': round(random.uniform(2, 15), 1),
        'actual_lift_pct': round(random.uniform(-5, 20), 1)
    })

df_promos = spark.createDataFrame(promotions)
df_promos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.promotions")
print(f"✓ promotions: {df_promos.count()}")

print(f"\n✓ All demand_analysis tables created in {SCHEMA}")