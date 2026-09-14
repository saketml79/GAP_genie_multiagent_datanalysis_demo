# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Step 3: Generate Inventory Management Data
# MAGIC Creates: warehouse_data, inventory_ledger, store_inventory, stock_movements
# MAGIC 
# MAGIC **Demo Story**: Western warehouses critically low on Apparel, 36+ SKU stockouts, outbound > inbound.

# COMMAND ----------
dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")
SCHEMA = f"{CATALOG}.inventory_management"

# COMMAND ----------
import random
from datetime import datetime, timedelta

base_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
regions = ['Western', 'Eastern', 'Central', 'Southern']
product_families = ['Apparel', 'Accessories', 'Footwear', 'Home Goods', 'Electronics']
product_categories = {
    'Apparel': ['T-Shirts', 'Jeans', 'Jackets', 'Dresses', 'Sweaters'],
    'Accessories': ['Bags', 'Belts', 'Watches', 'Sunglasses', 'Hats'],
    'Footwear': ['Sneakers', 'Boots', 'Sandals', 'Loafers', 'Heels'],
    'Home Goods': ['Bedding', 'Towels', 'Candles', 'Frames', 'Rugs'],
    'Electronics': ['Chargers', 'Headphones', 'Speakers', 'Cables', 'Cases']
}
skus = []
sku_id = 1000
for fam in product_families:
    for cat in product_categories[fam]:
        for variant in range(4):
            skus.append({'sku_id': f'SKU-{sku_id}', 'product_family': fam, 'product_category': cat})
            sku_id += 1

wh_regions = {
    'WH-001': 'Western', 'WH-002': 'Western', 'WH-003': 'Western',
    'WH-004': 'Eastern', 'WH-005': 'Eastern', 'WH-006': 'Eastern',
    'WH-007': 'Central', 'WH-008': 'Central', 'WH-009': 'Central',
    'WH-010': 'Southern', 'WH-011': 'Southern', 'WH-012': 'Southern'
}

# COMMAND ----------
# ---- Warehouses (12 DCs) ----
warehouses = []
for wh_id, region in wh_regions.items():
    warehouses.append({
        'warehouse_id': wh_id, 'warehouse_name': f'{region} DC {wh_id[-1]}',
        'region': region, 'city': f'{region}_City_{random.randint(1,5)}',
        'capacity_units': random.randint(50000, 200000),
        'current_utilization_pct': round(random.uniform(60, 95), 1),
        'warehouse_type': random.choice(['Primary', 'Secondary', 'Returns']),
        'is_active': True
    })
spark.createDataFrame(warehouses).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.warehouse_data")
print(f"✓ warehouse_data: {len(warehouses)}")

# COMMAND ----------
# ---- Inventory Ledger (per warehouse per SKU) ----
# STORY: Western + Apparel = critically low; Western has ~12% stockout rate
inventory = []
for wh_id, region in wh_regions.items():
    for sku in skus:
        on_hand = random.randint(0, 500)
        safety_stock = random.randint(20, 100)
        if region == 'Western' and sku['product_family'] == 'Apparel':
            on_hand = random.randint(0, 15)
        if region == 'Western' and random.random() < 0.12:
            on_hand = 0
        inventory.append({
            'ledger_id': f'INV-{len(inventory)+1:06d}',
            'snapshot_date': base_date.strftime('%Y-%m-%d'),
            'warehouse_id': wh_id, 'region': region,
            'sku_id': sku['sku_id'], 'product_family': sku['product_family'],
            'product_category': sku['product_category'],
            'on_hand_qty': on_hand,
            'allocated_qty': random.randint(0, min(on_hand, 50)),
            'available_qty': max(0, on_hand - random.randint(0, 30)),
            'safety_stock_level': safety_stock,
            'reorder_point': safety_stock * 2,
            'days_of_supply': round(on_hand / max(random.uniform(5, 30), 1), 1),
            'stockout_flag': on_hand == 0,
            'below_safety_stock_flag': on_hand < safety_stock
        })
spark.createDataFrame(inventory).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.inventory_ledger")
print(f"✓ inventory_ledger: {len(inventory)}")

# COMMAND ----------
# ---- Store Inventory (80 stores x ~40 SKUs) ----
stores = [f'STORE-{i:03d}' for i in range(1, 81)]
store_inv = []
for i, store in enumerate(stores):
    region = regions[i % 4]
    for sku in random.sample(skus, 40):
        oh = random.randint(0, 30)
        if region == 'Western' and sku['product_family'] in ['Apparel', 'Footwear']:
            oh = random.randint(0, 3)
        store_inv.append({
            'store_inventory_id': f'SINV-{len(store_inv)+1:06d}',
            'snapshot_date': base_date.strftime('%Y-%m-%d'),
            'store_id': store, 'region': region,
            'sku_id': sku['sku_id'], 'product_family': sku['product_family'],
            'on_hand_qty': oh, 'min_display_qty': random.randint(2, 5),
            'replenishment_status': 'Stockout' if oh == 0 else ('Low' if oh < 5 else 'Adequate'),
            'last_replenishment_date': (base_date - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d')
        })
spark.createDataFrame(store_inv).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.store_inventory")
print(f"✓ store_inventory: {len(store_inv)}")

# COMMAND ----------
# ---- Stock Movements (60 days) ----
# STORY: Western has more outbound than inbound (depleting)
movement_types = ['Inbound_Receipt', 'Outbound_Order', 'Transfer_In', 'Transfer_Out', 'Return_Receipt', 'Adjustment']
movements = []
for day_offset in range(60):
    move_date = base_date - timedelta(days=day_offset)
    for _ in range(random.randint(100, 250)):
        wh_id = random.choice(list(wh_regions.keys()))
        region = wh_regions[wh_id]
        sku = random.choice(skus)
        move_type = random.choice(movement_types)
        if region == 'Western' and day_offset < 30 and random.random() < 0.65:
            move_type = 'Outbound_Order'
        qty = random.randint(1, 200)
        movements.append({
            'movement_id': f'MOV-{len(movements)+1:08d}',
            'movement_date': move_date.strftime('%Y-%m-%d'),
            'movement_type': move_type,
            'warehouse_id': wh_id, 'region': region,
            'sku_id': sku['sku_id'], 'product_family': sku['product_family'],
            'quantity': qty if 'Inbound' in move_type or 'Return' in move_type or 'Transfer_In' in move_type else -qty,
            'reference_id': f'REF-{random.randint(100000, 999999)}',
            'reason_code': random.choice(['Customer_Order', 'Replenishment', 'Rebalance', 'Damage', 'Cycle_Count'])
        })
spark.createDataFrame(movements).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.stock_movements")
print(f"✓ stock_movements: {len(movements)}")

print(f"\n✓ All inventory_management tables created in {SCHEMA}")
