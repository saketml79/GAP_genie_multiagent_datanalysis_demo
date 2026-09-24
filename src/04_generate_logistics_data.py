# Databricks notebook source
# MAGIC %md
# MAGIC # Step 4: Generate Logistics Operations Data
# MAGIC Creates: carriers, distribution_centers, shipments, transit_data
# MAGIC
# MAGIC **Demo Story**: Western-bound shipments have 42%+ late rate, significant port congestion and carrier capacity issues.
# MAGIC
# MAGIC **IMPORTANT**: shipments uses `destination_region` / `origin_region` (not `region`).

# COMMAND ----------

dbutils.widgets.text("catalog_name", "GAP_Demo_Dev", "Catalog Name")
CATALOG = dbutils.widgets.get("catalog_name")
SCHEMA = f"{CATALOG}.logistics_operations"

# COMMAND ----------

import random
from datetime import datetime, timedelta

random.seed(44)  # Fixed seed for deterministic data generation
base_date = datetime(2026, 9, 1)  # Fixed reference date — data is always identical
regions = ['Western', 'Eastern', 'Central', 'Southern']
states_by_region = {
    'Western': ['CA','WA','OR','NV','AZ'], 'Eastern': ['NY','NJ','PA','MA','CT'],
    'Central': ['IL','OH','MI','IN','WI'], 'Southern': ['TX','FL','GA','NC','VA']
}

# ---- Carriers (8) ----
carriers_data = [
    {'carrier_id':'CAR-001','carrier_name':'FastFreight Logistics','carrier_type':'LTL','service_regions':'National','on_time_rate_pct':94.2,'avg_transit_days':3.5,'cost_per_mile':2.45},
    {'carrier_id':'CAR-002','carrier_name':'Pacific Express','carrier_type':'FTL','service_regions':'Western,Central','on_time_rate_pct':78.5,'avg_transit_days':5.2,'cost_per_mile':1.85},
    {'carrier_id':'CAR-003','carrier_name':'Atlantic Transport','carrier_type':'FTL','service_regions':'Eastern,Southern','on_time_rate_pct':91.8,'avg_transit_days':3.1,'cost_per_mile':2.10},
    {'carrier_id':'CAR-004','carrier_name':'MidWest Haulers','carrier_type':'LTL','service_regions':'Central,Southern','on_time_rate_pct':89.3,'avg_transit_days':4.0,'cost_per_mile':1.95},
    {'carrier_id':'CAR-005','carrier_name':'Global Parcel Co','carrier_type':'Parcel','service_regions':'National','on_time_rate_pct':96.1,'avg_transit_days':2.8,'cost_per_mile':3.20},
    {'carrier_id':'CAR-006','carrier_name':'Coast-to-Coast Freight','carrier_type':'FTL','service_regions':'National','on_time_rate_pct':85.7,'avg_transit_days':4.5,'cost_per_mile':1.75},
    {'carrier_id':'CAR-007','carrier_name':'RapidShip Inc','carrier_type':'Expedited','service_regions':'National','on_time_rate_pct':97.8,'avg_transit_days':1.5,'cost_per_mile':4.50},
    {'carrier_id':'CAR-008','carrier_name':'SunBelt Logistics','carrier_type':'LTL','service_regions':'Southern,Western','on_time_rate_pct':82.4,'avg_transit_days':4.8,'cost_per_mile':1.65},
]
spark.createDataFrame(carriers_data).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.carriers")
print(f"✓ carriers: {len(carriers_data)}")

# COMMAND ----------

# ---- Distribution Centers (7) ----
dc_data = [
    {'dc_id':'DC-W1','dc_name':'Los Angeles Hub','region':'Western','state':'CA','city':'Los Angeles','capacity_pallets':15000,'current_load_pct':92.3,'dock_doors':24,'is_cross_dock':True},
    {'dc_id':'DC-W2','dc_name':'Seattle Center','region':'Western','state':'WA','city':'Seattle','capacity_pallets':8000,'current_load_pct':88.1,'dock_doors':16,'is_cross_dock':False},
    {'dc_id':'DC-E1','dc_name':'New Jersey Mega','region':'Eastern','state':'NJ','city':'Edison','capacity_pallets':20000,'current_load_pct':71.5,'dock_doors':32,'is_cross_dock':True},
    {'dc_id':'DC-E2','dc_name':'Boston Fulfillment','region':'Eastern','state':'MA','city':'Boston','capacity_pallets':6000,'current_load_pct':65.8,'dock_doors':12,'is_cross_dock':False},
    {'dc_id':'DC-C1','dc_name':'Chicago Central','region':'Central','state':'IL','city':'Chicago','capacity_pallets':18000,'current_load_pct':74.2,'dock_doors':28,'is_cross_dock':True},
    {'dc_id':'DC-S1','dc_name':'Dallas Depot','region':'Southern','state':'TX','city':'Dallas','capacity_pallets':14000,'current_load_pct':68.9,'dock_doors':22,'is_cross_dock':True},
    {'dc_id':'DC-S2','dc_name':'Atlanta Gateway','region':'Southern','state':'GA','city':'Atlanta','capacity_pallets':10000,'current_load_pct':73.4,'dock_doors':18,'is_cross_dock':False},
]
spark.createDataFrame(dc_data).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.distribution_centers")
print(f"✓ distribution_centers: {len(dc_data)}")

# COMMAND ----------

# ---- Shipments (90 days, ~12K) ----
# NOTE: Uses destination_region and origin_region (NOT region)
wh_to_region = {
    'WH-001':'Western','WH-002':'Western','WH-003':'Western',
    'WH-004':'Eastern','WH-005':'Eastern','WH-006':'Eastern',
    'WH-007':'Central','WH-008':'Central','WH-009':'Central',
    'WH-010':'Southern','WH-011':'Southern','WH-012':'Southern'
}
carrier_ids = [c['carrier_id'] for c in carriers_data]
shipments = []
for day_offset in range(90):
    ship_date = base_date - timedelta(days=day_offset)
    for _ in range(random.randint(80, 200)):
        origin_wh = random.choice(list(wh_to_region.keys()))
        origin_region = wh_to_region[origin_wh]
        dest_region = random.choice(regions)
        planned_transit = random.randint(1, 7)
        if dest_region == 'Western' and day_offset < 30:
            actual_transit = planned_transit + random.randint(1, 5)
            delay_reason = random.choice(['Port_Congestion','Carrier_Capacity','Weather','Customs_Hold','Labor_Shortage'])
        elif origin_region == 'Western' and day_offset < 30:
            actual_transit = planned_transit + random.randint(0, 3)
            delay_reason = random.choice(['Warehouse_Congestion','Labor_Shortage','Carrier_Capacity',None])
        else:
            actual_transit = planned_transit + random.choices([0,0,0,1,2], weights=[50,20,15,10,5])[0]
            delay_reason = None if actual_transit <= planned_transit else random.choice(['Weather','Traffic','Carrier_Issue'])
        is_late = actual_transit > planned_transit
        delivery_date = ship_date + timedelta(days=actual_transit)
        status = 'Delivered' if delivery_date <= base_date else 'In_Transit'
        if random.random() < 0.02: status = 'Damaged'
        if random.random() < 0.01: status = 'Lost'
        shipments.append({
            'shipment_id': f'SHP-{len(shipments)+1:07d}',
            'ship_date': ship_date.strftime('%Y-%m-%d'),
            'expected_delivery_date': (ship_date + timedelta(days=planned_transit)).strftime('%Y-%m-%d'),
            'actual_delivery_date': delivery_date.strftime('%Y-%m-%d') if status == 'Delivered' else None,
            'origin_warehouse': origin_wh, 'origin_region': origin_region,
            'destination_region': dest_region,
            'destination_state': random.choice(states_by_region[dest_region]),
            'carrier_id': random.choice(carrier_ids),
            'shipment_type': random.choice(['Full_Truck','LTL','Parcel','Expedited']),
            'total_weight_kg': round(random.uniform(10, 5000), 1),
            'total_pallets': random.randint(1, 26),
            'planned_transit_days': planned_transit, 'actual_transit_days': actual_transit,
            'delay_days': max(0, actual_transit - planned_transit),
            'is_late': is_late, 'shipment_status': status,
            'delay_reason': delay_reason,
            'shipping_cost': round(random.uniform(50, 5000), 2),
            'order_count': random.randint(1, 50)
        })
spark.createDataFrame(shipments).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.shipments")
print(f"✓ shipments: {len(shipments)}")

# COMMAND ----------

# ---- Transit Data (tracking events) ----
event_types = ['Picked_Up','In_Transit','At_Hub','Out_For_Delivery','Delivered','Exception','Returned']
transit_events = []
for shp in random.sample(shipments, min(5000, len(shipments))):
    event_date = datetime.strptime(shp['ship_date'], '%Y-%m-%d')
    for j in range(random.randint(3, 7)):
        event_date = event_date + timedelta(hours=random.randint(4, 36))
        transit_events.append({
            'event_id': f'EVT-{len(transit_events)+1:08d}',
            'shipment_id': shp['shipment_id'],
            'event_timestamp': event_date.strftime('%Y-%m-%d %H:%M:%S'),
            'event_type': event_types[min(j, len(event_types)-1)],
            'location': f'{random.choice(["Hub","Terminal","DC","Station"])}_{random.randint(1,20)}',
            'region': shp['destination_region'],
            'notes': f'Shipment {event_types[min(j, len(event_types)-1)].lower().replace("_"," ")}' + (' - DELAYED' if shp['is_late'] and j > 1 else '')
        })
spark.createDataFrame(transit_events).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.transit_data")
print(f"✓ transit_data: {len(transit_events)}")

print(f"\n✓ All logistics_operations tables created in {SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Verify: logistics tables exist with rows
# ── Assertions ──
expected = ['shipments', 'carriers', 'distribution_centers', 'transit_data']
for t in expected:
    fqn = f"{SCHEMA}.{t}"
    assert spark.catalog.tableExists(fqn), f"MISSING table: {fqn}"
    cnt = spark.table(fqn).count()
    assert cnt > 0, f"EMPTY table: {fqn} (0 rows)"
print(f"✓ ASSERT PASS: all {len(expected)} logistics_operations tables exist with data")