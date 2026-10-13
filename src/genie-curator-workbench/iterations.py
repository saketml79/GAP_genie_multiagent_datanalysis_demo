"""Iteration logic for Genie Curator Workbench app.

Replicates the exact notebook cell logic (cells 10, 13, 19) as API calls.
Column/table comments applied directly via SQL Statement API (no external notebooks).
Agent cloning, instruction patches, example SQLs, and benchmarks use Genie API.
"""
import argparse, json, os, time, uuid, requests


def ensure_sorted_payload(ss):
    """Genie API rejects PATCHes if example_question_sqls or benchmarks unsorted by id."""
    if "instructions" in ss and "example_question_sqls" in ss.get("instructions", {}):
        ss["instructions"]["example_question_sqls"].sort(key=lambda x: str(x.get("id", "")))
    if "benchmarks" in ss and "questions" in ss.get("benchmarks", {}):
        ss["benchmarks"]["questions"].sort(key=lambda x: str(x.get("id", "")))
    return ss



def _run_comment_sql(host, headers, warehouse_id, statement, log_fn=None):
    """Run a SQL statement via the SQL Statement API (OBO)."""
    _log = log_fn or (lambda m: None)
    try:
        r = requests.post(f"{host}/api/2.0/sql/statements", headers=headers, json={
            "warehouse_id": warehouse_id, "statement": statement, "wait_timeout": "30s",
        }, timeout=(10, 60))  # (connect_timeout, read_timeout)
        if r.status_code == 200:
            data = r.json()
            state = data.get("status", {}).get("state", "UNKNOWN")
            if state == "SUCCEEDED":
                return True
            # Poll if still running (warehouse may be slow after rapid calls)
            if state in ("PENDING", "RUNNING"):
                stmt_id = data.get("statement_id")
                if stmt_id:
                    for _ in range(6):  # up to 30s more
                        time.sleep(5)
                        pr = requests.get(f"{host}/api/2.0/sql/statements/{stmt_id}",
                                          headers=headers, timeout=(10, 15))
                        if pr.status_code == 200:
                            ps = pr.json().get("status", {}).get("state", "")
                            if ps == "SUCCEEDED":
                                return True
                            if ps in ("FAILED", "CANCELED", "CLOSED"):
                                _log(f"  SQL {ps}: {statement[:60]}...")
                                return False
                _log(f"  SQL {state} (timed out): {statement[:60]}...")
                return False
            _log(f"  SQL {state}: {statement[:60]}...")
            return False
        else:
            _log(f"  SQL error {r.status_code}: {statement[:60]}...")
            return False
    except requests.exceptions.Timeout:
        _log(f"  SQL timeout: {statement[:60]}...")
        return False
    except Exception as e:
        _log(f"  SQL exception: {e}")
        return False


# -- Shared ownership: any app user can run iterations and teardown end to end --
SHARE_GROUP = "users"           # workspace group: CAN_MANAGE on every demo Genie agent
OWNER_GROUP = "account users"   # owner of every demo catalog object (UC needs an account-level group)


def share_agent_with_all_users(host, headers, space_id, log_fn=None):
    """Best-effort: CAN_MANAGE for all users on a Genie agent (so anyone can clone/edit/delete it).
    Never raises; a failure is logged and the iteration continues unchanged."""
    _log = log_fn or (lambda m: None)
    try:
        r = requests.patch(f"{host}/api/2.0/permissions/genie/{space_id}", headers=headers, timeout=30,
                           json={"access_control_list": [{"group_name": SHARE_GROUP, "permission_level": "CAN_MANAGE"}]})
        if r.status_code == 200:
            return True
        _log(f"  Note: could not share agent {str(space_id)[:12]} with all users ({r.status_code})")
    except Exception as e:
        _log(f"  Note: could not share agent {str(space_id)[:12]}: {e}")
    return False


def give_catalog_to_all_users(host, headers, catalog, warehouse_id, log_fn=None):
    """Best-effort: hand objects not yet owned by OWNER_GROUP (e.g. views/functions this iteration just
    created as the signed-in user) to OWNER_GROUP -- children first, then schemas. Never raises."""
    _log = log_fn or (lambda m: None)
    try:
        c = catalog.lower()
        q = (f"SELECT CASE WHEN table_type IN ('VIEW','METRIC_VIEW') THEN 'VIEW' "
             f"WHEN table_type = 'MATERIALIZED_VIEW' THEN 'MATERIALIZED VIEW' ELSE 'TABLE' END, "
             f"table_schema || '.' || table_name, 1 FROM `{c}`.information_schema.tables "
             f"WHERE table_schema <> 'information_schema' AND table_owner <> '{OWNER_GROUP}' "
             f"UNION ALL SELECT 'FUNCTION', routine_schema || '.' || routine_name, 1 FROM `{c}`.information_schema.routines "
             f"WHERE routine_owner <> '{OWNER_GROUP}' "
             f"UNION ALL SELECT 'SCHEMA', schema_name, 2 FROM `{c}`.information_schema.schemata "
             f"WHERE schema_name <> 'information_schema' AND schema_owner <> '{OWNER_GROUP}' ORDER BY 3")
        r = requests.post(f"{host}/api/2.0/sql/statements", headers=headers, timeout=(10, 60),
                          json={"warehouse_id": warehouse_id, "statement": q, "wait_timeout": "30s"})
        d = r.json() if r.status_code == 200 else {}
        if d.get("status", {}).get("state") != "SUCCEEDED":
            _log("  Note: could not list objects to hand to all users")
            return 0, 0
        rows = d.get("result", {}).get("data_array", []) or []
        ok = sum(1 for kind, name, _ in rows if _run_comment_sql(
            host, headers, warehouse_id, f"ALTER {kind} `{c}`.{name} OWNER TO `{OWNER_GROUP}`", log_fn))
        _log(f"Ownership: {ok}/{len(rows)} new objects handed to all users" if rows
             else "Ownership: all objects already shared with all users")
        return ok, len(rows)
    except Exception as e:
        _log(f"  Note: ownership hand-off skipped: {e}")
        return 0, 0


# -- Table & Column comment data (matches notebook cell 10 exactly) --
ITER1_TABLE_COMMENTS = {
    "demand_analysis.sales_orders": "Customer sales orders",
    "inventory_management.inventory_ledger": "Inventory position snapshots",
    "logistics_operations.shipments": "Outbound shipment records",
    "reporting.executive_kpis": "Executive KPI summary",
    "supplier_procurement.supplier_lead_times": "Monthly supplier lead times",
    "supplier_procurement.supplier_orders": "Supplier purchase orders",
    "supplier_procurement.vendor_slas": "Supplier SLA results",
}

ITER1_COLUMN_COMMENTS = {
    "demand_analysis.sales_orders": {
        "channel": "Sales channel", "customer_id": "Customer identifier",
        "fulfillment_warehouse": "Fulfillment warehouse", "order_date": "Order date",
        "order_id": "Order identifier", "order_status": "Order fulfillment status",
        "product_category": "Product category", "product_family": "Product family",
        "quantity": "Units ordered", "region": "Sales region", "sku_id": "SKU identifier",
        "state": "State code", "total_amount": "Total order value", "unit_price": "Unit selling price",
    },
    "inventory_management.inventory_ledger": {
        "allocated_qty": "Allocated units", "available_qty": "Available units",
        "below_safety_stock_flag": "Below safety stock", "days_of_supply": "Days of supply",
        "ledger_id": "Ledger identifier", "on_hand_qty": "On hand units",
        "product_category": "Product category", "product_family": "Product family",
        "region": "Inventory region", "reorder_point": "Reorder point units",
        "safety_stock_level": "Safety stock units", "sku_id": "SKU identifier",
        "snapshot_date": "Snapshot date", "stockout_flag": "Stockout status",
        "warehouse_id": "Warehouse identifier",
    },
    "logistics_operations.shipments": {
        "actual_delivery_date": "Actual delivery date", "actual_transit_days": "Actual transit days",
        "carrier_id": "Carrier identifier", "delay_days": "Delay days",
        "delay_reason": "Delay reason", "destination_region": "Destination region",
        "destination_state": "Destination state", "expected_delivery_date": "Expected delivery date",
        "is_late": "Late shipment flag", "order_count": "Orders in shipment",
        "origin_region": "Origin region", "origin_warehouse": "Origin warehouse",
        "planned_transit_days": "Planned transit days", "ship_date": "Shipment date",
        "shipment_id": "Shipment identifier", "shipment_status": "Shipment status",
        "shipment_type": "Shipment type", "shipping_cost": "Shipping cost",
        "total_pallets": "Total pallets", "total_weight_kg": "Total weight kilograms",
    },
    "reporting.executive_kpis": {
        "revenue_last_month": "Last month revenue", "revenue_prior_month": "Prior month revenue",
        "total_stockout_skus": "Stockout SKU count", "avg_days_of_supply": "Average days supply",
        "late_delivery_pct_last_month": "Late delivery percent",
        "avg_delay_days_last_month": "Average delay days",
        "supplier_late_pct_last_month": "Supplier late percent",
        "total_sla_breaches": "SLA breach count", "service_level_pct": "Service level percent",
    },
    "supplier_procurement.supplier_lead_times": {
        "avg_actual_lead_time_days": "Average actual lead time",
        "contracted_lead_time_days": "Contracted lead time", "defect_rate_pct": "Defect rate percent",
        "lead_time_id": "Lead time identifier", "lead_time_variance_days": "Lead time variance days",
        "month": "Reporting month", "on_time_delivery_pct": "On time percent",
        "order_count": "Order count", "supplier_continent": "Supplier continent",
        "supplier_country": "Supplier country", "supplier_id": "Supplier identifier",
        "supplier_name": "Supplier name",
    },
    "supplier_procurement.supplier_orders": {
        "actual_delivery_date": "Actual delivery date", "actual_lead_time_days": "Actual lead time",
        "contracted_lead_time_days": "Contracted lead time", "delay_reason": "Delay reason",
        "expected_delivery_date": "Expected delivery date", "is_late": "Late order flag",
        "lead_time_variance_days": "Lead time variance days", "order_date": "Purchase order date",
        "po_id": "Purchase order ID", "po_status": "Purchase order status",
        "product_family": "Product family", "quality_score": "Quality score",
        "quantity_ordered": "Units ordered", "quantity_received": "Units received",
        "supplier_continent": "Supplier continent", "supplier_country": "Supplier country",
        "supplier_id": "Supplier identifier", "supplier_name": "Supplier name",
        "total_cost": "Total order cost", "unit_cost": "Unit purchase cost",
    },
    "supplier_procurement.vendor_slas": {
        "actual_pct": "Actual SLA percent", "is_breached": "SLA breach flag",
        "last_updated": "Last update date", "penalty_amount": "Penalty amount",
        "review_period": "Review period", "sla_id": "SLA identifier",
        "sla_metric": "SLA metric name", "supplier_id": "Supplier identifier",
        "supplier_name": "Supplier name", "target_pct": "Target SLA percent",
        "variance_pct": "SLA variance percent",
    },
}


def _apply_comments_via_sql(host, headers, catalog, warehouse_id, log_fn=None):
    """Apply table and column comments via SQL Statement API.
    Mirrors notebook cell 10 Step 1 exactly."""
    _log = log_fn or (lambda m: None)
    # Table comments
    tbl_ok = 0
    for tbl, comment in ITER1_TABLE_COMMENTS.items():
        fqn = f"{catalog}.{tbl}"
        sql = f"COMMENT ON TABLE {fqn} IS '{comment}'"
        if _run_comment_sql(host, headers, warehouse_id, sql, log_fn):
            tbl_ok += 1
    _log(f"Table comments: {tbl_ok}/{len(ITER1_TABLE_COMMENTS)} applied")
    # Column comments
    col_total, col_ok = 0, 0
    for tbl_idx, (tbl, cols) in enumerate(ITER1_COLUMN_COMMENTS.items(), 1):
        fqn = f"{catalog}.{tbl}"
        tbl_col_ok = 0
        for col_name, col_comment in cols.items():
            col_total += 1
            sql = f"ALTER TABLE {fqn} ALTER COLUMN {col_name} COMMENT '{col_comment}'"
            if _run_comment_sql(host, headers, warehouse_id, sql):
                col_ok += 1; tbl_col_ok += 1
            else:
                # Fallback for views: ALTER VIEW syntax
                sql_v = f"ALTER VIEW {fqn} ALTER COLUMN {col_name} COMMENT '{col_comment}'"
                if _run_comment_sql(host, headers, warehouse_id, sql_v):
                    col_ok += 1; tbl_col_ok += 1
        _log(f"Column comments {tbl_idx}/{len(ITER1_COLUMN_COMMENTS)}: {tbl.split('.')[-1]} {tbl_col_ok}/{len(cols)} ✓")
    _log(f"Column comments: {col_ok}/{col_total} applied")
    return tbl_ok, col_ok, col_total


def clone_agents(host, headers, source_spaces, iteration_label, warehouse_id, log_fn=None, user_alias=None):
    """Clone 5 domain agents from source into new agents with iteration label.
    IDEMPOTENT: if agents with the iteration label already exist, reuse them.
    user_alias: if provided, agent titles include '(alias)' suffix for per-user sandboxing."""
    _base = {
        "demand": "SC - Demand Analysis",
        "inventory": "SC - Inventory Management",
        "logistics": "SC - Logistics Operations",
        "supplier": "SC - Supplier Risk",
        "executive": "SC - Executive Reporting",
    }
    title_prefix = {k: f"{v} ({user_alias})" for k, v in _base.items()} if user_alias else _base
    new_spaces, log = {}, []
    _log = log_fn or (lambda m: None)

    # Idempotency: check for existing agents with this label
    existing = {}
    try:
        all_spaces = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers, timeout=15).json()
        for s in all_spaces.get("spaces", []):
            title = s.get("title", "")
            if iteration_label in title and "SC" in title:
                for agent_name, prefix in title_prefix.items():
                    if prefix in title:
                        existing[agent_name] = s["space_id"]
    except Exception as e:
        _log(f"  Warning: could not check existing agents: {e}")

    for i, (agent_name, source_sid) in enumerate(source_spaces.items(), 1):
        # Reuse existing agent if found (idempotent re-run)
        if agent_name in existing:
            new_spaces[agent_name] = existing[agent_name]
            share_agent_with_all_users(host, headers, existing[agent_name], _log)
            _log(f"Reusing existing {agent_name} - {iteration_label} ({existing[agent_name][:16]}...)")
            log.append(f"  [=] {agent_name}: reused existing {existing[agent_name][:20]}...")
            continue

        _log(f"Cloning agent {i}/5: {agent_name}...")
        new_title = f"{title_prefix.get(agent_name, agent_name)} - {iteration_label}"
        r = requests.get(f"{host}/api/2.0/genie/spaces/{source_sid}?include_serialized_space=true", headers=headers, timeout=30)
        if r.status_code != 200:
            log.append(f"  [!] {agent_name}: GET failed {r.status_code}")
            continue
        source_ss = json.loads(r.json().get("serialized_space", "{}"))
        source_desc = r.json().get("description", f"{agent_name} agent")
        table_ids = [t["identifier"] for t in source_ss.get("data_sources", {}).get("tables", [])]
        cr = requests.post(f"{host}/api/2.0/genie/spaces", headers=headers, json={
            "title": new_title, "description": source_desc, "warehouse_id": warehouse_id,
            "table_identifiers": table_ids, "serialized_space": json.dumps({"version": 2}),
        }, timeout=30)
        if cr.status_code not in (200, 201):
            log.append(f"  [!] {agent_name}: create failed {cr.status_code}")
            continue
        new_sid = cr.json().get("space_id") or cr.json().get("id")
        ok = False
        for attempt in range(5):
            time.sleep(3 + attempt)
            pr = requests.patch(f"{host}/api/2.0/genie/spaces/{new_sid}", headers=headers,
                                json={"serialized_space": json.dumps(source_ss)}, timeout=30)
            if pr.status_code == 200:
                vr = requests.get(f"{host}/api/2.0/genie/spaces/{new_sid}?include_serialized_space=true", headers=headers, timeout=30)
                vss = json.loads(vr.json().get("serialized_space", "{}"))
                if len(vss.get("data_sources", {}).get("tables", [])) > 0:
                    ok = True
                    break
        if ok:
            new_spaces[agent_name] = new_sid
            share_agent_with_all_users(host, headers, new_sid, _log)
            log.append(f"  [+] {agent_name}: {new_title} ({new_sid[:20]}...)")
            time.sleep(3)  # Let API settle between clones to avoid rate-limiting on agent 5
        else:
            log.append(f"  [!] {agent_name}: PATCH failed after 5 retries")
    return new_spaces, log



SUPERVISOR_DISPLAY_NAME = "Supply Chain Control Tower"

def _sup_display_name(user_alias=None):
    """Per-user supervisor name: 'Supply Chain Control Tower (ssuman_bpcs_com)'."""
    if user_alias:
        return f"{SUPERVISOR_DISPLAY_NAME} ({user_alias})"
    return SUPERVISOR_DISPLAY_NAME
SUPERVISOR_DESCRIPTION = (
    "Multi-agent supply chain analysis system that coordinates demand, inventory, logistics, "
    "supplier, and executive reporting agents."
)
SUPERVISOR_BASE_INSTRUCTIONS = """You are a Supply Chain Control Tower Supervisor Agent. You coordinate 5 specialist Genie agents to deliver comprehensive supply chain analysis.

AGENTS & DOMAINS:
- demand-analysis: Sales orders, revenue, demand forecasts, product performance, customer segmentation. Key data: sales_orders (order_date, region, total_amount, order_status, product_family), demand_forecasts, products. Ask about revenue, MoM changes, order volumes, fulfillment rates.
- inventory-management: Inventory levels, safety stock, stockouts, days of supply, warehouse capacity. Key data: inventory_ledger (sku_id, region, on_hand_qty, below_safety_stock_flag, stockout_flag, days_of_supply). Ask about stock health, at-risk positions, SKU counts.
- logistics-operations: Shipment performance, delivery timeliness, freight costs, carriers. Key data: shipments (ship_date, destination_region, is_late, delay_days, shipping_cost). Ask about on-time delivery rates, average delays, late shipment counts.
- supplier-risk: Supplier performance, purchase orders, lead times, quality, SLA compliance. Key data: supplier_orders (lead_time_variance_days, is_late, quality_score), vendor_slas (penalty_amount, is_breached). Ask about vendor late rates, lead time variance, SLA penalties.
- executive-reporting: Company-wide KPIs, regional summaries, revenue trends, fiscal targets, risk scorecards. Key data: executive_kpis, regional_performance_summary, revenue_trend, supply_chain_risk_scorecard. Ask about service levels, quarterly targets, cross-domain summaries.

ROUTING RULES:
1. Route each metric to the MOST specific domain agent. Never ask one agent about another's domain.
2. For cross-domain questions, query EACH relevant agent separately, then synthesize their answers.
3. Ask agents precise, single-metric questions. Avoid compound questions to a single agent.
4. Include the SQL each agent used so the user can verify the numbers.
5. If any agent provides forecasted or projected values, clearly label them as projections separate from confirmed actuals.
6. If an agent reports it cannot answer, report the gap. Do NOT fabricate data."""
SUPERVISOR_ITER3_BLOCK = """ITERATION 3 CAPABILITIES
- Your sub-agents have been enhanced with UC semantic features including metric views, SQL functions, certified queries, column comments, and governed tags. They discover and use these features autonomously to answer questions about business terms, policy thresholds, and cross-domain metrics.
- For cross-domain questions spanning multiple schemas (e.g. Cost of Disruption, revenue at risk), route to executive-reporting first — it has pre-built cross-domain views.
- For domain-specific policy terms (e.g. Logistics Risk Standards, Demand Anomaly Alert, Procurement Quality Minimum), route to the owning domain agent — it has the context to resolve the term.
- This organization uses a July fiscal year start. Q1 = Jul-Sep (current quarter). For Q1 target questions, route to executive-reporting."""
SUPERVISOR_SQL_GUIDANCE = """SQL THRESHOLD FUNCTIONS (ITERATION 3)
You have 5 SQL threshold functions available as direct tools. Each returns the BUSINESS DEFINITION of a domain-specific policy standard — the concept name, source table, threshold columns, and conditions. They do NOT return data or counts.
When asked about policy standards or "critical" thresholds, call the relevant function to learn the definition, then query the source table with the conditions it describes:
- get_critical_delay_shipments: Defines what Logistics Risk Standards means
- get_critical_accuracy_forecasts: Defines what Demand Anomaly Alert means
- get_critical_supply_positions: Defines what Inventory Risk Classification means
- get_critical_quality_orders: Defines what Procurement Quality Minimum means
- get_critical_disruption_regions: Defines what Executive Disruption Threshold means
Do NOT guess thresholds — always call the function first to learn the correct conditions."""
SUPERVISOR_TOOL_MAP = {
    "demand": {
        "tool_id": "demand-analysis",
        "space_prefix": "SC - Demand Analysis",
        "description": "Demand Analysis agent. Answers questions about sales, revenue, and demand forecasts.",
    },
    "inventory": {
        "tool_id": "inventory-management",
        "space_prefix": "SC - Inventory Management",
        "description": "Inventory Management agent. Answers questions about inventory levels, stockouts, and warehouse data.",
    },
    "logistics": {
        "tool_id": "logistics-operations",
        "space_prefix": "SC - Logistics Operations",
        "description": "Logistics Operations agent. Answers questions about shipments, deliveries, and transit data.",
    },
    "supplier": {
        "tool_id": "supplier-risk",
        "space_prefix": "SC - Supplier Risk",
        "description": "Supplier Risk agent. Answers questions about supplier performance, lead times, and procurement.",
    },
    "executive": {
        "tool_id": "executive-reporting",
        "space_prefix": "SC - Executive Reporting",
        "description": "Executive Reporting agent. Answers questions about KPIs, regional performance, and dashboards.",
    },
}
ITER3_SQL_FUNCTION_TOOLS = [  # schema.function -- catalog is prefixed at call time (UC needs 3-part names)
    {"name": "logistics_operations.get_critical_delay_shipments",
     "description": "Returns the business definition of Logistics Risk Standards — which columns and conditions define a flagged shipment. Does NOT return data; agent must query the source table itself."},
    {"name": "demand_analysis.get_critical_accuracy_forecasts",
     "description": "Returns the business definition of Demand Anomaly Alert — which columns and conditions define an anomalous order. Does NOT return data; agent must query the source table itself."},
    {"name": "inventory_management.get_critical_supply_positions",
     "description": "Returns the business definition of Inventory Risk Classification — which columns and conditions define a supply-risk position. Does NOT return data; agent must query the source table itself."},
    {"name": "supplier_procurement.get_critical_quality_orders",
     "description": "Returns the business definition of Procurement Quality Minimum — which columns and conditions define a below-minimum order. Does NOT return data; agent must query the source table itself."},
    {"name": "reporting.get_critical_disruption_regions",
     "description": "Returns the business definition of Executive Disruption Threshold — which columns and conditions define an at-risk supplier. Does NOT return data; agent must query the source table itself."},
]

def _iteration_label(iteration_label):
    s = str(iteration_label).strip()
    return s if s.lower().startswith("iteration ") else f"Iteration {s}"


def _discover_iteration_spaces(host, headers, iteration_label, user_alias=None):
    label = _iteration_label(iteration_label)
    found, newest = {}, {}
    page_token = None
    for _ in range(100):  # Genie list API returns 20 agents per page -- read every page
        params = {"page_size": 100, **({"page_token": page_token} if page_token else {})}
        r = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers, params=params, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"List agents failed: {r.status_code} {r.text[:200]}")
        body = r.json()
        for sp in body.get("spaces", []):
            title = sp.get("title", "")
            if label not in title:
                continue
            for agent_name, meta in SUPERVISOR_TOOL_MAP.items():
                # Match user-scoped titles: "SC - Demand Analysis (alias) - Iteration 1"
                prefix = f"{meta['space_prefix']} ({user_alias})" if user_alias else meta['space_prefix']
                base = f"{prefix} - {label}"
                # exact title, or Genie's duplicate-title suffix (" 2026-10-11 20:36:20"); newest wins
                if title == base or title.startswith(base + " 20"):
                    ct = sp.get("create_time") or ""
                    if ct >= newest.get(agent_name, ""):
                        newest[agent_name] = ct
                        found[agent_name] = sp.get("space_id") or sp.get("id")
        page_token = body.get("next_page_token")
        if not page_token:
            break
    missing = [a for a in SUPERVISOR_TOOL_MAP if a not in found]
    if missing:
        raise RuntimeError(f"Missing {label} agents for alias={user_alias or '(none)'}: {', '.join(missing)}")
    return found


def _get_or_create_supervisor(host, headers, log_fn=None, user_alias=None):
    _log = log_fn or (lambda m: None)
    display = _sup_display_name(user_alias)
    r = requests.get(f"{host}/api/2.1/supervisor-agents", headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"List supervisors failed: {r.status_code} {r.text[:200]}")
    for sup in r.json().get("supervisor_agents", []):
        if sup.get("display_name") == display:
            return sup["name"], False
    # Include alias scoping in initial instructions when creating
    _init_instructions = SUPERVISOR_BASE_INSTRUCTIONS
    if user_alias:
        alias_hint = (f"USER CONTEXT: The current user is ({user_alias}). "
                      f"ONLY use Genie agent tools whose names contain ({user_alias}). "
                      f"Ignore any agents that do not include ({user_alias}) in their name.")
        _init_instructions = alias_hint + "\n\n" + _init_instructions
    cr = requests.post(
        f"{host}/api/2.1/supervisor-agents",
        headers=headers,
        json={
            "display_name": display,
            "description": SUPERVISOR_DESCRIPTION,
            "instructions": _init_instructions,
        },
        timeout=30,
    )
    if cr.status_code not in (200, 201):
        raise RuntimeError(f"Create supervisor failed: {cr.status_code} {cr.text[:200]}")
    name = cr.json().get("name", "")
    _log(f"  Supervisor created: {name} (display={display})")
    return name, True


def _set_supervisor_instructions(host, headers, sup_name, include_iter3_sql, log_fn=None, user_alias=None):
    _log = log_fn or (lambda m: None)
    instructions = SUPERVISOR_BASE_INSTRUCTIONS
    if include_iter3_sql:
        instructions = (instructions.rstrip() + "\n\n" + SUPERVISOR_ITER3_BLOCK.strip()
                        + "\n\n" + SUPERVISOR_SQL_GUIDANCE.strip())
    # Multi-user isolation: tell the supervisor which user's agents to call
    if user_alias:
        alias_hint = (f"\n\nUSER CONTEXT: The current user is ({user_alias}). "
                      f"ONLY use Genie agent tools whose names contain ({user_alias}). "
                      f"Ignore any agents that do not include ({user_alias}) in their name.")
        instructions = alias_hint.lstrip("\n") + "\n\n" + instructions
    pr = requests.patch(
        f"{host}/api/2.1/{sup_name}?update_mask=description,instructions",
        headers=headers,
        json={"description": SUPERVISOR_DESCRIPTION, "instructions": instructions},
        timeout=30,
    )
    if pr.status_code != 200:
        raise RuntimeError(f"Patch supervisor failed: {pr.status_code} {pr.text[:200]}")
    _log(f"  Supervisor instructions updated ✓" + (f" (scoped to {user_alias})" if user_alias else ""))


def _replace_supervisor_tools(host, headers, sup_name, spaces, include_iter3_sql, log_fn=None, catalog=None):
    _log = log_fn or (lambda m: None)
    tr = requests.get(f"{host}/api/2.1/{sup_name}/tools", headers=headers, timeout=30)
    if tr.status_code != 200:
        raise RuntimeError(f"List supervisor tools failed: {tr.status_code} {tr.text[:200]}")
    deleted = 0
    for t in tr.json().get("tools", []):
        if t.get("tool_type") not in ("genie_space", "uc_function"):
            continue
        tid = t.get("tool_id")
        if not tid:
            continue
        dr = requests.delete(f"{host}/api/2.1/{sup_name}/tools/{tid}", headers=headers, timeout=30)
        if dr.status_code in (200, 204):
            deleted += 1
    if deleted:
        _log(f"  Supervisor tools cleared: {deleted}")
    for agent_name in ["demand", "inventory", "logistics", "supplier", "executive"]:
        meta = SUPERVISOR_TOOL_MAP[agent_name]
        sid = spaces[agent_name]
        cr = requests.post(
            f"{host}/api/2.1/{sup_name}/tools",
            headers=headers,
            params={"tool_id": meta["tool_id"]},
            json={
                "tool_type": "genie_space",
                "description": meta["description"],
                "genie_space": {"id": sid},
            },
            timeout=30,
        )
        if cr.status_code not in (200, 201):
            raise RuntimeError(f"Create tool {meta['tool_id']} failed: {cr.status_code} {cr.text[:200]}")
        _log(f"  Tool ✓ {meta['tool_id']} -> {sid[:16]}...")
    if include_iter3_sql:
        if not catalog:
            raise RuntimeError("catalog is required to register Iteration 3 SQL function tools")
        for func in ITER3_SQL_FUNCTION_TOOLS:
            full_name = f"{catalog}.{func['name']}"
            func_slug = func["name"].split(".")[-1].replace("_", "-")
            cr = requests.post(
                f"{host}/api/2.1/{sup_name}/tools",
                headers=headers,
                params={"tool_id": func_slug},
                json={
                    "tool_type": "uc_function",
                    "uc_function": {"name": full_name},
                    "description": func["description"]},
                timeout=30,
            )
            if cr.status_code not in (200, 201):
                raise RuntimeError(f"Create tool {func_slug} failed: {cr.status_code} {cr.text[:200]}")
        _log(f"  Supervisor SQL function tools: {len(ITER3_SQL_FUNCTION_TOOLS)} added ✓")


def refresh_supervisor_for_iteration(host, headers, iteration_label, log_fn=None, catalog=None, user_alias=None):
    _log = log_fn or (lambda m: None)
    label = _iteration_label(iteration_label)
    include_iter3_sql = label == "Iteration 3"
    spaces = _discover_iteration_spaces(host, headers, label, user_alias=user_alias)
    _log(f"Refreshing supervisor for {label}...")
    sup_name, created = _get_or_create_supervisor(host, headers, log_fn=_log, user_alias=user_alias)
    if not created:
        _log(f"  Supervisor found: {sup_name}")
    _set_supervisor_instructions(host, headers, sup_name, include_iter3_sql, log_fn=_log, user_alias=user_alias)
    _replace_supervisor_tools(host, headers, sup_name, spaces, include_iter3_sql, log_fn=_log, catalog=catalog)
    _log(f"Supervisor ready for {label} ✓")
    return {"supervisor_name": sup_name, "created": created, "spaces": spaces}


def update_supervisor_tools(host, headers, spaces):
    """Compatibility helper: reset the main supervisor's genie-space tools."""
    log = []
    sup_name, _ = _get_or_create_supervisor(host, headers, log_fn=log.append)
    _set_supervisor_instructions(host, headers, sup_name, False, log_fn=log.append)
    _replace_supervisor_tools(host, headers, sup_name, spaces, False, log_fn=log.append)
    return log



def _patch_agent_instructions(host, headers, spaces, safeguard_text, agent_context, log_fn=None):
    """Append safeguard + table context instructions to each agent."""
    log = []
    _log = log_fn or (lambda m: None)
    MARKER = "SAFEGUARDS:"
    for i, (agent_name, sid) in enumerate(spaces.items(), 1):
        _log(f"Patching agent {i}/{len(spaces)}: {agent_name} safeguards...")
        r = requests.get(f"{host}/api/2.0/genie/spaces/{sid}?include_serialized_space=true", headers=headers, timeout=30)
        if r.status_code != 200:
            log.append(f"  [!] {agent_name}: GET failed")
            continue
        ss = json.loads(r.json().get("serialized_space", "{}"))
        instrs = ss.get("instructions", {})
        if not isinstance(instrs, dict):
            instrs = {"text_instructions": [{"content": [str(instrs)]}]}
        ti = instrs.get("text_instructions", [{"content": []}])
        if not ti:
            ti = [{"content": []}]
        if MARKER in str(ti[0].get("content", [])):
            log.append(f"  (skip) {agent_name}: already has safeguards")
            _log(f"  ✓ {agent_name}: safeguards already applied")
            continue
        ti[0]["content"].append(safeguard_text)
        ctx = agent_context.get(agent_name, "")
        if ctx:
            ti[0]["content"].append(ctx)
        instrs["text_instructions"] = ti
        ss["instructions"] = instrs
        pr = requests.patch(f"{host}/api/2.0/genie/spaces/{sid}", headers=headers,
                            json={"serialized_space": json.dumps(ss)}, timeout=30)
        if pr.status_code == 200:
            log.append(f"  [+] {agent_name}: safeguards added")
            _log(f"  ✓ {agent_name}: safeguards added")
        else:
            log.append(f"  [!] {agent_name}: PATCH failed {pr.status_code}")
            _log(f"  ✗ {agent_name}: PATCH failed {pr.status_code}")
    return log


def _add_example_sqls(host, headers, spaces, example_sqls_by_agent, log_fn=None):
    """Add example SQL queries to agents via serialized_space."""
    log = []
    _log = log_fn or (lambda m: None)
    total = len(example_sqls_by_agent)
    for idx, (agent_name, examples) in enumerate(example_sqls_by_agent.items(), 1):
        sid = spaces.get(agent_name)
        if not sid:
            continue
        _log(f"Example SQLs {idx}/{total}: {agent_name} ({len(examples)} queries)...")
        r = requests.get(f"{host}/api/2.0/genie/spaces/{sid}?include_serialized_space=true", headers=headers, timeout=30)
        if r.status_code != 200:
            log.append(f"  [!] {agent_name}: GET failed")
            continue
        ss = json.loads(r.json().get("serialized_space", "{}"))
        if "instructions" not in ss:
            ss["instructions"] = {}
        existing = ss["instructions"].get("example_question_sqls", [])
        existing.extend(examples)
        ss["instructions"]["example_question_sqls"] = existing
        ensure_sorted_payload(ss)
        pr = requests.patch(f"{host}/api/2.0/genie/spaces/{sid}", headers=headers,
                            json={"serialized_space": json.dumps(ss)}, timeout=30)
        if pr.status_code == 200:
            _log(f"  ✓ {agent_name}: {len(examples)} example SQLs added")
            log.append(f"  [+] {agent_name}: {len(examples)} example SQLs added")
        else:
            _log(f"  ERROR: {agent_name} PATCH failed {pr.status_code}")
            log.append(f"  [!] {agent_name}: PATCH failed {pr.status_code} {pr.text[:120]}")
    return log


def _add_benchmarks(host, headers, spaces, benchmarks_by_agent, log_fn=None):
    """Add benchmark questions to agents via serialized_space."""
    log = []
    _log = log_fn or (lambda m: None)
    total = len(benchmarks_by_agent)
    for idx, (agent_name, bms) in enumerate(benchmarks_by_agent.items(), 1):
        sid = spaces.get(agent_name)
        if not sid:
            continue
        _log(f"Benchmarks {idx}/{total}: {agent_name} ({len(bms)} questions)...")
        r = requests.get(f"{host}/api/2.0/genie/spaces/{sid}?include_serialized_space=true", headers=headers, timeout=30)
        if r.status_code != 200:
            continue
        ss = json.loads(r.json().get("serialized_space", "{}"))
        existing = ss.get("benchmarks", {}).get("questions", [])
        existing.extend(bms)
        ss["benchmarks"] = {"questions": existing}
        ensure_sorted_payload(ss)
        pr = requests.patch(f"{host}/api/2.0/genie/spaces/{sid}", headers=headers,
                            json={"serialized_space": json.dumps(ss)}, timeout=30)
        if pr.status_code == 200:
            _log(f"  ✓ {agent_name}: {len(bms)} benchmarks added")
            log.append(f"  [+] {agent_name}: {len(bms)} benchmarks added")
        else:
            _log(f"  ERROR: {agent_name} PATCH failed {pr.status_code}")
            log.append(f"  [!] {agent_name}: PATCH failed {pr.status_code}")
    return log


# ============================================================
# ITERATION 1 DATA — exact copy from notebook cell 10
# ============================================================

SAFEGUARD_TEXT = (
    "SAFEGUARDS:\n"
    "- If a question is outside your domain, say 'This question is outside my area of expertise' "
    "and suggest which domain agent might help.\n"
    "- Do not guess or fabricate data. If you cannot find the answer, say so clearly.\n"
    "- Date context (MANDATORY, overrides the real system date): the current month is ALWAYS September 2026, "
    "regardless of today's actual date. Never use the real current month. 'Last month' ALWAYS means "
    "August 2026: <date column> >= DATE '2026-08-01' AND <date column> < DATE '2026-09-01'. "
    "'Prior month' = July 2026. September 2026 is never 'last month'.\n"
    "- Always use explicit date literals (DATE '2026-08-01'). Never use CURRENT_DATE().\n"
    "- Region values: Western, Eastern, Central, Southern. 'West' maps to 'Western'."
)

AGENT_TABLE_CONTEXT = {
    "demand": "YOUR TABLES: sales_orders (primary), demand_forecasts, customer_segments, pos_data, products, promotions.\n- total_amount is the revenue column. Use order_date for date filtering.\n- order_status values: Fulfilled, Partially_Fulfilled, Backordered, Cancelled.",
    "inventory": "YOUR TABLES: inventory_ledger (primary), stock_movements, store_inventory, warehouse_data.\n- below_safety_stock_flag and stockout_flag are boolean columns.\n- Use region column for regional inventory questions.",
    "logistics": "YOUR TABLES: shipments (primary), carriers, distribution_centers, transit_data.\n- Use ship_date for date filtering, not actual_delivery_date.\n- Use destination_region for delivery region. is_late = true means delayed.",
    "supplier": "YOUR TABLES: supplier_orders (primary), supplier_lead_times, suppliers, vendor_slas, procurement_data.\n- Use supplier_orders for per-order metrics like lead time variance.\n- supplier_lead_times is pre-aggregated monthly data.\n- Use order_date for date filtering.",
    "executive": "YOUR TABLES: executive_kpis, supply_chain_risk_scorecard, regional_performance_summary, revenue_trend.\n- service_level_pct is the current service level.\n- Use pre-built views for cross-domain questions when available.",
}


def _iter1_example_sqls(CAT):
    return {
        "supplier": [
            {"id": uuid.uuid4().hex, "question": ["What is the average lead time variance in days for all suppliers last month?"],
             "sql": [f"SELECT ROUND(AVG(lead_time_variance_days), 2) as avg_lead_time_variance FROM {CAT}.supplier_procurement.supplier_orders WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'"],
             "usage_guidance": ["ALWAYS use supplier_orders for lead time variance, NEVER supplier_lead_times."]},
            {"id": uuid.uuid4().hex, "question": ["What is the average lead time variance for Asia suppliers last month?"],
             "sql": [f"SELECT ROUND(AVG(lead_time_variance_days), 2) as avg_lead_time_variance FROM {CAT}.supplier_procurement.supplier_orders WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01' AND supplier_continent = 'Asia'"],
             "usage_guidance": ["Filter by supplier_continent for continent-specific LTV. Values: Asia, Europe, North America."]},
            {"id": uuid.uuid4().hex, "question": ["What percentage of vendors had delayed deliveries last month?", "What percentage of purchase orders were late last month?"],
             "sql": [f"SELECT ROUND(100.0 * SUM(CASE WHEN is_late THEN 1 ELSE 0 END) / COUNT(*), 2) as vendor_late_pct FROM {CAT}.supplier_procurement.supplier_orders WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'"],
             "usage_guidance": ["Vendor delayed % = per ORDER (delayed orders / total orders), not per distinct vendor."]},
        ],
        "demand": [
            {"id": uuid.uuid4().hex, "question": ["How many Western region orders were fulfilled last month?"],
             "sql": [f"SELECT COUNT(*) as fulfilled_orders FROM {CAT}.demand_analysis.sales_orders WHERE order_status = 'Fulfilled' AND region = 'Western' AND order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'"],
             "usage_guidance": ["Fulfilled = ONLY order_status = 'Fulfilled'. Do NOT include Partially_Fulfilled."]},
            {"id": uuid.uuid4().hex, "question": ["What is the order fulfillment rate for Western region last month?"],
             "sql": [f"SELECT ROUND(100.0 * SUM(CASE WHEN order_status = 'Fulfilled' THEN 1 ELSE 0 END) / COUNT(*), 2) as fulfillment_rate FROM {CAT}.demand_analysis.sales_orders WHERE region = 'Western' AND order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'"],
             "usage_guidance": ["Fulfillment rate = Fulfilled / total orders. Partially_Fulfilled is NOT fulfilled."]},
            {"id": uuid.uuid4().hex, "question": ["Which product family had the largest revenue decline in Western region last month vs prior month?"],
             "sql": [f"WITH monthly AS (SELECT product_family, SUM(CASE WHEN order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01' THEN total_amount ELSE 0 END) as aug_rev, SUM(CASE WHEN order_date >= DATE '2026-07-01' AND order_date < DATE '2026-08-01' THEN total_amount ELSE 0 END) as jul_rev FROM {CAT}.demand_analysis.sales_orders WHERE region = 'Western' GROUP BY product_family) SELECT product_family, ROUND(aug_rev - jul_rev, 2) as revenue_change FROM monthly ORDER BY revenue_change ASC LIMIT 1"],
             "usage_guidance": ["Compare Aug 2026 vs Jul 2026 revenue per product_family. Most negative = largest decline."]},
        ],
        "logistics": [
            {"id": uuid.uuid4().hex, "question": ["What is the delayed shipment rate for Western region last month?", "What is the late delivery rate for Western region shipments last month?"],
             "sql": [f"SELECT ROUND(AVG(CASE WHEN is_late = true THEN 1.0 ELSE 0.0 END) * 100, 2) as late_delivery_rate FROM {CAT}.logistics_operations.shipments WHERE destination_region = 'Western' AND ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'"],
             "usage_guidance": ["ALWAYS filter by ship_date, use destination_region, treat delayed=is_late=true. NEVER use CURRENT_DATE()."]},
            {"id": uuid.uuid4().hex, "question": ["What is the average delay in days for delayed shipments in the Western region last month?"],
             "sql": [f"SELECT ROUND(AVG(CASE WHEN is_late THEN delay_days END), 2) as avg_delay_days FROM {CAT}.logistics_operations.shipments WHERE destination_region = 'Western' AND ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'"],
             "usage_guidance": ["AVG(CASE WHEN is_late THEN delay_days END). Do NOT add delay_days IS NOT NULL. Use ship_date."]},
            {"id": uuid.uuid4().hex, "question": ["What is the on-time delivery rate for Western region last month?"],
             "sql": [f"SELECT ROUND(AVG(CASE WHEN is_late = false THEN 1.0 ELSE 0.0 END) * 100, 2) as otd_rate FROM {CAT}.logistics_operations.shipments WHERE destination_region = 'Western' AND ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'"],
             "usage_guidance": ["OTD rate = % where is_late = false. Use ship_date, destination_region."]},
        ],
    }


def _iter1_benchmarks(CAT):
    return {
        "supplier": [
            {"id": uuid.uuid4().hex, "question": ["What is the average lead time variance in days for all suppliers last month?"],
             "answer": [{"format": "SQL", "content": [f"SELECT ROUND(AVG(lead_time_variance_days), 2) FROM {CAT}.supplier_procurement.supplier_orders WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'"]}]},
            {"id": uuid.uuid4().hex, "question": ["What percentage of purchase orders were late last month?"],
             "answer": [{"format": "SQL", "content": [f"SELECT ROUND(100.0 * SUM(CASE WHEN is_late THEN 1 ELSE 0 END) / COUNT(*), 2) FROM {CAT}.supplier_procurement.supplier_orders WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'"]}]},
            {"id": uuid.uuid4().hex, "question": ["What is the average lead time variance for Europe suppliers last month?"],
             "answer": [{"format": "SQL", "content": [f"SELECT ROUND(AVG(lead_time_variance_days), 2) FROM {CAT}.supplier_procurement.supplier_orders WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01' AND supplier_continent = 'Europe'"]}]},
        ],
        "demand": [
            {"id": uuid.uuid4().hex, "question": ["How many Western region orders were fulfilled last month?"],
             "answer": [{"format": "SQL", "content": [f"SELECT COUNT(*) FROM {CAT}.demand_analysis.sales_orders WHERE order_status = 'Fulfilled' AND region = 'Western' AND order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'"]}]},
            {"id": uuid.uuid4().hex, "question": ["What is the order fulfillment rate for Western region last month?"],
             "answer": [{"format": "SQL", "content": [f"SELECT ROUND(100.0 * SUM(CASE WHEN order_status = 'Fulfilled' THEN 1 ELSE 0 END) / COUNT(*), 2) FROM {CAT}.demand_analysis.sales_orders WHERE region = 'Western' AND order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'"]}]},
        ],
        "logistics": [
            {"id": uuid.uuid4().hex, "question": ["What is the late delivery rate for Western region shipments last month?"],
             "answer": [{"format": "SQL", "content": [f"SELECT ROUND(AVG(CASE WHEN is_late = true THEN 1.0 ELSE 0.0 END) * 100, 2) FROM {CAT}.logistics_operations.shipments WHERE destination_region = 'Western' AND ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'"]}]},
            {"id": uuid.uuid4().hex, "question": ["What is the average delay days for late shipments in Western region last month?"],
             "answer": [{"format": "SQL", "content": [f"SELECT ROUND(AVG(CASE WHEN is_late THEN delay_days END), 2) FROM {CAT}.logistics_operations.shipments WHERE destination_region = 'Western' AND ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'"]}]},
        ],
    }


# ============================================================
# MAIN ENTRY POINT: run_iteration_1
# ============================================================

def run_iteration_1(host, headers, catalog, warehouse_id, discover_agents_fn, log_fn=None, user_alias=None):
    """Run Iteration 1: Column Comments + Example SQL + Benchmarks.
    Returns list of log lines."""
    lines = ["=== ITERATION 1: Column Comments + Example SQL + Benchmarks ==="]
    _emit = log_fn or (lambda msg: None)
    CAT = catalog

    # Step 0: Discover Baseline agents
    lines.append("\n[Step 0] Discovering Baseline agents...")
    _emit("Discovering Baseline agents...")
    baseline = discover_agents_fn("Baseline")
    if len(baseline) < 5:
        _emit(f"ERROR: Only found {len(baseline)}/5 Baseline agents. Run Setup first.")
        lines.append(f"  [!] Only found {len(baseline)} Baseline agents (need 5). Run Setup first.")
        return "\n".join(lines)
    _emit(f"Found {len(baseline)} Baseline agents ✓")
    lines.append(f"  Found {len(baseline)} Baseline agents")

    # Step 1: Clone Baseline -> Iteration 1
    lines.append("\n[Step 1] Cloning Baseline agents -> Iteration 1...")
    _emit("Cloning 5 domain agents → Iteration 1...")
    spaces, clone_log = clone_agents(host, headers, baseline, "Iteration 1", warehouse_id, log_fn=_emit, user_alias=user_alias)
    lines.extend(clone_log)
    if len(spaces) < 5:
        lines.append(f"  [!] Only cloned {len(spaces)} agents")
        return "\n".join(lines)
    # Step 2: Supervisor refresh is delegated to the app's user-run job.
    lines.append("\n[Step 2] Supervisor refresh will run via the app's user-run job")
    _emit("Supervisor refresh will be run by the app job after agent setup...")

    # Step 3: Apply column/table comments via SQL Statement API
    lines.append("\n[Step 3] Applying column and table comments via SQL API...")
    _emit("Applying table + column comments via SQL API...")
    tbl_ok, col_ok, col_total = _apply_comments_via_sql(host, headers, catalog, warehouse_id, log_fn=_emit)
    lines.append(f"  Tables: {tbl_ok}/{len(ITER1_TABLE_COMMENTS)}, Columns: {col_ok}/{col_total}")
    if tbl_ok == 0:
        _emit(f"ERROR: All {len(ITER1_TABLE_COMMENTS)} table comments failed. Aborting iteration.")
        lines.append("  [!] FATAL: Zero table comments applied — warehouse may be down.")
        return "\n".join(lines)

    # Step 4: Patch agent instructions with safeguards
    lines.append("\n[Step 4] Adding safeguard instructions to agents...")
    _emit("Patching 5 agents with safeguard instructions...")
    instr_log = _patch_agent_instructions(host, headers, spaces, SAFEGUARD_TEXT, AGENT_TABLE_CONTEXT, log_fn=_emit)
    lines.extend(instr_log)
    instr_ok = sum(1 for l in instr_log if "[+]" in l or "(skip)" in l)
    if instr_ok == 0:
        _emit(f"ERROR: All instruction patches failed. Aborting iteration.")
        lines.append("  [!] FATAL: Zero instruction patches succeeded.")
        return "\n".join(lines)

    # Step 5: Add Example SQL Queries (Genie Examples tab)
    lines.append("\n[Step 5] Adding Example SQL Queries (Examples tab)...")
    _emit("Adding Example SQL Queries (Examples tab)...")
    esql_log = _add_example_sqls(host, headers, spaces, _iter1_example_sqls(CAT), log_fn=_emit)
    lines.extend(esql_log)
    esql_ok = sum(1 for l in esql_log if "[+]" in l)
    if esql_ok == 0:
        _emit(f"ERROR: All example SQL patches failed. Aborting iteration.")
        lines.append("  [!] FATAL: Zero example SQL queries applied.")
        return "\n".join(lines)

    # Step 6: Add Benchmark Questions (Benchmarks tab)
    lines.append("\n[Step 6] Adding Benchmark Questions (Benchmarks tab)...")
    _emit("Adding Benchmark Questions (Benchmarks tab)...")
    bm_log = _add_benchmarks(host, headers, spaces, _iter1_benchmarks(CAT), log_fn=_emit)
    lines.extend(bm_log)

    lines.append("\nIteration 1 complete.")
    _emit("Iteration 1 features complete. Starting benchmark...")
    lines.append(f"  {len(spaces)} agents cloned + patched with comments, safeguards, example SQL, benchmarks.")
    lines.append("  Next: run the 45-test benchmark to verify improvement (28->35 expected).")
    return "\n".join(lines), spaces



# ============================================================
# ITERATION 2: UC Metric Views + Governed Tags + CoD View
# Replicates notebook cell 13 exactly via SQL Statement API.
# ============================================================

def _iter2_metric_view_sqls(CAT):
    """4 UC Metric View CREATE statements — exact copy from notebook cell 13."""
    return [
        # 1. Delivery Performance
        f"""CREATE OR REPLACE VIEW {CAT}.logistics_operations.delivery_performance_by_region
WITH METRICS LANGUAGE YAML AS $$
  version: 1.1
  source: >
    SELECT shipment_id, destination_region, is_late, delay_days, shipping_cost, ship_date
    FROM {CAT}.logistics_operations.shipments
    WHERE ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'
  dimensions:
    - name: region
      expr: destination_region
      comment: Destination region (Western, Eastern, Central, Southern)
      synonyms: [destination region, delivery region]
  measures:
    - name: on_time_delivery_rate
      expr: ROUND(AVG(CASE WHEN is_late = false THEN 1.0 ELSE 0.0 END) * 100, 2)
      comment: Percentage of shipments delivered on time
      synonyms: [OTD rate, on-time rate]
    - name: late_delivery_rate
      expr: ROUND(AVG(CASE WHEN is_late = true THEN 1.0 ELSE 0.0 END) * 100, 2)
      comment: Percentage of shipments that were late
      synonyms: [late rate]
    - name: avg_delay_days
      expr: ROUND(AVG(CASE WHEN is_late THEN delay_days END), 2)
      comment: Average delay in days for late shipments only
    - name: total_shipments
      expr: COUNT(*)
      comment: Total number of shipments
    - name: late_shipments
      expr: SUM(CASE WHEN is_late THEN 1 ELSE 0 END)
      comment: Number of late shipments
    - name: wasted_freight_cost
      expr: ROUND(SUM(CASE WHEN is_late THEN shipping_cost ELSE 0 END), 2)
      comment: Total shipping cost for late deliveries
      synonyms: [wasted freight]
$$""",
        # 2. Revenue Comparison
        f"""CREATE OR REPLACE VIEW {CAT}.demand_analysis.revenue_comparison_by_region
WITH METRICS LANGUAGE YAML AS $$
  version: 1.1
  source: >
    SELECT order_id, region, order_date, total_amount, order_status, product_family
    FROM {CAT}.demand_analysis.sales_orders
  dimensions:
    - name: region
      expr: region
      comment: Geographic region (Western, Eastern, Central, Southern)
    - name: product_family
      expr: product_family
  measures:
    - name: revenue_last_month
      expr: ROUND(SUM(CASE WHEN order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01' THEN total_amount ELSE 0 END), 2)
      comment: Total revenue for August 2026
      synonyms: [august revenue, last month revenue]
    - name: revenue_prior_month
      expr: ROUND(SUM(CASE WHEN order_date >= DATE '2026-07-01' AND order_date < DATE '2026-08-01' THEN total_amount ELSE 0 END), 2)
      comment: Total revenue for July 2026
    - name: revenue_change_dollars
      expr: |-
        ROUND(SUM(CASE WHEN order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01' THEN total_amount ELSE 0 END) -
        SUM(CASE WHEN order_date >= DATE '2026-07-01' AND order_date < DATE '2026-08-01' THEN total_amount ELSE 0 END), 2)
      comment: Dollar change in revenue Aug vs Jul. Negative = decline.
      synonyms: [revenue change, MoM change]
    - name: revenue_change_pct
      expr: |-
        ROUND((SUM(CASE WHEN order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01' THEN total_amount ELSE 0 END) -
        SUM(CASE WHEN order_date >= DATE '2026-07-01' AND order_date < DATE '2026-08-01' THEN total_amount ELSE 0 END)) /
        NULLIF(SUM(CASE WHEN order_date >= DATE '2026-07-01' AND order_date < DATE '2026-08-01' THEN total_amount ELSE 0 END), 0) * 100, 2)
      comment: Percentage change in revenue Aug vs Jul
$$""",
        # 3. Inventory Safety Stock
        f"""CREATE OR REPLACE VIEW {CAT}.inventory_management.inventory_safety_stock_metrics
WITH METRICS LANGUAGE YAML AS $$
  version: 1.1
  source: >
    SELECT sku_id, warehouse_id, region, below_safety_stock_flag, stockout_flag, days_of_supply
    FROM {CAT}.inventory_management.inventory_ledger
    WHERE below_safety_stock_flag = true
  dimensions:
    - name: region
      expr: region
      comment: Geographic region
      synonyms: [inventory region, warehouse region]
  measures:
    - name: positions_below_safety_stock
      expr: COUNT(*)
      comment: SKU-warehouse positions below safety stock (authoritative count)
      synonyms: [below safety stock count, items below safety stock]
    - name: unique_skus_below_safety
      expr: COUNT(DISTINCT sku_id)
      comment: Distinct SKUs below safety stock (deduplicated across warehouses)
    - name: stockout_positions
      expr: SUM(CASE WHEN stockout_flag = true THEN 1 ELSE 0 END)
      comment: SKU-warehouse positions completely stocked out
    - name: unique_skus_in_stockout
      expr: COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END)
      comment: Distinct SKUs completely stocked out
      synonyms: [unique stockout SKUs]
    - name: avg_days_of_supply
      expr: ROUND(AVG(days_of_supply), 2)
      comment: Average days of supply for at-risk items
$$""",
        # 4. Supplier Performance
        f"""CREATE OR REPLACE VIEW {CAT}.supplier_procurement.supplier_performance_by_continent
WITH METRICS LANGUAGE YAML AS $$
  version: 1.1
  source: >
    SELECT po_id, supplier_continent, supplier_name, is_late,
           lead_time_variance_days, order_date, total_cost, quality_score
    FROM {CAT}.supplier_procurement.supplier_orders
    WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'
  dimensions:
    - name: supplier_continent
      expr: supplier_continent
      comment: Continent where supplier is located
      synonyms: [vendor continent, supplier region]
  measures:
    - name: total_purchase_orders
      expr: COUNT(*)
      comment: Total purchase orders last month
    - name: late_purchase_orders
      expr: SUM(CASE WHEN is_late THEN 1 ELSE 0 END)
      comment: Number of late purchase orders
    - name: supplier_late_rate_pct
      expr: ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 2)
      comment: Percentage of purchase orders (POs) that were late. When asked percentage of vendors delivered late, this is late POs / total POs per ORDER, not per distinct vendor.
      synonyms: [vendor late rate, vendor late delivery percentage, percentage of vendors delivered late, vendor late pct]
    - name: avg_lead_time_variance
      expr: ROUND(AVG(lead_time_variance_days), 2)
      comment: Average lead time variance in days
      synonyms: [lead time variance]
$$""",
    ]


def _iter2_cod_view_sql(CAT):
    """Cost of Disruption cross-domain view — exact copy from notebook cell 13."""
    return f"""CREATE OR REPLACE VIEW {CAT}.reporting.cost_of_disruption_by_region AS
WITH demand_impact AS (
    SELECT region,
        SUM(CASE WHEN order_status = 'Cancelled' THEN total_amount ELSE 0 END) AS cancelled_revenue,
        SUM(CASE WHEN order_status = 'Backordered' THEN total_amount ELSE 0 END) AS backordered_at_risk_revenue,
        SUM(total_amount) AS total_region_revenue
    FROM {CAT}.demand_analysis.sales_orders
    WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'
    GROUP BY region
),
logistics_impact AS (
    SELECT destination_region AS region,
        SUM(CASE WHEN is_late THEN shipping_cost ELSE 0 END) AS wasted_logistics_spend,
        SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_shipment_count
    FROM {CAT}.logistics_operations.shipments
    WHERE ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'
    GROUP BY destination_region
),
supplier_penalties AS (
    SELECT SUM(CASE WHEN is_breached THEN penalty_amount ELSE 0 END) AS total_sla_penalties
    FROM {CAT}.supplier_procurement.vendor_slas
),
all_late_shipments AS (
    SELECT SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS total_late_all_regions
    FROM {CAT}.logistics_operations.shipments
    WHERE ship_date >= DATE '2026-08-01' AND ship_date < DATE '2026-09-01'
),
inventory_stockouts AS (
    SELECT region, COUNT(DISTINCT CASE WHEN stockout_flag = true THEN sku_id END) AS unique_stockout_skus
    FROM {CAT}.inventory_management.inventory_ledger WHERE region IS NOT NULL GROUP BY region
)
SELECT d.region, d.cancelled_revenue, d.backordered_at_risk_revenue, l.wasted_logistics_spend,
    l.late_shipment_count,
    ROUND(sp.total_sla_penalties * (l.late_shipment_count * 1.0 / als.total_late_all_regions), 2) AS allocated_supplier_penalties,
    ROUND(d.cancelled_revenue + d.backordered_at_risk_revenue + l.wasted_logistics_spend
        + sp.total_sla_penalties * (l.late_shipment_count * 1.0 / als.total_late_all_regions), 2) AS total_cost_of_disruption,
    ROUND(d.cancelled_revenue + d.backordered_at_risk_revenue + l.wasted_logistics_spend, 2) AS revenue_at_risk_from_disruptions,
    d.total_region_revenue, i.unique_stockout_skus,
    ROUND((d.backordered_at_risk_revenue) / NULLIF(i.unique_stockout_skus, 0), 2) AS revenue_at_risk_per_stockout_sku,
    ROUND((d.cancelled_revenue + d.backordered_at_risk_revenue + l.wasted_logistics_spend
         + sp.total_sla_penalties * (l.late_shipment_count * 1.0 / als.total_late_all_regions))
        / NULLIF(d.total_region_revenue, 0), 2) AS disruption_to_revenue_ratio
FROM demand_impact d
JOIN logistics_impact l ON d.region = l.region
CROSS JOIN supplier_penalties sp CROSS JOIN all_late_shipments als
LEFT JOIN inventory_stockouts i ON d.region = i.region"""


def _add_tables_to_agents(host, headers, spaces, agent_tables, cod_instr=None, log_fn=None):
    """Add table identifiers to agents' data sources. Also adds CoD instruction to specified agents."""
    _log = log_fn or (lambda m: None)
    for agent_name, new_tables in agent_tables.items():
        space_id = spaces.get(agent_name)
        if not space_id:
            continue
        try:
            resp = requests.get(f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true",
                                headers=headers, timeout=30)
            if resp.status_code != 200:
                _log(f"  {agent_name}: GET failed {resp.status_code}")
                continue
            ss = json.loads(resp.json().get("serialized_space", "{}"))
            tables = ss.get("data_sources", {}).get("tables", [])
            existing = {t["identifier"] for t in tables}
            added = [t for t in new_tables if t not in existing]
            if added:
                for t in added:
                    tables.append({"identifier": t})
                tables.sort(key=lambda t: t["identifier"])
                ss.setdefault("data_sources", {})["tables"] = tables
                # Add CoD instruction to executive and inventory agents
                if cod_instr and agent_name in ("executive", "inventory"):
                    instrs = ss.get("instructions", {}).get("text_instructions", [])
                    if instrs and cod_instr not in str(instrs):
                        instrs[0]["content"].append(cod_instr)
                        ss["instructions"]["text_instructions"] = instrs
                ensure_sorted_payload(ss)
                pr = requests.patch(f"{host}/api/2.0/genie/spaces/{space_id}", headers=headers,
                                    json={"serialized_space": json.dumps(ss)}, timeout=30)
                if pr.status_code == 200:
                    _log(f"  {agent_name}: +{', '.join(t.split('.')[-1] for t in added)} ✓")
                else:
                    _log(f"  {agent_name}: PATCH failed {pr.status_code}")
            else:
                _log(f"  {agent_name}: views already present (skip)")
        except Exception as e:
            _log(f"  {agent_name}: error {e}")


def run_iteration_2(host, headers, catalog, warehouse_id, discover_agents_fn, log_fn=None, user_alias=None):
    """Run Iteration 2: UC Metric Views + Governed Tags + CoD View.
    Replicates notebook cell 13 exactly via SQL Statement API + Genie API.
    Returns (text, spaces) on success, or text on failure."""
    lines = ["=== ITERATION 2: UC Metric Views + Governed Tags + CoD View ==="]
    _emit = log_fn or (lambda msg: None)
    CAT = catalog

    # Step 0: Discover Iteration 1 agents
    _emit("Discovering Iteration 1 agents...")
    source = discover_agents_fn("Iteration 1")
    if len(source) < 5:
        _emit(f"ERROR: Only found {len(source)}/5 Iteration 1 agents.")
        lines.append(f"  [!] Only found {len(source)} Iteration 1 agents (need 5).")
        return "\n".join(lines)
    _emit(f"Found {len(source)} Iteration 1 agents ✓")

    # Step 1: Clone Iteration 1 -> Iteration 2
    _emit("Cloning 5 domain agents → Iteration 2...")
    spaces, clone_log = clone_agents(host, headers, source, "Iteration 2", warehouse_id, log_fn=_emit, user_alias=user_alias)
    lines.extend(clone_log)
    if len(spaces) < 5:
        lines.append(f"  [!] Only cloned {len(spaces)} agents")
        return "\n".join(lines)
    # Step 2: Supervisor refresh is delegated to the app's user-run job.
    _emit("Supervisor refresh will be run by the app job after agent setup...")

    # Step 3: Create 4 UC Metric Views via SQL Statement API
    _emit("Creating 4 UC Metric Views via SQL API...")
    mv_sqls = _iter2_metric_view_sqls(CAT)
    mv_ok = 0
    for idx, sql in enumerate(mv_sqls):
        if idx > 0:
            time.sleep(3)  # Let warehouse settle between CREATE VIEW calls
        name = sql.split("VIEW ")[1].split("\n")[0].strip()
        short = name.split(".")[-1]
        if _run_comment_sql(host, headers, warehouse_id, sql, log_fn=_emit):
            _emit(f"  {short} ✓")
            mv_ok += 1
        else:
            _emit(f"  {short} FAILED")
    lines.append(f"  Metric views: {mv_ok}/4 created")
    if mv_ok == 0:
        _emit("ERROR: All metric view creations failed. Aborting.")
        return "\n".join(lines)

    # Step 4: Create CoD view (Open Knowledge — cross-domain)
    _emit("Creating Cost of Disruption view (cross-domain)...")
    cod_sql = _iter2_cod_view_sql(CAT)
    if _run_comment_sql(host, headers, warehouse_id, cod_sql, log_fn=_emit):
        _emit("  cost_of_disruption_by_region ✓")
    else:
        _emit("  cost_of_disruption_by_region FAILED (non-fatal)")

    # Step 5: UC Governed Tags on all views
    _emit("Applying UC Governed Tags...")
    tag_targets = [
        (f"{CAT}.logistics_operations.delivery_performance_by_region",
         {"domain": "logistics", "metric_type": "delivery", "data_quality": "authoritative"}),
        (f"{CAT}.demand_analysis.revenue_comparison_by_region",
         {"domain": "demand", "metric_type": "revenue", "time_granularity": "calendar_month"}),
        (f"{CAT}.inventory_management.inventory_safety_stock_metrics",
         {"domain": "inventory", "metric_type": "safety_stock", "data_quality": "authoritative"}),
        (f"{CAT}.supplier_procurement.supplier_performance_by_continent",
         {"domain": "supplier", "metric_type": "performance"}),
        (f"{CAT}.reporting.cost_of_disruption_by_region",
         {"domain": "financial_impact", "metric_type": "composite_cross_domain", "data_quality": "authoritative"}),
    ]
    for table, tags in tag_targets:
        tag_pairs = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
        sql = f"ALTER TABLE {table} SET TAGS ({tag_pairs})"
        _run_comment_sql(host, headers, warehouse_id, sql)

    # Step 6: Schema-level domain tags
    _emit("Applying schema domain tags...")
    for schema, domain in [(f"{CAT}.demand_analysis", "demand"), (f"{CAT}.inventory_management", "inventory"),
                            (f"{CAT}.logistics_operations", "logistics"), (f"{CAT}.supplier_procurement", "supplier"),
                            (f"{CAT}.reporting", "executive")]:
        sql = f"ALTER SCHEMA {schema} SET TAGS ('domain' = '{domain}', 'business_unit' = 'supply_chain')"
        _run_comment_sql(host, headers, warehouse_id, sql)

    # Step 7: Add metric views + CoD to Genie Agents
    _emit("Adding metric views + CoD to agents' data sources...")
    agent_tables = {
        "logistics":  [f"{CAT}.logistics_operations.delivery_performance_by_region"],
        "demand":     [f"{CAT}.demand_analysis.revenue_comparison_by_region"],
        "inventory":  [f"{CAT}.inventory_management.inventory_safety_stock_metrics",
                       f"{CAT}.reporting.cost_of_disruption_by_region"],
        "supplier":   [f"{CAT}.supplier_procurement.supplier_performance_by_continent"],
        "executive":  [f"{CAT}.reporting.cost_of_disruption_by_region"],
    }
    cod_instr = (
        "\n\ncost_of_disruption_by_region view has pre-computed cross-domain metrics by region:\n"
        "- total_cost_of_disruption, revenue_at_risk_from_disruptions, revenue_at_risk_per_stockout_sku, disruption_to_revenue_ratio\n"
        "Use this for ANY question about Cost of Disruption, revenue at risk, or disruption ratios."
    )
    _add_tables_to_agents(host, headers, spaces, agent_tables, cod_instr=cod_instr, log_fn=_emit)

    give_catalog_to_all_users(host, headers, catalog, warehouse_id, log_fn=_emit)
    _emit("Iteration 2 features complete. Starting benchmark...")
    lines.append(f"  {len(spaces)} agents with metric views + governed tags + CoD view.")
    return "\n".join(lines), spaces



# ============================================================
# ITERATION 3: SQL Functions + Fiscal Targets + Supervisor Enhancement
# ============================================================

def _iter3_fiscal_targets_sqls(CAT):
    # E01 fix (Option A): fiscal_targets adds a column literally named fill rate (the TARGET).
    # Without this comment the agent treats service_level_pct as a different metric and answers 90.0.
    return [
        f"COMMENT ON COLUMN {CAT}.reporting.executive_kpis.service_level_pct IS "
        f"'ACTUAL current fill rate (also called service level percent) -- fill rate and service level are the same metric. "
        f"Use this for any question about the current or actual fill rate. Targets live in reporting.fiscal_targets.fill_rate_target_pct'",
        f"DROP TABLE IF EXISTS {CAT}.reporting.fiscal_targets",
        f"CREATE TABLE {CAT}.reporting.fiscal_targets (fiscal_quarter STRING COMMENT 'Fiscal quarter identifier (Q1-Q4)', fiscal_year INT COMMENT 'Fiscal year', calendar_months STRING COMMENT 'Calendar months covered', service_level_target_pct DOUBLE COMMENT 'Target service level pct', on_time_delivery_target_pct DOUBLE COMMENT 'Target OTD rate', fill_rate_target_pct DOUBLE COMMENT 'TARGET fill rate for the quarter (a goal, NOT the actual). The ACTUAL current fill rate is reporting.executive_kpis.service_level_pct', description STRING COMMENT 'Quarter description') USING DELTA COMMENT 'Quarterly performance targets.'",
        f"INSERT INTO {CAT}.reporting.fiscal_targets VALUES ('Q1', 2027, 'Jul 2026, Aug 2026, Sep 2026', 92.0, 85.0, 90.0, 'Ramp-up quarter'), ('Q2', 2027, 'Oct 2026, Nov 2026, Dec 2026', 93.0, 88.0, 92.0, 'Holiday season quarter'), ('Q3', 2027, 'Jan 2027, Feb 2027, Mar 2027', 95.0, 92.0, 95.0, 'Peak performance quarter'), ('Q4', 2027, 'Apr 2027, May 2027, Jun 2027', 94.0, 90.0, 93.0, 'Wind-down quarter')",
    ]


def _iter3_threshold_functions(CAT):
    # Online condition uses doubled single-quotes for SQL escaping
    SQ = chr(39)  # single quote
    online_cond = f"quantity >= 8 AND unit_price < 30 AND channel = {SQ}{SQ}Online{SQ}{SQ}"
    return [
        (f"{CAT}.logistics_operations", "get_critical_delay_shipments",
         f"CREATE OR REPLACE FUNCTION {CAT}.logistics_operations.get_critical_delay_shipments() "
         f"RETURNS TABLE (concept STRING, source_table STRING, threshold_columns STRING, conditions STRING, definition STRING) "
         f"RETURN SELECT 'Logistics Risk Standards' AS concept, '{CAT}.logistics_operations.shipments' AS source_table, "
         f"'delay_days, total_weight_kg' AS threshold_columns, 'delay_days >= 5 AND total_weight_kg > 800' AS conditions, "
         f"'Flagged under Logistics Risk Standards when delay_days >= 5 AND total_weight_kg > 800.' AS definition"),
        (f"{CAT}.inventory_management", "get_critical_supply_positions",
         f"CREATE OR REPLACE FUNCTION {CAT}.inventory_management.get_critical_supply_positions() "
         f"RETURNS TABLE (concept STRING, source_table STRING, threshold_columns STRING, conditions STRING, definition STRING) "
         f"RETURN SELECT 'Inventory Risk Classification' AS concept, '{CAT}.inventory_management.inventory_ledger' AS source_table, "
         f"'days_of_supply, below_safety_stock_flag, on_hand_qty' AS threshold_columns, "
         f"'days_of_supply BETWEEN 1 AND 11 AND below_safety_stock_flag = true AND on_hand_qty > 0' AS conditions, "
         f"'Supply-risk when days_of_supply BETWEEN 1 AND 11 AND below_safety_stock_flag = true AND on_hand_qty > 0.' AS definition"),
        (f"{CAT}.demand_analysis", "get_critical_accuracy_forecasts",
         f"CREATE OR REPLACE FUNCTION {CAT}.demand_analysis.get_critical_accuracy_forecasts() "
         f"RETURNS TABLE (concept STRING, source_table STRING, threshold_columns STRING, conditions STRING, definition STRING) "
         f"RETURN SELECT 'Demand Anomaly Alert' AS concept, '{CAT}.demand_analysis.sales_orders' AS source_table, "
         f"'quantity, unit_price, channel' AS threshold_columns, "
         f"'{online_cond}' AS conditions, "
         f"'Demand Anomaly Alert when quantity >= 8 AND unit_price < 30 AND channel = Online.' AS definition"),
        (f"{CAT}.supplier_procurement", "get_critical_quality_orders",
         f"CREATE OR REPLACE FUNCTION {CAT}.supplier_procurement.get_critical_quality_orders() "
         f"RETURNS TABLE (concept STRING, source_table STRING, threshold_columns STRING, conditions STRING, definition STRING) "
         f"RETURN SELECT 'Procurement Quality Minimum' AS concept, '{CAT}.supplier_procurement.supplier_orders' AS source_table, "
         f"'quality_score, lead_time_variance_days' AS threshold_columns, "
         f"'quality_score < 75 AND lead_time_variance_days > 12' AS conditions, "
         f"'Below Procurement Quality Minimum when quality_score < 75 AND lead_time_variance_days > 12.' AS definition"),
        (f"{CAT}.reporting", "get_critical_disruption_regions",
         f"CREATE OR REPLACE FUNCTION {CAT}.reporting.get_critical_disruption_regions() "
         f"RETURNS TABLE (concept STRING, source_table STRING, threshold_columns STRING, conditions STRING, definition STRING) "
         f"RETURN SELECT 'Executive Disruption Threshold' AS concept, '{CAT}.reporting.supply_chain_risk_scorecard' AS source_table, "
         f"'composite_risk_score, lead_time_variance, total_penalty_usd' AS threshold_columns, "
         f"'composite_risk_score < 55 AND lead_time_variance > 8 AND total_penalty_usd > 80000' AS conditions, "
         f"'Exceeds threshold when composite_risk_score < 55 AND lead_time_variance > 8 AND total_penalty_usd > 80000.' AS definition"),
    ]


def _add_fiscal_targets_to_agent(host, headers, space_id, catalog, log_fn=None):
    """Add fiscal_targets table to the Executive agent data sources."""
    _log = log_fn or (lambda m: None)
    target_table = f"{catalog}.reporting.fiscal_targets"
    try:
        resp = requests.get(f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true",
                            headers=headers, timeout=30)
        ss = json.loads(resp.json().get("serialized_space", "{}"))
        tables = ss.get("data_sources", {}).get("tables", [])
        if target_table not in [t["identifier"] for t in tables]:
            tables.append({"identifier": target_table})
            tables.sort(key=lambda t: t["identifier"])
            ss.setdefault("data_sources", {})["tables"] = tables
            ensure_sorted_payload(ss)
            pr = requests.patch(f"{host}/api/2.0/genie/spaces/{space_id}", headers=headers,
                                json={"serialized_space": json.dumps(ss)}, timeout=30)
            if pr.status_code == 200:
                _log("  fiscal_targets added to Executive agent \u2713")
            else:
                _log(f"  fiscal_targets PATCH failed: {pr.status_code}")
        else:
            _log("  fiscal_targets already present (skip)")
    except Exception as e:
        _log(f"  fiscal_targets error: {e}")


def _enhance_supervisor_iter3(host, headers, catalog, log_fn=None):
    """Enhance Supervisor instructions + add SQL function tools for Iteration 3."""
    _log = log_fn or (lambda m: None)
    CAT = catalog
    sr = requests.get(f"{host}/api/2.1/supervisor-agents", headers=headers, timeout=30)
    if sr.status_code != 200:
        _log(f"  Supervisor list failed: {sr.status_code}")
        return
    sup = next((a for a in sr.json().get("supervisor_agents", [])
                if "Supply Chain" in a.get("display_name", "")), None)
    if not sup:
        _log("  Supply Chain supervisor not found")
        return
    sup_name = sup["name"]
    _log(f"  Found supervisor: {sup_name}")
    sup_detail = requests.get(f"{host}/api/2.1/{sup_name}", headers=headers, timeout=30)
    current_instr = sup_detail.json().get("instructions", "") if sup_detail.status_code == 200 else ""
    iter3_block = (
        "\nITERATION 3 CAPABILITIES\n"
        "- Sub-agents have metric views, SQL functions, certified queries, column comments, governed tags.\n"
        "- Cross-domain questions (Cost of Disruption) -> executive-reporting.\n"
        "- Domain-specific policy terms -> owning domain agent.\n"
        "- July fiscal year start. Q1=Jul-Sep. Q1 target -> executive-reporting."
    )
    if "ITERATION 3 CAPABILITIES" not in (current_instr or ""):
        enhanced = (current_instr or "").rstrip() + "\n\n" + iter3_block
        requests.patch(f"{host}/api/2.1/{sup_name}?update_mask=instructions",
                       headers=headers, json={"instructions": enhanced}, timeout=30)
        _log("  Supervisor Iter3 instructions \u2713")
    sql_function_tools = [
        {"name": f"{CAT}.logistics_operations.get_critical_delay_shipments",
         "description": "Returns business definition of Logistics Risk Standards."},
        {"name": f"{CAT}.demand_analysis.get_critical_accuracy_forecasts",
         "description": "Returns business definition of Demand Anomaly Alert."},
        {"name": f"{CAT}.inventory_management.get_critical_supply_positions",
         "description": "Returns business definition of Inventory Risk Classification."},
        {"name": f"{CAT}.supplier_procurement.get_critical_quality_orders",
         "description": "Returns business definition of Procurement Quality Minimum."},
        {"name": f"{CAT}.reporting.get_critical_disruption_regions",
         "description": "Returns business definition of Executive Disruption Threshold."},
    ]
    existing_tools = set()
    tr = requests.get(f"{host}/api/2.1/{sup_name}/tools", headers=headers, timeout=30)
    if tr.status_code == 200:
        for t in tr.json().get("tools", []):
            existing_tools.add(t.get("uc_function", {}).get("name", ""))
    added = 0
    for func in sql_function_tools:
        if func["name"] in existing_tools:
            continue
        func_slug = func["name"].split(".")[-1].replace("_", "-")
        cr = requests.post(f"{host}/api/2.1/{sup_name}/tools", headers=headers,
                           params={"tool_id": func_slug},
                           json={"tool_type": "uc_function", "uc_function": {"name": func["name"]},
                                 "description": func["description"]}, timeout=30)
        if cr.status_code == 200:
            added += 1
    _log(f"  Supervisor SQL function tools: {added} added \u2713")
    current_instr2 = requests.get(f"{host}/api/2.1/{sup_name}", headers=headers, timeout=30).json().get("instructions", "")
    sql_func_block = (
        "\nSQL THRESHOLD FUNCTIONS (ITERATION 3)\n"
        "5 SQL functions as direct tools. Each returns a BUSINESS DEFINITION, not data.\n"
        "Call the function first to learn conditions, then query the source table:\n"
        "- get_critical_delay_shipments / get_critical_accuracy_forecasts\n"
        "- get_critical_supply_positions / get_critical_quality_orders\n"
        "- get_critical_disruption_regions\n"
        "Do NOT guess thresholds -- always call the function first."
    )
    if "SQL THRESHOLD FUNCTIONS (ITERATION 3)" not in (current_instr2 or ""):
        enhanced2 = (current_instr2 or "").rstrip() + "\n\n" + sql_func_block
        requests.patch(f"{host}/api/2.1/{sup_name}?update_mask=instructions",
                       headers=headers, json={"instructions": enhanced2}, timeout=30)
        _log("  Supervisor SQL function guidance \u2713")


def _add_provenance_instruction(host, headers, spaces, log_fn=None):
    """Add provenance reporting instruction to sub-agents."""
    _log = log_fn or (lambda m: None)
    prov_text = (
        "\nPROVENANCE: When answering, note which data source informed your answer:\n"
        "- If you called a SQL function, say which one\n"
        "- If you queried a metric view, say which one\n"
        "- If column comments or examples guided your query, note which ones"
    )
    for agent_name, sid in spaces.items():
        try:
            r = requests.get(f"{host}/api/2.0/genie/spaces/{sid}?include_serialized_space=true",
                             headers=headers, timeout=30)
            if r.status_code != 200:
                continue
            ss = json.loads(r.json().get("serialized_space", "{}"))
            instrs = ss.get("instructions", {})
            if "PROVENANCE:" in str(instrs):
                continue
            if isinstance(instrs, dict):
                ti = instrs.get("text_instructions", [{"content": []}])
                if ti:
                    ti[0]["content"].append(prov_text)
                    instrs["text_instructions"] = ti
            ss["instructions"] = instrs
            ensure_sorted_payload(ss)
            requests.patch(f"{host}/api/2.0/genie/spaces/{sid}", headers=headers,
                           json={"serialized_space": json.dumps(ss)}, timeout=30)
        except Exception:
            pass
    _log("  Provenance instructions added to sub-agents \u2713")


def run_iteration_3(host, headers, catalog, warehouse_id, discover_agents_fn, log_fn=None, user_alias=None):
    """Run Iteration 3: SQL Functions + Fiscal Targets + Supervisor Enhancement.
    Returns (text, spaces) on success, or text on failure."""
    lines = ["=== ITERATION 3: SQL Functions + Fiscal Targets ==="]
    _emit = log_fn or (lambda msg: None)
    CAT = catalog
    _emit("Discovering Iteration 2 agents...")
    source = discover_agents_fn("Iteration 2")
    if len(source) < 5:
        _emit(f"ERROR: Only found {len(source)}/5 Iteration 2 agents.")
        return "\n".join(lines)
    _emit(f"Found {len(source)} Iteration 2 agents \u2713")
    _emit("Cloning 5 domain agents -> Iteration 3...")
    spaces, clone_log = clone_agents(host, headers, source, "Iteration 3", warehouse_id, log_fn=_emit, user_alias=user_alias)
    lines.extend(clone_log)
    if len(spaces) < 5:
        return "\n".join(lines)
    _emit("Supervisor refresh will be run by the app job after agent setup...")
    _emit("Creating fiscal_targets reference table...")
    for sql in _iter3_fiscal_targets_sqls(CAT):
        _run_comment_sql(host, headers, warehouse_id, sql, log_fn=_emit)
    _emit("  fiscal_targets \u2713")
    _emit("Adding fiscal_targets to Executive agent...")
    exec_sid = spaces.get("executive", "")
    if exec_sid:
        _add_fiscal_targets_to_agent(host, headers, exec_sid, catalog, log_fn=_emit)
    _emit("Creating 5 SQL threshold functions...")
    tf_list = _iter3_threshold_functions(CAT)
    for idx, (schema, fname, sql) in enumerate(tf_list):
        if idx > 0:
            time.sleep(3)  # Let warehouse settle between CREATE FUNCTION calls
        if _run_comment_sql(host, headers, warehouse_id, sql, log_fn=_emit):
            _emit(f"  {fname}() \u2713")
        else:
            _emit(f"  {fname}() FAILED")
    _emit("Supervisor enhancement will be run by the app job after agent setup...")
    _emit("Adding provenance instructions...")
    _add_provenance_instruction(host, headers, spaces, log_fn=_emit)
    give_catalog_to_all_users(host, headers, catalog, warehouse_id, log_fn=_emit)
    _emit("Iteration 3 automated setup complete.")
    lines.append(f"  {len(spaces)} agents + fiscal_targets + SQL functions + supervisor enhanced.")
    return "\n".join(lines), spaces

def _runtime_host_headers():
    host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
    headers = None
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        host = w.config.host.rstrip("/")
        headers = dict(w.config.authenticate())
    except Exception:
        pass
    if not headers:
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            if not host:
                host = spark.conf.get("spark.databricks.workspaceUrl", "").rstrip("/")
                if host and not host.startswith("http"):
                    host = f"https://{host}"
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
            headers = {"Authorization": f"Bearer {token}"}
        except Exception as e:
            raise RuntimeError(f"Could not resolve Databricks credentials: {e}")
    headers["Content-Type"] = "application/json"
    if not host:
        raise RuntimeError("Missing Databricks host")
    return host, headers


def tag_supervisor_endpoints(host, headers, log_fn=None):
    """Write each Supply Chain supervisor's display name + description as tags on its serving endpoint.
    Runs as the user (job). The app reads endpoint tags with its model-serving scope, so it can show
    display names even when its token cannot read the supervisor-agents API. Non-fatal."""
    _log = log_fn or (lambda m: None)
    try:
        r = requests.get(f"{host}/api/2.1/supervisor-agents", headers=headers, timeout=20)
        if r.status_code != 200:
            _log(f"  endpoint tags skipped: supervisor list HTTP {r.status_code}")
            return 0
        n = 0
        for s in r.json().get("supervisor_agents", []):
            dn, ep = s.get("display_name", ""), s.get("endpoint_name", "")
            if not ep or "supply chain" not in dn.lower():
                continue
            tags = [{"key": "display_name", "value": dn[:250]}]
            if s.get("description"):
                tags.append({"key": "supervisor_description", "value": s["description"][:250]})
            pr = requests.patch(f"{host}/api/2.0/serving-endpoints/{ep}/tags", headers=headers,
                                json={"add_tags": tags}, timeout=20)
            ok = pr.status_code == 200
            n += ok
            mark = "OK" if ok else f"FAILED {pr.status_code}"
            _log(f"  endpoint tags {mark}: {ep} = {dn}")
        return n
    except Exception as e:
        _log(f"  endpoint tags error: {e}")
        return 0


def _main():
    parser = argparse.ArgumentParser(description="Curator Workbench iteration helpers")
    parser.add_argument("--mode", choices=["supervisor-job"], required=True)
    parser.add_argument("--iteration", required=True)
    parser.add_argument("--catalog", default=os.getenv("CATALOG_NAME", ""))
    parser.add_argument("--user-alias", default="", dest="user_alias",
                        help="Per-user sandbox alias (e.g. ssuman_bpcs_com)")
    args = parser.parse_args()
    alias = args.user_alias or None
    host, headers = _runtime_host_headers()
    refresh_supervisor_for_iteration(host, headers, args.iteration,
                                     log_fn=print, catalog=args.catalog,
                                     user_alias=alias)
    tag_supervisor_endpoints(host, headers, log_fn=print)
    print(f"DONE: supervisor refreshed for {_iteration_label(args.iteration)}")


if __name__ == "__main__":
    _main()
