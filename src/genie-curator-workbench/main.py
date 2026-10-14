"""Genie Curator Workbench v2 — Supply Chain Control Tower.

Benchmark, iterate, and compare Genie Agent configurations
using the Gap workshop light-theme design system.
"""
import os, time, json, re, threading, uuid
from datetime import datetime

import dash
from dash import html, dcc, Input, Output, State, callback, ctx, ALL, no_update, dash_table
from dash.exceptions import PreventUpdate
import plotly.graph_objects as go
import requests

from concurrent.futures import ThreadPoolExecutor, as_completed
from iterations import run_iteration_1, run_iteration_2, run_iteration_3, clone_agents

# ---- Config ----------------------------------------------------------------
BASE_CATALOG = os.getenv("CATALOG_NAME", "GAP_Demo_Dev")
# Per-user sandbox: derived at request time from OBO token
CATALOG = BASE_CATALOG  # module-level fallback; callbacks use user_catalog()
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "")  # from app.yaml — no hardcoded default
SRC_DIR = os.getenv("SRC_DIR", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # env var preferred; __file__ fallback for local dev
# Legacy single-user env vars (fallback if per-user jobs not found)
_FALLBACK_SETUP_JOB = os.getenv("SETUP_JOB_ID", "")
_FALLBACK_TEARDOWN_JOB = os.getenv("TEARDOWN_JOB_ID", "")
_FALLBACK_ITER_JOBS = {
    "it1": os.getenv("ITER1_SUPERVISOR_JOB_ID", ""),
    "it2": os.getenv("ITER2_SUPERVISOR_JOB_ID", ""),
    "it3": os.getenv("ITER3_SUPERVISOR_JOB_ID", ""),
}
# Per-user job map: populated by _discover_user_jobs() at first request.
# Structure: {"ssuman": {"setup": "123", "teardown": "456", "it1": "789", ...}, ...}
_USER_JOBS = {}

from flask import request as flask_request, jsonify as _jsonify

# Workspace host
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    HOST = w.config.host.rstrip("/")
except Exception:
    HOST = os.getenv("DATABRICKS_HOST", "")

import base64 as _b64

def get_sp_headers():
    """SP headers for Job API only (App resources CAN_MANAGE_RUN).
    The job has run_as=user, so it executes as them."""
    try:
        h = dict(w.config.authenticate())
        h["Content-Type"] = "application/json"
        return h
    except Exception as e:
        log(f"SP auth error: {e}")
        return {"Content-Type": "application/json"}

_TOKEN_SCOPES_LOGGED = False

def _log_token_info(token_value):
    """Decode OBO JWT payload to inspect scopes (one-time diagnostic)."""
    global _TOKEN_SCOPES_LOGGED
    if _TOKEN_SCOPES_LOGGED:
        return
    _TOKEN_SCOPES_LOGGED = True
    try:
        raw = token_value.replace("Bearer ", "") if token_value.startswith("Bearer ") else token_value
        parts = raw.split(".")
        if len(parts) >= 2:
            padded = parts[1] + "=" * (4 - len(parts[1]) % 4)
            claims = json.loads(_b64.urlsafe_b64decode(padded))
            scopes = claims.get("scp", claims.get("scope", "NOT_PRESENT"))
            aud = str(claims.get("aud", "?"))[:60]
            azp = str(claims.get("azp", claims.get("appid", "?")))[:60]
            log(f"OBO JWT scopes: {scopes}")
            log(f"OBO JWT aud={aud}, azp={azp}")
    except Exception as e:
        log(f"OBO JWT decode error: {e}")

def _get_user_from_headers(headers):
    """Extract user email from OBO JWT. Returns email or '?'."""
    auth = headers.get("Authorization", "")
    raw = auth.replace("Bearer ", "") if auth.startswith("Bearer ") else auth
    parts = raw.split(".")
    if len(parts) >= 2:
        padded = parts[1] + "=" * (4 - len(parts[1]) % 4)
        claims = json.loads(_b64.urlsafe_b64decode(padded))
        return claims.get("email") or claims.get("upn") or claims.get("sub", "?")
    return "?"

def _get_user_alias(headers=None):
    """Sanitised alias from OBO email: xxxxxx@bpcs.com -> xxxxxx_bpcs_com."""
    h = headers or (get_headers() if _in_request() else {})
    email = _get_user_from_headers(h)
    if email and email != "?":
        return email.replace("@", "_").replace(".", "_")
    return "unknown"

def _in_request():
    try:
        _ = flask_request.headers
        return True
    except RuntimeError:
        return False

def user_catalog(headers=None):
    """Per-user catalog: GAP_Demo_Dev_ssuman_bpcs_com."""
    return f"{BASE_CATALOG}_{_get_user_alias(headers)}"

def user_agent_tag(headers=None):
    """Short alias for agent/supervisor names, e.g. 'ssuman_bpcs_com'."""
    return _get_user_alias(headers)

# Tag -> job-type mapping (set by DAB resource YAMLs: tags.purpose)
_PURPOSE_MAP = {
    "setup": "setup",
    "teardown": "teardown",
    "iter1_supervisor": "it1",
    "iter2_supervisor": "it2",
    "iter3_supervisor": "it3",
}

def _discover_user_jobs():
    """List all jobs via SP, build per-user mapping from DAB tags.
    Filters: tags.application == 'gapgenie', tags.dev == <alias>, tags.purpose -> job type.
    Called once at first request; result cached in _USER_JOBS."""
    global _USER_JOBS
    if _USER_JOBS:
        return  # already populated
    h = get_sp_headers()
    try:
        r = requests.get(f"{HOST}/api/2.1/jobs/list?limit=100", headers=h, timeout=15)
        if r.status_code != 200:
            log(f"_discover_user_jobs: HTTP {r.status_code}")
            return
        for j in r.json().get("jobs", []):
            tags = j.get("settings", {}).get("tags", {})
            if tags.get("application") != "gapgenie":
                continue
            alias = tags.get("dev", "")
            purpose = tags.get("purpose", "")
            job_type = _PURPOSE_MAP.get(purpose)
            if not alias or not job_type:
                continue
            jid = str(j.get("job_id", ""))
            _USER_JOBS.setdefault(alias, {})[job_type] = jid
        log(f"_discover_user_jobs: {len(_USER_JOBS)} users: " +
            ", ".join(f"{a}({len(e)} jobs)" for a, e in _USER_JOBS.items()))
    except Exception as e:
        log(f"_discover_user_jobs: {e}")

def _get_user_job(user_email, job_type):
    """Look up per-user job ID. job_type: 'setup'|'teardown'|'it1'|'it2'|'it3'.
    Falls back to legacy env vars if no per-user job found."""
    _discover_user_jobs()
    short = user_email.split("@")[0] if user_email else ""
    entry = _USER_JOBS.get(short, {})
    jid = entry.get(job_type, "")
    if jid:
        return jid
    # Fallback to legacy env vars
    if job_type == "setup":
        return _FALLBACK_SETUP_JOB
    if job_type == "teardown":
        return _FALLBACK_TEARDOWN_JOB
    return _FALLBACK_ITER_JOBS.get(job_type, "")

def get_headers():
    """OBO only: use the logged-in user's token for ALL API calls."""
    try:
        for hdr in ["X-Forwarded-Access-Token", "X-Databricks-Token", "Authorization"]:
            val = flask_request.headers.get(hdr, "")
            if val:
                auth = val if val.startswith("Bearer ") else f"Bearer {val}"
                _log_token_info(auth)
                return {"Authorization": auth, "Content-Type": "application/json"}
        avail = [k for k in flask_request.headers.keys() if "auth" in k.lower() or "token" in k.lower() or "forward" in k.lower()]
        log(f"get_headers: no OBO token found. Headers: {avail}")
    except RuntimeError:
        pass
    return {"Content-Type": "application/json"}

# ---- Design tokens (from Gap_workshp_.html) --------------------------------
# HEX: raw colors for Plotly charts (which can't use CSS vars)
HEX = dict(
    bg="#F4F6FB", surface="#FFFFFF", soft="#EAEEF7",
    ink="#15203A", muted="#556079", line="#D6DCEA",
    glass="rgba(244,246,251,.88)",
    rose="#C2273F", rose_bg="#FCE7EB",
    teal="#087A70", teal_bg="#D9F3EE",
    indigo="#4338CA", indigo_bg="#E6E7FB",
)
# C: CSS variable refs — adapt to light/dark theme automatically
C = dict(
    bg="var(--bg)", surface="var(--surface)", soft="var(--soft)",
    ink="var(--ink)", muted="var(--muted)", line="var(--line)",
    glass="var(--glass)",
    rose="var(--rose)", rose_bg="var(--rose-bg)",
    teal="var(--teal)", teal_bg="var(--teal-bg)",
    indigo="var(--indigo)", indigo_bg="var(--indigo-bg)",
)
FONTS_URL = "https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,500..800&family=Instrument+Sans:wght@400..700&family=JetBrains+Mono:wght@400;500&display=swap"

# ---- 45 Ground Truth Assumptions -------------------------------------------
ASSUMPTIONS = [
    # GROUP A: Logistics
    ("A01","logistics","What is the on-time delivery rate for Western region shipments in August 2026?",5.43,"on_time_delivery_rate","Iter2"),
    ("A02","logistics","What is the delayed shipment rate for Western region shipments last month?",94.57,"late_delivery_rate","Iter2"),
    ("A03","logistics","What is the average delay in days for delayed shipments in the Western region last month?",2.94,"avg_delay_days","Iter2"),
    ("A04","logistics","How many total shipments went to the Western region in August?",1086,"total_shipments","Iter2"),
    ("A05","logistics","How many late shipments went to the Western region last month?",1027,"late_shipments","Iter2"),
    ("A06","logistics","What is the total wasted freight on late shipments in Western region last month?",2484985.57,"wasted_freight_cost","Iter2"),
    # GROUP B: Demand
    ("B01","demand","What is the total revenue for the Western region in August 2026?",3341062.58,"revenue_last_month","Iter2"),
    ("B02","demand","What was the total revenue for the Western region in July 2026?",4581392.7,"revenue_prior_month","Iter2"),
    ("B03","demand","What is the revenue change in dollars for Western region month-over-month?",-1240330.12,"revenue_change_dollars","Iter2"),
    ("B04","demand","What is the percentage change in revenue for Western region last month vs prior month?",-27.07,"revenue_change_pct","Iter2"),
    # GROUP C: Inventory
    ("C01","inventory","How many inventory positions are below safety stock in the Western region?",109,"positions_below_safety_stock","Iter2"),
    ("C02","inventory","How many unique SKUs are below safety stock in the Western region?",61,"unique_skus_below_safety","Iter2"),
    ("C03","inventory","How many stockout positions are there in the Western region?",35,"stockout_positions","Iter2"),
    ("C04","inventory","How many unique SKUs are completely stocked out in the Western region?",33,"unique_skus_in_stockout","Iter2"),
    ("C05","inventory","What is the average days of supply for at-risk items in the Western region?",0.96,"avg_days_of_supply","Iter2"),
    # GROUP D: Supplier
    ("D01","supplier","How many total purchase orders were placed in August 2026?",48,"total_purchase_orders","Iter2"),
    ("D02","supplier","How many purchase orders were delayed last month?",36,"late_purchase_orders","Iter2"),
    ("D03","supplier","What percentage of purchase orders were delayed last month?",75.0,"supplier_late_rate_pct","Iter2"),
    ("D04","supplier","What is the average lead time variance in days for all suppliers last month?",8.69,"avg_lead_time_variance","Iter1"),
    ("D05","supplier","What percentage of purchase orders from Asia suppliers were delayed in August?",100.0,"supplier_late_rate_pct Asia","Iter1"),
    ("D06","supplier","What is the average lead time variance for Asia suppliers last month?",13.67,"avg_lead_time_variance Asia","Iter1"),
    ("D07","supplier","How many purchase orders did we place with Asia suppliers in August?",30,"total_purchase_orders Asia","Iter2"),
    # GROUP E: Cross-Domain
    ("E01","executive","What is our current fill rate?",80.7,"service_level_pct","Iter2"),
    ("E02","supplier","What are the total vendor SLA penalties we incurred?",1185043.1,"penalty_amount","Iter2"),
    ("E03","executive","What is the total Cost of Disruption for the Western region last month?",3757298.31,"CoD view","Iter2"),
    # GROUP F: Indirect
    ("F01","demand","Show me the total revenue for the West region last month",3341062.58,"West=Western","Iter1"),
    ("F02","supplier","What percentage of vendors had delayed deliveries last month?",75.0,"per-order aggregation","Iter1"),
    ("F03","demand","How many Western region orders were fulfilled last month?",1342,"Fulfilled count","Iter2"),
    ("F04","demand","What is the total cancelled revenue in Western region in August?",179419.26,"Cancelled revenue","Iter2"),
    ("F05","demand","How many orders were backordered in Western region last month?",275,"Backordered count","Iter2"),
    ("F06","demand","Which product family had the largest revenue decline in Western region last month vs prior month?",349062.88,"Worst family","Iter1"),
    # GROUP H: Hard
    ("H01","supplier","What is the average lead time variance for Europe suppliers last month?",0.38,"supplier join","Iter1"),
    ("H02","supplier","What is the average lead time variance for North America suppliers last month?",0.4,"supplier join","Iter1"),
    ("H03","demand","What is the order fulfillment rate for Western region last month?",71.23,"Status ambiguity","Iter1"),
    ("H04","demand","What percentage of Western region orders were only partially fulfilled last month?",9.02,"Partially_Fulfilled","Iter2"),
    ("H05","executive","What is the total revenue at risk from supply chain disruptions in Western region including cancelled revenue, backordered revenue, and wasted freight combined?",3138569.66,"demand+logistics","Iter2"),
    ("H06","inventory","What is the average revenue at risk per stockout SKU in Western region?",14368.63,"inventory+demand","Iter2"),
    ("H07","executive","What is our total cost of supply chain disruptions as a ratio of Western region revenue?",1.12,"CoD/revenue","Iter2"),
    # GROUP G: Fiscal
    ("G01","executive","Are we going to miss our Q1 service-level targets?",92.0,"fiscal_targets","Iter3"),
    ("G02","executive","What is our Q1 service-level target?",92.0,"fiscal_targets","Iter3"),
    # GROUP P: Policy
    ("P01","logistics","How many shipments in August were flagged under the Logistics Risk Standards?",176,"delay>=5 AND weight>800","Iter3"),
    ("P02","demand","How many Western region orders in August triggered a Demand Anomaly Alert?",77,"qty>=8,price<30,Online","Iter3"),
    ("P03","inventory","How many inventory positions are classified as supply-risk under Inventory Standards?",106,"dos 1-11,below_ss,on_hand>0","Iter3"),
    ("P04","supplier","How many supplier orders last month fell below the Procurement Quality Minimum?",11,"quality<75,ltv>12","Iter3"),
    ("P05","executive","How many suppliers exceeded the Executive Disruption Threshold?",3,"risk<55,ltv>8,penalty>80K","Iter3"),
]
GROUPS = {
    "A":("Logistics","#4338CA"), "B":("Demand","#087A70"), "C":("Inventory","#C2273F"),
    "D":("Supplier","#D97706"), "E":("Cross-Domain","#7C3AED"), "F":("Indirect","#4F46E5"),
    "G":("Fiscal","#059669"), "H":("Hard","#C2273F"), "P":("Policy","#087A70"),
}
MAIN_PROMPT = (
    "We need a complete supply chain health check for our West region in August 2026. "
    "The CFO wants to understand what drove the revenue decline versus July -- "
    "show the actual August and July revenue numbers, the dollar change, and the percentage change -- "
    "and which product families are most at fault. "
    "Are our on-time delivery rate and average delay for West region shipments contributing to the problem? "
    "How many total shipments went out and how many were late? "
    "I also need our current fill rate, how many inventory positions are sitting below safety stock "
    "in the West region, how many unique SKUs are affected, what is our days of supply for those at-risk items, "
    "and how many SKUs are completely stocked out. "
    "On the vendor side: what percentage of vendors delivered late in August, "
    "how many purchase orders were late out of total, what is the average lead time variance, "
    "and what are the total vendor SLA penalties we have incurred? "
    "Bring it all together as our total Cost of Disruption by region for last month Aug 26 -- "
    "cancelled revenue, at-risk backorder revenue, wasted freight on late shipments, "
    "and supplier penalty exposure in one number per region. "
    "Additionally, how many shipments in August were flagged under the Logistics Risk Standards, "
    "how many Western region orders in August triggered a Demand Anomaly Alert, "
    "how many inventory positions are classified as supply-risk under Inventory Standards, "
    "how many supplier orders last month fell below the Procurement Quality Minimum, "
    "and how many suppliers exceeded the Executive Disruption Threshold? "
    "Are we going to miss our Q1 service-level targets, and what are the top actions we should take?"
)

# ---- Scorers ---------------------------------------------------------------
def find_value_in_text(text, expected):
    es = str(expected)
    if es in text:
        return True, expected, expected
    try:
        ef = float(expected)
        for c in [f"{ef:,.2f}",f"{ef:.2f}",f"{abs(ef):,.2f}",f"{ef:,.0f}",f"{ef:.0f}"]:
            if c and c in text:
                return True, ef, ef
        if ef == int(ef) and str(int(ef)) in text:
            return True, ef, ef
    except (ValueError, TypeError):
        pass
    nums = re.findall(r'-?[\d,]+\.?\d*', text)
    bd, bv = float('inf'), None
    try:
        ef = float(expected)
        for ns in nums:
            try:
                v = float(ns.replace(",",""))
                d = abs(v - ef)
                if d < bd: bd, bv = d, v
            except ValueError: pass
    except (ValueError, TypeError): pass
    return False, None, bv

def find_value_tolerant(text, expected, tol=0.005):
    m, f, cl = find_value_in_text(text, expected)
    if m: return True, f, cl, "EXACT"
    ef = float(expected)
    nums = re.findall(r'-?[\d,]+\.?\d*', text)
    bd, bv = float('inf'), cl
    for ns in nums:
        try:
            v = float(ns.replace(",",""))
            if ef == 0:
                if v == 0: return True, v, v, "EXACT_ZERO"
                continue
            rd = abs(v - ef)/abs(ef)
            if rd < bd: bd, bv = rd, v
            if rd <= tol: return True, v, v, f"TOLERANT({rd:.4%})"
        except ValueError: pass
    return False, None, bv, "MISS"

def score_response(text):
    results = []
    for aid, ag, q, exp, desc, fix in ASSUMPTIONS:
        me, fe, ce = find_value_in_text(text, exp)
        mt, ft, ct, method = find_value_tolerant(text, exp)
        verdict = "EXACT" if me else ("TOLERANT" if mt else "MISSING")
        g = aid[0]
        results.append(dict(id=aid, group=g, group_name=GROUPS.get(g,("",""))[0],
                            agent=ag, question=q, expected=exp, desc=desc, fix=fix,
                            verdict=verdict, method=method, closest=fe or ft or ct))
    return results

# ---- API Helpers -----------------------------------------------------------
AGENT_VERSIONS = ["Baseline", "Iteration 1", "Iteration 2", "Iteration 3"]
_AGENT_TITLE_PREFIX_BASE = {"demand": "SC - Demand Analysis", "inventory": "SC - Inventory Management",
                            "logistics": "SC - Logistics Operations", "supplier": "SC - Supplier Risk",
                            "executive": "SC - Executive Reporting"}
# Legacy alias kept for code that doesn't need per-user scoping
_AGENT_TITLE_PREFIX = _AGENT_TITLE_PREFIX_BASE

def _user_agent_prefix(alias=None):
    """Per-user agent title prefix: 'SC - Demand Analysis (ssuman_bpcs_com)'."""
    tag = alias or (user_agent_tag() if _in_request() else None)
    if tag and tag != "unknown":
        return {k: f"{v} ({tag})" for k, v in _AGENT_TITLE_PREFIX_BASE.items()}
    return _AGENT_TITLE_PREFIX_BASE


def _list_all_genie_spaces(h):
    """All Genie agents (the list API returns 20 per page). Returns (spaces, error_response)."""
    spaces, token = [], None
    for _ in range(100):
        params = {"page_size": 100, **({"page_token": token} if token else {})}
        r = requests.get(f"{HOST}/api/2.0/genie/spaces", headers=h, params=params, timeout=15)
        if r.status_code != 200:
            return spaces, r
        body = r.json()
        spaces += body.get("spaces", [])
        token = body.get("next_page_token")
        if not token:
            break
    return spaces, None


def _group_agent_versions(spaces, alias=None):
    """{version: {agent: space_id}} for Baseline / Iteration 1-3.
    Title must be "SC - <Agent> (<alias>) - <Version>".
    Plus optional Genie duplicate suffix (" 2026-10-11 20:36:20").
    If several match, the NEWEST (create_time) wins.
    Strict: only matches user-scoped titles when alias is provided."""
    versions, best = {}, {}
    pfx_map = _user_agent_prefix(alias)  # includes (alias) when alias is set
    for s in spaces:
        t = s.get("title", "")
        for ver in AGENT_VERSIONS:
            for agent, prefix in pfx_map.items():
                base = f"{prefix} - {ver}"
                if not (t == base or t.startswith(base + " 20")):
                    continue
                ct = s.get("create_time") or ""
                if ct >= best.get((ver, agent), ""):
                    best[(ver, agent)] = ct
                    versions.setdefault(ver, {})[agent] = s["space_id"]
    return {v: versions[v] for v in AGENT_VERSIONS if v in versions}


def discover_agents(headers=None):
    """Discover SC domain agents via Genie Agents API (OBO).
    Stores every version in _ust()["agent_versions"]; returns the Baseline set (what the
    baseline benchmark and the setup checks expect)."""
    h = headers or get_headers()
    spaces, err = _list_all_genie_spaces(h)
    if err is not None:
        log(f"discover_agents: genie API HTTP {err.status_code}: {err.text[:200]}")
        return {}
    log(f"discover_agents: {len(spaces)} total genie agents")
    alias = _get_user_alias(h)
    versions = _group_agent_versions(spaces, alias=alias)
    _ust()["agent_versions"] = versions
    _ust()["user_alias"] = alias
    if versions:
        log("discover_agents: versions " + ", ".join(f"{v}={len(a)}" for v, a in versions.items()))
    if len(versions.get("Baseline", {})) == 5:
        log("discover_agents: 5 SC domain agents found (Baseline)")
        return dict(versions["Baseline"])
    # Collect ALL SC agents (any label), group by domain, latest wins
    # Filter by user alias to avoid picking up other users' agents
    domain_kw = [("demand","Demand"),("inventory","Inventory"),("logistics","Logistics"),
                 ("supplier","Supplier"),("executive","Executive")]
    found = {}
    for s in spaces:
        t = s.get("title", "")
        if "SC" not in t or "Evaluator" in t:
            continue
        # Strict: only match user-scoped agents when alias is set
        if alias and alias != "unknown" and f"({alias})" not in t:
            continue
        sid = s["space_id"]
        for k, kw in domain_kw:
            if kw in t:
                found[k] = sid  # last match wins (spaces ordered by creation)
    if not found and alias and alias != "unknown":
        _warn_key = f"_warned_no_agents_{alias}"
        if not ST.get(_warn_key):
            ST[_warn_key] = True
            log(f"discover_agents: WARNING — no agents found for alias={alias}. Run Setup to create them.")
    if found:
        log(f"discover_agents: {len(found)} SC domain agents found (alias={alias})")
    return found

def _sync_agent_warehouses(agents, headers=None):
    """Ensure all discovered agents use the app's configured WAREHOUSE_ID.
    Config-driven: warehouse comes from app.yaml, agents auto-sync on discovery."""
    h = headers or get_headers()
    synced = 0
    for domain, sid in agents.items():
        try:
            r = requests.get(f"{HOST}/api/2.0/genie/spaces/{sid}", headers=h, timeout=10)
            if r.status_code != 200:
                continue
            current_wh = r.json().get("warehouse_id", "")
            if current_wh != WAREHOUSE_ID:
                pr = requests.patch(f"{HOST}/api/2.0/genie/spaces/{sid}",
                                    headers=h, json={"warehouse_id": WAREHOUSE_ID}, timeout=10)
                if pr.status_code == 200:
                    synced += 1
                    log(f"Agent {domain}: warehouse synced to {WAREHOUSE_ID}")
                else:
                    log(f"Agent {domain}: warehouse sync failed HTTP {pr.status_code}")
        except Exception as e:
            log(f"Agent {domain}: warehouse sync error: {e}")
    if synced:
        log(f"Warehouse sync: {synced}/{len(agents)} agents updated to {WAREHOUSE_ID}")


# ---- Persistent benchmark storage ------------------------------------------
def _bench_table(headers=None):
    """Per-user benchmark table: GAP_Demo_Dev_ssuman_bpcs_com.workbench.benchmark_runs."""
    cat = user_catalog(headers) if _in_request() or headers else CATALOG
    return f"{cat}.workbench.benchmark_runs"
_BENCH_TABLE = f"{CATALOG}.workbench.benchmark_runs"  # module-level fallback

def _ensure_workbench_tables(headers=None):
    """Create workbench schema + benchmark_runs table if not exist. OBO."""
    h = headers or get_headers()
    cat = user_catalog(h) if _in_request() or headers else CATALOG
    bt = _bench_table(h)
    ok1, _ = run_sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.workbench", h)
    ok2, _ = run_sql(f"""CREATE TABLE IF NOT EXISTS {bt} (
        run_id STRING, stage STRING, run_ts TIMESTAMP, user_email STRING,
        score INT, total INT, exact_count INT, tolerant_count INT,
        elapsed_secs DOUBLE, total_tokens BIGINT, results_json STRING
    ) USING DELTA""", h)
    if ok1 and ok2:
        log("Workbench tables ready")
    return ok1 and ok2

def _save_benchmark_run(label, results, elapsed, tokens, user_email, headers=None):
    """Persist a benchmark run to Delta table."""
    import uuid as _uuid
    h = headers or get_headers()
    try:
        _ensure_workbench_tables(h)
    except Exception as e:
        log(f"Cannot ensure tables for save: {e}")
        return
    run_id = str(_uuid.uuid4())
    score = sum(1 for r in results if r.get("verdict") in ("EXACT","TOLERANT"))
    exact = sum(1 for r in results if r.get("verdict") == "EXACT")
    tolerant = sum(1 for r in results if r.get("verdict") == "TOLERANT")
    # Safe JSON serialization — convert all values to strings
    safe = []
    for r in results:
        safe.append({k: str(v) if v is not None else "" for k, v in r.items()})
    # Spark SQL: '' is NOT an escaped quote (adjacent literals concatenate, apostrophe lost) -> use \'
    rj = json.dumps(safe).replace("\\", "\\\\").replace("'", "\\'")
    bt = _bench_table(h)
    stmt = f"""INSERT INTO {bt} VALUES (
        '{run_id}', '{label}', CURRENT_TIMESTAMP(), '{user_email}',
        {score}, {len(results)}, {exact}, {tolerant},
        {elapsed:.2f}, {tokens}, '{rj}'
    )"""
    ok, msg = run_sql(stmt, h)
    if ok:
        log(f"Benchmark run saved: {label} {score}/{len(results)}")
    else:
        log(f"Benchmark save failed: {msg}")

def _save_comparison_run(ea, eb, sa, sb, elapsed, user_email, headers=None):
    """Persist the latest comparison (both sides: scores, per-test verdicts, full answer, live steps)
    as one row with stage='Comparison' in the existing benchmark_runs table, so it survives restarts/redeploys."""
    import uuid as _uuid
    h = headers or get_headers()
    try:
        _ensure_workbench_tables(h)
    except Exception as e:
        log(f"Cannot ensure tables for comparison save: {e}")
        return
    def side(key, ep, secs):
        res = _ust().get(f"cmp_results_{key}") or []
        live = ST.get(f"cmp_live_{key}") or {}
        answer = live.get("answer") or next((r.get("answer") for r in res if r.get("answer")), "")
        rows = [{k: v for k, v in r.items() if k != "answer"} for r in res]  # full answer stored once per side
        return {"endpoint": ep, "elapsed": secs, "results": rows, "answer": answer,
                "live": {"status": live.get("status"), "calls": live.get("calls", 0), "t0": live.get("t0"),
                         "t_end": live.get("t_end"), "steps": [list(s) for s in live.get("steps", [])][-300:],
                         "answer": answer}}
    payload = {"a": side("a", ea, sa), "b": side("b", eb, sb), "elapsed": elapsed,
               "ts": _ust().get("cmp_last_ts"), "user": user_email,
               "usage_a": _ust().get("cmp_usage_a") or {}, "usage_b": _ust().get("cmp_usage_b") or {}}
    pa = sum(1 for r in payload["a"]["results"] if r.get("verdict") in ("EXACT", "TOLERANT"))
    total = len(payload["a"]["results"])
    rj = json.dumps(payload, default=str).replace("\\", "\\\\").replace("'", "\\'")
    bt = _bench_table(h)
    stmt = f"""INSERT INTO {bt} VALUES (
        '{_uuid.uuid4()}', 'Comparison', CURRENT_TIMESTAMP(), '{user_email}',
        {pa}, {total}, 0, 0, {elapsed:.2f}, 0, '{rj}'
    )"""
    ok, msg = run_sql(stmt, h)
    log("Comparison saved (survives restarts)" if ok else f"Comparison save failed: {msg}")


def _save_reliability_run(headers=None):
    """Persist the latest Reliability run (30 rephrased calls: verdicts, SQL, answers, log) as one row with
    stage='Reliability' in the existing benchmark_runs table, so it survives restarts/redeploys."""
    import uuid as _uuid
    h = headers or get_headers()
    try:
        _ensure_workbench_tables(h)
    except Exception as e:
        log(f"Cannot ensure tables for reliability save: {e}")
        return
    res = _ust().get("rob_results") or []
    sc = _rob_scores(res) if res else []
    rel = round(sum(s["reliability"] for s in sc) / len(sc)) if sc else 0
    dep = round(sum(s["dependability"] for s in sc) / len(sc)) if sc else 0
    t0, t1 = _ust().get("rob_t0") or 0, _ust().get("rob_t_end") or 0
    payload = {"version": _ust().get("rob_version"), "t0": t0, "t_end": t1, "user": _ust().get("rob_last_user"),
               "results": res, "log": (_ust().get("rob_log") or [])[-200:]}
    rj = json.dumps(payload, default=str).replace("\\", "\\\\").replace("'", "\\'")
    user = str(_ust().get("rob_last_user") or "").replace("'", "")
    bt = _bench_table(h)
    stmt = f"""INSERT INTO {bt} VALUES (
        '{_uuid.uuid4()}', 'Reliability', CURRENT_TIMESTAMP(), '{user}',
        {rel}, 100, {dep}, 0, {max(t1 - t0, 0):.2f}, 0, '{rj}'
    )"""
    ok, msg = run_sql(stmt, h)
    log("Reliability run saved (survives restarts)" if ok else f"Reliability save failed: {msg}")


def _restore_reliability(rj):
    """Load the latest persisted Reliability run back into the page state."""
    d = json.loads(rj)
    res = d.get("results") or []
    for r in res:  # numbers come back as numbers from JSON; keep ground truth numeric for display/scoring
        try:
            r["gt"] = float(r["gt"])
        except (TypeError, ValueError, KeyError):
            pass
    _ust().update(rob_results=res, rob_version=d.get("version"), rob_t0=d.get("t0"), rob_t_end=d.get("t_end"),
              rob_last_user=d.get("user"), rob_log=d.get("log") or [])


def _restore_comparison(rj):
    """Load the latest persisted comparison back into the live comparison state."""
    d = json.loads(rj)
    for key in ("a", "b"):
        s = d.get(key) or {}
        res = s.get("results") or []
        for r in res:
            r["answer"] = s.get("answer", "")
        _ust()[f"cmp_results_{key}"] = res
        _ust()[f"cmp_results_{key}_elapsed"] = float(s.get("elapsed") or 0)
        live = s.get("live") or {}
        live["steps"] = [tuple(x) for x in live.get("steps", [])]
        live.setdefault("answer", s.get("answer", ""))
        live.setdefault("calls", 0)
        if live.get("t0"):
            ST[f"cmp_live_{key}"] = live
    _ust()["cmp_last_eps"] = ((d.get("a") or {}).get("endpoint"), (d.get("b") or {}).get("endpoint"))
    _ust()["cmp_elapsed"] = float(d.get("elapsed") or 0)
    _ust()["cmp_last_ts"] = d.get("ts")
    _ust()["cmp_last_user"] = d.get("user")
    _ust()["cmp_usage_a"] = d.get("usage_a") or {}
    _ust()["cmp_usage_b"] = d.get("usage_b") or {}


def _load_persisted_state(headers=None):
    """Load benchmark history from Delta table into ST on startup."""
    h = headers or get_headers()
    try:
        bt = _bench_table(h)
        rows = run_sql_query(f"""
            SELECT stage, score, total, exact_count, tolerant_count,
                   elapsed_secs, total_tokens, run_ts, user_email, results_json
            FROM {bt}
            ORDER BY run_ts DESC
        """, h, timeout=30)
        if not rows:
            log("No persisted benchmark runs found")
            return
        # Map stage label → ST result key
        _stage_result_key = {
            "Baseline": "bench_results",
            "Iteration 1": "iter_it1_results",
            "Iteration 2": "iter_it2_results",
            "Iteration 3": "iter_it3_results",
        }
        # Latest run per stage (rows ordered by run_ts DESC)
        seen_stages = set()
        cmp_row = next((r for r in rows if r.get("stage") == "Comparison"), None)  # newest first
        # Restore only into a fresh process (after restart/redeploy) -- never over an in-session run
        if cmp_row and cmp_row.get("results_json") and not _ust().get("cmp_running") and not _ust().get("cmp_last_ts"):
            try:
                _restore_comparison(cmp_row["results_json"])
                log(f"Loaded latest comparison from {cmp_row.get('run_ts')}")
            except Exception as e:
                log(f"Comparison restore failed: {e}")
        rob_row = next((r for r in rows if r.get("stage") == "Reliability"), None)  # newest first
        # Restore only into a fresh process (after restart/redeploy) -- never over an in-session run
        if rob_row and rob_row.get("results_json") and not _ust().get("rob_running") and not _ust().get("rob_results"):
            try:
                _restore_reliability(rob_row["results_json"])
                log(f"Loaded latest Reliability run from {rob_row.get('run_ts')}")
            except Exception as e:
                log(f"Reliability restore failed: {e}")
        for row in rows:
            stage = row.get("stage", "")
            if stage in ("Comparison", "Reliability"):  # not benchmark stages
                continue
            if stage and stage not in seen_stages:
                seen_stages.add(stage)
                _ust()["benchmarks"][stage] = int(row.get("score", 0))
                # Parse results_json for this stage
                rj = row.get("results_json", "")
                result_key = _stage_result_key.get(stage)
                if result_key and rj:
                    try:
                        loaded = json.loads(rj)
                        for r in loaded:
                            if "expected" in r:
                                try: r["expected"] = float(r["expected"])
                                except (ValueError, TypeError): pass
                        ST[result_key] = loaded
                    except Exception as e:
                        log(f"Results JSON parse for {stage}: {e}")
                # Store metadata for Baseline specifically
                if stage == "Baseline":
                    _ust()["bench_elapsed"] = float(row.get("elapsed_secs", 0))
                    _ust()["bench_total_tokens"] = int(row.get("total_tokens", 0))
                    _ust()["bench_last_ts"] = str(row.get("run_ts", ""))
                    _ust()["bench_last_user"] = row.get("user_email", "?")
                # Mark iteration as done
                _iter_done_map = {"Iteration 1": "it1", "Iteration 2": "it2", "Iteration 3": "it3"}
                if stage in _iter_done_map:
                    _ust()["iterations_done"].add(_iter_done_map[stage])
        log(f"Loaded {len(seen_stages)} benchmark stages: {dict(_ust()['benchmarks'])}")
    except Exception as e:
        log(f"Load persisted state: {e} (table may not exist yet)")

def _fetch_supervisor_tools(h, sup_name):
    """Tools of one supervisor: [{tool_id, type, space_id, mcp, desc}] -- [] if not readable."""
    try:
        r = requests.get(f"{HOST}/api/2.1/{sup_name}/tools", headers=h, timeout=15)
        if r.status_code != 200:
            return []
        out = []
        for t in r.json().get("tools", []):
            out.append(dict(tool_id=t.get("tool_id", ""), type=t.get("tool_type", ""),
                            space_id=(t.get("genie_space") or {}).get("space_id", ""),
                            mcp=(t.get("uc_mcp") or {}).get("name", ""),
                            desc=t.get("description", "")))
        return out
    except Exception:
        return []


def _normalize_mas_agents(agents):
    """multi-agent-supervisors 'agents' -> the same tool format as _fetch_supervisor_tools."""
    out = []
    for a in agents or []:
        at = (a.get("agent_type") or "").replace("-", "_")
        uc = (a.get("unity_catalog_function") or {}).get("uc_path") or {}
        out.append(dict(tool_id=a.get("name", ""),
                        type={"genie_space": "genie_space", "uc_mcp": "uc_mcp",
                              "unity_catalog_function": "uc_function"}.get(at, at),
                        space_id=(a.get("genie_space") or {}).get("id", ""),
                        mcp=(a.get("uc_mcp") or {}).get("name", ""),
                        fn=".".join(x for x in (uc.get("catalog"), uc.get("schema"), uc.get("name")) if x),
                        desc=a.get("description", "")))
    return out


def _resolve_supervisor_from_tile(h, tile_id):
    """Look up a supervisor's display name / description / tools by its tile id (from the serving endpoint).
    Tries each API that exposes it; returns {} if none is readable with this token."""
    for path in (f"/api/2.0/multi-agent-supervisors/{tile_id}", f"/api/2.0/tiles/{tile_id}",
                 f"/api/2.1/supervisor-agents/{tile_id}"):
        try:
            r = requests.get(f"{HOST}{path}", headers=h, timeout=15)
            if r.status_code != 200:
                continue
            b = r.json()
            mas = b.get("multi_agent_supervisor") or {}
            tile = mas.get("tile") or b
            name = tile.get("name") or tile.get("display_name") or ""
            if not name:
                continue
            ct = tile.get("creation_timestamp_ms")
            created = (datetime.utcfromtimestamp(ct / 1000).strftime("%Y-%m-%d %H:%M") if ct
                       else (tile.get("create_time") or "")[:16].replace("T", " "))
            return dict(display_name=name, desc_full=tile.get("description", ""), create_time=created,
                        tools=_normalize_mas_agents(mas.get("agents")))
        except Exception:
            continue
    return {}


def discover_supervisors(headers=None):
    """Discover supervisor agents (OBO).
    Tries supervisor-agents API, then serving endpoints."""
    h = headers or get_headers()
    if "Authorization" not in h:
        log("discover_supervisors: skipped — no OBO token yet (will retry on next poll)")
        return _ust().get("supervisors", [])  # return cached data, don't overwrite with empty
    found = []
    alias = _get_user_alias(h)
    # Try supervisor-agents API
    try:
        r = requests.get(f"{HOST}/api/2.1/supervisor-agents", headers=h, timeout=15)
        # Only update status when we got a real response (not a missing-auth 403)
        if r.status_code != 403:
            ST["sup_api_status"] = r.status_code
        elif not ST.get("_sup_403_logged"):
            # Log once per app session — this 403 is expected (OBO token lacks
            # supervisor-agents scope). Serving-endpoints fallback always works.
            ST["sup_api_status"] = r.status_code
            ST["_sup_403_logged"] = True
            log(f"discover_supervisors: supervisor-agents API returned 403 (expected — using serving-endpoints fallback)")
        if r.status_code == 200:
            all_sups = r.json().get("supervisor_agents", [])
            log(f"discover_supervisors: {len(all_sups)} supervisor agents from API")
            sc_sups = [s for s in all_sups if "Supply Chain" in s.get("display_name", "") or "supply chain" in s.get("display_name", "").lower()]
            # Strict: only match user-scoped supervisors when alias is set
            if alias and alias != "unknown":
                user_sups = [s for s in sc_sups if f"({alias})" in s.get("display_name", "")]
                if not user_sups:
                    _swk = f"_warned_no_sups_{alias}"
                    if not ST.get(_swk):
                        ST[_swk] = True
                        log(f"discover_supervisors: WARNING — no supervisors found for alias={alias}. Run Setup to create them.")
                sc_sups = user_sups  # empty list if none match — no silent fallback
            for s in sc_sups:
                dn = s.get("display_name", "")
                tools = _fetch_supervisor_tools(h, s["name"])
                # Fallback: if direct tools endpoint failed (403), try via serving endpoint tile_id
                if not tools and s.get("endpoint_name"):
                    try:
                        _ep_r = requests.get(f"{HOST}/api/2.0/serving-endpoints/{s['endpoint_name']}",
                                             headers=h, timeout=15)
                        if _ep_r.status_code == 200:
                            _tid = (_ep_r.json().get("tile_endpoint_metadata") or {}).get("tile_id")
                            if _tid:
                                _info = _resolve_supervisor_from_tile(h, _tid)
                                tools = _info.get("tools", []) if _info else []
                                if tools:
                                    log(f"discover_supervisors: resolved {len(tools)} tools for {dn} via tile fallback")
                    except Exception:
                        pass
                found.append(dict(name=s["name"], display_name=dn,
                                  endpoint=s.get("endpoint_name",""),
                                  desc=s.get("description","")[:80],
                                  desc_full=s.get("description",""),
                                  create_time=s.get("create_time",""),
                                  tools=tools))
        elif r.status_code != 403:
            log(f"Supervisor API: HTTP {r.status_code} (falling back to serving endpoints)")
    except Exception as e:
        ST["sup_api_status"] = f"error: {e}"
        log(f"Supervisor API error: {e}")
    # Also scan serving endpoints
    existing_eps = {f.get("endpoint","") for f in found}
    try:
        er = requests.get(f"{HOST}/api/2.0/serving-endpoints", headers=h, timeout=15)
        log(f"discover_supervisors: serving-endpoints HTTP {er.status_code}, {len(er.json().get('endpoints',[])) if er.status_code==200 else 'N/A'} endpoints")
        if er.status_code == 200:
            for ep in er.json().get("endpoints", []):
                n = ep.get("name", "")
                if n.startswith("mas-") and n.endswith("-endpoint") and n not in existing_eps:
                    existing_eps.add(n)
                    tags = ep.get("tags", [])
                    display = n
                    tag_desc = ""
                    for t in tags:
                        if t.get("key") == "display_name":
                            display = t.get("value", n)
                        elif t.get("key") == "supervisor_description":
                            tag_desc = t.get("value", "")
                    # Strict: only match user-scoped supervisors
                    if alias and alias != "unknown" and f"({alias})" not in display:
                        continue
                    entry = dict(name=n, display_name=display, endpoint=n, desc="Supervisor (serving endpoint)",
                                 desc_full=tag_desc)
                    tile_id = (ep.get("tile_endpoint_metadata") or {}).get("tile_id")
                    if tile_id:
                        info = _resolve_supervisor_from_tile(h, tile_id)
                        if info:
                            entry.update(info, desc=info["desc_full"][:80] or entry["desc"])
                    found.append(entry)
    except Exception as e:
        log(f"Serving endpoint scan error: {e}")
    log(f"discover_supervisors: {len(found)} total")
    return found

def run_sql(statement, headers=None, timeout=60):
    """Execute a SQL statement via the SQL Statement Execution API (OBO).
    Works with 'sql' scope — no Jobs API needed. Returns (success, message)."""
    h = headers or get_headers()
    wait_s = min(max(timeout, 5), 50)   # API requires 0 or 5-50s
    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": statement,
        "wait_timeout": f"{wait_s}s",
        "on_wait_timeout": "CANCEL",
    }
    r = requests.post(f"{HOST}/api/2.0/sql/statements/", headers=h, json=payload, timeout=timeout+10)
    if r.status_code == 200:
        data = r.json()
        status = data.get("status", {}).get("state", "")
        if status == "SUCCEEDED":
            return True, "OK"
        elif status == "FAILED":
            err = data.get("status", {}).get("error", {}).get("message", "Unknown SQL error")
            return False, err
        else:
            return False, f"SQL state: {status}"
    else:
        return False, f"HTTP {r.status_code}: {r.text[:200]}"

def run_sql_query(statement, headers=None, timeout=60):
    """Execute a SQL query and return rows as list of dicts. OBO via SQL Statement API."""
    h = headers or get_headers()
    wait_s = min(max(timeout, 5), 50)   # API requires 0 or 5-50s
    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": statement,
        "wait_timeout": f"{wait_s}s",
        "on_wait_timeout": "CANCEL",
    }
    r = requests.post(f"{HOST}/api/2.0/sql/statements/", headers=h, json=payload, timeout=timeout+10)
    if r.status_code == 200:
        data = r.json()
        state = data.get("status", {}).get("state", "")
        if state == "SUCCEEDED":
            cols = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
            rows = []
            for chunk in data.get("result", {}).get("data_array", []):
                rows.append(dict(zip(cols, chunk)))
            return rows
        else:
            err = data.get("status", {}).get("error", {}).get("message", "")
            log(f"SQL query state={state}: {err[:200]}")
    else:
        log(f"SQL Statement API HTTP {r.status_code}: {r.text[:200]}")
    return []

def trigger_job(job_id, headers=None, notebook_params=None, python_params=None):
    """Trigger a pre-created job via run-now. The job has run_as=user, so it runs
    as the logged-in user with their full UC permissions. Returns (run_id, error).
    python_params: for spark_python_task jobs, overrides ALL CLI args."""
    h = headers or get_sp_headers()
    body = {"job_id": int(job_id)}
    if notebook_params:
        body["notebook_params"] = notebook_params
    if python_params:
        body["python_params"] = python_params
    r = requests.post(f"{HOST}/api/2.1/jobs/run-now", headers=h,
                      json=body, timeout=30)
    if r.status_code == 200:
        run_id = r.json().get("run_id")
        log(f"Triggered job {job_id}: run_id={run_id}")
        return run_id, None
    else:
        err = f"HTTP {r.status_code}: {r.text[:200]}"
        log(f"Failed to trigger job {job_id}: {err}")
        return None, err


def check_warehouse(headers=None):
    """Check SQL warehouse status via OBO. Returns state string."""
    h = headers or get_headers()
    try:
        r = requests.get(f"{HOST}/api/2.0/sql/warehouses/{WAREHOUSE_ID}", headers=h, timeout=15)
        if r.status_code == 200:
            state = r.json().get("state", "UNKNOWN")
            name = r.json().get("name", WAREHOUSE_ID)
            log(f"Warehouse '{name}': {state}")
            ST["warehouse_state"] = state
            ST["warehouse_name"] = name
            return state
        else:
            log(f"Warehouse check HTTP {r.status_code}: {r.text[:200]}")
            return "UNKNOWN"
    except Exception as e:
        log(f"Warehouse check error: {e}")
        return "ERROR"

def ensure_warehouse_running(headers=None):
    """Start warehouse if stopped, poll until RUNNING. Returns True if ready."""
    h = headers or get_headers()
    state = check_warehouse(h)
    if state == "RUNNING":
        return True
    if state in ("STOPPED", "STOPPING"):
        log("Starting SQL warehouse...")
        try:
            r = requests.post(f"{HOST}/api/2.0/sql/warehouses/{WAREHOUSE_ID}/start", headers=h, timeout=15)
            if r.status_code not in (200, 202):
                log(f"Warehouse start HTTP {r.status_code}: {r.text[:200]}")
                return False
        except Exception as e:
            log(f"Warehouse start error: {e}")
            return False
    # Poll until running (max 3 min)
    for i in range(36):
        time.sleep(5)
        state = check_warehouse(h)
        if state == "RUNNING":
            log("Warehouse is RUNNING")
            return True
        if state in ("STOPPED", "DELETED"):
            log(f"Warehouse startup failed: {state}")
            return False
    log("Warehouse startup timed out after 3 min")
    return False

def poll_run(run_id, timeout_s=1800, headers=None):
    """Poll a job run until completion. Returns (state, message).
    Pass headers explicitly when called from a background thread."""
    h = headers or get_sp_headers()
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        r = requests.get(f"{HOST}/api/2.1/jobs/runs/get?run_id={run_id}", headers=h, timeout=15)
        if r.status_code == 200:
            state = r.json().get("state", {})
            life = state.get("life_cycle_state", "")
            result = state.get("result_state", "")
            if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                msg = state.get("state_message", "")
                if result == "FAILED":
                    msg = _fetch_run_error(run_id, h) or msg
                return result or life, msg
        time.sleep(10)
    return "TIMEOUT", f"Exceeded {timeout_s}s"

def _fetch_run_error(run_id, headers):
    """Fetch the actual error message from a failed run (not just 'Workload failed')."""
    try:
        r = requests.get(f"{HOST}/api/2.1/jobs/runs/get-output?run_id={run_id}", headers=headers, timeout=15)
        if r.status_code == 200:
            data = r.json()
            err = data.get("error", "")
            if err:
                return err.split("\n")[0][:300]  # first line, max 300 chars
        # Try task-level run IDs
        r2 = requests.get(f"{HOST}/api/2.1/jobs/runs/get?run_id={run_id}", headers=headers, timeout=15)
        for task in r2.json().get("tasks", []):
            trid = task.get("run_id")
            if trid:
                r3 = requests.get(f"{HOST}/api/2.1/jobs/runs/get-output?run_id={trid}", headers=headers, timeout=15)
                err = r3.json().get("error", "")
                if err:
                    return err.split("\n")[0][:300]
    except Exception:
        pass
    return ""


def _cancel_job_run(run_id, headers=None):
    h = headers or get_sp_headers()
    try:
        requests.post(f"{HOST}/api/2.1/jobs/runs/cancel", headers=h, json={"run_id": int(run_id)}, timeout=15)
    except Exception:
        pass


def _run_supervisor_refresh_job(iter_key, tok, _log_step, obo_headers=None):
    """Trigger this iteration's Supervisor job (run_as = user) and wait for it.
    Non-fatal: any failure is logged loudly and the iteration continues (domain-agent
    benchmark does not depend on the Supervisor). User Stop cancels the job run.
    obo_headers: per-user OBO headers captured at button-click time (Flask context).
    MUST be passed — _ust()['user_alias'] is global and can contain another user's alias."""
    _user_email = _get_user_from_headers(obo_headers) if obo_headers else "unknown"
    job_id = _get_user_job(_user_email, iter_key)
    if not job_id:
        _log_step(f"\u26a0 Supervisor NOT updated: no job found for {iter_key} / {_user_email}")
        return None
    _alias = _get_user_alias(obo_headers) if obo_headers else (_ust().get("user_alias") or "")
    _cat = user_catalog(obo_headers) if obo_headers else (_ust().get("user_catalog") or CATALOG)
    _iter_num = iter_key.replace("it", "")
    _params = ["--mode", "supervisor-job",
               "--iteration", _iter_num,
               "--catalog", _cat,
               "--user-alias", _alias]
    _log_step(f"Updating Supervisor via job {job_id} (run_as={_user_email})...")
    rid, err = trigger_job(job_id, python_params=_params)
    if err or not rid:
        _log_step(f"\u26a0 Supervisor NOT updated: could not trigger job {job_id}: {err or 'unknown error'}")
        return None
    # Store run ID so _iter_stop_now can cancel it even if the bg thread
    # hasn't entered the polling loop yet (edge case: Stop between trigger and poll).
    _ust()["_iter_sup_rid"] = rid
    h = get_sp_headers()
    try:
        _log_step(f"  Supervisor job run_id={rid}  \u2192 {HOST}/#job/{job_id}/run/{rid}")
    except InterruptedError:
        _cancel_job_run(rid, h)
        log(f"{iter_key}: supervisor job run {rid} cancelled (stop before poll)")
        raise
    t0 = time.time()
    while time.time() - t0 < 1800:
        if not _iter_is_current(tok):
            _cancel_job_run(rid, h)
            log(f"{iter_key}: supervisor job run {rid} cancelled after user stop")
            raise InterruptedError("Stopped by user")
        try:
            r = requests.get(f"{HOST}/api/2.1/jobs/runs/get?run_id={rid}", headers=h, timeout=15)
        except Exception as e:
            log(f"{iter_key}: supervisor job poll error (retrying): {e}")
            time.sleep(5)
            continue
        if r.status_code == 200:
            state = r.json().get("state", {})
            life = state.get("life_cycle_state", "")
            result = state.get("result_state", "")
            if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                if result == "SUCCESS":
                    _log_step("  Supervisor updated \u2713")
                    ST.pop("_iter_sup_rid", None)
                    return rid
                detail = _fetch_run_error(rid, h) or state.get("state_message", "") or result or life
                _log_step(f"\u26a0 Supervisor NOT updated: job run {rid} failed: {detail}")
                ST.pop("_iter_sup_rid", None)
                return None
        time.sleep(5)
    _cancel_job_run(rid, h)
    ST.pop("_iter_sup_rid", None)
    _log_step(f"\u26a0 Supervisor NOT updated: job run {rid} timed out (30 min) and was cancelled")
    return None

def call_supervisor(ep, prompt, max_tokens=8192, timeout=600, headers=None):
    """headers: OBO headers captured in the callback -- REQUIRED when called from a background thread."""
    h = headers or get_headers()
    t0 = time.time()
    if "Authorization" not in h:
        return "ERROR:NO_AUTH:no user token available (called outside a request without captured OBO headers)", 0.0, {}
    r = requests.post(f"{HOST}/serving-endpoints/{ep}/invocations", headers=h,
                      json={"input":[{"role":"user","content":prompt}],"max_tokens":max_tokens}, timeout=timeout)
    elapsed = time.time() - t0
    if r.status_code != 200:
        return f"ERROR:{r.status_code}:{r.text[:300]}", elapsed, {}
    body = r.json()
    texts = []
    for item in body.get("output",[]):
        if item.get("role") != "assistant": continue
        for ct in item.get("content",[]):
            if ct.get("type") == "output_text" and ct.get("text"): texts.append(ct["text"])
    usage = body.get("usage", {})
    return "\n".join(texts) if texts else "(empty)", elapsed, usage

_NAME_TAG = re.compile(r"^\s*<name>(.*?)</name>\s*$", re.S)


SUP_MAX_CONTINUATIONS = 12  # long_task resumes; each round is one supervisor request window


def call_supervisor_stream(ep, prompt, headers=None, on_step=None, max_tokens=8192, timeout=600):
    """Streaming supervisor call. on_step(kind, text) receives live steps:
    call (tool + question), result (sub-agent output), progress (MCP progress), route, answer (text delta).
    Returns (final_answer_text, elapsed, usage) like call_supervisor."""
    h = headers or get_headers()
    emit = on_step or (lambda k, t: None)
    t0 = time.time()
    if "Authorization" not in h:
        return "ERROR:NO_AUTH:no user token available (called outside a request without captured OBO headers)", 0.0, {}
    # long_task: before its per-request limit (~290 s) the supervisor checkpoints and emits a
    # task_continue_request. We resend the conversation + checkpoint + task_continue_response and it
    # resumes where it stopped. Without this option it stops at the limit with "Continue / Wrap up".
    convo = list(prompt) if isinstance(prompt, list) else [{"role":"user","content":prompt}]
    deltas, last_delta_id, messages, usage = {}, None, [], {}
    seen_progress, pending_name = set(), None
    for _round in range(SUP_MAX_CONTINUATIONS + 1):
        r = requests.post(f"{HOST}/serving-endpoints/{ep}/invocations", headers=h, stream=True,
                          json={"input": convo, "max_tokens":max_tokens, "stream":True,
                                "databricks_options": {"long_task": True}},
                          timeout=(15, timeout))
        if r.status_code != 200:
            return f"ERROR:{r.status_code}:{r.text[:300]}", time.time() - t0, {}
        r.encoding = "utf-8"
        round_items, cont = [], None
        for line in r.iter_lines(decode_unicode=True):
            if not line or not line.startswith("data:"):
                continue
            payload = line[5:].strip()
            if payload == "[DONE]":
                break
            try:
                ev = json.loads(payload)
            except Exception:
                continue
            et, it = ev.get("type", ""), ev.get("item") or {}
            if et == "response.output_item.done" and it:
                if it.get("type") == "task_continue_request":
                    cont = it
                    continue
                round_items.append(it)
            if et == "response.output_text.delta":
                iid = ev.get("item_id") or "_"
                deltas[iid] = deltas.get(iid, "") + (ev.get("delta") or "")
                last_delta_id = iid
                emit("answer", ev.get("delta") or "")
            elif et == "response.output_item.done" and it.get("type") == "function_call":
                args = it.get("arguments") or ""
                try:
                    a = json.loads(args)
                    q = a.get("genie_query") or a.get("question") or a.get("query") or ""
                    if not q and a.get("conversation_id"):
                        q = "polling for the answer"
                except Exception:
                    q = args
                emit("call", f"{it.get('name','tool')}: {q}" if q else it.get("name", "tool"))
            elif et == "response.output_item.done" and it.get("type") == "function_call_output":
                out = str(it.get("output") or "")
                bullets = [b.strip() for b in out.splitlines() if b.strip().startswith("\u2022")]
                if out.startswith("Still running") and bullets:
                    for b in bullets:
                        if b not in seen_progress:
                            seen_progress.add(b); emit("progress", b.lstrip("\u2022 ").strip())
                elif '"status":"in_progress"' in out.replace(" ", ""):
                    emit("progress", "question accepted -- working")
                else:
                    # Drop agent-directed HTML comments and collapsed reasoning blocks; keep the readable result
                    clean = re.sub(r"<!--.*?--\s*>", "", out, flags=re.S)
                    clean = re.sub(r"<details>.*?</details>", "", clean, flags=re.S).strip()
                    emit("result", clean or out)
            elif et == "response.output_item.done" and it.get("type") == "message":
                txt = "".join(ct.get("text", "") for ct in it.get("content", []) if ct.get("type") == "output_text")
                mt = _NAME_TAG.match(txt)
                if mt:
                    pending_name = mt.group(1).strip()
                    emit("route", pending_name)
                    continue
                messages.append((it.get("id"), txt))
                if it.get("id") != last_delta_id:
                    emit("result", (f"[{pending_name}] " if pending_name else "") + txt)
            elif et in ("response.completed", "response.done"):
                usage = (ev.get("response") or {}).get("usage", {}) or usage
        if not cont:
            # Fallback: supervisor generated "Continue / Wrap up" text instead of a proper
            # task_continue_request. Detect it and auto-send "continue" as the user would.
            _round_text = deltas.get(last_delta_id, "") if last_delta_id else ""
            if not _round_text and messages:
                _round_text = messages[-1][1]
            if _is_supervisor_time_limit(_round_text) and _round < SUP_MAX_CONTINUATIONS:
                emit("progress", f"Supervisor hit 290s window — sending 'continue' automatically (round {_round + 1})")
                # Strip the timeout text from accumulated answer; it's not a real response
                if last_delta_id and last_delta_id in deltas:
                    deltas[last_delta_id] = ""
                convo = convo + round_items + [{"role": "user", "content": "continue"}]
                continue
            break
        emit("progress", f"Supervisor checkpoint after {cont.get('step', '?')} tool steps \u2014 continuing automatically")
        convo = convo + round_items + [cont, {"type": "task_continue_response", "continue_request_id": cont["id"]}]
    elapsed = time.time() - t0
    final = ""
    if last_delta_id:
        final = next((t for i, t in reversed(messages) if i == last_delta_id), "") or deltas.get(last_delta_id, "")
    if not final and messages:
        final = messages[-1][1]
    if not final and not messages and not deltas:
        # Endpoint produced no stream events -- fall back to the classic call so the run still scores
        emit("progress", "no streamed events -- falling back to a standard call")
        last_user = prompt[-1]["content"] if isinstance(prompt, list) else prompt
        return call_supervisor(ep, last_user, max_tokens=max_tokens, timeout=timeout, headers=h)
    return final or "(empty)", elapsed, usage


def teardown_agents(keep_label=None):
    h = get_headers()
    alias = _get_user_alias(h)
    r = requests.get(f"{HOST}/api/2.0/genie/spaces", headers=h)
    deleted, kept, errors = [], [], []
    for s in r.json().get("spaces", []):
        t = s.get("title", "")
        if "SC" not in t: continue
        # Scope to user's agents
        if alias and alias != "unknown" and f"({alias})" not in t:
            continue
        if keep_label and (keep_label in t or "Evaluator" in t):
            kept.append(t)
            continue
        dr = requests.delete(f"{HOST}/api/2.0/genie/spaces/{s['space_id']}", headers=h)
        if dr.status_code in (200, 204):
            deleted.append(t)
        else:
            errors.append(f"{t}: HTTP {dr.status_code} - {dr.text[:120]}")
    return deleted, kept, errors

def teardown_supervisors():
    h = get_headers()
    alias = _get_user_alias(h)
    deleted, errors = [], []
    try:
        r = requests.get(f"{HOST}/api/2.1/supervisor-agents", headers=h, timeout=15)
        if r.status_code != 200:
            return [], [f"List failed: HTTP {r.status_code} - {r.text[:120]}"]
        for s in r.json().get("supervisor_agents", []):
            dn = s.get("display_name", "")
            if not ("Supply Chain" in dn or "supply chain" in dn.lower()):
                continue
            if alias and alias != "unknown" and f"({alias})" not in dn:
                continue
            if True:
                dr = requests.delete(f"{HOST}/api/2.1/{s['name']}", headers=h)
                if dr.status_code in (200, 204):
                    deleted.append(dn)
                else:
                    errors.append(f"{dn}: HTTP {dr.status_code} - {dr.text[:120]}")
    except Exception as e:
        errors.append(f"API error: {e}")
    return deleted, errors

# ---- State -----------------------------------------------------------------
ST = dict(agents={}, supervisors=[], benchmarks={}, iterations_done=set(), iter_running=None, iter_setup_phase=False, iter_setup_log=[], iter_setup_error=False, iter_setup_done=False, iter_setup_t0=0, app_ready=False, logs=[])

# ---- Per-user state partitioning -------------------------------------------
# Background threads have no Flask context; store the alias in thread-local.
_thread_alias = threading.local()

def _ust(alias=None):
    """User STate: returns per-user partition of the global ST dict.
    In a Dash callback (Flask context) → alias from OBO JWT.
    In a background thread → alias from _thread_alias (set by _spawn_bg).
    Fallback → '_default' partition."""
    a = alias \
        or getattr(_thread_alias, "alias", None) \
        or (_get_user_alias() if _in_request() else None) \
        or "_default"
    key = f"_u_{a}"
    if key not in ST:
        ST[key] = dict(
            agents={}, supervisors=[], agent_versions={},
            benchmarks={}, iterations_done=set(),
            table_count=0, user_catalog="", catalog_exists=False, user_alias="",
            overview_agent_version=None, _tc_loaded=False, test_catalog=[], tc_meta={}, tc_source="",
            sup_learned={},
            # Benchmark
            bench_results=[], bench_running=False, bench_elapsed=0,
            bench_last_ts="", bench_last_user="", bench_stop_requested=False, bench_total_tokens=0,
            # Comparison
            cmp_results_a=[], cmp_results_b=[], cmp_running=False,
            cmp_results_a_elapsed=0, cmp_results_b_elapsed=0,
            cmp_usage_a={}, cmp_usage_b={}, cmp_live_a={}, cmp_live_b={},
            cmp_name_a="", cmp_name_b="", cmp_last_eps=None, cmp_last_ts="", cmp_last_user="", cmp_elapsed=0,
            # Reliability / Robustness
            rob_results=[], rob_running=False, rob_version=None,
            rob_t0=0, rob_t_end=0, rob_last_user="", rob_log=[], rob_total_tokens=0,
            # Iterations
            iter_running=None, iter_setup_phase=False, iter_setup_log=[], iter_setup_error=False,
            iter_setup_done=False, iter_setup_t0=0, iter_setup_t_end=0, iter_run_token=None,
            iter_stopped_key="", iter_bench_active=False,
            iter_it1_results=[], iter_it2_results=[], iter_it3_results=[],
            iter_manual_step=False, iter3_agent_spaces={},
            _iter_cloned_spaces={}, _iter_sup_rid=None, _iter_ui_epoch=0,
            # Setup / Teardown
            setup_running=False, setup_log=[], setup_step="", setup_tasks={},
            td_running=False, td_log=[],
            # Chat
            chat_run={},
            _active_tab="overview", _it3_gate_rendered=False, _last_page_fp="",
        )
    return ST[key]

def _spawn_bg(fn, *args, **kwargs):
    """Spawn a background thread with per-user alias context.
    Captures the current user alias (from Flask request) and injects it
    into the thread-local before the worker runs — so _ust() works."""
    alias = _get_user_alias() if _in_request() else getattr(_thread_alias, "alias", "_default")
    def _wrapper():
        _thread_alias.alias = alias
        fn(*args, **kwargs)
    threading.Thread(target=_wrapper, daemon=True).start()


def _build_loading_page(page_title="Loading..."):
    """Show a branded loading skeleton while initial state loads."""
    return html.Div([
        html.Div([
            html.Div(style={"width":"40px","height":"40px","borderRadius":"50%",
                            "border":f"3px solid {C['soft']}","borderTopColor":C["indigo"],
                            "animation":"spin 1s linear infinite","marginBottom":"16px"}),
            html.H3(page_title, style={"color":C["ink"],"marginBottom":"6px"}),
            html.P("Discovering environment \u2014 agents, warehouse, benchmarks...",
                   style={"color":C["muted"],"fontSize":"13px","margin":"0"}),
        ], style={"display":"flex","flexDirection":"column","alignItems":"center",
                  "justifyContent":"center","padding":"60px 20px","textAlign":"center"}),
        html.Style("@keyframes spin{to{transform:rotate(360deg)}} @keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.4;transform:scale(.85)}}"),
    ], className="stage")

def _build_warehouse_banner():
    """Pulsating warehouse banner for STARTING/STOPPED states. Empty div if RUNNING."""
    wh_state = ST.get("warehouse_state", "UNKNOWN")
    wh_name = ST.get("warehouse_name", "warehouse")
    if wh_state == "RUNNING":
        return html.Div()
    if wh_state in ("STARTING", "UNKNOWN"):
        return html.Div([
            html.Div([
                html.Div(style={"width":"16px","height":"16px","borderRadius":"50%",
                                "border":f"2px solid {C['indigo']}","borderTopColor":"transparent",
                                "animation":"spin 1s linear infinite","flexShrink":"0"}),
                html.Span(f"Warehouse {wh_name} is {wh_state.lower()}\u2026",
                           style={"fontWeight":"600","fontSize":"14px"}),
            ], style={"display":"flex","alignItems":"center","gap":"10px"}),
            html.P("Some data may be unavailable until the warehouse is running.",
                   style={"margin":"4px 0 0","fontSize":"12px","color":C["muted"]}),
        ], style={"background":C["indigo_bg"],"border":f"1px solid {C['indigo']}","borderRadius":"12px",
                  "padding":"12px 18px","marginBottom":"20px","color":C["indigo"],
                  "animation":"whPulse 2s ease-in-out infinite"})
    # STOPPED / STOPPING
    return html.Div([
        html.Span(f"\u26a0 Warehouse {wh_name} is {wh_state.lower()}", style={"fontWeight":"600","fontSize":"14px"}),
        html.P("Start the warehouse from Setup & Teardown or wait for auto-start.",
               style={"margin":"4px 0 0","fontSize":"12px","color":C["muted"]}),
    ], style={"background":C["rose_bg"],"border":f"1px solid {C['rose']}","borderRadius":"12px",
              "padding":"12px 18px","marginBottom":"20px","color":C["rose"]})

def log(msg):
    ST["logs"].append(f"[{datetime.now():%H:%M:%S}] {msg}")
    if len(ST["logs"]) > 200: ST["logs"] = ST["logs"][-100:]


import functools as _functools, traceback as _traceback


def _bg_guard(*flags, on_fail=None):
    """Crash-safety for background worker threads. If the worker raises, the error is logged
    (Logs tab + ST['bg_errors']), its busy flags are cleared so the UI/nav never stays locked,
    and the optional on_fail hook records a visible error. Normal completion is untouched."""
    def deco(fn):
        @_functools.wraps(fn)
        def wrapper(*a, **kw):
            try:
                return fn(*a, **kw)
            except Exception as e:
                try:
                    log(f"ERROR in {fn.__name__}: {type(e).__name__}: {e}")
                    ST.setdefault("bg_errors", []).append(
                        {"worker": fn.__name__, "error": f"{type(e).__name__}: {e}",
                         "trace": _traceback.format_exc()[-2000:], "at": time.time()})
                    del ST["bg_errors"][:-20]
                except Exception:
                    pass
                for f in flags:
                    _ust()[f] = False
                if on_fail:
                    try:
                        on_fail(e)
                    except Exception:
                        pass
        return wrapper
    return deco


def _thread_excepthook(args):
    """Last-resort logger for any other thread exception (never re-raises into the server)."""
    try:
        log(f"ERROR in thread {getattr(args.thread, 'name', '?')}: {args.exc_type.__name__}: {args.exc_value}")
    except Exception:
        pass


threading.excepthook = _thread_excepthook


def _chat_fail(e):
    run = _ust().get("chat_run") or {}
    tgt = run.get("target")
    if tgt:
        _chat_session(tgt)["history"].append({"role": "error", "text": f"Request failed: {type(e).__name__}: {e}"})
    run.update(running=False, status="error", t_end=time.time())


def _iter_bench_fail(e):
    _ust()["iter_bench_active"] = None
    _ust()["iter_setup_phase"] = None

# ---- Plotly template -------------------------------------------------------
PT = go.layout.Template(layout=go.Layout(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Instrument Sans, system-ui", size=13, color="#888"),
    xaxis=dict(gridcolor="rgba(128,128,128,0.15)", zerolinecolor="rgba(128,128,128,0.2)"),
    yaxis=dict(gridcolor="rgba(128,128,128,0.15)", zerolinecolor="rgba(128,128,128,0.2)"),
    colorway=[HEX["indigo"],HEX["teal"],HEX["rose"],"#F59E0B","#8B5CF6","#6366F1","#10B981"],
))

# ==== Dash App ==============================================================
app = dash.Dash(__name__, external_stylesheets=[FONTS_URL],
               suppress_callback_exceptions=True, title="Genie Curator Workbench",
               update_title=None)  # keep tab title fixed (Dash default flips it to "Updating..." on every poll)
server = app.server

# ---- Activity heartbeat (for idle-stop job) --------------------------------
# The lifecycle job needs to know if the app has real user traffic.
# We track in memory and also persist via the SQL Statement API to a
# lightweight Delta table the job can SELECT from.
_last_user_activity = time.time()
_HB_INTERVAL       = 300           # persist at most once per 5 min
_last_hb_persist    = 0
_hb_table_ready     = False         # set after first CREATE TABLE succeeds
_WH_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "e3a0fc2c08db05bb")

def _sql_fire(sql, wait="0s"):
    """Execute SQL via Statement API. wait='0s' = async (fire-and-forget)."""
    h = get_sp_headers()
    requests.post(f"{HOST}/api/2.0/sql/statements", headers=h, json={
        "warehouse_id": _WH_ID, "statement": sql, "wait_timeout": wait,
    }, timeout=5)

def _persist_heartbeat():
    """Best-effort: upsert a single-row heartbeat via SQL Statement API."""
    global _last_hb_persist, _hb_table_ready
    now_t = time.time()
    if now_t - _last_hb_persist < _HB_INTERVAL:
        return
    _last_hb_persist = now_t
    try:
        # Always write to the SHARED monitoring catalog (not user-scoped) so the
        # lifecycle job has a single well-known table to check.
        _HB_CATALOG = "gap_demo_dev_app_monitoring"
        tbl = f"{_HB_CATALOG}.genie_workbench.app_heartbeat"
        ts  = datetime.utcnow().isoformat()
        usr = _ust().get("user_alias") or "unknown"
        # Ensure table exists (once per app lifetime)
        if not _hb_table_ready:
            _sql_fire(f"CREATE CATALOG IF NOT EXISTS {_HB_CATALOG}", wait="10s")
            # Shared monitoring catalog: owned by account users so ALL users can access
            _sql_fire(f"ALTER CATALOG {_HB_CATALOG} SET OWNER TO `account users`", wait="10s")
            _sql_fire(f"CREATE SCHEMA IF NOT EXISTS {_HB_CATALOG}.genie_workbench", wait="10s")
            _sql_fire(f"ALTER SCHEMA {_HB_CATALOG}.genie_workbench SET OWNER TO `account users`", wait="10s")
            _sql_fire(f"CREATE TABLE IF NOT EXISTS {tbl} "
                      f"(app STRING, ts TIMESTAMP, user STRING) USING DELTA",
                      wait="10s")
            _sql_fire(f"ALTER TABLE {tbl} SET OWNER TO `account users`", wait="10s")
            _hb_table_ready = True
        # Upsert the heartbeat row (async — don't block the request)
        _sql_fire(
            f"MERGE INTO {tbl} AS t "
            f"USING (SELECT 'genie-curator-workbench' AS app, "
            f"TIMESTAMP '{ts}' AS ts, '{usr}' AS user) AS s "
            f"ON t.app = s.app "
            f"WHEN MATCHED THEN UPDATE SET t.ts = s.ts, t.user = s.user "
            f"WHEN NOT MATCHED THEN INSERT *"
        )
    except Exception:
        pass  # never block a user request for telemetry

@server.before_request
def _track_activity():
    """Record every inbound request; periodically persist to Delta."""
    global _last_user_activity
    _last_user_activity = time.time()
    _persist_heartbeat()

@server.route("/heartbeat")
def _heartbeat_endpoint():
    """In-app diagnostic (behind OAuth)."""
    idle = time.time() - _last_user_activity
    return _jsonify({"last_activity_utc": datetime.utcfromtimestamp(_last_user_activity).isoformat() + "Z",
                     "idle_seconds": int(idle), "user": _ust().get("user_alias") or "?"})

# ---- CSS (matches Gap_workshp_.html tokens) --------------------------------
_CSS = f"""
/* Light (default) */
:root{{--bg:#F4F6FB;--surface:#FFFFFF;--soft:#EAEEF7;--ink:#15203A;--muted:#556079;--line:#D6DCEA;--glass:rgba(244,246,251,.88);--rose:#C2273F;--rose-bg:#FCE7EB;--teal:#087A70;--teal-bg:#D9F3EE;--indigo:#4338CA;--indigo-bg:#E6E7FB;--display:"Bricolage Grotesque",system-ui,sans-serif;--body:"Instrument Sans",system-ui,sans-serif;--mono:"JetBrains Mono",monospace}}
/* Dark override */
:root[data-theme="dark"]{{--bg:#0D1324;--surface:#151D33;--soft:#1D2743;--ink:#E8ECF7;--muted:#9AA5C0;--line:#2A3555;--glass:rgba(13,19,36,.88);--rose:#FF7C93;--rose-bg:#3A1A27;--teal:#3DD9C4;--teal-bg:#10322F;--indigo:#9AA1FF;--indigo-bg:#222A5A}}
@media(prefers-color-scheme:dark){{:root:not([data-theme="light"]){{--bg:#0D1324;--surface:#151D33;--soft:#1D2743;--ink:#E8ECF7;--muted:#9AA5C0;--line:#2A3555;--glass:rgba(13,19,36,.88);--rose:#FF7C93;--rose-bg:#3A1A27;--teal:#3DD9C4;--teal-bg:#10322F;--indigo:#9AA1FF;--indigo-bg:#222A5A}}}}
html,body{{background:var(--bg)!important;color:var(--ink)!important;font-family:var(--body)!important;margin:0;-webkit-font-smoothing:antialiased}}
h1{{font-family:var(--display)!important;font-weight:800!important;letter-spacing:-.04em!important;font-size:clamp(28px,4.2vw,46px)!important;line-height:1.06!important}}
h2{{font-family:var(--display)!important;font-weight:700!important;letter-spacing:-.025em!important}}
h3{{font-family:var(--display)!important;font-weight:600!important}}
code{{font-family:var(--mono);font-size:.86em}}
.wrap{{max-width:1120px;margin:0 auto;padding-inline:clamp(18px,4vw,40px)}}

/* --- header (glass bar, sticky) --- */
.top-bar{{position:sticky;top:0;z-index:30;background:var(--glass);backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);border-bottom:1px solid var(--line)}}
.top-inner{{display:flex;align-items:center;gap:16px;height:56px;max-width:1120px;margin:0 auto;padding:0 clamp(18px,4vw,40px)}}
.brand{{font-family:var(--display);font-weight:700;font-size:16px;margin-right:auto;display:flex;align-items:center;gap:10px}}

/* --- theme toggle (from HTML .theme) --- */
.theme-btn{{width:38px;height:38px;border-radius:10px;border:1px solid var(--line);background:var(--surface);display:grid;place-items:center;font-size:18px;cursor:pointer;transition:background .2s,border-color .2s;color:var(--muted);flex-shrink:0}}
.theme-btn:hover{{background:var(--soft);color:var(--ink)}}

/* --- nav tabs --- */
.nav-row{{display:flex;gap:4px;padding:10px 0;flex-wrap:wrap}}
.nav-btn{{padding:9px 16px;border-radius:10px;font-weight:600;font-size:13px;cursor:pointer;border:1px solid var(--line);background:var(--surface);color:var(--muted);transition:transform .15s ease,box-shadow .15s ease,background .2s,color .2s;font-family:var(--body);box-shadow:0 1px 0 rgba(15,23,42,.08),0 2px 5px rgba(15,23,42,.07),inset 0 1px 0 rgba(255,255,255,.55)}}
.nav-btn:hover{{color:var(--ink);transform:translateY(-1px);box-shadow:0 2px 0 rgba(15,23,42,.08),0 5px 12px rgba(15,23,42,.10),inset 0 1px 0 rgba(255,255,255,.55)}}
.nav-btn:active{{transform:translateY(1px);box-shadow:0 0 0 rgba(15,23,42,0),0 1px 2px rgba(15,23,42,.10),inset 0 1px 2px rgba(15,23,42,.08)}}
.nav-btn:focus,.nav-btn:focus-visible{{outline:none}}
.nav-btn.active{{background:var(--indigo-bg);color:var(--indigo);border-color:var(--indigo);transform:translateY(-1px);box-shadow:0 2px 0 rgba(67,56,202,.45),0 6px 14px rgba(99,102,241,.22),inset 0 1px 0 rgba(255,255,255,.5)}}
.nav-btn:disabled{{opacity:0.35;cursor:not-allowed;pointer-events:none;transform:none;box-shadow:none}}
.nav-btn:disabled:hover{{background:var(--surface);color:var(--muted)}}

/* --- cards & stage (from HTML .stage) --- */
.stage{{background:var(--surface);border:1px solid var(--line);border-radius:22px;padding:clamp(16px,2.6vw,28px);box-shadow:0 18px 40px -28px rgba(21,32,58,.35);margin-bottom:24px}}
.card{{background:var(--surface);border:1px solid var(--line);border-radius:16px;padding:24px;margin-bottom:20px}}

/* --- metric tiles (from HTML .tile) --- */
.tile{{border:1px solid var(--line);border-radius:14px;padding:10px 14px;display:flex;flex-direction:column;gap:2px;min-width:100px}}
.tile b{{font-family:var(--display);font-size:clamp(16px,1.8vw,22px);letter-spacing:-.03em;line-height:1.1;font-variant-numeric:tabular-nums;color:var(--ink)}}
.tile code{{color:var(--muted);font-size:12px;white-space:normal;overflow-wrap:anywhere;max-width:320px}}
.tile.teal{{border-color:var(--teal);background:var(--teal-bg)}}
.tile.teal b{{color:var(--teal)}}
.tile.rose{{border-color:var(--rose);background:var(--rose-bg)}}
.tile.rose b{{color:var(--rose)}}
.tile.indigo{{border-color:var(--indigo);background:var(--indigo-bg)}}
.tile.indigo b{{color:var(--indigo)}}

/* --- chips (from HTML .mchip) --- */
.mchip{{display:inline-flex;align-items:center;gap:6px;font-family:var(--mono);font-size:13px;padding:6px 10px;border-radius:8px;border:1px solid var(--line);background:var(--surface);color:var(--ink)}}
.mchip.t{{border-color:var(--teal);background:var(--teal-bg);color:var(--teal)}}
.mchip.g{{border-color:var(--rose);background:var(--rose-bg);color:var(--rose)}}
.mchip.i{{border-color:var(--indigo);background:var(--indigo-bg);color:var(--indigo)}}

/* --- buttons (from HTML .replay) --- */
.btn{{border:1px solid var(--line);background:none;border-radius:999px;padding:10px 20px;font-size:14px;font-weight:600;color:var(--muted);cursor:pointer;font-family:var(--body);transition:all .2s}}
.btn:hover{{color:var(--ink);background:var(--soft)}}
.btn.primary{{background:var(--indigo);color:#fff;border-color:var(--indigo)}}
.btn.primary:hover{{opacity:.85}}
.btn.rose{{background:var(--rose);color:#fff;border-color:var(--rose)}}
.btn.rose:hover{{opacity:.85}}
.btn.danger{{border-color:var(--rose);color:var(--rose)}}
.btn.danger:hover{{background:var(--rose-bg)}}
.btn:disabled,.btn[disabled]{{cursor:not-allowed!important;opacity:0.4!important;pointer-events:none}}
.btn:disabled:hover{{color:var(--muted);background:none;opacity:0.4!important}}

/* --- output screen (from HTML .screen) --- */
.screen{{background:var(--surface);border:1px solid var(--line);border-radius:22px;overflow:hidden;box-shadow:0 18px 40px -28px rgba(21,32,58,.35);display:flex;flex-direction:column}}
.scr-bar{{display:flex;align-items:center;gap:10px;padding:12px 18px;border-bottom:1px solid var(--line);font-size:13.5px;font-weight:600;color:var(--muted);flex-shrink:0}}
.scr-dot{{width:9px;height:9px;border-radius:50%;background:var(--teal);transition:all .3s}}
.scr-dot.active{{animation:dotPulse 1.2s ease-in-out infinite}}
@keyframes dotPulse{{0%,100%{{box-shadow:0 0 0 0 rgba(61,217,196,.6);transform:scale(1)}}50%{{box-shadow:0 0 0 8px rgba(61,217,196,0);transform:scale(1.3)}}}}
.scr-body{{padding:18px;font-family:var(--mono);font-size:13px;color:var(--muted);flex:1;overflow:auto;white-space:pre;line-height:1.6;min-height:0}}
.scr-body::-webkit-scrollbar{{width:8px;height:8px}}
.scr-body::-webkit-scrollbar-track{{background:var(--surface)}}
.scr-body::-webkit-scrollbar-thumb{{background:var(--line);border-radius:4px}}
.scr-body::-webkit-scrollbar-thumb:hover{{background:var(--muted)}}

/* --- table --- */
.tbl{{width:100%;border-collapse:collapse;font-size:14px}}
.tbl th{{text-align:left;padding:10px 14px;background:var(--soft);color:var(--muted);font-weight:600;border-bottom:1px solid var(--line);font-size:12px;text-transform:uppercase;letter-spacing:.05em}}
.tbl td{{padding:10px 14px;border-bottom:1px solid var(--line)}}
.tbl tr:hover{{background:var(--soft)}}

/* --- steps (from HTML .step) --- */
.step-row{{display:flex;gap:16px;align-items:flex-start;padding:12px 0;border-bottom:1px solid var(--line)}}
.step-num{{width:32px;height:32px;border-radius:50%;border:2px solid var(--line);display:grid;place-items:center;font-family:var(--display);font-weight:700;font-size:14px;color:var(--muted);flex-shrink:0}}
.step-num.done{{border-color:var(--teal);color:var(--teal);background:var(--teal-bg)}}
.step-row.running{{background:rgba(157,161,255,.08);border-radius:12px;padding:12px 16px;border-color:var(--indigo)}}
.step-row.running .step-num{{border-color:var(--indigo);color:#fff;background:var(--indigo);animation:stepPulse 3s ease-in-out infinite}}
.step-row.failed .step-num{{border-color:var(--rose);color:#fff;background:var(--rose)}}
.step-row.done .step-num{{border-color:var(--teal);color:#fff;background:var(--teal)}}
.step-row.skipped{{opacity:.5}}
.step-row.skipped .step-num{{border-style:dashed}}
@keyframes stepPulse{{0%,100%{{box-shadow:0 0 0 0 rgba(154,161,255,.5)}}50%{{box-shadow:0 0 0 12px rgba(154,161,255,0)}}}}

/* --- teardown state cards (comic / typesafe-inspired) --- */
.state-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:14px;margin:18px 0}}
.state-card{{position:relative;border-radius:16px;padding:16px 18px;border:2px solid var(--line);background:var(--surface);overflow:hidden;transition:all .4s ease}}
.state-card::before{{content:'';position:absolute;top:0;left:0;right:0;height:4px;background:var(--line);transition:background .4s}}
.state-card.alive::before{{background:var(--teal)}}
.state-card.alive{{border-color:var(--teal);animation:cardPop .5s ease-out}}
.state-card.gone{{opacity:.4;border-style:dashed;transform:scale(.97)}}
.state-card .sc-label{{font-size:11px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);font-weight:700;margin-bottom:4px}}
.state-card .sc-value{{font-family:var(--display);font-size:22px;font-weight:800;letter-spacing:-.02em;overflow-wrap:break-word;word-break:break-word;line-height:1.15}}
.state-card.alive .sc-value{{color:var(--teal)}}
.state-card.gone .sc-value{{color:var(--muted);text-decoration:line-through}}
.state-card .sc-detail{{font-size:12px;color:var(--muted);margin-top:4px}}
@keyframes cardPop{{0%{{transform:scale(.9);opacity:0}}100%{{transform:scale(1);opacity:1}}}}
.state-card.starting{{border-color:var(--indigo);animation:whPulse 2s ease-in-out infinite}}
.state-card.starting::before{{background:var(--indigo)}}
.state-card.starting .sc-value{{color:var(--indigo)}}
.state-card.starting .sc-detail{{color:var(--indigo)}}
@keyframes whPulse{{0%,100%{{box-shadow:0 0 0 0 rgba(154,161,255,.4)}}50%{{box-shadow:0 0 0 10px rgba(154,161,255,0)}}}}
.state-card.waiting{{opacity:.5;border-style:dashed}}
.state-card.waiting .sc-value{{color:var(--muted)}}
.state-card.waiting .sc-detail{{font-style:italic}}

/* --- Dropdown dark-mode overrides --- */
.Select-control,.Select-control:hover{{background:var(--soft) !important;border-color:var(--line) !important;color:var(--ink) !important}}
.Select-menu-outer{{background:var(--surface) !important;border-color:var(--line) !important}}
.Select-option{{background:var(--surface) !important;color:var(--ink) !important}}
.Select-option.is-focused,.Select-option:hover{{background:var(--soft) !important}}
.Select-option.is-selected{{background:var(--indigo-bg) !important;color:var(--indigo) !important}}
.Select-value-label,.Select-value,.Select--single>.Select-control .Select-value .Select-value-label{{color:var(--ink) !important}}
.Select-placeholder{{color:var(--muted) !important}}
.Select-input>input{{color:var(--ink) !important}}
.Select-arrow-zone,.Select-clear-zone{{color:var(--muted) !important}}
.Select.is-open>.Select-control{{background:var(--soft) !important;border-color:var(--indigo) !important}}
.VirtualizedSelectOption{{background:var(--surface) !important;color:var(--ink) !important}}
.VirtualizedSelectFocusedOption{{background:var(--soft) !important}}

/* --- grid helpers --- */
.tiles-row{{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px;margin-bottom:24px}}
.btn-row{{display:flex;gap:10px;flex-wrap:wrap;margin-top:16px;margin-bottom:20px}}
.two-col{{display:grid;grid-template-columns:1fr 1fr;gap:24px}}
@media(max-width:800px){{.two-col{{grid-template-columns:1fr}}}}

/* --- test grid (per-test chips like the HTML) --- */
.test-grid{{display:grid;gap:6px}}
.test-row{{display:grid;grid-template-columns:36px 180px 32px 1fr;gap:12px;align-items:center;padding:8px 0;border-bottom:1px solid var(--line)}}
.test-row:last-child{{border-bottom:none}}
.grp-badge{{width:36px;height:36px;border-radius:50%;display:grid;place-items:center;font-family:var(--display);font-weight:700;font-size:15px;border:2px solid var(--line)}}
.grp-name{{font-weight:600;font-size:15px}}
.grp-count{{font-family:var(--mono);font-size:14px;color:var(--muted);text-align:center}}
.test-chips{{display:flex;flex-wrap:wrap;gap:8px}}
.tchip{{display:inline-flex;align-items:center;justify-content:center;font-family:var(--mono);font-size:13px;font-weight:500;padding:6px 12px;border-radius:8px;border:1px solid var(--line);background:var(--surface);color:var(--ink);min-width:52px;transition:all .3s}}
.tchip.pass{{border-color:var(--teal);background:var(--teal-bg);color:var(--teal)}}
.tchip.fail{{border-color:var(--rose);background:var(--rose-bg);color:var(--rose)}}
.tchip.tol{{border-color:var(--indigo);background:var(--indigo-bg);color:var(--indigo);border-style:dashed}}
.tchip.wait{{border-color:var(--line);background:var(--surface);color:var(--muted);opacity:.5}}
.tchip.running{{border-color:var(--indigo);background:var(--surface);color:var(--indigo);animation:chipPulse 1.5s ease-in-out infinite}}
.tchip:hover{{cursor:pointer;filter:brightness(1.15);transform:scale(1.06);transition:all .15s}}
.modal-backdrop{{position:fixed;top:0;left:0;width:100vw;height:100vh;background:rgba(0,0,0,0.55);z-index:9999;display:flex;align-items:center;justify-content:center}}
.modal-card{{background:var(--surface);border:1px solid var(--line);border-radius:18px;padding:28px;max-width:620px;width:92%;max-height:80vh;overflow-y:auto;box-shadow:0 20px 60px -20px rgba(0,0,0,.5)}}
.modal-close{{position:absolute;top:12px;right:16px;background:none;border:none;font-size:22px;color:var(--muted);cursor:pointer}}
.modal-close:hover{{color:var(--ink)}}
.modal-field{{margin-bottom:10px}}
.modal-label{{font-family:var(--mono);font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px}}
.modal-val{{font-size:14px;color:var(--ink);margin-top:2px;line-height:1.5}}
.modal-answer{{font-family:var(--mono);font-size:12px;color:var(--muted);background:var(--soft);border-radius:10px;padding:12px;max-height:200px;overflow-y:auto;white-space:pre-wrap;word-break:break-word;margin-top:2px}}
.progress-track{{width:100%;height:8px;background:var(--soft);border-radius:4px;overflow:hidden;position:relative}}
.progress-fill{{height:100%;border-radius:4px;transition:width .6s ease;background:linear-gradient(90deg,var(--teal),var(--indigo))}}
.progress-fill.done{{background:linear-gradient(90deg,var(--teal),var(--teal))}}
.progress-shimmer{{position:absolute;top:0;left:0;right:0;bottom:0;background:linear-gradient(90deg,transparent 0%,rgba(255,255,255,.12) 50%,transparent 100%);animation:shimmer 1.8s infinite}}
@keyframes shimmer{{0%{{transform:translateX(-100%)}}100%{{transform:translateX(100%)}}}}
@keyframes chipPulse{{0%,100%{{opacity:.5}}50%{{opacity:1}}}}
.legend{{display:flex;flex-wrap:wrap;gap:18px;padding:14px 0;margin-top:8px;border-top:1px solid var(--line);font-size:13px;color:var(--muted)}}
.legend-item{{display:flex;align-items:center;gap:8px}}
.legend-dot{{width:14px;height:14px;border-radius:4px;border:1.5px solid}}
.legend-dot.lpass{{border-color:var(--teal);background:var(--teal-bg)}}
.legend-dot.lfail{{border-color:var(--rose);background:var(--rose-bg)}}
.legend-dot.ltol{{border-color:var(--indigo);background:var(--indigo-bg);border-style:dashed}}

/* --- chat interface --- */
.chat-wrap{{display:grid;grid-template-rows:1fr auto;height:calc(100vh - 240px);min-height:400px}}
.chat-msgs{{overflow-y:auto;padding:18px;display:flex;flex-direction:column;gap:14px}}
.chat-bubble{{max-width:85%;padding:14px 18px;border-radius:18px;font-size:15px;line-height:1.55;word-break:break-word}}
.chat-bubble.user{{align-self:flex-end;background:var(--indigo-bg);color:var(--ink);border-bottom-right-radius:4px}}
.chat-bubble.agent{{align-self:flex-start;background:var(--soft);color:var(--ink);border-bottom-left-radius:4px}}
.chat-bubble pre{{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:12px;margin:8px 0;font-family:var(--mono);font-size:12.5px;overflow-x:auto;white-space:pre-wrap}}
.chat-bubble table{{width:100%;border-collapse:collapse;margin:8px 0;font-size:13px}}
.stage.rob-chart{{margin-bottom:0}}
.rob-chart .js-plotly-plot .xtick text,.rob-chart .js-plotly-plot .ytick text,.rob-chart .js-plotly-plot .legendtext,.rob-chart .js-plotly-plot .ytitle,.rob-chart .js-plotly-plot .xtitle{{fill:var(--muted)!important}}
.rob-chart .js-plotly-plot .annotation-text{{fill:var(--ink)!important}}
.rob-chart .js-plotly-plot .scatterlayer .textpoint text{{fill:var(--ink)!important}}
.rob-tbl td{{vertical-align:top;white-space:normal;word-break:normal;overflow-wrap:break-word}}
.rob-tbl th{{white-space:nowrap}}
.cmp-md{{font-size:13.5px;line-height:1.55;color:var(--ink);overflow-wrap:anywhere}}
.cmp-md h1,.cmp-md h2,.cmp-md h3,.cmp-md h4{{font-family:var(--display);color:var(--indigo);margin:14px 0 6px;line-height:1.25}}
.cmp-md h1{{font-size:19px}} .cmp-md h2{{font-size:17px;border-bottom:1px solid var(--line);padding-bottom:4px}}
.cmp-md h3{{font-size:15px}} .cmp-md h4{{font-size:14px}}
.cmp-md p{{margin:6px 0}} .cmp-md ul,.cmp-md ol{{margin:6px 0;padding-left:22px}} .cmp-md li{{margin:3px 0}}
.cmp-md strong{{color:var(--ink);font-weight:700;background:var(--indigo-bg);padding:0 3px;border-radius:4px}}
.cmp-md em{{color:var(--muted)}}
.cmp-md hr{{border:none;border-top:1px solid var(--line);margin:12px 0}}
.cmp-md blockquote{{margin:8px 0;padding:6px 12px;border-left:3px solid var(--teal);background:var(--teal-bg);border-radius:6px}}
.cmp-md a{{color:var(--indigo);text-decoration:none;font-weight:600}}
.cmp-md code{{font-family:var(--mono);font-size:12px;background:var(--soft);padding:1px 4px;border-radius:4px}}
.cmp-sql{{font-family:var(--mono);font-size:11.5px;line-height:1.45;white-space:pre-wrap;word-break:break-word;background:var(--surface);border:1px solid var(--line);border-left:3px solid var(--indigo);border-radius:8px;padding:8px 10px;margin:4px 0 0;color:var(--ink);max-height:260px;overflow:auto}}
.cmp-md pre{{white-space:pre-wrap;word-break:break-word;background:var(--soft);padding:8px 10px;border-radius:8px;font-size:11.5px}}
.cmp-md table{{display:block;max-width:100%;overflow-x:auto;overflow-y:auto;max-height:400px;border-collapse:collapse;margin:8px 0;font-size:12.5px}}
.cmp-md th{{background:var(--indigo-bg);color:var(--indigo);font-weight:700;text-align:left;padding:6px 10px;border:1px solid var(--line);white-space:nowrap;position:sticky;top:0;z-index:1}}
.cmp-md td{{padding:5px 10px;border:1px solid var(--line);font-variant-numeric:tabular-nums}}
.cmp-md tr:nth-child(even) td{{background:var(--soft)}}
.cmp-md mark{{background:rgba(20,184,166,0.18);color:var(--ink);padding:1px 3px;border-radius:4px;text-decoration:underline;text-decoration-color:var(--teal);text-underline-offset:2px;font-weight:600;cursor:help}}
.cmp-md-sm{{font-size:12px}} .cmp-md-sm p{{margin:2px 0;color:var(--muted)}} .cmp-md-sm table{{font-size:11.5px;margin:4px 0}}
.chat-bubble th{{text-align:left;padding:6px 10px;background:var(--soft);border-bottom:1px solid var(--line);font-size:11px;text-transform:uppercase}}
.chat-bubble td{{padding:6px 10px;border-bottom:1px solid var(--line)}}
.chat-input-row{{display:flex;gap:10px;padding:14px 18px;border-top:1px solid var(--line);background:var(--surface)}}
.chat-input{{flex:1;background:var(--soft);border:1px solid var(--line);border-radius:12px;padding:12px 16px;color:var(--ink);font-family:var(--body);font-size:15px;outline:none;resize:none}}
.chat-input:focus{{border-color:var(--indigo)}}
.chat-status{{text-align:center;padding:8px;color:var(--muted);font-size:13px;font-family:var(--mono)}}
"""

app.index_string = (
    '<!DOCTYPE html>\n<html data-theme="light">\n<head>\n'
    '    {%metas%}\n    <title>{%title%}</title>\n'
    '    {%favicon%}\n    {%css%}\n'
    '    <style>' + _CSS + '</style>\n'
    '</head>\n<body>\n    {%app_entry%}\n'
    '    <footer>{%config%}{%scripts%}{%renderer%}</footer>\n'
    '</body>\n</html>'
)

# ==== Reusable Components ===================================================
def Tile(value, label, color=None):
    cls = "tile" + (f" {color}" if color else "")
    return html.Div([html.B(str(value)), html.Code(label)], className=cls)

def Chip(text, kind=None):
    cls = "mchip" + (f" {kind}" if kind else "")
    return html.Span(text, className=cls)

def Screen(title, body_id, dot=True, height="380px", initial_content=None, clear_id=None):
    """An output screen container matching the HTML .screen pattern."""
    bar_children = []
    if dot:
        bar_children.append(html.Span(id=f"{body_id}-dot", className="scr-dot"))
    bar_children.append(html.Span(title, style={"flex":"1"}))
    # Optional Clear button (only for screens that persist logs)
    _bar_btn = {"fontSize":"11px","padding":"4px 12px",
                "background":C["soft"],"border":f"1px solid {C['line']}","color":C["ink"],
                "borderRadius":"8px","fontWeight":"600","cursor":"pointer","letterSpacing":".02em"}
    if clear_id:
        bar_children.append(html.Button("Clear", id=clear_id, className="btn",
            n_clicks=0, style={**_bar_btn, "marginLeft":"auto","color":C["muted"],"marginRight":"6px"}))
    # Copy button (clientside clipboard)
    bar_children.append(html.Button("Copy", id=f"{body_id}-copy", className="btn",
        style={**_bar_btn, **({} if clear_id else {"marginLeft":"auto"})}))
    body_text = initial_content or "Ready. Click a button above to run."
    return html.Div([
        html.Div(bar_children, className="scr-bar", style={"display":"flex","alignItems":"center"}),
        html.Div(body_text, id=body_id, className="scr-body"),
    ], className="screen", style={"marginTop":"0","height":height})

def TestGrid(results, source="bench_results"):
    """Visual grid of per-test chips grouped by category, matching HTML design.
    `source` = ST key holding these results; encoded in each chip id so the modal shows THIS grid's result."""
    # Group results by prefix letter
    grouped = {}
    for r in results:
        g = r["id"][0]
        grouped.setdefault(g, []).append(r)
    rows = []
    for g in ["A","B","C","D","E","F","G","H","P"]:
        if g not in grouped: continue
        items = grouped[g]
        gname, gcolor = GROUPS.get(g, ("",C["muted"]))
        chips = []
        for r in items:
            v = r.get("verdict", "WAITING")
            if v == "EXACT": cls = "tchip pass"
            elif v == "TOLERANT": cls = "tchip tol"
            elif v == "WAITING": cls = "tchip wait"
            elif v == "RUNNING": cls = "tchip running"
            else: cls = "tchip fail"
            tooltip = r.get("question", r.get("desc", ""))
            chips.append(html.Button(r["id"], id={"type":"tpill","index":f"{source}|{r['id']}"},
                         n_clicks=0, className=cls, title=tooltip))
        rows.append(html.Div([
            html.Div(g, className="grp-badge", style={"borderColor": gcolor, "color": gcolor}),
            html.Div(gname, className="grp-name"),
            html.Div(str(len(items)), className="grp-count"),
            html.Div(chips, className="test-chips"),
        ], className="test-row"))
    legend = html.Div([
        html.Div([html.Div(className="legend-dot lpass"), "Passes (exact)"], className="legend-item"),
        html.Div([html.Div(className="legend-dot ltol"), "Passes (tolerant)"], className="legend-item"),
        html.Div([html.Div(className="legend-dot lfail"), "Missing / fail"], className="legend-item"),
        html.Div([html.Div(style={"width":"14px","height":"14px","borderRadius":"4px",
                                   "border":"1.5px solid var(--line)","background":"var(--surface)","opacity":".5"}),
                  "Waiting"], className="legend-item"),
    ], className="legend")
    return html.Div([html.Div(rows, className="test-grid"), legend], className="stage")

def StepRow(num, title, desc, status=""):
    """status: '' | 'running' | 'done' | 'failed' | 'skipped'"""
    num_cls = "step-num" + (" done" if status == "done" else "")
    row_cls = "step-row" + (f" {status}" if status else "")
    # Status suffix (rendered as styled html.Span below)
    icon = ""
    if status == "done":     icon = " ✓"
    elif status == "failed": icon = " ✗"
    elif status == "running": icon = " ..."
    return html.Div([
        html.Div(str(num) + icon, className=num_cls),
        html.Div([
            html.Div(title, style={"fontWeight":"600","fontSize":"15px"}),
            html.Div([desc] + ([html.Span(" — DONE", style={"color":C["teal"],"fontWeight":"700"})] if status == "done" else [html.Span(" — FAILED", style={"color":C["rose"],"fontWeight":"700"})] if status == "failed" else [html.Span(" (skipped)", style={"fontStyle":"italic"})] if status == "skipped" else []), style={"color":C["muted"],"fontSize":"13px"}),
        ]),
    ], className=row_cls)

# ==== Layout ================================================================
def make_header():
    # Moon icon (dark) / Sun icon (light) — using Unicode symbols
    return html.Div([
        html.Div([
            html.Div([
                html.Span("\u2b22", style={"fontSize":"22px","color":C["indigo"]}),
                html.Span("Genie Curator Workbench"),
            ], className="brand"),
            Chip(f"Catalog: {_ust().get('user_catalog') or CATALOG}"),
            html.Div(id="wh-chip", children=Chip("WH: checking...")),
            html.Button("\u263e", id="theme-toggle", className="theme-btn",
                        title="Toggle light / dark theme", n_clicks=0),
        ], className="top-inner"),
    ], className="top-bar")

NAV_TAB_COUNT = 9  # overview, setup, tests, baseline, iterations, comparison, robustness, agents, logs


# ==== Test Catalog (curator-editable) ========================================
# The tests every benchmark asks. Baseline, all iterations and Supervisor Comparison score against
# ASSUMPTIONS, which is loaded from a JSON file the curator edits on the Test Catalog page.
# Storage: workspace file TEST_CATALOG_PATH (read/written with the user's OBO token, survives redeploys);
# fallback: the test_catalog.json shipped next to this file; last resort: the built-in list above.
DEFAULT_ASSUMPTIONS = list(ASSUMPTIONS)
TEST_CATALOG_PATH = os.getenv("TEST_CATALOG_PATH",
    "/Users/ssuman@bpcs.com/GAP_genie_multiagent_datanalysis_demo/src/genie-curator-workbench/test_catalog.json")
_TC_LOCAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_catalog.json")
TC_AGENTS = ["demand", "inventory", "logistics", "supplier", "executive"]
TC_STAGES = ["Iter1", "Iter2", "Iter3"]
TC_ORIGINS = ["From the CFO prompt", "Curator-added edge case"]
_TC_ID_RX = re.compile(r"^[A-Z]\d{2,3}$")


def _NT():
    """Number of active tests (was a hard-coded 45)."""
    return len(ASSUMPTIONS) or 1


def _pass_bar():
    """'Good' threshold, scaled from the original 40 of 45."""
    return max(1, round(_NT() * 40 / 45))


def _tc_default_tests():
    return [dict(id=a, group=a[0], agent=ag, question=q, expected=exp, metric=desc, fix_stage=fix, fixed_by="",
                 answer_source="", origin="", prompt_fragment="", explanation="", sql="", enabled=True)
            for a, ag, q, exp, desc, fix in DEFAULT_ASSUMPTIONS]


def _tc_sort_key(t):
    """Original tests keep their original order; new tests go right after the last test of their group."""
    order = {a[0]: i for i, a in enumerate(DEFAULT_ASSUMPTIONS)}
    tid = str(t.get("id", "?")) or "?"
    if tid in order:
        return (order[tid], 0, tid)
    same = [i for k, i in order.items() if k[0] == tid[0]]
    groups = list(GROUPS.keys())
    base = max(same) if same else len(order) + (groups.index(tid[0]) if tid[0] in groups else len(groups))
    return (base, 1, tid)


def _tc_validate(tests):
    """Return (clean_tests, errors). Never raises."""
    errs, clean, seen = [], [], set()
    for i, t in enumerate(tests or [], 1):
        tid = str(t.get("id", "")).strip().upper()
        where = tid or f"row {i}"
        if not _TC_ID_RX.match(tid):
            errs.append(f"{where}: ID must look like A01 (group letter + number)"); continue
        if tid[0] not in GROUPS:
            errs.append(f"{tid}: unknown group '{tid[0]}'"); continue
        if tid in seen:
            errs.append(f"{tid}: duplicate ID"); continue
        seen.add(tid)
        q = str(t.get("question", "")).strip()
        if not q:
            errs.append(f"{tid}: question is empty")
        try:
            exp = float(str(t.get("expected", "")).replace(",", "").replace("$", "").replace("%", "").strip())
        except ValueError:
            errs.append(f"{tid}: expected answer must be a number"); continue
        ag = str(t.get("agent", "")).strip()
        if ag not in TC_AGENTS:
            errs.append(f"{tid}: agent must be one of {', '.join(TC_AGENTS)}")
        fs = str(t.get("fix_stage", "")).strip() or "Iter1"
        if fs not in TC_STAGES:
            errs.append(f"{tid}: fixed-in stage must be Iter1, Iter2 or Iter3")
        en = t.get("enabled", True)
        en = (str(en).strip().lower() not in ("no", "false", "0", "")) if not isinstance(en, bool) else en
        clean.append(dict(id=tid, group=tid[0], agent=ag, question=q,
                          expected=int(exp) if exp.is_integer() and "." not in str(t.get("expected", "")) else exp,
                          metric=str(t.get("metric", "")).strip(), fix_stage=fs,
                          fixed_by=str(t.get("fixed_by", "")).strip(), answer_source=str(t.get("answer_source", "")).strip(),
                          origin=str(t.get("origin", "")).strip(), prompt_fragment=str(t.get("prompt_fragment", "")).strip(),
                          explanation=str(t.get("explanation", "")).strip(), sql=str(t.get("sql", "")).rstrip(),
                          enabled=en))
    if clean and not any(t["enabled"] for t in clean):
        errs.append("at least one test must be enabled")
    if not clean and not errs:
        errs.append("the catalog is empty")
    return sorted(clean, key=_tc_sort_key), errs


def _tc_apply(tests, source, meta=None):
    """Make these tests the live test set (in place, so every existing reader of ASSUMPTIONS sees it)."""
    _ust()["test_catalog"] = tests
    _ust()["tc_source"] = source
    _ust()["tc_meta"] = meta or {}
    ASSUMPTIONS[:] = [(t["id"], t["agent"], t["question"], t["expected"], t["metric"], t["fix_stage"])
                      for t in tests if t.get("enabled", True)]


def _tc_from_json(raw):
    d = json.loads(raw)
    tests, errs = _tc_validate(d.get("tests") if isinstance(d, dict) else d)
    if errs:
        raise ValueError("; ".join(errs[:5]))
    return tests, {k: d.get(k) for k in ("updated_at", "updated_by", "version")} if isinstance(d, dict) else {}


def _tc_load_local():
    try:
        with open(_TC_LOCAL, encoding="utf-8") as f:
            tests, meta = _tc_from_json(f.read())
        _tc_apply(tests, "bundled file", meta)
    except Exception as e:
        _tc_apply(_tc_default_tests(), "built-in defaults")
        print(f"Test catalog: using built-in defaults ({e})")


def _load_test_catalog(headers=None):
    """Load the curator's saved catalog from the workspace file (OBO). Keeps the current set on any failure."""
    h = headers or get_headers()
    try:
        r = requests.get(f"{HOST}/api/2.0/workspace/export", headers=h, timeout=20,
                         params={"path": TEST_CATALOG_PATH, "format": "AUTO", "direct_download": "true"})
        if r.status_code != 200:
            log(f"Test catalog: workspace file not readable (HTTP {r.status_code}) -- using {_ust().get('tc_source')}")
            return False
        tests, meta = _tc_from_json(r.content.decode("utf-8"))
        _tc_apply(tests, "workspace file", meta)
        log(f"Test catalog: {len(ASSUMPTIONS)} active tests ({len(tests)} total) from {TEST_CATALOG_PATH}")
        return True
    except Exception as e:
        log(f"Test catalog load failed: {e} -- using {_ust().get('tc_source')}")
        return False


def _save_test_catalog(rows, headers=None, user=""):
    """Validate, write the JSON file to the workspace (OBO), then make it live. Returns (ok, message)."""
    tests, errs = _tc_validate(rows)
    if errs:
        return False, "Not saved \u2014 " + "; ".join(errs[:6]) + (f" (+{len(errs) - 6} more)" if len(errs) > 6 else "")
    meta = {"version": 1, "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "updated_by": user or "curator"}
    body = json.dumps({**meta, "catalog_placeholder": "{CATALOG}", "tests": tests}, indent=2, ensure_ascii=False)
    h = headers or get_headers()
    try:
        r = requests.post(f"{HOST}/api/2.0/workspace/import", headers=h, timeout=30, json={
            "path": TEST_CATALOG_PATH, "format": "AUTO", "overwrite": True,
            "content": _b64.b64encode(body.encode("utf-8")).decode()})
    except Exception as e:
        return False, f"Not saved \u2014 workspace write failed: {e}"
    if r.status_code != 200:
        return False, f"Not saved \u2014 workspace write HTTP {r.status_code}: {r.text[:200]}"
    _tc_apply(tests, "workspace file", meta)
    n_on = sum(1 for t in tests if t["enabled"])
    log(f"Test catalog saved by {meta['updated_by']}: {n_on} active / {len(tests)} total")
    return True, f"Saved \u2014 {n_on} active tests ({len(tests)} total). The next Baseline, Iteration and Comparison runs use them."


def _tc_gt(tid, default):
    """Ground truth for a test ID from the live catalog (used by the Reliability page)."""
    for a in ASSUMPTIONS:
        if a[0] == tid:
            return a[3]
    return default


_tc_load_local()


def make_nav():
    tabs = [
        ("overview","Overview"), ("setup","Setup & Teardown"), ("tests","Test Catalog"),
        ("baseline","Baseline"), ("iterations","Iterations"),
        ("comparison","Supervisor Comparison"), ("robustness","Reliability"),
        ("agents","Agents"), ("logs","Logs"),
    ]
    return html.Div([
        html.Button(label, id={"type":"nav","index":tid}, n_clicks=0,
                    className="nav-btn active" if tid=="overview" else "nav-btn")
        for tid, label in tabs
    ], className="nav-row")

app.layout = html.Div([
    make_header(),
    html.Div([
        make_nav(),
        html.Div(id="global-wh-banner"),
        html.Div(id="page"),
    ], className="wrap"),
    dcc.Store(id="tab", data="overview", storage_type="session"),  # survives a browser/app reload
    dcc.Store(id="nav-seen", data={}),   # last n_clicks per nav/qchat button -> only real NEW clicks switch tabs
    dcc.Store(id="page-refresh", data=0),  # bumped by interval callbacks; render() is the only page writer
    dcc.Store(id="qchat-target", data=None),
    dcc.Store(id="theme-state", data="dark"),
    dcc.Store(id="deploy-sticker-dismissed", data=False, storage_type="session"),
    html.Div(id="theme-dummy", style={"display":"none"}),
    dcc.Interval(id="global-poll", interval=8000),
    dcc.Interval(id="loading-poll", interval=1500, disabled=False),
    # Test detail modal infrastructure
    dcc.Store(id="selected-test", data=""),
    html.Div(id="test-detail-modal"),
    html.Div(html.Button(id="close-test-modal", n_clicks=0), style={"display":"none"}),
    # Iteration setup-log modal infrastructure
    dcc.Store(id="setup-log-key", data=""),
    html.Div(id="setup-log-modal"),
])

# Clientside: toggle data-theme on <html> (same logic as the HTML reference)
app.clientside_callback(
    """
    function(n) {
        var root = document.documentElement;
        var cur = root.getAttribute('data-theme');
        if (!cur) { cur = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'; }
        var next = (cur === 'dark') ? 'light' : 'dark';
        root.setAttribute('data-theme', next);
        return next;
    }
    """,
    Output("theme-state", "data"),
    Input("theme-toggle", "n_clicks"),
    prevent_initial_call=True,
)

# Clientside: swap moon/sun icon
app.clientside_callback(
    """
    function(theme) {
        var btn = document.getElementById('theme-toggle');
        if (btn) { btn.textContent = (theme === 'dark') ? '\\u263e' : '\\u2600'; }
        return '';
    }
    """,
    Output("theme-dummy", "children"),
    Input("theme-state", "data"),
)


# Copy-to-clipboard for all Screen bodies
for _scr_id in ["setup-out", "td-out", "log-body"]:
    app.clientside_callback(
        """function(n, text) { if(n && text) { navigator.clipboard.writeText(text); } return ''; }""",
        Output(f"{_scr_id}-copy", "title"),
        Input(f"{_scr_id}-copy", "n_clicks"),
        State(_scr_id, "children"),
        prevent_initial_call=True,
    )

# Stage progression "Go to..." button
@callback(Output("tab","data", allow_duplicate=True),
          Input("stage-next-btn","n_clicks"),
          State("stage-next-tab","data"),
          prevent_initial_call=True)
def stage_next_nav(n, target_tab):
    if not n or not target_tab:
        raise PreventUpdate
    return target_tab

def _app_busy_now():
    """True while ANY long operation runs -> all nav tabs are frozen (same rule on every page)."""
    return any([_ust().get("bench_running"), _ust().get("setup_running"), _ust().get("td_running"),
                _ust().get("iter_setup_phase"), _ust().get("iter_bench_active"), _ust().get("cmp_running"),
                _ust().get("rob_running")])


# ==== Global warehouse status chip (updates every 8s) =======================
@callback(Output("wh-chip","children"), Output("global-wh-banner","children"),
          Output({"type":"nav","index":ALL}, "disabled"),
          Output("page-refresh","data"),
          Input("global-poll","n_intervals"),
          State("tab","data"),
          prevent_initial_call=True)
def update_wh_chip(n, current_tab):
    _req_alias = _get_user_alias() if _in_request() else "unknown"
    # On first poll cycle, trigger full discovery if not yet ready
    if not ST.get("app_ready"):
        try:
            _discover_all()
        except Exception as e:
            log(f"Initial discovery error: {e}")
    wh_state = ST.get("warehouse_state", "UNKNOWN")
    wh_name = ST.get("warehouse_name", "")
    # OBO check — fires every 8s, Dash callback has Flask context
    if wh_state != "RUNNING":
        wh_state = check_warehouse()  # OBO only (Rule 5)
    # Fire-and-forget start if stopped — OBO only
    if wh_state in ("STOPPED", "STOPPING") and not ST.get("_wh_starting"):
        try:
            requests.post(f"{HOST}/api/2.0/sql/warehouses/{WAREHOUSE_ID}/start",
                          headers=get_headers(), timeout=10)
            ST["_wh_starting"] = True
            ST["warehouse_state"] = "STARTING"
            wh_state = "STARTING"
            log("Warehouse start triggered via header chip (OBO)")
        except Exception as e:
            log(f"Warehouse chip start error: {e}")
    if wh_state == "RUNNING":
        ST["_wh_starting"] = False
        # One-time catalog check (runs once when warehouse RUNNING + catalog_exists is False)
        # Retry every ~30s until found (a cold warehouse can cancel the first query; never give up after one miss)
        if not ST.get(f"cat_{_req_alias}") and not ST.get("_cat_checking") \
                and time.time() - ST.get(f"_cat_last_try_{_req_alias}", 0) > 30:
            ST["_cat_checking"], ST["_cat_last_try"] = True, time.time()
            try:
                rows = run_sql_query(
                    f"SELECT COUNT(*) as n FROM `{user_catalog()}`.information_schema.tables",
                    headers=get_headers()
                )
                if rows:
                    _tc = int(rows[0].get("n", 0))
                    _uc = user_catalog()
                    ST[f"cat_{_req_alias}"] = {"exists": True, "tables": _tc, "catalog": _uc}
                    _ust()["catalog_exists"] = True  # legacy compat
                    _ust()["table_count"] = _tc
                    _ust()["user_catalog"] = _uc
                    log(f"Catalog verified: {_uc} has {_tc} tables (user={_req_alias})")
                else:
                    log(f"Catalog check: no result for {user_catalog()} yet -- will retry")
            except Exception as e:
                log(f"Catalog check deferred: {e}")
            finally:
                ST["_cat_checking"] = False
                ST[f"_cat_last_try_{_req_alias}"] = time.time()
        # One-time: load persisted benchmark history once warehouse is RUNNING
        _bkey = f"_bench_loaded_{_req_alias}"
        if not ST.get(_bkey) and not ST.get("_bench_loading"):
            ST["_bench_loading"] = True
            try:
                _load_persisted_state(headers=get_headers())
                ST[_bkey] = True
            except Exception as e:
                log(f"Bench history load deferred: {e}")
    # Re-discover supervisors if still empty (retry every 30s until populated).
    # Handles timing issues where initial discovery ran before endpoints were ready.
    if ST.get("app_ready") and not _ust().get("supervisors") \
            and time.time() - ST.get(f"_sup_last_try_{_req_alias}", 0) > 30:
        ST[f"_sup_last_try_{_req_alias}"] = time.time()
        try:
            _fresh_sups = discover_supervisors(headers=get_headers())
            if _fresh_sups:
                _ust()["supervisors"] = _fresh_sups
                log(f"Supervisor re-discovery: {len(_fresh_sups)} found (user={_req_alias})")
        except Exception as _e:
            log(f"Supervisor re-discovery deferred: {_e}")
    # Also re-discover agents if still empty (same retry pattern)
    if ST.get("app_ready") and not _ust().get("agents") \
            and time.time() - ST.get(f"_agt_last_try_{_req_alias}", 0) > 30:
        ST[f"_agt_last_try_{_req_alias}"] = time.time()
        try:
            _fresh_agents = discover_agents(headers=get_headers())
            if _fresh_agents:
                _ust()["agents"] = _fresh_agents
                log(f"Agent re-discovery: {len(_fresh_agents)} found (user={_req_alias})")
        except Exception as _e:
            log(f"Agent re-discovery deferred: {_e}")
    # Background-prefetch endpoint model names for supervisor cards (non-blocking — only
    # populates ST cache; page render reads the cache without making API calls)
    for _s in (_ust().get("supervisors") or []):
        _ep = _s.get("endpoint", "")
        if _ep and f"_ep_model_{_ep}" not in ST:
            try:
                _fetch_endpoint_model(_ep)
            except Exception:
                pass
            break  # one per poll cycle to stay non-blocking
    wh_name = ST.get("warehouse_name", WAREHOUSE_ID[:12])
    if wh_state == "RUNNING":
        bg, fg = C["teal_bg"], C["teal"]
        label = f"\u2713 {wh_name}"
    elif wh_state in ("STARTING", "RESUMING"):
        bg, fg = C["indigo_bg"], C["indigo"]
        label = f"\u21bb {wh_name}"
    else:
        bg, fg = C["rose_bg"], C["rose"]
        label = f"\u2717 {wh_name} ({wh_state})"
    _chip_anim = "chipPulse 1.5s ease-in-out infinite" if wh_state != "RUNNING" else "none"
    chip = html.Span(label, style={
        "fontFamily":"var(--mono)","fontSize":"12px","fontWeight":"600",
        "padding":"6px 14px","borderRadius":"20px",
        "background":bg,"color":fg,"border":f"1px solid {fg}",
        "animation":_chip_anim,
    })
    # Global lock: disable all nav tabs when any operation is running
    _app_busy = any([_ust().get("bench_running"), _ust().get("setup_running"), _ust().get("td_running"),
                     _ust().get("iter_setup_phase"), _ust().get("iter_bench_active"), _ust().get("cmp_running"),
                     _ust().get("rob_running")])
    _nav_disabled = [_app_busy] * NAV_TAB_COUNT
    # Auto-refresh: re-render page when key state changes (every 8s check)
    _fp = (
        str(sorted(_ust().get("benchmarks", {}).items())),
        len(_ust().get("agents", {})),
        len(_ust().get("supervisors", [])),
        ST.get("warehouse_state"),
        bool(ST.get(f"cat_{_req_alias}")),
        str(sorted(_ust().get("iterations_done", set()))),
        _ust().get("bench_running"),
        _ust().get("setup_running"),
        _ust().get("td_running"),
        _ust().get("iter_running"),
        _ust().get("_iter_ui_epoch", 0),   # bumps when any iteration run ends -> page refreshes badges/buttons
    )
    if _fp != _ust().get("_last_page_fp") and ST.get("app_ready") and not _app_busy and current_tab != "tests":
        _ust()["_last_page_fp"] = _fp
        # Never build a page here (State("tab") can be stale if the user just clicked a tab):
        # bump the refresh counter -- render() then draws the browser's CURRENT tab.
        _page_content = int(n or 0)
    else:
        _page_content = no_update
    return chip, _build_warehouse_banner(), _nav_disabled, _page_content



# ==== Test Detail Modal =====================================================
@callback(Output("selected-test", "data"),
          Input({"type": "tpill", "index": ALL}, "n_clicks"),
          prevent_initial_call=True)
def select_test(*_):
    """When a test chip is clicked, store its ID to trigger the modal."""
    click_val = ctx.triggered[0]["value"] if ctx.triggered else None
    if not click_val:
        raise PreventUpdate
    return ctx.triggered_id["index"]

@callback(Output("test-detail-modal", "children"),
          Input("selected-test", "data"))
def render_test_modal(test_id):
    """Build the modal overlay showing full test trace."""
    if not test_id:
        return []
    # Chip ids are "<ST results key>|<test id>" -- look up the result in the grid that was clicked
    src_key, _, tid = test_id.rpartition("|")
    sources = [src_key] if src_key else ["bench_results", "iter_it1_results", "iter_it2_results", "iter_it3_results"]
    test_id = tid
    result = None
    for src in sources:
        for r in ST.get(src, []) or []:
            if r.get("id") == test_id:
                result = r
                break
        if result:
            break
    if not result:
        return []
    v = result.get("verdict", "?")
    vc = C["teal"] if v in ("EXACT","TOLERANT") else (C["indigo"] if v == "RUNNING" else C["rose"])
    vbg = C["teal_bg"] if v in ("EXACT","TOLERANT") else (C["indigo_bg"] if v == "RUNNING" else C["rose_bg"])
    def field(label, value):
        return html.Div([
            html.Div(label, className="modal-label"),
            html.Div(str(value) if value is not None else "—", className="modal-val"),
        ], className="modal-field")
    card = html.Div([
        html.Div([
            html.Div([
                html.Span(test_id, style={"fontFamily":"var(--mono)","fontSize":"20px","fontWeight":"700"}),
                html.Span(f"  {result.get('group_name','')}",
                           style={"color":C["muted"],"fontSize":"14px","marginLeft":"8px"}),
            ]),
            html.Button("\u2715", id="close-test-modal", n_clicks=0,
                         style={"background":"none","border":"none","fontSize":"22px",
                                "color":C["muted"],"cursor":"pointer","padding":"0 4px"}),
        ], style={"display":"flex","justifyContent":"space-between","alignItems":"center","marginBottom":"16px"}),
        html.Span(v, style={"fontFamily":"var(--mono)","fontSize":"13px","fontWeight":"700",
                             "padding":"4px 14px","borderRadius":"20px","border":f"1px solid {vc}",
                             "color":vc,"background":vbg}),
        html.Div(style={"height":"16px"}),
        field("Agent", result.get("agent","?").title()),
        field("Question", result.get("question","?")),
        field("Expected Value", result.get("expected", "?")),
        field("Closest Found", result.get("closest", "—")),
        field("Match Method", result.get("method", "—")),
        field("Target Fix", result.get("fix", "—")),
        html.Div([
            html.Div("AGENT RESPONSE", className="modal-label"),
            html.Div(result.get("answer", "No response captured — run benchmark again to capture traces."),
                      className="modal-answer"),
        ], className="modal-field"),
    ], className="modal-card", style={"position":"relative"})
    return html.Div(card, className="modal-backdrop",
                     id="test-detail-backdrop",
                     style={"position":"fixed","top":"0","left":"0","width":"100vw","height":"100vh",
                            "background":"rgba(0,0,0,0.55)","zIndex":"9999",
                            "display":"flex","alignItems":"center","justifyContent":"center"})

@callback(Output("selected-test", "data", allow_duplicate=True),
          Input("close-test-modal", "n_clicks"),
          prevent_initial_call=True)
def close_test_modal(n):
    """Close the test detail modal."""
    if not n:
        raise PreventUpdate
    return ""
# ==== Iteration Setup Log Modal =============================================
def _has_setup_log(key):
    """True once this iteration has been run in this app session (log captured)."""
    return bool(ST.get(f"iter_{key}_setup_log"))


def _setuplog_btn_style(enabled):
    return {"padding":"10px 20px","opacity":"1" if enabled else "0.4",
            "cursor":"pointer" if enabled else "not-allowed"}


@callback(Output({"type": "setuplog-btn", "index": ALL}, "disabled"),
          Output({"type": "setuplog-btn", "index": ALL}, "style"),
          Input("iter-bench-poll", "n_intervals"),
          prevent_initial_call=True)
def sync_setuplog_buttons(_n):
    """Enable an iteration's View Setup Log button as soon as its run starts logging."""
    keys = [o["id"]["index"] for o in ctx.outputs_list[0]]
    has = [_has_setup_log(k) for k in keys]
    return [not h for h in has], [_setuplog_btn_style(h) for h in has]

def _build_setup_log_modal(key):
    """Modal with the preserved setup log of an iteration -- same rows/header/bar as the live panel."""
    if not key:
        return []
    idx = {"it1": 0, "it2": 1, "it3": 2}.get(key)
    title = ITER_INFO[idx][0] if idx is not None else key
    log_lines = list(ST.get(f"iter_{key}_setup_log") or [])
    meta = ST.get(f"iter_{key}_setup_meta") or {}
    live = _ust().get("iter_running") == key and _ust().get("iter_setup_phase") and not meta.get("t_end")
    if log_lines:
        t0 = meta.get("t0") or 0
        elapsed = int((meta.get("t_end") or time.time()) - t0) if t0 else 0
        elapsed_str = f"{elapsed // 60}m {elapsed % 60:02d}s" if elapsed >= 60 else f"{elapsed}s"
        started = datetime.fromtimestamp(t0).strftime("%Y-%m-%d %H:%M:%S") if t0 else ""
        status = "running" if live else ("completed" if meta.get("completed") else "did not complete")
        pct = _iter_setup_pct(log_lines, key=key, done=bool(meta.get("completed")))
        body = [
            html.Div(_setup_log_rows(log_lines), style={"padding":"8px 0","maxHeight":"60vh","overflowY":"auto"}),
            html.Div([
                html.Div(className="progress-fill", style={"width":f"{pct}%"}),
                html.Div(className="progress-shimmer"),
            ], className="progress-track", style={"marginTop":"12px"}),
        ]
        sub = f"Started {started} \u2022 setup {status}"
    else:
        elapsed_str, sub = "", ""
        body = [html.Div("No setup log yet for this iteration in this app session \u2014 run the iteration to capture it.",
                         style={"color":C["muted"],"fontSize":"13px","padding":"8px 0"})]
    header = html.Div([
        html.Div([
            html.Span(f"{title} \u2014 Setup Log",
                      style={"fontFamily":"var(--display)","fontSize":"18px","fontWeight":"700","color":C["ink"]}),
            html.Span(elapsed_str,
                      style={"fontFamily":"var(--mono)","fontSize":"13px","fontWeight":"600",
                             "color":C["indigo"],"padding":"2px 12px","borderRadius":"12px",
                             "border":f"1px solid {C['indigo']}","marginLeft":"12px"}) if elapsed_str else None,
        ], style={"display":"flex","alignItems":"center"}),
        html.Button("\u2715", id="close-setup-log-modal", n_clicks=0,
                    style={"background":"none","border":"none","fontSize":"22px",
                           "color":C["muted"],"cursor":"pointer","padding":"0 4px"}),
    ], style={"display":"flex","justifyContent":"space-between","alignItems":"center","marginBottom":"4px"})
    card = html.Div([
        header,
        html.Div(sub, style={"color":C["muted"],"fontSize":"12px","fontFamily":"var(--mono)","marginBottom":"8px"}),
        *body,
    ], className="modal-card", style={"position":"relative","maxWidth":"900px"})
    return html.Div(card, className="modal-backdrop", id="setup-log-backdrop",
                    style={"position":"fixed","top":"0","left":"0","width":"100vw","height":"100vh",
                           "background":"rgba(0,0,0,0.55)","zIndex":"9999",
                           "display":"flex","alignItems":"center","justifyContent":"center"})


@callback(Output("setup-log-key", "data"),
          Input({"type": "setuplog-btn", "index": ALL}, "n_clicks"),
          prevent_initial_call=True)
def open_setup_log(*_):
    """Open the setup-log modal for the clicked iteration (ignores re-render phantom triggers)."""
    click_val = ctx.triggered[0]["value"] if ctx.triggered else None
    if not click_val:
        raise PreventUpdate
    return ctx.triggered_id["index"]


@callback(Output("setup-log-modal", "children"),
          Input("setup-log-key", "data"))
def render_setup_log_modal(key):
    return _build_setup_log_modal(key)


@callback(Output("setup-log-key", "data", allow_duplicate=True),
          Input("close-setup-log-modal", "n_clicks"),
          prevent_initial_call=True)
def close_setup_log_modal(n):
    if not n:
        raise PreventUpdate
    return ""


# ==== Nav callback ==========================================================
@callback(Output("tab","data"),
          Output("qchat-target","data"),
          Output("nav-seen","data"),
          Input({"type":"nav","index":ALL},"n_clicks"),
          Input({"type":"qchat","index":ALL},"n_clicks"),
          State("nav-seen","data"),
          prevent_initial_call=True)
def nav_click(_nav, _qchat, seen):
    """Switch tabs ONLY on a real click. Dash also fires this callback when pattern-matching buttons
    appear or disappear (any page repaint), and can then report an old, non-zero n_clicks -- that used to
    switch the page to Overview by itself. A real click is a button whose n_clicks went UP since last time."""
    seen = dict(seen or {})
    clicked = None
    for group in ctx.inputs_list:              # [nav buttons, qchat buttons]
        for item in group:
            key = f'{item["id"]["type"]}:{item["id"]["index"]}'
            val = item.get("value") or 0
            if val > seen.get(key, 0) and clicked is None:
                clicked = item["id"]
            seen[key] = val
    if clicked is None:
        return no_update, no_update, seen     # phantom trigger (repaint) -> stay on the current page
    # GLOBAL LOCK: block tab switching during any running operation
    if _app_busy_now():
        return no_update, no_update, seen
    if clicked["type"] == "qchat":
        return "agents", clicked["index"], seen  # switch to Agents tab + pre-select target
    return clicked["index"], no_update, seen      # regular nav click

@callback(Output("page","children"),
          Output({"type":"nav","index":ALL},"className"),
          Input("tab","data"), Input("page-refresh","data"))
def render(tab, _refresh=None):
    if not tab:
        raise PreventUpdate   # never fall back to Overview just because the tab value is missing
    if ctx.triggered_id == "page-refresh" and tab == "tests":
        raise PreventUpdate   # auto-refresh must not wipe unsaved Test Catalog edits
    pages = dict(overview=page_overview, setup=page_setup, tests=page_tests, baseline=page_baseline,
                 iterations=page_iterations, comparison=page_comparison, robustness=page_robustness,
                 agents=page_agents, logs=page_logs)
    tab_ids = ["overview","setup","tests","baseline","iterations","comparison","robustness","agents","logs"]
    classes = ["nav-btn active" if tid == tab else "nav-btn" for tid in tab_ids]
    _ust()["_active_tab"] = tab or "overview"   # auto re-renders only paint this tab (see update_wh_chip/loading_refresh)
    return pages.get(tab, page_overview)(), classes

# ==== Loading refresh: auto-render page after initial discovery ============
@callback(Output("page-refresh", "data", allow_duplicate=True),
          Output("loading-poll", "disabled"),
          Input("loading-poll", "n_intervals"),
          State("tab", "data"),
          prevent_initial_call=True)
def loading_refresh(n, tab):
    """Re-render the active page once app_ready flips to True, then stop.
    One-shot guard: prevents queued interval callbacks from overwriting
    page.children after the user has already switched tabs."""
    if not ST.get("app_ready"):
        return no_update, False   # keep polling
    if ST.get("_loading_done"):
        return no_update, True    # already rendered — don't overwrite user's tab
    ST["_loading_done"] = True
    return -int(n or 1), True  # bump refresh -> render() draws the current tab; then stop polling

# ==== Page 1: Overview ======================================================
def _discover_all(retries=1, headers=None):
    """Discover agents + supervisors via OBO."""
    agents, sups = {}, []
    for attempt in range(retries + 1):
        try:
            agents = discover_agents(headers=headers)
        except Exception as e:
            log(f"Agent discovery attempt {attempt+1} failed: {e}")
        try:
            sups = discover_supervisors(headers=headers)
        except Exception as e:
            log(f"Supervisor discovery attempt {attempt+1} failed: {e}")
        if agents or sups:
            break
        if attempt < retries:
            time.sleep(2)
    # Only overwrite cache if we found something — never replace good data with empty
    if agents:
        _ust()["agents"] = agents
        # Config-driven: sync agent warehouses to app's WAREHOUSE_ID
        _sync_agent_warehouses(agents, headers=headers)
    if sups:
        _ust()["supervisors"] = sups
    # Return cached startup results as fallback
    agents = _ust().get("agents", agents)
    sups = _ust().get("supervisors", sups)
    log(f"Discovery: {len(agents)} agents, {len(sups)} supervisors (cached={not bool(agents) or agents == _ust().get('agents')})")
    if not _ust().get("_tc_loaded"):
        _ust()["_tc_loaded"] = _load_test_catalog(headers) or _ust().get("_tc_loaded")
    # Load persisted benchmark state if available (warehouse must be up)
    wh_state = ST.get("warehouse_state", "UNKNOWN")
    if wh_state == "RUNNING":
        try:
            _load_persisted_state(headers=headers)
        except Exception as e:
            log(f"Persisted state load deferred: {e}")

    # Auto-start warehouse if not running (fire-and-forget, OBO only — Rule 5)
    wh_state = ST.get("warehouse_state", "UNKNOWN")
    if wh_state != "RUNNING" and not ST.get("_wh_starting"):
        wh_state = check_warehouse(headers=headers)
        if wh_state in ("STOPPED", "STOPPING"):
            try:
                h = headers or get_headers()
                requests.post(f"{HOST}/api/2.0/sql/warehouses/{WAREHOUSE_ID}/start",
                              headers=h, timeout=10)
                ST["_wh_starting"] = True
                ST["warehouse_state"] = "STARTING"
                log("Warehouse start triggered on discovery (OBO)")
            except Exception as e:
                log(f"Warehouse auto-start error: {e}")
    ST["app_ready"] = True
    return agents, sups


def _build_stage_progress():
    """Build stage progression banner with scores and next-action recommendation."""
    benchmarks = _ust().get("benchmarks", {})
    stages = ["Baseline", "Iteration 1", "Iteration 2", "Iteration 3"]
    completed = [s for s in stages if s in benchmarks]
    current_idx = len(completed)
    current_stage = stages[current_idx] if current_idx < len(stages) else "Complete"
    # Stage dots
    dots = []
    for i, s in enumerate(stages):
        score = benchmarks.get(s)
        if score is not None:
            color, bg = C["teal"], C["teal_bg"]
            label = f"{s}: {score}/{_NT()}"
        elif i == current_idx:
            color, bg = C["indigo"], C["indigo_bg"]
            label = f"{s}: pending"
        else:
            color, bg = C["muted"], C["soft"]
            label = f"{s}: locked"
        dots.append(html.Div([
            html.Div(str(score) if score is not None else ("..." if i == current_idx else "—"),
                     style={"fontWeight":"700","fontSize":"20px","color":color}),
            html.Div(s, style={"fontSize":"12px","color":C["muted"],"marginTop":"2px"}),
        ], style={"textAlign":"center","padding":"12px 18px","borderRadius":"12px",
                  "background":bg,"border":f"1px solid {color}","minWidth":"120px","flex":"1"}))
    # Next-action banner
    if not completed:
        next_text = "Run Setup first, then run the Baseline benchmark."
        next_tab = "setup"
        btn_label = "Go to Setup"
    elif current_stage == "Complete":
        next_text = [html.B("All 4 stages complete (Baseline \u2192 Iteration 3). "),
                     "Comparison now sends the same full Control Tower brief to both supervisors \u2014 the multi-agent "
                     "Control Tower (routes to the 5 domain Genie agents + policy functions) and the Genie One MCP "
                     f"supervisor (one tool that finds the data itself) \u2014 and scores each answer against the same {_NT()} "
                     "ground-truth values: which architecture gets more numbers right, how long it takes, and where they disagree."]
        next_tab = "comparison"
        btn_label = "Go to Comparison"
    elif current_stage == "Baseline":
        next_text = "Environment ready. Run the Baseline benchmark to establish your starting score."
        next_tab = "baseline"
        btn_label = "Go to Baseline"
    elif current_stage.startswith("Iteration"):
        iter_n = current_stage.split()[-1]
        prev = completed[-1] if completed else "?"
        prev_score = benchmarks.get(prev, "?")
        next_text = f"{prev} scored {prev_score}/45. Next: run {current_stage} setup, then benchmark."
        next_tab = "iterations"
        btn_label = f"Go to Iterations"
    else:
        next_text = ""
        next_tab = "overview"
        btn_label = ""
    banner = html.Div([
        html.Div([
            html.Span("\u2192 ", style={"fontWeight":"700","fontSize":"16px"}),
            html.Span(next_text, style={"fontSize":"14px"}),
            html.Button(btn_label, id="stage-next-btn", n_clicks=0,
                        className="btn primary", style={"fontSize":"12px","padding":"6px 16px","marginLeft":"auto"})
                if btn_label else html.Span(),
            dcc.Store(id="stage-next-tab", data=next_tab),
        ], style={"display":"flex","alignItems":"center","gap":"12px"}),
    ], style={"background":C["indigo_bg"],"border":f"1px solid {C['indigo']}","borderRadius":"12px",
              "padding":"14px 20px","color":C["indigo"]})
    return html.Div([
        html.H3("Stage Progression", style={"marginBottom":"14px"}),
        html.Div(dots, style={"display":"flex","gap":"12px","flexWrap":"wrap","marginBottom":"16px"}),
        banner,
    ], className="stage", style={"marginBottom":"24px"})

def _latest_stage_name():
    """Return name of the most recently completed stage, in order."""
    bm = _ust().get("benchmarks", {})
    if not bm:
        return "--"
    for s in reversed(["Baseline", "Iteration 1", "Iteration 2", "Iteration 3"]):
        if s in bm:
            return s
    return "--"


def _latest_stage_score():
    """Return score string (e.g. '39/45') of the most recently completed stage."""
    bm = _ust().get("benchmarks", {})
    if not bm:
        return "--"
    for s in reversed(["Baseline", "Iteration 1", "Iteration 2", "Iteration 3"]):
        if s in bm:
            return f"{bm[s]}/{_NT()}"
    return "--"


def _agent_count_summary(agents=None):
    """(total, detail_children) across every version.
    Returns (int, list[Span]) so the card can render each version as a styled chip.
    Falls back to the single discovered set when no versioned agents are known."""
    versions = _ust().get("agent_versions") or {}
    if versions:
        total = sum(len(a) for a in versions.values())
        parts = []
        for v, a in versions.items():
            if parts:
                parts.append(html.Span(" \u00b7 ", style={"color": C["muted"], "margin": "0 2px"}))
            parts.append(html.Span([
                html.Span(v, style={"fontWeight": "600"}),
                html.Span(f" {len(a)}", style={"fontWeight": "400"}),
            ]))
        return total, html.Span(parts, style={"fontSize": "12px", "color": C["muted"], "lineHeight": "1.6"})
    agents = _ust().get("agents", {}) if agents is None else agents
    return len(agents), html.Span(", ".join(n.title() for n in agents), style={"fontSize": "12px", "color": C["muted"]})


def _agent_version_options():
    versions = _ust().get("agent_versions") or {}
    return [{"label": f"{v} ({len(a)}/5)", "value": v} for v, a in versions.items()]


def _selected_agent_version():
    """User's last pick if it still exists, else the latest version available."""
    versions = _ust().get("agent_versions") or {}
    pick = _ust().get("overview_agent_version")
    if pick in versions:
        return pick
    return list(versions)[-1] if versions else None


def _build_agent_version_table(version):
    versions = _ust().get("agent_versions") or {}
    agents = versions.get(version) or ({} if versions else _ust().get("agents", {}))
    order = ["logistics", "demand", "inventory", "executive", "supplier"]
    rows = [
        html.Tr([
            html.Td(name.replace("-"," ").title(), style={"fontWeight":"600"}),
            html.Td(version or "", style={"color":C["muted"],"fontSize":"13px"}),
            html.Td(html.Code(sid[:24]+"..."), style={"fontSize":"13px"}),
            html.Td(Chip("Active","t")),
            html.Td(html.Button(
                "Chat", className="btn",
                id={"type":"qchat","index":f"agent:{sid}"},
                style={"fontSize":"12px","padding":"5px 14px"},
            )),
        ]) for name, sid in sorted(agents.items(), key=lambda kv: order.index(kv[0]) if kv[0] in order else 99)
    ]
    return html.Table([
        html.Thead(html.Tr([html.Th(h) for h in ["Agent","Version","Space ID","Status",""]])),
        html.Tbody(rows or [html.Tr(html.Td("No agents found", colSpan=5, style={"color":C["muted"]}))]),
    ], className="tbl")


@callback(Output("overview-agents-body", "children"),
          Input("overview-agent-version", "value"),
          prevent_initial_call=True)
def select_agent_version(version):
    if not version:
        raise PreventUpdate
    _ust()["overview_agent_version"] = version
    return _build_agent_version_table(version)


def _build_overview_story():
    """Explains the purpose of the Control Tower and the workbench, with a flow diagram,
    the stage-by-stage curation story and the measured score progression."""
    card = {"border": f"1px solid {C['line']}", "borderRadius": "14px", "padding": "14px 16px",
            "background": C["surface"], "flex": "1", "minWidth": "180px"}
    lab = {"fontSize": "11px", "fontWeight": "700", "letterSpacing": "0.06em", "textTransform": "uppercase"}
    arrow = html.Div("\u2192", style={"fontSize": "22px", "color": C["muted"], "alignSelf": "center", "padding": "0 2px"})
    def node(kicker, title, body, tone):
        return html.Div([html.Div(kicker, style={**lab, "color": C[tone], "marginBottom": "4px"}),
                         html.Div(title, style={"fontWeight": "700", "fontSize": "15px", "marginBottom": "4px"}),
                         html.Div(body, style={"color": C["muted"], "fontSize": "12.5px", "lineHeight": "1.45"})],
                        style={**card, "borderTop": f"3px solid {C[tone]}"})
    flow = html.Div([
        node("1 \u2022 Data", "Unity Catalog", "5 supply-chain domains: demand, inventory, logistics, supplier, executive KPIs.", "muted"),
        arrow,
        node("2 \u2022 Specialists", "5 Genie domain agents", "Each turns plain-English questions into governed SQL on its own domain.", "teal"),
        arrow,
        node("3 \u2022 Orchestration", "Supervisor", "The Control Tower splits a question, routes each part, reconciles the numbers.", "indigo"),
        arrow,
        node("4 \u2022 Outcome", "One cited brief", "Root cause, risk and actions \u2014 every number traceable to the SQL that produced it.", "rose"),
    ], style={"display": "flex", "gap": "8px", "flexWrap": "wrap", "alignItems": "stretch"})

    marks = _ust().get("benchmarks") or {}
    stage_names = ["Baseline", "Iteration 1", "Iteration 2", "Iteration 3"]
    feats = {"Baseline": ("Out of the box", "Agents get tables only \u2014 no business context.",
                          ["Raw tables", "Agent instructions"])}
    for it in ITER_INFO:
        feats[it[0]] = (it[1], it[2], [f for f, _how in it[3]])
    def stage_card(sname):
        title, desc, items = feats[sname]
        sc = marks.get(sname)
        score = html.Div(f"{sc}/{_NT()}" if sc is not None else "not run",
                         style={"fontFamily": "var(--display)", "fontWeight": "800", "fontSize": "22px",
                                "color": C["teal"] if sc is not None else C["muted"]})
        return html.Div([
            html.Div([html.Div(sname, style={**lab, "color": C["indigo"]}), score],
                     style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}),
            html.Div(title, style={"fontWeight": "700", "fontSize": "14px", "margin": "4px 0"}),
            html.Div(desc, style={"color": C["muted"], "fontSize": "12.5px", "marginBottom": "8px", "lineHeight": "1.45"}),
            html.Div([html.Span(f, style={"fontSize": "11.5px", "padding": "2px 8px", "borderRadius": "999px",
                                          "background": C["indigo_bg"], "color": C["indigo"]}) for f in items],
                     style={"display": "flex", "gap": "5px", "flexWrap": "wrap"}),
        ], style=card)
    stages = html.Div([stage_card(s) for s in stage_names],
                      style={"display": "flex", "gap": "12px", "flexWrap": "wrap"})

    done = [s for s in stage_names if marks.get(s) is not None]
    chart = html.Div("Run the Baseline benchmark to see the accuracy curve.",
                     style={"color": C["muted"], "fontSize": "13px", "padding": "18px 0"})
    if done:
        pct = [round(100 * marks[s] / _NT()) for s in done]
        fig = _bar3d(done, [(f"Accuracy % (of {_NT()} ground-truth values)", pct, "teal")], height=320)
        chart = dcc.Graph(figure=fig, config={"displayModeBar": False})
    groups = html.Div([html.Span([html.B(g, style={"marginRight": "5px"}), GROUPS[g][0]],
                                 style={"fontSize": "12px", "padding": "3px 10px", "borderRadius": "999px",
                                        "border": f"1px solid {C['line']}", "background": C["soft"]})
                       for g in GROUPS], style={"display": "flex", "gap": "6px", "flexWrap": "wrap"})
    return html.Div([
        html.H3("What this workbench is for", style={"marginBottom": "6px"}),
        html.P(["The Genie Curator Workbench is where a ", html.B("data curator"), " makes Genie agents better over time "
                "\u2014 not by rewriting prompts, but by giving the agents a ", html.B("Unity Catalog semantic layer"),
                ": column and table comments, example SQL, metric views, governed tags, reference tables and policy "
                "functions. Each curation stage is measured against the same ", html.B(f"{_NT()} ground-truth values"),
                ", so every gain in ", html.B("accuracy"), " and ", html.B("reliability"), " is proven, not assumed."],
               style={"fontSize": "14px", "lineHeight": "1.6", "margin": "0 0 12px"}),
        html.Div([
            html.Div([html.Div(f"{n}", style={"width": "24px", "height": "24px", "borderRadius": "50%", "background": C[tone],
                                              "color": "#fff", "display": "inline-flex", "alignItems": "center",
                                              "justifyContent": "center", "fontWeight": "700", "fontSize": "12px",
                                              "flexShrink": "0"}),
                      html.Div([html.Div(t, style={"fontWeight": "700", "fontSize": "13.5px"}),
                                html.Div(d, style={"color": C["muted"], "fontSize": "12px", "lineHeight": "1.4"})])],
                     style={"display": "flex", "gap": "10px", "alignItems": "flex-start", "flex": "1 1 200px",
                            "padding": "10px 12px", "borderRadius": "12px", "background": C["soft"]})
            for n, t, d, tone in [
                (1, "Measure", f"Benchmark the agents on {_NT()} questions with known answers.", "indigo"),
                (2, "Diagnose", "See which answers miss and why \u2014 wrong table, filter, term or date.", "rose"),
                (3, "Curate", "Add the UC semantic asset that closes the gap \u2014 no prompt hacks.", "teal"),
                (4, "Re-measure", "Re-benchmark, re-test rephrasings, compare supervisors. Repeat.", "indigo"),
            ]
        ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap", "margin": "0 0 18px"}),
        html.P(["The agents serve the ", html.B("Supply Chain Control Tower"), ": leaders ask cross-domain questions \u2014 ",
                html.I("why did Western revenue drop, which suppliers and shipments caused it, what is at risk this quarter?"),
                " \u2014 and get one cited brief. Curation is what makes those numbers trustworthy."],
               style={"fontSize": "13px", "color": C["muted"], "lineHeight": "1.55", "margin": "0 0 10px"}),
        flow,
        html.H4("How accuracy is earned \u2014 curation stages", style={"margin": "22px 0 10px"}),
        stages,
        html.Div([
            html.Div([html.H4("Measured accuracy by stage", style={"margin": "0 0 4px"}),
                      html.Div(f"Share of the {_NT()} ground-truth values each stage's agents return exactly.",
                               style={"color": C["muted"], "fontSize": "12px"}), chart],
                     className="rob-chart", style={"flex": "2 1 420px", "minWidth": "0"}),
            html.Div([html.H4("What is measured", style={"margin": "0 0 4px"}),
                      html.Div(f"{_NT()} questions in these groups, each with a pinned ground-truth value.",
                               style={"color": C["muted"], "fontSize": "12px", "marginBottom": "10px"}), groups,
                      html.Div([html.B("Then: "), "Reliability rephrases the hardest questions to test consistency, and "
                                "Comparison pits the multi-agent Control Tower against the Genie One MCP supervisor."],
                               style={"marginTop": "14px", "fontSize": "12.5px", "padding": "8px 12px",
                                      "borderLeft": f"3px solid {C['indigo']}", "background": C["indigo_bg"],
                                      "borderRadius": "8px"})],
                     style={"flex": "1 1 280px", "minWidth": "0"}),
        ], style={"display": "flex", "gap": "24px", "flexWrap": "wrap", "marginTop": "22px"}),
    ], className="stage")


def _overview_hero():
    """Formatted subtitle: what the workbench does, and the 4-step curation path with its real sizes."""
    steps = [("1", "Benchmark", f"{_NT()} ground-truth values", "indigo"),
             ("2", "Curate", "UC semantic layer, 3 stages", "teal"),
             ("3", "Test reliability", "30 rephrased questions", "indigo"),
             ("4", "Compare", "2 supervisor architectures", "rose")]
    pill = lambda n, t, d, k: html.Div([
        html.Span(n, style={"width": "22px", "height": "22px", "borderRadius": "50%", "background": C[k], "color": "#fff",
                            "display": "inline-flex", "alignItems": "center", "justifyContent": "center",
                            "fontSize": "11.5px", "fontWeight": "800", "flexShrink": "0"}),
        html.Div([html.Div(t, style={"fontWeight": "700", "fontSize": "13.5px", "color": C["ink"]}),
                  html.Div(d, style={"fontSize": "11.5px", "color": C["muted"]})])],
        style={"display": "flex", "alignItems": "center", "gap": "9px", "padding": "8px 14px", "borderRadius": "12px",
               "background": C[f"{k}_bg"], "border": f"1px solid {C['line']}"})
    arrow = html.Span("\u2192", style={"color": C["muted"], "fontSize": "18px", "alignSelf": "center"})
    row = []
    for i, s in enumerate(steps):
        if i:
            row.append(arrow)
        row.append(pill(*s))
    return html.Div([
        html.Div("Genie Curator Workbench", style={"fontSize": "12px", "fontWeight": "700", "letterSpacing": "0.08em",
                                                   "textTransform": "uppercase", "color": C["indigo"], "marginTop": "6px"}),
        html.Div(["Curate Genie agents with a ", html.Span("Unity Catalog semantic layer", style={"color": C["teal"], "fontWeight": "700"}),
                  " \u2014 and prove every gain in accuracy and reliability."],
                 style={"fontSize": "19px", "lineHeight": "1.4", "color": C["ink"], "margin": "4px 0 14px",
                        "fontFamily": "var(--display)"}),
        html.Div(row, style={"display": "flex", "gap": "8px", "flexWrap": "wrap", "alignItems": "stretch"}),
    ], style={"marginBottom": "26px"})


def _overview_rob_tiles():
    """Two overview tiles: avg reliability & dependability (always shown; '--' when not run)."""
    sc = _rob_scores(_ust().get("rob_results") or []) if _ust().get("rob_results") else []
    if not sc:
        return [Tile("--", "RELIABILITY"), Tile("--", "DEPENDABILITY")]
    avg_rel = sum(s["reliability"] for s in sc) / len(sc)
    avg_dep = sum(s["dependability"] for s in sc) / len(sc)
    return [
        Tile(f"{avg_rel:.0f}%", "RELIABILITY", "teal" if avg_rel >= 80 else ("rose" if avg_rel < 50 else None)),
        Tile(f"{avg_dep:.0f}%", "DEPENDABILITY", "indigo" if avg_dep >= 80 else ("rose" if avg_dep < 50 else None)),
    ]



def _deploy_sticker(dismissed=False):
    """Dismissible sticker: how to do a DAB deploy (shown on overview, bottom-left)."""
    if dismissed:
        return html.Div(id="deploy-sticker")
    _REPO_URL = "https://github.com/saketml79/GAP_genie_multiagent_datanalysis_demo/tree/feature_v3_genie_curator_withapp"
    return html.Div([
        html.Div([
            html.Span("🚀", style={"fontSize": "16px", "marginRight": "6px"}),
            html.Span("Getting Started — Bundle Deploy",
                       style={"fontWeight": "700", "fontSize": "13px", "flex": "1"}),
            html.Button("✕", id="btn-dismiss-sticker",
                        style={"background": "none", "border": "none", "cursor": "pointer",
                               "fontSize": "16px", "color": C["muted"], "padding": "0 2px",
                               "lineHeight": "1"}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "10px"}),
        html.Ol([
            # Step 1: Clone + clickable repo link
            html.Li([
                html.Span("Clone the repo into a Databricks Git folder",
                          style={"fontWeight": "600", "fontSize": "12px"}),
                html.Div(html.Code("Repos → Add Repo → paste the URL below → Clone",
                         style={"fontSize": "11px", "wordBreak": "break-all"}),
                         style={"marginTop": "2px", "color": C["muted"]}),
                html.Div(html.A(_REPO_URL, href=_REPO_URL, target="_blank",
                         style={"fontSize": "11px", "color": C["indigo"],
                                "textDecoration": "underline", "wordBreak": "break-all"}),
                         style={"marginTop": "3px"}),
            ], style={"marginBottom": "8px"}),
            # Step 2
            html.Li([
                html.Span("Open a terminal in the repo folder",
                          style={"fontWeight": "600", "fontSize": "12px"}),
                html.Div(html.Code("File menu → New → Terminal  (or use Web Terminal)",
                         style={"fontSize": "11px", "wordBreak": "break-all"}),
                         style={"marginTop": "2px", "color": C["muted"]}),
            ], style={"marginBottom": "8px"}),
            # Step 3
            html.Li([
                html.Span("Validate the bundle",
                          style={"fontWeight": "600", "fontSize": "12px"}),
                html.Div(html.Code("databricks bundle validate --target dev",
                         style={"fontSize": "11px", "wordBreak": "break-all"}),
                         style={"marginTop": "2px", "color": C["muted"]}),
            ], style={"marginBottom": "8px"}),
            # Step 4
            html.Li([
                html.Span("Deploy your jobs",
                          style={"fontWeight": "600", "fontSize": "12px"}),
                html.Div(html.Code("databricks bundle deploy --target dev",
                         style={"fontSize": "11px", "wordBreak": "break-all"}),
                         style={"marginTop": "2px", "color": C["muted"]}),
            ], style={"marginBottom": "8px"}),
            # Step 5
            html.Li([
                html.Span("Verify in Jobs UI",
                          style={"fontWeight": "600", "fontSize": "12px"}),
                html.Div(html.Code("Filter by Tags → application: gapgenie",
                         style={"fontSize": "11px", "wordBreak": "break-all"}),
                         style={"marginTop": "2px", "color": C["muted"]}),
            ], style={"marginBottom": "8px"}),
        ], style={"margin": "0", "paddingLeft": "18px", "listStyleType": "decimal"}),
    ], id="deploy-sticker",
       style={"position": "fixed", "bottom": "20px", "left": "20px", "zIndex": "900",
              "background": C["surface"], "border": f"1px solid {C['line']}",
              "borderRadius": "12px", "padding": "14px 16px", "width": "310px",
              "boxShadow": "0 4px 20px rgba(0,0,0,.12)", "fontSize": "13px",
              "color": C["ink"]})

def page_overview():
    if not ST.get("app_ready"):
        return _build_loading_page("Loading Overview")
    agents = _ust().get("agents", {})
    sups = _ust().get("supervisors", [])
    n = len(agents); ns = len(sups)
    env_missing = (n == 0 and ns == 0)
    rebuild_banner = html.Div([
        html.Div([
            html.Span("!", style={"fontSize":"18px","fontWeight":"800","marginRight":"10px",
                                    "background":C["rose"],"color":"#fff","borderRadius":"50%",
                                    "width":"26px","height":"26px","display":"inline-flex",
                                    "alignItems":"center","justifyContent":"center"}),
            html.Span("Environment not found", style={"fontWeight":"700","fontSize":"16px"}),
        ], style={"display":"flex","alignItems":"center","marginBottom":"6px"}),
        html.P("No agents or supervisors detected. The environment needs to be built (or rebuilt after teardown).",
               style={"margin":"0 0 8px","fontSize":"14px"}),
        html.P("Go to Setup & Teardown and click Run Full Setup to create the catalog, data, agents, and supervisor.",
               style={"margin":"0","fontSize":"13px","color":C["muted"]}),
    ], style={"background":C["rose_bg"],"border":f"1px solid {C['rose']}","borderRadius":"14px",
             "padding":"16px 20px","marginBottom":"24px","color":C["rose"]}) if env_missing else html.Div()
    return html.Div([
        html.Div([
            html.H1("Supply Chain Control Tower", style={"flex":"1"}),
            html.Button("Refresh", id="btn-refresh-overview", className="btn",
                        style={"fontSize":"13px","padding":"8px 16px","alignSelf":"center"}),
        ], style={"display":"flex","alignItems":"center","gap":"16px"}),
        _overview_hero(),
        rebuild_banner,
        html.Div([
            Tile(_agent_count_summary(agents)[0], "DOMAIN AGENTS", "teal" if n>=5 else ("rose" if n==0 else None)),
            Tile(ns, "SUPERVISORS", "indigo" if ns>0 else ("rose" if ns==0 else None)),
            Tile(_latest_stage_name(), "LATEST STAGE", "indigo" if _ust().get("benchmarks") else None),
            Tile(_latest_stage_score(), "LATEST SCORE", "teal" if _ust().get("benchmarks") else None),
        ] + _overview_rob_tiles(), className="tiles-row"),
        # Stage progress + next-action recommendation
        _build_stage_progress(),
        # What the Control Tower is for: architecture, how accuracy is earned, progression
        _build_overview_story(),
        # Agent table -- user picks which version (Baseline / Iteration N) to list and chat with
        html.Div([
            html.Div([
                html.H3("Active Domain Agents", style={"marginBottom":"0","flex":"1"}),
                html.Span("Version", style={"color":C["muted"],"fontSize":"13px"}),
                dcc.Dropdown(id="overview-agent-version", options=_agent_version_options(),
                             value=_selected_agent_version(), clearable=False,
                             style={"width":"220px","background":C["soft"]}),
            ], style={"display":"flex","alignItems":"center","gap":"10px","marginBottom":"14px"}),
            html.Div(id="overview-agents-body", children=_build_agent_version_table(_selected_agent_version())),
        ], className="stage"),
        # Supervisors
        html.Div([
            html.H3("Supervisor Agents", style={"marginBottom":"14px"}),
            html.Table([
                html.Thead(html.Tr([html.Th(h) for h in ["Name","Endpoint","Description",""]])),
                html.Tbody([
                    html.Tr([
                        html.Td(_sup_title(s, "Supervisor"), style={"fontWeight":"600","overflowWrap":"anywhere"}),
                        html.Td(html.Code(s["endpoint"]), style={"fontSize":"13px"}),
                        html.Td(s["desc"], style={"color":C["muted"]}),
                        html.Td(html.Button(
                            "Chat", className="btn primary",
                            id={"type":"qchat","index":f"sup:{s['endpoint']}"},
                            style={"fontSize":"12px","padding":"5px 14px"},
                        )),
                    ]) for s in sups
                ] if sups else [html.Tr(html.Td("No supervisors found", colSpan=4, style={"color":C["muted"]}))])
            ], className="tbl"),
        ], className="stage"),
        # (Chat buttons are inline in the tables above)
        _deploy_sticker(),
    ])

# Refresh Overview — re-render the entire page
@callback(Output("page-refresh","data", allow_duplicate=True),
          Input("btn-refresh-overview","n_clicks"),
          prevent_initial_call=True)
def refresh_overview(n):
    # Explicit Refresh button: re-discover, then bump page-refresh so render() redraws
    # (render() is the sole writer of page.children — prevents race with tab switches)
    try:
        _discover_all()
    except Exception as e:
        log(f"refresh_overview error: {e}")
    _ust()["_last_page_fp"] = ""  # force fingerprint mismatch
    return -int(n or 1)

@callback(Output("deploy-sticker", "children"),
          Output("deploy-sticker", "style"),
          Input("btn-dismiss-sticker", "n_clicks"),
          prevent_initial_call=True)
def dismiss_deploy_sticker(_):
    return [], {"display": "none"}

# ==== Page 2: Setup & Teardown ==============================================
def _build_state_cards():
    """Build environment state cards from cached ST[] only.
    NO API calls here — discovery and warehouse checks happen via
    global-poll (_discover_all / update_wh_chip) every 8s.
    This keeps the callback non-blocking so tab switching stays instant."""
    agents = _ust().get("agents", {})
    sups = _ust().get("supervisors", [])
    wh_state = ST.get("warehouse_state", "UNKNOWN")
    _my_alias = _get_user_alias() if _in_request() else "unknown"
    _cat_info = ST.get(f"cat_{_my_alias}", {})
    catalog_exists = _cat_info.get("exists", False)
    table_count = _cat_info.get("tables", 0)
    _my_catalog = user_catalog() if _in_request() else (_ust().get("user_catalog") or CATALOG)
    # Build cards
    def card(label, value, detail, state="gone"):
        # state: "alive" | "gone" | "starting" | "waiting"
        cls = f"state-card {state}"
        return html.Div([
            html.Div(label, className="sc-label"),
            html.Div(str(value), className="sc-value"),
            html.Div(detail, className="sc-detail"),
        ], className=cls)
    cat_detail = f"{table_count} tables" if table_count else ("Exists" if catalog_exists else "Not found")
    wh_name = ST.get("warehouse_name", WAREHOUSE_ID[:12])
    wh_starting = wh_state in ("STARTING", "STOPPING")
    wh_st = "alive" if wh_state == "RUNNING" else ("starting" if wh_starting else "gone")
    wh_detail = "Ready" if wh_state == "RUNNING" else ("Starting — please wait…" if wh_starting else wh_state)
    # When warehouse not running, dependent cards show "waiting" state
    if wh_state != "RUNNING" and not catalog_exists:
        dep_st = "waiting" if wh_starting else "gone"
        dep_detail = "Waiting for warehouse…" if wh_starting else "Warehouse not running"
        return html.Div([
            card("WAREHOUSE", wh_name, wh_detail, wh_st),
            card("CATALOG", _my_catalog, dep_detail, dep_st),
            card("DOMAIN AGENTS", _agent_count_summary(agents)[0] or "—", dep_detail if not agents else _agent_count_summary(agents)[1], dep_st if not agents else "alive"),
            card("SUPERVISORS", len(sups) or "—", dep_detail if not sups else ", ".join(_sup_title(s, f"Supervisor {chr(65 + k)}") for k, s in enumerate(sups)), dep_st if not sups else "alive"),
            card("SCHEMAS", "—", dep_detail, dep_st),
        ], className="state-grid")
    return html.Div([
        card("WAREHOUSE", wh_name, wh_detail, wh_st),
        card("CATALOG", _my_catalog, cat_detail, "alive" if catalog_exists else "gone"),
        card("DOMAIN AGENTS", _agent_count_summary(agents)[0], _agent_count_summary(agents)[1] if agents else "None discovered", "alive" if agents else "gone"),
        card("SUPERVISORS", len(sups), ", ".join(_sup_title(s, f"Supervisor {chr(65 + k)}") for k, s in enumerate(sups)) if sups else "None discovered", "alive" if sups else "gone"),
        card("SCHEMAS", "6" if catalog_exists else "0", "demand, inventory, logistics, supplier, reporting, action_intelligence" if catalog_exists else "Catalog missing", "alive" if catalog_exists else "gone"),
    ], className="state-grid")

def _teardown_scope_card():
    """What Full Teardown removes, what it keeps, and how completion is proven."""
    def item(mark, tone, title, detail):
        return html.Div([
            html.Span(mark, style={"width": "20px", "height": "20px", "borderRadius": "50%", "background": C[tone],
                                   "color": "#fff", "display": "inline-flex", "alignItems": "center",
                                   "justifyContent": "center", "fontSize": "11px", "fontWeight": "800", "flexShrink": "0"}),
            html.Div([html.Span(title, style={"fontWeight": "700", "fontSize": "13px", "color": C["ink"]}),
                      html.Span(f" \u2014 {detail}", style={"fontSize": "12.5px", "color": C["muted"]})],
                     style={"lineHeight": "1.4"}),
        ], style={"display": "flex", "gap": "9px", "alignItems": "flex-start", "padding": "4px 0"})
    lab = {"fontSize": "11px", "fontWeight": "700", "letterSpacing": "0.06em", "textTransform": "uppercase",
           "margin": "4px 0 4px"}
    return html.Div([
        html.Div("Deletes", style={**lab, "color": C["rose"]}),
        item("\u2715", "rose", "All SC Genie agents", "every version: Baseline, Iteration 1, 2, 3 and the Evaluator"),
        item("\u2715", "rose", "Supervisors", "Control Tower and MCP supervisor, plus their serving endpoints"),
        item("\u2715", "rose", f"Catalog {user_catalog() if _in_request() else (_ust().get('user_catalog') or CATALOG)}", "DROP CATALOG CASCADE: all schemas, tables, views, metric views, "
             "functions and saved run history"),
        html.Div("Keeps", style={**lab, "color": C["teal"], "marginTop": "10px"}),
        item("\u2713", "teal", "UC Domains + Pages", "created by hand on the Discover page; delete them there if needed"),
        item("\u2713", "teal", "Jobs and this app", "managed by the bundle, so Full Setup can rebuild everything"),
        html.Div([html.B("Proof: "), "the job re-checks at the end and fails if any agent, supervisor or the catalog is "
                  "still present \u2014 \u201ccomplete\u201d is only shown when nothing is left."],
                 style={"marginTop": "10px", "padding": "8px 12px", "borderLeft": f"3px solid {C['indigo']}",
                        "background": C["indigo_bg"], "borderRadius": "8px", "fontSize": "12.5px", "color": C["ink"]}),
    ], style={"border": f"1px solid {C['line']}", "borderRadius": "12px", "padding": "12px 14px",
              "background": C["surface"], "marginBottom": "14px"})


def page_setup():
    if not ST.get("app_ready"):
        return _build_loading_page("Loading Setup & Teardown")
    steps = [
        ("01_create_catalog_schemas","Create catalog + 6 domain schemas"),
        ("02_generate_demand_data","Generate deterministic sales orders (seed=42)"),
        ("03_generate_inventory_data","Generate inventory ledger (seed=43)"),
        ("04_generate_logistics_data","Generate shipments data (seed=44)"),
        ("05_generate_supplier_data","Generate supplier orders + SLAs (seed=45)"),
        ("06_create_reporting_views","Create executive reporting views"),
        ("08_setup_genie_supervisor","Create 5 Genie Agents + 2 Supervisors + Evaluator"),
    ]
    # Teardown button initial state — disable when nothing to delete
    _td_has_agents = bool(_ust().get("agents"))
    _td_has_sups = bool(_ust().get("supervisors"))
    _ps_alias = _get_user_alias() if _in_request() else "unknown"
    _td_has_anything = _td_has_agents or _td_has_sups or ST.get(f"cat_{_ps_alias}", {}).get("exists", False)
    return html.Div([
        html.H2("Setup & Teardown"),
        html.P("Manage the full environment lifecycle: provision, configure, and reset.",
               style={"color":C["muted"],"marginBottom":"28px"}),
        # Current environment state
        html.Div([
            html.H3("Environment State", style={"marginBottom":"12px"}),
            html.P("Live discovery of what exists right now.",
                   style={"color":C["muted"],"fontSize":"14px","marginBottom":"14px"}),
            html.Div(id="td-state", children=_build_state_cards()),
        ], className="stage"),
        # Teardown — 60/40 layout: controls left, output right
        html.Div([
            html.H3("Teardown", style={"color":C["rose"],"marginBottom":"12px"}),
            html.P("Delete agents, supervisors, or drop the catalog.",
                   style={"color":C["muted"],"fontSize":"14px","marginBottom":"14px"}),
            html.Div([
                # Left column — 60%
                html.Div([
                    html.Div([
                        html.Button("Full Teardown (Agents + Supervisors + Catalog)", id="td-everything", className="btn danger",
                                    disabled=not _td_has_anything,
                                    style={"fontWeight":"700","fontSize":"15px","padding":"12px 28px",
                                           "opacity":"1" if _td_has_anything else "0.4"}),
                    ], style={"marginBottom":"14px"}),
                    _teardown_scope_card(),
                    html.Div([
                        html.Button("Delete Non-Iter-3 Agents Only", id="td-agents", className="btn danger",
                                    disabled=not _td_has_agents),
                        html.Button("Delete ALL SC Agents Only", id="td-all", className="btn danger",
                                    disabled=not _td_has_agents),
                        html.Button("Delete Supervisors Only", id="td-sups", className="btn danger",
                                    disabled=not _td_has_sups),
                    ], className="btn-row"),
                ], style={"flex":"6","minWidth":"0"}),
                # Right column — 40%: output box fills exactly the height of the left column
                html.Div([
                    dcc.Interval(id="td-poll", interval=1500, disabled=True),
                    html.Div(Screen("Teardown Output", "td-out", height="100%",
                                    initial_content=_linkify_log_block(_ust().get("td_log", [])) if _ust().get("td_log") else None,
                                    clear_id="td-clear"),
                             style={"position":"absolute","inset":"0"}),
                ], style={"flex":"4","minWidth":"300px","marginLeft":"20px","position":"relative"}),
            ], style={"display":"flex","gap":"0","alignItems":"stretch"}),
        ], className="stage"),
        # Setup — 60/40 layout: controls left, output right
        html.Div([
            html.H3("Baseline Setup", style={"color":C["teal"],"marginBottom":"12px"}),
            html.Div([
                html.P("Create catalog, generate deterministic data, and build baseline agents + supervisors.",
                       style={"color":C["muted"],"fontSize":"14px","marginBottom":"12px"}),
                html.Div("Next Up", style={"color":C["ink"],"fontSize":"12px","fontWeight":"700",
                         "textTransform":"uppercase","letterSpacing":".08em","marginBottom":"8px"}),
                html.Div([
                    html.Div([
                        html.Span("Iteration 1", style={"color":C["indigo"],"fontWeight":"700"}),
                        html.Span(" — Column Comments, Table Comments, Example SQL Queries, Benchmark Questions", style={"color":C["muted"]}),
                    ], style={"fontSize":"12px","marginBottom":"6px"}),
                    html.Div([
                        html.Span("Iteration 2", style={"color":C["teal"],"fontWeight":"700"}),
                        html.Span(" — UC Metric Views, Open Knowledge View, Governed Tags, Schema Domain Tags", style={"color":C["muted"]}),
                    ], style={"fontSize":"12px","marginBottom":"6px"}),
                    html.Div([
                        html.Span("Iteration 3", style={"color":C["rose"],"fontWeight":"700"}),
                        html.Span(" — UC Domain, UC Pages, Reference Table, SQL Functions, Entity Matching", style={"color":C["muted"]}),
                    ], style={"fontSize":"12px"}),
                ], style={"marginBottom":"14px","paddingLeft":"12px","borderLeft":f"2px solid {C['line']}"}),
            ]),
            html.Div([
                # Left column — 70% (steps + button)
                html.Div([
                    html.Div(id="setup-steps", children=[
                        StepRow(i+1, name, desc) for i, (name, desc) in enumerate(steps)
                    ]),
                    html.Div(id="setup-btn-wrap", children=[
                        html.Button("Run Full Setup", id="btn-setup",
                                    className="btn",
                                    disabled=True,
                                    style={"fontWeight":"700","fontSize":"15px","padding":"12px 28px","opacity":"0.4"}),
                        html.Span("Checking environment…", id="setup-btn-hint",
                                  style={"color":C["muted"],
                                         "fontSize":"10px","fontFamily":"var(--mono)","marginLeft":"12px","overflow":"hidden","textOverflow":"ellipsis","whiteSpace":"nowrap","maxWidth":"280px"}),
                    ], style={"display":"flex","alignItems":"center","marginTop":"20px"}),
                ], style={"flex":"6","minWidth":"0"}),
                # Right column — 40% (fixed-height output box)
                html.Div([
                    dcc.Interval(id="setup-poll", interval=2000, disabled=True),
                    Screen("Setup Output", "setup-out", height="418px",
                           initial_content=_linkify_log_block(_ust().get("setup_log", [])) if _ust().get("setup_log") else None,
                           clear_id="setup-clear"),
                ], style={"flex":"4","minWidth":"300px","marginLeft":"20px"}),
            ], style={"display":"flex","gap":"0","alignItems":"flex-start"}),
        ], className="stage"),
        # Auto-refresh interval for environment state cards (OBO)
        dcc.Interval(id="state-refresh", interval=10000, disabled=False),
    ])

@callback(Output("td-state","children", allow_duplicate=True),
          Output("btn-setup","disabled"),
          Output("btn-setup","className"),
          Output("btn-setup","style"),
          Output("setup-btn-hint","children"),
          Output("setup-btn-hint","style"),
          # Teardown buttons: disabled when nothing to tear down
          Output("td-everything","disabled"),
          Output("td-everything","style"),
          Output("td-agents","disabled"),
          Output("td-all","disabled"),
          Output("td-sups","disabled"),
          Input("state-refresh","n_intervals"),
          prevent_initial_call=True)
def auto_refresh_state(n):
    """Periodically refresh environment state + setup/teardown button state."""
    cards = _build_state_cards()
    has_agents = bool(_ust().get("agents"))
    has_sups = bool(_ust().get("supervisors"))
    _ars_alias = _get_user_alias() if _in_request() else "unknown"
    has_catalog = bool(ST.get(f"cat_{_ars_alias}", {}).get("exists"))
    has_anything = has_agents or has_sups or has_catalog
    setup_running = _ust().get("setup_running", False)
    td_running = _ust().get("td_running", False)
    # Setup button: disabled whenever ANYTHING exists OR setup/teardown running.
    # Multiple signals prevent the flicker gap where discover_agents returns
    # empty but the catalog already exists.
    btn_disabled = has_anything or setup_running or td_running
    btn_cls = "btn" if btn_disabled else "btn primary"
    btn_style = {"fontWeight":"700","fontSize":"15px","padding":"12px 28px",
                 "opacity":"0.4" if btn_disabled else "1"}
    _hint_base = {"fontSize":"10px","fontFamily":"var(--mono)","marginLeft":"12px","overflow":"hidden","textOverflow":"ellipsis","whiteSpace":"nowrap","maxWidth":"380px"}
    if setup_running:
        hint = "Setup is running… check the output panel for live progress."
        hint_style = {**_hint_base, "color":C["teal"]}
    elif td_running:
        hint = "Teardown is running…"
        hint_style = {**_hint_base, "color":C["rose"]}
    elif has_anything:
        hint = "Environment exists — teardown first to re-run setup."
        hint_style = {**_hint_base, "color":C["rose"]}
    else:
        hint = f"Source: {SRC_DIR}"
        hint_style = {**_hint_base, "color":C["muted"]}
    # Teardown buttons: disabled when nothing exists or teardown already running
    td_full_off = not has_anything or td_running or setup_running
    td_agents_off = not has_agents or td_running or setup_running
    td_sups_off = not has_sups or td_running or setup_running
    _td_full_style = {"fontWeight":"700","fontSize":"15px","padding":"12px 28px",
                      "opacity":"0.4" if td_full_off else "1"}
    return (cards, btn_disabled, btn_cls, btn_style, hint, hint_style,
            td_full_off, _td_full_style,
            td_agents_off, td_agents_off, td_sups_off)

@_bg_guard("td_running")
def _run_teardown_bg(obo_headers=None):
    """Background thread: trigger teardown job with live progress.
    obo_headers: per-user OBO headers captured at button-click time (Flask context).
    MUST be passed — _ust()['user_alias'] is global and can contain another user's alias."""
    lg = _ust().setdefault("td_log", [])
    if lg:
        lg.append("")
    _ust()["td_running"] = True
    _user_email = _get_user_from_headers(obo_headers) if obo_headers else "unknown"
    _td_jid = _get_user_job(_user_email, "teardown")
    lg.append("=== FULL TEARDOWN ===")
    lg.append(f"  Job ID: {_td_jid} (run_as={_user_email})")
    # Snapshot what exists BEFORE teardown -- every version (Baseline + Iterations), not just the Baseline set
    _vers = _ust().get("agent_versions") or {}
    _pre_by_ver = {v: len(set((a or {}).values())) for v, a in _vers.items() if a}
    _pre_ids = {sid for a in _vers.values() for sid in (a or {}).values() if sid}
    _pre_ids |= {sid for sid in (_ust().get("agents") or {}).values() if sid}
    _pre_sups = len(_ust().get("supervisors") or [])
    lg.append("Triggering teardown job...")
    try:
        _cat = user_catalog(obo_headers) if obo_headers else (_ust().get("user_catalog") or CATALOG)
        _alias = _get_user_alias(obo_headers) if obo_headers else (_ust().get("user_alias") or "unknown")
        if not _td_jid:
            lg.append("  ERROR: No teardown job found for this user.")
            _ust()["td_running"] = False
            return
        rid, err = trigger_job(_td_jid,
            notebook_params={"catalog_name": _cat, "user_alias": _alias})
        if err:
            lg.append(f"  Trigger failed: {err}")
            _ust()["td_running"] = False
            return
        lg.append(f"  Run ID: {rid} (catalog={_cat})")
        lg.append(f"  Job URL: {HOST}/#job/{_td_jid}/run/{rid}")
        lg.append("  Job starting...")
        t0 = time.time()
        last_state = ""
        while time.time() - t0 < 600:
            r = requests.get(f"{HOST}/api/2.1/jobs/runs/get?run_id={rid}",
                             headers=get_sp_headers(), timeout=15)
            if r.status_code == 200:
                state = r.json().get("state", {}).get("life_cycle_state", "UNKNOWN")
                if state != last_state:
                    elapsed = int(time.time() - t0)
                    lg.append(f"  [{elapsed}s] {state}")
                    last_state = state
                if state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                    result = r.json().get("state", {}).get("result_state", "UNKNOWN")
                    if result == "SUCCESS":
                        prev_a = len(_pre_ids)
                        prev_s = _pre_sups
                        lg.append("")
                        lg.append("=" * 50)
                        lg.append("  TEARDOWN COMPLETE")
                        lg.append("=" * 50)
                        lg.append(f"  \u2713 {prev_a} SC domain agents deleted (all versions)")
                        if _pre_by_ver:
                            lg.append("      " + ", ".join(f"{v}: {n}" for v, n in _pre_by_ver.items()))
                        lg.append(f"  \u2713 {prev_s} supervisors deleted")
                        lg.append(f"  \u2713 Catalog '{_cat}' dropped (all tables/views)")
                        lg.append(f"  \u2713 Verified by the job: no SC agents, supervisors or demo catalog left")
                        lg.append(f"  \u2139 UC Domains + Pages are created by hand on the Discover page \u2014 delete them there")
                        lg.append("")
                        lg.append("  \u2192 Click 'Run Full Setup' to rebuild")
                        _ust()["agents"] = {}
                        _ust()["agent_versions"] = {}
                        _ust()["supervisors"] = []
                        _ust()["catalog_exists"] = False
                        _ust()["table_count"] = 0
                        # Per-user reset
                        _td_alias = _get_user_alias(obo_headers) if obo_headers else (_ust().get("user_alias") or "unknown")
                        ST[f"cat_{_td_alias}"] = {}
                        # Reset one-time guards so post-teardown acts like fresh start
                        ST["_cat_checking"] = False
                        ST[f"_bench_loaded_{_td_alias}"] = False
                        ST["_bench_loading"] = False
                        _ust()["benchmarks"] = {}
                        _ust()["iterations_done"] = set()
                        _ust()["iter_it1_results"] = None
                        _ust()["iter_it2_results"] = None
                        _ust()["iter_it3_results"] = None
                        _ust()["bench_results"] = None
                        _ust()["bench_elapsed"] = 0
                        _ust()["bench_total_tokens"] = 0
                        _ust()["bench_last_ts"] = None
                        _ust()["bench_last_user"] = None
                        log(f"Teardown: SUCCESS (job {rid})")
                    else:
                        lg.append(f"")
                        lg.append(f"TEARDOWN FAILED: {result}")
                        lg.append(f"  Check: {HOST}/#job/{TEARDOWN_JOB_ID}/run/{rid}")
                        log(f"Teardown: FAILED ({result})")
                    break
            else:
                lg.append(f"  [{int(time.time()-t0)}s] Poll HTTP {r.status_code}")
            time.sleep(5)
        else:
            lg.append("  TIMEOUT: Job exceeded 10 minutes")
    except Exception as e:
        lg.append(f"  Error: {e}")
        log(f"Teardown bg error: {e}")
    finally:
        _ust()["td_running"] = False

@callback(Output("td-out","children"), Output("td-state","children", allow_duplicate=True),
          Input("td-everything","n_clicks"),
          Input("td-agents","n_clicks"), Input("td-all","n_clicks"),
          Input("td-sups","n_clicks"), prevent_initial_call=True)
def handle_teardown(n0,n1,n2,n3):
    tid = ctx.triggered_id
    if tid == "td-everything":
        if _ust().get("td_running"):
            return "Teardown already running...", no_update
        if not TEARDOWN_JOB_ID:
            return "ERROR: No teardown job configured", no_update
        lg = _ust().setdefault("td_log", [])
        if lg:
            lg.append("")
        lg.append("Launching teardown...")
        _td_obo = get_headers()  # capture per-user OBO headers BEFORE bg thread
        _spawn_bg(_run_teardown_bg, _td_obo)
        return _linkify_log_block(lg), no_update
    if tid == "td-agents":
        deleted, kept, errors = teardown_agents(keep_label="Iteration 3")
        log(f"Teardown: {len(deleted)} deleted, {len(kept)} kept, {len(errors)} errors")
        lines = [f"Deleted {len(deleted)} agents (preserved Iter 3 + Evaluator)\n"]
        for d in deleted: lines.append(f"  [-] {d}")
        for k in kept: lines.append(f"  [=] kept: {k}")
        if errors:
            lines.append(f"\nErrors ({len(errors)}):")
            for e in errors: lines.append(f"  [!] {e}")
        return "\n".join(lines), _build_state_cards()
    elif tid == "td-all":
        deleted, _, errors = teardown_agents()
        log(f"Teardown ALL: {len(deleted)} agents, {len(errors)} errors")
        lines = [f"Deleted ALL {len(deleted)} SC agents"]
        for d in deleted: lines.append(f"  [-] {d}")
        if errors:
            lines.append(f"\nErrors ({len(errors)}):")
            for e in errors: lines.append(f"  [!] {e}")
        return "\n".join(lines), _build_state_cards()
    elif tid == "td-sups":
        deleted, errors = teardown_supervisors()
        log(f"Teardown: {len(deleted)} supervisors, {len(errors)} errors")
        lines = [f"Deleted {len(deleted)} supervisors"]
        for d in deleted: lines.append(f"  [-] {d}")
        if errors:
            lines.append(f"\nErrors ({len(errors)}):")
            for e in errors: lines.append(f"  [!] {e}")
        return "\n".join(lines), _build_state_cards()
    return no_update, no_update

# Teardown progress poller
@callback(Output("td-out","children", allow_duplicate=True),
          Output("td-state","children", allow_duplicate=True),
          Output("td-poll","disabled"),
          Output("td-out-dot","className"),
          Output("btn-setup","disabled", allow_duplicate=True),
          Output("btn-setup","className", allow_duplicate=True),
          Output("setup-btn-hint","children", allow_duplicate=True),
          Input("td-poll","n_intervals"),
          prevent_initial_call=True)
def poll_teardown(n):
    lg = _ust().get("td_log", [])
    text = _linkify_log_block(lg) if lg else "Waiting..."
    running = _ust().get("td_running", False)
    if not running and lg:
        # Teardown done — enable setup button only if environment is truly empty
        _env = bool(_ust().get("agents")) or bool(_ust().get("supervisors")) or bool(ST.get(f"cat_{_get_user_alias() if _in_request() else 'unknown'}", {}).get("exists"))
        btn_dis = _env
        btn_cls = "btn" if btn_dis else "btn primary"
        hint = "Ready to run Setup" if not _env else "Environment exists — teardown first to re-run setup."
        return text, _build_state_cards(), True, "scr-dot", btn_dis, btn_cls, hint
    return text, no_update, False, "scr-dot active", no_update, no_update, no_update

# Enable td-poll when Full Teardown clicked
@callback(Output("td-poll","disabled", allow_duplicate=True),
          Input("td-everything","n_clicks"),
          prevent_initial_call=True)
def enable_td_poll(n):
    return False if n else True

SETUP_NOTEBOOKS = [
    ("01_create_catalog_schemas", None, "Create catalog + 6 domain schemas"),
    ("02_generate_demand_data", None, "Generate deterministic sales orders (seed=42)"),
    ("03_generate_inventory_data", None, "Generate inventory ledger (seed=43)"),
    ("04_generate_logistics_data", None, "Generate shipments data (seed=44)"),
    ("05_generate_supplier_data", None, "Generate supplier orders + SLAs (seed=45)"),
    ("06_create_reporting_views", None, "Create executive reporting views"),
    ("08_setup_genie_supervisor", {"warehouse_id": WAREHOUSE_ID}, "Create 5 Agents + Supervisor + Evaluator"),
]

# Clear log buttons — user-triggered only, never auto-cleared
@callback(Output("td-out","children", allow_duplicate=True),
          Output("td-out-dot","className", allow_duplicate=True),
          Input("td-clear","n_clicks"),
          prevent_initial_call=True)
def clear_td_log(n):
    if not n: raise PreventUpdate
    _ust()["td_log"] = []
    return "Ready. Click a button above to run.", "scr-dot"

@callback(Output("setup-out","children", allow_duplicate=True),
          Output("setup-out-dot","className", allow_duplicate=True),
          Input("setup-clear","n_clicks"),
          prevent_initial_call=True)
def clear_setup_log(n):
    if not n: raise PreventUpdate
    _ust()["setup_log"] = []
    return "Ready. Click a button above to run.", "scr-dot"

# Task metadata for verbose logging
SETUP_TASK_META = {
    "create_catalog": {"num": 1, "name": "01_create_catalog_schemas", "desc": "Create catalog + 6 domain schemas", "detail": "Creates catalog with schemas: demand_analysis, inventory_management, logistics_operations, supplier_procurement, reporting, action_intelligence"},
    "gen_demand": {"num": 2, "name": "02_generate_demand_data", "desc": "Generate deterministic sales orders", "detail": "~20,142 sales orders (seed=42). Covers all regions, product families, order statuses."},
    "gen_inventory": {"num": 3, "name": "03_generate_inventory_data", "desc": "Generate inventory ledger", "detail": "~1,200 inventory positions (seed=43). SKU x warehouse safety stock levels."},
    "gen_logistics": {"num": 4, "name": "04_generate_logistics_data", "desc": "Generate shipments data", "detail": "~12,808 shipments (seed=44). Carrier performance, delay tracking, regional routing."},
    "gen_supplier": {"num": 5, "name": "05_generate_supplier_data", "desc": "Generate supplier orders + SLAs", "detail": "~162 supplier POs (seed=45). Vendor SLAs, lead times, continental distribution."},
    "reporting_views": {"num": 6, "name": "06_create_reporting_views", "desc": "Create executive reporting views", "detail": "Cross-domain views: delivery performance, inventory health, revenue analysis, CoD."},
    "setup_agents": {"num": 7, "name": "08_setup_genie_supervisor", "desc": "Create 5 Agents + 2 Supervisors + Evaluator", "detail": "Demand, Inventory, Logistics, Supplier, Executive agents. Supply Chain Control Tower + MCP Supervisor endpoints. Evaluator space."},
}
SETUP_TASK_ORDER = ["create_catalog", "gen_demand", "gen_inventory", "gen_logistics",
                    "gen_supplier", "reporting_views", "setup_agents"]

@_bg_guard("setup_running")
def _run_setup_bg(obo_headers):
    """Background thread: trigger the pre-created setup job via run-now.
    The job has run_as configured, so tasks run as the logged-in user.
    OBO headers are captured at trigger time and reused throughout."""
    _ust()["setup_running"] = True
    _ust()["setup_step"] = 0
    _ust()["setup_tasks"] = {}  # {task_key: "pending"|"running"|"done"|"failed"}
    for tk in SETUP_TASK_ORDER:
        _ust()["setup_tasks"][tk] = "pending"
    lg = _ust().setdefault("setup_log", [])
    if lg:
        lg.append("")
    def say(msg): lg.append(msg); _ust()["setup_log"] = lg

    _cat = user_catalog(obo_headers)
    _alias = _get_user_alias(obo_headers)
    _ust()["user_catalog"] = _cat
    _ust()["user_alias"] = _alias

    say("=" * 68)
    say("  FULL SETUP -- Curator Workbench")
    say("=" * 68)
    _user_email = _get_user_from_headers(obo_headers)
    _setup_jid = _get_user_job(_user_email, "setup")
    say(f"  Job ID:     {_setup_jid}")
    say(f"  run_as:     {_user_email} (per-user job)")
    say(f"  Compute:    Serverless")
    say(f"  Catalog:    {_cat}")
    say(f"  Alias:      {_alias}")
    say(f"  Source:     {SRC_DIR}")
    say(f"  Tasks:      {len(SETUP_TASK_ORDER)} steps")
    say("=" * 68 + "\n")

    if not _setup_jid:
        say("ERROR: No setup job found for this user. Clone the template job with [alias] prefix.")
        _ust()["setup_running"] = False
        return

    say("[TRIGGER] Starting setup job...")
    rid, err = trigger_job(_setup_jid,
        notebook_params={"agent_label": "Baseline", "catalog_name": _cat, "user_alias": _alias})
    if err:
        say(f"[TRIGGER] FAILED: {err}")
        _ust()["setup_running"] = False
        return
    say(f"[TRIGGER] Success -- run_id={rid}")
    say(f"[TRIGGER] Job URL: {HOST}/#job/{_setup_jid}/run/{rid}\n")

    # Poll with full verbose per-task tracking
    completed = set()
    running_tasks = set()
    task_start_times = {}
    t0 = time.time()

    while time.time() - t0 < 900:
        elapsed = time.time() - t0
        r = requests.get(f"{HOST}/api/2.1/jobs/runs/get?run_id={rid}",
                         headers=get_sp_headers(), timeout=15)
        if r.status_code != 200:
            say(f"  [{elapsed:.0f}s] Poll error: HTTP {r.status_code}")
            time.sleep(8); continue
        data = r.json()
        state = data.get("state", {})
        life = state.get("life_cycle_state", "")
        result = state.get("result_state", "")

        for task in data.get("tasks", []):
            tk = task.get("task_key", "")
            tlife = task.get("state", {}).get("life_cycle_state", "")
            tresult = task.get("state", {}).get("result_state", "")
            meta = SETUP_TASK_META.get(tk, {})

            # Detect task starting
            if tk not in running_tasks and tlife == "RUNNING":
                running_tasks.add(tk)
                task_start_times[tk] = time.time()
                _ust()["setup_tasks"][tk] = "running"
                say(f"  [{elapsed:.0f}s] RUNNING  [{meta.get('num','?')}/7] {meta.get('name', tk)}")
                say(f"           {meta.get('desc', '')}")
                say(f"           {meta.get('detail', '')}")

            # Detect task completion
            if tk not in completed:
                if tresult == "SUCCESS":
                    completed.add(tk)
                    _ust()["setup_tasks"][tk] = "done"
                    dur = time.time() - task_start_times.get(tk, t0)
                    say(f"  [{elapsed:.0f}s] DONE     [{meta.get('num','?')}/7] {meta.get('name', tk)}  ({dur:.0f}s)")
                    if tk in SETUP_TASK_ORDER:
                        _ust()["setup_step"] = max(_ust().get("setup_step", 0),
                                               SETUP_TASK_ORDER.index(tk) + 1)
                elif tresult == "FAILED":
                    completed.add(tk)
                    _ust()["setup_tasks"][tk] = "failed"
                    tmsg = task.get("state", {}).get("state_message", "")
                    say(f"  [{elapsed:.0f}s] FAILED   [{meta.get('num','?')}/7] {meta.get('name', tk)}")
                    say(f"           Error: {tmsg[:300]}")

        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            total_dur = time.time() - t0
            say("")
            say("=" * 68)
            if result == "SUCCESS":
                _ust()["setup_step"] = len(SETUP_TASK_ORDER)
                for tk in SETUP_TASK_ORDER:
                    _ust()["setup_tasks"][tk] = "done"
                say(f"  SETUP COMPLETE  ({total_dur:.0f}s total)")
                say(f"  {len(SETUP_TASK_ORDER)}/{len(SETUP_TASK_ORDER)} tasks passed")
                say(f"  Catalog '{_cat}' built with all data + agents")
                say(f"  Next: run the {_NT()}-test baseline benchmark")
                # Refresh cached state: re-discover agents, supervisors, catalog
                try:
                    _ust()["agents"] = discover_agents(headers=obo_headers)
                    if _ust()["agents"]:
                        _sync_agent_warehouses(_ust()["agents"], headers=obo_headers)
                    _ust()["supervisors"] = discover_supervisors(headers=obo_headers)
                    rows = run_sql_query(
                        f"SELECT COUNT(*) as n FROM `{_cat}`.information_schema.tables",
                        headers=obo_headers
                    )
                    if rows:
                        _ust()["catalog_exists"] = True
                        _setup_tc = int(rows[0].get("n", 0))
                        _ust()["table_count"] = _setup_tc
                        ST[f"cat_{_alias}"] = {"exists": True, "tables": _setup_tc, "catalog": _cat}
                    log(f"Post-setup refresh: {len(_ust()['agents'])} agents, {len(_ust()['supervisors'])} sups, {_ust().get('table_count',0)} tables (user={_alias})")
                except Exception as e:
                    log(f"Post-setup refresh failed: {e}")
                log("Setup: SUCCESS")
            else:
                msg = state.get("state_message", "")
                err_detail = _fetch_run_error(rid, get_sp_headers())
                say(f"  SETUP FAILED  ({total_dur:.0f}s)")
                say(f"  {len(completed)}/{len(SETUP_TASK_ORDER)} tasks completed")
                say(f"  Error: {err_detail or msg}")
                log(f"Setup FAILED: {err_detail or msg}")
            say("=" * 68)
            _ust()["setup_running"] = False
            return
        time.sleep(8)

    say("\nTIMEOUT: Setup exceeded 15 minutes.")
    _ust()["setup_running"] = False

@callback(Output("setup-out","children"),
          Output("setup-poll","disabled"),
          Output("setup-steps","children"),
          Output("td-state","children", allow_duplicate=True),
          Output("setup-out-dot","className"),
          # Setup button: update from this 2s poll so there is ZERO gap
          Output("btn-setup","disabled", allow_duplicate=True),
          Output("btn-setup","className", allow_duplicate=True),
          Output("btn-setup","style", allow_duplicate=True),
          Output("setup-btn-hint","children", allow_duplicate=True),
          Output("setup-btn-hint","style", allow_duplicate=True),
          Input("btn-setup","n_clicks"),
          Input("setup-poll","n_intervals"),
          prevent_initial_call=True)
def handle_setup(n_click, n_poll):
    """Start setup ONLY on explicit button click. Poll for live updates.
    Also outputs directly to btn-setup so the button state updates on
    the 2s poll cadence (not waiting for the 10s auto_refresh)."""
    _hint_base = {"fontSize":"10px","fontFamily":"var(--mono)","marginLeft":"12px","overflow":"hidden","textOverflow":"ellipsis","whiteSpace":"nowrap","maxWidth":"380px"}
    _btn_base = {"fontWeight":"700","fontSize":"15px","padding":"12px 28px"}
    triggered = ctx.triggered_id
    # GUARD: only start setup on explicit button click with actual clicks
    if triggered == "btn-setup":
        if not n_click or n_click < 1:
            raise PreventUpdate
        if _ust().get("setup_running"):
            return ("Setup already running...", False, no_update, no_update, "scr-dot active",
                    True, "btn", {**_btn_base, "opacity":"0.4"},
                    "Setup is running…", {**_hint_base, "color":C["teal"]})
        lg = _ust().setdefault("setup_log", [])
        if lg:
            lg.append("")
        lg.append("Starting setup...")
        _ust()["setup_step"] = 0
        _ust()["setup_tasks"] = {}
        obo_h = get_headers()
        _spawn_bg(_run_setup_bg, obo_h)
        return ("\n".join(lg), False, no_update, no_update, "scr-dot active",
                True, "btn", {**_btn_base, "opacity":"0.4"},
                "Setup is running…", {**_hint_base, "color":C["teal"]})
    # GUARD: if setup was never started, don't update anything
    if not _ust().get("setup_running") and not _ust().get("setup_tasks"):
        raise PreventUpdate
    # Poll update — build step rows from per-task status
    task_display = [
        ("create_catalog", "01_create_catalog_schemas", "Create catalog + 6 domain schemas"),
        ("gen_demand", "02_generate_demand_data", "Generate deterministic sales orders (seed=42)"),
        ("gen_inventory", "03_generate_inventory_data", "Generate inventory ledger (seed=43)"),
        ("gen_logistics", "04_generate_logistics_data", "Generate shipments data (seed=44)"),
        ("gen_supplier", "05_generate_supplier_data", "Generate supplier orders + SLAs (seed=45)"),
        ("reporting_views", "06_create_reporting_views", "Create executive reporting views"),
        ("setup_agents", "08_setup_genie_supervisor", "Create 5 Genie Agents + Supervisor + Evaluator"),
    ]
    running = _ust().get("setup_running", False)
    log_lines = _ust().get("setup_log", ["Waiting..."])
    task_states = _ust().get("setup_tasks", {})
    step_rows = []
    for idx, (tk, name, desc) in enumerate(task_display):
        ts = task_states.get(tk, "")
        if ts == "done":
            status = "done"
        elif ts == "running":
            status = "running"
        elif ts == "failed":
            status = "failed"
        else:
            status = ""
        step_rows.append(StepRow(idx + 1, name, desc, status=status))
    # Refresh state cards when setup completes (NO auto-switch)
    state_cards = no_update
    if not running and task_states:
        state_cards = _build_state_cards()
    dot_cls = "scr-dot active" if running else "scr-dot"
    # Setup button state: keep disabled during running AND after completion
    # (catalog_exists / agents persist, so environment still exists)
    _env_exists = bool(_ust().get("agents")) or bool(_ust().get("supervisors")) or bool(ST.get(f"cat_{_get_user_alias() if _in_request() else 'unknown'}", {}).get("exists"))
    _btn_off = running or _env_exists
    _btn_cls = "btn" if _btn_off else "btn primary"
    _btn_sty = {**_btn_base, "opacity":"0.4" if _btn_off else "1"}
    if running:
        _hint = "Setup is running… check the output panel for live progress."
        _hint_sty = {**_hint_base, "color":C["teal"]}
    elif _env_exists:
        _hint = "Environment exists — teardown first to re-run setup."
        _hint_sty = {**_hint_base, "color":C["rose"]}
    else:
        _hint = f"Source: {SRC_DIR}"
        _hint_sty = {**_hint_base, "color":C["muted"]}
    return (_linkify_log_block(log_lines), not running, step_rows, state_cards, dot_cls,
            _btn_off, _btn_cls, _btn_sty, _hint, _hint_sty)

# ==== Page 3: Baseline ======================================================
def submit_notebook_run(notebook_name):
    """Submit one-time notebook run. SP credentials for Jobs API."""
    h = get_sp_headers()
    nb_path = SRC_DIR + "/notebooks/" + notebook_name
    payload = {
        "run_name": "Workbench: " + notebook_name,
        "tasks": [{"task_key": "run",
            "notebook_task": {"notebook_path": nb_path},
            "new_cluster": {"spark_version": "15.4.x-scala2.12",
                "num_workers": 0, "node_type_id": "Standard_DS3_v2"}}],
    }
    endpoint = HOST + "/api/2.1/jobs/runs/submit"
    r = requests.post(endpoint, headers=h, json=payload, timeout=30)
    if r.status_code in (200, 201):
        return r.json().get("run_id"), None
    return None, "HTTP " + str(r.status_code)

def _build_waiting_results():
    """Build initial results (one per active catalog test) with all WAITING status."""
    return [dict(id=aid, group=aid[0], group_name=GROUPS.get(aid[0],("",""))[0],
                 agent=ag, question=q, expected=exp, desc=desc, fix=fix,
                 verdict="WAITING", method="", closest=None)
            for aid, ag, q, exp, desc, fix in ASSUMPTIONS]

def _build_bench_grid(results=None, source="bench_results"):
    """Build the benchmark score + grid from results. `source` = ST key of these results."""
    if results is None:
        results = _build_waiting_results()
    n_total = len(results) or 1
    total_done = sum(1 for r in results if r.get("verdict") not in ("WAITING","RUNNING"))
    exact = sum(1 for r in results if r.get("verdict") == "EXACT")
    tol = sum(1 for r in results if r.get("verdict") == "TOLERANT")
    passed = exact + tol
    miss = total_done - passed if total_done > 0 else 0
    is_cmp = source.startswith("cmp_results")
    if is_cmp:  # each supervisor side has its own state while the comparison runs
        running = _ust().get("cmp_running", False) and any(r.get("verdict") in ("WAITING","RUNNING") for r in results)
    else:
        running = _ust().get("bench_running", False)
    # Score display
    if total_done == 0 and is_cmp and running:
        score_text = f"\u2014/{n_total}"
        score_sub = "Waiting for supervisor response\u2026"
    elif total_done == 0:
        score_text = f"—/{n_total}"
        score_sub = "Click Run to start"
    elif running:
        score_text = f"{passed}/{total_done}"
        score_sub = f"Testing… {total_done}/{n_total} complete"
    else:
        pct = int(100 * passed / n_total) if passed else 0
        score_text = f"{passed}/{n_total}"
        score_sub = f"{pct}% accuracy ({exact} exact + {tol} tolerant)"
    score = html.Div([
        html.Span(score_text, style={"fontFamily":"var(--display)","fontSize":"clamp(32px,5vw,52px)",
                                      "fontWeight":"800","letterSpacing":"-.03em","color":C["ink"]}),
        html.Span(score_sub, style={"color":C["muted"],"fontSize":"14px","marginLeft":"16px"}),
    ], style={"display":"flex","alignItems":"baseline","gap":"8px","marginBottom":"8px"})
    # Progress bar
    pct_done = int(100 * total_done / n_total) if total_done > 0 else 0
    fill_cls = "progress-fill" + (" done" if not running and total_done > 0 else "")
    # Color the fill based on pass rate
    if total_done > 0 and not running:
        pass_pct = int(100 * passed / n_total)
        fill_color = C["teal"] if pass_pct >= 80 else (C["indigo"] if pass_pct >= 50 else C["rose"])
        fill_style = {"width":f"{pct_done}%","background":fill_color}
    else:
        fill_style = {"width":f"{pct_done}%"}
    shimmer = [html.Div(className="progress-shimmer")] if running else []
    progress = html.Div([
        html.Div([html.Div(className=fill_cls, style=fill_style)] + shimmer, className="progress-track"),
        html.Div(f"{total_done}/{n_total} tests", style={"color":C["muted"],"fontSize":"11px",
                                                    "fontFamily":"var(--mono)","marginTop":"4px",
                                                    "textAlign":"right"}),
    ], style={"marginBottom":"16px"})
    # Last-run info + token count
    if is_cmp:  # comparison grids show the comparison run's own timestamp/duration
        last_ts = _ust().get("cmp_last_ts")
        last_user = _ust().get("cmp_last_user")
        _cmp_side = source[-1]  # "a" or "b"
        _cmp_usg = _ust().get(f"cmp_usage_{_cmp_side}") or {}
        total_tok = _cmp_usg.get("total_tokens") or ((_cmp_usg.get("input_tokens") or 0) + (_cmp_usg.get("output_tokens") or 0))
        elapsed_s = _ust().get(f"{source}_elapsed", 0)
    else:
        last_ts = _ust().get("bench_last_ts")
        last_user = _ust().get("bench_last_user")
        total_tok = _ust().get("bench_total_tokens", 0)
        elapsed_s = _ust().get("bench_elapsed", 0)
    info_parts = []
    if last_ts:
        info_parts.append(f"Last run: {last_ts} by {last_user}")
        if elapsed_s:
            info_parts.append(f" | {elapsed_s:.0f}s")
        if total_tok:
            info_parts.append(f" | {total_tok:,} tokens")
    last_run_row = html.Div(
        "".join(info_parts) if info_parts else "No benchmark run yet",
        style={"color": C["muted"], "fontSize": "12px", "fontFamily": "var(--mono)",
               "marginBottom": "12px", "opacity": "0.8"},
    )
    grid = TestGrid(results, source=source)
    return html.Div([score, progress, last_run_row, grid])


def _current_bench_stage():
    """Auto-determine which stage the benchmark should run as."""
    stages = ["Baseline", "Iteration 1", "Iteration 2", "Iteration 3"]
    benchmarks = _ust().get("benchmarks", {})
    iterations_done = _ust().get("iterations_done", set())
    # Find the latest completed iteration whose benchmark hasn't been run
    for s in stages:
        if s not in benchmarks:
            # For iterations, only allow if setup is done
            if s.startswith("Iteration"):
                iter_n = int(s.split()[-1])
                if iter_n not in iterations_done:
                    return s  # show it but button may be disabled
            return s
    return "Baseline"  # all done, allow re-running baseline

def _build_bench_next_step():
    """Show next-step banner if baseline is complete."""
    benchmarks = _ust().get("benchmarks", {})
    if "Baseline" not in benchmarks:
        return html.Div()
    score = benchmarks["Baseline"]
    return html.Div([
        html.Div([
            html.Span("\u2713 ", style={"fontWeight":"700","fontSize":"18px","color":C["teal"]}),
            html.Span(f"Baseline complete: {score}/45. ", style={"fontWeight":"600","fontSize":"14px"}),
            html.Span("Proceed to Iterations to improve the score with UC semantic features.",
                       style={"fontSize":"14px","color":C["muted"]}),
        ], style={"display":"flex","alignItems":"center","gap":"8px"}),
    ], style={"background":C["teal_bg"],"border":f"1px solid {C['teal']}","borderRadius":"12px",
              "padding":"14px 20px","marginBottom":"16px"})

# ==== Test Catalog page ======================================================
_TC_COLS = [
    dict(id="enabled", name="On", presentation="dropdown", editable=True),
    dict(id="id", name="ID", editable=False),
    dict(id="agent", name="Agent", presentation="dropdown", editable=True),
    dict(id="question", name="Question", editable=True),
    dict(id="expected", name="Expected answer", type="numeric", editable=True),
    dict(id="fix_stage", name="Fixed in", presentation="dropdown", editable=True),
    dict(id="origin", name="Origin", editable=False),
]


def _tc_rows(tests):
    """Catalog tests -> DataTable rows (all fields kept; only some are shown as columns)."""
    return [{**t, "enabled": "Yes" if t.get("enabled", True) else "No"} for t in tests]


def _tc_tiles(rows):
    rows = rows or []
    on = [r for r in rows if str(r.get("enabled", "Yes")) != "No"]
    prompt = sum(1 for r in on if r.get("origin", "").startswith("From"))
    with_sql = sum(1 for r in on if (r.get("sql") or "").strip())
    by_agent = {a: sum(1 for r in on if r.get("agent") == a) for a in TC_AGENTS}
    norm, errs = _tc_validate(rows)
    dirty = bool(errs) or norm != (_ust().get("test_catalog") or [])
    return html.Div([
        html.Div([
            Tile(len(on), "ACTIVE TESTS", "teal"),
            Tile(len(rows) - len(on), "SWITCHED OFF"),
            Tile(prompt, "FROM THE CFO PROMPT", "indigo"),
            Tile(len(on) - prompt, "CURATOR-ADDED"),
            Tile(with_sql, "WITH GROUND-TRUTH SQL"),
        ], className="tiles-row"),
        html.Div([html.Span(f"{a}: {n}", style={"fontSize": "12px", "padding": "3px 10px", "borderRadius": "999px",
                                                "background": C["soft"], "border": f"1px solid {C['line']}"})
                  for a, n in by_agent.items()] +
                 [html.Span("Unsaved changes" if dirty else "Saved \u2014 this is what the benchmarks use",
                            style={"fontSize": "12px", "fontWeight": "700", "padding": "3px 10px", "borderRadius": "999px",
                                   "marginLeft": "auto",
                                   "background": C["rose_bg"] if dirty else C["teal_bg"],
                                   "color": C["rose"] if dirty else C["teal"]})],
                 style={"display": "flex", "gap": "6px", "flexWrap": "wrap", "alignItems": "center", "marginTop": "10px"}),
    ])


def page_tests():
    tests = _ust().get("test_catalog") or []
    meta = _ust().get("tc_meta") or {}
    rows = _tc_rows(tests)
    lab = {"fontSize": "11px", "fontWeight": "700", "letterSpacing": "0.06em", "textTransform": "uppercase",
           "color": C["muted"], "margin": "10px 0 4px"}
    field = {"width": "100%", "boxSizing": "border-box", "background": C["soft"], "color": C["ink"],
             "border": f"1px solid {C['line']}", "borderRadius": "8px", "padding": "8px 10px", "fontSize": "13px"}
    src = _ust().get("tc_source", "?")
    saved_line = (f"Last saved {meta.get('updated_at')} by {meta.get('updated_by')}" if meta.get("updated_at") else "Default test set")
    return html.Div([
        html.H2("Test Catalog"),
        html.P(["The questions every benchmark asks, with the ", html.B("expected answer"), ", the ",
                html.B("ground-truth SQL"), " that produces it, and ", html.B("why the test exists"), ". ",
                "Baseline, all three Iterations and Supervisor Comparison score against exactly this list \u2014 "
                "add, edit, switch off or delete tests here, save, and the next run uses them."],
               style={"color": C["muted"], "fontSize": "14px", "lineHeight": "1.55", "marginBottom": "6px"}),
        html.Div([html.Span(f"Source: {src}", style={"fontFamily": "var(--mono)"}), html.Span(" \u2022 "),
                  html.Span(saved_line), html.Span(" \u2022 "),
                  html.Span(["Stored as ", html.Code(TEST_CATALOG_PATH.rsplit('/', 1)[-1]), " in the workbench folder"])],
                 style={"fontSize": "12px", "color": C["muted"], "marginBottom": "14px"}),
        html.Div(id="tc-tiles", children=_tc_tiles(rows)),
        html.Div([
            html.Div([
                html.Span("New test in group", style={"fontSize": "13px", "color": C["muted"]}),
                dcc.Dropdown(id="tc-new-group", value="A", clearable=False, style={"width": "190px"},
                             options=[{"label": f"{g} \u2014 {GROUPS[g][0]}", "value": g} for g in GROUPS]),
                html.Button("Add test", id="tc-add", className="btn", n_clicks=0),
                html.Span(style={"flex": "1"}),
                html.Button("Reset to defaults", id="tc-reset", className="btn", n_clicks=0,
                            title="Load the original test set into the editor (not saved until you click Save)"),
                html.Button("Discard changes", id="tc-reload", className="btn", n_clicks=0,
                            title="Reload the saved catalog"),
                html.Button("Save catalog", id="tc-save", className="btn primary", n_clicks=0),
            ], style={"display": "flex", "gap": "10px", "alignItems": "center", "flexWrap": "wrap"}),
            html.Div(id="tc-edit-status", style={"fontSize": "12.5px", "marginTop": "8px", "color": C["muted"]}),
            html.Div(id="tc-save-status", style={"fontSize": "13px", "marginTop": "4px", "fontWeight": "600"}),
        ], className="stage", style={"padding": "14px 18px", "margin": "16px 0"}),
        html.Div([
            html.Div([
                dash_table.DataTable(
                    id="tc-table", data=rows, columns=_TC_COLS, row_deletable=True, editable=True,
                    sort_action="native", filter_action="native", page_size=60,
                    dropdown={"enabled": {"options": [{"label": "Yes", "value": "Yes"}, {"label": "No", "value": "No"}],
                                          "clearable": False},
                              "agent": {"options": [{"label": a, "value": a} for a in TC_AGENTS], "clearable": False},
                              "fix_stage": {"options": [{"label": s, "value": s} for s in TC_STAGES], "clearable": False}},
                    style_table={"overflowX": "auto", "maxHeight": "640px", "overflowY": "auto"},
                    fixed_rows={"headers": True},
                    style_header={"background": C["indigo_bg"], "color": C["indigo"], "fontWeight": "700",
                                  "border": f"1px solid {C['line']}", "fontSize": "12px"},
                    style_filter={"background": C["soft"], "color": C["ink"]},
                    style_cell={"background": C["surface"], "color": C["ink"], "border": f"1px solid {C['line']}",
                                "fontFamily": "Instrument Sans, system-ui", "fontSize": "12.5px", "padding": "6px 8px",
                                "textAlign": "left", "whiteSpace": "normal", "height": "auto"},
                    style_cell_conditional=[
                        {"if": {"column_id": "question"}, "minWidth": "300px", "width": "42%"},
                        {"if": {"column_id": "id"}, "width": "56px", "fontFamily": "var(--mono)", "fontWeight": "700"},
                        {"if": {"column_id": "enabled"}, "width": "60px"},
                        {"if": {"column_id": "expected"}, "width": "120px", "textAlign": "right",
                         "fontVariantNumeric": "tabular-nums"},
                    ],
                    style_data_conditional=[
                        {"if": {"filter_query": '{enabled} = "No"'}, "opacity": "0.45"},
                        {"if": {"state": "active"}, "background": C["indigo_bg"], "border": f"1px solid {C['indigo']}"},
                    ],
                    css=[{"selector": ".dash-spreadsheet-menu", "rule": "display:none"}],
                ),
                html.Div("Click a row to see and edit its SQL and explanation. Delete with the \u00d7 on the left.",
                         style={"fontSize": "12px", "color": C["muted"], "marginTop": "6px"}),
            ], style={"flex": "3 1 620px", "minWidth": "0"}),
            html.Div([
                html.Div(id="tc-detail-title", children="Select a test",
                         style={"fontFamily": "var(--display)", "fontWeight": "700", "fontSize": "16px"}),
                html.Div("Question", style=lab),
                dcc.Textarea(id="tc-f-question", style={**field, "height": "70px"}),
                html.Div([
                    html.Div([html.Div("Expected answer", style=lab), dcc.Input(id="tc-f-expected", type="text", style=field)],
                             style={"flex": "1"}),
                    html.Div([html.Div("Origin", style=lab),
                              dcc.Dropdown(id="tc-f-origin", options=TC_ORIGINS, clearable=True)], style={"flex": "1"}),
                ], style={"display": "flex", "gap": "10px"}),
                html.Div("Where the answer lives", style=lab),
                dcc.Input(id="tc-f-source", type="text", style=field, placeholder="e.g. MV: late_delivery_rate"),
                html.Div("Fixed by (semantic asset)", style=lab),
                dcc.Input(id="tc-f-fixedby", type="text", style=field, placeholder="e.g. Metric View (Iter 2)"),
                html.Div("Why this test exists", style=lab),
                dcc.Textarea(id="tc-f-expl", style={**field, "height": "90px"}),
                html.Div(["Ground-truth SQL ", html.Span("({CATALOG} = " + CATALOG + ")", style={"textTransform": "none"})],
                         style=lab),
                dcc.Textarea(id="tc-f-sql", style={**field, "height": "190px", "fontFamily": "var(--mono)",
                                                    "fontSize": "11.5px", "whiteSpace": "pre"}),
                html.Button("Apply to this test", id="tc-apply", className="btn primary", n_clicks=0,
                            style={"marginTop": "10px"}),
                dcc.Store(id="tc-sel"),
            ], className="stage", style={"flex": "2 1 380px", "minWidth": "0", "padding": "14px 18px", "margin": "0"}),
        ], style={"display": "flex", "gap": "18px", "flexWrap": "wrap", "alignItems": "flex-start"}),
    ])


@callback(Output("tc-sel", "data"), Output("tc-detail-title", "children"),
          Output("tc-f-question", "value"), Output("tc-f-expected", "value"), Output("tc-f-origin", "value"),
          Output("tc-f-source", "value"), Output("tc-f-fixedby", "value"), Output("tc-f-expl", "value"),
          Output("tc-f-sql", "value"),
          Input("tc-table", "active_cell"), State("tc-table", "data"), prevent_initial_call=True)
def tc_select(cell, rows):
    """Load the clicked test into the detail editor (row_id = test ID, so sorting/filtering is safe)."""
    if not cell:
        raise PreventUpdate
    tid = cell.get("row_id")
    r = next((x for x in rows or [] if x.get("id") == tid), None)
    if r is None:
        raise PreventUpdate
    title = html.Span([html.Span(r["id"], style={"fontFamily": "var(--mono)", "color": C["indigo"], "marginRight": "8px"}),
                       f"{GROUPS.get(r['id'][0], ('?',))[0]} \u2022 {r.get('agent', '')} agent"])
    return (tid, title, r.get("question", ""), str(r.get("expected", "")), r.get("origin") or None,
            r.get("answer_source", ""), r.get("fixed_by", ""), r.get("explanation", ""), r.get("sql", ""))


@callback(Output("tc-table", "data"), Output("tc-edit-status", "children"),
          Input("tc-add", "n_clicks"), Input("tc-apply", "n_clicks"), Input("tc-reset", "n_clicks"),
          Input("tc-reload", "n_clicks"),
          State("tc-table", "data"), State("tc-sel", "data"), State("tc-new-group", "value"),
          State("tc-f-question", "value"), State("tc-f-expected", "value"), State("tc-f-origin", "value"),
          State("tc-f-source", "value"), State("tc-f-fixedby", "value"), State("tc-f-expl", "value"),
          State("tc-f-sql", "value"), prevent_initial_call=True)
def tc_mutate(_a, _p, _r, _l, rows, sel, grp, q, exp, origin, source, fixedby, expl, sql):
    """All edits to the table's rows (single owner of tc-table.data). Nothing is saved until Save."""
    rows = list(rows or [])
    trig = ctx.triggered_id
    if trig == "tc-add":
        grp = grp if grp in GROUPS else "A"
        nums = [int(r["id"][1:]) for r in rows if str(r.get("id", "")).startswith(grp) and str(r["id"][1:]).isdigit()]
        nid = f"{grp}{(max(nums) + 1) if nums else 1:02d}"
        rows.append(dict(id=nid, group=grp, agent=TC_AGENTS[0], question="", expected=0, metric="", fix_stage="Iter1",
                         fixed_by="", answer_source="", origin="Curator-added edge case", prompt_fragment="",
                         explanation="", sql="", enabled="Yes"))
        rows.sort(key=_tc_sort_key)
        return rows, f"Added {nid} \u2014 fill in the question, agent and expected answer, then Save. (Click the row to edit SQL and explanation.)"
    if trig == "tc-apply":
        if not sel:
            return no_update, "Select a test first (click its row)."
        for r in rows:
            if r.get("id") == sel:
                r.update(question=(q or "").strip(), expected=(exp or "").strip(), origin=origin or "",
                         answer_source=(source or "").strip(), fixed_by=(fixedby or "").strip(),
                         explanation=(expl or "").strip(), sql=(sql or "").rstrip())
                try:
                    v = float(str(r["expected"]).replace(",", "").replace("$", "").replace("%", ""))
                    r["expected"] = int(v) if v.is_integer() and "." not in str(r["expected"]) else v
                except ValueError:
                    return rows, f"{sel}: applied, but the expected answer is not a number \u2014 fix it before saving."
                return rows, f"{sel} updated in the editor \u2014 click Save catalog to make it live."
        return no_update, f"{sel} is no longer in the table."
    if trig == "tc-reset":
        try:
            with open(_TC_LOCAL, encoding="utf-8") as f:
                tests, _m = _tc_from_json(f.read())
        except Exception:
            tests = _tc_default_tests()
        return _tc_rows(tests), f"Original {len(tests)} tests loaded into the editor \u2014 click Save catalog to make them live."
    if trig == "tc-reload":
        return _tc_rows(_ust().get("test_catalog") or []), "Unsaved changes discarded."
    raise PreventUpdate


@callback(Output("tc-tiles", "children"), Input("tc-table", "data"), prevent_initial_call=True)
def tc_tiles(rows):
    return _tc_tiles(rows)


@callback(Output("tc-save-status", "children"), Output("tc-tiles", "children", allow_duplicate=True),
          Input("tc-save", "n_clicks"), State("tc-table", "data"), prevent_initial_call=True)
def tc_save(n, rows):
    if not n:
        raise PreventUpdate
    if _app_busy_now():
        return html.Span("A run is in progress \u2014 save after it finishes so the running benchmark keeps its tests.",
                         style={"color": C["rose"]}), no_update
    h = get_headers()
    ok, msg = _save_test_catalog(rows, h, _get_user_from_headers(h))
    return html.Span(msg, style={"color": C["teal"] if ok else C["rose"]}), _tc_tiles(rows)


def page_baseline():
    if not ST.get("app_ready"):
        return _build_loading_page("Loading Baseline Benchmark")
    agents = _ust().get("agents", {})
    agent_count = len(agents)
    agent_status = f"{agent_count}/5 domain agents discovered" if agent_count else "No agents found \u2014 run Setup first"
    # Pre-render all 45 tests in waiting state
    results = _ust().get("bench_results")
    initial_grid = _build_bench_grid(results)
    return html.Div([
        html.H2(f"{_NT()}-Test Baseline Benchmark"),
        html.P([
            f"Send {_NT()} individual questions to the 5 domain agents and score each response. ",
            html.Br(),
            html.Span(agent_status, style={"color":C["teal"] if agent_count >= 5 else C["rose"],
                                            "fontWeight":"600","fontSize":"13px"}),
        ], style={"color":C["muted"],"marginBottom":"20px"}),
        dcc.Store(id="bench-ep", data="agents"),
        html.Div([
            html.Div([
                html.Label("Stage", style={"fontSize":"12px","color":C["muted"],"marginRight":"12px"}),
                html.Span("Baseline", style={
                    "background":C["soft"],"border":f"1px solid {C['line']}","borderRadius":"8px",
                    "padding":"8px 16px","color":C["ink"],"fontFamily":"var(--body)",
                    "fontWeight":"600","fontSize":"14px","display":"inline-block","minWidth":"120px",
                }),
                dcc.Store(id="bench-label", data="Baseline"),
                html.Button(
                    "Re-run Baseline" if "Baseline" in _ust().get("benchmarks", {}) else f"Run {_NT()}-Test Benchmark",
                    id="btn-bench", className="btn primary",
                    style={"whiteSpace":"nowrap","padding":"10px 28px","marginLeft":"20px"},
                    disabled=agent_count < 5),
            ], style={"display":"inline-flex","alignItems":"center"}),
        ], style={"marginBottom":"20px"}),
        # Live test grid (updated by polling)
        html.Div(id="bench-grid", children=initial_grid),
        dcc.Interval(id="bench-poll", interval=2000, disabled=True),
        # Post-run next-step banner
        html.Div(id="bench-next-step", children=_build_bench_next_step()),
        # Charts section — populated after benchmark completes
        html.Div(id="bench-charts", children=_build_bench_charts(results) if results and not _ust().get("bench_running") else None),
    ])


@_bg_guard("bench_running")
def _run_bench_bg(label, obo_headers):
    """Background thread: run all active catalog tests in PARALLEL, update _ust()['bench_results'] live."""
    agents = _ust().get("agents", {})
    log(f"Benchmark starting: {len(agents)} agents available — {list(agents.keys())}")
    if not agents:
        log("ABORT benchmark: no agents in ST — post-setup refresh may have failed")
        _ust()["bench_running"] = False
        return
    _ust()["bench_running"] = True
    _ust()["bench_results"] = _build_waiting_results()
    results = _ust()["bench_results"]
    t0 = time.time()

    _token_lock = threading.Lock()
    _total_tokens = [0]

    def _test_one(idx, aid, agent_key, question, exp):
        """Run a single test against a domain agent. Thread-safe."""
        if _ust().get("bench_stop_requested"):
            results[idx]["verdict"] = "SKIPPED"
            return
        results[idx]["verdict"] = "RUNNING"
        sid = agents.get(agent_key)
        if not sid:
            results[idx]["verdict"] = "MISSING"
            results[idx]["method"] = f"no '{agent_key}' agent"
            return
        resp = ask_genie(sid, question, headers=obo_headers)
        # Accumulate token usage
        usage = resp.get("usage", {})
        tok = usage.get("total_tokens", 0) or usage.get("output_tokens", 0)
        if tok:
            with _token_lock:
                _total_tokens[0] += tok
        if resp.get("status") != "COMPLETED":
            err_snippet = resp.get("answer", "?")[:80]
            results[idx]["verdict"] = "MISSING"
            results[idx]["method"] = err_snippet
            if idx < 3:  # log first 3 failures for diagnostics
                log(f"Test {aid} ({agent_key}): status={resp.get('status')}, err={err_snippet}")
            return
        all_text = resp.get("answer", "")
        me, fe, ce = find_value_in_text(all_text, exp)
        mt, ft, ct, method = find_value_tolerant(all_text, exp)
        verdict = "EXACT" if me else ("TOLERANT" if mt else "MISSING")
        results[idx]["verdict"] = verdict
        results[idx]["method"] = method
        results[idx]["closest"] = fe or ft or ct
        results[idx]["answer"] = all_text

    # Fire all 45 tests with up to 8 parallel workers
    _ust()["bench_stop_requested"] = False
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {}
        for idx, (aid, agent_key, question, exp, desc, fix) in enumerate(ASSUMPTIONS):
            f = pool.submit(_test_one, idx, aid, agent_key, question, exp)
            futures[f] = aid
        for f in as_completed(futures):
            try:
                f.result()  # propagate exceptions
            except Exception as e:
                log(f"Test {futures[f]} error: {e}")
    stopped = _ust().get("bench_stop_requested")
    if stopped:
        log("Benchmark stopped by user")

    elapsed = time.time() - t0
    total = sum(1 for r in results if r["verdict"] in ("EXACT","TOLERANT"))
    _ust()["benchmarks"][label or "Run"] = total
    _ust()["bench_elapsed"] = elapsed
    _ust()["bench_total_tokens"] = _total_tokens[0]
    _ust()["bench_running"] = False
    _ust()["bench_last_ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _ust()["bench_last_user"] = _get_user_from_headers(obo_headers)
    log(f"Benchmark: {total}/{len(results)} in {elapsed:.0f}s, tokens={_total_tokens[0]:,}")
    # Persist to Delta table
    try:
        _save_benchmark_run(label, results, elapsed, _total_tokens[0],
                            _ust()["bench_last_user"], headers=obo_headers)
    except Exception as e:
        log(f"Benchmark persist error: {e}")

def _build_bench_charts(results):
    """Build analysis charts after benchmark completes."""
    if not results or any(r.get("verdict") in ("WAITING","RUNNING") for r in results):
        return html.Div()
    try:
        return _build_bench_charts_inner(results)
    except Exception as e:
        log(f"Chart build error: {type(e).__name__}: {e}")
        import traceback
        log(traceback.format_exc())
        return html.Div([
            html.P(f"Charts error: {e}", style={"color":C["rose"],"padding":"12px","fontFamily":"var(--mono)","fontSize":"13px"}),
        ], className="stage")

def _chart_head(title, sub):
    """Chart card heading: title + one-line reading hint."""
    return html.Div([html.Div(title, style={"fontFamily": "var(--display)", "fontWeight": "700", "fontSize": "15px",
                                            "color": C["ink"]}),
                     html.Div(sub, style={"fontSize": "12px", "color": C["muted"]})], style={"marginBottom": "4px"})


def _verdict_strip(results):
    """One-row verdict strip: a cell per test (✓ exact, ≈ tolerant, ✕ failed) with a coloured band + letter
    above each test group, and the question/answer in the hover."""
    ids = [r["id"] for r in results]
    z = [1 if r["verdict"] == "EXACT" else (0.5 if r["verdict"] == "TOLERANT" else 0) for r in results]
    sym = ["\u2713" if v == 1 else ("\u2248" if v == 0.5 else "\u2715") for v in z]
    hover = [f"<b>{r['id']}</b> \u2022 {r['verdict']}<br>{(r.get('question') or '')[:70]}"
             f"<br>Expected: {r.get('expected', r.get('gt', ''))}" for r in results]
    fig = go.Figure(go.Heatmap(
        z=[z], x=list(range(len(ids))), y=[0], zmin=0, zmax=1, showscale=False, xgap=3, ygap=0,
        colorscale=[[0, "#F43F5E"], [0.5, "#6366F1"], [1, "#14B8A6"]],
        text=[sym], texttemplate="%{text}", textfont=dict(size=13, color="#FFFFFF"),
        hovertext=[hover], hoverinfo="text"))
    shapes, ann = [], []
    i = 0
    while i < len(results):
        g = results[i].get("group") or results[i]["id"][:1]
        j = i
        while j + 1 < len(results) and (results[j + 1].get("group") or results[j + 1]["id"][:1]) == g:
            j += 1
        col = GROUPS.get(g, ("", HEX["muted"]))[1]
        shapes.append(dict(type="rect", xref="x", yref="paper", x0=i - 0.45, x1=j + 0.45, y0=1.04, y1=1.12,
                           fillcolor=col, line=dict(width=0), layer="above"))
        n_ok = sum(1 for r in results[i:j + 1] if r["verdict"] in ("EXACT", "TOLERANT"))
        ann.append(dict(x=(i + j) / 2, y=1.16, xref="x", yref="paper", yanchor="bottom", showarrow=False,
                        text=f"<b>{g}</b> {n_ok}/{j - i + 1}", font=dict(size=11, color=col)))
        i = j + 1
    fig.update_layout(template=PT, height=128, margin=dict(t=34, b=34, l=8, r=8), shapes=shapes, annotations=ann,
                      xaxis=dict(tickmode="array", tickvals=list(range(len(ids))), ticktext=ids, tickangle=-45,
                                 tickfont=dict(size=9), showgrid=False, zeroline=False),
                      yaxis=dict(showticklabels=False, showgrid=False, zeroline=False))
    return fig


def _verdict_legend():
    chip = lambda s, t, col: html.Span([html.Span(s, style={"background": col, "color": "#fff", "borderRadius": "4px",
                                                             "padding": "0 6px", "marginRight": "6px", "fontWeight": "700"}), t],
                                       style={"fontSize": "12px", "color": C["muted"], "marginRight": "16px"})
    return html.Div([chip("\u2713", "Exact", "#14B8A6"), chip("\u2248", "Tolerant", "#6366F1"),
                     chip("\u2715", "Failed", "#F43F5E")], style={"display": "flex", "flexWrap": "wrap"})


def _build_bench_charts_inner(results):
    """Inner chart builder — separated so errors are caught."""
    exact = sum(1 for r in results if r["verdict"] == "EXACT")
    tol = sum(1 for r in results if r["verdict"] == "TOLERANT")
    miss = sum(1 for r in results if r["verdict"] not in ("EXACT","TOLERANT"))
    total_pass = exact + tol
    elapsed = _ust().get("bench_elapsed", 0)
    tiles = html.Div([
        Tile(f"{total_pass}/{_NT()}","PASSED","teal" if total_pass >= _pass_bar() else None),
        Tile(exact,"EXACT","teal"), Tile(tol,"TOLERANT","indigo"),
        Tile(miss,"FAILED","rose" if miss > 5 else None),
        Tile(f"{elapsed:.0f}s","ELAPSED"),
    ], className="tiles-row")
    # Heatmap strip: one cell per test, grouped with coloured group bands + symbols in each cell
    fig_heat = _verdict_strip(results)
    # Horizontal bar by group
    gd = []
    for g in GROUPS:
        gname, gcolor = GROUPS.get(g, ("",HEX["muted"]))
        items = [r for r in results if r.get("group")==g]
        if not items:
            continue
        passed = sum(1 for r in items if r["verdict"] in ("EXACT","TOLERANT"))
        gd.append((f"{g}: {gname}", passed, len(items), gcolor))
    fig_grp = _hprogress(gd)
    # Donut
    # Drop zero-value slices: Plotly draws their labels outside the ring, where they get clipped
    fig_pie = _donut([("Exact", exact, "#14B8A6"), ("Tolerant", tol, "#6366F1"), ("Failed", miss, "#F43F5E")],
                     f"{total_pass}/{len(results)}",
                     f"{round(100 * total_pass / len(results)) if results else 0}% passed")
    # Agent perf
    ast2 = {}
    for r in results:
        ag = r.get("agent","?")
        ast2.setdefault(ag,{"pass":0,"total":0})
        ast2[ag]["total"] += 1
        if r["verdict"] in ("EXACT","TOLERANT"): ast2[ag]["pass"] += 1
    agl = sorted(ast2.keys())
    fig_ag = _bar3d([a.title() for a in agl],
                    [("Tests asked", [ast2[a]["total"] for a in agl], "slate"),
                     ("Passed", [ast2[a]["pass"] for a in agl], "teal")],
                    height=360, ymax=max([ast2[a]["total"] for a in agl] + [1]), ytitle="Tests",
                    labels={"Passed": [f"{ast2[a]['pass']} ({round(100 * ast2[a]['pass'] / ast2[a]['total'])}%)"
                                       if ast2[a]["total"] else "0" for a in agl]})
    return html.Div([
        tiles,
        html.Div([html.Div([_chart_head("Every test at a glance", "One cell per test, grouped by test group -- hover a cell for the question"),
                            _verdict_legend()], style={"display":"flex","justifyContent":"space-between",
                                                      "alignItems":"flex-end","flexWrap":"wrap","gap":"8px"}),
                  dcc.Graph(figure=fig_heat,config={"displayModeBar":False})],className="stage rob-chart",
                 style={"marginBottom":"16px"}),
        html.Div([
            html.Div([_chart_head("Per-Group Results", "Passed out of asked, per test group"),
                       dcc.Graph(figure=fig_grp,config={"displayModeBar":False})],
                     className="stage rob-chart",style={"flex":"3","minWidth":"0"}),
            html.Div([_chart_head("Verdict Breakdown", "Exact = 2-decimal match, Tolerant = rounding match"),
                       dcc.Graph(figure=fig_pie,config={"displayModeBar":False})],
                     className="stage rob-chart",style={"flex":"2","minWidth":"280px"}),
        ],style={"display":"flex","gap":"20px","flexWrap":"wrap"}),
        html.Div([_chart_head("Agent Performance", "Tests asked of each domain agent and how many it got right"),
                   dcc.Graph(figure=fig_ag,config={"displayModeBar":False})],className="stage rob-chart"),
    ])


@callback(Output("bench-grid","children"),
          Output("bench-poll","disabled"),
          Output("bench-charts","children"),
          Output("btn-bench","disabled"),
          Output("bench-next-step","children"),
          Output("btn-bench","children"),
          Output("btn-bench","className"),
          Output("btn-bench","style"),
          Input("btn-bench","n_clicks"),
          Input("bench-poll","n_intervals"),
          State("bench-label","data"),
          prevent_initial_call=True)
def handle_bench(n_click, n_poll, label):
    triggered = ctx.triggered_id
    _run_style = {"whiteSpace":"nowrap","padding":"10px 28px","marginLeft":"20px"}
    _stop_style = {**_run_style, "opacity":"1"}
    if triggered == "btn-bench":
        if not n_click or n_click < 1:
            raise PreventUpdate
        if _ust().get("bench_running"):
            # User clicked Stop — request cancellation
            _ust()["bench_stop_requested"] = True
            log("Benchmark: user requested stop")
            return (no_update, no_update, no_update, True, no_update,
                    "Stopping...", "btn", {**_run_style, "opacity":"0.5"})
        obo_h = get_headers()
        _spawn_bg(_run_bench_bg, label, obo_h)
        return (_build_bench_grid(_ust().get("bench_results")), False, html.Div(), False,
                no_update, "\u25a0 Stop Run", "btn rose", _stop_style)
    # Polling update
    results = _ust().get("bench_results")
    if not results:
        raise PreventUpdate
    running = _ust().get("bench_running", False)
    charts = _build_bench_charts(results) if not running else no_update
    if running:
        return (_build_bench_grid(results), False, charts, False,
                no_update, "\u25a0 Stop Run", "btn rose", _stop_style)
    # Done — restore original button
    btn_label = "Re-run Baseline" if "Baseline" in _ust().get("benchmarks", {}) else f"Run {_NT()}-Test Benchmark"
    return (_build_bench_grid(results), True, charts, False,
            _build_bench_next_step(), btn_label, "btn primary", _run_style)


# ==== Page 4: Iterations ====================================================
ITER_INFO = [
    ("Iteration 1", "Column Comments + Example SQL",
     "Add business-friendly column descriptions, table comments, and example SQL queries.",
     [("Column Comments","ALTER TABLE ALTER COLUMN COMMENT"),
      ("Table Comments","ALTER TABLE SET TBLPROPERTIES"),
      ("Example SQL Queries","serialized_space API"),
      ("Benchmark Questions","serialized_space API")],
     "A02, A03, B03, D04, D06, F02, F06, H01, H02"),
    ("Iteration 2", "Metric Views + CoD View",
     "Create pre-computed metric views for delivery, revenue, inventory, supplier, and Cost of Disruption.",
     [("4 UC Metric Views","CREATE VIEW WITH METRICS"),
      ("Open Knowledge View","Cross-domain CoD view"),
      ("UC Governed Tags","ALTER TABLE SET TAGS"),
      ("Schema Domain Tags","ALTER SCHEMA SET TAGS")],
     "E03, H05, H06, H07, P03"),
    ("Iteration 3", "SQL Functions + Fiscal Targets + UC Domain",
     "Add SQL threshold functions, fiscal_targets table, UC Domain + Pages, entity matching.",
     [("UC Domain","Discover page (UI)"),
      ("UC Pages (7 pages)","Discover page (UI)"),
      ("Reference Table","fiscal_targets"),
      ("SQL Functions (5)","CREATE FUNCTION"),
      ("Entity Matching","serialized_space API")],
     "G01, G02, P01, P02, P04"),
]

def _build_iter_score_label(iter_idx):
    """Show actual score if stage is complete, otherwise 'Upcoming'."""
    benchmarks = _ust().get("benchmarks", {})
    stage_keys = ["Baseline", "Iteration 1", "Iteration 2", "Iteration 3"]
    prev_key = stage_keys[iter_idx]
    prev_score = benchmarks.get(prev_key)
    curr_key = stage_keys[iter_idx + 1]
    curr_score = benchmarks.get(curr_key)
    if prev_score is not None and curr_score is not None:
        return html.Span(f"Score: {prev_score} \u2192 {curr_score}/{_NT()}",
                         style={"color":C["teal"],"fontSize":"13px","fontFamily":"var(--mono)","fontWeight":"600"})
    elif prev_score is not None:
        return html.Span(f"Baseline: {prev_score}/{_NT()} \u2014 run this iteration next",
                         style={"color":C["indigo"],"fontSize":"13px","fontFamily":"var(--mono)","fontWeight":"600"})
    else:
        return html.Span("Upcoming",
                         style={"color":C["muted"],"fontSize":"13px","fontFamily":"var(--mono)","fontWeight":"600"})

def _build_iter_progression_chart():
    """4-stage progression as 3D blocks, one colour per stage; stages not run yet show a flat 'Upcoming' slab."""
    labels_ = ["Baseline", "Iter 1", "Iter 2", "Iter 3"]
    keys = ["Baseline", "Iteration 1", "Iteration 2", "Iteration 3"]
    tones = ["rose", "amber", "indigo", "teal"]
    bm = _ust().get("benchmarks", {})
    vals = [bm.get(k) or 0 for k in keys]
    txt = [f"{bm[k]}/{_NT()} ({round(100 * bm[k] / _NT())}%)" if bm.get(k) is not None else "Upcoming" for k in keys]
    return _bar3d(labels_, [("Tests passing", vals, tones)], height=300, ymax=_NT(), labels={"Tests passing": txt},
                  ytitle="Tests passing")


def page_iterations():
    if not ST.get("app_ready"):
        return _build_loading_page("Loading Iterations")
    has_baseline = bool(_ust().get("agents"))
    iter_running = _ust().get("iter_running")
    iter_done = _ust().get("iterations_done", set())
    can_run = [False, False, False]
    all_done = {"it1", "it2", "it3"}.issubset(iter_done)
    if has_baseline and not iter_running:
        if "it1" not in iter_done: can_run[0] = True
        elif "it2" not in iter_done: can_run[1] = True
        elif "it3" not in iter_done: can_run[2] = True
        elif all_done: can_run[2] = True  # allow re-running Iteration 3 after all done

    fig = _build_iter_progression_chart()

    cards = []
    for i, (title, subtitle, desc, features, fixes) in enumerate(ITER_INFO):
        key = f"it{i+1}"
        if key == iter_running: badge_text, badge_color = "Running...", C["indigo"]
        elif key in iter_done: badge_text, badge_color = "Completed", C["teal"]
        elif can_run[i]: badge_text, badge_color = "Ready", C["teal"]
        elif not has_baseline: badge_text, badge_color = "Needs baseline", C["muted"]
        else: badge_text, badge_color = f"Waiting for Iter {i}", C["muted"]

        # UC features list
        feat_chips = html.Div([
            html.Span(f.strip(), style={"fontSize":"11px","padding":"3px 10px","borderRadius":"12px",
                                        "border":f"1px solid {C['line']}","color":C["muted"],
                                        "fontFamily":"var(--mono)"})
            for f, _ in features
        ], style={"display":"flex","flexWrap":"wrap","gap":"6px","marginTop":"8px"})

        # Iteration grid — reconstruct state on tab switch
        iter_results = ST.get(f"iter_{key}_results")
        if key == iter_running and _ust().get("iter_setup_phase") and _ust().get("iter_manual_step") and key == "it3":
            grid_content = _build_manual_step_ui(_ust().get("iter3_agent_spaces", {}))  # waiting for user
        elif key == iter_running and _ust().get("iter_setup_phase"):
            grid_content = _build_iter_status()  # live progress for running iteration
        elif key == _ust().get("iter_stopped_key") and not iter_running:
            grid_content = _build_iter_status()  # preserved log of a user-stopped run
        elif iter_results:
            grid_content = _build_bench_grid(iter_results, source=f"iter_{key}_results")
        else:
            grid_content = html.Div()

        card = html.Div([
            html.Div([
                html.Div([
                    html.H3(title, style={"marginBottom":"0"}),
                    html.Span(subtitle, style={"color":C["muted"],"fontSize":"13px","fontFamily":"var(--mono)"}),
                ], style={"flex":"1"}),
                html.Span(badge_text, style={"fontSize":"12px","fontWeight":"600","padding":"4px 14px",
                                             "borderRadius":"20px","border":f"1px solid {badge_color}",
                                             "color":badge_color}),
            ], style={"display":"flex","alignItems":"center","gap":"12px"}),
            html.P(desc, style={"color":C["muted"],"fontSize":"13px","marginTop":"6px","marginBottom":"0"}),
            feat_chips,
            html.Div([
                _build_iter_score_label(i),
                html.Span(f"Target fixes: {fixes}", style={"color":C["muted"],"fontSize":"11px",
                                                     "fontFamily":"var(--mono)"}),
            ], style={"display":"flex","gap":"16px","alignItems":"center","marginTop":"10px"}),
            html.Div([
                html.Button(
                    f"\u25a0 Stop {title}" if iter_running == f"it{i+1}"
                    else (f"Re-run {title}" if key in iter_done else f"Apply {title}"),
                    id=f"btn-it{i+1}",
                    className="btn rose" if iter_running == f"it{i+1}" else ("btn primary" if can_run[i] else "btn"),
                    disabled=False if iter_running == f"it{i+1}" else not can_run[i],
                    style={"opacity":"1" if (can_run[i] or iter_running == f"it{i+1}") else "0.4","padding":"10px 28px"}),
                html.Button("\u2630 View Setup Log", id={"type": "setuplog-btn", "index": key}, n_clicks=0,
                            className="btn",
                            title="Open the setup log of the latest run of this iteration",
                            disabled=not _has_setup_log(key),
                            style=_setuplog_btn_style(_has_setup_log(key))),
            ], style={"marginTop":"14px","display":"flex","gap":"10px","alignItems":"center"}),
            # Test grid area (same template as baseline) -- spaced away from the buttons
            html.Div(id=f"it{i+1}-grid", children=grid_content, style={"marginTop":"16px"}),
            # Charts area — rebuild from completed results on tab switch
            html.Div(id=f"it{i+1}-charts", children=(
                _build_bench_charts(iter_results)
                if iter_results and not any(r.get("verdict") in ("WAITING","RUNNING") for r in iter_results)
                else html.Div()
            )),
        ], className="stage")
        cards.append(card)

    return html.Div([
        html.H2("Iteration Workflow"),
        html.P("Progressive UC semantic improvements across 3 iterations.",
               style={"color":C["muted"],"marginBottom":"20px"}),
        html.Div(id="iter-progression-chart", children=[dcc.Graph(figure=fig, config={"displayModeBar":False})], className="stage rob-chart"),
        *cards,
        # Shared polling interval for iteration benchmarks
        dcc.Interval(id="iter-bench-poll", interval=2000, disabled=not bool(_ust().get("iter_running"))),
    ])

def discover_agents_by_label(label, headers=None):
    """Discover SC agents matching a specific iteration label.
    Uses OBO for production correctness. Accepts pre-captured headers for thread safety."""
    h = headers or get_headers()
    alias = _get_user_alias(h)
    spaces, _err = _list_all_genie_spaces(h)  # all pages (API returns 20 per page)
    if label in AGENT_VERSIONS:
        # Strict: pass alias so only user-scoped titles match
        return dict(_group_agent_versions(spaces, alias=alias).get(label, {}))
    found = {}
    for s in spaces:
        title = s.get("title", "")
        if "SC" not in title or label not in title:
            continue
        # Strict: only match user-scoped agents
        if alias and alias != "unknown" and f"({alias})" not in title:
            continue
        sid = s["space_id"]
        if "Demand" in title: found["demand"] = sid
        elif "Inventory" in title: found["inventory"] = sid
        elif "Logistics" in title: found["logistics"] = sid
        elif "Supplier" in title: found["supplier"] = sid
        elif "Executive" in title: found["executive"] = sid
    return found

@_bg_guard(on_fail=_iter_bench_fail)
def _run_iter_bench_bg(iter_key, label, agents, obo_headers):
    """Background thread: run all active catalog tests against iteration agents."""
    log(f"Iter benchmark starting ({label}): {len(agents)} agents — {list(agents.keys())}")
    if not agents:
        log(f"ABORT {label} benchmark: no agents supplied")
        _ust()["iter_bench_active"] = None
        _ust()["iter_running"] = None
        _iter_finished()
        return
    my_tok = _ust().get("iter_run_token")
    ST[f"iter_{iter_key}_results"] = _build_waiting_results()
    _ust()["iter_bench_active"] = iter_key
    results = ST[f"iter_{iter_key}_results"]
    t0 = time.time()

    _token_lock = threading.Lock()
    _total_tokens = [0]

    def _test_one(idx, aid, agent_key, question, exp):
        if not _iter_is_current(my_tok):
            return  # user stopped -- skip remaining tests
        results[idx]["verdict"] = "RUNNING"
        sid = agents.get(agent_key)
        if not sid:
            results[idx]["verdict"] = "MISSING"
            results[idx]["method"] = f"no '{agent_key}' agent"
            return
        resp = ask_genie(sid, question, headers=obo_headers)
        # Accumulate token usage (thread-safe)
        usage = resp.get("usage", {})
        tok = usage.get("total_tokens", 0) or usage.get("output_tokens", 0)
        if tok:
            with _token_lock:
                _total_tokens[0] += tok
        if resp.get("status") != "COMPLETED":
            err_snippet = resp.get("answer", "?")[:80]
            results[idx]["verdict"] = "MISSING"
            results[idx]["method"] = err_snippet
            if idx < 3:  # log first 3 failures for diagnostics
                log(f"Test {aid} ({agent_key}): status={resp.get('status')}, err={err_snippet}")
            return
        all_text = resp.get("answer", "")
        me, fe, ce = find_value_in_text(all_text, exp)
        mt, ft, ct, method = find_value_tolerant(all_text, exp)
        verdict = "EXACT" if me else ("TOLERANT" if mt else "MISSING")
        results[idx]["verdict"] = verdict
        results[idx]["method"] = method
        results[idx]["closest"] = fe or ft or ct
        results[idx]["answer"] = all_text

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {}
        for idx, (aid, agent_key, question, exp, desc, fix) in enumerate(ASSUMPTIONS):
            f = pool.submit(_test_one, idx, aid, agent_key, question, exp)
            futures[f] = aid
        for f in as_completed(futures):
            try: f.result()
            except Exception as e: log(f"Iter bench {futures[f]} error: {e}")

    elapsed = time.time() - t0
    if not _iter_is_current(my_tok):
        log(f"{label} benchmark: stopped by user -- results discarded, not saved")
        return
    total = sum(1 for r in results if r["verdict"] in ("EXACT","TOLERANT"))
    _ust()["benchmarks"][label] = total
    _ust()["iter_bench_active"] = None
    _ust().setdefault("iterations_done", set()).add(iter_key)
    _ust()["iter_running"] = None
    _iter_finished()
    _ust()["bench_last_ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _ust()["bench_last_user"] = _get_user_from_headers(obo_headers)
    _ust()["bench_total_tokens"] = _total_tokens[0]
    _ust()["bench_elapsed"] = elapsed
    log(f"{label} benchmark: {total}/{_NT()} in {elapsed:.0f}s, tokens={_total_tokens[0]:,}")
    # Persist to Delta table
    try:
        _save_benchmark_run(label, results, elapsed, _total_tokens[0],
                            _ust()["bench_last_user"], headers=obo_headers)
    except Exception as e:
        log(f"Iter benchmark persist error: {e}")

_ITER_CLONE_STEPS = ["Cloning 5 domain agents"] + [f"Cloning agent {i}/5" for i in range(1, 6)]
_ITER_SUP_STEPS = ["Updating Supervisor via job", "Supervisor job run_id",
                   ("Supervisor updated", "Supervisor NOT updated")]
# Ordered setup milestones per iteration -- substrings of the step messages each iteration logs.
_ITER_PROGRESS_MILESTONES = {
    "it1": ["Discovering Baseline agents"] + _ITER_CLONE_STEPS + [
        "Supervisor refresh will be run", "Applying table + column comments", "Table comments:"]
        + [f"Column comments {i}/7" for i in range(1, 8)]
        + ["Patching 5 agents"] + [f"Patching agent {i}/5" for i in range(1, 6)]
        + [f"Example SQLs {i}/3" for i in range(1, 4)] + [f"Benchmarks {i}/3" for i in range(1, 4)]
        + ["Iteration 1 features complete", "Done:"] + _ITER_SUP_STEPS,
    "it2": ["Discovering Iteration 1 agents"] + _ITER_CLONE_STEPS + [
        "Supervisor refresh will be run", "Creating 4 UC Metric Views",
        "delivery_performance_by_region", "revenue_comparison_by_region",
        "inventory_safety_stock_metrics", "supplier_performance_by_continent",
        "Creating Cost of Disruption view", "Applying UC Governed Tags", "Applying schema domain tags",
        "Adding metric views + CoD", "Iteration 2 features complete", "Done:"] + _ITER_SUP_STEPS,
    "it3": ["Discovering Iteration 2 agents"] + _ITER_CLONE_STEPS + [
        "Supervisor refresh will be run", "Creating fiscal_targets reference table",
        "Adding fiscal_targets to Executive agent", "Creating 5 SQL threshold functions",
        "get_critical_delay_shipments()", "get_critical_supply_positions()",
        "get_critical_accuracy_forecasts()", "get_critical_quality_orders()",
        "get_critical_disruption_regions()", "Supervisor enhancement will be run",
        "Adding provenance instructions", "Iteration 3 automated setup complete"] + _ITER_SUP_STEPS,
}


def _iter_setup_pct(log_lines, key=None, done=None):
    """Setup progress 0-100 from the furthest milestone reached in the log; 100 only when setup is done."""
    if done if done is not None else (_ust().get("iter_setup_done") or _ust().get("iter_manual_step")):
        return 100
    steps = _ITER_PROGRESS_MILESTONES.get(key or _ust().get("iter_running") or _ust().get("iter_stopped_key"), [])
    if not steps:
        return 2
    reached = -1
    for idx, step in enumerate(steps):
        alts = step if isinstance(step, tuple) else (step,)
        if any(a in line for line in log_lines for a in alts):
            reached = idx
    return max(2, min(99, round(100 * (reached + 1) / len(steps))))


_LOG_URL_RX = re.compile(r"(https?://\S+)")

def _linkify_log(line, text_color=None):
    """Turn URLs in a log line into clickable links; return a list of Dash children."""
    parts = _LOG_URL_RX.split(line)
    if len(parts) == 1:
        return line  # no URL — plain string
    children = []
    for i, seg in enumerate(parts):
        if i % 2 == 1:  # matched URL
            children.append(html.A(seg, href=seg, target="_blank",
                                   style={"color": C["teal"], "textDecoration": "underline",
                                          "wordBreak": "break-all"}))
        elif seg:
            children.append(seg)
    return children

def _linkify_log_block(lines):
    """Turn a list of log lines into Dash children with clickable URLs (preserving newlines)."""
    children = []
    for i, ln in enumerate(lines):
        if i > 0:
            children.append("\n")
        content = _linkify_log(ln) if "://" in ln else ln
        if isinstance(content, list):
            children.extend(content)  # flatten — Dash can't handle nested lists
        else:
            children.append(content)
    return children or "Ready. Click a button above to run."

def _setup_log_rows(log_lines):
    """Render setup log lines as step rows (dot + text; errors/warnings red, last line highlighted)."""
    children = []
    for i, line in enumerate(log_lines):
        is_last = (i == len(log_lines) - 1)
        is_err = "ERROR" in line.upper() or "WARNING" in line.upper() or "STOPPED BY USER" in line.upper()
        dot_color = C["rose"] if is_err else (C["indigo"] if is_last else C["teal"])
        text_color = C["rose"] if is_err else (C["indigo"] if is_last else C["ink"])
        content = _linkify_log(line, text_color)
        children.append(html.Div([
            html.Span(className="scr-dot active" if is_last else "scr-dot",
                       style={"width":"6px","height":"6px","marginRight":"8px","display":"inline-block",
                              "borderRadius":"50%","background":dot_color}),
            html.Span(content, style={"fontFamily":"var(--mono)","fontSize":"12px","color":text_color,
                                      "fontWeight":"700" if is_err else "400"}),
        ], style={"display":"flex","alignItems":"center","padding":"4px 0"}))
    return children


def _build_iter_status():
    """Build live status panel during iteration setup phase with elapsed timer."""
    log_lines = _ust().get("iter_setup_log", [])
    if not log_lines:
        log_lines = ["Starting..."]
    t0 = _ust().get("iter_setup_t0", 0)
    elapsed = int((_ust().get("iter_setup_t_end") or time.time()) - t0) if t0 else 0
    elapsed_str = f"{elapsed // 60}m {elapsed % 60:02d}s" if elapsed >= 60 else f"{elapsed}s"
    children = _setup_log_rows(log_lines)
    # Header row with title + elapsed badge
    header = html.Div([
        html.Span("Applying UC Semantic Features",
                   style={"fontFamily":"var(--display)","fontSize":"18px","fontWeight":"700","color":C["ink"]}),
        html.Span(elapsed_str,
                   style={"fontFamily":"var(--mono)","fontSize":"13px","fontWeight":"600",
                          "color":C["indigo"],"padding":"2px 12px","borderRadius":"12px",
                          "border":f"1px solid {C['indigo']}","marginLeft":"12px"}),
    ], style={"display":"flex","alignItems":"center","marginBottom":"12px"})
    return html.Div([
        header,
        html.Div(children, style={"padding":"8px 0","maxHeight":"260px","overflowY":"auto"}),
        html.Div([
            html.Div(className="progress-fill", style={"width":f"{_iter_setup_pct(log_lines)}%"}),
            html.Div(className="progress-shimmer"),
        ], className="progress-track", style={"marginTop":"12px"}),
    ], className="stage", style={"marginBottom":"16px"})

def _start_iter_bench(iter_key, label, obo_headers):
    """After iteration logic completes, discover new agents and benchmark."""
    agents = ST.pop("_iter_cloned_spaces", None) or discover_agents_by_label(label, headers=obo_headers)
    if len(agents) < 5:
        log(f"{label}: only {len(agents)} agents found, need 5")
        _ust()["iter_bench_active"] = None
        _ust()["iter_running"] = None
        _iter_finished()
        return
    log(f"{label}: {len(agents)} agents discovered, starting benchmark")
    _spawn_bg(_run_iter_bench_bg, iter_key, label, agents, obo_headers)

def _iter_finished():
    """Mark that an iteration run ended (success, error, or stop) so the page auto-refreshes."""
    _ust()["_iter_ui_epoch"] = _ust().get("_iter_ui_epoch", 0) + 1


def _iter_is_current(tok):
    """True if this background run has not been stopped/superseded by the user."""
    return tok is not None and _ust().get("iter_run_token") == tok


def _iter_begin(key, label):
    """Common start-of-run state for an iteration. Returns this run's token."""
    tok = uuid.uuid4().hex
    _ust()["iter_run_token"] = tok
    _ust()["iter_running"] = key
    _ust()["iter_setup_phase"] = True
    _ust()["iter_setup_error"] = False
    _ust()["iter_setup_done"] = False
    _ust()["iter_manual_step"] = False
    _ust()["_it3_gate_rendered"] = False
    _ust()["iter_stopped_key"] = None
    _ust()["iter_setup_t_end"] = None
    _ust()["iter_setup_log"] = [f"Starting {label}..."]
    _ust()["iter_setup_t0"] = time.time()
    # Preserved copy for the "View Setup Log" modal: same list object as the live log
    ST[f"iter_{key}_setup_log"] = _ust()["iter_setup_log"]
    ST[f"iter_{key}_setup_meta"] = {"label": label, "t0": _ust()["iter_setup_t0"], "t_end": None, "completed": False}
    return tok


def _iter_mark_setup_end(key, completed=False):
    """Record when this iteration's setup phase ended (for the preserved-log modal)."""
    meta = ST.get(f"iter_{key}_setup_meta")
    if meta and not meta.get("t_end"):
        meta["t_end"] = time.time()
        meta["completed"] = completed


def _iter_stop_now(key):
    """User clicked Stop: release the UI immediately. The bg thread is not killed
    (Python can't kill threads) -- it is orphaned and exits at its next checkpoint
    without touching app state, because its token no longer matches.
    Any running supervisor job is cancelled immediately."""
    _ust()["iter_run_token"] = None
    # Cancel any running supervisor refresh job
    _sup_rid = ST.pop("_iter_sup_rid", None)
    if _sup_rid:
        try:
            _cancel_job_run(_sup_rid, get_sp_headers())
            log(f"{key}: cancelled supervisor job run {_sup_rid} on user stop")
        except Exception as e:
            log(f"{key}: failed to cancel supervisor job {_sup_rid}: {e}")
    _ust().setdefault("iter_setup_log", []).append(
        "\u26d4 Stopped by user (any in-flight API call finishes in the background; no further steps run)")
    _ust()["iter_setup_t_end"] = time.time()
    _iter_mark_setup_end(key)
    _ust()["iter_stopped_key"] = key
    _ust()["iter_setup_phase"] = False
    _ust()["iter_setup_done"] = False
    _ust()["iter_setup_error"] = False
    _ust()["iter_manual_step"] = False
    _ust()["iter_bench_active"] = None
    _ust()["iter_running"] = None
    ST.pop("_iter_cloned_spaces", None)
    if ST.get(f"iter_{key}_results") and any(r.get("verdict") in ("WAITING", "RUNNING") for r in ST[f"iter_{key}_results"]):
        ST[f"iter_{key}_results"] = None  # discard partial benchmark
    log(f"{key}: stopped by user -- UI released")
    _iter_finished()


def _make_log_step(tok):
    def _log_step(msg):
        if not _iter_is_current(tok):
            raise InterruptedError("Stopped by user")
        _ust()["iter_setup_log"].append(msg)
    return _log_step


@_bg_guard(on_fail=_iter_bench_fail)
def _run_iter_setup_bg(tok, key, run_fn, obo_h, on_success):
    """Shared bg runner for Iter 1/2/3 setup. Never writes app state once stopped."""
    _log_step = _make_log_step(tok)
    try:
        _alias = _get_user_alias(obo_h)
        _cat = user_catalog(obo_h)
        result = run_fn(
            host=HOST, headers=obo_h, catalog=_cat,
            warehouse_id=WAREHOUSE_ID,
            discover_agents_fn=lambda label: discover_agents_by_label(label, headers=obo_h),
            log_fn=_log_step, user_alias=_alias,
        )
        if not _iter_is_current(tok):
            log(f"{key}: finished after user stop -- result discarded"); return
        if isinstance(result, tuple):
            _text, new_spaces = result
            on_success(new_spaces, _log_step, tok, obo_h)
            _iter_mark_setup_end(key, completed=True)
        else:
            _log_step(f"ABORTED: {key} setup failed (see log above)")
            log(f"{key} ABORTED: setup returned error string")
            _ust()["iter_setup_error"] = True
            _iter_mark_setup_end(key)
    except InterruptedError:
        log(f"{key}: bg thread exited after user stop")
    except Exception as e:
        if not _iter_is_current(tok):
            log(f"{key}: error after user stop (ignored): {e}"); return
        _ust()["iter_setup_log"].append(f"ERROR: {e}")
        log(f"{key} ERROR: {e}")
        _ust()["iter_setup_error"] = True
        _iter_mark_setup_end(key)



def _it1_success(new_spaces, _log_step, tok, obo_h=None):
    _log_step(f"Done: {len(new_spaces)} agents configured")
    _run_supervisor_refresh_job("it1", tok, _log_step, obo_headers=obo_h)
    log(f"Iter1 features applied: {len(new_spaces)} agents")
    _ust()["iter_setup_done"] = True


def _it2_success(new_spaces, _log_step, tok, obo_h=None):
    _ust()["_iter_cloned_spaces"] = new_spaces
    _log_step(f"Done: {len(new_spaces)} agents configured with metric views + CoD")
    _run_supervisor_refresh_job("it2", tok, _log_step, obo_headers=obo_h)
    log(f"Iter2 features applied: {len(new_spaces)} agents")
    _ust()["iter_setup_done"] = True


def _it3_success(new_spaces, _log_step, tok, obo_h=None):
    _ust()["_iter_cloned_spaces"] = new_spaces
    _ust()["iter3_agent_spaces"] = new_spaces  # For manual step UI
    _run_supervisor_refresh_job("it3", tok, _log_step, obo_headers=obo_h)
    _log_step("Cloned agents:")
    for a_name, a_sid in new_spaces.items():
        _log_step(f"  • {a_name}: {a_sid[:20]}...")
    _log_step("⚠ MANUAL STEP REQUIRED: Add SQL functions to each agent via Genie UI")
    _log_step("Then click 'Continue Iteration 3 (run benchmark)' in the panel below.")
    _ust()["iter_manual_step"] = True


_ITER_RUNNERS = {

    "it1": ("Iteration 1", run_iteration_1, _it1_success),
    "it2": ("Iteration 2", run_iteration_2, _it2_success),
    "it3": ("Iteration 3", run_iteration_3, _it3_success),
}


def _iter_button_click(key):
    """Start or stop an iteration. Returns (grid, btn_label, poll_disabled, nav_disabled)."""
    label, run_fn, on_success = _ITER_RUNNERS[key]
    if _ust().get("iter_running") == key:
        _iter_stop_now(key)
        done_label = f"Re-run {label}" if key in _ust().get("iterations_done", set()) else f"Apply {label}"
        return _build_iter_status(), done_label, True, [False] * NAV_TAB_COUNT
    if _ust().get("iter_running"):
        raise PreventUpdate
    log(f"{label}: starting background thread")
    tok = _iter_begin(key, label)
    obo_h = get_headers()
    _spawn_bg(_run_iter_setup_bg, tok, key, run_fn, obo_h, on_success)
    return _build_iter_status(), f"\u25a0 Stop {label}", False, [True] * NAV_TAB_COUNT


@callback(Output("it1-grid","children"),
          Output("btn-it1","children", allow_duplicate=True),
          Output("iter-bench-poll","disabled", allow_duplicate=True),
          Output({"type":"nav","index":ALL}, "disabled", allow_duplicate=True),
          Input("btn-it1","n_clicks"), prevent_initial_call=True)
def run_it1(n):
    if not n:  # phantom trigger on re-render (n_clicks=None/0) must never start/stop a run
        raise PreventUpdate
    return _iter_button_click("it1")

@callback(Output("it2-grid","children"),
          Output("btn-it2","children", allow_duplicate=True),
          Output("iter-bench-poll","disabled", allow_duplicate=True),
          Output({"type":"nav","index":ALL}, "disabled", allow_duplicate=True),
          Input("btn-it2","n_clicks"), prevent_initial_call=True)
def run_it2(n):
    if not n:  # phantom trigger on re-render (n_clicks=None/0) must never start/stop a run
        raise PreventUpdate
    return _iter_button_click("it2")

@callback(Output("it3-grid","children"),
          Output("btn-it3","children", allow_duplicate=True),
          Output("iter-bench-poll","disabled", allow_duplicate=True),
          Output({"type":"nav","index":ALL}, "disabled", allow_duplicate=True),
          Input("btn-it3","n_clicks"), prevent_initial_call=True)
def run_it3(n):
    if not n:  # phantom trigger on re-render (n_clicks=None/0) must never start/stop a run
        raise PreventUpdate
    return _iter_button_click("it3")

# Which SQL threshold function each Iteration 3 agent needs (matches notebook Iter 3 table P01-P05)
_ITER3_AGENT_FUNCTIONS = {
    "logistics": ("logistics_operations.get_critical_delay_shipments", "P01 Logistics Risk Standards"),
    "demand":    ("demand_analysis.get_critical_accuracy_forecasts",   "P02 Demand Anomaly Alert"),
    "inventory": ("inventory_management.get_critical_supply_positions", "P03 Inventory Risk Classification"),
    "supplier":  ("supplier_procurement.get_critical_quality_orders",  "P04 Procurement Quality Minimum"),
    "executive": ("reporting.get_critical_disruption_regions",         "P05 Executive Disruption Threshold"),
}

def _ws_base():
    """Normalised workspace base URL (always https)."""
    base = (HOST or "").rstrip("/")
    if base and not base.startswith(("http://", "https://")):
        base = "https://" + base
    return base

def _genie_agent_url(space_id):
    """Absolute link to the agent on the Databricks workspace Genie page (not the app's own URL)."""
    return f"{_ws_base()}/genie/rooms/{space_id}"

def _supervisor_url(sup):
    """Absolute link to a supervisor's page in the Databricks workspace.
    For supervisor-agents: /ml/supervisor-agents/{uuid}
    For serving endpoints: /ml/endpoints/{endpoint_name}"""
    name = sup.get("name", "") if sup else ""
    if name.startswith("supervisor-agents/"):
        uuid = name.split("/", 1)[1]
        return f"{_ws_base()}/ml/supervisor-agents/{uuid}"
    ep = sup.get("endpoint", "")
    if ep:
        return f"{_ws_base()}/ml/endpoints/{ep}"
    return None


def _build_manual_step_ui(spaces):
    """Iteration 3 manual gate. Waits until the user has added the SQL functions to each
    agent in the Genie UI and clicks Continue. UC Domain + Pages are a reminder only."""
    rows = []
    for agent_name in ["logistics", "demand", "inventory", "supplier", "executive"]:
        sid = spaces.get(agent_name)
        fn, test = _ITER3_AGENT_FUNCTIONS[agent_name]
        rows.append(html.Tr([
            html.Td(html.A(f"SC - {agent_name.title()} - Iteration 3", href=_genie_agent_url(sid) if sid else None,
                           target="_blank", style={"color":C["teal"],"textDecoration":"underline","fontWeight":"600"})
                    if sid else html.Span(f"{agent_name.title()} (not found)", style={"color":C["rose"]})),
            html.Td(html.Code(f"{CATALOG}.{fn}()"), style={"fontSize":"12px"}),
            html.Td(test, style={"color":C["muted"],"fontSize":"12px"}),
        ]))
    small = {"color":C["muted"],"fontSize":"13px"}
    return html.Div([
        html.Div([
            html.Div("\u23f8", style={"fontSize":"24px"}),
            html.Div([
                html.Div("Manual Step Required \u2014 Iteration 3 is paused", style={"fontWeight":"700","fontSize":"16px","color":C["rose"]}),
                html.Div("Automated setup is done. The benchmark will NOT start until you finish step 1 and click Continue.", style=small),
            ]),
        ], style={"display":"flex","gap":"12px","alignItems":"center","marginBottom":"16px"}),
        # Step 1 -- REQUIRED
        html.Div([
            html.Div("1. REQUIRED \u2014 Add the SQL function to each agent's data sources (Genie UI)",
                     style={"fontWeight":"700","marginBottom":"6px"}),
            html.Div("Open each agent (new tab) \u2192 Configure \u2192 Data \u2192 Add \u2192 SQL function \u2192 pick the function below \u2192 Save. "
                     "The API cannot do this step.", style={**small, "marginBottom":"8px"}),
            html.Table([html.Thead(html.Tr([html.Th("Agent"), html.Th("SQL function to add"), html.Th("Fixes test")])),
                        html.Tbody(rows)], className="tbl"),
        ], style={"padding":"12px 16px","borderRadius":"8px","border":f"1px solid {C['line']}","marginBottom":"12px"}),
        # Step 2 -- REMINDER
        html.Div([
            html.Div("2. REMINDER \u2014 UC Domain + Pages on the Discover page (if not already created)",
                     style={"fontWeight":"700","marginBottom":"6px"}),
            html.Div(f"Domain \u201cSupply Chain Operations\u201d with the 5 {CATALOG} schemas, plus the 7 Pages from the notebook's "
                     "Manual Step (Fiscal Calendar & Targets, Cross-Domain Metric Definitions, and the 5 domain policy pages). "
                     "Link fiscal_targets as a Related Asset on Page 1.", style=small),
            html.Div("Genie Agents cannot read UC Pages \u2014 these are human-facing governance docs and do not affect the benchmark.",
                     style={**small, "fontStyle":"italic","marginTop":"4px"}),
        ], style={"padding":"12px 16px","borderRadius":"8px","border":f"1px solid {C['line']}","marginBottom":"16px"}),
        html.Button("\u2713 I've added the functions \u2014 Continue Iteration 3 (run benchmark)", id="btn-it3-confirm",
                    n_clicks=0, className="btn primary",
                    style={"padding":"12px 32px","fontSize":"14px","fontWeight":"700"}),
    ], style={"padding":"20px","borderRadius":"12px","border":f"1px solid {C['rose']}","marginTop":"16px"})


@callback(Output("it3-grid","children", allow_duplicate=True),
          Input("btn-it3-confirm","n_clicks"), prevent_initial_call=True)
def confirm_it3_manual_step(n):
    """User finished the manual step: release the gate; the poll then starts the benchmark."""
    if not n or not (_ust().get("iter_manual_step") and _ust().get("iter_running") == "it3" and _ust().get("iter_run_token")):
        raise PreventUpdate
    _ust()["iter_manual_step"] = False
    _ust().setdefault("iter_setup_log", []).append("\u2713 Manual step confirmed by user \u2014 starting benchmark...")
    _ust()["iter_setup_done"] = True   # poll_iter_bench sees this and calls _start_iter_bench
    log("Iter3: manual step confirmed -- starting benchmark")
    return _build_iter_status()


_ITER_TITLES = ["Iteration 1", "Iteration 2", "Iteration 3"]

def _iter_btn_outputs(running_key=None):
    """Return 9 values: children×3, disabled×3, className×3 for iteration buttons."""
    children = [no_update, no_update, no_update]
    disabled = [no_update, no_update, no_update]
    classnames = [no_update, no_update, no_update]
    if running_key and running_key.startswith("it"):
        idx = int(running_key[-1]) - 1
        children[idx] = f"\u25a0 Stop {_ITER_TITLES[idx]}"
        disabled[idx] = False
        classnames[idx] = "btn rose"
    return children + disabled + classnames

def _iter_btn_reset_outputs():
    """Return 9 values resetting buttons with the SAME can_run logic as page_iterations()."""
    has_baseline = bool(_ust().get("agents"))
    iter_done = _ust().get("iterations_done", set())
    all_done = {"it1", "it2", "it3"}.issubset(iter_done)
    can_run = [False, False, False]
    if has_baseline and not _ust().get("iter_running"):
        if "it1" not in iter_done: can_run[0] = True
        elif "it2" not in iter_done: can_run[1] = True
        elif "it3" not in iter_done: can_run[2] = True
        elif all_done: can_run[2] = True  # allow re-running Iteration 3 after all done
    children = [f"Re-run {t}" if f"it{i+1}" in iter_done else f"Apply {t}"
                for i, t in enumerate(_ITER_TITLES)]
    disabled = [not ok for ok in can_run]
    classnames = ["btn primary" if ok else "btn" for ok in can_run]
    return children + disabled + classnames

# Shared polling for iteration benchmarks
@callback(Output("it1-grid","children", allow_duplicate=True),
          Output("it1-charts","children"),
          Output("it2-grid","children", allow_duplicate=True),
          Output("it2-charts","children"),
          Output("it3-grid","children", allow_duplicate=True),
          Output("it3-charts","children"),
          Output("iter-bench-poll","disabled"),
          Output("iter-progression-chart","children", allow_duplicate=True),
          Output("btn-it1","children", allow_duplicate=True),
          Output("btn-it2","children", allow_duplicate=True),
          Output("btn-it3","children", allow_duplicate=True),
          Output("btn-it1","disabled", allow_duplicate=True),
          Output("btn-it2","disabled", allow_duplicate=True),
          Output("btn-it3","disabled", allow_duplicate=True),
          Output("btn-it1","className", allow_duplicate=True),
          Output("btn-it2","className", allow_duplicate=True),
          Output("btn-it3","className", allow_duplicate=True),
          Output("page-refresh","data", allow_duplicate=True),
          Input("iter-bench-poll","n_intervals"),
          prevent_initial_call=True)
def poll_iter_bench(n):
    active = _ust().get("iter_bench_active")
    setup_phase = _ust().get("iter_setup_phase", False)
    still_running = active is not None or setup_phase or _ust().get("iter_running") is not None
    grid1 = grid2 = grid3 = no_update
    ch1 = ch2 = ch3 = no_update

    # During setup phase: show live log
    if setup_phase:
        running_key = _ust().get("iter_running")
        status = _build_iter_status()
        if running_key == "it1": grid1 = status
        elif running_key == "it2": grid2 = status
        elif running_key == "it3": grid3 = status
        # Check if thread errored — render the error, THEN clean up
        if _ust().get("iter_setup_error"):
            _ust()["iter_setup_phase"] = False
            _ust()["iter_setup_error"] = False
            _ust()["iter_running"] = None
            _iter_finished()
            return grid1, ch1, grid2, ch2, grid3, ch3, True, dcc.Graph(figure=_build_iter_progression_chart(), config={"displayModeBar":False}), *_iter_btn_reset_outputs(), -int(n or 1)  # disable poll + force page re-render
        # Check if manual step gate is active (Iter 3 only)
        if _ust().get("iter_manual_step"):
            running_key = _ust().get("iter_running")
            spaces = _ust().get("iter3_agent_spaces", {})
            if running_key == "it3":
                if not _ust().get("_it3_gate_rendered"):
                    grid3 = _build_manual_step_ui(spaces)   # draw once; redrawing every 2s can swallow clicks
                    _ust()["_it3_gate_rendered"] = True
                else:
                    grid3 = no_update   # keep the panel on screen (do NOT overwrite it with the log)
            return grid1, ch1, grid2, ch2, grid3, ch3, False, no_update, *_iter_btn_outputs(running_key), no_update  # keep polling
        # Check if setup completed — transition to benchmark phase
        if _ust().get("iter_setup_done"):
            _ust()["iter_setup_phase"] = False
            _ust()["iter_setup_done"] = False
            _start_iter_bench(running_key, "Iteration " + running_key[-1], get_headers())
            return grid1, ch1, grid2, ch2, grid3, ch3, False, no_update, *_iter_btn_outputs(running_key), no_update  # keep polling
        # Check for hung thread (>5 min with no new log entries)
        t0 = _ust().get("iter_setup_t0", 0)
        if t0 and (time.time() - t0) > 300:
            last_log = _ust().get("iter_setup_log", [])
            if last_log and not any("ERROR" in l.upper() for l in last_log[-3:]):
                _ust()["iter_setup_log"].append("WARNING: Setup taking >5 minutes — may be hung")
        return grid1, ch1, grid2, ch2, grid3, ch3, not still_running, dcc.Graph(figure=_build_iter_progression_chart(), config={"displayModeBar":False}) if not still_running else no_update, *_iter_btn_outputs(_ust().get("iter_running")), -int(n or 1) if not still_running else no_update

    for key, idx in [("it1",0), ("it2",1), ("it3",2)]:
        results = ST.get(f"iter_{key}_results")
        if results:
            g = _build_bench_grid(results, source=f"iter_{key}_results")
            done = not any(r.get("verdict") in ("WAITING","RUNNING") for r in results)
            charts = _build_bench_charts(results) if done else no_update
            if idx == 0: grid1, ch1 = g, charts
            elif idx == 1: grid2, ch2 = g, charts
            else: grid3, ch3 = g, charts

    still_running = _ust().get("iter_bench_active") is not None or _ust().get("iter_setup_phase", False) or _ust().get("iter_running") is not None
    return grid1, ch1, grid2, ch2, grid3, ch3, not still_running, dcc.Graph(figure=_build_iter_progression_chart(), config={"displayModeBar":False}) if not still_running else no_update, *(_iter_btn_reset_outputs() if not still_running else _iter_btn_outputs(_ust().get("iter_running"))), -int(n or 1) if not still_running else no_update



# ==== Page 5: Comparison ====================================================
# ==== Comparison: supervisor profiles, live run panel, CFO prompt ==========
_MCP_HINT = "genie_one_mcp"


def _sup_learn(endpoint, kind=None, tool=None, name=None):
    """Remember what a supervisor is from its live stream (works without supervisor-agents API access)."""
    d = _ust().setdefault("sup_learned", {}).setdefault(endpoint, {"tools": [], "name": ""})
    if tool and tool not in d["tools"]:
        d["tools"].append(tool)
    if name:
        d["name"] = name


def _fetch_endpoint_model(ep):
    """Query serving endpoint config and return the served model name (cached in ST)."""
    cache_key = f"_ep_model_{ep}"
    if cache_key in ST:
        return ST[cache_key]
    try:
        h = get_headers()
        r = requests.get(f"{HOST}/api/2.0/serving-endpoints/{ep}", headers=h, timeout=10)
        if r.status_code == 200:
            cfg = r.json().get("config") or {}
            entities = cfg.get("served_entities") or cfg.get("served_models") or []
            names = []
            for e in entities:
                n = e.get("entity_name") or e.get("model_name") or e.get("name") or ""
                v = e.get("entity_version") or e.get("model_version") or ""
                if n:
                    names.append(f"{n}" + (f" v{v}" if v else ""))
            result = ", ".join(names) if names else None
        else:
            result = None
    except Exception:
        result = None
    ST[cache_key] = result
    return result


def _sup_profile(sup):
    """What a supervisor IS: title, kind, one-line summary, detail bullets."""
    if not sup:
        return None
    ep = sup.get("endpoint", "")
    learned = (_ust().get("sup_learned") or {}).get(ep, {})
    tools = sup.get("tools") or []
    space_to_ver = {}
    for ver, agents in (_ust().get("agent_versions") or {}).items():
        for an, sid in agents.items():
            space_to_ver[sid] = (an, ver)
    title = sup.get("display_name") or ep
    if _is_system_name(title):
        title = learned.get("name") or ""
    bullets, kind = [], "unknown"
    if tools:
        genie = [t for t in tools if t["type"] == "genie_space"]
        mcp = [t for t in tools if t["type"] == "uc_mcp" or t.get("mcp")]
        if mcp and not genie:
            kind = "mcp"
            bullets = ["Single tool: Genie One (MCP)" for t in mcp]
        elif genie:
            kind = "multi"
            vers = sorted({space_to_ver[t["space_id"]][1] for t in genie if t["space_id"] in space_to_ver})
            pretty = lambda tid: tid.replace("-", " ").replace("_", " ").title()
            bullets = [f"{pretty(t['tool_id'])} \u2192 Genie agent"
                       + (f" ({space_to_ver[t['space_id']][1]})" if t["space_id"] in space_to_ver else "")
                       for t in genie]
            if vers:
                bullets.insert(0, "Agent set: " + ", ".join(vers))
        fns = [t for t in tools if t["type"] == "uc_function"]
        if fns:
            bullets.append(f"{len(fns)} policy SQL function{'s' if len(fns) != 1 else ''} (Iteration 3 thresholds)")
        other = [t for t in tools if t not in genie and t not in mcp and t not in fns]
        bullets += [f"{t['type'] or 'tool'}: {t['tool_id']}" for t in other]
    elif learned.get("tools"):
        names = learned["tools"]
        kind = "mcp" if any(_MCP_HINT in n for n in names) else "multi"
        # Categorize learned tools into readable summaries (not raw dumps)
        _genie = [n for n in names if n in ("demand-analysis", "inventory-management",
                  "logistics-operations", "supplier-risk", "executive-reporting")]
        _mcp = [n for n in names if _MCP_HINT in n]
        _fns = [n for n in names if "__" in n and n not in _mcp]  # UC functions have catalog__schema__fn format
        _other = [n for n in names if n not in _genie and n not in _mcp and n not in _fns]
        bullets = []
        if _genie:
            pretty = lambda tid: tid.replace("-", " ").title()
            bullets.append(f"{len(_genie)} domain agents: {', '.join(pretty(g) for g in _genie)}")
        if _mcp:
            bullets.append("Genie One MCP (single-tool architecture)")
        if _fns:
            # Show count + short names (strip catalog__schema__ prefix)
            short = [n.rsplit("__", 1)[-1] if "__" in n else n for n in _fns]
            bullets.append(f"{len(_fns)} policy SQL function{'s' if len(_fns) != 1 else ''}: {', '.join(short)}")
        bullets += [f"Tool: {n}" for n in _other]
    # Fallback: infer architecture from description when tools endpoint isn't readable
    if kind == "unknown":
        _dl = (sup.get("desc_full") or "").lower()
        if "mcp" in _dl or "genie one" in _dl or "genie-one" in _dl:
            kind = "mcp"
            bullets = bullets or ["Single tool: Genie One (MCP) — inferred from description"]
        elif any(w in _dl for w in ("multi-agent", "coordinates", "5 specialist", "domain agents")):
            kind = "multi"
            bullets = bullets or ["Multi-agent architecture — inferred from description"]
    summary = {
        "multi": "Multi-agent supervisor: splits the question and routes each part to Genie domain agents, then synthesises one answer.",
        "mcp": "Single-tool supervisor: forwards the whole question to Genie One MCP, which finds the data itself.",
    }.get(kind, "")
    if sup.get("desc_full") and kind != "unknown":
        summary = sup["desc_full"]
    elif sup.get("desc_full"):
        summary = sup["desc_full"]
    if kind == "unknown" and not summary:
        code = ST.get("sup_api_status")
        summary = ("Configuration not readable with your app token"
                   + (f" (supervisor-agents API: HTTP {code})" if code else "")
                   + ". Its architecture is detected live during the first comparison run.")
    if title == ep and kind == "mcp":
        title = "Genie One MCP Supervisor"
    model = ST.get(f"_ep_model_{ep}") if ep else None  # read cache only; fetch happens in update_wh_chip
    return dict(title=title, kind=kind, summary=summary, bullets=bullets,
                created=(sup.get("create_time") or "")[:16].replace("T", " "),
                model=model)


def _is_system_name(name):
    """True for code-like names such as mas-14f57ebd-endpoint (never shown to users)."""
    return not name or bool(re.match(r"^mas-[0-9a-f]+-endpoint$", name)) or name.startswith("supervisor-agents/")


def _sup_title(sup, fallback):
    prof = _sup_profile(sup)
    return (prof["title"] if prof and prof["title"] else fallback)


def _is_supervisor_time_limit(text):
    """The Agent Bricks supervisor has a ~290 s window per request; long_task mode (call_supervisor_stream) resumes past it automatically.
    When it runs out it returns a 'Continue / Wrap up' message instead of an answer."""
    t = (text or "")[:600].lower()
    return "reached the time limit" in t and ("wrap up" in t or "continue" in t)


def _cmp_live_reset(side):
    ST[f"cmp_live_{side}"] = {"steps": [], "answer": "", "status": "starting", "t0": time.time(),
                              "t_end": None, "calls": 0}


def _cmp_on_step(side, endpoint):
    """Build the on_step callback for one side's streaming supervisor call."""
    tool_ids = {v["tool_id"] for v in SUPERVISOR_TOOL_MAP.values()} if "SUPERVISOR_TOOL_MAP" in globals() else set()
    def on_step(kind, text):
        live = ST.get(f"cmp_live_{side}")
        if live is None:
            return
        if kind == "answer":
            live["answer"] += text
            live["status"] = "answering"
            return
        if kind == "call":
            live["calls"] += 1
            live["status"] = "calling"
            tool, _, rest = text.partition(":")
            _sup_learn(endpoint, tool=tool.strip())
            text = f"Asking {_pretty_tool(tool.strip())}" + (f": {rest.strip()}" if rest.strip() else "")
        if kind == "route":
            if text in tool_ids or text.endswith(("-analysis", "-management", "-operations", "-risk", "-reporting")):
                text = f"Answer from {_pretty_tool(text)}"
            else:
                _sup_learn(endpoint, name=text)
                text = f"{text} is composing the final answer"
        live["steps"].append((time.time() - live["t0"], kind, text))
    return on_step


def _pretty_tool(name):
    """User-facing tool name: 'executive-reporting' -> 'Executive Reporting agent', Genie One MCP calls -> 'Genie One'."""
    if _MCP_HINT in name:
        return "Genie One (checking progress)" if "poll" in name else "Genie One"
    if name.startswith(("get-critical", "get_critical")):
        return "policy function " + name.replace("-", "_")
    return name.replace("-", " ").replace("_", " ").title() + " agent"


_CMP_STEP_STYLE = {
    "call": ("\u25b6", "indigo"), "result": ("\u25c0", "teal"), "progress": ("\u2022", "muted"),
    "route": ("\u21aa", "ink"), "error": ("\u2715", "rose"), "info": ("\u2139", "muted"),
}


_MD_PROTECT = re.compile(r"```.*?```|`[^`\n]*`|\[[^\]]*\]\([^)]*\)|https?://\S+", re.S)
_MD_BIGNUM = re.compile(r"(?<![\w.\-/$,])(\d{4,})(\.\d+)?(?![\w.\-/,]*\d)")


def _fmt_bignum(m):
    whole, frac = m.group(1), m.group(2) or ""
    if not frac and len(whole) == 4:  # 4-digit integers are usually years / codes -- leave untouched
        return m.group(0)
    if frac and len(frac) > 3:        # float noise like 2484985.570000001 -> 2 decimals (carry-safe)
        return f"{round(float(whole + frac), 2):,.2f}"
    return f"{int(whole):,}{frac}"


_MD_NEG = re.compile(r"(?<![\w.\-/$,])-(\d{4,})(\.\d+)?(?![\w.\-/,]*\d)")


def _fmt_neg(m):
    return "-" + _fmt_bignum(m)


def _pretty_md(text):
    """Display-only polish for supervisor markdown: thousands separators on long numbers and float-noise rounding.
    Code blocks, inline code, links and URLs are never touched."""
    if not text:
        return text
    out, pos = [], 0
    for mm in _MD_PROTECT.finditer(text):
        out.append(_MD_NEG.sub(_fmt_neg, _MD_BIGNUM.sub(_fmt_bignum, text[pos:mm.start()])))
        out.append(mm.group(0))
        pos = mm.end()
    out.append(_MD_NEG.sub(_fmt_neg, _MD_BIGNUM.sub(_fmt_bignum, text[pos:])))
    return "".join(out)


def _annotate_gt_in_md(text, results):
    """Inject <mark> tags around ground-truth values found in the supervisor answer.
    Each matched value gets a tooltip with the test ID.  Only EXACT/TOLERANT results are annotated.
    Protected regions (code fences, inline code, links) are skipped."""
    if not text or not results:
        return text
    # Collect (literal_string, test_id) pairs, longest first to avoid partial matches
    hits = []
    for r in results:
        if r.get("verdict") not in ("EXACT", "TOLERANT"):
            continue
        cl = r.get("closest")
        if cl and len(str(cl)) >= 2:
            hits.append((str(cl), r["id"]))
    if not hits:
        return text
    hits.sort(key=lambda x: -len(x[0]))
    # Build regex that skips code fences and inline code
    escaped = [f"(?P<gt{i}>{re.escape(h[0])})" for i, h in enumerate(hits)]
    tag_map = {f"gt{i}": tid for i, (_, tid) in enumerate(hits)}
    prot = r"(?P<code>```[\s\S]*?```|`[^`]+`)"
    pat = re.compile(prot + "|" + "|".join(escaped))
    used = set()  # annotate each test ID only once
    def _repl(m):
        if m.group("code"):
            return m.group(0)
        for gn, tid in tag_map.items():
            if m.group(gn) is not None and tid not in used:
                used.add(tid)
                return f'<mark title="{tid}">{m.group(gn)}</mark>'
        return m.group(0)
    return pat.sub(_repl, text)


_TBL_SEP_RX = re.compile(r"\|\s*:?-+:?\s*\|")
_TBL_SEP_RUN = re.compile(r"(?:\|\s*:?-+:?\s*)+\|")


def _is_md_table(txt):
    """True when the text holds a markdown table (separator row '| --- |' or compact '|-|-|')."""
    return "|" in txt and bool(_TBL_SEP_RX.search(txt))


def _tableize_result(txt):
    """Normalise Genie tool output into renderable markdown tables.
    Handles (a) one-line tables ('| a | b | | --- | --- | | 1 | 2 |'), (b) compact multi-line tables with a pandas
    index column ('||region|..|' + '|0|Western|..'), and strips agent-directed <!-- ... -- > markers."""
    txt = re.sub(r"<!--.*?--\s*>", "", txt or "", flags=re.S)
    txt = re.sub(r"<details>.*?</details>", "", txt, flags=re.S).strip()
    if "\n|" not in txt and not txt.startswith("|\n"):
        m = _TBL_SEP_RUN.search(txt)
        start = txt.find("|")
        if m and start >= 0:
            n = len(re.findall(r"-+", m.group(0)))
            head, body = txt[:start].strip(), txt[start:].strip()
            toks = [t.strip() for t in body.split("|")]
            toks = toks[1:-1] if len(toks) >= 2 else toks
            rows, i = [], 0
            while i + n <= len(toks):
                rows.append(toks[i:i + n]); i += n
                if i < len(toks) and toks[i] == "":
                    i += 1
            tail = " ".join(t for t in toks[i:] if t)
            lines = ["| " + " | ".join(r) + " |" for r in rows]
            txt = (head + "\n\n" if head else "") + "\n".join(lines) + ("\n\n" + tail if tail else "")
    out, block = [], []
    def flush():
        if not block:
            return
        def _cells(ln):
            ln = ln.strip()
            ln = ln[1:] if ln.startswith("|") else ln
            ln = ln[:-1] if ln.endswith("|") else ln
            return [c.strip() for c in ln.split("|")]
        cells = [_cells(ln) for ln in block]
        if len(cells) >= 3 and cells[0] and cells[0][0] == "" and all(
                (r[0].isdigit() or set(r[0]) <= set("-: ")) for r in cells[1:] if r):
            cells = [r[1:] for r in cells]
        # Separator row must have exactly one '---' per header cell, and every row the same width,
        # otherwise markdown falls back to plain text (e.g. after dropping a pandas index column).
        w = len(cells[0])
        cells = [["---"] * w if (r and all(c_ and set(c_) <= set("-: ") for c_ in r)) else (r + [""] * w)[:w]
                 for r in cells]
        if out and out[-1].strip():
            out.append("")
        out.extend("| " + " | ".join(r) + " |" for r in cells)
        out.append("")
        block.clear()
    for ln in txt.splitlines():
        if ln.strip().startswith("|"):
            block.append(ln)
        else:
            flush(); out.append(ln)
    flush()
    return "\n".join(out).strip()


_SQL_START = re.compile(r"^\s*(WITH|SELECT)\b", re.I)
_SQL_CLAUSE = re.compile(r"\s+(FROM|WHERE|GROUP BY|ORDER BY|HAVING|LIMIT|LEFT JOIN|RIGHT JOIN|INNER JOIN|FULL JOIN|"
                         r"JOIN|UNION ALL|UNION)\b", re.I)


def _fmt_sql(sql):
    """Light display formatter for one-line SQL: one clause per line, top-level select columns on their own lines."""
    s = re.sub(r"\s+", " ", sql or "").strip()
    out, depth, q = [], 0, None
    base = 1 if s[:4].upper() == "WITH" else 0
    for ch in s:
        if q:
            q = None if ch == q else q
        elif ch in "'`\"":
            q = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        out.append(ch)
        if ch == "," and not q and depth == base:
            out.append("\n ")
    s = "".join(out)
    s = _SQL_CLAUSE.sub(lambda m: "\n" + m.group(1), s)
    s = re.sub(r"\)\s+SELECT\b", ")\nSELECT", s)
    return re.sub(r"\n\s*\n", "\n", s)


def _fence_sql(md):
    """Wrap raw multi-line SQL statements (outside tables/fences) in ```sql blocks so they render as code."""
    lines, out, buf, in_fence = md.split("\n"), [], [], False
    def flush():
        if buf:
            if out and out[-1].strip():
                out.append("")
            out.extend(["```sql", *buf, "```", ""])
            buf.clear()
    for ln in lines:
        if ln.strip().startswith("```"):
            flush(); in_fence = not in_fence; out.append(ln); continue
        if in_fence:
            out.append(ln); continue
        cte_tail = bool(buf) and buf[0].lstrip().upper().startswith("WITH") and \
            (buf[-1].strip() == ")" or buf[-1].rstrip().endswith("),")) and not any(
                _SQL_START.match(b) and not b.lstrip().upper().startswith("WITH") and not b.startswith((" ", "\t"))
                for b in buf[1:])
        if _SQL_START.match(ln) and not (buf and ln.startswith((" ", "\t"))) and not cte_tail:
            flush(); buf.append(ln); continue
        if buf and ln.strip() and not ln.strip().startswith("|") and \
                not ln.strip().lstrip("*").lower().startswith("query returned"):
            buf.append(ln); continue
        flush(); out.append(ln)
    flush()
    return "\n".join(out).strip()


def _step_body(kind, text):
    """Render one live step consistently: SQL as formatted code, tables as real tables, numbers polished."""
    t = re.sub(r"<!--.*?--\s*>", "", text or "", flags=re.S).strip()
    # Strip agent-name prefix before table data (e.g. "demand-analysis ||col1|col2|")
    if "|" in t:
        idx = t.index("|")
        if idx > 0 and "\n" not in t[:idx] and "|" not in t[:idx]:
            t = t[idx:]
    base = {"fontSize": "12.5px", "color": C["ink"] if kind in ("call", "route") else C["muted"],
            "fontWeight": "600" if kind == "call" else "400", "flex": "1", "minWidth": "0"}
    m = re.match(r"(Running SQL|Executing SQL)\s*:\s*(.*)", t, re.S)
    if m:
        return html.Div([html.Div(m.group(1), style={"fontWeight": "700", "fontSize": "11px", "letterSpacing": "0.04em",
                                                     "textTransform": "uppercase", "color": C["indigo"]}),
                         html.Pre(_fmt_sql(m.group(2))[:6000], className="cmp-sql")], style=base)
    has_sql = any(_SQL_START.match(ln) for ln in t.split("\n"))
    if _is_md_table(t) or has_sql:
        md = _tableize_result(t[:20000]) if _is_md_table(t) else t[:20000]
        md = re.sub(r"(?m)^(Query result:\s*)?(Query returned \d+ rows?:?)", r"**\2**", _fence_sql(md))
        return dcc.Markdown(_pretty_md(md), className="cmp-md cmp-md-sm", style=base)
    if t.startswith("Thinking:"):
        return html.Span(t[len("Thinking:"):].strip()[:1500],
                         style={**base, "fontStyle": "italic", "whiteSpace": "pre-wrap"})
    short = t if len(t) <= 700 else t[:700] + " \u2026"
    return html.Span(short, title=t[:2000] if len(t) > 700 else None,
                     style={**base, "whiteSpace": "pre-wrap", "wordBreak": "break-word"})


def _build_cmp_live(side):
    """Live panel for one supervisor: steps + streamed output, each in its own scrollable box."""
    live = ST.get(f"cmp_live_{side}")
    box = {"border": f"1px solid {C['line']}", "borderRadius": "12px", "background": C["soft"],
           "padding": "10px 12px", "overflowY": "auto", "display": "flex", "flexDirection": "column-reverse"}
    if not live:
        return html.Div("Not run yet \u2014 click Run Comparison to watch this supervisor work live.",
                        style={"color": C["muted"], "fontSize": "13px", "padding": "10px 0", "marginBottom": "12px"})
    end = live.get("t_end") or time.time()
    el = int(end - live["t0"])
    el_s = f"{el // 60}m {el % 60:02d}s" if el >= 60 else f"{el}s"
    status = {"starting": "Sending prompt\u2026", "calling": "Calling tools\u2026", "answering": "Writing answer\u2026",
              "done": "Complete", "error": "Failed"}.get(live["status"], live["status"])
    col = C["rose"] if live["status"] == "error" else (C["teal"] if live["status"] == "done" else C["indigo"])
    rows = []
    for t, kind, text in live["steps"]:
        icon, ck = _CMP_STEP_STYLE.get(kind, ("\u2022", "muted"))
        rows.append(html.Div([
            html.Span(f"{int(t):>4}s", style={"fontFamily": "var(--mono)", "fontSize": "11px", "color": C["muted"],
                                             "minWidth": "42px"}),
            html.Span(icon, style={"color": C[ck], "minWidth": "14px", "fontWeight": "700"}),
            _step_body(kind, text),
        ], style={"display": "flex", "gap": "8px", "padding": "3px 0", "alignItems": "flex-start"}))
    if not rows:
        rows = [html.Div("Waiting for the first step\u2026", style={"color": C["muted"], "fontSize": "12.5px"})]
    answer = live.get("answer") or ""
    # Annotate matched ground-truth values after scoring is complete
    cmp_results = _ust().get(f"cmp_results_{side}")
    display_answer = answer
    if answer and cmp_results and live.get("status") == "done":
        processed = _tableize_result(answer) if _is_md_table(answer) else answer
        display_answer = _annotate_gt_in_md(_pretty_md(processed), cmp_results)
    else:
        display_answer = _pretty_md(_tableize_result(answer) if _is_md_table(answer) else answer) if answer else ""
    # Rename generic "Explore in Databricks" link to the supervisor-specific label
    if display_answer:
        sups = _ust().get("supervisors", [])
        _idx = 0 if side == "a" else 1
        _side_sup = sups[_idx] if len(sups) > _idx else None
        _side_prof = _sup_profile(_side_sup) if _side_sup else None
        if _side_prof and _side_prof["kind"] == "mcp":
            display_answer = display_answer.replace("Explore in Databricks", "Explore Genie One")
    _tok = _ust().get(f"cmp_usage_{side}") or {}
    _tok_total = _tok.get("total_tokens") or ((_tok.get("input_tokens") or 0) + (_tok.get("output_tokens") or 0))
    _tok_str = f" \u2022 {_tok_total:,} tokens" if _tok_total else ""
    return html.Div([
        html.Div([
            html.Span(status, style={"fontWeight": "700", "color": col, "fontSize": "13px"}),
            html.Span(f"{el_s} \u2022 {live['calls']} tool call{'s' if live['calls'] != 1 else ''}"
                      f" \u2022 {len(answer):,} chars{_tok_str}",
                      style={"fontFamily": "var(--mono)", "fontSize": "12px", "color": C["muted"]}),
        ], style={"display": "flex", "justifyContent": "space-between", "gap": "10px", "marginBottom": "6px"}),
        html.Div("Live steps", style={"fontSize": "11px", "fontWeight": "700", "letterSpacing": "0.05em",
                                      "textTransform": "uppercase", "color": C["muted"], "margin": "4px 0"}),
        html.Div(html.Div(rows), style={**box, "height": "240px", "maxHeight": "240px", "boxSizing": "border-box", "flexShrink": "0"}),
        html.Div("Supervisor output", style={"fontSize": "11px", "fontWeight": "700", "letterSpacing": "0.05em",
                                             "textTransform": "uppercase", "color": C["muted"], "margin": "10px 0 4px"}),
        html.Div(html.Div(dcc.Markdown(display_answer, className="cmp-md", link_target="_blank",
                                       dangerously_allow_html=True) if answer else
                          html.Span("No output yet \u2014 the answer streams here as the supervisor writes it.",
                                    style={"color": C["muted"], "fontSize": "12.5px"}),
                          style={"fontSize": "13px"}),
                 style={**box, "height": "320px", "maxHeight": "320px", "boxSizing": "border-box", "flexShrink": "0", "background": C["surface"]}),
        # Explore link is already in the supervisor card above — no duplicate here.
    ], style={"marginBottom": "14px"})


@callback(Output("btn-cmp", "children"), Input("cmp-poll", "n_intervals"), Input("btn-cmp", "n_clicks"),
          prevent_initial_call=True)
def cmp_button_label(_n, _c):
    """Label follows the run state; the disabled state is owned by handle_cmp."""
    if _ust().get("cmp_running"):
        t0 = (_ust().get("cmp_live_a") or {}).get("t0")
        if not t0:
            return "Comparison running\u2026"
        el = int(time.time() - t0)
        return f"Comparison running\u2026 {el // 60}m {el % 60:02d}s"
    return "Run Comparison"


def _cmp_step_summary(kind, text):
    """One readable line for a live step: markers removed, tables summarised as 'Query result: table with N rows'."""
    t = re.sub(r"<!--.*?--\s*>", "", text or "", flags=re.S)
    if _is_md_table(t):
        md = _tableize_result(t)
        n = sum(1 for ln in md.splitlines() if ln.startswith("|")) - 2
        head = md.split("\n|", 1)[0].strip().rstrip(":")
        return (head + ": " if head and not head.startswith("|") else "Query result: ") + \
            f"table with {max(n, 0)} row{'s' if n != 1 else ''}"
    return re.sub(r"\s+", " ", t).strip()


def _cmp_status_line(names=("Supervisor A", "Supervisor B")):
    """Formatted live status for both supervisors: state chip, elapsed time, tool calls, output size and the
    latest step (2-line clamp, full text on hover)."""
    cards = []
    for (side, _), name in zip((("a", "A"), ("b", "B")), names):
        # Use first letter of the supervisor kind as badge, not generic A/B
        lab = name[0].upper() if name else side.upper()
        live = ST.get(f"cmp_live_{side}") or {}
        status = live.get("status")
        st, ck = {"starting": ("Sending prompt", "indigo"), "calling": ("Calling tools", "indigo"),
                  "answering": ("Writing answer", "indigo"), "done": ("Done", "teal"),
                  "error": ("Failed", "rose")}.get(status, ("Waiting", "muted"))
        t0 = live.get("t0")
        el = int((live.get("t_end") or time.time()) - t0) if t0 else 0
        el_s = f"{el // 60}m {el % 60:02d}s" if el >= 60 else f"{el}s"
        steps = live.get("steps") or []
        kind, last = (steps[-1][1], steps[-1][2]) if steps else ("info", "")
        icon, ick = _CMP_STEP_STYLE.get(kind, ("\u2022", "muted"))
        summ = _cmp_step_summary(kind, last) if last else "No steps yet"
        calls = live.get("calls", 0)
        _utok = _ust().get(f"cmp_usage_{side}") or {}
        _utot = _utok.get("total_tokens") or ((_utok.get("input_tokens") or 0) + (_utok.get("output_tokens") or 0))
        _utxt = f" \u2022 {_utot:,} tok" if _utot else ""
        stats = f"{el_s} \u2022 {calls} tool call{'s' if calls != 1 else ''} \u2022 {len(live.get('answer') or ''):,} chars{_utxt}"
        cards.append(html.Div([
            html.Div([
                html.Span(lab, style={"background": C[ck], "color": "#fff", "borderRadius": "6px", "padding": "1px 8px",
                                      "fontWeight": "700", "fontSize": "12px"}),
                html.Span(name, style={"fontWeight": "700", "fontSize": "13px", "color": C["ink"], "overflow": "hidden",
                                       "textOverflow": "ellipsis", "whiteSpace": "nowrap", "minWidth": "0"}),
                html.Span(st, style={"marginLeft": "auto", "color": C[ck], "fontWeight": "700", "fontSize": "12px",
                                     "border": f"1px solid {C[ck]}", "borderRadius": "999px", "padding": "1px 10px",
                                     "whiteSpace": "nowrap"}),
            ], style={"display": "flex", "alignItems": "center", "gap": "8px"}),
            html.Div(stats, style={"fontFamily": "var(--mono)", "fontSize": "11.5px", "color": C["muted"],
                                   "margin": "6px 0 4px"}),
            html.Div([html.Span(icon, style={"color": C[ick], "fontWeight": "700", "minWidth": "14px"}),
                      html.Span(summ, title=summ[:600],
                                style={"overflow": "hidden", "display": "-webkit-box", "WebkitLineClamp": "2",
                                       "WebkitBoxOrient": "vertical", "wordBreak": "break-word"})],
                     style={"display": "flex", "gap": "6px", "fontSize": "12.5px", "color": C["ink"], "lineHeight": "1.4"}),
        ], style={"flex": "1", "minWidth": "260px", "border": f"1px solid {C['line']}", "borderRadius": "12px",
                  "background": C["soft"], "padding": "10px 12px"}))
    return html.Div(cards, style={"display": "flex", "gap": "12px", "flexWrap": "wrap"})


# Phrases of the CFO prompt highlighted like the workshop HTML (kind -> where the answer comes from)
_PROMPT_HL = [
    ("actual August and July revenue numbers", "m"), ("the dollar change", "m"), ("the percentage change", "m"),
    ("on-time delivery rate", "m"), ("average delay", "m"), ("How many total shipments went out", "m"),
    ("how many were late", "m"), ("fill rate", "v"),
    ("inventory positions are sitting below safety stock", "m"), ("how many unique SKUs are affected", "m"),
    ("days of supply for those at-risk items", "m"), ("SKUs are completely stocked out", "m"),
    ("percentage of vendors delivered late", "m"), ("purchase orders were late out of total", "m"),
    ("average lead time variance", "m"), ("total vendor SLA penalties", "v"),
    ("total Cost of Disruption by region", "g"), ("cancelled revenue", "g"), ("at-risk backorder revenue", "g"),
    ("wasted freight on late shipments", "m"), ("supplier penalty exposure", "g"),
    ("Logistics Risk Standards", "f"), ("Demand Anomaly Alert", "f"), ("Inventory Standards", "f"),
    ("Procurement Quality Minimum", "f"), ("Executive Disruption Threshold", "f"),
    ("Q1 service-level targets", "p"),
]
_PROMPT_HL_STYLE = {
    "m": {"background": C["indigo_bg"], "color": C["indigo"], "boxShadow": f"inset 0 -2px 0 {C['indigo']}"},
    "g": {"background": C["teal_bg"], "color": C["teal"], "boxShadow": f"inset 0 -2px 0 {C['teal']}"},
    "p": {"background": C["rose_bg"], "color": C["rose"], "boxShadow": f"inset 0 -2px 0 {C['rose']}"},
    "f": {"background": C["rose_bg"], "color": C["rose"], "boxShadow": f"inset 0 -2px 0 {C['rose']}"},
    "v": {"color": C["ink"], "boxShadow": f"inset 0 -2px 0 {C['muted']}",
          "outline": f"1.5px dashed {C['muted']}", "outlineOffset": "1px"},
}
_PROMPT_HL_LEGEND = [("m", "Metric view (Iter 2)"), ("g", "Cost of Disruption view (Iter 2)"),
                     ("f", "Policy SQL function (Iter 3)"), ("p", "UC Page / fiscal targets (Iter 3)"),
                     ("v", "Business word, different column")]


def _build_prompt_box():
    """The exact prompt sent to both supervisors, formatted like the workshop HTML prompt box."""
    kinds = dict(_PROMPT_HL)
    pat = re.compile("|".join(re.escape(t) for t, _ in sorted(_PROMPT_HL, key=lambda x: -len(x[0]))))
    parts, pos = [], 0
    for mm in pat.finditer(MAIN_PROMPT):
        if mm.start() > pos:
            parts.append(MAIN_PROMPT[pos:mm.start()])
        parts.append(html.Span(mm.group(0), style={"padding": "1px 3px", "margin": "0 -1px", "borderRadius": "5px",
                                                   **_PROMPT_HL_STYLE[kinds[mm.group(0)]]}))
        pos = mm.end()
    parts.append(MAIN_PROMPT[pos:])
    legend = [html.Span([html.Span("Aa", style={"padding": "0 4px", "borderRadius": "4px", "marginRight": "6px",
                                                "fontSize": "11px", **_PROMPT_HL_STYLE[k]}), lab],
                        style={"display": "inline-flex", "alignItems": "center", "border": f"1px solid {C['line']}",
                               "borderRadius": "999px", "padding": "4px 10px", "fontSize": "12px",
                               "color": C["muted"], "background": C["surface"]})
              for k, lab in _PROMPT_HL_LEGEND]
    return html.Blockquote([
        html.Span("CFO request to the Supply Chain Control Tower \u2014 sent unchanged to both supervisors",
                  style={"display": "block", "fontSize": "13px", "fontWeight": "600", "color": C["muted"],
                         "marginBottom": "12px"}),
        html.P(["\u201c", *parts, "\u201d"], style={"fontSize": "16px", "lineHeight": "1.75", "margin": "0",
                                                  "color": C["ink"]}),
        html.Div(legend, style={"display": "flex", "flexWrap": "wrap", "gap": "6px", "marginTop": "16px"}),
    ], style={"margin": "0 0 20px 0", "background": C["surface"], "border": f"1px solid {C['line']}",
              "borderRadius": "22px", "padding": "24px 28px", "boxShadow": "0 18px 40px -28px rgba(21,32,58,.35)"})


# ==== Page: Reliability & Dependability (Robustness) =======================
# Same 10 hardest tests x 3 rephrasings as the Curator Workbench notebook (cell 28).
ROBUSTNESS_TESTS = [
    {"id": "A02", "agent": "logistics", "gt": 94.57, "desc": "Late delivery rate (known non-deterministic flaker)",
     "variations": ["What percentage of August 2026 shipments to the Western region were delayed?",
                    "For Western deliveries last month, what share arrived late?",
                    "Calculate the delayed shipment rate for the Western territory in August"]},
    {"id": "A03", "agent": "logistics", "gt": 2.94, "desc": "Avg delay days for late shipments (filter sensitivity)",
     "variations": ["How many days on average were Western region delayed shipments late in August?",
                    "What is the mean delay duration for delayed Western deliveries last month?",
                    "Average number of days late for delayed Western shipments in Aug 2026"]},
    {"id": "D04", "agent": "supplier", "gt": 8.69, "desc": "Avg lead time variance (wrong-table trap)",
     "variations": ["What was the mean lead time deviation across all vendors in August 2026?",
                    "How many days on average did supplier deliveries deviate from planned lead times last month?",
                    "Across all purchase orders in August, what is the average lead time variance?"]},
    {"id": "F02", "agent": "supplier", "gt": 75.00, "desc": "Vendor late rate (per-order vs per-vendor trap)",
     "variations": ["What is the vendor late delivery rate for August 2026?",
                    "What share of purchase orders had late deliveries last month?",
                    "Late delivery percentage across all supplier orders in August"]},
    {"id": "H03", "agent": "demand", "gt": 71.23, "desc": "Fulfillment rate (Fulfilled vs Partially_Fulfilled trap)",
     "variations": ["What percentage of Western orders were fully fulfilled in August 2026?",
                    "For the Western region, what is the fulfilled order rate last month?",
                    "How many Western region orders last month had status Fulfilled as a percentage of total?"]},
    {"id": "H05", "agent": "executive", "gt": 3138569.66, "desc": "Revenue at risk (cross-domain aggregation)",
     "variations": ["How much revenue is at risk in the Western region from cancellations, backorders, and late shipment costs combined?",
                    "Calculate the total financial exposure from Western supply chain disruptions including cancelled orders, backordered revenue, and wasted freight",
                    "What is the combined revenue impact of cancellations, backorders, and freight waste in the Western region?"]},
    {"id": "G01", "agent": "executive", "gt": 92.0, "desc": "Q1 service-level target (fiscal calendar trap)",
     "variations": ["What is our Q1 service level target and are we at risk of missing it?",
                    "Will we hit our first quarter service-level goals?",
                    "How do our current service levels compare to the Q1 target?"]},
    {"id": "P01", "agent": "logistics", "gt": 176, "desc": "Logistics Risk Standards (delay>=5 AND weight>800)",
     "variations": ["Count of August deliveries that triggered the logistics risk policy",
                    "How many shipments last month violated the Logistics Risk Standards?",
                    "Total shipments flagged by logistics risk criteria in August 2026"]},
    {"id": "P04", "agent": "supplier", "gt": 11, "desc": "Procurement Quality Minimum (quality<75 AND ltv>12)",
     "variations": ["How many August purchase orders failed to meet the procurement quality threshold?",
                    "Count of vendor orders last month that were below the quality minimum standard",
                    "Number of supplier POs in August that didn't meet Procurement Quality Minimum"]},
    {"id": "P02", "agent": "demand", "gt": 77, "desc": "Demand Anomaly Alert (qty>=8 AND price<30 AND Online)",
     "variations": ["Count of Western August orders that set off a demand anomaly flag",
                    "How many orders in the Western territory last month were flagged as demand anomalies?",
                    "Total demand anomaly alerts for Western region orders in August 2026"]},
]
_ROB_TOTAL = sum(len(t["variations"]) for t in ROBUSTNESS_TESTS)


def _rob_sig(sql):
    """(structural, full) SQL signature -- identical to the notebook's _extract_sig."""
    if not sql:
        return ("(no SQL)", "(no SQL)")
    sl = " ".join(sql.lower().replace("`", "").replace('"', "").split())
    sl = re.sub(r"date\s+'", "'", sl)
    tables = re.findall(r"from\s+(\w+(?:\.\w+)*)", sl)
    aggs = sorted(set(re.findall(r"(count|sum|avg|min|max)\s*\(", sl)))
    table_str = tables[0] if tables else "?"
    agg_str = "+".join(aggs) if aggs else "sel"
    where = re.search(r"where\s+(.+?)(?:\s+group\s|\s+order\s|\s+limit\s|\s+having\s|$)", sl)
    if where:
        conds = sorted(x.strip() for x in re.split(r"\s+and\s+", where.group(1).strip()) if x.strip())
        where_sketch = " AND ".join(conds)[:150]
    else:
        where_sketch = "(none)"
    return (f"{table_str}|{agg_str}", f"{table_str}|{agg_str}|{where_sketch}")


def _rob_scores(results):
    """Per-test Reliability (% variations correct) and Dependability (SQL consistency 100/85/60/33/50/0)."""
    scores = []
    for t in ROBUSTNESS_TESTS:
        tr = sorted([r for r in results if r["id"] == t["id"]], key=lambda r: r["vi"])
        done = [r for r in tr if r["verdict"] in ("PASS", "FAIL", "ERROR")]
        passes = sum(1 for r in done if r["verdict"] == "PASS")
        rel = 100 * passes / max(len(t["variations"]), 1)
        sigs = [_rob_sig(r.get("sql")) for r in done if r.get("sql")]
        if len(sigs) >= 2:
            uf, us = len({s[1] for s in sigs}), len({s[0] for s in sigs})
            dep, lab = ((100, "Identical SQL") if uf == 1 else (85, "Same structure, different filters") if us == 1
                        else (60, "Mixed approaches") if us == 2 else (33, "Genuinely different SQL"))
        elif len(sigs) == 1:
            dep, lab = 50, "Only 1 SQL available"
        else:
            dep, lab = 0, "No SQL generated"
        scores.append(dict(id=t["id"], desc=t["desc"], agent=t["agent"], reliability=round(rel, 1),
                           dependability=dep, dep_label=lab, passes=passes, done=len(done),
                           verdicts=[r["verdict"] for r in tr],
                           func_equiv=(dep < 100 and rel == 100)))
    return scores


@_bg_guard("rob_running")
def _run_rob_bg(version, obo_headers):
    """Background thread: 30 Genie calls (3 parallel), scored against ground truth."""
    agents = (_ust().get("agent_versions") or {}).get(version) or {}
    res = []
    for t in ROBUSTNESS_TESTS:
        for vi, q in enumerate(t["variations"], 1):
            res.append(dict(id=t["id"], vi=vi, question=q, gt=_tc_gt(t["id"], t["gt"]), agent=t["agent"], verdict="WAITING",
                            found=None, closest=None, sql=None, answer="", secs=0))
    _rob_tokens = [0]  # mutable accumulator for thread safety
    _ust().update(rob_results=res, rob_version=version, rob_t0=time.time(), rob_t_end=None, rob_running=True,
              rob_total_tokens=0, rob_log=[f"Started on {version} agents ({_ROB_TOTAL} Genie calls)"])
    def _one(r):
        sid = agents.get(r["agent"])
        if not sid:
            r.update(verdict="ERROR", answer=f"No {r['agent']} agent in {version}")
            return
        r["verdict"] = "RUNNING"
        t0 = time.time()
        resp = ask_genie(sid, r["question"], headers=obo_headers)
        r["secs"] = round(time.time() - t0, 1)
        _u = resp.get("usage") or {}
        _rob_tokens[0] += _u.get("total_tokens") or _u.get("output_tokens") or 0
        _ust()["rob_total_tokens"] = _rob_tokens[0]
        if resp.get("status") != "COMPLETED":
            r.update(verdict="ERROR", answer=str(resp.get("answer", ""))[:300])
        else:
            text = (resp.get("answer") or "") + " " + (resp.get("sql") or "")
            me, fe, ce = find_value_in_text(text, r["gt"])
            mt, ft, ct, _m = find_value_tolerant(text, r["gt"])
            r.update(verdict="PASS" if (me or mt) else "FAIL", found=fe or ft, closest=ce if ce is not None else ct,
                     sql=resp.get("sql"), answer=resp.get("answer") or "")
        _ust()["rob_log"].append(f"{r['id']} V{r['vi']}: {r['verdict']}"
                             + (f" (found {r['found']})" if r["found"] is not None else
                                f" (closest {r['closest']})" if r["closest"] is not None else "")
                             + f" in {r['secs']}s")
    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            for f in as_completed([pool.submit(_one, r) for r in res]):
                try:
                    f.result()
                except Exception as e:
                    _ust()["rob_log"].append(f"call error: {e}")
    finally:
        for r in res:
            if r["verdict"] in ("WAITING", "RUNNING"):
                r["verdict"] = "ERROR"
        _ust()["rob_t_end"] = time.time()
        _ust()["rob_last_user"] = _get_user_from_headers(obo_headers)
        _ust()["rob_running"] = False
        sc = _rob_scores(res)
        log(f"Robustness ({version}): reliability {sum(s['reliability'] for s in sc)/len(sc):.0f}%, "
            f"dependability {sum(s['dependability'] for s in sc)/len(sc):.0f}%")
        try:
            _save_reliability_run(obo_headers)
        except Exception as e:
            log(f"Reliability save error: {e}")


_ROB_CHIP = {"PASS": ("\u2713", "teal"), "FAIL": ("\u2715", "rose"), "ERROR": ("!", "rose"),
             "RUNNING": ("\u2026", "indigo"), "WAITING": ("\u00b7", "muted")}


def _rob_color(v):
    return C["teal"] if v >= 80 else (C["indigo"] if v >= 50 else C["rose"])


def _build_stability_section():
    """Deep Reliability across stages: verdict flips + value stability per test (from this session's runs)."""
    stages = [("Baseline", _ust().get("bench_results"))] + [
        (ITER_INFO[i][0], ST.get(f"iter_it{i+1}_results")) for i in range(3)]
    stages = [(n, r) for n, r in stages if r and not any(x.get("verdict") in ("WAITING", "RUNNING") for x in r)]
    head = html.H3("Cross-iteration stability")
    if len(stages) < 2:
        return html.Div([head, html.P("Needs at least two completed benchmark stages in this app session "
                                      "(Baseline and an iteration) to compare verdict flips and value stability.",
                                      style={"color": C["muted"], "fontSize": "13px"})],
                        className="stage", style={"marginTop": "32px"})
    ok = lambda v: v in ("EXACT", "TOLERANT")
    by = [{r["id"]: r for r in res} for _, res in stages]
    ids = [r["id"] for r in stages[0][1]]
    rows, fixed, regressed, flaky, stable_vals = [], 0, 0, 0, 0
    for tid in ids:
        seq = [ok(b.get(tid, {}).get("verdict")) for b in by]
        flips = sum(1 for a, b in zip(seq, seq[1:]) if a != b)
        vals = {str(b.get(tid, {}).get("closest")) for b in by if ok(b.get(tid, {}).get("verdict"))}
        stable_vals += 1 if len(vals) <= 1 else 0
        if not seq[0] and seq[-1] and flips == 1: kind = "Fixed"; fixed += 1
        elif seq[0] and not seq[-1]: kind = "Regressed"; regressed += 1
        elif flips >= 2: kind = "Flaky"; flaky += 1
        elif all(seq): kind = "Stable \u2713"
        else: kind = "Stable \u2715"
        _kind_color = {"Fixed": C["teal"], "Regressed": C["rose"], "Flaky": C["indigo"],
                       "Stable \u2713": C["muted"], "Stable \u2715": C["muted"]}
        rows.append(html.Tr([html.Td(tid, style={"fontFamily": "var(--mono)", "fontWeight": "600"}),
                             *[html.Td("\u2713" if s else "\u2715", style={"color": C["teal"] if s else C["rose"],
                                                                         "textAlign": "center"}) for s in seq],
                             html.Td(kind, style={"color": _kind_color.get(kind, C["muted"]),
                                                  "fontWeight": "600"})]))
    last_pass = sum(1 for r in stages[-1][1] if ok(r["verdict"]))
    verdict = ("READY \u2014 no regressions and no flaky tests" if regressed == 0 and flaky == 0 and last_pass >= 40
               else "NOT READY \u2014 fix regressions/flaky tests first" if regressed or flaky
               else f"IMPROVING \u2014 accuracy still below {_pass_bar()}/{_NT()}")
    return html.Div([
        head,
        html.Div([Tile(fixed, "FIXED"), Tile(regressed, "REGRESSED", "rose" if regressed else None),
                  Tile(flaky, "FLAKY", "rose" if flaky else None),
                  Tile(f"{stable_vals}/{len(ids)}", "STABLE VALUES"),
                  Tile(f"{last_pass}/{_NT()}", f"LATEST ({stages[-1][0]})", "teal" if last_pass >= _pass_bar() else None)],
                 className="tiles-row"),
        html.P(f"Deploy verdict: {verdict}", style={"fontWeight": "700", "color": C["ink"]}),
        html.Table([html.Thead(html.Tr([html.Th("Test")] + [html.Th(n) for n, _ in stages] + [html.Th("Change")])),
                    html.Tbody(rows or [html.Tr(html.Td("No verdict changes between stages.", colSpan=len(stages) + 2))])],
                   className="tbl"),
    ], className="stage", style={"marginTop": "32px"})


# 3D-style palette: (front, top, side) -- mid-tone colours that read on both light and dark backgrounds
# 3D-style palette: (front, top, side) -- mid-tone colours that read on both light and dark backgrounds
_BAR3D_TONES = {"teal": ("#14B8A6", "#5EEAD4", "#0F766E"), "indigo": ("#6366F1", "#A5B4FC", "#4338CA"),
                "rose": ("#F43F5E", "#FDA4AF", "#BE123C"), "amber": ("#F59E0B", "#FCD34D", "#B45309"),
                "violet": ("#8B5CF6", "#C4B5FD", "#6D28D9"), "slate": ("#94A3B8", "#CBD5E1", "#64748B")}


def _bar3d(cats, series, height=400, ymax=100, labels=None, tickangle=0, ytitle=None):
    """Grouped bars drawn as extruded 3D blocks (front, lit top, shaded side). Zero values still show a slab.
    series: [(name, values, tone)] -- tone is one palette key, or a list with one key per category.
    ymax: scale of the values (100 for percentages, or a count). labels: {name: [text per bar]} overrides values."""
    n, k = len(cats), len(series)
    ymax = float(ymax or 1)
    slot = 0.8 / k
    bw, dx, dy = slot * 0.82, slot * 0.22, ymax * 0.032
    floor = ymax * 0.008
    tone_at = lambda tone, i: tone[i] if isinstance(tone, (list, tuple)) else tone
    fig, shapes = go.Figure(), []
    for i in range(n):
        for j, (_name, vals, tone) in enumerate(series):
            front, top, side = _BAR3D_TONES[tone_at(tone, i)]
            h = max(float(vals[i] or 0), floor)
            x0 = i - 0.4 + j * slot + (slot - bw) / 2
            x1 = x0 + bw
            line = dict(width=0.6, color="rgba(15,23,42,0.25)")
            shapes += [
                dict(type="path", path=f"M {x1},0 L {x1+dx},{dy} L {x1+dx},{h+dy} L {x1},{h} Z",
                     fillcolor=side, line=line, layer="above"),
                dict(type="path", path=f"M {x0},{h} L {x0+dx},{h+dy} L {x1+dx},{h+dy} L {x1},{h} Z",
                     fillcolor=top, line=line, layer="above"),
                dict(type="rect", x0=x0, x1=x1, y0=0, y1=h, fillcolor=front, line=line, layer="above"),
            ]
    for j, (name, vals, tone) in enumerate(series):
        xs = [i - 0.4 + j * slot + slot / 2 + dx / 2 for i in range(n)]
        txt = (labels or {}).get(name) or [f"{float(v or 0):.0f}" for v in vals]
        fig.add_trace(go.Scatter(x=xs, y=[max(float(v or 0), floor) + dy + ymax * 0.015 for v in vals], mode="text",
                                 text=txt, textposition="top center",
                                 textfont=dict(size=11, color=[_BAR3D_TONES[tone_at(tone, i)][2] for i in range(n)],
                                               family="Instrument Sans, system-ui"),
                                 name=name, showlegend=False,
                                 customdata=cats, hovertemplate=f"%{{customdata}} \u2022 {name}: %{{text}}<extra></extra>"))
        if not isinstance(tone, (list, tuple)):
            fig.add_trace(go.Scatter(x=[None], y=[None], mode="markers", name=name,
                                     marker=dict(symbol="square", size=12, color=_BAR3D_TONES[tone][0])))
    step = 20 if ymax == 100 else max(1, round(ymax / 5))
    fig.update_layout(template=PT, height=height, shapes=shapes,
                      margin=dict(t=36, b=36 if not tickangle else 110, l=56, r=16),
                      legend=dict(orientation="h", x=0, y=1.1, font=dict(size=12)),
                      xaxis=dict(tickmode="array", tickvals=list(range(n)), ticktext=cats, range=[-0.6, n - 0.3],
                                 showgrid=False, zeroline=False, tickangle=tickangle, automargin=True,
                                 tickfont=dict(size=12)),
                      yaxis=dict(range=[0, ymax * 1.22], tickvals=list(range(0, int(ymax) + 1, step)),
                                 zeroline=False, title=ytitle),
                      hovermode="closest")
    return fig


def _donut(parts, center_big, center_small, height=330):
    """3D donut: the top ring sits on a stack of darker 'side' layers (same palette as the 3D bars), giving an
    extruded coin look. White slice separators, slight pull on the first slice, two-line centre label.
    parts: [(label, value, hex_colour)] -- zero slices are dropped (their labels would be clipped)."""
    parts = [x for x in parts if x[1] > 0]
    side_of = {v[0].upper(): v[2] for v in _BAR3D_TONES.values()}
    cols = [x[2] for x in parts]
    sides = [side_of.get(c.upper(), c) for c in cols]
    layers, step = 8, 0.008
    d = layers * step
    common = dict(labels=[x[0] for x in parts], values=[x[1] for x in parts], sort=False, direction="clockwise",
                  rotation=90, hole=0.62, pull=[0.04] + [0] * (len(parts) - 1))
    fig = go.Figure()
    for k in range(layers, 0, -1):  # bottom layer first so the top ring is drawn last
        fig.add_trace(go.Pie(**common, domain=dict(x=[0, 1], y=[d - k * step, 1 - k * step]),
                             marker=dict(colors=sides, line=dict(color=sides, width=1)),
                             textinfo="none", hoverinfo="skip", showlegend=False))
    fig.add_trace(go.Pie(**common, domain=dict(x=[0, 1], y=[d, 1]),
                         marker=dict(colors=cols, line=dict(color="rgba(255,255,255,0.95)", width=3)),
                         textinfo="label+value", textposition="inside", insidetextorientation="horizontal",
                         textfont=dict(size=12.5, color="#FFFFFF", family="Instrument Sans,system-ui"),
                         hovertemplate="%{label}: %{value} (%{percent})<extra></extra>"))
    yc = (d + 1) / 2
    fig.update_layout(template=PT, height=height, margin=dict(t=8, b=8, l=8, r=8), showlegend=False,
                      annotations=[dict(text=f"<b>{center_big}</b>", x=0.5, y=yc + 0.04, showarrow=False, font=dict(size=28)),
                                   dict(text=center_small, x=0.5, y=yc - 0.08, showarrow=False, font=dict(size=12))])
    return fig


def _hprogress(rows, height=380):
    """Horizontal 'progress track' per row: rounded track (total) + coloured fill (passed) + 'p/t  pct' label.
    rows: [(label, passed, total, hex_colour)]."""
    ys = [r[0] for r in rows]
    tot = [r[2] for r in rows]
    mx = max(tot + [1])
    fig = go.Figure()
    fig.add_trace(go.Bar(y=ys, x=tot, orientation="h", name="Total", hoverinfo="skip", width=0.62,
                         marker=dict(color="rgba(148,163,184,0.18)", line=dict(width=1, color="rgba(148,163,184,0.35)"),
                                     cornerradius=10)))
    fig.add_trace(go.Bar(y=ys, x=[max(r[1], 0.0001) for r in rows], orientation="h", name="Passed", width=0.62,
                         marker=dict(color=[r[3] for r in rows], line=dict(width=1.2, color="rgba(15,23,42,0.28)"),
                                     cornerradius=10),
                         customdata=[[r[1], r[2]] for r in rows],
                         hovertemplate="%{y}: %{customdata[0]} of %{customdata[1]} passed<extra></extra>"))
    fig.add_trace(go.Scatter(y=ys, x=[t + mx * 0.03 for t in tot], mode="text", showlegend=False, hoverinfo="skip",
                             text=[f"<b>{r[1]}</b>/{r[2]}  {round(100 * r[1] / r[2]) if r[2] else 0}%" for r in rows],
                             textposition="middle right", textfont=dict(size=12, family="Instrument Sans,system-ui")))
    fig.update_layout(template=PT, barmode="overlay", height=height, bargap=0.3,
                      margin=dict(t=30, b=30, l=120, r=20), legend=dict(orientation="h", y=1.08, x=0, font=dict(size=12)),
                      xaxis=dict(range=[0, mx * 1.35], dtick=1 if mx <= 10 else None, showgrid=True, zeroline=False),
                      yaxis=dict(autorange="reversed", showgrid=False))
    return fig


def _rob_heatmap(sc, z):
    """Pass/fail grid with tick marks in the cells; pending cells stay neutral."""
    sym = {1: "\u2713", -1: "\u2715", 0: "\u2026"}
    fig = go.Figure(go.Heatmap(
        z=z, x=["V1", "V2", "V3"], y=[s["id"] for s in sc], zmin=-1, zmax=1, showscale=False,
        colorscale=[[0, "#F43F5E"], [0.5, "rgba(148,163,184,0.35)"], [1, "#14B8A6"]], xgap=4, ygap=4,
        text=[[sym[v] for v in row] for row in z], texttemplate="%{text}",
        textfont=dict(size=15, color="#FFFFFF"),
        customdata=[["PASS" if v == 1 else "FAIL" if v == -1 else "pending" for v in row] for row in z],
        hovertemplate="%{y} %{x}: %{customdata}<extra></extra>"))
    fig.update_layout(template=PT, height=400, margin=dict(t=10, b=30, l=46, r=10),
                      yaxis=dict(autorange="reversed", showgrid=False), xaxis=dict(side="top", showgrid=False))
    return fig


def _build_rob_body():
    res = _ust().get("rob_results")
    if not res:
        return html.Div([html.P("Not run yet \u2014 pick an agent version and click Run Robustness Test.",
                                style={"color": C["muted"]}), _build_stability_section()])
    running = _ust().get("rob_running")
    done = sum(1 for r in res if r["verdict"] in ("PASS", "FAIL", "ERROR"))
    el = int((_ust().get("rob_t_end") or time.time()) - _ust().get("rob_t0", time.time()))
    sc = _rob_scores(res)
    avg_rel = sum(s["reliability"] for s in sc) / len(sc)
    avg_dep = sum(s["dependability"] for s in sc) / len(sc)
    tiles = html.Div([
        Tile(f"{avg_rel:.0f}%", "RELIABILITY", "teal" if avg_rel >= 80 else ("rose" if avg_rel < 50 else None)),
        Tile(f"{avg_dep:.0f}%", "DEPENDABILITY", "indigo" if avg_dep >= 80 else ("rose" if avg_dep < 50 else None)),
        Tile(f"{sum(1 for s in sc if s['reliability'] == 100)}/10", "FULLY RELIABLE"),
        Tile(f"{sum(1 for s in sc if s['dependability'] >= 100)}/10", "IDENTICAL SQL"),
        Tile(f"{done}/{_ROB_TOTAL}", "CALLS DONE"),
    ], className="tiles-row")
    progress = html.Div([
        html.Div([html.Div(className="progress-fill" + ("" if running else " done"),
                           style={"width": f"{int(100 * done / _ROB_TOTAL)}%"})]
                 + ([html.Div(className="progress-shimmer")] if running else []), className="progress-track"),
        html.Div(f"{_ust().get('rob_version')} agents \u2022 {done}/{_ROB_TOTAL} calls \u2022 {el // 60}m {el % 60:02d}s"
                 + (f" \u2022 run by {_ust().get('rob_last_user')}" if _ust().get("rob_last_user") and not running else ""),
                 style={"color": C["muted"], "fontSize": "12px", "fontFamily": "var(--mono)", "marginTop": "4px"}),
    ], style={"marginBottom": "16px"})
    by_id = {}
    for r in res:
        by_id.setdefault(r["id"], []).append(r)
    rows = []
    for s in sc:
        chips = []
        for r in sorted(by_id[s["id"]], key=lambda r: r["vi"]):
            icon, ck = _ROB_CHIP.get(r["verdict"], ("?", "muted"))
            chips.append(html.Details([
                html.Summary(f"V{r['vi']} {icon}", style={"color": C[ck], "fontWeight": "700", "cursor": "pointer"}),
                html.Div([html.P(r["question"], style={"fontStyle": "italic", "margin": "4px 0"}),
                          html.P(f"Expected {r['gt']} \u2022 found {r['found']} \u2022 closest {r['closest']} \u2022 {r['secs']}s",
                                 style={"fontFamily": "var(--mono)", "fontSize": "11.5px", "color": C["muted"], "margin": "2px 0"}),
                          html.Pre(r["sql"] or "(no SQL)", style={"whiteSpace": "pre-wrap", "fontSize": "11px",
                                                                  "maxHeight": "160px", "overflowY": "auto"})],
                         style={"padding": "6px 8px", "border": f"1px solid {C['line']}", "borderRadius": "8px",
                                "background": C["soft"], "maxWidth": "420px"}),
            ]))
        rows.append(html.Tr([
            html.Td(s["id"], style={"fontFamily": "var(--mono)", "fontWeight": "700"}),
            html.Td([html.Div(s["desc"]), html.Div(f"{s['agent'].title()} agent", style={"color": C["muted"], "fontSize": "12px"})]),
            html.Td(html.Div(chips, style={"display": "flex", "gap": "10px", "flexWrap": "wrap", "alignItems": "flex-start"})),
            html.Td(f"{s['reliability']:.0f}%", style={"color": _rob_color(s["reliability"]), "fontWeight": "700"}),
            html.Td([html.Div(f"{s['dependability']}%", style={"color": _rob_color(s["dependability"]), "fontWeight": "700"}),
                     html.Div(s["dep_label"] + (" \u2022 functionally equivalent" if s["func_equiv"] else ""),
                              style={"color": C["muted"], "fontSize": "11.5px"})]),
        ]))
    table = html.Table([html.Thead(html.Tr([html.Th(h) for h in ("Test", "What it probes", "Variations", "Reliability", "Dependability")])),
                        html.Tbody(rows)], className="tbl rob-tbl", style={"width": "100%"})
    charts = html.Div()
    if done:
        zmap = {"PASS": 1, "FAIL": -1, "ERROR": -1}
        z = [[zmap.get(r["verdict"], 0) for r in sorted(by_id[s["id"]], key=lambda r: r["vi"])] for s in sc]
        heat = _rob_heatmap(sc, z)
        bars = _bar3d([s["id"] for s in sc],
                      [("Reliability", [s["reliability"] for s in sc], "teal"),
                       ("Dependability", [s["dependability"] for s in sc], "indigo")])
        head = lambda t, sub: html.Div([html.Div(t, style={"fontFamily": "var(--display)", "fontWeight": "700",
                                                           "fontSize": "15px", "color": C["ink"]}),
                                        html.Div(sub, style={"fontSize": "12px", "color": C["muted"]})],
                                       style={"marginBottom": "4px"})
        charts = html.Div([
            html.Div([head("Variation heatmap", "Each rephrasing: \u2713 hit ground truth, \u2715 missed"),
                      dcc.Graph(figure=heat, config={"displayModeBar": False})],
                     className="stage rob-chart", style={"flex": "1 1 300px", "minWidth": "0"}),
            html.Div([head("Reliability vs Dependability", "Per test, 0\u2013100 \u2014 tall teal + short indigo = right, but by different SQL"),
                      dcc.Graph(figure=bars, config={"displayModeBar": False})],
                     className="stage rob-chart", style={"flex": "2 1 520px", "minWidth": "0"}),
        ], style={"display": "flex", "gap": "20px", "flexWrap": "wrap", "marginTop": "16px"})
    logbox = html.Div(html.Div([html.Div(l, style={"fontFamily": "var(--mono)", "fontSize": "12px"}) for l in _ust().get("rob_log", [])]),
                      style={"maxHeight": "180px", "overflowY": "auto", "display": "flex", "flexDirection": "column-reverse",
                             "border": f"1px solid {C['line']}", "borderRadius": "12px", "padding": "8px 12px",
                             "background": C["soft"], "marginBottom": "16px"})
    return html.Div([tiles, progress, logbox, table, charts, _build_stability_section()])


def _rob_intro():
    """Formatted explanation: what the test does, the two scores, and how to read them."""
    card = {"border": f"1px solid {C['line']}", "borderRadius": "14px", "padding": "14px 18px",
            "background": C["surface"], "flex": "1", "minWidth": "280px"}
    lab = {"fontSize": "11px", "fontWeight": "700", "letterSpacing": "0.06em", "textTransform": "uppercase",
           "marginBottom": "6px"}
    scale = [("100", "Identical SQL", "teal"), ("85", "Same structure", "teal"), ("60", "Mixed approaches", "indigo"),
             ("50", "Only one SQL", "indigo"), ("33", "Different SQL", "rose"), ("0", "No SQL", "rose")]
    chip = lambda v, t, k: html.Span([html.B(v, style={"marginRight": "6px"}), t], style={
        "display": "inline-flex", "alignItems": "center", "fontSize": "12px", "padding": "3px 10px",
        "borderRadius": "999px", "border": f"1px solid {C[k]}", "color": C[k], "background": C[f"{k}_bg"]})
    return html.Div([
        html.P([html.B("Robustness test: "), "the ", html.B("10 hardest questions"), ", each asked ",
                html.B("3 rephrased ways"), " \u2014 ", html.B("30 Genie calls"), " against the agent version you pick."],
               style={"fontSize": "14px", "color": C["ink"], "margin": "0 0 12px"}),
        html.Div([
            html.Div([
                html.Div("Reliability", style={**lab, "color": C["teal"]}),
                html.Div("Is the answer right?", style={"fontWeight": "700", "fontSize": "15px", "marginBottom": "4px"}),
                html.Div("Share of the 3 rephrasings that hit the ground-truth value.",
                         style={"color": C["muted"], "fontSize": "13px"}),
                html.Div([chip("100%", "3 of 3", "teal"), chip("67%", "2 of 3", "indigo"),
                          chip("33%", "1 of 3", "rose"), chip("0%", "none", "rose")],
                         style={"display": "flex", "gap": "6px", "flexWrap": "wrap", "marginTop": "10px"}),
            ], style=card),
            html.Div([
                html.Div("Dependability", style={**lab, "color": C["indigo"]}),
                html.Div("Is it right the same way?", style={"fontWeight": "700", "fontSize": "15px", "marginBottom": "4px"}),
                html.Div("How consistent the generated SQL is across the rephrasings.",
                         style={"color": C["muted"], "fontSize": "13px"}),
                html.Div([chip(v, t, k) for v, t, k in scale],
                         style={"display": "flex", "gap": "6px", "flexWrap": "wrap", "marginTop": "10px"}),
            ], style=card),
        ], style={"display": "flex", "gap": "14px", "flexWrap": "wrap"}),
        html.Div([html.B("How to read it: "), "high reliability with low dependability means the agent got the right "
                  "number by different routes \u2014 correct today, but not locked to one governed approach."],
                 style={"marginTop": "12px", "padding": "10px 14px", "borderLeft": f"3px solid {C['indigo']}",
                        "background": C["indigo_bg"], "borderRadius": "8px", "fontSize": "13px", "color": C["ink"]}),
    ], style={"marginBottom": "18px"})


def page_robustness():
    if not ST.get("app_ready"):
        return _build_loading_page("Loading Reliability")
    versions = list((_ust().get("agent_versions") or {}).keys())
    running = bool(_ust().get("rob_running"))
    default = _ust().get("rob_version") if _ust().get("rob_version") in versions else (versions[-1] if versions else None)
    return html.Div([
        html.H2("Reliability & Dependability"),
        _rob_intro(),
        html.Div([
            dcc.Dropdown(id="rob-version", options=[{"label": f"{v} agents", "value": v} for v in versions],
                         value=default, clearable=False, disabled=running, style={"minWidth": "240px"}),
            html.Button(_rob_button_label(), id="btn-rob", className="btn primary", n_clicks=0,
                        disabled=running or not versions, style={"padding": "10px 28px"}),
            html.Span(id="rob-status", style={"color": C["muted"], "fontSize": "13px"}),
        ], style={"display": "flex", "gap": "12px", "alignItems": "center", "marginBottom": "20px", "flexWrap": "wrap"}),
        html.Div(id="rob-body", children=_build_rob_body()),
        dcc.Interval(id="rob-poll", interval=3000, disabled=not running),
        # One-shot: re-discover agent versions right after the page opens (the cached list can be stale)
        dcc.Interval(id="rob-opts-refresh", interval=400, max_intervals=1),
    ], style={"paddingBottom": "60px"})


def _rob_button_label():
    """'Running... Xm Ys' while the reliability test runs, otherwise the normal label."""
    if not _ust().get("rob_running"):
        return "Run Robustness Test"
    el = int(time.time() - (_ust().get("rob_t0") or time.time()))
    return f"Running\u2026 {el // 60}m {el % 60:02d}s"


@callback(Output("rob-version", "options"), Output("rob-version", "value"),
          Input("rob-opts-refresh", "n_intervals"), State("rob-version", "value"), prevent_initial_call=True)
def refresh_rob_versions(_n, current):
    """Refresh the agent-version list from the Genie API (OBO) so every built version shows on first load."""
    if _ust().get("rob_running"):
        raise PreventUpdate
    try:
        discover_agents(get_headers())
    except Exception as e:
        log(f"refresh_rob_versions: {e}")
        raise PreventUpdate
    versions = list((_ust().get("agent_versions") or {}).keys())
    opts = [{"label": f"{v} agents", "value": v} for v in versions]
    value = current if current in versions else (versions[-1] if versions else None)
    return opts, value


@callback(Output("rob-body", "children"), Output("rob-status", "children"), Output("rob-poll", "disabled"),
          Output("btn-rob", "disabled"), Output({"type": "nav", "index": ALL}, "disabled", allow_duplicate=True),
          Output("btn-rob", "children"),
          Input("btn-rob", "n_clicks"), Input("rob-poll", "n_intervals"), State("rob-version", "value"),
          prevent_initial_call=True)
def handle_rob(n_click, _n, version):
    n_nav = len(ctx.outputs_list[4])   # the nav-buttons output (5th); NOT [-1] -- that is btn-rob children
    if ctx.triggered_id == "btn-rob":
        if not n_click:
            raise PreventUpdate
        if _ust().get("rob_running") or _app_busy_now():
            return no_update, "Another operation is running.", no_update, True, no_update, no_update
        if not version or not (_ust().get("agent_versions") or {}).get(version):
            return no_update, "Pick an agent version first.", True, False, no_update, no_update
        _ust()["rob_running"] = True  # before the thread starts -> nav locks immediately
        _ust()["rob_t0"], _ust()["rob_t_end"] = time.time(), None  # button timer starts now
        _spawn_bg(_run_rob_bg, version, get_headers())
        return _build_rob_body(), f"Running on {version} agents\u2026", False, True, [True] * n_nav, _rob_button_label()
    if _ust().get("rob_running"):
        _rtok = _ust().get("rob_total_tokens") or 0
        _rtxt = f" \u2022 {_rtok:,} tokens so far" if _rtok else ""
        return _build_rob_body(), f"Running on {_ust().get('rob_version') or version} agents\u2026{_rtxt}", False, True, \
            [True] * n_nav, _rob_button_label()
    _rtok = _ust().get("rob_total_tokens") or 0
    _rtxt = f" \u2022 {_rtok:,} tokens" if _rtok else ""
    return _build_rob_body(), f"Complete{_rtxt}", True, False, [_app_busy_now()] * n_nav, _rob_button_label()


def _cmp_done_status(ra, rb):
    pa = sum(1 for r in ra if r["verdict"] in ("EXACT", "TOLERANT"))
    pb = sum(1 for r in rb if r["verdict"] in ("EXACT", "TOLERANT"))
    na = _ust().get("cmp_name_a") or "Supervisor A"
    nb = _ust().get("cmp_name_b") or "Supervisor B"
    _ua = _ust().get("cmp_usage_a") or {}
    _ub = _ust().get("cmp_usage_b") or {}
    _ta = _ua.get("total_tokens") or ((_ua.get("input_tokens") or 0) + (_ua.get("output_tokens") or 0))
    _tb = _ub.get("total_tokens") or ((_ub.get("input_tokens") or 0) + (_ub.get("output_tokens") or 0))
    _tok_part = f" \u2022 Tokens: {na} {_ta:,}, {nb} {_tb:,}" if (_ta or _tb) else ""
    return html.Span(f"Complete \u2014 {na}: {pa}/{len(ra) or _NT()}, {nb}: {pb}/{len(rb) or _NT()}{_tok_part}",
                     style={"color": C["teal"], "fontSize": "13px", "fontWeight": "600"})


def _any_sup_unknown():
    """True when at least one supervisor still has unknown architecture (needs retry)."""
    for s in (_ust().get("supervisors") or []):
        p = _sup_profile(s)
        if p and p["kind"] == "unknown":
            return True
    return False

def page_comparison():
    if not ST.get("app_ready"):
        return _build_loading_page("Loading Comparison")
    sups = _ust().get("supervisors", [])
    # Keep the A/B sides of the last (possibly restored) comparison, so its results stay under the right supervisor
    _eps = _ust().get("cmp_last_eps") or (None, None)
    _by_ep = {s["endpoint"]: s for s in sups}
    if _eps[0] in _by_ep and _eps[1] in _by_ep and _eps[0] != _eps[1]:
        sups = [_by_ep[_eps[0]], _by_ep[_eps[1]]] + [s for s in sups if s["endpoint"] not in _eps]
    sup_a = sups[0] if len(sups) > 0 else None
    sup_b = sups[1] if len(sups) > 1 else None

    # Supervisor info cards
    def sup_card(label, sup, idx):
        if not sup:
            return html.Div([
                html.Span(label, style={"color":C["muted"],"fontSize":"12px","fontWeight":"600"}),
                html.P("No supervisor discovered", style={"color":C["rose"],"fontSize":"13px"}),
            ], className="stage", style={"flex":"1","minWidth":"0"})
        prof = _sup_profile(sup)
        badge = {"multi": ("MULTI-AGENT", C["indigo"], C["indigo_bg"]),
                 "mcp": ("GENIE ONE MCP", C["teal"], C["teal_bg"])}.get(prof["kind"], ("ARCHITECTURE UNKNOWN", C["muted"], C["soft"]))
        return html.Div([
            html.Div([
                html.Span(label, style={"color":C["muted"],"fontSize":"12px","fontWeight":"600",
                                        "textTransform":"uppercase","letterSpacing":"0.05em"}),
                html.Span(badge[0], style={"fontSize":"11px","fontWeight":"700","letterSpacing":"0.05em",
                                           "color":badge[1],"background":badge[2],"borderRadius":"999px",
                                           "padding":"3px 10px"}),
            ], style={"display":"flex","justifyContent":"space-between","alignItems":"center","gap":"10px"}),
            html.H3(prof["title"] or label, style={"marginTop":"4px","marginBottom":"4px",
                                                   "overflowWrap":"anywhere","whiteSpace":"normal"}),
            html.Span(f"Created {prof['created']}", style={"color":C["muted"],"fontSize":"12px"}) if prof["created"] else html.Div(),
            html.Span(f"Model: {prof['model']}", style={"color":C["muted"],"fontSize":"12px","display":"block",
                                                         "marginTop":"2px"}) if prof.get("model") else html.Div(),
            html.P(prof["summary"], style={"color":C["ink"],"fontSize":"13.5px","margin":"10px 0 6px",
                                           "lineHeight":"1.5"}),
            html.Ul([html.Li(b, style={"fontSize":"12.5px","color":C["muted"],"fontFamily":"var(--mono)"})
                     for b in prof["bullets"]], style={"margin":"0","paddingLeft":"18px"}) if prof["bullets"] else html.Div(),
            # Explore link — different label for each architecture
            html.A("Explore Genie One" if prof["kind"] == "mcp" else "Explore Supervisor Agent",
                   href=_supervisor_url(sup), target="_blank",
                   style={"fontSize":"13px","fontWeight":"600","color":C["indigo"],
                          "textDecoration":"none","marginTop":"10px","display":"inline-block"})
            if _supervisor_url(sup) else html.Div(),
        ], className="stage", style={"flex":"1","minWidth":"0"})

    # Pre-rendered grids from previous run (if any)
    results_a = _ust().get("cmp_results_a")
    results_b = _ust().get("cmp_results_b")
    grid_a = _build_bench_grid(results_a or _build_waiting_results(), source="cmp_results_a")
    grid_b = _build_bench_grid(results_b or _build_waiting_results(), source="cmp_results_b")

    cmp_running = _ust().get("cmp_running", False)
    return html.Div([
        html.H2("Supervisor Architecture Comparison"),
        html.P(f"Same CFO prompt sent to both supervisors simultaneously \u2014 side-by-side scoring against {_NT()} ground truth values.",
               style={"color":C["muted"],"marginBottom":"20px"}),
        # Hidden stores for supervisor endpoints
        dcc.Store(id="cmp-a", data=sup_a["endpoint"] if sup_a else None),
        dcc.Store(id="cmp-b", data=sup_b["endpoint"] if sup_b else None),
        # Supervisor info cards
        html.Div([sup_card(_sup_title(sup_a, "Supervisor A"), sup_a, 0),
                  sup_card(_sup_title(sup_b, "Supervisor B"), sup_b, 1)],
                 style={"display":"flex","gap":"20px","marginBottom":"16px"}),
        # The exact CFO prompt, formatted like the workshop HTML
        _build_prompt_box(),
        # Run button
        html.Div([
            html.Button("Comparison running\u2026" if cmp_running else "Run Comparison", id="btn-cmp", className="btn primary",
                        style={"padding":"10px 28px"},
                        disabled=not sup_a or not sup_b or cmp_running),
        ], style={"marginBottom":"20px"}),
        # Side-by-side test grids
        html.Div([
            html.Div([
                html.H4(_sup_title(sup_a, "Supervisor A"),
                         style={"marginBottom":"8px","fontSize":"14px","color":C["indigo"],
                                "overflowWrap":"anywhere","whiteSpace":"normal"}),
                html.Div(id="cmp-live-a", children=_build_cmp_live("a")),
                html.Div(id="cmp-grid-a", children=grid_a),
            ], style={"flex":"1","minWidth":"0"}),
            html.Div([
                html.H4(_sup_title(sup_b, "Supervisor B"),
                         style={"marginBottom":"8px","fontSize":"14px","color":C["teal"],
                                "overflowWrap":"anywhere","whiteSpace":"normal"}),
                html.Div(id="cmp-live-b", children=_build_cmp_live("b")),
                html.Div(id="cmp-grid-b", children=grid_b),
            ], style={"flex":"1","minWidth":"0"}),
        ], style={"display":"flex","gap":"24px","flexWrap":"wrap"}),
        # Progress
        html.Div(id="cmp-status", style={"marginTop":"12px"}, children=_cmp_done_status(results_a, results_b)
                 if (results_a and results_b and not cmp_running) else None),
        # Charts
        html.Div(id="cmp-chart", children=_build_cmp_charts(results_a, results_b, _sup_title(sup_a, "Supervisor A"),
                                                            _sup_title(sup_b, "Supervisor B"))
                 if (results_a and results_b and not cmp_running
                     and all(r.get("verdict") != "RUNNING" for r in results_a + results_b)) else None),
        dcc.Interval(id="cmp-poll", interval=3000, disabled=not cmp_running),  # resume live updates after a page reload
        # Auto-retry: re-discover supervisors until profiles resolve (then self-disables)
        dcc.Interval(id="cmp-sup-poll", interval=5000, disabled=not _any_sup_unknown()),
    ])

@callback(Output("page-refresh", "data", allow_duplicate=True),
          Output("cmp-sup-poll", "disabled"),
          Input("cmp-sup-poll", "n_intervals"),
          prevent_initial_call=True)
def _retry_sup_discovery(n):
    """Auto-retry: re-discover supervisors while any profile is UNKNOWN."""
    if not _any_sup_unknown():
        return no_update, True  # all resolved — stop polling
    h = get_headers()
    if "Authorization" not in h:
        raise PreventUpdate  # no token yet — wait for next interval
    fresh = discover_supervisors(headers=h)
    if fresh:
        _ust()["supervisors"] = fresh
    if _any_sup_unknown():
        raise PreventUpdate  # still unknown — keep polling, don't re-render yet
    log("cmp-sup-poll: supervisor profiles resolved — refreshing comparison page")
    return -int(n or 1), True  # bump page-refresh to re-render, stop polling

@_bg_guard("cmp_running")
def _run_cmp_bg(ea, eb, obo_headers):
    """Background thread: send CFO prompt to both supervisors in parallel."""
    _us = _ust()  # capture per-user state — ThreadPoolExecutor threads lack _thread_alias
    _us["cmp_running"] = True
    _us["cmp_results_a"] = _build_waiting_results()
    _us["cmp_results_b"] = _build_waiting_results()
    # Mark all as RUNNING
    for r in _us["cmp_results_a"]: r["verdict"] = "RUNNING"
    for r in _us["cmp_results_b"]: r["verdict"] = "RUNNING"
    _us["cmp_results_a_elapsed"] = _us["cmp_results_b_elapsed"] = 0
    _cmp_live_reset("a"); _cmp_live_reset("b")
    t0 = time.time()

    def _call_and_score(ep, results_key):
        side = results_key[-1]
        live = ST[f"cmp_live_{side}"]
        try:
            text, secs, usage = call_supervisor_stream(ep, MAIN_PROMPT, headers=obo_headers,
                                                        on_step=_cmp_on_step(side, ep))
        except Exception as e:
            live["status"], live["t_end"] = "error", time.time()
            live["steps"].append((time.time() - live["t0"], "error", f"call failed: {e}"))
            raise
        if _is_supervisor_time_limit(text):
            text = "ERROR:TIME_LIMIT:" + text
        failed = text.startswith("ERROR:") or text == "(empty)"
        if failed:
            live["steps"].append((time.time() - live["t0"], "error", text[:300]))
        elif not live["answer"]:
            live["answer"] = text  # non-streamed fallback: still show the output
        live["status"], live["t_end"] = ("error" if failed else "done"), time.time()
        _us[f"{results_key}_elapsed"] = secs
        _us[f"cmp_usage_{side}"] = usage
        results = _us[results_key]
        if text.startswith("ERROR:") or text == "(empty)":
            # Never score an error message -- digits in it would produce fake passes
            log(f"Cmp {ep}: no supervisor answer -- {text[:300]}")
            for r in results:
                r["verdict"] = "MISSING"
                r["method"] = f"supervisor call failed: {text[:120]}"
                r["closest"] = None
                r["answer"] = text
            return secs
        scored = score_response(text)
        for i, s in enumerate(scored):
            results[i]["verdict"] = s["verdict"]
            results[i]["method"] = s.get("method","")
            results[i]["closest"] = s.get("closest")
            results[i]["answer"] = text  # full supervisor response, shown in the test modal
        return secs

    with ThreadPoolExecutor(max_workers=2) as pool:
        fa = pool.submit(_call_and_score, ea, "cmp_results_a")
        fb = pool.submit(_call_and_score, eb, "cmp_results_b")
        try:
            sa = fa.result()  # no cap: long_task continuations can run well past 10 min
        except Exception as e:
            log(f"Cmp A error: {e}")
            for r in _us["cmp_results_a"]:
                if r["verdict"] == "RUNNING": r["verdict"], r["method"] = "MISSING", f"supervisor call failed: {str(e)[:120]}"
            sa = 0
        try:
            sb = fb.result()
        except Exception as e:
            log(f"Cmp B error: {e}")
            for r in _us["cmp_results_b"]:
                if r["verdict"] == "RUNNING": r["verdict"], r["method"] = "MISSING", f"supervisor call failed: {str(e)[:120]}"
            sb = 0

    elapsed = time.time() - t0
    ra, rb = _us["cmp_results_a"], _us["cmp_results_b"]
    pa = sum(1 for r in ra if r["verdict"] in ("EXACT","TOLERANT"))
    pb = sum(1 for r in rb if r["verdict"] in ("EXACT","TOLERANT"))
    _us["cmp_elapsed"] = elapsed
    _us["cmp_last_ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _us["cmp_last_user"] = _get_user_from_headers(obo_headers)
    _us["cmp_last_eps"] = (ea, eb)
    _us["cmp_running"] = False
    na = _us.get("cmp_name_a") or "A"
    nb = _us.get("cmp_name_b") or "B"
    _ua = _us.get("cmp_usage_a") or {}
    _ub = _us.get("cmp_usage_b") or {}
    _ta = _ua.get("total_tokens") or ((_ua.get("input_tokens") or 0) + (_ua.get("output_tokens") or 0))
    _tb = _ub.get("total_tokens") or ((_ub.get("input_tokens") or 0) + (_ub.get("output_tokens") or 0))
    _tlog = f", tokens: {na}={_ta:,} {nb}={_tb:,}" if (_ta or _tb) else ""
    log(f"Comparison: {na}={pa}/{len(ra)} ({sa:.0f}s), {nb}={pb}/{len(rb)} ({sb:.0f}s), total {elapsed:.0f}s{_tlog}")
    try:
        _save_comparison_run(ea, eb, sa, sb, elapsed, _us["cmp_last_user"], obo_headers)
    except Exception as e:
        log(f"Comparison save error: {e}")

def _build_cmp_charts(ra, rb, sup_a_name, sup_b_name):
    """Build comparison analysis charts."""
    pa = sum(1 for r in ra if r["verdict"] in ("EXACT","TOLERANT"))
    pb = sum(1 for r in rb if r["verdict"] in ("EXACT","TOLERANT"))
    elapsed = _ust().get("cmp_elapsed", 0)
    # Tiles
    tiles = html.Div([
        Tile(f"{pa}/{len(ra)}", sup_a_name, "teal" if pa >= pb else None),
        Tile(f"{pb}/{len(rb)}", sup_b_name, "teal" if pb > pa else None),
        Tile(f"{elapsed:.0f}s", "TOTAL TIME"),
    ], className="tiles-row")
    # Per-group comparison
    gs = list(GROUPS.keys())
    ag = [sum(1 for r in ra if r["id"].startswith(g) and r["verdict"] in ("EXACT","TOLERANT")) for g in gs]
    bg = [sum(1 for r in rb if r["id"].startswith(g) and r["verdict"] in ("EXACT","TOLERANT")) for g in gs]
    labels = [f"{g}: {GROUPS[g][0]}" for g in gs]
    gmax = max([sum(1 for r in ra if r["id"].startswith(g)) for g in gs] + [1])
    fig_grp = _bar3d(labels, [(sup_a_name, ag, "indigo"), (sup_b_name, bg, "teal")], height=480, ymax=gmax,
                     tickangle=-30, ytitle="Tests passed")
    # Venn overlap
    both = sum(1 for a,b in zip(ra,rb)
               if a["verdict"] in ("EXACT","TOLERANT") and b["verdict"] in ("EXACT","TOLERANT"))
    only_a = pa - both
    only_b = pb - both
    neither = len(ra) - pa - pb + both
    _short_a = sup_a_name.split("(")[0].strip()[:20] if len(sup_a_name) > 22 else sup_a_name
    _short_b = sup_b_name.split("(")[0].strip()[:20] if len(sup_b_name) > 22 else sup_b_name
    fig_venn = _donut([("Both", both, "#14B8A6"), (f"Only {_short_a}", only_a, "#6366F1"),
                       (f"Only {_short_b}", only_b, "#F59E0B"),
                       ("Neither", neither, "#F43F5E")], f"{both}/{len(ra)}", "found by both", height=480)
    return html.Div([
        tiles,
        html.Div([
            html.Div([_chart_head("Per-group results", "Ground-truth values each supervisor's brief got right"),
                      dcc.Graph(figure=fig_grp, config={"displayModeBar":False})],
                     className="stage rob-chart", style={"flex":"2","minWidth":"0"}),
            html.Div([_chart_head("Overlap", "Which values both, one or neither supervisor found"),
                      dcc.Graph(figure=fig_venn, config={"displayModeBar":False})],
                     className="stage rob-chart", style={"flex":"1","minWidth":"0"}),
        ], style={"display":"flex","gap":"20px","flexWrap":"wrap"}),
    ])

@callback(Output("cmp-grid-a","children"), Output("cmp-grid-b","children"),
          Output("cmp-poll","disabled"), Output("cmp-status","children"),
          Output("cmp-chart","children"), Output("btn-cmp","disabled"),
          Output({"type":"nav","index":ALL}, "disabled", allow_duplicate=True),
          Input("btn-cmp","n_clicks"), Input("cmp-poll","n_intervals"),
          State("cmp-a","data"), State("cmp-b","data"),
          prevent_initial_call=True)
def handle_cmp(n_click, n_poll, ea, eb):
    triggered = ctx.triggered_id
    _n_nav = len(ctx.outputs_list[-1])
    nav_lock, nav_free = [True] * _n_nav, [_app_busy_now()] * _n_nav
    sups = _ust().get("supervisors", [])
    sup_a_name = _sup_title(sups[0], "Supervisor A") if len(sups) > 0 else "Supervisor A"
    sup_b_name = _sup_title(sups[1], "Supervisor B") if len(sups) > 1 else "Supervisor B"

    if triggered == "btn-cmp":
        if not ea or not eb:
            return no_update, no_update, True, html.Span("No supervisors discovered.", style={"color":C["rose"]}), no_update, False, no_update
        if _ust().get("cmp_running"):
            return no_update, no_update, False, no_update, no_update, True, nav_lock
        if not n_click:  # phantom trigger on re-render must never start a run
            raise PreventUpdate
        _ust()["cmp_running"] = True  # set BEFORE the thread starts so the nav lock is immediate
        _ust()["cmp_name_a"] = sup_a_name
        _ust()["cmp_name_b"] = sup_b_name
        obo_h = get_headers()
        _spawn_bg(_run_cmp_bg, ea, eb, obo_h)
        # Initial grids with all RUNNING
        waiting_a = _build_waiting_results()
        waiting_b = _build_waiting_results()
        for r in waiting_a: r["verdict"] = "RUNNING"
        for r in waiting_b: r["verdict"] = "RUNNING"
        status = html.Span("Sending CFO prompt to both supervisors...",
                           style={"color":C["teal"],"fontSize":"13px","fontStyle":"italic"})
        return _build_bench_grid(waiting_a, source="cmp_results_a"), _build_bench_grid(waiting_b, source="cmp_results_b"), False, status, html.Div(), True, nav_lock

    # Polling
    ra = _ust().get("cmp_results_a")
    rb = _ust().get("cmp_results_b")
    if not ra or not rb:
        raise PreventUpdate
    running = _ust().get("cmp_running", False)
    grid_a = _build_bench_grid(ra, source="cmp_results_a")
    grid_b = _build_bench_grid(rb, source="cmp_results_b")
    if running:
        status = _cmp_status_line((sup_a_name, sup_b_name))
        return grid_a, grid_b, False, status, no_update, True, nav_lock
    # Done
    charts = _build_cmp_charts(ra, rb, sup_a_name, sup_b_name)
    pa = sum(1 for r in ra if r["verdict"] in ("EXACT","TOLERANT"))
    pb = sum(1 for r in rb if r["verdict"] in ("EXACT","TOLERANT"))
    status = _cmp_done_status(ra, rb)
    return grid_a, grid_b, True, status, charts, False, nav_free

@callback(Output("cmp-live-a","children"), Output("cmp-live-b","children"),
          Input("cmp-poll","n_intervals"), prevent_initial_call=True)
def render_cmp_live(_n):
    """Refresh both live supervisor panels while the comparison runs."""
    return _build_cmp_live("a"), _build_cmp_live("b")


# ==== Page 6: Agents (Chat Interface) =======================================
def page_agents():
    if not ST.get("app_ready"):
        return _build_loading_page("Loading Agents")
    agents = _ust().get("agents",{})
    sups = _ust().get("supervisors",[])
    versions = _ust().get("agent_versions") or {}
    # Build combined options: domain agents (every version, newest first) + supervisors
    if versions:
        agent_opts = [{"label":f"Agent: {n.replace('-',' ').title()} \u2014 {v}","value":f"agent:{s}"}
                      for v in reversed(list(versions)) for n, s in versions[v].items()]
    else:
        agent_opts = [{"label":f"Agent: {n.replace('-',' ').title()}","value":f"agent:{s}"} for n,s in agents.items()]
    sup_opts = [{"label":f"Supervisor: {_sup_title(s, chr(65 + k))}","value":f"sup:{s['endpoint']}"} for k, s in enumerate(sups)]
    all_opts = sup_opts + agent_opts
    return html.Div([
        html.H2("Chat with Agents"),
        html.P("Talk to any domain agent or supervisor. Each one keeps its own conversation, so follow-up questions have context. "
               "Replies stream in live, with the steps the agent took.",
               style={"color":C["muted"],"marginBottom":"20px"}),
        html.Div([
            html.Div([
                dcc.Dropdown(id="chat-target", options=all_opts, placeholder="Choose an agent or supervisor...",
                             value=all_opts[0]["value"] if all_opts else None,
                             style={"background":C["soft"],"flex":"1"}),
                html.Button("New conversation", id="btn-chat-new", n_clicks=0, className="btn", style={"flexShrink":"0"}),
                html.Button("Refresh list", id="btn-disc", className="btn", style={"flexShrink":"0"}),
            ], style={"display":"flex","gap":"10px","marginBottom":"14px"}),
            html.Div(id="disc-out", style={"color":C["muted"],"fontSize":"13px","marginBottom":"8px"}),
        ]),
        # Chat area
        html.Div([
            html.Div([
                html.Span(id="chat-dot", className="scr-dot"),
                html.Span("Conversation"),
            ], className="scr-bar"),
            html.Div([
                html.Div(id="chat-msgs", className="chat-msgs",
                         children=_build_chat_view(all_opts[0]["value"] if all_opts else None)),
                html.Div(id="chat-status", className="chat-status"),
                html.Div([
                    dcc.Textarea(id="chat-input", placeholder="Type your question here...",
                                 className="chat-input", style={"height":"48px"}),
                    html.Button("Send", id="btn-chat-send", className="btn primary"),
                ], className="chat-input-row"),
            ], className="chat-wrap"),
        ], className="screen", style={"marginTop":"0"}),
        dcc.Store(id="chat-history", data=[]),
        dcc.Interval(id="chat-poll", interval=1500, disabled=not (_ust().get("chat_run") or {}).get("running")),
    ])

@callback(Output("disc-out","children"), Input("btn-disc","n_clicks"), prevent_initial_call=True)
def disc_click(n):
    h = get_headers()
    _ust()["agents"] = discover_agents(headers=h)
    _ust()["supervisors"] = discover_supervisors(headers=h)
    return f"Found {len(_ust()['agents'])} domain agents, {len(_ust()['supervisors'])} supervisors."

@callback(Output("chat-target","value"), Input("qchat-target","data"), prevent_initial_call=True)
def apply_qchat(target):
    """Pre-select chat target when a Quick Chat button is clicked on Overview."""
    return target if target else no_update

def _chat_genie_stream(sid, question, conversation_id=None, headers=None, on_step=None, timeout=300):
    """Ask a Genie agent (Agent Mode responses API, streamed). Follow-ups reuse conversation_id so the
    agent remembers the conversation. Returns (answer_text, conversation_id)."""
    h = dict(headers or get_headers()); h["Accept"] = "text/event-stream"
    emit = on_step or (lambda k, t: None)
    if "Authorization" not in h:
        return "ERROR:NO_AUTH:no user token available", conversation_id
    payload = {"input": [{"type":"message","role":"user","content":[{"type":"input_text","text":question}]}],
               "stream": True}
    if conversation_id:
        payload["conversation_id"] = conversation_id
    r = None
    for attempt in range(3):
        r = requests.post(f"{HOST}/api/2.0/genie/agents/{sid}/responses", headers=h, json=payload,
                          stream=True, timeout=(15, timeout))
        if r.status_code == 429 and attempt < 2:
            emit("progress", "rate limited -- retrying"); time.sleep(10 * (attempt + 1)); continue
        break
    if r.status_code != 200:
        return f"ERROR:{r.status_code}:{r.text[:300]}", conversation_id
    r.encoding = "utf-8"
    answer, outputs, seen = "", [], set()
    for line in r.iter_lines(decode_unicode=True):
        if not line or not line.startswith("data:"):
            continue
        pl = line[5:].strip()
        if pl == "[DONE]":
            break
        try:
            ev = json.loads(pl)
        except Exception:
            continue
        et, it = ev.get("type", ""), ev.get("item") or {}
        if et == "response.created":
            conversation_id = (ev.get("response") or {}).get("conversation_id") or conversation_id
        if et != "response.output_item.done" or (it.get("type"), it.get("id")) in seen:
            continue
        seen.add((it.get("type"), it.get("id")))
        t = it.get("type")
        if t == "reasoning":
            txt = " ".join(ct.get("text", "") for ct in it.get("content", []))
            if txt.strip(): emit("progress", f"Thinking: {txt.strip()}")
        elif t == "function_call":
            try:
                a = json.loads(it.get("arguments") or "{}")
            except Exception:
                a = {}
            label = a.get("title") or it.get("name", "tool")
            emit("call", f"{label}" + (f"\n{a['query']}" if a.get("query") else ""))
        elif t == "function_call_output":
            out = str(it.get("output") or "")
            outputs.append(out); emit("result", out)
        elif t == "message" and it.get("role") == "assistant":
            answer = "".join(ct.get("text", "") for ct in it.get("content", []) if ct.get("type") == "output_text")
    return (answer or (outputs[-1] if outputs else "(empty response)")), conversation_id


def _chat_session(target):
    return _ust().setdefault("chat_sessions", {}).setdefault(target, {"history": [], "genie_cid": None})


@_bg_guard(on_fail=_chat_fail)
def _chat_bg(target, question, obo_headers):
    """Background chat turn -- the Dash callback returns immediately; the UI polls _ust()['chat_run']."""
    sess = _chat_session(target)
    run = _ust()["chat_run"]
    def on_step(kind, text):
        if kind == "answer":
            run["answer"] += text; return
        if kind == "call":
            run["calls"] += 1
            tool, sep, rest = text.partition(":")
            if target.startswith("sup:") and sep:
                text = f"Asking {_pretty_tool(tool.strip())}: {rest.strip()}"
        run["steps"].append((time.time() - run["t0"], kind, text))
    try:
        if target.startswith("sup:"):
            convo = [{"role": "assistant" if m["role"] == "agent" else "user", "content": m["text"]}
                     for m in sess["history"] if m["role"] in ("user", "agent")]
            resp, elapsed, usage = call_supervisor_stream(target[4:], convo, headers=obo_headers, on_step=on_step)
            run["usage"] = usage
            if _is_supervisor_time_limit(resp):
                resp = "ERROR:TIME_LIMIT:" + resp
        else:
            resp, cid = _chat_genie_stream(target[6:], question, sess.get("genie_cid"), headers=obo_headers,
                                           on_step=on_step)
            sess["genie_cid"] = cid
        if resp.startswith("ERROR:"):
            sess["history"].append({"role": "error", "text": _chat_error_text(resp)})
            run["status"] = "error"
        else:
            sess["history"].append({"role": "agent", "text": resp})
            run["status"] = "done"
    except Exception as e:
        sess["history"].append({"role": "error", "text": f"Request failed: {e}"})
        run["status"] = "error"
    run["t_end"] = time.time()
    run["running"] = False
    log(f"Chat reply ({run['status']}) from {target} in {run['t_end'] - run['t0']:.0f}s")


def _chat_error_text(resp):
    if resp.startswith("ERROR:TIME_LIMIT:"):
        return ("The supervisor exhausted all continuation rounds (~290s each) without finishing. "
                "Try a narrower question, or send 'wrap up' to get a partial answer.")
    if resp.startswith("ERROR:NO_AUTH"):
        return "No user token available -- reload the page and try again."
    return "The agent returned an error: " + resp.split(":", 2)[-1][:300]


def _build_chat_view(target):
    """Conversation + (while running) a live steps panel."""
    sess = _chat_session(target) if target else {"history": []}
    msgs = [_render_message(m["role"], m["text"]) for m in sess["history"]]
    run = _ust().get("chat_run") or {}
    if run.get("target") == target and (run.get("running") or run.get("steps")):
        el = int((run.get("t_end") or time.time()) - run["t0"])
        rows = [html.Div([
                    html.Span(f"{int(t):>3}s", style={"fontFamily":"var(--mono)","fontSize":"11px","color":C["muted"],
                                                     "minWidth":"36px"}),
                    html.Span(_CMP_STEP_STYLE.get(k, ("\u2022", "muted"))[0],
                              style={"color":C[_CMP_STEP_STYLE.get(k, ("", "muted"))[1]],"minWidth":"14px"}),
                    (dcc.Markdown(_pretty_md(_tableize_result(x if len(x) <= 4000 else x[:4000])),
                                  className="cmp-md cmp-md-sm", style={"flex":"1","minWidth":"0"})
                     if _is_md_table(x) else
                     html.Span(x if len(x) <= 600 else x[:600] + " \u2026",
                               style={"whiteSpace":"pre-wrap","wordBreak":"break-word","fontSize":"12.5px"})),
                ], style={"display":"flex","gap":"8px","padding":"2px 0"}) for t, k, x in run["steps"]]
        _cu = run.get("usage") or {}
        _ctot = _cu.get("total_tokens") or ((_cu.get("input_tokens") or 0) + (_cu.get("output_tokens") or 0))
        _ctxt = f" \u2022 {_ctot:,} tokens" if _ctot else ""
        head = (f"Working\u2026 {el}s \u2022 {run.get('calls', 0)} tool call(s)" if run.get("running")
                else f"Steps of the last reply ({el}s{_ctxt})")
        live = html.Details([
            html.Summary(head, style={"cursor":"pointer","fontSize":"12.5px","color":C["muted"],"fontWeight":"600"}),
            html.Div(html.Div(rows or [html.Span("Waiting for the first step\u2026",
                                                 style={"color":C["muted"],"fontSize":"12.5px"})]),
                     style={"maxHeight":"260px","overflowY":"auto","display":"flex","flexDirection":"column-reverse",
                            "border":f"1px solid {C['line']}","borderRadius":"10px","padding":"8px 10px",
                            "background":C["soft"],"marginTop":"6px"}),
        ], open=bool(run.get("running")), style={"margin":"6px 0"})
        msgs.append(live)
        if run.get("running") and run.get("answer"):
            msgs.append(_render_message("agent", run["answer"] + " \u2026"))
    if not msgs:
        msgs = [html.Div("Select an agent or supervisor above and start chatting.",
                         style={"color":C["muted"],"textAlign":"center","padding":"40px"})]
    return msgs


def _call_genie_agent(sid, question):
    """Call a Genie Agent (domain) and return response text."""
    h = get_headers()
    cr = requests.post(f"{HOST}/api/2.0/genie/spaces/{sid}/conversations", headers=h, json={})
    if cr.status_code not in (200,201): return f"ERROR creating conversation: {cr.status_code} {cr.text[:100]}"
    cid = cr.json().get("conversation_id") or cr.json().get("id")
    mr = requests.post(f"{HOST}/api/2.0/genie/spaces/{sid}/conversations/{cid}/messages",
                       headers=h, json={"content":question})
    if mr.status_code not in (200,201): return f"ERROR sending message: {mr.status_code} {mr.text[:100]}"
    mid = mr.json().get("message_id") or mr.json().get("id")
    t0 = time.time()
    while time.time()-t0 < 180:
        pr = requests.get(f"{HOST}/api/2.0/genie/spaces/{sid}/conversations/{cid}/messages/{mid}", headers=h)
        if pr.status_code == 200:
            st = pr.json().get("status","")
            if st in ("COMPLETED","completed"):
                parts = []
                for att in pr.json().get("attachments",[]):
                    if "text" in att: parts.append(att["text"].get("content",""))
                    if "query" in att:
                        sql = att["query"].get("query","")
                        desc = att["query"].get("description","")
                        if desc: parts.append(desc)
                        if sql: parts.append(f"```sql\n{sql}\n```")
                return "\n\n".join(parts) if parts else "(empty response)"
            if st in ("FAILED","failed","CANCELLED"): return f"Agent returned: {st}"
        time.sleep(3)
    return "Timeout after 180s"

def ask_genie(space_id, question, headers=None, timeout_secs=300):
    """Send question to Genie Agent via Agent Mode API (SSE stream).
    Returns dict with 'status', 'answer', 'sql'."""
    h = headers or get_headers()
    h_copy = dict(h)
    h_copy["Accept"] = "text/event-stream"
    payload = {
        "input": [{"type":"message","role":"user",
                    "content":[{"type":"input_text","text":question}]}]
    }
    for attempt in range(4):
        try:
            resp = requests.post(f"{HOST}/api/2.0/genie/agents/{space_id}/responses",
                                 headers=h_copy, json=payload, stream=False, timeout=timeout_secs)
        except requests.exceptions.Timeout:
            return {"status":"ERROR","answer":"timeout","sql":None}
        if resp.status_code == 429:
            wait = 15 * (2 ** attempt)
            time.sleep(wait)
            continue
        # Retry 404 — newly created agents need propagation time
        if resp.status_code == 404 and attempt < 3:
            time.sleep(15)
            continue
        break
    else:
        return {"status":"ERROR","answer":"rate-limited/unavailable after retries","sql":None}
    if resp.status_code != 200:
        return {"status":"ERROR","answer":f"HTTP {resp.status_code}: {resp.text[:300]}","sql":None}
    resp.encoding = "utf-8"  # SSE has no charset header; default latin-1 garbles em dashes etc.
    all_json = []
    for line in resp.text.split("\n"):
        line = line.strip()
        if line.startswith("data:"):
            try: all_json.append(json.loads(line[5:].strip()))
            except: pass
    sql_queries, answer, fn_outputs = [], "", []
    _seen_items, _seen_texts = set(), set()
    def _extract(item):
        nonlocal answer
        itype = item.get("type","")
        # The same item arrives in several SSE events (output_item.done + response.completed) -- count it once
        ikey = (itype, item.get("id") or json.dumps(item, sort_keys=True, default=str))
        if ikey in _seen_items:
            return
        _seen_items.add(ikey)
        if itype == "function_call":
            try:
                args = json.loads(item.get("arguments","{}")) if isinstance(item.get("arguments"), str) else item.get("arguments",{})
                sql = args.get("query", args.get("sql",""))
                if sql and sql.strip(): sql_queries.append(sql)
            except: pass
        elif itype == "function_call_output":
            out = item.get("output","")
            if out: fn_outputs.append(out)
        elif itype == "message" and item.get("role") == "assistant":
            for ct in item.get("content",[]):
                if ct.get("type") == "output_text" and ct.get("text") and ct["text"] not in _seen_texts:
                    _seen_texts.add(ct["text"])
                    answer += ct["text"]
    for data in all_json:
        if "item" in data: _extract(data["item"])
        for out_item in data.get("response",{}).get("output",[]): _extract(out_item)
    full_text = answer
    for out in fn_outputs: full_text += "\n" + out
    for sql in sql_queries: full_text += "\n" + sql
    # Extract usage from SSE response events
    usage = {}
    for data in all_json:
        u = data.get("response", {}).get("usage", {})
        if u:
            usage = u
    return {"status":"COMPLETED","answer":full_text,"sql":sql_queries[-1] if sql_queries else None,"usage":usage}


def _render_message(role, text):
    """Render a chat message as a styled bubble."""
    cls = f"chat-bubble {role}"
    # For agent messages, try to render tables and code blocks nicely
    if role == "agent":
        return html.Div(dcc.Markdown(text or "(empty response)", link_target="_blank"), className=cls)
    if role == "error":
        return html.Div(text, className="chat-bubble agent", style={"color": C["rose"]})
    else:
        return html.Div(text, className=cls)

@callback(
    Output("chat-msgs","children"),
    Output("chat-history","data"),
    Output("chat-input","value"),
    Output("chat-status","children"),
    Output("chat-poll","disabled", allow_duplicate=True),
    Output("btn-chat-send","disabled", allow_duplicate=True),
    Input("btn-chat-send","n_clicks"),
    State("chat-input","value"),
    State("chat-target","value"),
    State("chat-history","data"),
    prevent_initial_call=True,
)
def chat_send(n_click, msg, target, history):
    if not n_click:  # phantom trigger on re-render
        raise PreventUpdate
    if not msg or not msg.strip() or not target:
        return no_update, no_update, no_update, "Type a question and choose an agent first.", no_update, no_update
    if (_ust().get("chat_run") or {}).get("running"):
        return no_update, no_update, no_update, "Still waiting for the previous reply\u2026", False, True
    msg = msg.strip()
    sess = _chat_session(target)
    sess["history"].append({"role":"user","text":msg})
    _ust()["chat_run"] = {"target": target, "running": True, "steps": [], "answer": "", "calls": 0,
                      "t0": time.time(), "t_end": None, "status": "running"}
    log(f"Chat: {msg[:50]} -> {target}")
    _spawn_bg(_chat_bg, target, msg, get_headers())
    return _build_chat_view(target), no_update, "", "Sent \u2014 waiting for the reply\u2026", False, True


@callback(Output("chat-msgs","children", allow_duplicate=True),
          Output("chat-status","children", allow_duplicate=True),
          Output("chat-poll","disabled"),
          Output("btn-chat-send","disabled"),
          Input("chat-poll","n_intervals"),
          State("chat-target","value"), prevent_initial_call=True)
def chat_poll(_n, target):
    """Live view of the running chat turn; stops polling once the reply has arrived."""
    run = _ust().get("chat_run") or {}
    if run.get("running"):
        el = int(time.time() - run["t0"])
        return _build_chat_view(target), f"Waiting for the reply\u2026 {el}s", False, True
    status = ""
    if run.get("t_end") and run.get("target") == target:
        _pu = run.get("usage") or {}
        _ptot = _pu.get("total_tokens") or ((_pu.get("input_tokens") or 0) + (_pu.get("output_tokens") or 0))
        _ptxt = f" \u2022 {_ptot:,} tokens" if _ptot else ""
        status = (f"Replied in {run['t_end'] - run['t0']:.0f}s{_ptxt}" if run.get("status") == "done"
                  else "The last request failed \u2014 see the message above.")
    return _build_chat_view(target), status, True, False


@callback(Output("chat-msgs","children", allow_duplicate=True),
          Output("chat-status","children", allow_duplicate=True),
          Input("chat-target","value"), prevent_initial_call=True)
def chat_switch_target(target):
    """Each agent/supervisor keeps its own conversation."""
    return _build_chat_view(target), ""


@callback(Output("chat-msgs","children", allow_duplicate=True),
          Output("chat-status","children", allow_duplicate=True),
          Input("btn-chat-new","n_clicks"), State("chat-target","value"), prevent_initial_call=True)
def chat_new_conversation(n, target):
    if not n or not target:
        raise PreventUpdate
    if (_ust().get("chat_run") or {}).get("running"):
        return no_update, "Wait for the current reply before starting a new conversation."
    _ust().setdefault("chat_sessions", {})[target] = {"history": [], "genie_cid": None}
    return _build_chat_view(target), "New conversation started."

# ==== Page 7: Logs ==========================================================
def page_logs():
    return html.Div([
        html.H2("Activity Log"),
        Screen("Log", "log-body", dot=False),
    ])

@callback(Output("log-body","children"), Input("tab","data"))
def show_logs(tab):
    if tab != "logs": return no_update
    return "\n".join(ST.get("logs",["No activity yet."]))

# ==== Run ===================================================================
if __name__ == "__main__":
    # OBO-only: no discovery at startup (no logged-in user yet).
    # State populates on first user request via auto-refresh callback.
    log("Startup: OBO-only — discovery runs on first user request")
    port = int(os.getenv("DATABRICKS_APP_PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
