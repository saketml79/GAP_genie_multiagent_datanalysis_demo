"""Genie Curator Workbench — Databricks App (Dash)

Replicates the Supply Chain Control Tower Curator Workbench notebook
as an interactive web application with the Gap workshop dark theme.
"""
import os, time, json, re, threading
from datetime import datetime

import dash
from dash import html, dcc, Input, Output, State, callback, ctx, ALL, no_update
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
import requests

# ── Config ────────────────────────────────────────────────────────────────
CATALOG = os.getenv("CATALOG_NAME", "GAP_Demo_Dev")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "bf50738cf2819197")

# Auth via Databricks App OAuth
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    HOST = w.config.host.rstrip("/")
    def get_headers():
        h = w.config.authenticate()
        h["Content-Type"] = "application/json"
        return h
except Exception:
    HOST = os.getenv("DATABRICKS_HOST", "")
    TOKEN = os.getenv("DATABRICKS_TOKEN", "")
    def get_headers():
        return {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# ── CSS Theme (from Gap_workshp_.html) ────────────────────────────────────
CSS_VARS = {
    "bg": "#0D1324", "surface": "#151D33", "soft": "#1D2743",
    "ink": "#E8ECF7", "muted": "#9AA5C0", "line": "#2A3555",
    "glass": "rgba(13,19,36,.88)",
    "rose": "#FF7C93", "rose_bg": "#3A1A27",
    "teal": "#3DD9C4", "teal_bg": "#10322F",
    "indigo": "#9AA1FF", "indigo_bg": "#222A5A",
}
FONTS = "https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,500..800&family=Instrument+Sans:wght@400..700&family=JetBrains+Mono:wght@400;500&display=swap"

# ── 45 Ground Truth Assumptions ───────────────────────────────────────────
ASSUMPTIONS = [
    ("A01","logistics","on-time delivery rate",5.43,"MV: on_time_delivery_rate","Iter2"),
    ("A02","logistics","late delivery rate",94.57,"MV: late_delivery_rate","Iter2"),
    ("A03","logistics","avg delay days",2.94,"MV: avg_delay_days","Iter2"),
    ("A04","logistics","total shipments",1086,"MV: total_shipments","Iter2"),
    ("A05","logistics","late shipments",1027,"MV: late_shipments","Iter2"),
    ("A06","logistics","wasted freight",2484985.57,"MV: wasted_freight_cost","Iter2"),
    ("B01","demand","August revenue",3341062.58,"MV: revenue_last_month","Iter2"),
    ("B02","demand","July revenue",4581392.7,"MV: revenue_prior_month","Iter2"),
    ("B03","demand","revenue change $",-1240330.12,"MV: revenue_change_dollars","Iter2"),
    ("B04","demand","revenue change %",-27.07,"MV: revenue_change_pct","Iter2"),
    ("C01","inventory","positions below SS",109,"MV: positions_below_safety_stock","Iter2"),
    ("C02","inventory","unique SKUs below SS",61,"MV: unique_skus_below_safety","Iter2"),
    ("C03","inventory","stockout positions",35,"MV: stockout_positions","Iter2"),
    ("C04","inventory","stocked-out SKUs",33,"MV: unique_skus_in_stockout","Iter2"),
    ("C05","inventory","days of supply",0.96,"MV: avg_days_of_supply","Iter2"),
    ("D01","supplier","total POs",48,"MV: total_purchase_orders","Iter2"),
    ("D02","supplier","late POs",36,"MV: late_purchase_orders","Iter2"),
    ("D03","supplier","vendor late %",75.0,"MV: supplier_late_rate_pct","Iter2"),
    ("D04","supplier","avg LTV",8.69,"MV: avg_lead_time_variance","Iter1"),
    ("D05","supplier","Asia late %",100.0,"MV: supplier_late_rate_pct (Asia)","Iter1"),
    ("D06","supplier","Asia LTV",13.67,"MV: avg_lead_time_variance (Asia)","Iter1"),
    ("D07","supplier","Asia POs",30,"MV: total_purchase_orders (Asia)","Iter2"),
    ("E01","cross","fill rate",80.7,"fill rate = service_level_pct","Iter2"),
    ("E02","cross","SLA penalties",1185043.1,"SUM(penalty_amount)","Iter2"),
    ("E03","cross","total CoD",3757298.31,"CoD view","Iter2"),
    ("F01","indirect","West revenue",3341062.58,"West -> Western mapping","Iter1"),
    ("F02","indirect","vendor late %",75.0,"per-order aggregation","Iter1"),
    ("F03","indirect","fulfilled orders",1342,"Fulfilled order count","Iter2"),
    ("F04","indirect","cancelled revenue",179419.26,"Cancelled revenue","Iter2"),
    ("F05","indirect","backordered count",275,"Backordered order count","Iter2"),
    ("F06","indirect","Home Goods decline",349062.88,"Worst product family","Iter1"),
    ("H01","hard","Europe LTV",0.38,"supplier_lead_times join","Iter1"),
    ("H02","hard","NA LTV",0.4,"supplier_lead_times join","Iter1"),
    ("H03","hard","order fulfillment rate",71.23,"Status ambiguity","Iter1"),
    ("H04","hard","partially fulfilled rate",9.02,"Partially_Fulfilled exact","Iter2"),
    ("H05","hard","revenue at risk",3138569.66,"demand + logistics join","Iter2"),
    ("H06","hard","per-SKU risk",14368.63,"inventory + demand join","Iter2"),
    ("H07","hard","CoD/revenue ratio",1.12,"CoD / revenue ratio","Iter2"),
    ("G01","fiscal","Q1 SL target",92.0,"fiscal_targets table","Iter3"),
    ("G02","fiscal","Q1 SL target value",92.0,"fiscal_targets table","Iter3"),
    ("P01","policy","logistics risk shipments",176,"delay>=5 AND weight>800","Iter3"),
    ("P02","policy","demand anomaly orders",77,"qty>=8 AND price<30 AND Online","Iter3"),
    ("P03","policy","inventory risk positions",106,"dos 1-11 AND below_ss AND on_hand>0","Iter3"),
    ("P04","policy","quality minimum orders",11,"quality<75 AND ltv>12","Iter3"),
    ("P05","policy","disruption threshold",3,"risk<55 AND ltv>8 AND penalty>80K","Iter3"),
]

GROUP_META = {
    "A": ("Logistics", CSS_VARS["indigo"]),
    "B": ("Demand", CSS_VARS["teal"]),
    "C": ("Inventory", CSS_VARS["rose"]),
    "D": ("Supplier", "#F59E0B"),
    "E": ("Cross-Domain", "#8B5CF6"),
    "F": ("Indirect", "#6366F1"),
    "G": ("Fiscal", "#10B981"),
    "H": ("Hard", CSS_VARS["rose"]),
    "P": ("Policy", CSS_VARS["teal"]),
}

MAIN_PROMPT = (
    "We need a complete supply chain health check for our West region in August 2026. "
    "The CFO wants to understand what drove the revenue decline versus July \u2014 "
    "show the actual August and July revenue numbers, the dollar change, and the percentage change \u2014 "
    "and which product families are most at fault. "
    "Are our on-time delivery rate and average delay for West region shipments contributing to the problem? "
    "How many total shipments went out and how many were late? "
    "I also need our current fill rate, how many inventory positions are sitting below safety stock "
    "in the West region, how many unique SKUs are affected, what is our days of supply for those at-risk items, "
    "and how many SKUs are completely stocked out. "
    "On the vendor side: what percentage of vendors delivered late in August, "
    "how many purchase orders were late out of total, what is the average lead time variance, "
    "and what are the total vendor SLA penalties we have incurred? "
    "Bring it all together as our total Cost of Disruption by region for last month Aug 26 \u2014 "
    "cancelled revenue, at-risk backorder revenue, wasted freight on late shipments, "
    "and supplier penalty exposure in one number per region. "
    "Additionally, how many shipments in August were flagged under the Logistics Risk Standards, "
    "how many Western region orders in August triggered a Demand Anomaly Alert, "
    "how many inventory positions are classified as supply-risk under Inventory Standards, "
    "how many supplier orders last month fell below the Procurement Quality Minimum, "
    "and how many suppliers exceeded the Executive Disruption Threshold? "
    "Are we going to miss our Q1 service-level targets, and what are the top actions we should take?"
)

# ── Scorers ───────────────────────────────────────────────────────────────
def find_value_in_text(text, expected):
    """Exact 2-decimal match scorer."""
    exp_str = str(expected)
    if exp_str in text:
        return True, expected, expected
    # Try common formats
    try:
        exp_f = float(expected)
        candidates = [
            f"{exp_f:,.2f}", f"{exp_f:.2f}", f"{abs(exp_f):,.2f}",
            f"{exp_f:,.0f}", f"{exp_f:.0f}", str(int(exp_f)) if exp_f == int(exp_f) else None,
        ]
        for c in candidates:
            if c and c in text:
                return True, exp_f, exp_f
    except (ValueError, TypeError):
        pass
    # Find closest number
    numbers = re.findall(r'-?[\d,]+\.?\d*', text)
    best_diff, best_val = float('inf'), None
    try:
        exp_f = float(expected)
        for ns in numbers:
            try:
                val = float(ns.replace(",", ""))
                diff = abs(val - exp_f)
                if diff < best_diff:
                    best_diff, best_val = diff, val
            except ValueError:
                continue
    except (ValueError, TypeError):
        pass
    return False, None, best_val


def find_value_tolerant(text, expected, tolerance=0.005):
    """Match within 0.5% relative tolerance."""
    match, found, closest = find_value_in_text(text, expected)
    if match:
        return True, found, closest, "EXACT"
    exp_val = float(expected)
    numbers = re.findall(r'-?[\d,]+\.?\d*', text)
    best_diff, best_val = float('inf'), closest
    for ns in numbers:
        try:
            val = float(ns.replace(",", ""))
            if exp_val == 0:
                if val == 0:
                    return True, val, val, "EXACT_ZERO"
                continue
            rd = abs(val - exp_val) / abs(exp_val)
            if rd < best_diff:
                best_diff, best_val = rd, val
            if rd <= tolerance:
                return True, val, val, f"TOLERANT({rd:.4%})"
        except ValueError:
            continue
    return False, None, best_val, f"MISS"


# ── API Helpers ───────────────────────────────────────────────────────────
def discover_agents(label="Iteration 3"):
    """Find SC agents matching a label."""
    headers = get_headers()
    resp = requests.get(f"{HOST}/api/2.0/genie/spaces", headers=headers)
    found = {}
    for s in resp.json().get("spaces", []):
        title = s.get("title", "")
        if "SC" not in title or label not in title:
            continue
        sid = s["space_id"]
        for key, kw in [("demand","Demand"),("inventory","Inventory"),("logistics","Logistics"),("supplier","Supplier"),("executive","Executive")]:
            if kw in title:
                found[key] = sid
    return found


def discover_supervisors():
    """Find all Supply Chain supervisors."""
    headers = get_headers()
    resp = requests.get(f"{HOST}/api/2.1/supervisor-agents", headers=headers)
    result = []
    for s in resp.json().get("supervisor_agents", []):
        if "Supply Chain" in s.get("display_name", ""):
            result.append({
                "name": s["name"],
                "display_name": s["display_name"],
                "endpoint": s.get("endpoint_name", ""),
                "description": s.get("description", "")[:100],
            })
    return result


def call_supervisor(endpoint_name, prompt, max_tokens=8192, timeout=600):
    """Call a supervisor endpoint and return (text, elapsed_seconds)."""
    headers = get_headers()
    t0 = time.time()
    resp = requests.post(
        f"{HOST}/serving-endpoints/{endpoint_name}/invocations",
        headers=headers,
        json={"input": [{"role": "user", "content": prompt}], "max_tokens": max_tokens},
        timeout=timeout,
    )
    elapsed = time.time() - t0
    if resp.status_code != 200:
        return f"ERROR:{resp.status_code}:{resp.text[:300]}", elapsed
    texts = []
    for item in resp.json().get("output", []):
        if item.get("role") != "assistant":
            continue
        for c in item.get("content", []):
            if c.get("type") == "output_text" and c.get("text"):
                texts.append(c["text"])
    return "\n".join(texts) if texts else "(empty response)", elapsed


def ask_genie(space_id, question, timeout=120):
    """Send a question to a Genie Agent and return the response text."""
    headers = get_headers()
    cr = requests.post(
        f"{HOST}/api/2.0/genie/spaces/{space_id}/conversations",
        headers=headers, json={},
    )
    if cr.status_code not in (200, 201):
        return f"ERROR: conversation create {cr.status_code}"
    conv_id = cr.json().get("conversation_id") or cr.json().get("id")
    mr = requests.post(
        f"{HOST}/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages",
        headers=headers, json={"content": question},
    )
    if mr.status_code not in (200, 201):
        return f"ERROR: message send {mr.status_code}"
    msg_id = mr.json().get("message_id") or mr.json().get("id")
    # Poll for completion
    t0 = time.time()
    while time.time() - t0 < timeout:
        pr = requests.get(
            f"{HOST}/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}",
            headers=headers,
        )
        if pr.status_code != 200:
            time.sleep(3)
            continue
        status = pr.json().get("status", "")
        if status in ("COMPLETED", "completed"):
            # Extract text
            parts = []
            for att in pr.json().get("attachments", []):
                if "text" in att:
                    parts.append(att["text"].get("content", ""))
                if "query" in att:
                    q = att["query"]
                    parts.append(f"SQL: {q.get('query', '')}")
                    desc = q.get("description", "")
                    if desc:
                        parts.append(desc)
            return "\n".join(parts) if parts else str(pr.json())
        if status in ("FAILED", "failed", "CANCELLED"):
            return f"ERROR: {status}"
        time.sleep(3)
    return "ERROR: timeout"


def score_response(response_text):
    """Score a response against all 45 assumptions. Returns list of dicts."""
    results = []
    for aid, agent_key, question, expected, desc, fix in ASSUMPTIONS:
        match_e, found_e, closest_e = find_value_in_text(response_text, expected)
        match_t, found_t, closest_t, method = find_value_tolerant(response_text, expected)
        verdict = "EXACT" if match_e else ("TOLERANT" if match_t else "MISSING")
        group = aid[0]
        results.append({
            "id": aid, "group": group, "group_name": GROUP_META.get(group, ("",""))[0],
            "agent": agent_key, "question": question, "expected": expected,
            "desc": desc, "fix": fix, "verdict": verdict, "method": method,
            "closest": found_e or found_t or closest_t,
        })
    return results


# ── App State (in-memory) ─────────────────────────────────────────────────
app_state = {
    "agents": {},
    "supervisors": [],
    "benchmark_results": {},     # label -> results list
    "comparison_results": {},    # label -> {text, time, scores}
    "logs": [],
}

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    app_state["logs"].append(f"[{ts}] {msg}")
    if len(app_state["logs"]) > 200:
        app_state["logs"] = app_state["logs"][-100:]


# ── Dash App ──────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    external_stylesheets=[FONTS],
    suppress_callback_exceptions=True,
    title="Genie Curator Workbench",
)

# ── Custom CSS injection ─────────────────────────────────────────────────
CUSTOM_CSS = f"""
@import url('{FONTS}');
:root {{
  --bg:{CSS_VARS['bg']}; --surface:{CSS_VARS['surface']}; --soft:{CSS_VARS['soft']};
  --ink:{CSS_VARS['ink']}; --muted:{CSS_VARS['muted']}; --line:{CSS_VARS['line']};
  --glass:{CSS_VARS['glass']};
  --rose:{CSS_VARS['rose']}; --rose-bg:{CSS_VARS['rose_bg']};
  --teal:{CSS_VARS['teal']}; --teal-bg:{CSS_VARS['teal_bg']};
  --indigo:{CSS_VARS['indigo']}; --indigo-bg:{CSS_VARS['indigo_bg']};
  --display:"Bricolage Grotesque",system-ui,sans-serif;
  --body:"Instrument Sans",system-ui,sans-serif;
  --mono:"JetBrains Mono",monospace;
}}
html,body {{ background:var(--bg) !important; color:var(--ink) !important; font-family:var(--body) !important; margin:0; }}
.dash-header {{ background:var(--glass) !important; backdrop-filter:blur(10px); border-bottom:1px solid var(--line); }}
.card {{ background:var(--surface); border:1px solid var(--line); border-radius:16px; padding:24px; }}
.card-sm {{ background:var(--surface); border:1px solid var(--line); border-radius:12px; padding:16px; }}
.metric-tile {{ background:var(--surface); border:1px solid var(--line); border-radius:14px; padding:14px 20px; display:flex; flex-direction:column; gap:4px; }}
.metric-tile .value {{ font-family:var(--display); font-size:32px; font-weight:700; letter-spacing:-0.03em; font-variant-numeric:tabular-nums; }}
.metric-tile .label {{ font-size:13px; color:var(--muted); font-family:var(--mono); }}
.metric-tile.pass {{ border-color:var(--teal); background:var(--teal-bg); }}
.metric-tile.pass .value {{ color:var(--teal); }}
.metric-tile.fail {{ border-color:var(--rose); background:var(--rose-bg); }}
.metric-tile.fail .value {{ color:var(--rose); }}
.chip {{ display:inline-flex; align-items:center; gap:6px; font-family:var(--mono); font-size:13px; padding:6px 10px; border-radius:8px; border:1px solid var(--line); background:var(--surface); }}
.chip.pass {{ border-color:var(--teal); background:var(--teal-bg); color:var(--teal); }}
.chip.fail {{ border-color:var(--rose); background:var(--rose-bg); color:var(--rose); }}
.nav-tab {{ padding:10px 18px; border-radius:10px; font-weight:600; font-size:14px; cursor:pointer; border:1px solid transparent; background:transparent; color:var(--muted); transition:all .2s; }}
.nav-tab:hover {{ background:var(--soft); color:var(--ink); }}
.nav-tab.active {{ background:var(--indigo-bg); color:var(--indigo); border-color:var(--indigo); }}
.btn-primary {{ background:var(--indigo); color:#fff; border:none; border-radius:10px; padding:12px 24px; font-weight:600; font-size:15px; cursor:pointer; font-family:var(--body); transition:opacity .2s; }}
.btn-primary:hover {{ opacity:.85; }}
.btn-primary:disabled {{ opacity:.4; cursor:not-allowed; }}
.btn-outline {{ background:transparent; color:var(--ink); border:1px solid var(--line); border-radius:10px; padding:10px 20px; font-weight:600; font-size:14px; cursor:pointer; font-family:var(--body); transition:all .2s; }}
.btn-outline:hover {{ background:var(--soft); }}
h1 {{ font-family:var(--display) !important; font-weight:800 !important; letter-spacing:-0.04em !important; }}
h2 {{ font-family:var(--display) !important; font-weight:700 !important; letter-spacing:-0.025em !important; }}
h3 {{ font-family:var(--display) !important; font-weight:600 !important; }}
.log-area {{ background:var(--soft); border:1px solid var(--line); border-radius:12px; padding:16px; font-family:var(--mono); font-size:12px; color:var(--muted); max-height:300px; overflow-y:auto; white-space:pre-wrap; }}
.table-dark {{ width:100%; border-collapse:collapse; font-size:14px; }}
.table-dark th {{ text-align:left; padding:10px 14px; background:var(--soft); color:var(--muted); font-weight:600; border-bottom:1px solid var(--line); font-size:12px; text-transform:uppercase; letter-spacing:.05em; }}
.table-dark td {{ padding:10px 14px; border-bottom:1px solid var(--line); }}
.table-dark tr:hover {{ background:var(--soft); }}
.progress-bar {{ height:8px; background:var(--soft); border-radius:99px; overflow:hidden; }}
.progress-fill {{ height:100%; border-radius:99px; transition:width .6s ease; }}
"""

# Plotly dark template matching the theme
PLOTLY_TEMPLATE = go.layout.Template(
    layout=go.Layout(
        paper_bgcolor=CSS_VARS["surface"],
        plot_bgcolor=CSS_VARS["surface"],
        font=dict(family="Instrument Sans, system-ui", color=CSS_VARS["ink"], size=13),
        xaxis=dict(gridcolor=CSS_VARS["line"], zerolinecolor=CSS_VARS["line"]),
        yaxis=dict(gridcolor=CSS_VARS["line"], zerolinecolor=CSS_VARS["line"]),
        colorway=[CSS_VARS["indigo"], CSS_VARS["teal"], CSS_VARS["rose"], "#F59E0B", "#8B5CF6", "#6366F1", "#10B981"],
    )
)


# ── Layout Components ─────────────────────────────────────────────────────
def make_header():
    return html.Div([
        html.Div([
            html.Div([
                html.Div("\u2b22", style={"fontSize": "24px", "color": CSS_VARS["indigo"]}),
                html.Span("Genie Curator Workbench", style={"fontFamily": "var(--display)", "fontWeight": "700", "fontSize": "16px"}),
            ], style={"display": "flex", "alignItems": "center", "gap": "10px", "marginRight": "auto"}),
            html.Div([
                html.Span(f"Catalog: {CATALOG}", className="chip"),
                html.Span(f"Warehouse: {WAREHOUSE_ID[:8]}...", className="chip"),
            ], style={"display": "flex", "gap": "8px"}),
        ], style={"display": "flex", "alignItems": "center", "gap": "16px", "maxWidth": "1200px", "margin": "0 auto", "padding": "0 24px", "height": "56px"}),
    ], style={"position": "sticky", "top": "0", "zIndex": "30", "background": CSS_VARS["glass"], "backdropFilter": "blur(10px)", "borderBottom": f"1px solid {CSS_VARS['line']}"})


def make_nav():
    tabs = [
        ("dashboard", "\ud83d\udcca Dashboard"),
        ("benchmark", "\ud83e\uddea Benchmark"),
        ("comparison", "\u2696\ufe0f Comparison"),
        ("agents", "\ud83e\udd16 Agents"),
        ("logs", "\ud83d\udcdd Logs"),
    ]
    return html.Div([
        html.Button(label, id={"type": "nav-tab", "index": tid}, n_clicks=0,
                    className="nav-tab active" if tid == "dashboard" else "nav-tab")
        for tid, label in tabs
    ], style={"display": "flex", "gap": "4px", "padding": "12px 24px", "maxWidth": "1200px", "margin": "0 auto", "flexWrap": "wrap"})


def make_metric_tile(value, label, status=None):
    cls = "metric-tile"
    if status == "pass":
        cls += " pass"
    elif status == "fail":
        cls += " fail"
    return html.Div([
        html.Div(str(value), className="value"),
        html.Div(label, className="label"),
    ], className=cls)


# ── Main Layout ───────────────────────────────────────────────────────────
app.layout = html.Div([
    html.Style(CUSTOM_CSS),
    make_header(),
    make_nav(),
    html.Div(id="page-content", style={"maxWidth": "1200px", "margin": "0 auto", "padding": "24px"}),
    dcc.Store(id="active-tab", data="dashboard"),
    dcc.Store(id="app-data", data={}),
    dcc.Interval(id="refresh-interval", interval=5000, n_intervals=0, disabled=True),
])


# ── Navigation Callback ──────────────────────────────────────────────────
@callback(
    Output("active-tab", "data"),
    Input({"type": "nav-tab", "index": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def switch_tab(clicks):
    triggered = ctx.triggered_id
    if triggered and isinstance(triggered, dict):
        return triggered["index"]
    return no_update


@callback(
    Output("page-content", "children"),
    Input("active-tab", "data"),
)
def render_page(tab):
    if tab == "dashboard":
        return render_dashboard()
    elif tab == "benchmark":
        return render_benchmark()
    elif tab == "comparison":
        return render_comparison()
    elif tab == "agents":
        return render_agents()
    elif tab == "logs":
        return render_logs()
    return html.Div("Unknown tab")


# ── Page: Dashboard ──────────────────────────────────────────────────────
def render_dashboard():
    # Discover current state
    try:
        agents = discover_agents()
        supervisors = discover_supervisors()
        app_state["agents"] = agents
        app_state["supervisors"] = supervisors
    except Exception as e:
        agents = {}
        supervisors = []

    agent_count = len(agents)
    sup_count = len(supervisors)
    bench_count = len(app_state.get("benchmark_results", {}))

    # Latest benchmark score
    latest_score = "--"
    latest_label = "No benchmarks run"
    if app_state["benchmark_results"]:
        last_key = list(app_state["benchmark_results"].keys())[-1]
        results = app_state["benchmark_results"][last_key]
        found = sum(1 for r in results if r["verdict"] in ("EXACT", "TOLERANT"))
        latest_score = f"{found}/45"
        latest_label = f"Latest: {last_key}"

    return html.Div([
        html.Div([
            html.H1("Supply Chain Control Tower", style={"fontSize": "clamp(28px,4vw,46px)", "lineHeight": "1.06"}),
            html.P("Genie Agent curator workbench \u2014 benchmark, iterate, and compare agent architectures.",
                   style={"color": CSS_VARS["muted"], "fontSize": "18px", "marginTop": "8px"}),
        ], style={"marginBottom": "32px"}),

        # Metric tiles
        html.Div([
            make_metric_tile(agent_count, "DOMAIN AGENTS", "pass" if agent_count == 5 else "fail"),
            make_metric_tile(sup_count, "SUPERVISORS"),
            make_metric_tile(latest_score, latest_label, "pass" if "45/45" in str(latest_score) else None),
            make_metric_tile(bench_count, "BENCHMARK RUNS"),
        ], style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(200px, 1fr))", "gap": "16px", "marginBottom": "32px"}),

        # Agent list
        html.Div([
            html.H3("Active Agents (Iteration 3)", style={"marginBottom": "16px"}),
            html.Table([
                html.Thead(html.Tr([html.Th(h) for h in ["Agent", "Space ID", "Status"]])),
                html.Tbody([
                    html.Tr([
                        html.Td(name.title(), style={"fontWeight": "600"}),
                        html.Td(html.Code(sid[:16] + "..."), style={"fontFamily": "var(--mono)", "fontSize": "13px"}),
                        html.Td(html.Span("\u2713 Active", className="chip pass")),
                    ]) for name, sid in agents.items()
                ]) if agents else html.Tbody(html.Tr(html.Td("No agents discovered", colSpan=3, style={"color": CSS_VARS["muted"]})))
            ], className="table-dark"),
        ], className="card", style={"marginBottom": "24px"}),

        # Supervisors
        html.Div([
            html.H3("Supervisor Agents", style={"marginBottom": "16px"}),
            html.Table([
                html.Thead(html.Tr([html.Th(h) for h in ["Name", "Endpoint", "Description"]])),
                html.Tbody([
                    html.Tr([
                        html.Td(s["display_name"], style={"fontWeight": "600"}),
                        html.Td(html.Code(s["endpoint"]), style={"fontFamily": "var(--mono)", "fontSize": "13px"}),
                        html.Td(s["description"], style={"color": CSS_VARS["muted"]}),
                    ]) for s in supervisors
                ]) if supervisors else html.Tbody(html.Tr(html.Td("No supervisors found", colSpan=3, style={"color": CSS_VARS["muted"]})))
            ], className="table-dark"),
        ], className="card"),
    ])


# ── Page: Benchmark ──────────────────────────────────────────────────────
def render_benchmark():
    bench_keys = list(app_state.get("benchmark_results", {}).keys())
    return html.Div([
        html.H2("45-Test Benchmark", style={"marginBottom": "8px"}),
        html.P("Send the CFO prompt to a Supervisor and score against 45 ground truth metrics.",
               style={"color": CSS_VARS["muted"], "marginBottom": "24px"}),

        # Controls
        html.Div([
            html.Div([
                html.Label("Endpoint", style={"fontSize": "13px", "color": CSS_VARS["muted"], "display": "block", "marginBottom": "4px"}),
                dcc.Dropdown(
                    id="bench-endpoint",
                    options=[{"label": s["display_name"], "value": s["endpoint"]} for s in app_state.get("supervisors", [])],
                    value=app_state["supervisors"][0]["endpoint"] if app_state.get("supervisors") else None,
                    style={"background": CSS_VARS["soft"], "color": CSS_VARS["ink"]},
                ),
            ], style={"flex": "1"}),
            html.Div([
                html.Label("Label", style={"fontSize": "13px", "color": CSS_VARS["muted"], "display": "block", "marginBottom": "4px"}),
                dcc.Input(id="bench-label", value="Run 1", type="text",
                          style={"background": CSS_VARS["soft"], "border": f"1px solid {CSS_VARS['line']}", "borderRadius": "8px",
                                 "padding": "8px 12px", "color": CSS_VARS["ink"], "width": "100%"}),
            ], style={"flex": "1"}),
            html.Button("\u25b6 Run Benchmark", id="btn-run-bench", className="btn-primary",
                        style={"alignSelf": "flex-end"}),
        ], style={"display": "flex", "gap": "16px", "alignItems": "flex-end", "marginBottom": "24px"}, className="card"),

        # Status
        html.Div(id="bench-status", style={"marginBottom": "24px"}),

        # Results
        html.Div(id="bench-results"),

        # Previous runs
        html.Div([
            html.H3("Previous Runs", style={"marginBottom": "12px"}),
            *[html.Div([
                html.Span(k, style={"fontWeight": "600"}),
                html.Span(f" \u2014 {sum(1 for r in v if r['verdict'] in ('EXACT','TOLERANT'))}/45",
                          style={"color": CSS_VARS["muted"]}),
            ], style={"padding": "8px 0", "borderBottom": f"1px solid {CSS_VARS['line']}"}) for k, v in app_state.get("benchmark_results", {}).items()],
        ], className="card", style={"marginTop": "24px"}) if bench_keys else html.Div(),
    ])


@callback(
    Output("bench-status", "children"),
    Output("bench-results", "children"),
    Input("btn-run-bench", "n_clicks"),
    State("bench-endpoint", "value"),
    State("bench-label", "value"),
    prevent_initial_call=True,
)
def run_benchmark(n_clicks, endpoint, label):
    if not endpoint:
        return html.Div("Select an endpoint first", style={"color": CSS_VARS["rose"]}), no_update

    # Show loading
    log(f"Benchmark started: {label} on {endpoint}")

    # Call supervisor
    text, elapsed = call_supervisor(endpoint, MAIN_PROMPT)
    log(f"Benchmark response: {len(text)} chars in {elapsed:.0f}s")

    if text.startswith("ERROR:"):
        return html.Div(f"\u274c {text[:200]}", style={"color": CSS_VARS["rose"]}), no_update

    # Score
    results = score_response(text)
    app_state["benchmark_results"][label or f"Run {len(app_state['benchmark_results'])+1}"] = results

    exact = sum(1 for r in results if r["verdict"] == "EXACT")
    tolerant = sum(1 for r in results if r["verdict"] == "TOLERANT")
    total = exact + tolerant
    missing = 45 - total

    # Status bar
    status = html.Div([
        html.Div([
            make_metric_tile(f"{total}/45", "TOTAL FOUND", "pass" if total >= 40 else None),
            make_metric_tile(exact, "EXACT", "pass"),
            make_metric_tile(tolerant, "TOLERANT"),
            make_metric_tile(missing, "MISSING", "fail" if missing > 5 else None),
            make_metric_tile(f"{elapsed:.0f}s", "RESPONSE TIME"),
        ], style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(140px, 1fr))", "gap": "12px"}),
    ], className="card")

    # Per-group chart
    group_data = []
    for prefix, (gname, gcolor) in GROUP_META.items():
        found = sum(1 for r in results if r["id"].startswith(prefix) and r["verdict"] in ("EXACT", "TOLERANT"))
        total_g = sum(1 for r in results if r["id"].startswith(prefix))
        group_data.append({"Group": f"{prefix}: {gname}", "Found": found, "Total": total_g, "Color": gcolor})

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=[g["Group"] for g in group_data],
        y=[g["Total"] for g in group_data],
        name="Total", marker_color=CSS_VARS["soft"], marker_line_width=0,
    ))
    fig.add_trace(go.Bar(
        x=[g["Group"] for g in group_data],
        y=[g["Found"] for g in group_data],
        name="Found", marker_color=[g["Color"] for g in group_data], marker_line_width=0,
    ))
    fig.update_layout(template=PLOTLY_TEMPLATE, barmode="overlay", height=320, margin=dict(t=30, b=40),
                      legend=dict(orientation="h", y=1.12), title="Per-Group Results")

    # Detail table
    table_rows = []
    for r in results:
        verdict_cls = "chip pass" if r["verdict"] in ("EXACT", "TOLERANT") else "chip fail"
        verdict_text = "\u2713" if r["verdict"] in ("EXACT", "TOLERANT") else "\u2717"
        table_rows.append(html.Tr([
            html.Td(r["id"], style={"fontFamily": "var(--mono)", "fontWeight": "600"}),
            html.Td(r["group_name"]),
            html.Td(r["desc"][:45]),
            html.Td(str(r["expected"]), style={"fontFamily": "var(--mono)"}),
            html.Td(html.Span(f"{verdict_text} {r['verdict']}", className=verdict_cls)),
        ]))

    results_div = html.Div([
        dcc.Graph(figure=fig, config={"displayModeBar": False}),
        html.Div([
            html.Table([
                html.Thead(html.Tr([html.Th(h) for h in ["ID", "Group", "Description", "Expected", "Verdict"]])),
                html.Tbody(table_rows),
            ], className="table-dark"),
        ], style={"marginTop": "16px", "maxHeight": "500px", "overflowY": "auto"}),
    ], className="card", style={"marginTop": "16px"})

    return status, results_div


# ── Page: Comparison ─────────────────────────────────────────────────────
def render_comparison():
    return html.Div([
        html.H2("Supervisor Architecture Comparison", style={"marginBottom": "8px"}),
        html.P("Send the same CFO prompt to multiple supervisor endpoints and compare results.",
               style={"color": CSS_VARS["muted"], "marginBottom": "24px"}),

        html.Div([
            html.Div([
                html.Label("Endpoint A", style={"fontSize": "13px", "color": CSS_VARS["muted"]}),
                dcc.Dropdown(id="cmp-ep-a",
                    options=[{"label": s["display_name"], "value": s["endpoint"]} for s in app_state.get("supervisors", [])],
                    style={"background": CSS_VARS["soft"]},
                ),
            ], style={"flex": "1"}),
            html.Div([
                html.Label("Endpoint B", style={"fontSize": "13px", "color": CSS_VARS["muted"]}),
                dcc.Dropdown(id="cmp-ep-b",
                    options=[{"label": s["display_name"], "value": s["endpoint"]} for s in app_state.get("supervisors", [])],
                    style={"background": CSS_VARS["soft"]},
                ),
            ], style={"flex": "1"}),
            html.Button("\u25b6 Run Comparison", id="btn-run-cmp", className="btn-primary",
                        style={"alignSelf": "flex-end"}),
        ], style={"display": "flex", "gap": "16px", "alignItems": "flex-end"}, className="card"),

        html.Div(id="cmp-status", style={"marginTop": "24px"}),
        html.Div(id="cmp-results", style={"marginTop": "16px"}),
    ])


@callback(
    Output("cmp-status", "children"),
    Output("cmp-results", "children"),
    Input("btn-run-cmp", "n_clicks"),
    State("cmp-ep-a", "value"),
    State("cmp-ep-b", "value"),
    prevent_initial_call=True,
)
def run_comparison(n_clicks, ep_a, ep_b):
    if not ep_a or not ep_b:
        return html.Div("Select both endpoints", style={"color": CSS_VARS["rose"]}), no_update

    log(f"Comparison: {ep_a} vs {ep_b}")

    text_a, time_a = call_supervisor(ep_a, MAIN_PROMPT)
    log(f"Endpoint A: {len(text_a)} chars in {time_a:.0f}s")
    text_b, time_b = call_supervisor(ep_b, MAIN_PROMPT)
    log(f"Endpoint B: {len(text_b)} chars in {time_b:.0f}s")

    scores_a = score_response(text_a)
    scores_b = score_response(text_b)
    found_a = sum(1 for r in scores_a if r["verdict"] in ("EXACT", "TOLERANT"))
    found_b = sum(1 for r in scores_b if r["verdict"] in ("EXACT", "TOLERANT"))

    status = html.Div([
        html.Div([
            make_metric_tile(f"{found_a}/45", f"A: {ep_a[:20]}", "pass" if found_a > found_b else None),
            make_metric_tile(f"{found_b}/45", f"B: {ep_b[:20]}", "pass" if found_b > found_a else None),
            make_metric_tile(f"{time_a:.0f}s", "A TIME"),
            make_metric_tile(f"{time_b:.0f}s", "B TIME"),
        ], style={"display": "grid", "gridTemplateColumns": "repeat(4, 1fr)", "gap": "12px"}),
    ], className="card")

    # Side-by-side chart
    groups = list(GROUP_META.keys())
    a_groups = [sum(1 for r in scores_a if r["id"].startswith(g) and r["verdict"] in ("EXACT","TOLERANT")) for g in groups]
    b_groups = [sum(1 for r in scores_b if r["id"].startswith(g) and r["verdict"] in ("EXACT","TOLERANT")) for g in groups]
    labels = [f"{g}: {GROUP_META[g][0]}" for g in groups]

    fig = go.Figure()
    fig.add_trace(go.Bar(x=labels, y=a_groups, name=f"A: {ep_a[:16]}", marker_color=CSS_VARS["indigo"]))
    fig.add_trace(go.Bar(x=labels, y=b_groups, name=f"B: {ep_b[:16]}", marker_color=CSS_VARS["teal"]))
    fig.update_layout(template=PLOTLY_TEMPLATE, barmode="group", height=350, margin=dict(t=30, b=40),
                      legend=dict(orientation="h", y=1.12), title="Per-Group Comparison")

    # Detail table
    rows = []
    both, only_a, only_b, neither = 0, 0, 0, 0
    for ra, rb in zip(scores_a, scores_b):
        va = ra["verdict"] in ("EXACT", "TOLERANT")
        vb = rb["verdict"] in ("EXACT", "TOLERANT")
        if va and vb: both += 1
        elif va: only_a += 1
        elif vb: only_b += 1
        else: neither += 1
        a_icon = "\u2713" if va else "\u2717"
        b_icon = "\u2713" if vb else "\u2717"
        rows.append(html.Tr([
            html.Td(ra["id"], style={"fontFamily": "var(--mono)"}),
            html.Td(ra["desc"][:35]),
            html.Td(html.Span(a_icon, className="chip pass" if va else "chip fail")),
            html.Td(html.Span(b_icon, className="chip pass" if vb else "chip fail")),
        ]))

    results = html.Div([
        dcc.Graph(figure=fig, config={"displayModeBar": False}),
        html.Div([
            html.Span(f"Both: {both}", className="chip pass"),
            html.Span(f"Only A: {only_a}", className="chip"),
            html.Span(f"Only B: {only_b}", className="chip"),
            html.Span(f"Neither: {neither}", className="chip fail"),
        ], style={"display": "flex", "gap": "8px", "margin": "16px 0"}),
        html.Table([
            html.Thead(html.Tr([html.Th(h) for h in ["ID", "Description", "Endpoint A", "Endpoint B"]])),
            html.Tbody(rows),
        ], className="table-dark", style={"maxHeight": "400px", "overflowY": "auto"}),
    ], className="card")

    return status, results


# ── Page: Agents ─────────────────────────────────────────────────────────
def render_agents():
    agents = app_state.get("agents", {})
    return html.Div([
        html.H2("Agent Management", style={"marginBottom": "8px"}),
        html.P("Discover, inspect, and manage Genie domain agents.",
               style={"color": CSS_VARS["muted"], "marginBottom": "24px"}),

        html.Div([
            html.Button("\ud83d\udd0d Discover Agents", id="btn-discover", className="btn-outline"),
            html.Div(id="discover-result", style={"marginTop": "12px"}),
        ], className="card", style={"marginBottom": "24px"}),

        html.Div([
            html.H3("Test Individual Agent", style={"marginBottom": "12px"}),
            html.Div([
                dcc.Dropdown(
                    id="agent-select",
                    options=[{"label": f"{n.title()} ({sid[:12]}...)", "value": sid} for n, sid in agents.items()],
                    placeholder="Select agent",
                    style={"flex": "2", "background": CSS_VARS["soft"]},
                ),
                dcc.Input(id="agent-question", placeholder="Ask a question...",
                          style={"flex": "3", "background": CSS_VARS["soft"], "border": f"1px solid {CSS_VARS['line']}",
                                 "borderRadius": "8px", "padding": "8px 12px", "color": CSS_VARS["ink"]}),
                html.Button("Ask", id="btn-ask-agent", className="btn-primary"),
            ], style={"display": "flex", "gap": "12px"}),
            html.Div(id="agent-response", style={"marginTop": "16px"}),
        ], className="card"),
    ])


@callback(
    Output("discover-result", "children"),
    Input("btn-discover", "n_clicks"),
    prevent_initial_call=True,
)
def discover_click(n):
    agents = discover_agents()
    supervisors = discover_supervisors()
    app_state["agents"] = agents
    app_state["supervisors"] = supervisors
    return html.Div([
        html.Span(f"\u2713 Found {len(agents)} domain agents, {len(supervisors)} supervisors",
                  style={"color": CSS_VARS["teal"]}),
    ])


@callback(
    Output("agent-response", "children"),
    Input("btn-ask-agent", "n_clicks"),
    State("agent-select", "value"),
    State("agent-question", "value"),
    prevent_initial_call=True,
)
def ask_agent_click(n, space_id, question):
    if not space_id or not question:
        return html.Div("Select an agent and enter a question", style={"color": CSS_VARS["rose"]})
    log(f"Asking agent {space_id[:12]}... : {question[:60]}")
    result = ask_genie(space_id, question)
    return html.Div([
        html.Pre(result[:3000], style={"fontFamily": "var(--mono)", "fontSize": "13px",
                 "background": CSS_VARS["soft"], "padding": "16px", "borderRadius": "12px",
                 "whiteSpace": "pre-wrap", "maxHeight": "400px", "overflowY": "auto"}),
    ])


# ── Page: Logs ───────────────────────────────────────────────────────────
def render_logs():
    return html.Div([
        html.H2("Activity Log", style={"marginBottom": "16px"}),
        html.Div(
            "\n".join(app_state.get("logs", ["No activity yet."])),
            className="log-area",
        ),
    ])


# ── Run ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Initial discovery
    try:
        app_state["agents"] = discover_agents()
        app_state["supervisors"] = discover_supervisors()
        log(f"Startup: {len(app_state['agents'])} agents, {len(app_state['supervisors'])} supervisors")
    except Exception as e:
        log(f"Startup discovery failed: {e}")

    app.run(host="0.0.0.0", port=8050, debug=False)
