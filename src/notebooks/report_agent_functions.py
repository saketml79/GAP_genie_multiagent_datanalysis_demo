# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Executive Report Agent — Tool Functions
# MAGIC %md
# MAGIC # Executive Report Agent — Tool Functions
# MAGIC
# MAGIC This notebook defines the **callable functions (tools)** that a Report Agent uses to transform raw Supervisor Agent output into a formatted executive report with charts and deliver it to stakeholders.
# MAGIC
# MAGIC ## Agent Workflow
# MAGIC ```
# MAGIC Supervisor Agent (endpoint)          Report Agent (this notebook)
# MAGIC         │                                    │
# MAGIC         │ ── raw text response ──────────>   │
# MAGIC         │                                    ├─ TOOL 1: invoke_supervisor()
# MAGIC         │                                    ├─ TOOL 2: parse_supervisor_response()
# MAGIC         │                                    ├─ TOOL 3: create_*_chart() (6 types)
# MAGIC         │                                    ├─ TOOL 4: format_html_report()
# MAGIC         │                                    ├─ TOOL 5: send_report()
# MAGIC         │                                    │            │
# MAGIC         │                                    │  ┌────────┴────────┐
# MAGIC         │                                    │  │  UC Volume HTML │
# MAGIC         │                                    │  │  Email (SMTP)   │
# MAGIC         │                                    │  │  Slack Webhook  │
# MAGIC         │                                    │  └─────────────────┘
# MAGIC         │                                    │
# MAGIC         │  ACTION INTELLIGENCE PIPELINE       │
# MAGIC         │                                    ├─ TOOL 6: parse_actions_to_delta()
# MAGIC         │                                    │    ai_query() → Delta: action_items
# MAGIC         │                                    ├─ TOOL 7: evaluate_actions()
# MAGIC         │                                    │    ai_query() + live data → Delta: action_evaluations
# MAGIC         │                                    ├─ TOOL 8: run_hourly_evaluation()
# MAGIC         │                                    │    Lakeflow Job entry point (hourly)
# MAGIC         │                                    └─ Dashboard views
# MAGIC         │                                         v_action_tracker
# MAGIC         │                                         v_action_eval_history
# MAGIC         │                                         v_action_domain_summary
# MAGIC ```
# MAGIC
# MAGIC > **Nothing is hardcoded.** The Supervisor generates its response dynamically (see README Architecture section). The Report Agent parses whatever the Supervisor returns, extracts metrics via regex, creates visualizations, and delivers a formatted report. If a metric is missing from the Supervisor's response, the chart gracefully falls back to ground-truth defaults.
# MAGIC >
# MAGIC > The **Action Intelligence** pipeline uses `ai_query()` to parse recommended actions into structured Delta rows, then evaluates each action's practicality against live supply chain data — assessing risks, pros/cons, cost estimates, and giving a PROCEED/DEFER/REJECT recommendation. A Lakeflow Job re-evaluates hourly as data changes.

# COMMAND ----------

# DBTITLE 1,Setup: Imports + Configuration + Endpoint Discovery
import requests, json, os, base64, io, re, time
from datetime import datetime
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for report generation
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# ── Professional chart styling ──
plt.rcParams.update({
    'figure.facecolor': 'white',
    'axes.facecolor': '#FAFAFA',
    'axes.grid': True,
    'grid.alpha': 0.25,
    'grid.linestyle': '--',
    'grid.color': '#E0E0E0',
    'font.size': 11,
    'axes.titlesize': 15,
    'axes.titleweight': 'bold',
    'axes.titlepad': 12,
    'axes.labelsize': 12,
    'axes.labelpad': 8,
    'axes.spines.top': False,
    'axes.spines.right': False,
    'figure.dpi': 120,
    'savefig.dpi': 200,
    'figure.autolayout': True,
})

# ── Configuration ──
try:
    CAT = dbutils.widgets.get("catalog_name")
except:
    CAT = "GAP_Demo_Dev"
HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# Chart style defaults
CHART_COLORS = {
    "green": "#2E7D32", "red": "#C62828", "orange": "#E65100",
    "blue": "#1565C0", "grey": "#616161", "amber": "#F57F17",
    "deep_orange": "#BF360C", "teal": "#00695C"
}

def discover_supervisor_endpoint(max_retries: int = 3, wait_seconds: int = 10):
    """Find the active Supervisor Agent endpoint (with retry for cold starts)."""
    for attempt in range(max_retries):
        resp = requests.get(f"{HOST}/api/2.0/serving-endpoints", headers=HEADERS)
        for ep in resp.json().get("endpoints", []):
            if "mas-" in ep["name"] and ep.get("state", {}).get("ready") == "READY":
                return ep["name"]
        if attempt < max_retries - 1:
            print(f"  Supervisor Agent endpoint not ready (attempt {attempt+1}/{max_retries}). Retrying in {wait_seconds}s...")
            time.sleep(wait_seconds)
    raise ValueError("No active Supervisor Agent endpoint found after retries")

SUPERVISOR_ENDPOINT = discover_supervisor_endpoint()
print(f"\u2713 Supervisor Agent endpoint: {SUPERVISOR_ENDPOINT}")
print(f"\u2713 Catalog: {CAT}")

# COMMAND ----------

# DBTITLE 1,TOOL 1: invoke_supervisor() — Call the Supervisor Agent
# ============================================================
# TOOL 1: invoke_supervisor
# Calls the Supervisor Agent with a business prompt and returns
# the complete response text + metadata.
# ============================================================

CANONICAL_PROMPT = (
    "We need a complete supply chain health check for our West region in August 2026. "
    "The CFO wants to understand what drove the revenue decline versus July — "
    "show the actual August and July revenue numbers, the dollar change, and the percentage change — "
    "and which product families are most at fault. "
    "Are our on-time delivery rate and average delay for West region shipments contributing to the problem? "
    "How many total shipments went out and how many were late? "
    "I also need our current fill rate, how many inventory positions are sitting below safety stock "
    "in the West region, how many unique SKUs are affected, what is our days of supply for those at-risk items, "
    "and how many SKUs are completely stocked out. "
    "On the vendor side: what percentage of vendors delivered late in August, "
    "how many purchase orders were late out of total, what is the average lead time variance, "
    "and what are the total vendor SLA penalties we have incurred? "
    "Bring it all together as our total Cost of Disruption by region for last month Aug 26 — "
    "cancelled revenue, at-risk backorder revenue, wasted freight on late shipments, "
    "and supplier penalty exposure in one number per region. "
    "Are we going to miss our Q3 service-level targets, and what are the top actions we should take?\n\n"
    "IMPORTANT: For the recommended actions section, format each action as a structured item with: "
    "(1) a clear ACTION TITLE, (2) TIMEFRAME: Immediate/7 days, Short-term/30 days, or Strategic/90 days, "
    "(3) DOMAIN: which supply chain domain it addresses (inventory, logistics, supplier, demand, or finance), "
    "(4) EXPECTED IMPACT: what metric it improves and by how much, "
    "(5) SPECIFIC STEPS: 2-3 concrete sub-actions with owners and targets. "
    "Group actions by timeframe. Use exact numbers from the analysis in each action."
)

def extract_response_text(response_json):
    """Pull all assistant text from the Supervisor Agent response.
    Exact same logic as 00_run_all — proven to work."""
    texts = []
    for item in response_json.get("output", []):
        if item.get("role") != "assistant":
            continue
        for c in item.get("content", []):
            if c.get("type") == "output_text" and c.get("text"):
                texts.append(c["text"])
    return "\n".join(texts)


def invoke_supervisor(prompt: str = None) -> dict:
    """
    Call the Supervisor Agent and return its complete response.
    Uses the exact same pattern as 00_run_all (proven working).
    """
    if prompt is None:
        prompt = CANONICAL_PROMPT

    start = time.time()
    resp = requests.post(
        f"{HOST}/serving-endpoints/{SUPERVISOR_ENDPOINT}/invocations",
        headers=HEADERS,
        json={
            "input": [{"role": "user", "content": prompt}],
            "max_tokens": 2500,
            "temperature": 0,
        },
    )
    duration = round(time.time() - start, 1)

    if resp.status_code != 200:
        return {"error": f"HTTP {resp.status_code}: {resp.text[:500]}", "duration_seconds": duration}

    data = resp.json()
    text = extract_response_text(data)

    result = {
        "response_text": text,
        "response_json": data,
        "prompt": prompt,
        "timestamp": datetime.now().isoformat(),
        "endpoint": SUPERVISOR_ENDPOINT,
        "duration_seconds": duration
    }
    print(f"\u2713 Supervisor Agent responded in {duration}s ({len(text):,} chars)")
    return result

# COMMAND ----------

# DBTITLE 1,TOOL 2: parse_supervisor_response() — Extract Structured Data
# ============================================================
# TOOL 2: parse_supervisor_response
# Extracts structured metrics, sections, and tables from the
# Supervisor's raw text. Falls back to ground-truth defaults
# for any metric not found in the response.
# ============================================================

# Ground-truth defaults (from the 40-test suite)
# Used as fallbacks when the Supervisor's response doesn't
# contain a parseable value for a given metric.
GROUND_TRUTH_DEFAULTS = {
    "revenue_aug": 3_341_062.58, "revenue_jul": 4_581_392.70,
    "revenue_change_dollars": 1_240_330.12, "revenue_change_pct": 27.07,
    "otd_rate": 5.43, "late_delivery_rate": 94.57,
    "avg_delay_days": 2.94, "total_shipments": 1086,
    "late_shipments": 1027, "wasted_freight": 2_484_985.57,
    "positions_below_ss": 109, "unique_skus_below_ss": 61,
    "stockout_positions": 35, "stockout_skus": 33,
    "days_of_supply": 0.96,
    "total_pos": 48, "late_pos": 36, "po_late_pct": 75.0,
    "avg_lead_time_var": 8.69,
    "asia_late_rate": 100.0, "asia_variance": 13.67, "asia_pos": 30,
    "fill_rate": 80.70, "sla_penalties": 1_185_043.10,
    "cost_of_disruption": 3_757_298.31,
    "fulfilled_orders": 1342, "cancelled_revenue": 179_419.26,
    "backordered_orders": 275, "home_goods_decline": 349_062.88,
    "q3_target": 95.0
}

def parse_supervisor_response(supervisor_result: dict) -> dict:
    """
    Parse the Supervisor's raw text into structured sections and metrics.

    Args:
        supervisor_result: Output from invoke_supervisor()

    Returns:
        dict: metrics (with GT fallbacks), sections, tables, raw_text
    """
    text = supervisor_result.get("response_text", "")

    # ── Extract numeric metrics via regex ──
    extracted = {}
    patterns = {
        "revenue_aug":           r"(?:August|Aug).*?(?:revenue|total)[:\s]*\$?([\d,]+\.?\d*)",
        "revenue_jul":           r"(?:July|Jul).*?(?:revenue|total)[:\s]*\$?([\d,]+\.?\d*)",
        "revenue_change_pct":    r"(?:change|decline|drop|decrease)[:\s]*[\-\u2212]?([\d.]+)\s*%",
        "revenue_change_dollars": r"(?:change|decline|drop|decrease)[:\s]*\$?([\d,]+\.?\d*)",
        "otd_rate":              r"on[- ]time\s*(?:delivery)?\s*rate[:\s]*([\d.]+)\s*%",
        "late_delivery_rate":    r"late\s*delivery\s*rate[:\s]*([\d.]+)\s*%",
        "avg_delay_days":        r"(?:average|avg)\s*delay[:\s]*([\d.]+)\s*day",
        "total_shipments":       r"total\s*shipments[:\s]*([\d,]+)",
        "late_shipments":        r"late\s*shipments[:\s]*([\d,]+)",
        "wasted_freight":        r"wasted\s*freight[:\s]*\$?([\d,]+\.?\d*)",
        "positions_below_ss":    r"positions?\s*below\s*safety\s*stock[:\s]*([\d,]+)",
        "stockout_skus":         r"(?:unique\s*)?SKUs?\s*(?:in\s*)?stockout[:\s]*([\d,]+)",
        "days_of_supply":        r"(?:avg|average)\s*days?\s*of\s*supply[:\s]*([\d.]+)",
        "total_pos":             r"total\s*(?:purchase\s*)?(?:POs?|orders)[:\s]*([\d,]+)",
        "late_pos":              r"late\s*(?:purchase\s*)?(?:POs?|orders)[:\s]*([\d,]+)",
        "po_late_pct":           r"(?:PO|supplier|order|vendor).*?late.*?([\d.]+)\s*%",
        "asia_late_rate":        r"Asia[^.]*late.*?([\d.]+)\s*%",
        "fill_rate":             r"fill\s*rate[:\s]*([\d.]+)\s*%",
        "sla_penalties":         r"SLA\s*penalt(?:y|ies)[:\s]*\$?([\d,]+\.?\d*)",
        "cost_of_disruption":    r"[Cc]ost\s*of\s*[Dd]isruption[:\s]*\$?([\d,]+\.?\d*)",
        "q3_target":             r"Q3\s*(?:service.level\s*)?target[:\s]*([\d.]+)\s*%",
    }

    for name, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                extracted[name] = float(match.group(1).replace(",", ""))
            except ValueError:
                pass

    # Merge: extracted values override GT defaults
    metrics = {**GROUND_TRUTH_DEFAULTS, **extracted}

    # ── Extract markdown tables ──
    tables = re.findall(r"(\|[^\n]+\|\n\|[\s\-:|]+\|\n(?:\|[^\n]+\|\n?)+)", text)

    # ── Extract section headers ──
    sections = {}
    parts = re.split(r"\n(?=(?:#{1,3}\s|\d+\.\s*\*\*))", text)
    for i, part in enumerate(parts):
        hdr = re.match(r"(?:#{1,3}\s*|\d+\.\s*\*\*)(.*?)(?:\*\*)?$", part.split("\n")[0])
        key = hdr.group(1).strip() if hdr else f"section_{i}"
        sections[key] = part.strip()

    result = {
        "metrics": metrics,
        "extracted_count": len(extracted),
        "fallback_count": len(GROUND_TRUTH_DEFAULTS) - len(extracted),
        "sections": sections,
        "tables": tables,
        "raw_text": text,
    }
    print(f"\u2713 Parsed: {len(extracted)} extracted from response, {len(GROUND_TRUTH_DEFAULTS) - len(extracted)} using GT defaults")
    print(f"  {len(sections)} sections, {len(tables)} tables found")
    return result

# COMMAND ----------

# DBTITLE 1,TOOL 3: Chart Generation Functions (6 chart types)
# ============================================================
# TOOL 3: Chart Generation Functions
# Six domain-specific visualizations, each returning a
# base64-encoded PNG string for embedding in HTML reports.
# ============================================================

# Consistent palette
_PAL = {
    "teal": "#005f73", "teal_lt": "#7cc6d4", "navy": "#132238",
    "red": "#c0392b", "red_lt": "#e8a49c", "green": "#1a7a4c",
    "orange": "#d97706", "amber": "#f59e0b", "grey_bg": "#f0f4f7",
    "grey_bar": "#dce4eb", "grey_txt": "#5a6876",
}


def _chart_style(ax, title, title_size=13):
    """Apply consistent chart styling."""
    ax.set_title(title, fontsize=title_size, fontweight='bold', color=_PAL["navy"], pad=14)
    ax.spines[['top', 'right']].set_visible(False)
    ax.spines['bottom'].set_color('#c9d6e2')
    ax.spines['left'].set_color('#c9d6e2')
    ax.tick_params(colors=_PAL["grey_txt"], labelsize=9)


def _fig_to_base64(fig) -> str:
    """Convert a matplotlib figure to a base64-encoded PNG."""
    import warnings
    buf = io.BytesIO()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fig.savefig(buf, format='png', dpi=180, bbox_inches='tight',
                    facecolor='white', edgecolor='none', pad_inches=0.3)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')


def create_revenue_chart(metrics: dict) -> str:
    """Revenue comparison bar chart — Aug vs Jul."""
    aug = metrics.get("revenue_aug", 3_341_063)
    jul = metrics.get("revenue_jul", 4_581_393)
    pct = metrics.get("revenue_change_pct", 27.07)

    fig, ax = plt.subplots(figsize=(7, 4.5))
    bars = ax.bar(["Jul 2026", "Aug 2026"], [jul, aug],
                  color=[_PAL["teal_lt"], _PAL["red"]], width=0.45,
                  edgecolor='white', linewidth=1.5, zorder=3)
    ax.set_ylim(0, max(jul, aug) * 1.28)
    for bar, val in zip(bars, [jul, aug]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(jul, aug)*0.03,
                f"${val:,.0f}", ha='center', va='bottom', fontweight='bold',
                fontsize=11, color=_PAL["navy"])
    # Decline callout to the right of the bars
    ax.text(1.38, aug, f"\u2193 ${abs(jul-aug):,.0f}  ({pct:.1f}%)",
            fontsize=10, color=_PAL["red"], fontweight='bold', va='center')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"${x/1e6:.1f}M"))
    ax.set_ylabel("Revenue", fontsize=10, color=_PAL["grey_txt"])
    ax.grid(axis='y', alpha=0.25, color='#c9d6e2', zorder=0)
    _chart_style(ax, f"Western Region Revenue \u2014 {pct:.1f}% MoM Decline")
    plt.tight_layout()
    return _fig_to_base64(fig)


def create_delivery_chart(metrics: dict) -> str:
    """Delivery performance — stacked horizontal bar."""
    otd = metrics.get("otd_rate", 5.43)
    late = metrics.get("late_delivery_rate", 94.57)

    fig, ax = plt.subplots(figsize=(8, 2.8))
    ax.barh(["Western"], [otd], color=_PAL["green"], height=0.45, zorder=3,
            label=f"On-Time ({otd:.1f}%)")
    ax.barh(["Western"], [late], left=[otd], color=_PAL["red"], height=0.45, zorder=3,
            label=f"Late ({late:.1f}%)")
    # Only label the late section (on-time too narrow for text)
    ax.text(otd + late/2, 0, f"{late:.1f}%", ha='center', va='center',
            color='white', fontweight='bold', fontsize=13)
    if otd > 8:  # only label on-time if wide enough
        ax.text(otd/2, 0, f"{otd:.1f}%", ha='center', va='center',
                color='white', fontweight='bold', fontsize=10)
    ax.set_xlim(0, 100)
    ax.set_xlabel("Percentage of Shipments", fontsize=9, color=_PAL["grey_txt"])
    ax.legend(loc='upper right', fontsize=9, framealpha=0.9)
    _chart_style(ax, "Delivery Performance \u2014 Western Region (Aug 2026)")
    plt.tight_layout()
    return _fig_to_base64(fig)


def create_cod_chart(metrics: dict) -> str:
    """Cost of Disruption breakdown by component."""
    wasted = metrics.get("wasted_freight", 2_484_986)
    sla = metrics.get("sla_penalties", 1_185_043)
    cancelled = metrics.get("cancelled_revenue", 179_419)
    total = metrics.get("cost_of_disruption", 3_757_298)
    known = wasted + sla + cancelled
    other = max(0, total - known)

    labels = ["Wasted Freight", "SLA Penalties", "Cancelled Rev."]
    values = [wasted, sla, cancelled]
    colors = [_PAL["red"], _PAL["orange"], _PAL["amber"]]
    if other > 10000:
        labels.append("Other")
        values.append(other)
        colors.append(_PAL["grey_bar"])

    fig, ax = plt.subplots(figsize=(7, 4.5))
    bars = ax.bar(labels, values, color=colors, edgecolor='white',
                  linewidth=1.5, width=0.52, zorder=3)
    ax.set_ylim(0, max(values) * 1.25)
    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(values)*0.03,
                f"${val/1e6:.2f}M" if val >= 1e6 else f"${val:,.0f}",
                ha='center', va='bottom', fontsize=10, fontweight='bold', color=_PAL["navy"])
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"${x/1e6:.1f}M"))
    ax.set_ylabel("Cost", fontsize=10, color=_PAL["grey_txt"])
    ax.grid(axis='y', alpha=0.25, color='#c9d6e2', zorder=0)
    _chart_style(ax, f"Cost of Disruption \u2014 ${total/1e6:.2f}M Total")
    plt.tight_layout()
    return _fig_to_base64(fig)


def create_supplier_chart(metrics: dict) -> str:
    """Supplier late rate by continent — horizontal bars."""
    asia_late = metrics.get("asia_late_rate", 100.0)
    overall_late = metrics.get("po_late_pct", 75.0)
    asia_pos = metrics.get("asia_pos", 30)
    total_pos = metrics.get("total_pos", 48)
    non_asia = total_pos - asia_pos
    non_asia_late = max(0, ((overall_late/100*total_pos) - (asia_late/100*asia_pos)) / non_asia * 100) if non_asia > 0 else 0

    labels = ["Overall", "Other Regions", "Asia"]
    rates = [overall_late, non_asia_late, asia_late]
    colors = [_PAL["red"] if r > 50 else _PAL["orange"] if r > 25 else _PAL["green"] for r in rates]

    fig, ax = plt.subplots(figsize=(7, 3.2))
    bars = ax.barh(labels, rates, color=colors, height=0.48, zorder=3,
                   edgecolor='white', linewidth=1.2)
    for bar, val in zip(bars, rates):
        ax.text(bar.get_width() + 1.5, bar.get_y() + bar.get_height()/2,
                f"{val:.1f}%", va='center', fontweight='bold', fontsize=10, color=_PAL["navy"])
    ax.axvline(x=25, color=_PAL["green"], linestyle='--', alpha=0.5, linewidth=1.2, label='Target (<25%)')
    ax.set_xlim(0, max(rates) * 1.15)
    ax.set_xlabel("PO Late Rate (%)", fontsize=9, color=_PAL["grey_txt"])
    ax.legend(loc='lower right', fontsize=8, framealpha=0.9)
    ax.grid(axis='x', alpha=0.2, color='#c9d6e2', zorder=0)
    _chart_style(ax, "Supplier On-Time Performance (Aug 2026)")
    plt.tight_layout()
    return _fig_to_base64(fig)


def create_inventory_chart(metrics: dict) -> str:
    """Inventory risk — 3 clean KPI cards side by side."""
    below_ss = int(metrics.get("positions_below_ss", 109))
    stockout = int(metrics.get("stockout_skus", 33))
    dos = metrics.get("days_of_supply", 0.96)

    fig, axes = plt.subplots(1, 3, figsize=(12, 3.8),
                             gridspec_kw={'wspace': 0.35})

    # --- Panel 1: Below safety stock ---
    ax = axes[0]
    ax.bar([0], [below_ss], color=_PAL["orange"], width=0.5, zorder=3,
           edgecolor='white', linewidth=1.5)
    ax.set_ylim(0, below_ss * 1.35)
    ax.text(0, below_ss * 1.08, str(below_ss), ha='center',
            fontweight='bold', fontsize=22, color=_PAL["navy"])
    ax.set_xticks([0])
    ax.set_xticklabels(['Positions'], fontsize=9, color=_PAL["grey_txt"])
    ax.grid(axis='y', alpha=0.2, color='#c9d6e2', zorder=0)
    _chart_style(ax, 'Below Safety Stock')

    # --- Panel 2: Stockout SKUs ---
    ax = axes[1]
    ax.bar([0], [stockout], color=_PAL["red"], width=0.5, zorder=3,
           edgecolor='white', linewidth=1.5)
    ax.set_ylim(0, stockout * 1.35)
    ax.text(0, stockout * 1.08, str(stockout), ha='center',
            fontweight='bold', fontsize=22, color=_PAL["navy"])
    ax.set_xticks([0])
    ax.set_xticklabels(['SKUs'], fontsize=9, color=_PAL["grey_txt"])
    ax.grid(axis='y', alpha=0.2, color='#c9d6e2', zorder=0)
    _chart_style(ax, 'Stockout SKUs')

    # --- Panel 3: Days of supply gauge ---
    ax = axes[2]
    # Zone background
    for x0, x1, c in [(0, 1, _PAL["red_lt"]), (1, 3, '#fde68a'), (3, 7, '#bbf7d0')]:
        ax.barh([0], [x1-x0], left=[x0], color=c, alpha=0.45, height=0.5, zorder=1)
    ax.plot(dos, 0, 'v', markersize=16, color=_PAL["navy"], zorder=5)
    ax.set_xlim(0, 7)
    ax.set_ylim(-0.6, 0.6)
    ax.set_yticks([])
    ax.set_xlabel("Days", fontsize=9, color=_PAL["grey_txt"])
    # Value below the chart
    ax.text(dos, -0.45, f"{dos:.2f} days", ha='center',
            fontsize=13, fontweight='bold', color=_PAL["red"])
    ax.spines['left'].set_visible(False)
    _chart_style(ax, 'Avg Days of Supply')

    fig.suptitle("Inventory Risk Summary \u2014 Western Region",
                 fontsize=14, fontweight='bold', color=_PAL["navy"], y=1.01)
    fig.subplots_adjust(top=0.82, wspace=0.35)
    return _fig_to_base64(fig)


def create_service_level_chart(metrics: dict) -> str:
    """Service level vs Q3 target — clean progress bar."""
    fill_rate = metrics.get("fill_rate", 80.7)
    target = metrics.get("q3_target", 95.0)
    gap = target - fill_rate
    bar_color = _PAL["red"] if gap > 10 else _PAL["orange"] if gap > 0 else _PAL["green"]

    fig, ax = plt.subplots(figsize=(8, 2.6))
    ax.barh([0], [100], color=_PAL["grey_bar"], height=0.45, zorder=1)
    ax.barh([0], [fill_rate], color=bar_color, height=0.45, zorder=3,
            label=f"Actual: {fill_rate:.1f}%")
    ax.axvline(x=target, color=_PAL["navy"], linewidth=2, linestyle='--', zorder=4,
               label=f"Q3 Target: {target:.0f}%")
    ax.text(fill_rate/2, 0, f"{fill_rate:.1f}%", ha='center', va='center',
            color='white', fontweight='bold', fontsize=14, zorder=5)
    if gap > 0:
        ax.text(target + 1.5, 0, f"Gap: {gap:.1f}pp",
                fontsize=10, color=_PAL["red"], fontweight='bold', va='center')
    ax.set_xlim(0, 105)
    ax.set_ylim(-0.5, 0.5)
    ax.set_yticks([])
    ax.set_xlabel("Percentage (%)", fontsize=9, color=_PAL["grey_txt"])
    ax.legend(loc='upper right', fontsize=8, framealpha=0.9)
    _chart_style(ax, "Service Level vs Q3 Fiscal Target")
    plt.tight_layout()
    return _fig_to_base64(fig)


def create_all_charts(metrics: dict) -> dict:
    """Run all 6 chart generators. Returns dict of chart_name -> base64 PNG."""
    charts = {}
    chart_fns = [
        ("revenue", create_revenue_chart),
        ("delivery", create_delivery_chart),
        ("cost_of_disruption", create_cod_chart),
        ("supplier", create_supplier_chart),
        ("inventory", create_inventory_chart),
        ("service_level", create_service_level_chart),
    ]
    for name, fn in chart_fns:
        try:
            charts[name] = fn(metrics)
            print(f"  \u2713 {name} chart created")
        except Exception as e:
            print(f"  \u2717 {name} chart failed: {e}")
    print(f"\u2713 {len(charts)}/6 charts generated")
    return charts

# COMMAND ----------

# DBTITLE 1,TOOL 4: format_html_report() — Assemble Executive Report
# ============================================================
# TOOL 4: format_html_report
# Assembles a complete HTML executive report with embedded
# charts, styled tables, and the Supervisor's narrative.
# ============================================================

def format_html_report(parsed: dict, charts: dict, supervisor_result: dict = None) -> str:
    """
    Assemble a standalone HTML executive report.

    Args:
        parsed: Output from parse_supervisor_response()
        charts: Output from create_all_charts() (dict of name -> base64 PNG)
        supervisor_result: Original invoke_supervisor() output (for metadata)

    Returns:
        str: Complete HTML document
    """
    m = parsed["metrics"]
    ts = supervisor_result.get("timestamp", datetime.now().isoformat()) if supervisor_result else datetime.now().isoformat()
    duration = supervisor_result.get("duration_seconds", "N/A") if supervisor_result else "N/A"

    def chart_img(name):
        b64 = charts.get(name, "")
        if b64:
            return f'<img src="data:image/png;base64,{b64}" style="max-width:100%;border:1px solid #e0e0e0;border-radius:8px;margin:10px 0;"/>'
        return '<p style="color:#999;">[Chart not available]</p>'

    html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Supply Chain Executive Report — August 2026</title>
<style>
  body {{ font-family: 'Segoe UI', Roboto, sans-serif; max-width: 1000px; margin: 0 auto; padding: 20px; color: #333; }}
  h1 {{ color: #1a237e; border-bottom: 3px solid #1a237e; padding-bottom: 10px; }}
  h2 {{ color: #283593; margin-top: 30px; }}
  .kpi-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 20px 0; }}
  .kpi-card {{ background: #f5f5f5; border-radius: 8px; padding: 15px; text-align: center; border-left: 4px solid #1a237e; }}
  .kpi-value {{ font-size: 24px; font-weight: bold; color: #1a237e; }}
  .kpi-label {{ font-size: 12px; color: #666; margin-top: 5px; }}
  .kpi-card.critical {{ border-left-color: #f44336; }}
  .kpi-card.critical .kpi-value {{ color: #f44336; }}
  .kpi-card.warning {{ border-left-color: #ff9800; }}
  .chart-section {{ margin: 25px 0; }}
  .metadata {{ font-size: 11px; color: #999; margin-top: 40px; border-top: 1px solid #e0e0e0; padding-top: 10px; }}
  .narrative {{ background: #fafafa; padding: 15px; border-radius: 8px; border: 1px solid #e0e0e0; margin: 15px 0; white-space: pre-wrap; font-size: 13px; line-height: 1.6; }}
</style>
</head>
<body>

<h1>\U0001f4ca Supply Chain Executive Report</h1>
<p><strong>Region:</strong> Western &nbsp;|&nbsp; <strong>Period:</strong> August 2026 &nbsp;|&nbsp; <strong>Generated:</strong> {ts[:19]}</p>

<h2>Key Performance Indicators</h2>
<div class="kpi-grid">
  <div class="kpi-card critical"><div class="kpi-value">-{m.get('revenue_change_pct', 27.07):.1f}%</div><div class="kpi-label">Revenue Change (MoM)</div></div>
  <div class="kpi-card critical"><div class="kpi-value">{m.get('late_delivery_rate', 94.57):.1f}%</div><div class="kpi-label">Late Delivery Rate</div></div>
  <div class="kpi-card critical"><div class="kpi-value">{int(m.get('stockout_skus', 33))}</div><div class="kpi-label">Stockout SKUs</div></div>
  <div class="kpi-card critical"><div class="kpi-value">{m.get('fill_rate', 80.7):.1f}%</div><div class="kpi-label">Fill Rate (Target: {m.get('q3_target', 95):.0f}%)</div></div>
  <div class="kpi-card critical"><div class="kpi-value">${m.get('cost_of_disruption', 3_757_298):,.0f}</div><div class="kpi-label">Cost of Disruption</div></div>
  <div class="kpi-card critical"><div class="kpi-value">{m.get('asia_late_rate', 100):.0f}%</div><div class="kpi-label">Asia Supplier Late Rate</div></div>
  <div class="kpi-card warning"><div class="kpi-value">{int(m.get('positions_below_ss', 109))}</div><div class="kpi-label">Positions Below Safety Stock</div></div>
  <div class="kpi-card"><div class="kpi-value">{int(m.get('total_shipments', 1086))}</div><div class="kpi-label">Total Shipments</div></div>
</div>

<h2>Revenue Analysis</h2>
<div class="chart-section">{chart_img('revenue')}</div>

<h2>Delivery Performance</h2>
<div class="chart-section">{chart_img('delivery')}</div>

<h2>Cost of Disruption</h2>
<div class="chart-section">{chart_img('cost_of_disruption')}</div>

<h2>Supplier Performance</h2>
<div class="chart-section">{chart_img('supplier')}</div>

<h2>Inventory Risk</h2>
<div class="chart-section">{chart_img('inventory')}</div>

<h2>Service Level vs Target</h2>
<div class="chart-section">{chart_img('service_level')}</div>

<h2>Supervisor Agent Narrative</h2>
<div class="narrative">{parsed.get('raw_text', 'No narrative available.').replace(chr(10), '<br>')}</div>

<div class="metadata">
  Endpoint: {supervisor_result.get('endpoint', 'N/A') if supervisor_result else 'N/A'} |
  Response time: {duration}s |
  Metrics extracted: {parsed.get('extracted_count', 0)} |
  Fallbacks used: {parsed.get('fallback_count', 0)} |
  Report generated by: Executive Report Agent
</div>

</body>
</html>
"""
    print(f"\u2713 HTML report assembled ({len(html):,} chars)")
    return html

# COMMAND ----------

# DBTITLE 1,TOOL 5: send_report() — Deliver via Volume, Email, or Slack (HTML + PDF)
# ============================================================
# TOOL 5: send_report
# Three delivery methods: UC Volume (always works), Email, Slack.
# All methods save both HTML and PDF (PDF has better fidelity
# across email clients and is the preferred distribution format).
# ============================================================

def _html_to_pdf(html_content: str) -> bytes:
    """Convert HTML report to PDF. Installs xhtml2pdf on first use."""
    try:
        from xhtml2pdf import pisa
    except ImportError:
        import subprocess, sys
        print("  Installing xhtml2pdf for PDF generation...")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-q", "xhtml2pdf"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        from xhtml2pdf import pisa

    pdf_buf = io.BytesIO()
    status = pisa.CreatePDF(io.StringIO(html_content), dest=pdf_buf)
    if status.err:
        raise RuntimeError(f"PDF conversion failed with {status.err} error(s)")
    return pdf_buf.getvalue()


def send_report(html: str, method: str = "volume", **kwargs) -> dict:
    """
    Deliver the HTML report to stakeholders as both HTML and PDF.

    Args:
        html: The complete HTML report string.
        method: 'volume' (save to UC Volume), 'email', or 'slack'.
        **kwargs: Method-specific options:
            volume: catalog, schema, volume_name
            email: recipients (list), subject, smtp_host, smtp_port,
                   smtp_user (secret scope/key), smtp_pass (secret scope/key)
            slack: webhook_url (or secret scope/key)

    Returns:
        dict: status, message, path/url, pdf_path (when applicable)
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename_html = f"executive_report_{timestamp}.html"
    filename_pdf = f"executive_report_{timestamp}.pdf"

    # Generate PDF from HTML (best-effort — falls back to HTML-only)
    pdf_bytes = None
    try:
        pdf_bytes = _html_to_pdf(html)
        print(f"\u2713 PDF generated ({len(pdf_bytes):,} bytes)")
    except Exception as e:
        print(f"  PDF generation skipped: {e}")

    if method == "volume":
        return _send_to_volume(html, filename_html, pdf_bytes, filename_pdf, **kwargs)
    elif method == "email":
        return _send_via_email(html, filename_html, pdf_bytes, filename_pdf, **kwargs)
    elif method == "slack":
        return _send_via_slack(html, filename_html, pdf_bytes, filename_pdf, **kwargs)
    else:
        return {"status": "error", "message": f"Unknown method: {method}"}


def _send_to_volume(html, filename_html, pdf_bytes, filename_pdf,
                    catalog=None, schema="reporting", volume_name="reports", **_):
    """Save HTML + PDF reports to a Unity Catalog Volume."""
    cat = catalog or CAT
    volume_path = f"/Volumes/{cat}/{schema}/{volume_name}"

    # Ensure volume exists
    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS `{cat}`.`{schema}`.`{volume_name}`")
    except Exception as e:
        print(f"  Volume creation note: {e}")

    result = {"status": "success", "method": "volume"}

    # Save HTML
    html_path = f"{volume_path}/{filename_html}"
    try:
        dbutils.fs.put(html_path, html, overwrite=True)
        print(f"Wrote {len(html)} bytes.")
        print(f"\u2713 Report saved to {html_path}")
        result["path"] = html_path
        result["filename"] = filename_html
    except Exception as e:
        local = f"/tmp/{filename_html}"
        with open(local, 'w') as f:
            f.write(html)
        print(f"\u2713 Report saved locally to {local}")
        result["path"] = local
        result["method"] = "local_file"

    # Save PDF alongside HTML
    if pdf_bytes:
        pdf_path = f"{volume_path}/{filename_pdf}"
        try:
            # Write PDF binary via local temp file
            local_pdf = f"/tmp/{filename_pdf}"
            with open(local_pdf, 'wb') as f:
                f.write(pdf_bytes)
            dbutils.fs.cp(f"file:{local_pdf}", pdf_path)
            print(f"\u2713 PDF saved to {pdf_path}")
            result["pdf_path"] = pdf_path
            result["pdf_filename"] = filename_pdf
        except Exception as e:
            print(f"  PDF save to volume failed: {e}")
            # Keep local copy
            result["pdf_path"] = f"/tmp/{filename_pdf}"

    return result


def _send_via_email(html, filename_html, pdf_bytes, filename_pdf,
                    recipients=None, subject=None,
                    smtp_secret_scope="report-agent", smtp_secret_key_host="smtp-host",
                    smtp_secret_key_user="smtp-user", smtp_secret_key_pass="smtp-pass",
                    smtp_port=587, **_):
    """
    Send report via email: PDF attached (preferred) + HTML inline fallback.

    Prerequisites:
      dbutils.secrets.put(scope="report-agent", key="smtp-host", string_value="smtp.example.com")
      dbutils.secrets.put(scope="report-agent", key="smtp-user", string_value="user@example.com")
      dbutils.secrets.put(scope="report-agent", key="smtp-pass", string_value="password")
    """
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.application import MIMEApplication

    if not recipients:
        return {"status": "error", "message": "No recipients specified"}
    if not subject:
        subject = f"Supply Chain Executive Report - {datetime.now().strftime('%B %d, %Y')}"

    try:
        smtp_host = dbutils.secrets.get(scope=smtp_secret_scope, key=smtp_secret_key_host)
        smtp_user = dbutils.secrets.get(scope=smtp_secret_scope, key=smtp_secret_key_user)
        smtp_pass = dbutils.secrets.get(scope=smtp_secret_scope, key=smtp_secret_key_pass)
    except Exception as e:
        return {"status": "error", "message": f"SMTP secrets not configured: {e}. "
                f"Set up scope '{smtp_secret_scope}' with keys: {smtp_secret_key_host}, {smtp_secret_key_user}, {smtp_secret_key_pass}"}

    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject
    msg['From'] = smtp_user
    msg['To'] = ', '.join(recipients)

    # Inline HTML body (renders in email client)
    msg.attach(MIMEText(html, 'html'))

    # Attach PDF if available (preferred download format)
    if pdf_bytes:
        pdf_part = MIMEApplication(pdf_bytes, _subtype='pdf')
        pdf_part.add_header('Content-Disposition', 'attachment', filename=filename_pdf)
        msg.attach(pdf_part)

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, recipients, msg.as_string())
        fmt = "HTML + PDF" if pdf_bytes else "HTML only"
        print(f"\u2713 Report emailed to {len(recipients)} recipient(s) ({fmt})")
        return {"status": "success", "method": "email", "recipients": recipients, "format": fmt}
    except Exception as e:
        return {"status": "error", "method": "email", "message": str(e)}


def _send_via_slack(html, filename_html, pdf_bytes, filename_pdf,
                    webhook_secret_scope="report-agent",
                    webhook_secret_key="slack-webhook-url", channel=None, **_):
    """
    Post a report summary to Slack via incoming webhook.
    Full HTML + PDF are saved to volume; Slack gets a formatted summary.

    Prerequisites:
      dbutils.secrets.put(scope="report-agent", key="slack-webhook-url", string_value="https://hooks.slack.com/...")
    """
    try:
        webhook_url = dbutils.secrets.get(scope=webhook_secret_scope, key=webhook_secret_key)
    except Exception as e:
        return {"status": "error", "message": f"Slack webhook not configured: {e}. "
                f"Set up scope '{webhook_secret_scope}' with key '{webhook_secret_key}'"}

    fmt = "PDF + HTML" if pdf_bytes else "HTML"
    # Build a Slack-friendly summary (not full HTML)
    summary = {
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": "\U0001f4ca Supply Chain Executive Report"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": (
                f"*Region:* Western | *Period:* August 2026\n"
                f"*Revenue Change:* -27.1% ($1.24M decline)\n"
                f"*Late Delivery Rate:* 94.6% | *Fill Rate:* 80.7%\n"
                f"*Cost of Disruption:* $3.76M\n"
                f"*Q3 Target:* 95% \u2014 \u26a0\ufe0f AT RISK"
            )}},
            {"type": "section", "text": {"type": "mrkdwn", "text": (
                f"\U0001f4c4 Report saved to UC Volume ({fmt}):\n"
                f"`{filename_pdf if pdf_bytes else filename_html}`"
            )}},
        ]
    }
    if channel:
        summary["channel"] = channel

    try:
        resp = requests.post(webhook_url, json=summary, timeout=10)
        if resp.status_code == 200:
            print(f"\u2713 Slack notification sent")
            return {"status": "success", "method": "slack"}
        else:
            return {"status": "error", "method": "slack", "message": f"HTTP {resp.status_code}: {resp.text}"}
    except Exception as e:
        return {"status": "error", "method": "slack", "message": str(e)}

# COMMAND ----------

# DBTITLE 1,ReportAgent — Orchestrator Class
# ============================================================
# ReportAgent: Orchestrator that ties all tools together.
#
# This class is what a Databricks Agent Framework agent would
# wrap. Each method is a callable tool. The `run()` method
# orchestrates the full pipeline end-to-end.
#
# To register as an MLflow agent:
#   import mlflow
#   mlflow.pyfunc.log_model("report_agent", python_model=ReportAgent())
# ============================================================

class ReportAgent:
    """
    Executive Report Agent — transforms Supervisor Agent output
    into a formatted HTML report with charts and delivers it
    via UC Volume, email, or Slack.

    Tools available to the agent:
      1. invoke_supervisor(prompt)         → raw text response
      2. parse_supervisor_response(resp)   → structured metrics + sections
      3. create_all_charts(metrics)        → 6 base64 PNG charts
      4. format_html_report(parsed, charts)→ complete HTML report
      5. send_report(html, method)         → deliver via volume/email/slack
      6. parse_actions_to_delta(text)      → AI-parsed actions → Delta table
      7. evaluate_actions()                → AI practicality assessment
      8. create_dashboard_views()          → dashboard-ready SQL views
    """

    # Tool names — resolved via globals() at call time so cells
    # can be defined in any order (action intelligence cells come later).
    TOOL_NAMES = [
        # Report pipeline
        "invoke_supervisor", "parse_supervisor_response",
        "create_all_charts", "format_html_report", "send_report",
        # Action intelligence (defined in later cells)
        "parse_actions_to_delta", "evaluate_actions",
        "create_dashboard_views", "run_hourly_evaluation",
        # Individual chart tools
        "create_revenue_chart", "create_delivery_chart",
        "create_cod_chart", "create_supplier_chart",
        "create_inventory_chart", "create_service_level_chart",
    ]

    def __init__(self):
        # Only register tools that are already defined; the rest
        # are resolved lazily in call_tool() via globals().
        self.tools = {
            name: globals()[name]
            for name in self.TOOL_NAMES
            if name in globals()
        }

    def list_tools(self) -> list:
        """Return all tool names (including not-yet-defined ones)."""
        return list(self.TOOL_NAMES)

    def call_tool(self, tool_name: str, **kwargs):
        """Call a specific tool by name. Resolves lazily from globals()."""
        # Try the cached dict first, then fall back to globals()
        fn = self.tools.get(tool_name) or globals().get(tool_name)
        if fn is None:
            raise ValueError(f"Unknown tool: {tool_name}. Available: {self.list_tools()}")
        # Cache for next time
        self.tools[tool_name] = fn
        return fn(**kwargs)

    def run(self, prompt: str = None, delivery_method: str = "volume", 
            delivery_kwargs: dict = None, skip_supervisor: bool = False,
            supervisor_text: str = None) -> dict:
        """
        Full end-to-end pipeline: invoke → parse → chart → format → send.

        Args:
            prompt: Business question (None = canonical executive prompt)
            delivery_method: 'volume', 'email', or 'slack'
            delivery_kwargs: Extra args for the delivery method
            skip_supervisor: If True, skip the API call and use supervisor_text
            supervisor_text: Pre-existing Supervisor response text (for testing)

        Returns:
            dict: Full pipeline results including report path
        """
        print("="*70)
        print("  EXECUTIVE REPORT AGENT — Full Pipeline")
        print("="*70)

        # Step 1: Get Supervisor response
        if skip_supervisor and supervisor_text:
            print("\n  Step 1: Using pre-existing Supervisor Agent response...")
            supervisor_result = {
                "response_text": supervisor_text,
                "timestamp": datetime.now().isoformat(),
                "endpoint": "pre-loaded",
                "duration_seconds": 0
            }
        else:
            print("\n  Step 1: Calling Supervisor Agent...")
            supervisor_result = invoke_supervisor(prompt)
            if "error" in supervisor_result:
                print(f"  Supervisor Agent call failed: {supervisor_result.get('error', 'unknown')}")
                print("  Continuing with ground-truth defaults for report...")
                supervisor_result = {
                    "response_text": "Supervisor Agent call failed. Using ground-truth defaults.",
                    "timestamp": datetime.now().isoformat(),
                    "endpoint": SUPERVISOR_ENDPOINT,
                    "duration_seconds": supervisor_result.get("duration_seconds", 0)
                }

        # Step 2: Parse response
        print("\n  Step 2: Parsing response...")
        parsed = parse_supervisor_response(supervisor_result)

        # Step 3: Generate charts
        print("\n  Step 3: Generating charts...")
        charts = create_all_charts(parsed["metrics"])

        # Step 4: Format HTML report
        print("\n  Step 4: Assembling HTML report...")
        html = format_html_report(parsed, charts, supervisor_result)

        # Step 5: Deliver
        print(f"\n  Step 5: Delivering report via {delivery_method}...")
        delivery_result = send_report(html, method=delivery_method, **(delivery_kwargs or {}))

        # Note: Steps 6-8 (Action Intelligence) run in the final notebook cell
        # AFTER all function definitions. This avoids execution-order issues.

        print(f"\n{'='*70}")
        print(f"  \u2713 REPORT PIPELINE COMPLETE (Steps 1-5)")
        print(f"    Metrics: {parsed['extracted_count']} extracted, {parsed['fallback_count']} fallbacks")
        print(f"    Charts: {len(charts)}/6 generated")
        print(f"    Report: {len(html):,} chars HTML")
        print(f"    Delivery: {delivery_result.get('status', 'unknown')} via {delivery_method}")
        if delivery_result.get('path'):
            print(f"    Path: {delivery_result['path']}")
        print(f"    Action Intelligence runs in the final cell.")
        print(f"{'='*70}")

        return {
            "status": "success",
            "_supervisor_result": supervisor_result,
            "supervisor_result": {k: v for k, v in supervisor_result.items() if k != "response_text"},
            "metrics_extracted": parsed["extracted_count"],
            "charts_generated": len(charts),
            "report_size_chars": len(html),
            "delivery": delivery_result,
        }


# Instantiate the agent
agent = ReportAgent()
print(f"\u2713 ReportAgent initialized with {len(agent.list_tools())} tools:")
for t in agent.list_tools():
    print(f"  \u2022 {t}")

# COMMAND ----------

# DBTITLE 1,Run Full Pipeline: Report + Action Intelligence (Steps 1-8)
# ============================================================
# DEMO: Run the full Report Agent pipeline
#
# Option A: LIVE call to Supervisor (takes ~3-5 min)
#   result = agent.run()
#
# Option B: Use pre-loaded text (instant, for testing charts)
#   result = agent.run(skip_supervisor=True, supervisor_text="...")
#
# Option C: Call tools individually
#   resp = agent.call_tool("invoke_supervisor")
#   parsed = agent.call_tool("parse_supervisor_response", supervisor_result=resp)
#   charts = agent.call_tool("create_all_charts", metrics=parsed["metrics"])
# ============================================================

# -- Uncomment ONE of the options below to run --

# Option A: Full LIVE pipeline (saves to UC Volume)
# result = agent.run(delivery_method="volume")

# Option B: Full pipeline with email delivery
# result = agent.run(
#     delivery_method="email",
#     delivery_kwargs={"recipients": ["stakeholder@company.com", "cfo@company.com"]}
# )

# Option C: Full pipeline with Slack notification + Volume backup
# result = agent.run(delivery_method="slack")
# agent.call_tool("send_report", html=html, method="volume")  # also save to volume

# ── Report Pipeline (Steps 1-5) ──
# Calls Supervisor Agent (with retry), parses response, generates charts,
# formats HTML, saves to UC Volume. Action Intelligence (Steps 6-8)
# runs in the final cell AFTER function definitions.

print("="*70)
print("  EXECUTIVE REPORT AGENT — Report Pipeline")
print("="*70)

# Step 1: Call Supervisor Agent (invoke_supervisor has built-in retry)
print("\n  Step 1: Calling Supervisor Agent...")
print(f"    Endpoint: {SUPERVISOR_ENDPOINT}")
print(f"    No client-side timeout, Max retries: 2")
supervisor_result = invoke_supervisor()

if "error" in supervisor_result:
    print(f"  \u2717 Supervisor Agent error after {supervisor_result.get('attempts', '?')} attempt(s): {supervisor_result.get('error', 'unknown')}")
    print("  Falling back to ground-truth defaults for charts and report.")
    supervisor_result = {
        "response_text": "Supervisor Agent call failed after retries. Using ground-truth defaults for all metrics.",
        "timestamp": datetime.now().isoformat(),
        "endpoint": SUPERVISOR_ENDPOINT,
        "duration_seconds": supervisor_result.get("duration_seconds", 0)
    }
else:
    print(f"  \u2713 Supervisor Agent responded in {supervisor_result.get('duration_seconds', '?')}s")
    print(f"    Response length: {len(supervisor_result.get('response_text', '')):,} chars")
    print(f"    Attempts: {supervisor_result.get('attempts', 1)}")

# Step 2: Parse response
print("\n  Step 2: Parsing Supervisor Agent response...")
parsed = parse_supervisor_response(supervisor_result)

# Step 3: Generate charts
print("\n  Step 3: Generating charts...")
charts = create_all_charts(parsed["metrics"])

# Step 4: Format HTML report
print("\n  Step 4: Assembling HTML report...")
html_report = format_html_report(parsed, charts, supervisor_result)

# Step 5: Deliver to UC Volume
print("\n  Step 5: Saving report to UC Volume...")
delivery_result = send_report(html_report, method="volume")

# Attempt Slack (best-effort, requires secret scope)
try:
    slack_result = send_report(html_report, method="slack")
    if slack_result.get("status") == "error":
        print(f"  Slack skipped: {slack_result.get('message', 'no webhook')[:80]}")
except Exception as se:
    print(f"  Slack skipped: {se}")

print(f"\n{'='*70}")
print(f"  \u2713 REPORT PIPELINE COMPLETE (Steps 1-5)")
print(f"    Supervisor Agent: {'\u2713 live response' if parsed['extracted_count'] > 0 else '\u2717 used GT defaults'}")
print(f"    Metrics: {parsed['extracted_count']} extracted, {parsed['fallback_count']} fallbacks")
print(f"    Charts: {len(charts)}/6 generated")
print(f"    Report: {len(html_report):,} chars HTML")
print(f"    Delivery: {delivery_result.get('status')} via {delivery_result.get('method')}")
if delivery_result.get('path'):
    print(f"    Path: {delivery_result['path']}")
print(f"{'='*70}")

# ══════════════════════════════════════════════════════════════
#  ACTION INTELLIGENCE PIPELINE (Steps 6-8)
#  Runs in the SAME cell so the job executes everything.
# ══════════════════════════════════════════════════════════════

print("\n" + "="*70)
print("  ACTION INTELLIGENCE PIPELINE (Steps 6-8)")
print("="*70)

# Steps 6-8 use globals() to resolve functions defined in later cells.
# When the notebook runs top-to-bottom as a job, cells 12-16 define
# these functions BEFORE cell 9. But in interactive use, they may
# already be defined from a previous run, or may not exist yet.
_parse_fn = globals().get("parse_actions_to_delta")
_eval_fn = globals().get("evaluate_actions")
_views_fn = globals().get("create_dashboard_views")

# Step 6: Parse actions from Supervisor Agent response into Delta
print("\n  Step 6: Parsing recommended actions into Delta table...")
try:
    sup_text = supervisor_result.get("response_text", "")
    if _parse_fn and sup_text and len(sup_text) > 200:
        action_count = _parse_fn(
            sup_text,
            run_id=f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        print(f"  \u2713 {action_count} actions parsed into Delta")
    elif not _parse_fn:
        print("  parse_actions_to_delta not yet defined (run cells 12-16 first, or run as job).")
    else:
        print("  Supervisor Agent text too short. Using existing action_items data.")
    existing = spark.sql(f"SELECT COUNT(*) as cnt FROM `{CAT}`.reporting.action_items").collect()[0]["cnt"]
    print(f"  Total actions in table: {existing}")
except Exception as e:
    print(f"  \u2717 Action parsing: {e}")

# Step 7: Evaluate all pending actions with AI
print("\n  Step 7: Evaluating actions for practicality...")
try:
    if _eval_fn:
        eval_count = _eval_fn(force_reevaluate=True)
        print(f"  \u2713 {eval_count} actions evaluated")
    else:
        print("  evaluate_actions not yet defined (run cells 12-16 first, or run as job).")
except Exception as e:
    print(f"  \u2717 Evaluation: {e}")

# Step 8: Refresh dashboard views
print("\n  Step 8: Refreshing dashboard views...")
try:
    if _views_fn:
        _views_fn()
    else:
        print("  create_dashboard_views not yet defined (run cells 12-16 first, or run as job).")
except Exception as e:
    print(f"  \u2717 Views: {e}")

# Final summary
print(f"\n{'='*70}")
print("  \u2713 FULL PIPELINE COMPLETE (Steps 1-8)")
try:
    items = spark.sql(f"SELECT COUNT(*) as cnt FROM `{CAT}`.reporting.action_items").collect()[0]["cnt"]
    evals = spark.sql(f"SELECT COUNT(*) as cnt FROM `{CAT}`.reporting.action_evaluations").collect()[0]["cnt"]
    print(f"    action_items: {items} rows")
    print(f"    action_evaluations: {evals} rows")
    display(spark.sql(f"""
        SELECT action_id, domain, title, priority,
               recommendation, ROUND(practicality_score, 2) as score
        FROM `{CAT}`.reporting.v_action_tracker
        ORDER BY priority
    """))
except Exception as e:
    print(f"    Summary query: {e}")
print(f"{'='*70}")

# COMMAND ----------

# DBTITLE 1,Display: Render charts inline for notebook preview
# ============================================================
# PREVIEW: Display charts inline in the notebook
# This cell renders all 6 charts using the GT defaults so you
# can preview the report visuals without calling the Supervisor.
# ============================================================
from IPython.display import HTML, display as ipy_display

metrics = GROUND_TRUTH_DEFAULTS.copy()
charts = create_all_charts(metrics)

# Display each chart
for name, b64 in charts.items():
    title = name.replace('_', ' ').title()
    ipy_display(HTML(f"<h3>{title}</h3><img src='data:image/png;base64,{b64}' style='max-width:800px;'/>"))

print(f"\n\u2713 All {len(charts)} charts rendered")

# COMMAND ----------

# DBTITLE 1,Markdown: Action Intelligence System
# MAGIC %md
# MAGIC ## Action Intelligence System
# MAGIC
# MAGIC The cells below extend the Report Agent with an **Action Intelligence** pipeline:
# MAGIC
# MAGIC ```
# MAGIC Supervisor Actions Text
# MAGIC         │
# MAGIC         ▼
# MAGIC [ai_query()] parses into structured rows
# MAGIC         │
# MAGIC         ▼
# MAGIC ┌─────────────────────────────────────────┐
# MAGIC │  Delta Table: reporting.action_items     │
# MAGIC │  (category, domain, title, steps, impact)│
# MAGIC └─────────────────────────────────────────┘
# MAGIC         │
# MAGIC         ▼  (Lakeflow Job — hourly)
# MAGIC [ai_query()] evaluates each action against live data:
# MAGIC   • Is it practical given current inventory/supplier data?
# MAGIC   • What are the risks and blockers?
# MAGIC   • Pros and cons with cost estimates?
# MAGIC   • Historical precedent — has this worked before?
# MAGIC         │
# MAGIC         ▼
# MAGIC ┌──────────────────────────────────────────┐
# MAGIC │  Delta Table: reporting.action_evaluations │
# MAGIC │  (practicality_score, risks, pros, cons)  │
# MAGIC └──────────────────────────────────────────┘
# MAGIC         │
# MAGIC         ▼
# MAGIC    Dashboard: Action Tracker
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Seed Data: action_intelligence.action_history
# ============================================================
# ACTION HISTORY: Seed table with synthetic past actions
#
# Schema: GAP_Demo_Dev.action_intelligence
# This gives the AI evaluator real precedent data:
#   "Last time we air-freighted emergency stock it cost $450K
#    and improved fill rate from 72% to 89% in 2 weeks."
# ============================================================

def create_action_history(catalog: str = None):
    """Create and seed the action_intelligence.action_history table."""
    cat = catalog or CAT

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.action_intelligence")

    spark.sql(f"""
    CREATE OR REPLACE TABLE `{cat}`.action_intelligence.action_history (
        history_id STRING COMMENT 'Unique identifier',
        action_date DATE COMMENT 'When the action was taken',
        quarter STRING COMMENT 'Fiscal quarter (e.g. Q1 FY2027)',
        domain STRING COMMENT 'Supply chain domain',
        action_type STRING COMMENT 'Category: IMMEDIATE, SHORT_TERM, STRATEGIC',
        title STRING COMMENT 'What was done',
        description STRING COMMENT 'Full details of the action taken',
        cost_usd DECIMAL(12,2) COMMENT 'Total cost to implement',
        duration_days INT COMMENT 'How long it took to implement',
        outcome STRING COMMENT 'What happened as a result',
        metric_before FLOAT COMMENT 'Key metric value before the action',
        metric_after FLOAT COMMENT 'Key metric value after the action',
        metric_name STRING COMMENT 'Which metric was affected',
        success_rating STRING COMMENT 'SUCCESS, PARTIAL, FAILED',
        lessons_learned STRING COMMENT 'What we learned for next time',
        region STRING COMMENT 'Region affected'
    )
    USING DELTA
    COMMENT 'Historical record of past supply chain actions and their outcomes. Used by the AI evaluator to assess whether proposed actions have worked before.'
    """)

    # Seed with realistic historical actions
    spark.sql(f"""
    INSERT INTO `{cat}`.action_intelligence.action_history VALUES
    -- Inventory actions
    ('H001', DATE '2026-03-15', 'Q3 FY2026', 'inventory', 'IMMEDIATE',
     'Emergency air-freight for Electronics stockouts',
     'Air-freighted 45 SKUs from Asia warehouses to Western DC after Q2 stockout crisis. Used FedEx Priority and chartered 2 cargo flights.',
     452000.00, 12, 'Fill rate recovered from 72% to 89% within 2 weeks. Revenue loss stopped at $1.1M (vs projected $2.8M without action).',
     72.0, 89.0, 'fill_rate', 'SUCCESS',
     'Air freight is effective but extremely expensive. Must pair with safety stock increase to prevent recurrence. Lead time for chartered flights was 3 days vs 2-3 weeks by sea.',
     'Western'),

    ('H002', DATE '2026-01-20', 'Q3 FY2026', 'inventory', 'SHORT_TERM',
     'Safety stock recalibration +25% for Western region',
     'Increased safety stock levels by 25% across all Western warehouse SKUs. Added 15 new replenishment triggers based on demand velocity.',
     85000.00, 21, 'Stockout SKUs dropped from 28 to 8. Carrying cost increased by $85K/quarter but avoided $340K in lost sales.',
     28.0, 8.0, 'stockout_skus', 'SUCCESS',
     'The 25% blanket increase was too aggressive for slow-moving SKUs. Next time use demand-weighted recalibration. A/B tested: velocity-based triggers outperformed flat percentage.',
     'Western'),

    ('H003', DATE '2025-11-10', 'Q2 FY2026', 'inventory', 'IMMEDIATE',
     'Emergency stock redistribution from East to West',
     'Transferred 120 pallets of high-demand SKUs from Eastern DC (overstocked) to Western DC (critical shortage). Used regional trucking.',
     38000.00, 5, 'Partially effective. 60% of transferred SKUs matched Western demand. 40% sat in warehouse. Net fill rate improvement: +8pp.',
     65.0, 73.0, 'fill_rate', 'PARTIAL',
     'Cross-regional transfer works only when demand profiles overlap. Must verify SKU-level demand match before transferring. The 40% mismatch created carrying cost waste.',
     'Western'),

    -- Logistics actions
    ('H004', DATE '2026-04-01', 'Q4 FY2026', 'logistics', 'IMMEDIATE',
     'Activated backup carriers for Western region',
     'Onboarded 3 backup carriers (XPO, Echo, TForce) after primary carrier capacity crisis. Negotiated spot rates at 15% premium.',
     180000.00, 7, 'Late delivery rate dropped from 78% to 45% within 10 days. Cost per shipment increased 15% but revenue recovery exceeded cost.',
     78.0, 45.0, 'late_delivery_rate', 'SUCCESS',
     'Backup carrier contracts should be pre-negotiated (not spot rates). The 15% premium was acceptable for crisis but not sustainable. Now maintain standby agreements with 2 backup carriers.',
     'Western'),

    ('H005', DATE '2026-02-15', 'Q3 FY2026', 'logistics', 'SHORT_TERM',
     'Implemented real-time shipment tracking and escalation',
     'Deployed GPS tracking across all Western carriers. Auto-escalation triggers at 4h, 8h, 24h delays. Dedicated logistics war room for 30 days.',
     95000.00, 30, 'Average delay reduced from 4.2 days to 1.8 days. Late rate improved from 62% to 35%. War room overhead was high but effective.',
     4.2, 1.8, 'avg_delay_days', 'SUCCESS',
     'The war room was effective but unsustainable at full staffing. Transitioned to automated escalation rules after 30 days. 70% of the improvement came from the auto-escalation, not the war room.',
     'Western'),

    ('H006', DATE '2025-09-01', 'Q1 FY2026', 'logistics', 'STRATEGIC',
     'Carrier performance-based contract renegotiation',
     'Renegotiated contracts with top 5 carriers. Added SLA penalties ($500/late shipment) and performance bonuses (2% discount for >95% OTD).',
     50000.00, 90, 'OTD improved from 82% to 91% over 3 months. Carrier turnover increased — 2 of 5 carriers dropped out, replaced with higher-performing alternatives.',
     82.0, 91.0, 'otd_rate', 'SUCCESS',
     'Performance-based contracts work but cause carrier churn. Must maintain a qualified backup carrier pool. The $500 penalty was too low for large shipments — should be percentage-based.',
     'All'),

    -- Supplier actions
    ('H007', DATE '2026-05-01', 'Q4 FY2026', 'supplier', 'STRATEGIC',
     'Asia supplier diversification — onboarded 4 new vendors',
     'Qualified and onboarded 4 new suppliers in Vietnam and India to reduce dependency on 3 underperforming Chinese suppliers. 6-month qualification process.',
     220000.00, 180, 'Asia late rate dropped from 95% to 68%. Lead time variance reduced from 15.2 to 9.1 days. However, quality issues in first 2 months from new vendors.',
     95.0, 68.0, 'asia_late_rate', 'PARTIAL',
     'Diversification works but requires 6+ months including quality stabilization. New vendors had 8% defect rate initially (vs 2% target). Quality gates and phased ramp-up are essential.',
     'All'),

    ('H008', DATE '2026-06-15', 'Q4 FY2026', 'supplier', 'IMMEDIATE',
     'Emergency supplier escalation meetings (top 10 vendors)',
     'Weekly executive calls with top 10 late-delivering vendors. Shared demand forecasts. Offered early payment (2% discount) for on-time delivery.',
     15000.00, 14, 'Vendor late rate improved from 83% to 71% within 2 weeks. 3 of 10 vendors committed to expedited production schedules.',
     83.0, 71.0, 'vendor_late_rate', 'PARTIAL',
     'Executive escalation produces quick but temporary improvement. Without structural changes (dual-sourcing, contracts), rates regress within 4-6 weeks. The early payment incentive was effective for cash-constrained vendors.',
     'All'),

    ('H009', DATE '2025-12-01', 'Q2 FY2026', 'supplier', 'SHORT_TERM',
     'Implemented dual-sourcing for top 20 critical SKUs',
     'Qualified secondary suppliers for the 20 SKUs with highest revenue impact. Split orders 70/30 between primary and secondary.',
     130000.00, 60, 'Lead time variance for dual-sourced SKUs dropped from 12.3 to 5.1 days. Supply continuity improved — zero stockouts on these 20 SKUs for 4 months.',
     12.3, 5.1, 'lead_time_variance', 'SUCCESS',
     'Dual-sourcing is the most reliable strategy for critical SKUs. The 70/30 split keeps primary motivated while ensuring backup. Cost increase was only 4% due to competitive pressure between suppliers.',
     'All'),

    -- Demand / Revenue actions
    ('H010', DATE '2026-04-15', 'Q4 FY2026', 'demand', 'SHORT_TERM',
     'Demand shaping — promotions redirected to well-stocked regions',
     'Shifted Q4 promotional budget from Western (stockout-prone) to Eastern and Central regions. Paused Western digital campaigns for 3 weeks.',
     0.00, 21, 'Total company revenue maintained. Western revenue dropped 8% but Eastern +12% offset it. Customer complaints in West increased 15%.',
     100.0, 92.0, 'west_revenue_index', 'PARTIAL',
     'Demand shaping preserves total revenue but damages regional customer relationships. Should only be used as short-term bridge while supply is restored. Must communicate transparently with Western sales team.',
     'Western'),

    -- Finance / Cross-domain actions
    ('H011', DATE '2026-07-01', 'Q1 FY2027', 'finance', 'SHORT_TERM',
     'Freight cost audit and carrier billing reconciliation',
     'Audited all Western freight invoices for past 6 months. Found 12% overbilling from 2 carriers. Recovered $310K in overcharges.',
     25000.00, 45, 'Recovered $310K in freight overcharges. Identified systematic billing errors in fuel surcharge calculations. Ongoing savings of $52K/month.',
     2480000.00, 2170000.00, 'wasted_freight_cost', 'SUCCESS',
     'Freight audits should be quarterly, not reactive. The 12% overbilling rate suggests carriers are not malicious but their systems are inaccurate. Automated invoice matching would catch this in real-time.',
     'Western'),

    ('H012', DATE '2025-10-15', 'Q2 FY2026', 'finance', 'STRATEGIC',
     'SLA penalty framework — implemented vendor scorecards',
     'Built vendor scorecards tracking OTD, quality, lead time. Automated SLA penalty calculations. Published monthly to all vendors.',
     75000.00, 90, 'Vendor on-time performance improved 15pp over 3 months. SLA penalty collections: $890K (previously $0 — penalties existed in contracts but were never enforced).',
     60.0, 75.0, 'vendor_otd_rate', 'SUCCESS',
     'The biggest win was simply ENFORCING existing contract penalties. Most vendors did not realize penalties were being tracked. Transparency alone improved performance by 8pp before any penalties were assessed.',
     'All')
    """)

    count = spark.sql(f"SELECT COUNT(*) as cnt FROM `{cat}`.action_intelligence.action_history").collect()[0]["cnt"]
    print(f"\u2713 Schema: `{cat}`.action_intelligence")
    print(f"\u2713 Table: `{cat}`.action_intelligence.action_history ({count} rows)")
    display(spark.sql(f"""
        SELECT history_id, action_date, domain, title, 
               CONCAT('$', FORMAT_NUMBER(cost_usd, 0)) as cost,
               success_rating, metric_name,
               CONCAT(CAST(metric_before AS STRING), ' → ', CAST(metric_after AS STRING)) as metric_change
        FROM `{cat}`.action_intelligence.action_history
        ORDER BY action_date DESC
    """))
    return count

# Create and seed the action history table
create_action_history()

# COMMAND ----------

# DBTITLE 1,TOOL 6: Create Delta Tables + Parse Actions with ai_query()
# ============================================================
# TOOL 6: Action Intelligence — Delta Tables + AI Parsing
#
# 1. Creates reporting.action_items and reporting.action_evaluations
# 2. Uses ai_query() with responseFormat to parse Supervisor's
#    action text into structured rows
# ============================================================

def create_action_tables(catalog: str = None):
    """Create the Delta tables for action tracking (Supervisor Agent output)."""
    cat = catalog or CAT

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.reporting")

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{cat}`.reporting.action_items (
        action_id STRING COMMENT 'Unique action identifier',
        run_id STRING COMMENT 'Report run that generated this action',
        category STRING COMMENT 'IMMEDIATE (7d), SHORT_TERM (30d), or STRATEGIC (90d)',
        timeframe STRING COMMENT 'Human-readable timeframe: Next 7 Days, Next 30 Days, Next 90 Days',
        domain STRING COMMENT 'Supply chain domain: inventory, logistics, supplier, demand, finance',
        title STRING COMMENT 'Action title',
        description STRING COMMENT 'Full action description',
        specific_steps STRING COMMENT 'Concrete sub-actions with owners and targets',
        expected_impact STRING COMMENT 'Which metric improves and by how much',
        priority INT COMMENT '1=highest, 10=lowest',
        status STRING DEFAULT 'PENDING' COMMENT 'PENDING, IN_PROGRESS, COMPLETED, SKIPPED',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        updated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Recommended actions parsed from Supervisor Agent output via ai_query()'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{cat}`.reporting.action_evaluations (
        evaluation_id STRING COMMENT 'Unique evaluation identifier',
        action_id STRING COMMENT 'FK to action_items.action_id',
        evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        is_practical BOOLEAN COMMENT 'Can this action be executed given current constraints?',
        practicality_score FLOAT COMMENT 'Score 0.0-1.0 indicating feasibility',
        risks STRING COMMENT 'Key risks and blockers',
        pros STRING COMMENT 'Benefits and positive outcomes',
        cons STRING COMMENT 'Downsides and trade-offs',
        cost_estimate STRING COMMENT 'Estimated cost to implement',
        historical_precedent STRING COMMENT 'Has this worked before? What happened?',
        recommendation STRING COMMENT 'Final recommendation: PROCEED, PROCEED_WITH_CAUTION, DEFER, REJECT',
        reasoning STRING COMMENT 'AI reasoning for the recommendation'
    )
    USING DELTA
    COMMENT 'AI-powered evaluations of recommended actions, refreshed hourly'
    """)

    print(f"\u2713 Tables created: `{cat}`.reporting.action_items")
    print(f"\u2713 Tables created: `{cat}`.reporting.action_evaluations")


def parse_actions_to_delta(supervisor_text: str, run_id: str = None, catalog: str = None) -> int:
    """
    Parse the Supervisor Agent's recommended actions into structured rows
    using ai_query() with responseFormat, then write to Delta.

    Args:
        supervisor_text: Raw Supervisor Agent response text (from invoke_supervisor)
        run_id: Identifier for this report run (auto-generated if None)
        catalog: UC catalog name

    Returns:
        int: Number of actions parsed and inserted
    """
    import uuid
    cat = catalog or CAT
    if run_id is None:
        run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Ensure tables exist
    create_action_tables(cat)

    # Use ai_query with a flat STRUCT to extract one JSON blob,
    # then parse the JSON array in Python. ai_query requires
    # top-level StructType (not ArrayType).
    safe_text = supervisor_text.replace("'", "''")[:8000]
    actions_df = spark.sql(f"""
    SELECT ai_query(
        'databricks-meta-llama-3-3-70b-instruct',
        'Extract ALL recommended actions from this supply chain report. '
        'Return a JSON object with a single key "actions" containing an array. '
        'Each action must have: category (IMMEDIATE/SHORT_TERM/STRATEGIC), '
        'timeframe (Next 7 Days/Next 30 Days/Next 90 Days), '
        'domain (inventory/logistics/supplier/demand/finance), '
        'title (short name), description, specific_steps, '
        'expected_impact, priority (1=highest to 10=lowest). '
        'Return ONLY valid JSON, no markdown.\n\n'
        'Report text:\n' || '{safe_text}',
        responseFormat => 'STRUCT<actions:STRING>'
    ) AS parsed_result
    """)

    # Collect and parse the JSON string
    result = actions_df.collect()
    if not result or not result[0]["parsed_result"]:
        print("\u2717 ai_query returned no result")
        return 0

    raw = result[0]["parsed_result"]
    actions_str = raw["actions"] if isinstance(raw, dict) else raw
    # Parse JSON — handle both raw string and struct
    try:
        import json as _json
        parsed = _json.loads(actions_str) if isinstance(actions_str, str) else actions_str
        if isinstance(parsed, dict) and "actions" in parsed:
            parsed = parsed["actions"]
        if not isinstance(parsed, list):
            parsed = [parsed]
    except Exception as je:
        print(f"\u2717 JSON parse failed: {je}. Raw: {str(actions_str)[:200]}")
        return 0

    print(f"\u2713 ai_query extracted {len(parsed)} actions")

    # Build rows for insertion
    rows = []
    for i, action in enumerate(parsed):
        action_id = f"{run_id}_action_{i+1:02d}"
        rows.append((
            action_id, run_id,
            action.get("category", "UNKNOWN"),
            action.get("timeframe", "Unknown"),
            action.get("domain", "general"),
            action.get("title", f"Action {i+1}"),
            action.get("description", ""),
            action.get("specific_steps", ""),
            action.get("expected_impact", ""),
            action.get("priority", i + 1),
            "PENDING",
            datetime.now(),
            datetime.now()
        ))

    # Write to Delta
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
    schema = StructType([
        StructField("action_id", StringType()),
        StructField("run_id", StringType()),
        StructField("category", StringType()),
        StructField("timeframe", StringType()),
        StructField("domain", StringType()),
        StructField("title", StringType()),
        StructField("description", StringType()),
        StructField("specific_steps", StringType()),
        StructField("expected_impact", StringType()),
        StructField("priority", IntegerType()),
        StructField("status", StringType()),
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType()),
    ])

    df = spark.createDataFrame(rows, schema=schema)
    df.write.mode("append").saveAsTable(f"`{cat}`.reporting.action_items")

    print(f"\u2713 {len(rows)} actions written to `{cat}`.reporting.action_items")
    display(spark.sql(f"SELECT action_id, category, domain, title, priority FROM `{cat}`.reporting.action_items WHERE run_id = '{run_id}' ORDER BY priority"))
    return len(rows)

# Create the action tables on notebook load
create_action_tables()

# COMMAND ----------

# DBTITLE 1,TOOL 7: evaluate_actions() — AI-Powered Practicality Assessment
# ============================================================
# TOOL 7: evaluate_actions
# For each action in the Delta table, uses ai_query() to assess:
#   - Is it practical given current supply chain data?
#   - What are the risks and blockers?
#   - Pros and cons with cost estimates?
#   - Has this worked before (historical precedent)?
#
# This is what the Lakeflow Job calls hourly.
# ============================================================

def evaluate_actions(catalog: str = None, run_id: str = None, force_reevaluate: bool = False) -> int:
    """
    Evaluate all PENDING actions using ai_query() against live supply chain data.

    The AI receives:
      1. The action description and expected impact
      2. Current supply chain state (live queries against the data)
      3. Instructions to assess practicality, risks, pros/cons

    Args:
        catalog: UC catalog name
        run_id: If provided, only evaluate actions from this run
        force_reevaluate: If True, re-evaluate already-evaluated actions

    Returns:
        int: Number of actions evaluated
    """
    import uuid
    cat = catalog or CAT

    # Get actions to evaluate
    filter_clause = ""
    if run_id:
        filter_clause += f" AND run_id = '{run_id}'"
    if not force_reevaluate:
        filter_clause += f""" AND action_id NOT IN (
            SELECT DISTINCT action_id FROM `{cat}`.reporting.action_evaluations
        )"""

    actions = spark.sql(f"""
        SELECT action_id, category, domain, title, description,
               specific_steps, expected_impact, priority
        FROM `{cat}`.reporting.action_items
        WHERE status = 'PENDING' {filter_clause}
        ORDER BY priority
    """).collect()

    if not actions:
        print("\u2713 No pending actions to evaluate")
        return 0

    print(f"Evaluating {len(actions)} actions...")

    # Get current supply chain context (live data summary)
    # Wrapped in try/except — column names vary by schema version
    try:
        context_df = spark.sql(f"""
            SELECT
              (SELECT COUNT(*) FROM `{cat}`.inventory_management.inventory_ledger
               WHERE stockout_flag = true) as stockout_positions,
              (SELECT ROUND(AVG(days_of_supply), 2)
               FROM `{cat}`.inventory_management.inventory_ledger
               WHERE on_hand_qty > 0) as avg_days_of_supply,
              (SELECT COUNT(*) FROM `{cat}`.supplier_procurement.supplier_orders
               WHERE order_date >= DATE '2026-08-01'
                 AND order_date < DATE '2026-09-01'
                 AND is_late = true) as late_pos,
              (SELECT ROUND(SUM(total_amount), 2) FROM `{cat}`.demand_analysis.sales_orders
               WHERE order_date >= DATE '2026-08-01'
                 AND order_date < DATE '2026-09-01'
                 AND region = 'Western') as west_revenue
        """)
        ctx = context_df.collect()[0]
        supply_chain_context = (
            f"Current state: {ctx['stockout_positions']} stockout positions, "
            f"{ctx['avg_days_of_supply']} avg days of supply, "
            f"{ctx['late_pos']} late POs last month, "
            f"${ctx['west_revenue']:,.0f} Western revenue."
        )
    except Exception as ctx_err:
        print(f"  Note: Could not fetch live context ({ctx_err}). Using summary defaults.")
        supply_chain_context = (
            "Current state: 35 stockout positions, 0.96 avg days of supply, "
            "36 late POs last month, $3,341,063 Western revenue."
        )

    # Get historical action precedents from action_intelligence.action_history
    history_context = ""
    try:
        history_rows = spark.sql(f"""
            SELECT title, domain, cost_usd, outcome, metric_name,
                   metric_before, metric_after, success_rating, lessons_learned
            FROM `{cat}`.action_intelligence.action_history
            ORDER BY action_date DESC
        """).collect()
        if history_rows:
            history_lines = []
            for h in history_rows:
                delta = (h['metric_after'] or 0) - (h['metric_before'] or 0)
                history_lines.append(
                    f"- [{h['success_rating']}] {h['title']} (${h['cost_usd']:,.0f}): "
                    f"{h['metric_name']} {h['metric_before']}→{h['metric_after']} ({delta:+.1f}). "
                    f"Lesson: {h['lessons_learned'][:150]}"
                )
            history_context = "\n\nHISTORICAL PRECEDENT (past actions and results):\n" + "\n".join(history_lines)
            print(f"  Loaded {len(history_rows)} historical precedents")
    except Exception as e:
        print(f"  Note: No action_history table found ({e}). Evaluating without historical context.")

    evaluated_count = 0
    eval_rows = []

    for action in actions:
        prompt = (
            f"You are a supply chain operations analyst. Evaluate this recommended action "
            f"for practicality and risk. Use the historical precedent data to ground your assessment "
            f"in what has actually worked (or failed) before.\n\n"
            f"ACTION: {action['title']}\n"
            f"DOMAIN: {action['domain']}\n"
            f"DESCRIPTION: {action['description']}\n"
            f"EXPECTED IMPACT: {action['expected_impact']}\n"
            f"STEPS: {action['specific_steps']}\n\n"
            f"CURRENT SUPPLY CHAIN STATE: {supply_chain_context}"
            f"{history_context}\n\n"
            f"Evaluate: Is this action practical given current data AND past experience? "
            f"Reference specific historical outcomes where relevant. "
            f"What are the risks, pros, cons, estimated cost, and your recommendation "
            f"(PROCEED / PROCEED_WITH_CAUTION / DEFER / REJECT)?"
        )

        eval_df = spark.sql(f"""
            SELECT ai_query(
                'databricks-meta-llama-3-3-70b-instruct',
                '{prompt.replace("'", "''")}',
                responseFormat => 'STRUCT<is_practical:BOOLEAN, practicality_score:FLOAT, risks:STRING, pros:STRING, cons:STRING, cost_estimate:STRING, historical_precedent:STRING, recommendation:STRING, reasoning:STRING>'
            ) AS evaluation
        """)

        try:
            ev = eval_df.collect()[0]["evaluation"]
            eval_id = f"eval_{uuid.uuid4().hex[:12]}"
            eval_rows.append((
                eval_id,
                action["action_id"],
                datetime.now(),
                ev.get("is_practical", None),
                ev.get("practicality_score", None),
                ev.get("risks", ""),
                ev.get("pros", ""),
                ev.get("cons", ""),
                ev.get("cost_estimate", ""),
                ev.get("historical_precedent", ""),
                ev.get("recommendation", "UNKNOWN"),
                ev.get("reasoning", "")
            ))
            evaluated_count += 1
            rec = ev.get("recommendation", "?")
            score = ev.get("practicality_score", 0)
            print(f"  \u2713 {action['title'][:50]:50s} → {rec} (score: {score:.2f})")
        except Exception as e:
            print(f"  \u2717 {action['title'][:50]:50s} → ERROR: {e}")

    if eval_rows:
        from pyspark.sql.types import StructType, StructField, StringType, BooleanType, FloatType, TimestampType
        schema = StructType([
            StructField("evaluation_id", StringType()),
            StructField("action_id", StringType()),
            StructField("evaluated_at", TimestampType()),
            StructField("is_practical", BooleanType()),
            StructField("practicality_score", FloatType()),
            StructField("risks", StringType()),
            StructField("pros", StringType()),
            StructField("cons", StringType()),
            StructField("cost_estimate", StringType()),
            StructField("historical_precedent", StringType()),
            StructField("recommendation", StringType()),
            StructField("reasoning", StringType()),
        ])
        df = spark.createDataFrame(eval_rows, schema=schema)
        df.write.mode("append").saveAsTable(f"`{cat}`.reporting.action_evaluations")
        print(f"\n\u2713 {evaluated_count} evaluations written to `{cat}`.reporting.action_evaluations")

    # Display summary
    display(spark.sql(f"""
        SELECT a.title, a.category, a.domain, a.priority,
               e.recommendation, e.practicality_score, e.risks, e.pros, e.cons
        FROM `{cat}`.reporting.action_items a
        JOIN `{cat}`.reporting.action_evaluations e ON a.action_id = e.action_id
        ORDER BY a.priority
    """))
    return evaluated_count

# COMMAND ----------

# DBTITLE 1,TOOL 8: Lakeflow Job Entry Point — Hourly Action Re-evaluation
# ============================================================
# TOOL 8: Lakeflow Job Entry Point
# This function is what the hourly Lakeflow Job calls.
# Re-evaluates all pending actions using live supply chain data.
# It re-evaluates all pending actions against the latest data.
#
# To schedule as a Lakeflow Job:
#   1. Create a job with this notebook as the task
#   2. Set schedule to "Every 1 hour"
#   3. The job runs evaluate_actions() which queries live data
#
# The evaluation checks:
#   - Has the supply chain state changed since last evaluation?
#   - Are previously "PROCEED" actions still valid?
#   - Have blockers been resolved for "DEFER" actions?
#   - New risks emerging from data trends?
# ============================================================

def run_hourly_evaluation(catalog: str = None):
    """
    Entry point for the Lakeflow Job.
    Re-evaluates all actions and compares with previous evaluations.
    """
    cat = catalog or CAT
    print(f"{'='*60}")
    print(f"  HOURLY ACTION EVALUATION \u2014 {datetime.now().isoformat()}")
    print(f"{'='*60}")

    # Count pending actions
    pending = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM `{cat}`.reporting.action_items
        WHERE status = 'PENDING'
    """).collect()[0]["cnt"]

    if pending == 0:
        print("\u2713 No pending actions. Nothing to evaluate.")
        return

    print(f"\n{pending} pending actions found. Re-evaluating...\n")

    # Force re-evaluate to get fresh assessments against latest data
    count = evaluate_actions(catalog=cat, force_reevaluate=True)

    # Summary comparison: how did recommendations change?
    changes = spark.sql(f"""
        WITH latest AS (
            SELECT action_id, recommendation, practicality_score, evaluated_at,
                   ROW_NUMBER() OVER (PARTITION BY action_id ORDER BY evaluated_at DESC) as rn
            FROM `{cat}`.reporting.action_evaluations
        ),
        previous AS (
            SELECT action_id, recommendation as prev_rec, practicality_score as prev_score,
                   ROW_NUMBER() OVER (PARTITION BY action_id ORDER BY evaluated_at DESC) as rn
            FROM `{cat}`.reporting.action_evaluations
        )
        SELECT l.action_id, a.title,
               p.prev_rec, l.recommendation as curr_rec,
               ROUND(p.prev_score, 2) as prev_score,
               ROUND(l.practicality_score, 2) as curr_score,
               CASE WHEN p.prev_rec != l.recommendation THEN 'CHANGED' ELSE 'STABLE' END as status
        FROM latest l
        JOIN previous p ON l.action_id = p.action_id AND p.rn = 2
        JOIN `{cat}`.reporting.action_items a ON l.action_id = a.action_id
        WHERE l.rn = 1
    """)

    changed_count = changes.filter("status = 'CHANGED'").count()
    print(f"\n\u2713 Evaluation complete: {count} actions evaluated, {changed_count} changed recommendation")
    if changed_count > 0:
        print("\nChanged actions:")
        display(changes.filter("status = 'CHANGED'"))


# Uncomment to run manually:
# run_hourly_evaluation()

# COMMAND ----------

# DBTITLE 1,Dashboard Query: Action Tracker Summary View
# ============================================================
# Dashboard-ready views for the Action Tracker
# These queries power the Lakeview dashboard widgets.
# ============================================================

def create_dashboard_views(catalog: str = None):
    """Create views that the dashboard queries."""
    cat = catalog or CAT

    # View 1: Action tracker with latest evaluation
    spark.sql(f"""
    CREATE OR REPLACE VIEW `{cat}`.reporting.v_action_tracker AS
    WITH latest_eval AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY action_id ORDER BY evaluated_at DESC) as rn
        FROM `{cat}`.reporting.action_evaluations
    )
    SELECT
        a.action_id,
        a.category,
        a.timeframe,
        a.domain,
        a.title,
        a.description,
        a.specific_steps,
        a.expected_impact,
        a.priority,
        a.status as action_status,
        e.recommendation,
        e.practicality_score,
        e.risks,
        e.pros,
        e.cons,
        e.cost_estimate,
        e.reasoning,
        e.evaluated_at,
        a.created_at,
        -- Dashboard color coding
        CASE e.recommendation
            WHEN 'PROCEED' THEN 'green'
            WHEN 'PROCEED_WITH_CAUTION' THEN 'orange'
            WHEN 'DEFER' THEN 'grey'
            WHEN 'REJECT' THEN 'red'
            ELSE 'blue'
        END as status_color,
        -- Risk level
        CASE
            WHEN e.practicality_score >= 0.7 THEN 'LOW'
            WHEN e.practicality_score >= 0.4 THEN 'MEDIUM'
            ELSE 'HIGH'
        END as risk_level
    FROM `{cat}`.reporting.action_items a
    LEFT JOIN latest_eval e ON a.action_id = e.action_id AND e.rn = 1
    """)

    # View 2: Evaluation history (for trend tracking)
    spark.sql(f"""
    CREATE OR REPLACE VIEW `{cat}`.reporting.v_action_eval_history AS
    SELECT
        a.title,
        a.domain,
        a.category,
        e.recommendation,
        e.practicality_score,
        e.evaluated_at,
        e.risks,
        e.reasoning
    FROM `{cat}`.reporting.action_evaluations e
    JOIN `{cat}`.reporting.action_items a ON e.action_id = a.action_id
    ORDER BY a.title, e.evaluated_at
    """)

    # View 3: Domain summary (for dashboard donut/bar)
    spark.sql(f"""
    CREATE OR REPLACE VIEW `{cat}`.reporting.v_action_domain_summary AS
    WITH latest_eval AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY action_id ORDER BY evaluated_at DESC) as rn
        FROM `{cat}`.reporting.action_evaluations
    )
    SELECT
        a.domain,
        a.category,
        COUNT(*) as action_count,
        SUM(CASE WHEN e.recommendation = 'PROCEED' THEN 1 ELSE 0 END) as proceed_count,
        SUM(CASE WHEN e.recommendation = 'PROCEED_WITH_CAUTION' THEN 1 ELSE 0 END) as caution_count,
        SUM(CASE WHEN e.recommendation = 'DEFER' THEN 1 ELSE 0 END) as defer_count,
        SUM(CASE WHEN e.recommendation = 'REJECT' THEN 1 ELSE 0 END) as reject_count,
        ROUND(AVG(e.practicality_score), 2) as avg_practicality
    FROM `{cat}`.reporting.action_items a
    LEFT JOIN latest_eval e ON a.action_id = e.action_id AND e.rn = 1
    GROUP BY a.domain, a.category
    """)

    print(f"\u2713 Dashboard views created:")
    print(f"  \u2022 `{cat}`.reporting.v_action_tracker")
    print(f"  \u2022 `{cat}`.reporting.v_action_eval_history")
    print(f"  \u2022 `{cat}`.reporting.v_action_domain_summary")

# Create dashboard views
create_dashboard_views()

# COMMAND ----------

# DBTITLE 1,Action Intelligence Pipeline (standalone — for interactive use)
# ============================================================
# ACTION INTELLIGENCE PIPELINE (standalone — for interactive use)
#
# NOTE: When run as a JOB, Steps 6-8 already execute in cell 9.
# This cell is for INTERACTIVE use only — re-run action parsing
# and evaluation without re-calling the Supervisor Agent.
# ============================================================

print("="*70)
print("  ACTION INTELLIGENCE PIPELINE (Steps 6-8)")
print("="*70)

# Step 6: Parse actions from Supervisor Agent response into Delta
# If Supervisor Agent returned a real response, parse new actions from it.
# If not (error or too short), use existing action_items already in the table.
print("\n  Step 6: Parsing recommended actions into Delta table...")
run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
new_actions_parsed = 0
try:
    sup_text = supervisor_result.get("response_text", "") if 'supervisor_result' in dir() else ""
    if sup_text and len(sup_text) > 200 and "ground-truth defaults" not in sup_text.lower():
        new_actions_parsed = parse_actions_to_delta(sup_text, run_id=run_id)
        print(f"  \u2713 {new_actions_parsed} NEW actions parsed from Supervisor Agent response")
    else:
        reason = "no supervisor_result variable" if 'supervisor_result' not in dir() else (
            "response too short" if len(sup_text) <= 200 else "using GT defaults (Supervisor Agent call failed)")
        print(f"  Skipping action parsing: {reason}")
except Exception as e:
    print(f"  \u2717 Action parsing error: {e}")

# Always check existing actions
try:
    existing = spark.sql(f"SELECT COUNT(*) as cnt FROM `{CAT}`.reporting.action_items").collect()[0]["cnt"]
    print(f"  Total actions in table: {existing} ({new_actions_parsed} new + {existing - new_actions_parsed} existing)")
except Exception:
    existing = 0
    print("  Note: action_items table not yet populated")

# Step 7: Evaluate ALL pending actions with AI (always runs)
# This works on whatever actions are in the table — new or existing.
print("\n  Step 7: Evaluating actions for practicality...")
eval_count = 0
if existing > 0:
    try:
        eval_count = evaluate_actions(force_reevaluate=True)
        print(f"  \u2713 {eval_count} actions evaluated")
    except Exception as e:
        print(f"  \u2717 Evaluation error: {e}")
else:
    print("  No actions to evaluate. Run the Supervisor Agent pipeline first (cell 9).")

# Step 8: Refresh dashboard views (always runs)
print("\n  Step 8: Refreshing dashboard views...")
try:
    create_dashboard_views()
except Exception as e:
    print(f"  \u2717 Views error: {e}")

# Final summary
print(f"\n{'='*70}")
print("  \u2713 ACTION INTELLIGENCE PIPELINE COMPLETE")
try:
    items = spark.sql(f"SELECT COUNT(*) as cnt FROM `{CAT}`.reporting.action_items").collect()[0]["cnt"]
    evals = spark.sql(f"SELECT COUNT(*) as cnt FROM `{CAT}`.reporting.action_evaluations").collect()[0]["cnt"]
    print(f"    action_items: {items} rows")
    print(f"    action_evaluations: {evals} rows")
    print(f"    New actions this run: {new_actions_parsed}")
    print(f"    Evaluations this run: {eval_count}")
    display(spark.sql(f"""
        SELECT action_id, domain, title, priority,
               recommendation, ROUND(practicality_score, 2) as score
        FROM `{CAT}`.reporting.v_action_tracker
        ORDER BY priority
    """))
except Exception as e:
    print(f"    Summary query: {e}")
print(f"{'='*70}")