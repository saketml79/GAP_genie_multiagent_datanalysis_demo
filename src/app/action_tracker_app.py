"""Action Intelligence Tracker — Streamlit App

Reads from GAP_Demo_Dev.reporting.action_items, action_evaluations,
and action_intelligence.action_history Delta tables.

Displays:
  - Executive KPI header with action counts by recommendation
  - Interactive action cards with PROCEED/DEFER/REJECT badges
  - Practicality score distribution (plotly)
  - Domain breakdown chart
  - Evaluation history trends
  - Historical precedent explorer
  - Risk/Pros/Cons drill-down

Run: databricks apps deploy (or locally: streamlit run action_tracker_app.py)
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from databricks.sdk import WorkspaceClient
from databricks import sql as dbsql
import pandas as pd
import os
from datetime import datetime

# ── Page Config ──
st.set_page_config(
    page_title="Action Intelligence Tracker",
    page_icon="\U0001f3af",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Connection ──
CATALOG = os.getenv("CATALOG", "GAP_Demo_Dev")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "bf50738cf2819197")

@st.cache_resource
def get_connection():
    """Get Databricks SQL connection."""
    w = WorkspaceClient()
    return dbsql.connect(
        server_hostname=w.config.host.replace("https://", ""),
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: w.config._header_factory(),
    )

def run_query(query: str) -> pd.DataFrame:
    """Execute SQL and return a pandas DataFrame."""
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=cols)


# ── Custom CSS ──
st.markdown("""
<style>
    .action-card {
        border: 1px solid #e0e0e0;
        border-radius: 12px;
        padding: 20px;
        margin: 10px 0;
        background: white;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }
    .badge-proceed {
        background: #4CAF50; color: white; padding: 4px 12px;
        border-radius: 20px; font-weight: bold; font-size: 13px;
    }
    .badge-caution {
        background: #FF9800; color: white; padding: 4px 12px;
        border-radius: 20px; font-weight: bold; font-size: 13px;
    }
    .badge-defer {
        background: #9E9E9E; color: white; padding: 4px 12px;
        border-radius: 20px; font-weight: bold; font-size: 13px;
    }
    .badge-reject {
        background: #F44336; color: white; padding: 4px 12px;
        border-radius: 20px; font-weight: bold; font-size: 13px;
    }
    .kpi-big { font-size: 36px; font-weight: bold; color: #1a237e; }
    .kpi-label { font-size: 13px; color: #666; }
    .risk-high { color: #F44336; font-weight: bold; }
    .risk-medium { color: #FF9800; font-weight: bold; }
    .risk-low { color: #4CAF50; font-weight: bold; }
</style>
""", unsafe_allow_html=True)


# ── Sidebar ──
st.sidebar.title("\U0001f3af Action Intelligence")
st.sidebar.markdown("---")

# Filters
st.sidebar.subheader("Filters")
domain_filter = st.sidebar.multiselect(
    "Domain", ["inventory", "logistics", "supplier", "demand", "finance"],
    default=["inventory", "logistics", "supplier", "demand", "finance"]
)
category_filter = st.sidebar.multiselect(
    "Timeframe", ["IMMEDIATE", "SHORT_TERM", "STRATEGIC"],
    default=["IMMEDIATE", "SHORT_TERM", "STRATEGIC"]
)
rec_filter = st.sidebar.multiselect(
    "Recommendation", ["PROCEED", "PROCEED_WITH_CAUTION", "DEFER", "REJECT"],
    default=["PROCEED", "PROCEED_WITH_CAUTION", "DEFER", "REJECT"]
)

st.sidebar.markdown("---")
st.sidebar.markdown("**Data Source**")
st.sidebar.code(f"{CATALOG}.reporting.action_items", language=None)
st.sidebar.code(f"{CATALOG}.action_intelligence.action_history", language=None)
st.sidebar.markdown(f"*Last refreshed: {datetime.now().strftime('%H:%M:%S')}*")
if st.sidebar.button("\U0001f504 Refresh Data"):
    st.cache_data.clear()
    st.rerun()


# ── Load Data ──
@st.cache_data(ttl=60)
def load_action_tracker():
    """Load action tracker view."""
    try:
        return run_query(f"""
            WITH latest_eval AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY action_id ORDER BY evaluated_at DESC) as rn
                FROM `{CATALOG}`.reporting.action_evaluations
            )
            SELECT a.action_id, a.category, a.timeframe, a.domain, a.title, a.description,
                   a.specific_steps, a.expected_impact, a.priority, a.status as action_status,
                   e.recommendation, e.practicality_score, e.risks, e.pros, e.cons,
                   e.cost_estimate, e.reasoning, e.evaluated_at, a.created_at,
                   e.historical_precedent
            FROM `{CATALOG}`.reporting.action_items a
            LEFT JOIN latest_eval e ON a.action_id = e.action_id AND e.rn = 1
            ORDER BY a.priority
        """)
    except Exception as e:
        st.warning(f"Action tracker not populated yet. Run the Report Agent notebook first. ({e})")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_history():
    """Load action history."""
    try:
        return run_query(f"""
            SELECT * FROM `{CATALOG}`.action_intelligence.action_history
            ORDER BY action_date DESC
        """)
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_eval_history():
    """Load evaluation history for trend charts."""
    try:
        return run_query(f"""
            SELECT a.title, a.domain, a.category, e.recommendation,
                   e.practicality_score, e.evaluated_at
            FROM `{CATALOG}`.reporting.action_evaluations e
            JOIN `{CATALOG}`.reporting.action_items a ON e.action_id = a.action_id
            ORDER BY e.evaluated_at
        """)
    except Exception as e:
        return pd.DataFrame()


df_actions = load_action_tracker()
df_history = load_history()
df_eval_hist = load_eval_history()


# ── Header ──
st.title("\U0001f3af Action Intelligence Tracker")
st.markdown("AI-powered evaluation of recommended supply chain actions — practicality assessed against live data and historical precedent.")
st.markdown("---")

if df_actions.empty:
    st.info(
        "\U0001f4cb **No actions found.** Run the Report Agent notebook "
        "(`report_agent_functions`) to populate the action tables, then refresh."
    )
    st.stop()


# ── Apply Filters ──
df = df_actions.copy()
if domain_filter:
    df = df[df["domain"].isin(domain_filter)]
if category_filter:
    df = df[df["category"].isin(category_filter)]
if rec_filter and "recommendation" in df.columns:
    df = df[df["recommendation"].isin(rec_filter)]


# ── KPI Header ──
col1, col2, col3, col4, col5 = st.columns(5)

total = len(df)
proceed = len(df[df["recommendation"] == "PROCEED"]) if "recommendation" in df.columns else 0
caution = len(df[df["recommendation"] == "PROCEED_WITH_CAUTION"]) if "recommendation" in df.columns else 0
defer = len(df[df["recommendation"] == "DEFER"]) if "recommendation" in df.columns else 0
reject = len(df[df["recommendation"] == "REJECT"]) if "recommendation" in df.columns else 0

col1.metric("Total Actions", total)
col2.metric("\u2705 Proceed", proceed)
col3.metric("\u26a0\ufe0f Caution", caution)
col4.metric("\u23f8\ufe0f Defer", defer)
col5.metric("\u274c Reject", reject)

st.markdown("---")


# ── Tab Layout ──
tab1, tab2, tab3, tab4 = st.tabs([
    "\U0001f4cb Action Cards",
    "\U0001f4ca Analytics",
    "\U0001f4c5 History & Precedent",
    "\U0001f50d Detail Explorer"
])


# ── Tab 1: Action Cards ──
with tab1:
    st.subheader("Recommended Actions")

    for timeframe in ["IMMEDIATE", "SHORT_TERM", "STRATEGIC"]:
        label_map = {"IMMEDIATE": "\U0001f534 Immediate (Next 7 Days)",
                     "SHORT_TERM": "\U0001f7e0 Short-Term (Next 30 Days)",
                     "STRATEGIC": "\U0001f535 Strategic (Next 90 Days)"}
        group = df[df["category"] == timeframe]
        if group.empty:
            continue

        st.markdown(f"### {label_map.get(timeframe, timeframe)}")

        for _, row in group.iterrows():
            rec = row.get("recommendation", "PENDING")
            score = row.get("practicality_score", 0) or 0
            badge_class = {
                "PROCEED": "badge-proceed",
                "PROCEED_WITH_CAUTION": "badge-caution",
                "DEFER": "badge-defer",
                "REJECT": "badge-reject"
            }.get(rec, "badge-defer")

            score_color = "#4CAF50" if score >= 0.7 else "#FF9800" if score >= 0.4 else "#F44336"

            with st.container():
                left, right = st.columns([4, 1])
                with left:
                    st.markdown(f"""
                    <div class="action-card">
                        <div style="display:flex; justify-content:space-between; align-items:center;">
                            <h4 style="margin:0;">{row['title']}</h4>
                            <span class="{badge_class}">{rec}</span>
                        </div>
                        <p style="color:#666; margin:5px 0;"><b>Domain:</b> {row.get('domain','')} &nbsp;|&nbsp;
                        <b>Priority:</b> {row.get('priority','-')} &nbsp;|&nbsp;
                        <b>Score:</b> <span style="color:{score_color}; font-weight:bold;">{score:.0%}</span></p>
                        <p>{row.get('description','')[:300]}</p>
                    </div>
                    """, unsafe_allow_html=True)

                with right:
                    with st.expander("Details"):
                        st.markdown(f"**Expected Impact:** {row.get('expected_impact', 'N/A')}")
                        st.markdown(f"**Steps:** {row.get('specific_steps', 'N/A')}")
                        st.markdown(f"**Cost Estimate:** {row.get('cost_estimate', 'N/A')}")
                        if row.get("risks"):
                            st.markdown(f"\U0001f6a8 **Risks:** {row['risks']}")
                        if row.get("pros"):
                            st.markdown(f"\u2705 **Pros:** {row['pros']}")
                        if row.get("cons"):
                            st.markdown(f"\u26a0\ufe0f **Cons:** {row['cons']}")
                        if row.get("reasoning"):
                            st.markdown(f"\U0001f4a1 **AI Reasoning:** {row['reasoning']}")


# ── Tab 2: Analytics ──
with tab2:
    st.subheader("Action Analytics")

    c1, c2 = st.columns(2)

    with c1:
        # Recommendation distribution
        if "recommendation" in df.columns and not df["recommendation"].isna().all():
            rec_counts = df["recommendation"].value_counts().reset_index()
            rec_counts.columns = ["Recommendation", "Count"]
            color_map = {"PROCEED": "#4CAF50", "PROCEED_WITH_CAUTION": "#FF9800",
                         "DEFER": "#9E9E9E", "REJECT": "#F44336"}
            fig = px.pie(rec_counts, names="Recommendation", values="Count",
                         title="Recommendation Distribution",
                         color="Recommendation", color_discrete_map=color_map,
                         hole=0.4)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    with c2:
        # Domain breakdown
        if "domain" in df.columns:
            domain_df = df.groupby(["domain", "recommendation"]).size().reset_index(name="count")
            fig = px.bar(domain_df, x="domain", y="count", color="recommendation",
                         title="Actions by Domain & Recommendation",
                         color_discrete_map=color_map,
                         barmode="stack")
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    # Practicality score distribution
    if "practicality_score" in df.columns and not df["practicality_score"].isna().all():
        fig = px.histogram(df, x="practicality_score", nbins=20,
                           title="Practicality Score Distribution",
                           color_discrete_sequence=["#2196F3"])
        fig.add_vline(x=0.7, line_dash="dash", line_color="green",
                      annotation_text="High Feasibility")
        fig.add_vline(x=0.4, line_dash="dash", line_color="orange",
                      annotation_text="Medium")
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    # Score by category
    if "practicality_score" in df.columns and not df["practicality_score"].isna().all():
        fig = px.box(df, x="category", y="practicality_score", color="category",
                     title="Practicality Score by Timeframe",
                     color_discrete_map={"IMMEDIATE": "#F44336",
                                         "SHORT_TERM": "#FF9800",
                                         "STRATEGIC": "#2196F3"})
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)


# ── Tab 3: History & Precedent ──
with tab3:
    st.subheader("Historical Action Outcomes")
    st.markdown("Past actions and their measured results — used by the AI evaluator as precedent.")

    if df_history.empty:
        st.info("No historical data. Run `create_action_history()` in the notebook.")
    else:
        # Success rate by domain
        c1, c2 = st.columns(2)
        with c1:
            success_df = df_history.groupby(["domain", "success_rating"]).size().reset_index(name="count")
            fig = px.bar(success_df, x="domain", y="count", color="success_rating",
                         title="Historical Outcomes by Domain",
                         color_discrete_map={"SUCCESS": "#4CAF50", "PARTIAL": "#FF9800", "FAILED": "#F44336"},
                         barmode="stack")
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            # Cost vs impact scatter
            if "cost_usd" in df_history.columns and "metric_after" in df_history.columns:
                hist = df_history.copy()
                hist["improvement"] = hist["metric_after"].astype(float) - hist["metric_before"].astype(float)
                fig = px.scatter(hist, x="cost_usd", y="improvement",
                                 color="success_rating", size="cost_usd",
                                 hover_name="title", title="Cost vs Metric Improvement",
                                 color_discrete_map={"SUCCESS": "#4CAF50", "PARTIAL": "#FF9800", "FAILED": "#F44336"})
                fig.update_xaxes(title="Cost ($)")
                fig.update_yaxes(title="Metric Change (pp or units)")
                st.plotly_chart(fig, use_container_width=True)

        # Detailed history table
        st.markdown("### Action History Detail")
        for _, h in df_history.iterrows():
            with st.expander(f"{h.get('action_date', '')} — {h['title']} ({h.get('success_rating', '')})" ):
                col1, col2, col3 = st.columns(3)
                col1.metric("Cost", f"${h.get('cost_usd', 0):,.0f}")
                col2.metric(h.get("metric_name", "Metric"),
                            f"{h.get('metric_after', 0)}",
                            f"{(h.get('metric_after', 0) or 0) - (h.get('metric_before', 0) or 0):+.1f}")
                col3.metric("Duration", f"{h.get('duration_days', 0)} days")
                st.markdown(f"**Outcome:** {h.get('outcome', 'N/A')}")
                st.markdown(f"**Lessons Learned:** {h.get('lessons_learned', 'N/A')}")

    # Evaluation trend (if hourly data exists)
    if not df_eval_hist.empty:
        st.markdown("### Evaluation Score Trends")
        fig = px.line(df_eval_hist, x="evaluated_at", y="practicality_score",
                      color="title", title="Practicality Score Over Time",
                      markers=True)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)


# ── Tab 4: Detail Explorer ──
with tab4:
    st.subheader("Action Detail Explorer")

    if not df.empty:
        selected = st.selectbox("Select an action:",
                                options=df["title"].tolist(),
                                index=0)
        row = df[df["title"] == selected].iloc[0]

        c1, c2, c3 = st.columns(3)
        c1.metric("Domain", row.get("domain", "N/A"))
        c2.metric("Priority", row.get("priority", "N/A"))
        score = row.get("practicality_score", 0) or 0
        c3.metric("Practicality", f"{score:.0%}")

        st.markdown("---")

        # Gauge chart
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=score * 100,
            title={"text": "Practicality Score"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "#2196F3"},
                "steps": [
                    {"range": [0, 40], "color": "#FFCDD2"},
                    {"range": [40, 70], "color": "#FFE0B2"},
                    {"range": [70, 100], "color": "#C8E6C9"}
                ],
                "threshold": {
                    "line": {"color": "red", "width": 4},
                    "thickness": 0.75,
                    "value": 70
                }
            }
        ))
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### \u2705 Pros")
            st.markdown(row.get("pros", "N/A"))
            st.markdown("### \u26a0\ufe0f Risks")
            st.markdown(row.get("risks", "N/A"))
        with col2:
            st.markdown("### \u274c Cons")
            st.markdown(row.get("cons", "N/A"))
            st.markdown("### \U0001f4b0 Cost Estimate")
            st.markdown(row.get("cost_estimate", "N/A"))

        st.markdown("### \U0001f4a1 AI Reasoning")
        st.info(row.get("reasoning", "No reasoning available."))

        st.markdown("### \U0001f4dc Full Description")
        st.markdown(row.get("description", "N/A"))
        st.markdown(f"**Specific Steps:** {row.get('specific_steps', 'N/A')}")
        st.markdown(f"**Expected Impact:** {row.get('expected_impact', 'N/A')}")


# ── Footer ──
st.markdown("---")
st.caption(
    f"Action Intelligence Tracker | Catalog: {CATALOG} | "
    f"Powered by ai_query() + Delta + Databricks Apps | "
    f"Actions are evaluated against live supply chain data and 12 historical precedents."
)
