"""Action Intelligence Tracker v6 — GAP Supply Chain Control Tower"""
from __future__ import annotations
import json as _json, os, time as _time
from datetime import datetime
from urllib import request as urlreq, parse as urlparse
import html as _html
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Action Intelligence | GAP Supply Chain",
    page_icon="\U0001f3ed",
    layout="wide",
    initial_sidebar_state="expanded",
)

APP_VERSION = "v6-interactions"
CATALOG = os.getenv("CATALOG", "GAP_Demo_Dev")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "bf50738cf2819197")
HTTP_PATH = f"/sql/1.0/warehouses/{WAREHOUSE_ID}"
SERVER_HOSTNAME = os.getenv(
    "DATABRICKS_HOST",
    "adb-1866518241053589.9.azuredatabricks.net",
).replace("https://", "").replace("http://", "").strip().rstrip("/")


_LOADED_AT = datetime.now().strftime("%b %d %Y, %H:%M")
emdash = "\u2014"  # em-dash — extracted for use inside f-string expressions

# ═══════════════════════════════════════════════════════════════
#  CSS
# ═══════════════════════════════════════════════════════════════
_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Sora:wght@500;600;700;800&display=swap');
:root{
    --bg-0:#f5f9fc;--bg-1:#edf4f8;--bg-2:#e4eef4;
    --surface:#ffffff;--ink:#0c1a27;--ink2:#0f1d2d;--muted:#374151;
    --line:#c4d4de;--brand:#005f73;--brand-dark:#024557;--brand-soft:#daeef3;
    --good:#166534;--good-bg:#dcfce7;--good-bd:#86efac;
    --bad:#991b1b;--bad-bg:#fee2e2;--bad-bd:#fca5a5;
    --warn:#92400e;--warn-bg:#fef3c7;--warn-bd:#fcd34d;
    --def-fg:#374151;--def-bg:#f3f4f6;--def-bd:#d1d5db;
}
html,body,[class*="css"]{font-family:"Plus Jakarta Sans",sans-serif;color:var(--ink);font-size:15px;}
.stApp{background:linear-gradient(175deg,var(--bg-0) 0%,var(--bg-1) 55%,var(--bg-2) 100%);}

/* ── SIDEBAR — dark navy ── */
[data-testid="stSidebar"]{
    background:linear-gradient(180deg,#00111a 0%,#001f2d 60%,#002637 100%);
    border-right:1px solid rgba(0,95,115,.25);
}
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] div{color:#c8dde8 !important;}
[data-testid="stSidebar"] [data-testid="stMetricValue"]{color:#7dd3e8 !important;font-size:1.3rem !important;}
[data-testid="stSidebar"] [data-testid="stMetricLabel"] p{color:#5590a8 !important;}
[data-testid="stSidebar"] .stCaption p{color:#5a8fa8 !important;}
[data-testid="stSidebar"] hr{border-color:rgba(255,255,255,.1);margin:.5rem 0;}
[data-testid="stSidebar"] [data-testid="stButton"] button{
    background:rgba(0,95,115,.22) !important;border:1px solid rgba(0,95,115,.45) !important;
    color:#a8d8e8 !important;border-radius:9px !important;
}
[data-testid="stSidebar"] [data-testid="stButton"] button:hover{
    background:rgba(0,95,115,.42) !important;color:#fff !important;
}

/* ── TABS — clear active vs inactive ── */
.stTabs [data-baseweb="tab-list"]{
    background:var(--surface);border:1.5px solid var(--line);
    border-radius:12px;padding:4px 5px;gap:4px;
    box-shadow:0 2px 8px rgba(0,0,0,.05);
}
.stTabs [data-baseweb="tab"]{
    border-radius:9px;font-family:"Plus Jakarta Sans",sans-serif;
    font-weight:700;font-size:.82rem;letter-spacing:.01em;
    color:#0c1a27 !important;background:#f0f4f8 !important;
    padding:.42rem 1rem;transition:all .15s ease;
    border:1px solid #c4d4de;
}
.stTabs [data-baseweb="tab"]:hover{background:var(--bg-1) !important;color:var(--brand) !important;}
.stTabs [data-baseweb="tab"][aria-selected="true"]{
    background:var(--brand) !important;color:#ffffff !important;
    box-shadow:0 3px 10px rgba(0,95,115,.3);
}
.stTabs [data-baseweb="tab"][aria-selected="true"] *,
.stTabs [data-baseweb="tab"][aria-selected="true"] p,
.stTabs [data-baseweb="tab"][aria-selected="true"] span,
.stTabs [data-baseweb="tab"][aria-selected="true"] div{color:#ffffff !important;}
.stTabs [data-baseweb="tab-highlight"]{display:none !important;}
.stTabs [data-baseweb="tab-border"]{display:none !important;}
.stTabs [data-baseweb="tab-panel"]{background:var(--surface);border:1px solid var(--line);border-top:none;border-radius:0 0 12px 12px;padding:.8rem .6rem;}

/* ── ACTION CARDS — shadow, no hard borders ── */
.act-card{
    border:none;border-radius:14px;
    background:var(--surface);margin-bottom:.7rem;
    box-shadow:0 4px 18px rgba(0,31,45,.09), 0 1.5px 4px rgba(0,0,0,.04);
    overflow:hidden;transition:box-shadow .15s ease, transform .15s ease;
}
.act-card:hover{box-shadow:0 8px 28px rgba(0,31,45,.13);transform:translateY(-1px);}
.act-card.rec-proceed{border-left:4px solid #166534;}
.act-card.rec-caution{border-left:4px solid #92400e;}
.act-card.rec-defer{border-left:4px solid #991b1b;}
.act-card.rec-pending{border-left:4px solid #0369a1;}
.act-card.decided{opacity:.85;}
.card-top{padding:.9rem 1.1rem .5rem;}

/* ── BADGES ── */
.badge{padding:3px 10px;border-radius:20px;font-weight:800;font-size:.62rem;letter-spacing:.04em;text-transform:uppercase;display:inline-block;}
.b-proceed{background:var(--good-bg);color:var(--good);border:1px solid var(--good-bd);}
.b-caution{background:var(--warn-bg);color:var(--warn);border:1px solid var(--warn-bd);}
.b-reject{background:var(--bad-bg);color:var(--bad);border:1px solid var(--bad-bd);}
.b-defer{background:var(--def-bg);color:var(--def-fg);border:1px solid var(--def-bd);}
.b-pending{background:#f0f9ff;color:#0369a1;border:1px solid #bae6fd;}
.dtag{display:inline-block;padding:2px 9px;border-radius:8px;font-size:.62rem;font-weight:800;letter-spacing:.04em;text-transform:uppercase;background:var(--brand-soft);color:var(--brand);}
.sp-approved{background:var(--good-bg);color:var(--good);border:1px solid var(--good-bd);padding:3px 10px;border-radius:20px;font-weight:800;font-size:.62rem;letter-spacing:.04em;text-transform:uppercase;display:inline-block;}
.sp-rejected{background:var(--bad-bg);color:var(--bad);border:1px solid var(--bad-bd);padding:3px 10px;border-radius:20px;font-weight:800;font-size:.62rem;letter-spacing:.04em;text-transform:uppercase;display:inline-block;}
.sp-deferred{background:var(--def-bg);color:var(--def-fg);border:1px solid var(--def-bd);padding:3px 10px;border-radius:20px;font-weight:800;font-size:.62rem;letter-spacing:.04em;text-transform:uppercase;display:inline-block;}

/* ── KPI ROW ── */
.kpi-row{display:grid;grid-template-columns:repeat(6,1fr);gap:.55rem;margin:.7rem 0 1.1rem;}
.kpi{background:var(--surface);border:none;border-radius:12px;padding:.72rem .8rem;text-align:center;box-shadow:0 3px 12px rgba(0,31,45,.07);}
.kpi-val{font-size:1.7rem;font-weight:800;font-family:"Sora",sans-serif;color:var(--brand);}
.kpi-label{font-size:.7rem;color:#1a2b3d;font-weight:700;margin-top:.08rem;text-transform:uppercase;letter-spacing:.04em;}

/* ── BUTTONS ── */
[data-testid="stButton"] button{
    border-radius:9px;font-family:"Plus Jakarta Sans",sans-serif;
    font-weight:700;font-size:.8rem;transition:all .14s ease;
    border:1.5px solid var(--line) !important;
    background:linear-gradient(180deg,#fff 0%,#f4f8fb 100%) !important;
    color:var(--ink) !important;
}
[data-testid="stButton"] button:hover{
    transform:translateY(-1px);box-shadow:0 4px 14px rgba(0,0,0,.09);
    border-color:var(--brand) !important;
}
button[kind="primary"],button[data-testid="baseButton-primary"]{
    background:var(--brand) !important;border-color:var(--brand) !important;color:#fff !important;
}
button[kind="primary"]:hover,button[data-testid="baseButton-primary"]:hover{
    background:var(--brand-dark) !important;
}
button[kind="primary"] p,button[data-testid="baseButton-primary"] p{color:#fff !important;}

/* ── HERO ── */
.hero{padding:1.1rem 1.4rem;border-radius:16px;background:linear-gradient(115deg,#fff 0%,#f0f7fb 100%);border:none;box-shadow:0 6px 24px rgba(0,31,45,.08);margin-bottom:.85rem;}
.hero-title{margin:0;font-family:"Sora",sans-serif;font-size:1.75rem;font-weight:800;letter-spacing:-.03em;color:var(--ink);}
.hero-sub{margin-top:.25rem;color:#1a2b3d;font-size:.92rem;font-weight:500;line-height:1.5;}

/* ── MISC ── */
p,li,div{color:var(--ink);}
h1,h2,h3,h4{font-family:"Sora",sans-serif;color:var(--ink) !important;}
.section-kicker{color:var(--brand);font-size:.68rem;font-weight:800;letter-spacing:.08em;text-transform:uppercase;margin-bottom:.2rem;}
.detail-section{background:var(--surface);border:none;border-radius:12px;padding:.9rem 1.1rem;margin-bottom:.5rem;box-shadow:0 3px 12px rgba(0,31,45,.07);}
.detail-section h4{margin:0 0 .3rem;font-size:.9rem;color:var(--ink);}
.detail-section p{color:var(--ink2);font-size:.88rem;line-height:1.5;}
.hist-card{border:none;border-radius:12px;padding:.8rem .95rem;background:var(--surface);margin-bottom:.6rem;box-shadow:0 4px 14px rgba(0,31,45,.08);transition:box-shadow .15s ease;}
.hist-card:hover{box-shadow:0 6px 22px rgba(0,31,45,.12);}
.hist-card .mrow{display:flex;gap:1rem;font-size:.77rem;margin-top:.3rem;color:var(--ink);flex-wrap:wrap;}
.hist-card .mrow strong{color:var(--ink);font-weight:700;}
[data-testid="stMetricValue"]{color:var(--ink) !important;font-weight:800 !important;}
[data-testid="stMetricLabel"] p{color:var(--muted) !important;font-weight:600 !important;}
[data-baseweb="input"] input,textarea{background:#fff !important;color:var(--ink) !important;border:1px solid var(--line) !important;border-radius:9px !important;}
[data-baseweb="input"] input:focus,textarea:focus{border-color:var(--brand) !important;box-shadow:0 0 0 2px rgba(0,95,115,.15) !important;}
[data-baseweb="select"] > div,[data-baseweb="select"] [role="combobox"]{background:#fff !important;border-color:var(--line) !important;color:var(--ink) !important;}
[data-baseweb="select"] *{color:var(--ink) !important;}
[data-baseweb="select"] input{caret-color:transparent !important;}
[data-baseweb="popover"],[data-baseweb="menu"]{background:#fff !important;}
[role="option"]{background:#fff !important;color:var(--ink) !important;}
[role="option"]:hover,[role="option"][aria-selected="true"]{background:var(--brand-soft) !important;}
[data-baseweb="tag"]{background:var(--brand-soft) !important;color:var(--brand) !important;border:1px solid rgba(0,95,115,.2) !important;}
</style>
"""


# ═══════════════════════════════════════════════════════════════
#  AUTH — exact insurance-app pattern (proven v5)
# ═══════════════════════════════════════════════════════════════
def _oauth_bearer_token() -> tuple[str, str]:
    """Get an access token. Mirrors the insurance app's proven pattern exactly."""
    # 1. Static token (local dev)
    static_token = os.getenv("DATABRICKS_TOKEN", "").strip()
    if static_token:
        return static_token, "ENV"

    # 2. OAuth client_credentials (SP identity in app container)
    client_id = os.getenv("DATABRICKS_CLIENT_ID", "").strip()
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET", "").strip()
    host = os.getenv("DATABRICKS_HOST", "").strip().rstrip("/")
    if client_id and client_secret and host:
        if not host.startswith("http"):
            host = f"https://{host}"
        form = urlparse.urlencode({
            "grant_type": "client_credentials",
            "scope": "all-apis",
            "client_id": client_id,
            "client_secret": client_secret,
        }).encode("utf-8")
        req = urlreq.Request(
            f"{host}/oidc/v1/token",
            data=form, method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
        )
        with urlreq.urlopen(req, timeout=20) as resp:
            payload = _json.loads(resp.read().decode("utf-8") or "{}")
        access_token = str(payload.get("access_token", "")).strip()
        if access_token:
            return access_token, "OAuth"

    # 3. OBO forwarded token (fallback, may lack sql scope)
    try:
        hdrs = st.context.headers
        tok = hdrs.get("X-Forwarded-Access-Token", "") or hdrs.get("x-forwarded-access-token", "")
        if tok:
            return tok, "OBO"
    except Exception:
        pass

    return "", "NONE"


def run_query(query: str) -> pd.DataFrame:
    """Execute a SELECT query and return a DataFrame."""
    tok, mode = _oauth_bearer_token()
    if not tok:
        st.error("No credentials available.")
        return pd.DataFrame()
    try:
        from databricks import sql as dbsql
        host = SERVER_HOSTNAME.replace("https://", "").replace("http://", "")
        with dbsql.connect(server_hostname=host, http_path=HTTP_PATH, access_token=tok) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                cols = [d[0] for d in (cur.description or [])]
                rows = cur.fetchall()
                return pd.DataFrame(rows, columns=cols) if cols else pd.DataFrame()
    except Exception as exc:
        st.error(f"Query failed: {exc}")
        return pd.DataFrame()


def run_dml(query: str, silent: bool = False) -> bool:
    """Execute a DML statement (INSERT/UPDATE/MERGE/CREATE). Returns True on success."""
    tok, _ = _oauth_bearer_token()
    if not tok:
        return False
    try:
        from databricks import sql as dbsql
        host = SERVER_HOSTNAME.replace("https://", "").replace("http://", "")
        with dbsql.connect(server_hostname=host, http_path=HTTP_PATH, access_token=tok) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
        return True
    except Exception as exc:
        if not silent:
            st.error(f"Update failed: {exc}")
        return False


# ═══════════════════════════════════════════════════════════════
#  ACTION DECISIONS TABLE — user decisions persisted to Delta
# ═══════════════════════════════════════════════════════════════
def _ensure_decisions_table():
    run_dml(f"""
        CREATE TABLE IF NOT EXISTS `{CATALOG}`.reporting.action_decisions (
            action_id  STRING,
            decision   STRING,
            notes      STRING,
            decided_at TIMESTAMP
        ) USING DELTA
    """, silent=True)


def write_decision(action_id: str, decision: str, notes: str = "") -> bool:
    """Upsert a user decision and update action_items.status."""
    safe_notes = notes.replace("'", "''").replace("\\", "\\\\")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ok = run_dml(f"""
        MERGE INTO `{CATALOG}`.reporting.action_decisions t
        USING (SELECT '{action_id}' AS action_id,
                      '{decision}'   AS decision,
                      '{safe_notes}' AS notes,
                      TIMESTAMP '{ts}' AS decided_at) s
        ON t.action_id = s.action_id
        WHEN MATCHED THEN UPDATE SET
            t.decision=s.decision, t.notes=s.notes, t.decided_at=s.decided_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    if ok:
        run_dml(f"UPDATE `{CATALOG}`.reporting.action_items "
                f"SET status='{decision}' WHERE action_id='{action_id}'")
    return ok


# ═══════════════════════════════════════════════════════════════
#  DATA LOADERS
# ═══════════════════════════════════════════════════════════════
@st.cache_data(ttl=120)
def load_actions():
    return run_query(f"""
        WITH latest AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY action_id ORDER BY evaluated_at DESC) rn
            FROM `{CATALOG}`.reporting.action_evaluations)
        SELECT a.*, e.recommendation, e.practicality_score, e.risks, e.pros, e.cons,
               e.cost_estimate, e.reasoning, e.evaluated_at, e.historical_precedent
        FROM `{CATALOG}`.reporting.action_items a
        LEFT JOIN latest e ON a.action_id=e.action_id AND e.rn=1
        ORDER BY a.priority""")


@st.cache_data(ttl=120)
def load_decisions():
    try:
        df = run_query(f"""
            SELECT action_id, decision, notes, decided_at
            FROM `{CATALOG}`.reporting.action_decisions""")
        return df
    except Exception:
        return pd.DataFrame(columns=["action_id", "decision", "notes", "decided_at"])


@st.cache_data(ttl=120)
def load_history():
    return run_query(f"SELECT * FROM `{CATALOG}`.action_intelligence.action_history ORDER BY action_date DESC")


@st.cache_data(ttl=120)
def load_evals():
    return run_query(f"SELECT * FROM `{CATALOG}`.reporting.action_evaluations ORDER BY evaluated_at DESC")


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════
# Badge/pill color definitions: (bg, border)
_BADGE_COLORS = {
    "PROCEED":  ("#166534", "#86efac"),   # dark green bg
    "CAUTION":  ("#92400e", "#fcd34d"),   # dark amber bg
    "REJECT":   ("#991b1b", "#fca5a5"),   # dark red bg
    "DEFER":    ("#374151", "#d1d5db"),   # dark grey bg
    "PENDING":  ("#0369a1", "#bae6fd"),   # dark blue bg
}
_DEC_COLORS = {
    "APPROVED": ("#166534", "#86efac"),
    "REJECTED": ("#991b1b", "#fca5a5"),
    "DEFERRED": ("#374151", "#d1d5db"),
}


def _badge_html(rec):
    if not rec:
        bg, bd = _BADGE_COLORS["PENDING"]
        return _pill("Pending", bg, bd)
    r = str(rec).upper()
    if "CAUTION" in r:
        bg, bd = _BADGE_COLORS["CAUTION"]
        return _pill("With Caution", bg, bd)
    if "PROCEED" in r:
        bg, bd = _BADGE_COLORS["PROCEED"]
        return _pill("Proceed", bg, bd)
    if "REJECT" in r:
        bg, bd = _BADGE_COLORS["REJECT"]
        return _pill("Reject", bg, bd)
    bg, bd = _BADGE_COLORS["DEFER"]
    return _pill("Defer", bg, bd)


def _decision_pill(d: str) -> str:
    d = (d or "").upper()
    if d in _DEC_COLORS:
        bg, bd = _DEC_COLORS[d]
        icons = {"APPROVED": "\u2713", "REJECTED": "\u2717", "DEFERRED": "\u23f8"}
        return _pill(f"{icons[d]} {d.title()}", bg, bd)
    return ""


def _esc(val) -> str:
    """HTML-escape a value for safe embedding in HTML templates."""
    return _html.escape(str(val if val is not None else ""))


def _render(html_str: str):
    """Render HTML via st.markdown, stripping blank lines first.
    Streamlit's markdown parser treats blank lines as HTML-block terminators,
    so any empty f-string interpolation (e.g. {dec_pill} when empty) would
    break the card rendering. Stripping blank lines prevents that."""
    cleaned = "\n".join(ln for ln in html_str.split("\n") if ln.strip())
    st.markdown(cleaned, unsafe_allow_html=True)


def _auto_fg(hex_bg: str) -> str:
    """Return white or near-black text color based on background luminance.
    Uses WCAG relative-luminance formula for AA contrast."""
    h = hex_bg.lstrip('#')
    if len(h) != 6:
        return '#0c1a27'
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    lum = (0.299 * r + 0.587 * g + 0.114 * b) / 255
    return '#ffffff' if lum < 0.55 else '#0c1a27'


def _pill(label: str, bg: str, border: str = "") -> str:
    """Return an HTML pill/badge with auto-contrasting text."""
    fg = _auto_fg(bg)
    bd = f"border:1px solid {border};" if border else ""
    return (f'<span style="background:{bg};color:{fg};{bd}'
            f'padding:3px 10px;border-radius:20px;font-weight:800;'
            f'font-size:.62rem;letter-spacing:.04em;text-transform:uppercase;'
            f'display:inline-block;">{label}</span>')


def _rec_class(rec):
    """Card accent class based on AI recommendation (green=proceed, red=risk)."""
    r = str(rec or "").upper()
    if "CAUTION" in r: return "rec-caution"
    if "PROCEED" in r: return "rec-proceed"
    if "REJECT" in r:  return "rec-defer"
    if "DEFER" in r:   return "rec-defer"
    return "rec-pending"


# ═══════════════════════════════════════════════════════════════
#  APP START
# ═══════════════════════════════════════════════════════════════
st.markdown(_CSS, unsafe_allow_html=True)

tok, auth_mode = _oauth_bearer_token()
bar = st.progress(0, "Initializing...")
if not tok:
    bar.progress(100, "Authentication failed")
    st.error("No credentials found. Check app configuration.")
    st.stop()

bar.progress(20, "Authenticated. Setting up...")
if "decisions_table_ready" not in st.session_state:
    _ensure_decisions_table()
    st.session_state["decisions_table_ready"] = True

bar.progress(45, "Loading actions...")
df_act = load_actions()
bar.progress(65, "Loading decisions...")
df_dec = load_decisions()
bar.progress(82, "Loading history...")
df_hist = load_history()
bar.progress(100, "Ready")
_time.sleep(0.22)
bar.empty()

# Merge user decisions into action dataframe
if not df_dec.empty and not df_act.empty and "action_id" in df_act.columns:
    df_act = df_act.merge(
        df_dec[["action_id", "decision", "notes"]].rename(
            columns={"decision": "user_decision", "notes": "user_notes"}),
        on="action_id", how="left")
if "user_decision" not in df_act.columns:
    df_act["user_decision"] = ""
if "user_notes" not in df_act.columns:
    df_act["user_notes"] = ""
# NaN from LEFT JOIN must become empty strings — float NaN is truthy in Python,
# so str(NaN) becomes "nan" and breaks decision/note display logic.
df_act["user_decision"] = df_act["user_decision"].fillna("")
df_act["user_notes"] = df_act["user_notes"].fillna("")


# ── SIDEBAR ──────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:.35rem 0 .15rem;">
        <div style="font-family:Sora,sans-serif;font-size:1.12rem;font-weight:800;
                    color:#7dd3e8;letter-spacing:-.02em;line-height:1.2;">GAP Supply Chain</div>
        <div style="font-size:.68rem;font-weight:700;color:#4a7a8e;
                    letter-spacing:.08em;text-transform:uppercase;margin-top:.12rem;">Control Tower</div>
    </div>""", unsafe_allow_html=True)
    st.markdown("---")

    # Action queue stats
    if not df_act.empty:
        total = len(df_act)
        recs = df_act.get("recommendation", pd.Series(dtype=str)).fillna("").str.upper()
        n_proceed = int(recs.str.contains("PROCEED").sum())
        n_decided = int(df_act["user_decision"].fillna("").ne("").sum())
        n_pending = total - n_decided
        st.markdown('<div style="font-size:.62rem;font-weight:800;color:#4a7a8e;letter-spacing:.08em;text-transform:uppercase;margin-bottom:.4rem;">Action Queue</div>', unsafe_allow_html=True)
        m1, m2, m3 = st.columns(3)
        m1.metric("Total", total)
        m2.metric("Proceed", n_proceed)
        m3.metric("Pending", n_pending)

    st.markdown("---")
    st.markdown(f'<div style="font-size:.73rem;color:#5590a8;line-height:1.6;">'  
                f'<span style="color:#5a9e70;">&#9679;</span> Connected&nbsp;&nbsp;'  
                f'Refreshed {_LOADED_AT}</div>', unsafe_allow_html=True)
    st.markdown("<div style='height:.3rem'/>"  , unsafe_allow_html=True)
    if st.button("\u21ba  Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.markdown("---")
    st.markdown("""
    <div style="font-size:.71rem;color:#4a7a8e;line-height:1.55;">
        <div style="font-weight:700;color:#5590a8;margin-bottom:.3rem;
                    letter-spacing:.06em;text-transform:uppercase;font-size:.62rem;">About</div>
        Review AI-evaluated supply chain recovery actions from the Multi-Agent Supervisor.
        Approve, reject, or defer each action to build your execution plan.
        Decisions are saved to Delta and update the action tracker in real time.
        <br/><br/>
        <span style="opacity:.55;">Genie agent integration coming soon.</span>
    </div>""", unsafe_allow_html=True)


# ── HERO ─────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
    <h1 class="hero-title">Action Intelligence Tracker</h1>
    <div class="hero-sub">Review AI-recommended supply chain interventions and build your execution plan.
    Approve, reject, or defer each action — your decisions are saved to Delta and reflected instantly.</div>
</div>""", unsafe_allow_html=True)

# KPI row
if not df_act.empty:
    recs = df_act.get("recommendation", pd.Series(dtype=str)).fillna("").str.upper()
    n_proceed  = int(recs.str.contains("PROCEED").sum())
    n_caution  = int(recs.str.contains("CAUTION").sum())
    n_ai_defer = int(recs.str.contains("DEFER").sum())
    total      = len(df_act)
    avg_score  = pd.to_numeric(df_act.get("practicality_score"), errors="coerce").mean()
    avg_score_s = f"{avg_score:.0f}" if pd.notna(avg_score) else "\u2014"
    n_decided  = int(df_act["user_decision"].fillna("").ne("").sum())
    _render(f"""
    <div class="kpi-row">
        <div class="kpi"><div class="kpi-val">{total}</div><div class="kpi-label">Total Actions</div></div>
        <div class="kpi"><div class="kpi-val" style="color:var(--good);">{n_proceed}</div><div class="kpi-label">AI: Proceed</div></div>
        <div class="kpi"><div class="kpi-val" style="color:var(--warn);">{n_caution}</div><div class="kpi-label">AI: Caution</div></div>
        <div class="kpi"><div class="kpi-val" style="color:var(--muted);">{n_ai_defer}</div><div class="kpi-label">AI: Defer</div></div>
        <div class="kpi"><div class="kpi-val">{avg_score_s}</div><div class="kpi-label">Avg Score</div></div>
        <div class="kpi"><div class="kpi-val" style="color:var(--brand);">{n_decided}/{total}</div><div class="kpi-label">Your Decisions</div></div>
    </div>""")


# ── TABS ─────────────────────────────────────────────────────
tab5, tab1, tab2, tab3, tab4 = st.tabs([
    "Executive Report",
    "Action Queue",
    "Analytics",
    "History & Precedent",
    "Detail Explorer",
])


# ══════════════════════════════════════════════════════════════
#  TAB 1 — ACTION QUEUE
# ══════════════════════════════════════════════════════════════
with tab1:
    if df_act.empty:
        st.info("No actions found. Run the Report Agent notebook to populate data.")
    else:
        # Filter bar
        fc1, fc2, fc3, fc4 = st.columns([2, 2, 2, 2])
        doms = sorted(df_act["domain"].dropna().unique()) if "domain" in df_act.columns else []
        tfs  = sorted(df_act["timeframe"].dropna().unique()) if "timeframe" in df_act.columns else []
        sel_d  = fc1.multiselect("Domain", doms, default=doms,
                                  label_visibility="collapsed", placeholder="Filter domain...")
        sel_t  = fc2.multiselect("Timeframe", tfs, default=tfs,
                                  label_visibility="collapsed", placeholder="Filter timeframe...")
        sel_st = fc3.selectbox("Status", ["All", "Undecided only", "Decided only"],
                                label_visibility="collapsed")
        sel_p  = fc4.selectbox("Priority", ["All", "P1-2 Critical", "P3-4 High", "P5+ Low"],
                                label_visibility="collapsed")

        filt = df_act.copy()
        if sel_d: filt = filt[filt["domain"].isin(sel_d)]
        if sel_t: filt = filt[filt["timeframe"].isin(sel_t)]
        if sel_st == "Undecided only":
            filt = filt[filt["user_decision"].fillna("") == ""]
        elif sel_st == "Decided only":
            filt = filt[filt["user_decision"].fillna("") != ""]
        if sel_p == "P1-2 Critical":
            filt = filt[pd.to_numeric(filt["priority"], errors="coerce") <= 2]
        elif sel_p == "P3-4 High":
            filt = filt[pd.to_numeric(filt["priority"], errors="coerce").between(3, 4)]
        elif sel_p == "P5+ Low":
            filt = filt[pd.to_numeric(filt["priority"], errors="coerce") >= 5]

        st.markdown(f"<div style='font-size:.75rem;color:var(--muted);margin:.3rem 0 .8rem;font-weight:600;'>"
                    f"Showing {len(filt)} of {len(df_act)} actions</div>", unsafe_allow_html=True)

        for _, r in filt.iterrows():
            action_id  = str(r.get("action_id", ""))
            rclass     = _rec_class(r.get("recommendation"))
            sc         = r.get("practicality_score", "")
            score_html = (f'<span style="font-size:.8rem;font-weight:800;color:var(--brand);">'
                          f'{float(sc):.0f}%</span>') if sc else ""
            _raw_dec   = r.get("user_decision", "")
            user_dec   = str(_raw_dec).upper() if pd.notna(_raw_dec) and _raw_dec else ""
            dec_pill   = _decision_pill(user_dec)
            decided_cls = "decided" if user_dec else ""

            # Card header (HTML) — all data values escaped, blank lines stripped
            card_domain = _esc(r.get('domain', ''))
            card_title = _esc(r.get('title', ''))
            card_desc = _esc(r.get('description', ''))
            card_impact = _esc(r.get('expected_impact', ''))
            card_pri = _esc(r.get('priority', '?'))
            _render(f"""
            <div class="act-card {rclass} {decided_cls}">
              <div class="card-top">
                <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:.35rem;">
                  <div style="display:flex;gap:.38rem;align-items:center;flex-wrap:wrap;">
                    <span class="dtag">{card_domain}</span>
                    {_badge_html(r.get('recommendation'))}
                    {dec_pill}
                  </div>
                  <div style="display:flex;gap:.45rem;align-items:center;">
                    {score_html}
                    <span style="color:var(--muted);font-size:.7rem;font-weight:700;
                                 background:var(--bg-1);padding:2px 8px;border-radius:7px;
                                 ">P{card_pri}</span>
                  </div>
                </div>
                <div style="font-size:1.05rem;font-weight:800;color:var(--ink);
                            font-family:Sora,sans-serif;margin-bottom:.3rem;">{card_title}</div>
                <div style="font-size:.88rem;color:var(--muted);line-height:1.5;
                            font-weight:500;margin-bottom:.3rem;">{card_desc}</div>
                <div style="font-size:.84rem;color:var(--brand);font-weight:700;">
                    &#8594; {card_impact}</div>
              </div>
            </div>""")

            # Action buttons (real Streamlit widgets below the HTML card)
            mode_key = f"mode_{action_id}"
            pending  = st.session_state.get(mode_key, "")

            if not pending:
                ba1, ba2, ba3, ba4 = st.columns([1.1, 1.1, 1.1, 5])
                if ba1.button("\u2713  Approve", key=f"app_{action_id}", use_container_width=True):
                    st.session_state[mode_key] = "APPROVED"; st.rerun()
                if ba2.button("\u2717  Reject", key=f"rej_{action_id}", use_container_width=True):
                    st.session_state[mode_key] = "REJECTED"; st.rerun()
                if ba3.button("\u23f8  Defer", key=f"def_{action_id}", use_container_width=True):
                    st.session_state[mode_key] = "DEFERRED"; st.rerun()
                _raw_note = r.get("user_notes", "")
                if user_dec and pd.notna(_raw_note) and _raw_note:
                    ba4.caption(f'Note: {str(_raw_note)[:80]}')
            else:
                label_map = {"APPROVED": "\u2713 Confirming Approval",
                             "REJECTED": "\u2717 Confirming Rejection",
                             "DEFERRED": "\u23f8 Confirming Deferral"}
                st.markdown(
                    f'<div style="font-size:.8rem;font-weight:800;color:var(--brand);'
                    f'margin:.3rem 0 .2rem;">{label_map.get(pending,pending)}: '
                    f'{r.get("title","")}</div>', unsafe_allow_html=True)
                nc, bc1, bc2 = st.columns([4, 1, 1])
                note = nc.text_input("Notes", key=f"note_{action_id}",
                                     placeholder="Add rationale, owner, or timeline...",
                                     label_visibility="collapsed")
                if bc1.button("Confirm", key=f"ok_{action_id}", type="primary",
                              use_container_width=True):
                    with st.spinner("Saving..."):
                        ok = write_decision(action_id, pending, note)
                    if ok:
                        del st.session_state[mode_key]
                        st.cache_data.clear()
                        st.success(f"Saved: {r.get('title','')} \u2192 {pending.title()}")
                        _time.sleep(0.7)
                        st.rerun()
                if bc2.button("Cancel", key=f"can_{action_id}", use_container_width=True):
                    del st.session_state[mode_key]; st.rerun()

            st.markdown("<div style='height:.25rem'/>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
#  TAB 2 — ANALYTICS
# ══════════════════════════════════════════════════════════════
with tab2:
    if df_act.empty:
        st.info("No data for analytics.")
    else:
        import plotly.express as px
        _PL = dict(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                   font=dict(family="Plus Jakarta Sans", color="#0c1a27", size=12),
                   margin=dict(t=20, b=20, l=10, r=10))

        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div class="section-kicker">AI Recommendation Mix</div>', unsafe_allow_html=True)
            rc = df_act["recommendation"].fillna("PENDING").value_counts().reset_index()
            rc.columns = ["Recommendation", "Count"]
            cm = {"PROCEED":"#166534", "PROCEED_WITH_CAUTION":"#92400e",
                  "DEFER":"#374151",   "REJECT":"#991b1b", "PENDING":"#94a3b8"}
            fig = px.pie(rc, values="Count", names="Recommendation",
                         color="Recommendation", color_discrete_map=cm, hole=0.48)
            fig.update_layout(**_PL, height=265)
            fig.update_traces(textposition="inside", textinfo="label+value",
                              textfont=dict(color="#fff", size=11))
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            st.markdown('<div class="section-kicker">Your Decisions</div>', unsafe_allow_html=True)
            if not df_dec.empty:
                dc = df_dec["decision"].value_counts().reset_index()
                dc.columns = ["Decision", "Count"]
                dcm = {"APPROVED":"#166534", "REJECTED":"#991b1b", "DEFERRED":"#374151"}
                fig2 = px.bar(dc, x="Decision", y="Count",
                              color="Decision", color_discrete_map=dcm)
                fig2.update_layout(**_PL, height=265, showlegend=False,
                                   xaxis=dict(gridcolor="#e4eef4"),
                                   yaxis=dict(gridcolor="#e4eef4", tickformat="d"))
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info("No decisions yet \u2014 use the Action Queue tab to start.")

        if "practicality_score" in df_act.columns:
            st.markdown('<div class="section-kicker" style="margin-top:.5rem;">Practicality Score by Action</div>',
                        unsafe_allow_html=True)
            sdf = df_act[["title", "practicality_score", "domain"]].copy()
            sdf["practicality_score"] = pd.to_numeric(sdf["practicality_score"], errors="coerce")
            sdf = sdf.dropna(subset=["practicality_score"]).sort_values("practicality_score")
            colmap = {"logistics":"#005f73","inventory":"#0a9396",
                      "supplier":"#457b6b", "demand":"#94725a", "finance":"#c17b3e"}
            fig3 = px.bar(sdf, x="practicality_score", y="title",
                          color="domain", orientation="h", color_discrete_map=colmap,
                          labels={"practicality_score": "Score (%)", "title": ""})
            fig3.update_layout(**_PL, height=max(220, len(sdf) * 38),
                               xaxis=dict(gridcolor="#e4eef4", range=[0, 105]),
                               yaxis=dict(gridcolor="rgba(0,0,0,0)"))
            st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════════════════════════
#  TAB 3 — HISTORY & PRECEDENT
# ══════════════════════════════════════════════════════════════
with tab3:
    if df_hist.empty:
        st.info("No historical data. Run the Report Agent notebook to seed action_history.")
    else:
        doms_h = sorted(df_hist["domain"].dropna().unique()) if "domain" in df_hist.columns else []
        sel_dh = st.multiselect("Filter domain", doms_h, default=doms_h,
                                 label_visibility="collapsed", placeholder="Filter domain...")
        filt_h = df_hist[df_hist["domain"].isin(sel_dh)] if sel_dh else df_hist
        st.markdown(f"<div style='font-size:.75rem;color:var(--muted);margin:.2rem 0 .6rem;font-weight:600;'>"
                    f"{len(filt_h)} historical actions</div>", unsafe_allow_html=True)

        for _, h in filt_h.iterrows():
            rating = str(h.get("success_rating", "")).upper()
            r_bg = {"SUCCESS": "#166534", "PARTIAL": "#92400e"}.get(rating, "#991b1b")
            r_bd = {"SUCCESS": "#86efac", "PARTIAL": "#fcd34d"}.get(rating, "#fca5a5")
            r_fg = _auto_fg(r_bg)
            cost = float(h.get("cost_usd", 0) or 0)
            mb   = float(h.get("metric_before", 0) or 0)
            ma   = float(h.get("metric_after", 0) or 0)
            delta = ma - mb
            sign  = "+" if delta >= 0 else ""
            h_dom = _esc(h.get('domain', ''))
            h_title = _esc(h.get('title', h.get('action_type', '')))
            h_metric = _esc(h.get('metric_name', ''))
            h_date = _esc(h.get('action_date', ''))
            h_region = _esc(h.get('region', emdash))
            h_lessons = _esc(str(h.get('lessons_learned', ''))[:260])
            _render(f"""
            <div class="hist-card">
                <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                    <div>
                        <span class="dtag">{h_dom}</span>
                        <span style="font-weight:800;font-size:.92rem;margin-left:.5rem;
                                     color:var(--ink);">{h_title}</span>
                    </div>
                    <span style="color:{r_fg};font-weight:800;font-size:.7rem;
                                 background:{r_bg};border:1px solid {r_bd};
                                 padding:2px 10px;border-radius:20px;white-space:nowrap;">{rating}</span>
                </div>
                <div class="mrow">
                    <span>Cost: <strong>${cost:,.0f}</strong></span>
                    <span>{h_metric}: <strong>{mb:.1f} &#8594; {ma:.1f} ({sign}{delta:.1f})</strong></span>
                    <span>Date: <strong>{h_date}</strong></span>
                    <span>Region: <strong>{h_region}</strong></span>
                </div>
                <p style="color:var(--muted);font-size:.77rem;margin:.32rem 0 0;
                          line-height:1.38;font-weight:500;">{h_lessons}</p>
            </div>""")


# ══════════════════════════════════════════════════════════════
#  TAB 4 — DETAIL EXPLORER
# ══════════════════════════════════════════════════════════════
with tab4:
    if df_act.empty:
        st.info("No actions to explore.")
    else:
        titles = df_act["title"].tolist() if "title" in df_act.columns else []
        if titles:
            sel = st.selectbox("Select an action to inspect", titles)
            row = df_act[df_act["title"] == sel].iloc[0] if sel else None
            if row is not None:
                action_id = str(row.get("action_id", ""))
                _raw_dec  = row.get("user_decision", "")
                user_dec  = str(_raw_dec).upper() if pd.notna(_raw_dec) and _raw_dec else ""

                d_title = _esc(row.get('title', ''))
                d_desc = _esc(row.get('description', ''))
                d_domain = _esc(row.get('domain', ''))
                d_pri = _esc(row.get('priority', '?'))
                d_tf = _esc(row.get('timeframe', ''))
                d_impact = _esc(row.get('expected_impact', 'N/A'))
                d_steps = _esc(row.get('specific_steps', 'N/A'))
                d_cost = _esc(row.get('cost_estimate', 'N/A'))
                d_reasoning = _esc(row.get('reasoning', 'No evaluation yet.'))
                d_risks = _esc(row.get('risks', 'N/A'))
                d_pros = _esc(row.get('pros', emdash))
                d_cons = _esc(row.get('cons', emdash))
                _render(f"""
                <div class="detail-section">
                    <div class="section-kicker">Overview</div>
                    <h4 style="font-size:1.05rem;margin:.1rem 0 .35rem;">{d_title}</h4>
                    <p>{d_desc}</p>
                    <div style="margin-top:.4rem;display:flex;gap:.4rem;align-items:center;flex-wrap:wrap;">
                        <span class="dtag">{d_domain}</span>
                        {_badge_html(row.get('recommendation'))}
                        {_decision_pill(user_dec) if user_dec else '<span class="badge b-pending">Not Yet Decided</span>'}
                        <span style="color:var(--muted);font-size:.74rem;font-weight:700;">
                            Priority {d_pri} &middot; {d_tf}</span>
                    </div>
                </div>""")

                c1, c2 = st.columns(2)
                with c1:
                    _render(f"""
                    <div class="detail-section">
                        <div class="section-kicker">Expected Impact</div>
                        <p>{d_impact}</p>
                    </div>
                    <div class="detail-section">
                        <div class="section-kicker">Specific Steps</div>
                        <p style="white-space:pre-wrap;">{d_steps}</p>
                    </div>
                    <div class="detail-section">
                        <div class="section-kicker">Cost Estimate</div>
                        <p style="font-weight:800;font-size:1rem;color:var(--ink);">{d_cost}</p>
                    </div>""")
                with c2:
                    _render(f"""
                    <div class="detail-section">
                        <div class="section-kicker">AI Reasoning</div>
                        <p>{d_reasoning}</p>
                    </div>
                    <div class="detail-section">
                        <div class="section-kicker">Risks</div>
                        <p>{d_risks}</p>
                    </div>
                    <div class="detail-section">
                        <div class="section-kicker">Pros / Cons</div>
                        <p><strong>Pros:</strong> {d_pros}<br/>
                        <strong>Cons:</strong> {d_cons}</p>
                    </div>""")

                if row.get("historical_precedent"):
                    d_prec = _esc(row.get('historical_precedent', ''))
                    _render(f"""
                    <div class="detail-section">
                        <div class="section-kicker">Historical Precedent</div>
                        <p>{d_prec}</p>
                    </div>""")

                # Decision panel
                st.markdown("---")
                st.markdown('<div class="section-kicker">Your Decision</div>', unsafe_allow_html=True)

                if user_dec:
                    _raw_note_d = row.get("user_notes", "")
                    note_s = _esc(str(_raw_note_d)) if pd.notna(_raw_note_d) and _raw_note_d else ""
                    dec_bg = _DEC_COLORS.get(user_dec, ("#374151", "#d1d5db"))[0]
                    dec_bd = _DEC_COLORS.get(user_dec, ("#374151", "#d1d5db"))[1]
                    dec_fg = _auto_fg(dec_bg)
                    _render(f"""
                    <div style="background:{dec_bg};border:1px solid {dec_bd};border-radius:12px;
                                padding:.75rem 1rem;margin-bottom:.5rem;">
                        <strong style="color:{dec_fg};">{user_dec.title()}</strong>
                        {f'<br/><span style="font-size:.82rem;color:{dec_fg};opacity:.8;">{note_s}</span>' if note_s else ''}
                    </div>""")
                    if st.button("Change Decision", key=f"chg_{action_id}"):
                        run_dml(f"DELETE FROM `{CATALOG}`.reporting.action_decisions "
                                f"WHERE action_id='{action_id}'")
                        run_dml(f"UPDATE `{CATALOG}`.reporting.action_items "
                                f"SET status='open' WHERE action_id='{action_id}'")
                        st.cache_data.clear(); st.rerun()
                else:
                    dc1, dc2, dc3 = st.columns([1, 1, 1])
                    approve = dc1.button("\u2713  Approve", key=f"det_app_{action_id}",
                                         type="primary", use_container_width=True)
                    reject  = dc2.button("\u2717  Reject",  key=f"det_rej_{action_id}",
                                         use_container_width=True)
                    defer   = dc3.button("\u23f8  Defer",   key=f"det_def_{action_id}",
                                         use_container_width=True)

                    if approve or reject or defer:
                        dec = "APPROVED" if approve else "REJECTED" if reject else "DEFERRED"
                        st.session_state[f"det_mode_{action_id}"] = dec

                    if st.session_state.get(f"det_mode_{action_id}"):
                        dec = st.session_state[f"det_mode_{action_id}"]
                        note = st.text_area("Notes", key=f"det_note_{action_id}",
                                            placeholder="Add rationale, owner, or timeline...")
                        cc1, cc2 = st.columns([1, 3])
                        if cc1.button("Confirm", key=f"det_ok_{action_id}", type="primary"):
                            with st.spinner("Saving..."):
                                write_decision(action_id, dec, note)
                            del st.session_state[f"det_mode_{action_id}"]
                            st.cache_data.clear()
                            st.success("Decision saved!")
                            _time.sleep(0.7); st.rerun()
                        if cc2.button("Cancel", key=f"det_can_{action_id}"):
                            del st.session_state[f"det_mode_{action_id}"]; st.rerun()


# ══════════════════════════════════════════════════════════════
#  TAB 5 — EXECUTIVE REPORT (first tab — uses only reporting schema)
# ══════════════════════════════════════════════════════════════
with tab5:
    import plotly.graph_objects as go

    _render(f"""
    <div style="margin-bottom:.5rem;">
        <div class="section-kicker">Supply Chain Executive Report</div>
        <div style="font-size:1.1rem;font-weight:700;color:var(--ink);font-family:Sora,sans-serif;">
            Western Region &mdash; August 2026</div>
        <div style="font-size:.88rem;color:var(--muted);margin-top:.25rem;line-height:1.5;">
            Generated by the Multi-Agent Supervisor pipeline. Review these KPIs and visuals first,
            then proceed to the <strong>Action Queue</strong> tab to approve, reject, or defer
            the AI-recommended recovery actions.</div>
    </div>""")

    # Chart card helper — elevated 3D card with shadow
    _CARD_OPEN = '<div style="background:#fff;border-radius:14px;padding:.8rem 1rem .5rem;margin-bottom:.7rem;box-shadow:0 4px 18px rgba(0,31,45,.09),0 1.5px 4px rgba(0,0,0,.04);overflow:visible;">'
    _CARD_CLOSE = '</div>'

    # Shared plotly layout — clean, no hover labels, no modebar
    _PL5 = dict(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Plus Jakarta Sans", color="#0c1a27", size=13),
                margin=dict(t=40, b=30, l=55, r=20),
                hovermode=False)
    _PL_CFG = dict(displayModeBar=False)

    # ── KPI Banner from executive_kpis ──
    try:
        df_kpi = run_query(f"SELECT * FROM `{CATALOG}`.reporting.executive_kpis")
        if not df_kpi.empty:
            k = df_kpi.iloc[0]
            rev_aug = float(k.get('revenue_last_month', 0))
            rev_jul = float(k.get('revenue_prior_month', 0))
            rev_chg = ((rev_aug - rev_jul) / rev_jul * 100) if rev_jul else 0
            late_pct = float(k.get('late_delivery_pct_last_month', 0))
            svc_lvl = float(k.get('service_level_pct', 0))
            stockouts = int(k.get('total_stockout_skus', 0))
            sup_late = float(k.get('supplier_late_pct_last_month', 0))
            avg_delay = float(k.get('avg_delay_days_last_month', 0))
            avg_dos = float(k.get('avg_days_of_supply', 0))

            _render(f"""
            <div class="kpi-row" style="grid-template-columns:repeat(4,1fr);margin-top:.4rem;">
                <div class="kpi"><div class="kpi-val" style="color:#c0392b;">${rev_aug/1e6:.2f}M</div>
                    <div class="kpi-label">Aug Revenue ({rev_chg:+.1f}%)</div></div>
                <div class="kpi"><div class="kpi-val" style="color:#c0392b;">{late_pct:.1f}%</div>
                    <div class="kpi-label">Late Delivery Rate</div></div>
                <div class="kpi"><div class="kpi-val" style="color:#d97706;">{svc_lvl:.1f}%</div>
                    <div class="kpi-label">Service Level</div></div>
                <div class="kpi"><div class="kpi-val">{stockouts}</div>
                    <div class="kpi-label">Stockout SKUs</div></div>
            </div>
            <div class="kpi-row" style="grid-template-columns:repeat(4,1fr);margin-top:.3rem;">
                <div class="kpi"><div class="kpi-val" style="color:#c0392b;">{sup_late:.0f}%</div>
                    <div class="kpi-label">Supplier Late Rate</div></div>
                <div class="kpi"><div class="kpi-val" style="color:#d97706;">{avg_delay:.1f}d</div>
                    <div class="kpi-label">Avg Delay Days</div></div>
                <div class="kpi"><div class="kpi-val" style="color:#c0392b;">{avg_dos:.2f}</div>
                    <div class="kpi-label">Avg Days of Supply</div></div>
                <div class="kpi"><div class="kpi-val" style="color:var(--brand);">Q3 FY27</div>
                    <div class="kpi-label">Reporting Period</div></div>
            </div>""")
    except Exception as exc:
        st.warning(f"KPI load: {exc}")

    # ── Row 1: Revenue + Cost of Disruption ──
    rc1, rc2 = st.columns(2)
    try:
        if not df_kpi.empty:
            with rc1:
                st.markdown(f'{_CARD_OPEN}<div class="section-kicker">Revenue Month-over-Month</div>',
                            unsafe_allow_html=True)
                months = ['Jul 2026', 'Aug 2026']
                vals = [rev_jul, rev_aug]
                colors = ['#005f73', '#c0392b']
                fig_r = go.Figure(go.Bar(x=months, y=vals, marker_color=colors,
                                         text=[f"${v/1e6:.2f}M" for v in vals],
                                         textposition='outside',
                                         textfont=dict(size=14, family='Sora', color='#0c1a27')))
                ymax = max(vals) * 1.25
                fig_r.update_layout(**_PL5, height=310,
                                    yaxis=dict(gridcolor='#e4eef4', range=[0, ymax],
                                               tickvals=[v for v in range(0, int(ymax)+1, 5_000_000)],
                                               ticktext=[f'${v/1e6:.0f}M' for v in range(0, int(ymax)+1, 5_000_000)]),
                                    xaxis=dict(gridcolor='rgba(0,0,0,0)'))
                st.plotly_chart(fig_r, use_container_width=True, config=_PL_CFG)
                st.markdown(_CARD_CLOSE, unsafe_allow_html=True)
    except Exception:
        pass

    try:
        df_cod = run_query(f"""
            SELECT region,
                   COALESCE(cancelled_revenue, 0) AS cancelled,
                   COALESCE(backordered_at_risk_revenue, 0) AS backordered,
                   COALESCE(wasted_logistics_spend, 0) AS wasted_freight,
                   COALESCE(allocated_supplier_penalties, 0) AS sla_penalties,
                   COALESCE(total_cost_of_disruption, 0) AS total
            FROM `{CATALOG}`.reporting.cost_of_disruption_by_region
            ORDER BY total DESC""")
        if not df_cod.empty:
            with rc2:
                st.markdown(f'{_CARD_OPEN}<div class="section-kicker">Cost of Disruption by Region</div>',
                            unsafe_allow_html=True)
                fig_c = go.Figure()
                for col, name, clr in [
                    ('wasted_freight', 'Wasted Freight', '#c0392b'),
                    ('sla_penalties', 'SLA Penalties', '#d97706'),
                    ('cancelled', 'Cancelled Rev', '#f59e0b'),
                    ('backordered', 'Backorder Risk', '#374151')]:
                    fig_c.add_trace(go.Bar(name=name, x=df_cod['region'], y=df_cod[col],
                                           marker_color=clr))
                fig_c.update_layout(**_PL5, height=310, barmode='stack',
                                    legend=dict(orientation='h', y=-0.18, font=dict(size=10)),
                                    yaxis=dict(gridcolor='#e4eef4', tickprefix='$', tickformat=',.0s'))
                st.plotly_chart(fig_c, use_container_width=True, config=_PL_CFG)
                st.markdown(_CARD_CLOSE, unsafe_allow_html=True)
    except Exception as exc:
        st.warning(f"CoD chart: {exc}")

    # ── Row 2: Supplier On-Time + Regional Revenue ──
    sc1, sc2 = st.columns(2)
    try:
        df_risk = run_query(f"""
            SELECT continent, risk_tier,
                   ROUND(AVG(on_time_pct), 1) AS avg_otd,
                   COUNT(*) AS suppliers,
                   SUM(sla_breaches) AS total_breaches
            FROM `{CATALOG}`.reporting.supply_chain_risk_scorecard
            GROUP BY continent, risk_tier
            ORDER BY avg_otd ASC""")
        if not df_risk.empty:
            with sc1:
                st.markdown(f'{_CARD_OPEN}<div class="section-kicker">Supplier On-Time by Continent</div>',
                            unsafe_allow_html=True)
                agg = df_risk.groupby('continent').agg({'avg_otd': 'mean'}).reset_index().sort_values('avg_otd')
                bar_colors = ['#c0392b' if v < 50 else '#d97706' if v < 75 else '#166534' for v in agg['avg_otd']]
                fig_s = go.Figure(go.Bar(y=agg['continent'], x=agg['avg_otd'],
                                         orientation='h', marker_color=bar_colors,
                                         text=[f"{v:.0f}%" for v in agg['avg_otd']],
                                         textposition='outside', textfont=dict(size=13, family='Sora')))
                fig_s.update_layout(**_PL5, height=290,
                                    xaxis=dict(gridcolor='#e4eef4', range=[0, 120],
                                               title=dict(text='On-Time %', font=dict(size=11))))
                st.plotly_chart(fig_s, use_container_width=True, config=_PL_CFG)
                st.markdown(_CARD_CLOSE, unsafe_allow_html=True)
    except Exception:
        pass

    try:
        df_reg = run_query(f"""
            SELECT region, total_revenue, fulfillment_rate, stockout_skus, late_shipment_pct
            FROM `{CATALOG}`.reporting.regional_performance_summary
            ORDER BY total_revenue DESC""")
        if not df_reg.empty:
            with sc2:
                st.markdown(f'{_CARD_OPEN}<div class="section-kicker">Regional Revenue (Aug 2026)</div>',
                            unsafe_allow_html=True)
                fig_p = go.Figure(go.Bar(
                    x=df_reg['region'],
                    y=[v/1e6 for v in df_reg['total_revenue']],
                    marker_color=['#c0392b' if r == 'Western' else '#005f73' for r in df_reg['region']],
                    text=[f"${v/1e6:.1f}M" for v in df_reg['total_revenue']],
                    textposition='outside', textfont=dict(size=13, family='Sora')))
                ymax_r = max(v/1e6 for v in df_reg['total_revenue']) * 1.22
                fig_p.update_layout(**_PL5, height=290,
                                    yaxis=dict(gridcolor='#e4eef4', range=[0, ymax_r],
                                               tickprefix='$', ticksuffix='M'))
                st.plotly_chart(fig_p, use_container_width=True, config=_PL_CFG)
                st.markdown(_CARD_CLOSE, unsafe_allow_html=True)
    except Exception:
        pass

    # ── Service Level vs Target (full width card) ──
    try:
        svc = float(df_kpi.iloc[0].get('service_level_pct', 80.7)) if not df_kpi.empty else 80.7
        target = 95.0
        st.markdown(f'{_CARD_OPEN}<div class="section-kicker">Service Level vs Q3 Fiscal Target</div>',
                    unsafe_allow_html=True)
        fig_sl = go.Figure()
        fig_sl.add_trace(go.Bar(x=[svc], y=['Service Level'], orientation='h',
                                marker_color='#c0392b', name=f'Actual: {svc:.1f}%',
                                text=[f'{svc:.1f}%'], textposition='inside',
                                textfont=dict(color='white', size=15, family='Sora')))
        fig_sl.add_vline(x=target, line_dash='dash', line_width=2.5,
                         annotation_text=f'Q3 Target: {target}%',
                         annotation_position='top',
                         annotation_font=dict(size=12, color='#166534'))
        fig_sl.update_layout(**_PL5, height=140, margin=dict(t=40, b=40, l=55, r=70),
                             xaxis=dict(range=[0, 108], gridcolor='#e4eef4'),
                             showlegend=True,
                             legend=dict(orientation='h', y=-0.45, font=dict(size=11)))
        st.plotly_chart(fig_sl, use_container_width=True, config=_PL_CFG)
        st.markdown(_CARD_CLOSE, unsafe_allow_html=True)
    except Exception:
        pass

    # Workflow guidance
    _render(f"""
    <div style="margin-top:.5rem;padding:.75rem 1.1rem;background:linear-gradient(115deg,#f0f7fb,#e8f4f0);
                border-radius:12px;border-left:4px solid var(--brand);font-size:.9rem;color:var(--ink);
                line-height:1.55;box-shadow:0 3px 10px rgba(0,31,45,.06);">
        <strong style="font-family:Sora,sans-serif;">Next step:</strong> Switch to the
        <strong>Action Queue</strong> tab to review AI-recommended recovery actions.
        Approve, reject, or defer each action to build your execution plan.
    </div>""")
