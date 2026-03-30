"""
IBM MQ Architecture Intelligence
─────────────────────────────────
Entry-point for the Streamlit application.
All business logic lives in the mq/ package.
"""

import pandas as pd
import streamlit as st

from mq.data import load_data, neighborhood_col
from mq.tabs import (
    complexity_metrics,
    current_architecture,
    data_explorer,
    dc_migration,
    deep_analytics,
    disaster_recovery,
    target_architecture,
    topology_explorer,
    validation,
)

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG  (must be first Streamlit call)
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="MQ Architecture Intelligence",
    page_icon="🔀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL CSS  – IBM Carbon Dark theme enhancement
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
/* ── Fonts ── */
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
}
code, pre, .stCode { font-family: 'IBM Plex Mono', monospace !important; }

/* ── Metric cards ── */
div[data-testid="stMetric"] {
    background: linear-gradient(135deg, #1e2235 0%, #262a3e 100%);
    border-left: 3px solid #0f62fe;
    border-radius: 8px;
    padding: 1rem 1.2rem;
    box-shadow: 0 2px 8px rgba(0,0,0,0.35);
    transition: transform .15s ease;
}
div[data-testid="stMetric"]:hover { transform: translateY(-2px); }
div[data-testid="stMetricValue"]  { font-size: 1.9rem !important; font-weight: 700 !important; color: #f4f4f4 !important; }
div[data-testid="stMetricLabel"]  { font-size: 0.78rem !important; color: #8b8d9b !important; text-transform: uppercase; letter-spacing: .06em; }

/* ── Tabs ── */
button[data-baseweb="tab"] {
    font-size: 0.82rem !important;
    font-weight: 600 !important;
    letter-spacing: .04em;
    text-transform: uppercase;
    padding: 0.6rem 1rem !important;
    color: #8b8d9b !important;
}
button[data-baseweb="tab"][aria-selected="true"] {
    color: #0f62fe !important;
    border-bottom: 2px solid #0f62fe !important;
}
div[data-testid="stTabs"] { border-bottom: 1px solid #2a2a40; }

/* ── Info / warning / success boxes ── */
.info-box {
    background: linear-gradient(90deg,#0f1a3e 0%,#1a2550 100%);
    border-left: 4px solid #0f62fe;
    padding: 14px 18px; border-radius: 6px; margin: 10px 0;
    font-size: 0.92rem; line-height: 1.6;
}
.warning-box {
    background: linear-gradient(90deg,#2b1e00 0%,#3b2800 100%);
    border-left: 4px solid #f1c21b;
    padding: 14px 18px; border-radius: 6px; margin: 10px 0;
    font-size: 0.92rem; line-height: 1.6;
}
.success-box {
    background: linear-gradient(90deg,#001a0a 0%,#002910 100%);
    border-left: 4px solid #42be65;
    padding: 14px 18px; border-radius: 6px; margin: 10px 0;
    font-size: 0.92rem; line-height: 1.6;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0d1117 0%, #161b27 100%);
    border-right: 1px solid #21262d;
}
section[data-testid="stSidebar"] .stMarkdown h1,
section[data-testid="stSidebar"] .stMarkdown h2,
section[data-testid="stSidebar"] .stMarkdown h3 {
    color: #58a6ff;
    font-size: 0.75rem !important;
    text-transform: uppercase;
    letter-spacing: .08em;
}

/* ── Expanders ── */
details { background: #1c1f2e !important; border-radius: 6px !important; border: 1px solid #2d3148 !important; }
summary { font-weight: 600 !important; }

/* ── DataFrames ── */
div[data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }

/* ── Buttons ── */
button[kind="primary"] {
    background: linear-gradient(135deg, #0f62fe, #0043ce) !important;
    border: none !important;
    border-radius: 6px !important;
    font-weight: 600 !important;
    letter-spacing: .04em !important;
    box-shadow: 0 4px 12px rgba(15,98,254,0.35) !important;
    transition: box-shadow .2s ease !important;
}
button[kind="primary"]:hover {
    box-shadow: 0 6px 18px rgba(15,98,254,0.55) !important;
}

/* ── Hero header ── */
.hero-header {
    background: linear-gradient(135deg,#0a0e1a 0%,#0d1a3e 50%,#0a1228 100%);
    border: 1px solid #1e2d5e;
    border-radius: 12px;
    padding: 1.6rem 2rem;
    margin-bottom: 1.5rem;
    position: relative;
    overflow: hidden;
}
.hero-header::before {
    content: '';
    position: absolute;
    top: -60px; right: -60px;
    width: 200px; height: 200px;
    background: radial-gradient(circle, rgba(15,98,254,0.15) 0%, transparent 70%);
    border-radius: 50%;
}
.hero-title {
    font-size: 1.75rem;
    font-weight: 700;
    color: #f4f4f4;
    margin: 0 0 4px 0;
    letter-spacing: -.01em;
}
.hero-sub {
    font-size: 0.9rem;
    color: #8b96ae;
    margin: 0;
    letter-spacing: .02em;
}
.hero-pill {
    display: inline-block;
    background: rgba(15,98,254,0.15);
    color: #4d8eff;
    border: 1px solid rgba(15,98,254,0.3);
    border-radius: 20px;
    padding: 2px 10px;
    font-size: 0.74rem;
    font-weight: 600;
    letter-spacing: .06em;
    margin-right: 6px;
    text-transform: uppercase;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

def render_sidebar() -> pd.DataFrame:
    with st.sidebar:
        st.markdown("### 📂 Data Source")
        uploaded = st.file_uploader("Upload MQ CSV", type=["csv"],
                                    help="Upload your own queue object export. "
                                         "Falls back to bundled sample data.")
        df_raw = load_data(uploaded)

        if df_raw.empty:
            st.error("No data loaded.")
            st.stop()

        st.success(f"✓ {len(df_raw):,} rows loaded")

        st.markdown("---")
        st.markdown("### 🔍 Filters")
        nb_col   = neighborhood_col(df_raw)
        all_nb   = sorted(df_raw[nb_col].dropna().unique().tolist())
        sel_nb   = st.multiselect("Neighbourhood", all_nb, default=all_nb)

        all_apps = sorted(df_raw["app_id"].dropna().unique().tolist())
        sel_apps = st.multiselect("App IDs", all_apps, default=all_apps)

        df = df_raw[
            df_raw[nb_col].isin(sel_nb) & df_raw["app_id"].isin(sel_apps)
        ].copy()

        st.markdown("---")
        st.markdown(
            f"**Rows:**&nbsp;&nbsp;`{len(df):,}`  \n"
            f"**QMs:**&nbsp;&nbsp;`{df['queue_manager_name'].nunique()}`  \n"
            f"**Apps:**&nbsp;&nbsp;`{df['app_id'].nunique()}`  \n"
            f"**Queues:**&nbsp;&nbsp;`{df['Discrete Queue Name'].nunique()}`"
        )

        st.markdown("---")
        st.caption("MQ Architecture Intelligence · v2.0")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# HERO HEADER
# ─────────────────────────────────────────────────────────────────────────────

def render_hero(df: pd.DataFrame) -> None:
    nb_col         = neighborhood_col(df)
    num_qms        = df["queue_manager_name"].nunique()
    num_apps       = df["app_id"].nunique()
    qm_app         = df.groupby("queue_manager_name")["app_id"].nunique()
    shared_qms     = int((qm_app > 1).sum())
    remote_count   = len(df[df["q_type"] == "Remote"])
    neighborhoods  = df[nb_col].nunique()

    st.markdown(f"""
<div class="hero-header">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:12px">
    <div>
      <p class="hero-title">🔀 MQ Architecture Intelligence</p>
      <p class="hero-sub">Analyse · Quantify · Transform · Automate IBM MQ environments</p>
      <div style="margin-top:10px">
        <span class="hero-pill">IBM MQ</span>
        <span class="hero-pill">Topology</span>
        <span class="hero-pill">1-QM-per-App</span>
        <span class="hero-pill">vis.js</span>
      </div>
    </div>
    <div style="display:flex;gap:24px;flex-wrap:wrap">
      <div style="text-align:center">
        <div style="font-size:2rem;font-weight:700;color:#0f62fe">{num_qms}</div>
        <div style="font-size:0.72rem;color:#8b96ae;text-transform:uppercase;letter-spacing:.06em">Queue Mgrs</div>
      </div>
      <div style="text-align:center">
        <div style="font-size:2rem;font-weight:700;color:#42be65">{num_apps}</div>
        <div style="font-size:0.72rem;color:#8b96ae;text-transform:uppercase;letter-spacing:.06em">Applications</div>
      </div>
      <div style="font-size:2rem;font-weight:700;text-align:center">
        <div style="color:#fa4d56">{shared_qms}</div>
        <div style="font-size:0.72rem;color:#8b96ae;text-transform:uppercase;letter-spacing:.06em">Shared QMs</div>
      </div>
      <div style="text-align:center">
        <div style="font-size:2rem;font-weight:700;color:#f1c21b">{remote_count}</div>
        <div style="font-size:0.72rem;color:#8b96ae;text-transform:uppercase;letter-spacing:.06em">Msg Flows</div>
      </div>
      <div style="text-align:center">
        <div style="font-size:2rem;font-weight:700;color:#be95ff">{neighborhoods}</div>
        <div style="font-size:0.72rem;color:#8b96ae;text-transform:uppercase;letter-spacing:.06em">Neighbourhoods</div>
      </div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    df = render_sidebar()
    render_hero(df)

    qm_app_counts   = df.groupby("queue_manager_name")["app_id"].nunique()
    shared_qm_count = int((qm_app_counts > 1).sum())

    (
        tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9,
    ) = st.tabs([
        "📊  Data",
        "🗺️  Topology",
        "🏗️  Current",
        "🎯  Target",
        "🔬  Analytics",
        "📈  Complexity",
        "🚚  Migration",
        "🛡️  DR",
        "✅  Validate",
    ])

    with tab1: data_explorer.render(df)
    with tab2: topology_explorer.render(df)
    with tab3: current_architecture.render(df)
    with tab4: target_architecture.render(df, shared_qm_count)
    with tab5: deep_analytics.render(df)
    with tab6: complexity_metrics.render(df)
    with tab7: dc_migration.render(df)
    with tab8: disaster_recovery.render(df)
    with tab9: validation.render(df)


if __name__ == "__main__":
    main()
