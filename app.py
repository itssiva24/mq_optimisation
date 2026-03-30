"""
IBM MQ Architecture Intelligence
Streamlit entry-point – all logic lives in the mq/ package.
"""

import pandas as pd
import streamlit as st

from mq.data import load_data, neighborhood_col
from mq.tabs import (
    current_architecture,
    data_explorer,
    dc_migration,
    disaster_recovery,
    complexity_metrics,
    target_architecture,
    validation,
)

st.set_page_config(
    page_title="MQ Architecture Intelligence",
    page_icon="🔀",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""<style>
div[data-testid="stMetric"] {
    background-color: #f8f9fa;
    padding: 1rem;
    border-radius: 8px;
    border-left: 4px solid #0062cc;
}
.info-box {
    background-color: #e8f4fd;
    border-left: 4px solid #0062cc;
    padding: 12px;
    border-radius: 4px;
    margin: 8px 0;
}
.warning-box {
    background-color: #fff3cd;
    border-left: 4px solid #ffc107;
    padding: 12px;
    border-radius: 4px;
    margin: 8px 0;
}
.success-box {
    background-color: #d4edda;
    border-left: 4px solid #28a745;
    padding: 12px;
    border-radius: 4px;
    margin: 8px 0;
}
</style>""", unsafe_allow_html=True)


def sidebar(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Render sidebar controls and return the filtered DataFrame."""
    with st.sidebar:
        st.header("📂 Data Source")
        uploaded = st.file_uploader("Upload MQ CSV", type=["csv"])
        df_raw   = load_data(uploaded)

        if df_raw.empty:
            st.error("No data loaded.")
            st.stop()

        st.success(f"Loaded {len(df_raw):,} rows")

        st.header("🔍 Filters")
        nb_col = neighborhood_col(df_raw)
        all_nb = sorted(df_raw[nb_col].dropna().unique().tolist())
        sel_nb = st.multiselect("Neighborhood", all_nb, default=all_nb)

        all_apps = sorted(df_raw["app_id"].dropna().unique().tolist())
        sel_apps = st.multiselect("App IDs", all_apps, default=all_apps,
                                  help="Filter by application identifier")

        df = df_raw[df_raw[nb_col].isin(sel_nb) & df_raw["app_id"].isin(sel_apps)].copy()

        st.markdown("---")
        st.markdown(f"**Filtered rows:** {len(df):,}")
        st.markdown(f"**Queue Managers:** {df['queue_manager_name'].nunique()}")
        st.markdown(f"**Applications:** {df['app_id'].nunique()}")

    return df


def main() -> None:
    st.title("🔀 IBM MQ Architecture Intelligence")
    st.markdown(
        "**Analyse, optimise, and transform your IBM MQ messaging infrastructure** – "
        "from tangled shared queue managers to a clean, one-QM-per-app target architecture."
    )

    df_raw = load_data()
    df     = sidebar(df_raw)

    # Shared derived value used across multiple tabs
    qm_app_counts  = df.groupby("queue_manager_name")["app_id"].nunique()
    shared_qm_count = int((qm_app_counts > 1).sum())

    (
        tab1, tab2, tab3, tab4, tab5, tab6, tab7
    ) = st.tabs([
        "📊 Data Explorer",
        "🏗️ Current Architecture",
        "🎯 Target Architecture",
        "📈 Complexity Metrics",
        "🚚 DC Migration",
        "🛡️ Disaster Recovery",
        "✅ Validation",
    ])

    with tab1: data_explorer.render(df)
    with tab2: current_architecture.render(df)
    with tab3: target_architecture.render(df, shared_qm_count)
    with tab4: complexity_metrics.render(df)
    with tab5: dc_migration.render(df)
    with tab6: disaster_recovery.render(df)
    with tab7: validation.render(df)


if __name__ == "__main__":
    main()
