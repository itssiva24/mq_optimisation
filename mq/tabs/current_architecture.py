"""Tab 2 – Current Architecture."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from ..charts import create_heatmap, create_network_graph
from ..data import extract_flows, neighborhood_col
from ..graph import build_app_graph, build_qm_graph


def render(df: pd.DataFrame) -> None:
    st.header("Current Architecture")
    st.markdown(
        '<div class="warning-box">'
        "<b>Pain Point:</b> Multiple applications share the same Queue Managers, "
        "creating tight coupling, complex channel configurations, and high blast-radius "
        "for failures."
        "</div>",
        unsafe_allow_html=True,
    )

    nb_col = neighborhood_col(df)
    qm_app = df.groupby("queue_manager_name")["app_id"].nunique()
    shared_qm_count = int((qm_app > 1).sum())
    flows = extract_flows(df)
    apps_multi_qm = len({f["prod_app"] for f in flows if f["prod_qm"] != f["cons_qm"]})

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Queue Managers", df["queue_manager_name"].nunique(),
              help="Number of unique QMs in the current environment")
    m2.metric("Shared QMs", shared_qm_count,
              delta=f"-{shared_qm_count} pain points", delta_color="inverse",
              help="QMs that host more than one application")
    m3.metric("Cross-QM App Flows", apps_multi_qm,
              help="Apps that communicate across different Queue Managers")
    m4.metric("Total Message Flows", len(flows),
              help="Remote queue entries representing app-to-app message paths")

    st.markdown("---")
    view_mode = st.radio(
        "View", ["QM-Level Network", "App-Level Network", "Channel Heatmap"],
        horizontal=True,
    )

    if view_mode == "QM-Level Network":
        G_qm = build_qm_graph(df)
        st.plotly_chart(
            create_network_graph(G_qm, "Current QM Topology – Shared Queue Managers"),
            use_container_width=True,
        )

    elif view_mode == "App-Level Network":
        G_app = build_app_graph(df)
        if len(G_app.nodes) > 50:
            st.info("Large graph – showing adjacency matrix for clarity.")
            fig = create_heatmap(df, "app_id", "remote_q_mgr_name",
                                 "App to QM Channel Density")
        else:
            fig = create_network_graph(G_app, "Current App-Level Message Flow")
        st.plotly_chart(fig, use_container_width=True)

    else:
        st.plotly_chart(
            create_heatmap(df, "queue_manager_name", "remote_q_mgr_name",
                           "Channel Density: Source QM → Target QM"),
            use_container_width=True,
        )

    st.markdown("---")
    st.subheader("Queue Manager Details")
    qm_detail = (
        df.groupby("queue_manager_name")
          .agg(
              Apps=(       "app_id",             lambda x: ", ".join(sorted(x.dropna().unique()))),
              App_Count=(  "app_id",             "nunique"),
              Queue_Count=("Discrete Queue Name","count"),
              Neighborhood=(nb_col,              lambda x: ", ".join(sorted(x.dropna().unique()))),
          )
          .reset_index()
          .sort_values("App_Count", ascending=False)
    )
    st.dataframe(qm_detail, use_container_width=True)

    st.markdown("---")
    st.subheader("App ↔ QM Sharing Matrix")
    pivot = df.pivot_table(
        index="app_id", columns="queue_manager_name",
        values="Discrete Queue Name", aggfunc="count", fill_value=0,
    )
    fig_hm = px.imshow(
        pivot, text_auto=True, color_continuous_scale="Reds",
        title="Queue Count: App × QM (non-zero = shared risk)",
        aspect="auto",
    )
    fig_hm.update_layout(height=max(400, len(pivot) * 28 + 100))
    st.plotly_chart(fig_hm, use_container_width=True)
