"""
Tab: Interactive Topology Explorer

Production-grade interactive network using pyvis/vis.js.
Supports large graphs via Barnes-Hut physics, neighbourhood filtering,
and both QM-level and App-level views.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from ..analytics import qm_neighborhood_map
from ..constants import NEIGHBORHOOD_COLORS
from ..data import neighborhood_col
from ..graph import build_app_graph, build_qm_graph
from ..network_viz import st_pyvis


def render(df: pd.DataFrame) -> None:
    st.header("Interactive Topology Explorer")
    st.markdown(
        "**vis.js powered** — drag nodes, scroll to zoom, hover for details. "
        "Handles large topologies via Barnes-Hut physics simulation."
    )

    nb_col = neighborhood_col(df)

    # ── Controls ──────────────────────────────────────────────────────────────
    col_l, col_r = st.columns([2, 1])
    with col_l:
        view = st.radio(
            "Graph level",
            ["Queue Manager", "Application"],
            horizontal=True,
        )
    with col_r:
        physics_on = st.toggle("Live physics", value=True)
        show_ctrl  = st.toggle("Show vis.js controls", value=False)

    all_nbs = sorted(df[nb_col].dropna().unique().tolist())
    filter_nbs = st.multiselect(
        "Filter by Neighbourhood", all_nbs, default=all_nbs,
        help="Restrict graph to selected neighbourhoods only",
    )

    df_filt = df[df[nb_col].isin(filter_nbs)].copy()

    # ── Stats strip ───────────────────────────────────────────────────────────
    if view == "Queue Manager":
        G = build_qm_graph(df_filt)
        label = "Queue Manager"
    else:
        G = build_app_graph(df_filt)
        label = "Application"

    s1, s2, s3, s4 = st.columns(4)
    s1.metric(f"{label} nodes",  len(G.nodes))
    s2.metric("Directed edges",  len(G.edges))
    s3.metric("Weakly connected components",
              len(list(__import__("networkx").weakly_connected_components(G))))
    s4.metric("Avg degree", f"{sum(dict(G.degree()).values()) / max(len(G.nodes), 1):.1f}")

    st.markdown("---")

    # Large-graph warning
    if len(G.nodes) > 100:
        st.warning(
            f"Graph has {len(G.nodes)} nodes. Physics is automatically tuned for "
            "performance — disable it if rendering is slow."
        )

    st_pyvis(G, height=660, physics=physics_on, show_controls=show_ctrl)

    # ── Neighbourhood legend ───────────────────────────────────────────────────
    st.markdown("---")
    st.caption("Neighbourhood colour legend")
    cols = st.columns(len(NEIGHBORHOOD_COLORS))
    for i, (nb, col) in enumerate(NEIGHBORHOOD_COLORS.items()):
        cols[i].markdown(
            f'<span style="display:inline-block;width:14px;height:14px;'
            f'background:{col};border-radius:50%;margin-right:4px;'
            f'vertical-align:middle"></span> **{nb}**',
            unsafe_allow_html=True,
        )

    # ── Adjacency table (fallback / detail) ───────────────────────────────────
    with st.expander("Raw edge list"):
        edge_rows = [{"From": u, "To": v,
                      "Weight": G[u][v].get("weight", 1)} for u, v in G.edges()]
        if edge_rows:
            st.dataframe(pd.DataFrame(edge_rows).sort_values("Weight", ascending=False),
                         use_container_width=True)
        else:
            st.info("No edges in filtered graph.")
