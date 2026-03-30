"""Tab 3 – Target Architecture."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from ..architecture import generate_target_architecture
from ..graph import build_app_graph, build_qm_graph
from ..network_viz import st_pyvis


def render(df: pd.DataFrame, shared_qm_count: int) -> None:
    st.header("Target Architecture")
    st.markdown(
        '<div class="info-box">'
        "<b>Design Principle:</b> One dedicated Queue Manager per application "
        "<code>QM.{app_id}</code>. This eliminates shared-QM coupling, reduces blast "
        "radius, and simplifies channel management."
        "</div>",
        unsafe_allow_html=True,
    )

    if st.button("🚀 Generate Target Architecture", type="primary"):
        with st.spinner("Computing target architecture…"):
            target_df = generate_target_architecture(df)
            st.session_state["target_df"] = target_df
        st.success("Target architecture generated!")

    if "target_df" not in st.session_state:
        st.info("Click **Generate Target Architecture** to compute the 1-QM-per-app layout.")
        return

    target_df: pd.DataFrame = st.session_state["target_df"]

    st.subheader("Before vs After")
    b1, b2, b3, b4 = st.columns(4)
    b1.metric("Current QMs",  df["queue_manager_name"].nunique(),
              delta=f"→ {target_df['queue_manager_name'].nunique()} target",
              delta_color="off")
    b2.metric("Shared QMs (Current)", shared_qm_count,
              delta="→ 0 target", delta_color="inverse")
    b3.metric("Target QMs",    target_df["queue_manager_name"].nunique())
    b4.metric("Target Queues", len(target_df))

    st.markdown("---")
    view_t = st.radio(
        "Target View",
        ["QM-Level Network", "App-Level Network", "Channel List"],
        horizontal=True, key="tv",
    )

    if view_t == "QM-Level Network":
        G_tqm = build_qm_graph(target_df)
        st_pyvis(G_tqm, height=620, physics=False)   # target is clean – static layout looks better

    elif view_t == "App-Level Network":
        G_tapp = build_app_graph(target_df)
        st_pyvis(G_tapp, height=620, physics=False)

    else:
        channels = {
            (row["queue_manager_name"].strip(), row.get("remote_q_mgr_name", "").strip())
            for _, row in target_df[target_df["q_type"] == "Remote"].iterrows()
            if row["queue_manager_name"].strip() and row.get("remote_q_mgr_name", "").strip()
        }
        ch_df = pd.DataFrame(sorted(channels), columns=["Sender QM", "Receiver QM"])
        ch_df["Sender Channel"]   = ch_df["Sender QM"] + "." + ch_df["Receiver QM"]
        ch_df["Receiver Channel"] = ch_df["Sender QM"] + "." + ch_df["Receiver QM"]
        st.dataframe(ch_df, use_container_width=True)
        st.info(f"Total channel pairs: {len(ch_df)}")

    st.markdown("---")
    st.subheader("Generated Target Data")
    st.dataframe(target_df, use_container_width=True, height=300)

    csv_t = target_df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download Target CSV", csv_t,
                       "target_mq_architecture.csv", "text/csv")
