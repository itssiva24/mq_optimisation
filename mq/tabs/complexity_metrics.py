"""Tab 4 – Complexity Metrics."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from ..architecture import compute_complexity
from ..charts import complexity_gauge, complexity_radar


_COMPLEXITY_DIMS = {
    "QM Count":     ("num_qms",      "Number of Queue Managers"),
    "Shared QMs":   ("shared_qms",   "QMs hosting > 1 application"),
    "Channels":     ("num_channels", "Unique QM-to-QM channel pairs"),
    "Apps/QM":      ("apps_per_qm",  "Average apps per QM"),
    "Max Fan-Out":  ("max_fan_out",  "Max outgoing channels from one QM"),
    "Max Fan-In":   ("max_fan_in",   "Max incoming channels to one QM"),
    "XmitQs":       ("num_xmitqs",  "Transmission queues"),
    "Total Queues": ("num_queues",   "Total queue objects"),
}


def _level_label(score: float) -> str:
    if score < 50:  return "🟢 Low"
    if score < 100: return "🟡 Medium"
    if score < 200: return "🟠 High"
    return "🔴 Critical"


def render(df: pd.DataFrame) -> None:
    st.header("Complexity Metrics")
    st.markdown(
        "Quantify the architectural complexity of current vs target configuration "
        "across multiple dimensions."
    )

    curr_c = compute_complexity(df, "Current")
    tgt_c  = (compute_complexity(st.session_state["target_df"], "Target")
              if "target_df" in st.session_state else None)

    if tgt_c is None:
        st.warning("Generate the target architecture in **Tab 3** first to see comparisons.")

    g1, g2 = st.columns(2)
    with g1:
        st.plotly_chart(complexity_gauge(curr_c["complexity_score"], "Current Complexity Score"),
                        use_container_width=True)
        st.markdown(f"**Level:** {_level_label(curr_c['complexity_score'])}")

    with g2:
        if tgt_c:
            st.plotly_chart(complexity_gauge(tgt_c["complexity_score"], "Target Complexity Score"),
                            use_container_width=True)
            st.markdown(f"**Level:** {_level_label(tgt_c['complexity_score'])}")
        else:
            st.info("Target not yet generated.")

    if not tgt_c:
        st.markdown("---")
        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("QM Count",    curr_c["num_qms"])
        mc2.metric("Shared QMs",  curr_c["shared_qms"])
        mc3.metric("Channels",    curr_c["num_channels"])
        mc4.metric("Max Fan-Out", curr_c["max_fan_out"])
        return

    st.markdown("---")
    st.subheader("Dimension Comparison")
    rows = []
    for display, (key, desc) in _COMPLEXITY_DIMS.items():
        c_val = curr_c[key]
        t_val = tgt_c[key]
        delta = t_val - c_val
        pct   = f"{delta / max(c_val, 1) * 100:+.0f}%" if c_val else "N/A"
        rows.append({"Metric": display, "Current": c_val, "Target": t_val,
                     "Change": delta, "% Change": pct, "Description": desc})
    st.dataframe(pd.DataFrame(rows), use_container_width=True)

    st.markdown("---")
    col_r, col_b = st.columns(2)

    with col_r:
        st.plotly_chart(complexity_radar(curr_c, tgt_c), use_container_width=True)

    with col_b:
        bar_data = []
        for display, (key, _) in _COMPLEXITY_DIMS.items():
            bar_data.append({"Metric": display, "Value": curr_c[key], "State": "Current"})
            bar_data.append({"Metric": display, "Value": tgt_c[key],  "State": "Target"})
        fig = px.bar(
            pd.DataFrame(bar_data),
            x="Metric", y="Value", color="State", barmode="group",
            title="Current vs Target – All Dimensions",
            color_discrete_map={"Current": "#dc3545", "Target": "#28a745"},
        )
        fig.update_layout(xaxis_tickangle=-30, height=420)
        st.plotly_chart(fig, use_container_width=True)
