"""Tab 1 – Data Explorer."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from ..constants import NEIGHBORHOOD_COLORS
from ..data import neighborhood_col


def render(df: pd.DataFrame) -> None:
    st.header("Data Explorer")
    st.markdown(
        "Browse the raw MQ queue object data. "
        "Each row represents a queue object configured on an IBM MQ Queue Manager."
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Rows",    f"{len(df):,}")
    c2.metric("Unique QMs",    df["queue_manager_name"].nunique())
    c3.metric("Unique Apps",   df["app_id"].nunique())
    c4.metric("Unique Queues", df["Discrete Queue Name"].nunique())

    search   = st.text_input("🔎 Search queue names", "")
    df_view  = (df[df["Discrete Queue Name"].str.contains(search, case=False, na=False)]
                if search else df)
    st.dataframe(df_view, use_container_width=True, height=320)

    st.markdown("---")
    nb_col = neighborhood_col(df)
    col_a, col_b, col_c = st.columns(3)

    with col_a:
        qm_counts = df.groupby("queue_manager_name").size().reset_index(name="count")
        fig = px.bar(
            qm_counts.sort_values("count", ascending=False),
            x="queue_manager_name", y="count",
            title="Queues per Queue Manager",
            color="count", color_continuous_scale="Blues",
            labels={"queue_manager_name": "QM", "count": "Queues"},
        )
        fig.update_layout(showlegend=False, height=350, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        nb_counts = df.groupby(nb_col).size().reset_index(name="count")
        fig = px.pie(
            nb_counts, names=nb_col, values="count",
            title="Queues by Neighborhood",
            color=nb_col, color_discrete_map=NEIGHBORHOOD_COLORS,
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col_c:
        qt_counts = df.groupby("q_type").size().reset_index(name="count")
        fig = px.bar(
            qt_counts, x="q_type", y="count",
            title="Queue Type Distribution",
            color="q_type",
            labels={"q_type": "Queue Type", "count": "Count"},
        )
        fig.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig, use_container_width=True)

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download Filtered Data", csv_bytes,
                       "filtered_mq_data.csv", "text/csv")
