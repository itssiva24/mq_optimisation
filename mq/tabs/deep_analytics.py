"""
Tab: Deep Analytics

Novel visualisations and metrics that go beyond standard MQ tooling:

  1.  Architecture Debt – waterfall breakdown
  2.  Neighbourhood Coupling – Sankey diagram
  3.  Flow Risk Matrix – business criticality × structural risk scatter
  4.  QM Centrality – betweenness centrality bar chart
  5.  Cross-Neighbourhood Hop Distribution – histogram + table
  6.  Blast Radius Explorer – select QM, see direct + transitive impact
  7.  Migration Wave Planner – topological ordering
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from ..analytics import (
    compute_architecture_debt,
    compute_blast_radius,
    cross_neighborhood_hops,
    flow_risk_matrix,
    migration_wave_planner,
    neighborhood_coupling_matrix,
    qm_centrality_analysis,
)
from ..constants import NEIGHBORHOOD_COLORS, TRTC_LABELS
from ..graph import build_qm_graph
from ..network_viz import st_pyvis


# ─────────────────────────────────────────────────────────────────────────────
# SECTION HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _section(title: str, icon: str = "") -> None:
    st.markdown(
        f"<div style='margin-top:2rem;padding:0.4rem 0 0.3rem 0;"
        f"border-bottom:2px solid #0f62fe'>"
        f"<span style='font-size:1.15rem;font-weight:600'>{icon} {title}</span>"
        f"</div>",
        unsafe_allow_html=True,
    )


def _badge(label: str, value, color: str = "#0f62fe") -> str:
    return (
        f"<span style='background:{color};color:#fff;padding:2px 10px;"
        f"border-radius:12px;font-size:0.82rem;margin-right:6px'>"
        f"<b>{label}:</b> {value}</span>"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 1. ARCHITECTURE DEBT
# ─────────────────────────────────────────────────────────────────────────────

def _architecture_debt(df: pd.DataFrame) -> None:
    _section("Architecture Debt Score", "🏦")
    st.markdown(
        "Quantifies how far the current environment deviates from the target "
        "1-QM-per-app design across five independent cost dimensions."
    )

    debt = compute_architecture_debt(df)
    total = debt["total"]

    # Color thresholds
    color = "#42be65" if total < 50 else ("#f1c21b" if total < 150 else "#fa4d56")
    grade = "A" if total < 50 else ("B" if total < 100 else ("C" if total < 150 else "D"))

    g1, g2 = st.columns([1, 2])
    with g1:
        st.markdown(
            f"<div style='text-align:center;padding:24px;background:#1e1e2e;"
            f"border-radius:12px;border:2px solid {color}'>"
            f"<div style='font-size:3.5rem;font-weight:700;color:{color}'>{grade}</div>"
            f"<div style='font-size:1.8rem;color:{color}'>{total}</div>"
            f"<div style='color:#aaa;font-size:0.85rem'>Architecture Debt Score</div>"
            f"</div>",
            unsafe_allow_html=True,
        )

    with g2:
        dims = {
            "Shared QM":     debt["shared_qm"],
            "Cross-NB":      debt["cross_nb"],
            "Relay Hop":     debt["relay_hop"],
            "Channel Fan":   debt["channel_fan"],
            "Naming":        debt["naming"],
        }
        fig = go.Figure(go.Waterfall(
            orientation="v",
            measure=["relative"] * len(dims) + ["total"],
            x=list(dims.keys()) + ["TOTAL"],
            y=list(dims.values()) + [0],
            text=[f"+{v}" for v in dims.values()] + [str(total)],
            textposition="outside",
            connector={"line": {"color": "#444"}},
            increasing={"marker": {"color": "#fa4d56"}},
            totals={"marker": {"color": "#0f62fe"}},
        ))
        fig.update_layout(
            title="Debt by Component",
            height=320,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font={"color": "#f4f4f4"},
            margin=dict(l=10, r=10, t=40, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)

    with st.expander("What do these components mean?"):
        st.markdown("""
| Component | What it measures | Fix |
|-----------|-----------------|-----|
| **Shared QM** | Each extra app on a QM (+15 pts each) | 1 QM per app |
| **Cross-NB** | Channels crossing datacenter boundaries (+3/+5 critical) | Co-locate communicating apps or accept explicit boundary |
| **Relay Hop** | QMs acting as pure message relays (+4 each) | Direct sender→receiver channels |
| **Channel Fan** | Fan-out > 4 per QM (+2 per extra channel) | Reduce producer scope |
| **Naming** | XmitQ names not following `{dest}.XMIT` (+2 each) | Enforce naming standard |
""")


# ─────────────────────────────────────────────────────────────────────────────
# 2. NEIGHBOURHOOD COUPLING SANKEY
# ─────────────────────────────────────────────────────────────────────────────

def _neighbourhood_coupling(df: pd.DataFrame) -> None:
    _section("Neighbourhood Coupling", "🌐")
    st.markdown(
        "A Sankey diagram showing how many message flows cross each datacenter / "
        "neighbourhood boundary. Thick bands indicate tight cross-DC coupling that "
        "increases latency, blast radius, and migration complexity."
    )

    matrix = neighborhood_coupling_matrix(df)
    if matrix.empty:
        st.info("No cross-neighbourhood flows found in current dataset.")
        return

    all_labels = sorted(set(matrix["source"]) | set(matrix["target"]))
    label_idx  = {l: i for i, l in enumerate(all_labels)}
    colors     = [NEIGHBORHOOD_COLORS.get(l, "#888") for l in all_labels]

    fig = go.Figure(go.Sankey(
        arrangement="snap",
        node=dict(
            pad=20, thickness=22,
            line=dict(color="#333", width=0.5),
            label=all_labels,
            color=colors,
            hovertemplate="%{label}<br>Total flows: %{value}<extra></extra>",
        ),
        link=dict(
            source=[label_idx[r["source"]] for _, r in matrix.iterrows()],
            target=[label_idx[r["target"]] for _, r in matrix.iterrows()],
            value=matrix["flow_count"].tolist(),
            color=["rgba(15,98,254,0.25)"] * len(matrix),
            hovertemplate="%{source.label} → %{target.label}<br>Flows: %{value}<extra></extra>",
        ),
    ))
    fig.update_layout(
        height=440,
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "#f4f4f4", "size": 12},
        margin=dict(l=10, r=10, t=30, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

    c1, c2 = st.columns(2)
    c1.metric("Cross-NB Flows", int(matrix["flow_count"].sum()))
    c2.metric("Coupled Neighbourhood Pairs", len(matrix))


# ─────────────────────────────────────────────────────────────────────────────
# 3. FLOW RISK MATRIX
# ─────────────────────────────────────────────────────────────────────────────

def _flow_risk_matrix(df: pd.DataFrame) -> None:
    _section("Flow Risk Matrix", "⚠️")
    st.markdown(
        "Each message flow is scored on two axes. "
        "**Top-right quadrant** = high business criticality AND high structural risk "
        "= highest priority to fix."
    )

    risk_df = flow_risk_matrix(df)
    if risk_df.empty:
        st.info("No flow risk data available.")
        return

    # Jitter so overlapping points are visible
    import numpy as np
    rng = np.random.default_rng(42)
    risk_df = risk_df.copy()
    risk_df["x_jitter"] = risk_df["structural_risk"]  + rng.uniform(-0.15, 0.15, len(risk_df))
    risk_df["y_jitter"] = risk_df["business_criticality"] + rng.uniform(-0.15, 0.15, len(risk_df))

    trtc_label = {"00": "Critical (TRTC 00)", "02": "High (TRTC 02)", "03": "Normal (TRTC 03)"}
    risk_df["trtc_label"] = risk_df["trtc"].map(trtc_label).fillna("Normal")

    fig = px.scatter(
        risk_df,
        x="x_jitter", y="y_jitter",
        color="prod_neighborhood",
        color_discrete_map=NEIGHBORHOOD_COLORS,
        size="risk_score",
        size_max=22,
        hover_data={
            "queue_name": True,
            "prod_app": True,
            "trtc_label": True,
            "is_cross_nb": True,
            "x_jitter": False,
            "y_jitter": False,
        },
        labels={
            "x_jitter": "Structural Risk  (1=same-NB  2=cross-NB  3=shared+cross)",
            "y_jitter": "Business Criticality  (1=Normal  2=High  3=Critical)",
        },
        title="Flow Risk Matrix",
    )

    # Quadrant shading
    fig.add_shape(type="rect", x0=1.5, x1=3.5, y0=1.5, y1=3.5,
                  fillcolor="rgba(250,77,86,0.08)", line=dict(width=0))
    fig.add_annotation(x=3.1, y=3.3, text="⚠️ High Priority",
                       showarrow=False, font=dict(color="#fa4d56", size=11))

    fig.update_layout(
        height=480,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.03)",
        font={"color": "#f4f4f4"},
        margin=dict(l=10, r=10, t=50, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

    top_risk = risk_df.nlargest(5, "risk_score")[
        ["queue_name", "prod_app", "trtc_label", "prod_neighborhood",
         "cons_neighborhood", "is_cross_nb", "risk_score"]
    ]
    st.caption("Top 5 highest-risk flows")
    st.dataframe(top_risk, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# 4. QM CENTRALITY
# ─────────────────────────────────────────────────────────────────────────────

def _qm_centrality(df: pd.DataFrame) -> None:
    _section("QM Centrality & Single-Point-of-Failure Risk", "🎯")
    st.markdown(
        "**Betweenness centrality** measures how often a QM lies on the shortest "
        "path between other QMs. High centrality = critical relay = highest-risk "
        "single point of failure."
    )

    G   = build_qm_graph(df)
    cdf = qm_centrality_analysis(G)
    if cdf.empty:
        st.info("No centrality data available.")
        return

    fig = px.bar(
        cdf.sort_values("risk_score", ascending=True),
        x="risk_score", y="qm", orientation="h",
        color="neighborhood", color_discrete_map=NEIGHBORHOOD_COLORS,
        hover_data=["betweenness", "num_apps", "in_degree", "out_degree"],
        labels={"risk_score": "Risk Score", "qm": "Queue Manager"},
        title="QM Risk Score (betweenness × apps)",
    )
    fig.update_layout(
        height=max(350, len(cdf) * 28 + 80),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.03)",
        font={"color": "#f4f4f4"},
        yaxis={"categoryorder": "total ascending"},
        margin=dict(l=10, r=10, t=50, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

    top3 = cdf.head(3)
    if not top3.empty:
        st.markdown(
            "**Highest-risk QMs to decompose first:**  "
            + "  ·  ".join(
                f"**{r['qm']}** (score {r['risk_score']}, {r['num_apps']} apps)"
                for _, r in top3.iterrows()
            )
        )

    with st.expander("Full centrality table"):
        st.dataframe(cdf, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# 5. CROSS-NB HOP DISTRIBUTION
# ─────────────────────────────────────────────────────────────────────────────

def _cross_nb_hops(df: pd.DataFrame) -> None:
    _section("Cross-Neighbourhood Channel Analysis", "🛣️")
    st.markdown(
        "Every channel that crosses a neighbourhood boundary adds latency, increases "
        "blast radius, and complicates data-centre migrations."
    )

    hops = cross_neighborhood_hops(df)
    if hops.empty:
        st.info("No hop data available.")
        return

    n_cross  = int(hops["is_cross_neighborhood"].sum())
    n_total  = len(hops)
    n_same   = n_total - n_cross

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Channels", n_total)
    c2.metric("Cross-NB Channels", n_cross,
              delta=f"{n_cross/max(n_total,1)*100:.0f}% of total",
              delta_color="inverse")
    c3.metric("Same-NB Channels", n_same)

    col_a, col_b = st.columns(2)

    with col_a:
        pie_data = pd.DataFrame({
            "Type":  ["Same Neighbourhood", "Cross Neighbourhood"],
            "Count": [n_same, n_cross],
        })
        fig = px.pie(
            pie_data, names="Type", values="Count",
            color="Type",
            color_discrete_map={
                "Same Neighbourhood":  "#42be65",
                "Cross Neighbourhood": "#fa4d56",
            },
            title="Channel Type Distribution",
        )
        fig.update_layout(height=300, paper_bgcolor="rgba(0,0,0,0)",
                          font={"color": "#f4f4f4"})
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        if n_cross > 0:
            cross_df = hops[hops["is_cross_neighborhood"]].copy()
            pair_counts = (
                cross_df.groupby(["prod_neighborhood", "cons_neighborhood"])
                        .size().reset_index(name="count")
            )
            pair_counts["pair"] = (
                pair_counts["prod_neighborhood"] + " → " + pair_counts["cons_neighborhood"]
            )
            fig = px.bar(
                pair_counts.sort_values("count", ascending=False),
                x="pair", y="count",
                title="Cross-NB Pairs",
                labels={"pair": "NB Pair", "count": "Channels"},
                color="count", color_continuous_scale="Reds",
            )
            fig.update_layout(height=300, paper_bgcolor="rgba(0,0,0,0)",
                              font={"color": "#f4f4f4"}, showlegend=False,
                              xaxis_tickangle=-25)
            st.plotly_chart(fig, use_container_width=True)

    with st.expander("Cross-neighbourhood channel detail"):
        show = hops[hops["is_cross_neighborhood"]][
            ["queue_name", "prod_app", "prod_qm", "cons_qm",
             "prod_neighborhood", "cons_neighborhood", "trtc"]
        ]
        st.dataframe(show, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# 6. BLAST RADIUS EXPLORER
# ─────────────────────────────────────────────────────────────────────────────

def _blast_radius(df: pd.DataFrame) -> None:
    _section("Blast Radius Explorer", "💥")
    st.markdown(
        "Select a QM to model the failure. "
        "**Direct** apps are hosted on the QM. "
        "**Transitive** apps on other QMs that will be starved of messages "
        "because a dependency was cut."
    )

    qm_list = sorted(df["queue_manager_name"].dropna().unique().tolist())
    qm_sel  = st.selectbox("Model failure of QM:", qm_list, key="blast_qm")

    if not qm_sel:
        return

    br = compute_blast_radius(df, qm_sel)

    b1, b2, b3, b4 = st.columns(4)
    b1.metric("Direct Apps Impacted",     len(br["direct_apps"]))
    b2.metric("Transitive Apps Impacted", len(br["transitive_apps"]))
    b3.metric("Dead Flows",               len(br["dead_flows"]),
              delta=f"{br['blast_pct']:.0f}% of total", delta_color="inverse")
    b4.metric("Business Criticality Impact", f"{br['weighted_impact']:.0f}%",
              delta="TRTC-weighted", delta_color="inverse")

    # Sunburst: centre = failed QM, ring 1 = direct apps, ring 2 = transitive
    labels = [qm_sel] + br["direct_apps"] + br["transitive_apps"]
    parents = (
        [""]
        + [qm_sel] * len(br["direct_apps"])
        + [
            (br["direct_apps"][0] if br["direct_apps"] else qm_sel)
            for _ in br["transitive_apps"]
        ]
    )
    colors = (
        ["#fa4d56"]
        + ["#ff832b"] * len(br["direct_apps"])
        + ["#f1c21b"] * len(br["transitive_apps"])
    )

    fig = go.Figure(go.Sunburst(
        labels=labels,
        parents=parents,
        marker=dict(colors=colors),
        hovertemplate="<b>%{label}</b><br>Parent: %{parent}<extra></extra>",
        branchvalues="remainder",
    ))
    fig.update_layout(
        title=f"Blast Radius – {qm_sel} failure",
        height=420,
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "#f4f4f4"},
        margin=dict(l=0, r=0, t=50, b=0),
    )
    st.plotly_chart(fig, use_container_width=True)

    if br["dead_flows"]:
        with st.expander(f"Dead flows ({len(br['dead_flows'])})"):
            df_dead = pd.DataFrame([{
                "Queue":     f["queue_name"],
                "Producer":  f["prod_app"],
                "Consumer":  f["cons_app"],
                "Prod QM":   f["prod_qm"],
                "Cons QM":   f["cons_qm"],
                "Priority":  ("🔴" if f["trtc"] == "00"
                              else "🟠" if f["trtc"] == "02"
                              else "🟡") + " " + TRTC_LABELS.get(f["trtc"], f["trtc"]),
            } for f in sorted(br["dead_flows"], key=lambda x: x["trtc"])])
            st.dataframe(df_dead, use_container_width=True)

    # Interactive network: highlight the failed QM + affected QMs
    with st.expander("Show impact on network graph"):
        G = build_qm_graph(df)
        affected_qms = {qm_sel}
        for f in br["dead_flows"]:
            affected_qms.add(f["prod_qm"])
            affected_qms.add(f["cons_qm"])
        st_pyvis(G, height=500, highlight_nodes=affected_qms, physics=False)


# ─────────────────────────────────────────────────────────────────────────────
# 7. MIGRATION WAVE PLANNER
# ─────────────────────────────────────────────────────────────────────────────

def _migration_waves(df: pd.DataFrame) -> None:
    _section("Migration Wave Planner", "🌊")
    st.markdown(
        "Determines the **optimal order** to migrate apps to their dedicated QMs "
        "using topological sort. Apps in Wave 1 have no upstream dependencies and can "
        "migrate independently. Each subsequent wave only depends on already-migrated apps."
    )

    result = migration_wave_planner(df)
    waves  = result["waves"]

    if result["cycles_removed"]:
        st.warning(
            f"{len(result['cycles_removed'])} dependency cycle(s) were broken to "
            f"produce a migration order: "
            + ", ".join(f"{u}→{v}" for u, v in result["cycles_removed"])
        )

    w1, w2, w3 = st.columns(3)
    w1.metric("Total Migration Waves", result["total_waves"])
    w2.metric("Total Apps",            result["total_apps"])
    w3.metric("Cycles Broken",         len(result["cycles_removed"]))

    st.markdown("---")
    wave_rows = []
    for i, wave in enumerate(waves, 1):
        for app in wave:
            wave_rows.append({"Wave": i, "App ID": app})
    wave_df = pd.DataFrame(wave_rows)

    fig = px.scatter(
        wave_df,
        x="Wave", y="App ID",
        color="Wave",
        color_continuous_scale="Blues",
        title="Migration Wave Assignment",
        labels={"Wave": "Migration Wave", "App ID": "Application"},
        size_max=14,
    )
    fig.update_traces(marker=dict(size=14, symbol="square"))
    fig.update_layout(
        height=max(350, len(set(wave_df["App ID"])) * 22 + 80),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.03)",
        font={"color": "#f4f4f4"},
        margin=dict(l=10, r=10, t=50, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

    for i, wave in enumerate(waves, 1):
        with st.expander(f"Wave {i} – {len(wave)} app(s)"):
            st.markdown("  ·  ".join(f"`{a}`" for a in wave))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN RENDER
# ─────────────────────────────────────────────────────────────────────────────

def render(df: pd.DataFrame) -> None:
    st.header("Deep Analytics")
    st.markdown(
        "Novel intelligence beyond standard MQ tooling — architecture debt scoring, "
        "cross-datacenter coupling, flow risk prioritisation, and automated migration planning."
    )

    _architecture_debt(df)
    st.markdown("<br>", unsafe_allow_html=True)
    _neighbourhood_coupling(df)
    st.markdown("<br>", unsafe_allow_html=True)
    _flow_risk_matrix(df)
    st.markdown("<br>", unsafe_allow_html=True)
    _qm_centrality(df)
    st.markdown("<br>", unsafe_allow_html=True)
    _cross_nb_hops(df)
    st.markdown("<br>", unsafe_allow_html=True)
    _blast_radius(df)
    st.markdown("<br>", unsafe_allow_html=True)
    _migration_waves(df)
