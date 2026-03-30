"""Reusable Plotly chart builders."""

from __future__ import annotations

from collections import defaultdict

import networkx as nx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from .constants import COMPLEXITY_THRESHOLDS, NEIGHBORHOOD_COLORS


# ─────────────────────────────────────────────────────────────────────────────
# NETWORK GRAPH
# ─────────────────────────────────────────────────────────────────────────────

_FALLBACK_PALETTE = [
    "#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4", "#FFEAA7",
    "#DDA0DD", "#98D8C8", "#F7DC6F", "#BB8FCE", "#82E0AA",
]


def create_network_graph(
    G: nx.DiGraph,
    title: str,
    highlight_nodes: set[str] | None = None,
) -> go.Figure:
    """
    Render a directed NetworkX graph with Plotly.

    Nodes are coloured by neighbourhood and sized by degree.
    Pass *highlight_nodes* to override colours (used for migration view).
    For graphs > 50 nodes the spring layout uses higher k to reduce overlap.
    """
    if len(G.nodes) == 0:
        fig = go.Figure()
        fig.update_layout(title=title, height=500)
        return fig

    k_val = 2.5 if len(G.nodes) > 20 else 1.5
    try:
        pos = nx.spring_layout(G, k=k_val, iterations=50, seed=42)
    except Exception:
        pos = nx.random_layout(G, seed=42)

    # neighbourhood → colour
    all_nbs = sorted({G.nodes[n].get("neighborhood", "Unknown") for n in G.nodes})
    nb_color: dict[str, str] = {}
    for i, nb in enumerate(all_nbs):
        nb_color[nb] = NEIGHBORHOOD_COLORS.get(nb, _FALLBACK_PALETTE[i % len(_FALLBACK_PALETTE)])

    # Edge trace
    ex, ey = [], []
    for u, v in G.edges():
        x0, y0 = pos[u]
        x1, y1 = pos[v]
        ex += [x0, x1, None]
        ey += [y0, y1, None]

    edge_trace = go.Scatter(
        x=ex, y=ey, mode="lines",
        line=dict(width=1, color="#888"),
        hoverinfo="none", showlegend=False,
    )

    # One node trace per neighbourhood (for legend grouping)
    nb_nodes: dict[str, list] = defaultdict(list)
    for node in G.nodes():
        nb = G.nodes[node].get("neighborhood", "Unknown")
        nb_nodes[nb].append(node)

    traces: list[go.BaseTraceType] = [edge_trace]
    for nb, nodes in sorted(nb_nodes.items()):
        x_vals, y_vals, sizes, hovers, labels = [], [], [], [], []
        for node in nodes:
            x, y = pos[node]
            x_vals.append(x)
            y_vals.append(y)
            deg = G.degree(node)
            sizes.append(max(12, min(40, 10 + deg * 4)))
            apps = G.nodes[node].get("apps", [node])
            hovers.append(
                f"<b>{node}</b><br>"
                f"Neighborhood: {nb}<br>"
                f"Apps: {G.nodes[node].get('num_apps', 1)}<br>"
                f"Connections: {deg}<br>"
                f"App IDs: {', '.join(apps) if isinstance(apps, list) else apps}"
            )
            labels.append(node)

        colour = nb_color.get(nb, "#888")
        if highlight_nodes:
            node_colours = ["#FF0000" if n in highlight_nodes else colour for n in nodes]
            node_sizes   = [sizes[i] * 1.5 if nodes[i] in highlight_nodes else sizes[i]
                            for i in range(len(nodes))]
        else:
            node_colours = colour
            node_sizes   = sizes

        traces.append(go.Scatter(
            x=x_vals, y=y_vals,
            mode="markers+text",
            name=nb,
            text=labels,
            textposition="top center",
            textfont=dict(size=9),
            hovertext=hovers,
            hoverinfo="text",
            marker=dict(
                size=node_sizes,
                color=node_colours,
                line=dict(width=1.5, color="white"),
            ),
        ))

    fig = go.Figure(data=traces)
    fig.update_layout(
        title=dict(text=title, font=dict(size=16)),
        showlegend=True,
        legend=dict(title="Neighborhood", orientation="v", x=1.02, y=1),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        height=580,
        margin=dict(l=20, r=150, t=50, b=20),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# HEATMAP
# ─────────────────────────────────────────────────────────────────────────────

def create_heatmap(df: pd.DataFrame, row_col: str, col_col: str, title: str) -> go.Figure:
    """Adjacency heatmap – suitable for large graphs where node diagrams become unreadable."""
    sub = df[df["q_type"] == "Remote"][[row_col, col_col]].copy()
    sub = sub[(sub[row_col].str.strip() != "") & (sub[col_col].str.strip() != "")]
    if sub.empty:
        fig = go.Figure()
        fig.update_layout(title=title)
        return fig

    pivot = sub.groupby([row_col, col_col]).size().unstack(fill_value=0)
    fig = px.imshow(
        pivot,
        text_auto=True,
        color_continuous_scale="Blues",
        title=title,
        labels=dict(x=col_col, y=row_col, color="Channel Count"),
        aspect="auto",
    )
    fig.update_layout(height=max(400, len(pivot) * 30 + 100))
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# COMPLEXITY GAUGE
# ─────────────────────────────────────────────────────────────────────────────

def complexity_gauge(score: float, label: str) -> go.Figure:
    lo, med, hi = (COMPLEXITY_THRESHOLDS[k] for k in ("low", "medium", "high"))
    if score < lo:
        colour = "#28a745"
    elif score < med:
        colour = "#ffc107"
    elif score < hi:
        colour = "#fd7e14"
    else:
        colour = "#dc3545"

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        title={"text": label},
        gauge={
            "axis": {"range": [0, 300]},
            "bar":  {"color": colour},
            "steps": [
                {"range": [0,   lo],  "color": "#d4edda"},
                {"range": [lo,  med], "color": "#fff3cd"},
                {"range": [med, hi],  "color": "#fde8d8"},
                {"range": [hi,  300], "color": "#f8d7da"},
            ],
            "threshold": {
                "line": {"color": "red", "width": 3},
                "thickness": 0.75,
                "value": score,
            },
        },
    ))
    fig.update_layout(height=280, margin=dict(l=30, r=30, t=60, b=20))
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# COMPLEXITY RADAR
# ─────────────────────────────────────────────────────────────────────────────

def complexity_radar(curr: dict, tgt: dict) -> go.Figure:
    dims   = ["num_qms", "shared_qms", "num_channels", "max_fan_out", "max_fan_in", "apps_per_qm"]
    labels = ["QM Count", "Shared QMs", "Channels", "Max Fan-Out", "Max Fan-In", "Apps/QM"]

    def _normalise(vals: list[float]) -> list[float]:
        mx = max((v for v in vals if v > 0), default=1)
        return [v / mx * 10 for v in vals]

    curr_n = _normalise([curr[d] for d in dims])
    tgt_n  = _normalise([tgt[d]  for d in dims])

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=curr_n + [curr_n[0]], theta=labels + [labels[0]],
        fill="toself", name="Current", line_color="#dc3545",
    ))
    fig.add_trace(go.Scatterpolar(
        r=tgt_n + [tgt_n[0]], theta=labels + [labels[0]],
        fill="toself", name="Target", line_color="#28a745", opacity=0.7,
    ))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 10])),
        showlegend=True,
        title="Complexity Dimensions (normalised)",
        height=420,
    )
    return fig
