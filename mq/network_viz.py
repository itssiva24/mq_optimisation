"""
Production-grade interactive network visualisation using pyvis / vis.js.

pyvis generates a self-contained HTML page backed by vis.js (WebGL-accelerated).
This scales to thousands of nodes and edges with smooth physics simulation,
giving a far better experience than static Plotly scatter charts for graph data.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import networkx as nx
import streamlit as st
import streamlit.components.v1 as components

from .constants import NEIGHBORHOOD_COLORS

try:
    from pyvis.network import Network as PyvisNetwork
    _PYVIS_AVAILABLE = True
except ImportError:
    _PYVIS_AVAILABLE = False


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

_NODE_BORDER = "#3d3d3d"
_EDGE_COLOR  = "#444466"
_EDGE_HOVER  = "#0f62fe"
_BG          = "#0d1117"


def _node_color(nb: str, highlight: bool = False) -> dict:
    base = NEIGHBORHOOD_COLORS.get(nb, "#58a6ff") if not highlight else "#ff4444"
    return {
        "background":  base,
        "border":      "#ffffff" if highlight else _NODE_BORDER,
        "highlight": {"background": "#ffcc00", "border": "#ffffff"},
        "hover":     {"background": "#ffcc00", "border": "#ffffff"},
    }


def _edge_config(weight: int = 1) -> dict:
    return {
        "color":  {"color": _EDGE_COLOR, "highlight": _EDGE_HOVER, "hover": _EDGE_HOVER},
        "width":  max(1, min(8, weight)),
        "arrows": {"to": {"enabled": True, "scaleFactor": 0.6}},
        "smooth": {"type": "dynamic"},
        "shadow": False,
    }


# ─────────────────────────────────────────────────────────────────────────────
# MAIN BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_pyvis(
    G: nx.DiGraph,
    *,
    height: int = 640,
    highlight_nodes: set[str] | None = None,
    physics: bool = True,
    show_controls: bool = False,
    node_label_field: str = "label",          # node attr to use as label (default = node id)
) -> str:
    """
    Convert a NetworkX DiGraph to a pyvis HTML string.

    Parameters
    ----------
    G               :  directed graph; node attrs: neighborhood, num_apps, apps, name
    height          :  iframe height in pixels
    highlight_nodes :  set of node ids to colour red (migration / failure views)
    physics         :  enable Barnes-Hut physics simulation
    show_controls   :  show vis.js control panel
    """
    if not _PYVIS_AVAILABLE:
        return "<p style='color:#fa4d56'>pyvis not installed – run <code>pip install pyvis</code></p>"

    net = PyvisNetwork(
        height=f"{height}px",
        width="100%",
        bgcolor=_BG,
        font_color="#c9d1d9",
        directed=True,
        notebook=False,
        cdn_resources="remote",
    )

    if physics:
        net.set_options("""
        {
          "physics": {
            "barnesHut": {
              "gravitationalConstant": -12000,
              "centralGravity": 0.25,
              "springLength": 220,
              "springConstant": 0.04,
              "damping": 0.12,
              "avoidOverlap": 0.4
            },
            "maxVelocity": 50,
            "minVelocity": 0.75,
            "stabilization": { "iterations": 150 }
          },
          "edges": { "font": { "size": 10, "color": "#888" } },
          "interaction": {
            "hover": true,
            "navigationButtons": true,
            "keyboard": { "enabled": true }
          }
        }
        """)
    else:
        net.toggle_physics(False)

    for node in G.nodes():
        attrs  = G.nodes[node]
        nb     = attrs.get("neighborhood", "Unknown")
        apps   = attrs.get("apps", [node])
        deg    = G.degree(node)
        size   = max(16, min(55, 12 + deg * 5))
        label  = attrs.get(node_label_field, node) if node_label_field != "label" else node
        is_hl  = highlight_nodes is not None and node in highlight_nodes

        if is_hl:
            size = int(size * 1.6)

        app_list = ", ".join(apps) if isinstance(apps, list) else str(apps)
        title_html = (
            f'<div style="font-family:monospace;padding:6px 10px;'
            f'background:#1c1c2e;border-radius:6px;border:1px solid #333;">'
            f'<b style="color:{NEIGHBORHOOD_COLORS.get(nb,"#58a6ff")};'
            f'font-size:13px">{node}</b><br>'
            f'<span style="color:#888">Neighborhood:</span> {nb}<br>'
            f'<span style="color:#888">Apps:</span> {app_list}<br>'
            f'<span style="color:#888">Degree:</span> {deg}'
            f'</div>'
        )

        net.add_node(
            node,
            label=label,
            color=_node_color(nb, is_hl),
            size=size,
            title=title_html,
            font={"size": 10, "color": "#e6edf3", "face": "monospace", "bold": True},
            borderWidth=2 if is_hl else 1,
            shadow={"enabled": True, "color": "rgba(0,0,0,0.5)", "size": 8},
        )

    for u, v, data in G.edges(data=True):
        net.add_edge(u, v, **_edge_config(data.get("weight", 1)))

    if show_controls:
        net.show_buttons(filter_=["physics", "layout", "interaction"])

    html = net.generate_html()
    # Patch: remove default white background
    html = html.replace(
        "body {",
        f"body {{ background-color: {_BG}; ",
    )
    return html


# ─────────────────────────────────────────────────────────────────────────────
# STREAMLIT COMPONENT WRAPPER
# ─────────────────────────────────────────────────────────────────────────────

def st_pyvis(
    G: nx.DiGraph,
    *,
    height: int = 640,
    highlight_nodes: set[str] | None = None,
    physics: bool = True,
    show_controls: bool = False,
) -> None:
    """Render a pyvis network directly inside a Streamlit page."""
    if len(G.nodes) == 0:
        st.info("No nodes to display.")
        return

    html = build_pyvis(
        G,
        height=height,
        highlight_nodes=highlight_nodes,
        physics=physics,
        show_controls=show_controls,
    )
    components.html(html, height=height + 20, scrolling=False)
