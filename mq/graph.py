"""NetworkX graph builders for QM-level and App-level topologies."""

from __future__ import annotations

from collections import defaultdict

import networkx as nx
import pandas as pd

from .data import extract_flows, neighborhood_col


def build_qm_graph(df: pd.DataFrame) -> nx.DiGraph:
    """
    Build a directed graph where:
      - nodes  = Queue Managers
      - edges  = sender/receiver channel pairs derived from Remote queue rows
      - weight = number of Remote queues using that channel
    """
    G = nx.DiGraph()
    nb_col = neighborhood_col(df)

    qm_apps: dict[str, set] = defaultdict(set)
    qm_neighborhood: dict[str, str] = {}

    for _, row in df.iterrows():
        qm = row["queue_manager_name"].strip()
        if qm:
            qm_apps[qm].add(row["app_id"].strip())
            qm_neighborhood.setdefault(qm, row.get(nb_col, "").strip())

    for qm, apps in qm_apps.items():
        G.add_node(qm, apps=sorted(apps), num_apps=len(apps),
                   neighborhood=qm_neighborhood.get(qm, "Unknown"))

    for _, row in df[df["q_type"] == "Remote"].iterrows():
        src = row["queue_manager_name"].strip()
        dst = row.get("remote_q_mgr_name", "").strip()
        if src and dst and src != dst:
            if G.has_edge(src, dst):
                G[src][dst]["weight"] += 1
            else:
                G.add_edge(src, dst, weight=1)
    return G


def build_app_graph(df: pd.DataFrame) -> nx.DiGraph:
    """
    Build a directed graph where:
      - nodes = Applications (identified by app_id / full name)
      - edges = message flows between apps
    """
    G = nx.DiGraph()
    nb_col = neighborhood_col(df)

    app_meta: dict[str, dict] = {}
    for _, row in df.iterrows():
        aid = row["app_id"].strip()
        if aid and aid not in app_meta:
            app_meta[aid] = {
                "neighborhood": row.get(nb_col, "").strip(),
                "name":         row.get("Primary App_Full_Name",
                                        row.get("ProducerName", aid)).strip(),
                "qm":           row["queue_manager_name"].strip(),
                "lob":          row.get("line_of_business", "").strip(),
            }

    for meta in app_meta.values():
        G.add_node(meta["name"], **meta)

    for flow in extract_flows(df):
        src = app_meta.get(flow["prod_app"], {}).get("name", flow["prod_app"])
        dst = app_meta.get(flow["cons_app"], {}).get("name", flow["cons_app"])
        if src and dst:
            if G.has_edge(src, dst):
                G[src][dst]["weight"] += 1
            else:
                G.add_edge(src, dst, weight=1, queue=flow["queue_name"])
    return G
