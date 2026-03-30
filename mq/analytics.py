"""
Novel analytics for IBM MQ architecture intelligence.

Concepts implemented here do not appear in standard MQ tooling:
  - Cross-neighbourhood hop analysis
  - Architecture debt scoring (multi-dimensional)
  - QM blast-radius (direct + transitive)
  - Flow risk matrix (business criticality × structural risk)
  - QM betweenness centrality
  - Neighbourhood coupling matrix
  - Migration wave planner (topological sort)
"""

from __future__ import annotations

from collections import defaultdict

import networkx as nx
import numpy as np
import pandas as pd

from .data import extract_flows, neighborhood_col


# ─────────────────────────────────────────────────────────────────────────────
# CROSS-NEIGHBOURHOOD HOP ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

def qm_neighborhood_map(df: pd.DataFrame) -> dict[str, str]:
    """Map each QM to its primary (most frequent) neighbourhood."""
    nb_col = neighborhood_col(df)
    return (
        df[df["queue_manager_name"].str.strip() != ""]
        .groupby("queue_manager_name")[nb_col]
        .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else "Unknown")
        .to_dict()
    )


def cross_neighborhood_hops(df: pd.DataFrame) -> pd.DataFrame:
    """
    For every Remote-queue row (= one directed channel between two QMs) determine
    whether the channel crosses a neighbourhood boundary.

    Returns one row per Remote queue with:
      prod_qm, cons_qm, prod_neighborhood, cons_neighborhood,
      is_cross_neighborhood, trtc, lob, queue_name, prod_app
    """
    nb_col   = neighborhood_col(df)
    qm_nb    = qm_neighborhood_map(df)
    app_nb   = (
        df[df["app_id"].str.strip() != ""]
        .groupby("app_id")[nb_col]
        .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else "Unknown")
        .to_dict()
    )

    rows = []
    for _, row in df[df["q_type"] == "Remote"].iterrows():
        prod_qm  = row["queue_manager_name"].strip()
        cons_qm  = row.get("remote_q_mgr_name", "").strip()
        prod_nb  = qm_nb.get(prod_qm, "Unknown")
        cons_nb  = qm_nb.get(cons_qm, "Unknown")
        is_cross = prod_nb != cons_nb and "Unknown" not in (prod_nb, cons_nb)
        rows.append({
            "queue_name":        row["Discrete Queue Name"],
            "prod_app":          row["app_id"].strip(),
            "prod_qm":           prod_qm,
            "cons_qm":           cons_qm,
            "prod_neighborhood": prod_nb,
            "cons_neighborhood": cons_nb,
            "is_cross_neighborhood": is_cross,
            "trtc":              str(row.get("Primary TRTC", "03")).strip(),
            "lob":               row.get("line_of_business", "").strip(),
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def neighborhood_coupling_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Count cross-neighbourhood flows for each (source_nb, target_nb) pair.
    Useful for Sankey diagrams.
    """
    hops = cross_neighborhood_hops(df)
    if hops.empty:
        return pd.DataFrame(columns=["source", "target", "flow_count"])
    cross = hops[hops["is_cross_neighborhood"]].copy()
    if cross.empty:
        return pd.DataFrame(columns=["source", "target", "flow_count"])
    agg = (
        cross.groupby(["prod_neighborhood", "cons_neighborhood"])
             .size()
             .reset_index(name="flow_count")
             .rename(columns={"prod_neighborhood": "source",
                               "cons_neighborhood": "target"})
    )
    return agg


# ─────────────────────────────────────────────────────────────────────────────
# BLAST RADIUS
# ─────────────────────────────────────────────────────────────────────────────

def compute_blast_radius(df: pd.DataFrame, failed_qm: str) -> dict:
    """
    Model the impact of a single QM failure.

    - Direct apps:      apps whose QM is *failed_qm*
    - Dead flows:       every message flow involving *failed_qm*
    - Transitive apps:  apps on other QMs that will be starved of messages
                        because their upstream dependencies are in *dead_flows*
    - Weighted impact:  TRTC-weighted fraction of total business criticality
    """
    flows        = extract_flows(df)
    trtc_weights = {"00": 10, "02": 5, "03": 1}

    direct_apps = set(
        df[df["queue_manager_name"] == failed_qm]["app_id"].str.strip().unique()
    ) - {""}

    dead_flows = [
        f for f in flows
        if f["prod_qm"] == failed_qm or f["cons_qm"] == failed_qm
    ]

    # Build app dependency graph to find transitive impact
    G_dep = nx.DiGraph()
    for f in flows:
        if f["prod_app"] and f["cons_app"]:
            G_dep.add_edge(f["prod_app"], f["cons_app"],
                           trtc=f["trtc"], qm_pair=(f["prod_qm"], f["cons_qm"]))

    transitive: set[str] = set()
    for app in direct_apps:
        if app in G_dep:
            transitive.update(nx.descendants(G_dep, app))
    transitive -= direct_apps

    total_weight   = sum(trtc_weights.get(f["trtc"], 1) for f in flows)
    dead_weight    = sum(trtc_weights.get(f["trtc"], 1) for f in dead_flows)

    return {
        "failed_qm":        failed_qm,
        "direct_apps":      sorted(direct_apps),
        "transitive_apps":  sorted(transitive),
        "dead_flows":       dead_flows,
        "total_flows":      len(flows),
        "blast_pct":        round(len(dead_flows) / max(len(flows), 1) * 100, 1),
        "weighted_impact":  round(dead_weight / max(total_weight, 1) * 100, 1),
    }


# ─────────────────────────────────────────────────────────────────────────────
# ARCHITECTURE DEBT
# ─────────────────────────────────────────────────────────────────────────────

def compute_architecture_debt(df: pd.DataFrame) -> dict:
    """
    Multi-dimensional architecture debt score.  Higher = more work to modernise.

    Components
    ----------
    shared_qm_debt      Each extra app on a QM adds 15 points (tight coupling)
    cross_nb_debt       Each cross-neighbourhood channel adds 3 points;
                        critical (TRTC=00) ones add 5 extra
    hop_debt            Flows that traverse more than 1 QM add 4 points each
    channel_fan_debt    Each outgoing channel above 4 on any single QM adds 2
    naming_debt         Channels that violate {from}.{to} convention add 2 each
    """
    flows  = extract_flows(df)
    hops   = cross_neighborhood_hops(df)

    # 1. Shared QM debt
    qm_app     = df.groupby("queue_manager_name")["app_id"].nunique()
    shared_debt = float(sum((n - 1) * 15 for n in qm_app if n > 1))

    # 2. Cross-neighbourhood debt
    if not hops.empty:
        cross_count    = int(hops["is_cross_neighborhood"].sum())
        critical_cross = int(hops[hops["is_cross_neighborhood"] & (hops["trtc"] == "00")].shape[0])
    else:
        cross_count = critical_cross = 0
    cross_nb_debt = float(cross_count * 3 + critical_cross * 5)

    # 3. Hop debt (detect relay QMs – appear only as remote targets, never as source)
    source_qms = set(df[df["q_type"] == "Remote"]["queue_manager_name"].str.strip().unique())
    target_qms = set(df["remote_q_mgr_name"].str.strip().dropna().unique()) - {""}
    relay_qms  = target_qms - source_qms
    hop_debt   = float(len(relay_qms) * 4)

    # 4. Channel fan-out debt
    fan_out: dict[str, int] = defaultdict(int)
    for _, row in df[df["q_type"] == "Remote"].iterrows():
        fan_out[row["queue_manager_name"].strip()] += 1
    channel_fan_debt = float(sum(max(0, v - 4) * 2 for v in fan_out.values()))

    # 5. Naming convention debt  (XmitQ should contain '.')
    naming_debt = float(
        sum(1 for f in flows if f["xmit_q"] and "." not in f["xmit_q"]) * 2
    )

    total = shared_debt + cross_nb_debt + hop_debt + channel_fan_debt + naming_debt
    return {
        "total":          round(total, 1),
        "shared_qm":      round(shared_debt, 1),
        "cross_nb":       round(cross_nb_debt, 1),
        "relay_hop":      round(hop_debt, 1),
        "channel_fan":    round(channel_fan_debt, 1),
        "naming":         round(naming_debt, 1),
    }


# ─────────────────────────────────────────────────────────────────────────────
# FLOW RISK MATRIX
# ─────────────────────────────────────────────────────────────────────────────

def flow_risk_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Score each message flow on two independent axes:

    Business Criticality  (Y)  – from TRTC: 00→3, 02→2, 03→1
    Structural Risk       (X)  – cross-neighbourhood channel = 2, same-nb = 1;
                                  add 1 if producer QM is shared (> 1 app)

    Flows in the top-right quadrant (high criticality + high structural risk)
    are the highest-priority candidates for remediation.
    """
    hops    = cross_neighborhood_hops(df)
    if hops.empty:
        return pd.DataFrame()

    trtc_score = {"00": 3, "02": 2, "03": 1}
    qm_app     = df.groupby("queue_manager_name")["app_id"].nunique().to_dict()

    rows = []
    for _, row in hops.iterrows():
        crit     = trtc_score.get(row["trtc"], 1)
        s_risk   = 2 if row["is_cross_neighborhood"] else 1
        s_risk  += 1 if qm_app.get(row["prod_qm"], 1) > 1 else 0
        rows.append({
            "queue_name":            row["queue_name"],
            "prod_app":              row["prod_app"],
            "lob":                   row["lob"],
            "prod_neighborhood":     row["prod_neighborhood"],
            "cons_neighborhood":     row["cons_neighborhood"],
            "business_criticality":  crit,
            "structural_risk":       s_risk,
            "is_cross_nb":           row["is_cross_neighborhood"],
            "trtc":                  row["trtc"],
            "risk_score":            crit * s_risk,
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# QM CENTRALITY
# ─────────────────────────────────────────────────────────────────────────────

def qm_centrality_analysis(G: nx.DiGraph) -> pd.DataFrame:
    """
    Betweenness centrality for each QM node.

    High betweenness → QM lies on many shortest paths → single point of failure.
    Combined with num_apps → overall risk score per QM.
    """
    if len(G.nodes) == 0:
        return pd.DataFrame()

    try:
        bc = nx.betweenness_centrality(G, normalized=True)
    except Exception:
        bc = {n: 0.0 for n in G.nodes}

    rows = []
    for node in G.nodes:
        num_apps = G.nodes[node].get("num_apps", 1)
        rows.append({
            "qm":           node,
            "neighborhood": G.nodes[node].get("neighborhood", "Unknown"),
            "num_apps":     num_apps,
            "in_degree":    G.in_degree(node),
            "out_degree":   G.out_degree(node),
            "betweenness":  round(bc.get(node, 0), 4),
            # Risk: shared QMs with high betweenness are most dangerous
            "risk_score":   round(bc.get(node, 0) * 10 + (num_apps - 1) * 2, 2),
        })
    return pd.DataFrame(rows).sort_values("risk_score", ascending=False)


# ─────────────────────────────────────────────────────────────────────────────
# MIGRATION WAVE PLANNER
# ─────────────────────────────────────────────────────────────────────────────

def migration_wave_planner(df: pd.DataFrame) -> dict:
    """
    Determine the optimal order to migrate apps to their dedicated QMs.

    Uses topological generations on the app-dependency graph so that each
    wave only contains apps whose upstream dependencies are already migrated.
    Returns waves (lists of app_ids) and any dependency cycles that had to
    be broken to produce a DAG.
    """
    flows = extract_flows(df)
    apps  = sorted({a for a in df["app_id"].str.strip().unique() if a})

    G_dep = nx.DiGraph()
    G_dep.add_nodes_from(apps)
    for f in flows:
        if f["prod_app"] and f["cons_app"] and f["prod_app"] != f["cons_app"]:
            G_dep.add_edge(f["prod_app"], f["cons_app"])

    # Break cycles (keep weakest – lowest-degree edge)
    cycles_removed: list[tuple] = []
    G_dag = G_dep.copy()
    for _ in range(500):           # guard against infinite loop
        try:
            cycle = nx.find_cycle(G_dag, orientation="original")
            u, v  = cycle[0][0], cycle[0][1]
            G_dag.remove_edge(u, v)
            cycles_removed.append((u, v))
        except nx.NetworkXNoCycle:
            break

    try:
        waves = [sorted(w) for w in nx.topological_generations(G_dag)]
    except Exception:
        waves = [apps]

    return {
        "waves":          waves,
        "cycles_removed": cycles_removed,
        "total_waves":    len(waves),
        "total_apps":     len(apps),
    }
