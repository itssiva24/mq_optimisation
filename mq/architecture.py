"""Target architecture generation and complexity metrics."""

from __future__ import annotations

from collections import defaultdict

import numpy as np
import pandas as pd

from .data import neighborhood_col


# ─────────────────────────────────────────────────────────────────────────────
# TARGET ARCHITECTURE GENERATION
# ─────────────────────────────────────────────────────────────────────────────

def generate_target_architecture(df: pd.DataFrame) -> pd.DataFrame:
    """
    Produce a target-state MQ object inventory enforcing:
      1. One dedicated QM per application  (QM.{app_id})
      2. Producers write to a Remote queue backed by an XmitQ on their own QM
      3. Consumers read from a Local queue on their own QM
      4. No new queue names are invented – only names present in the source are used

    Every unique (prod_app, cons_app, queue_name) triple in the source Remote
    rows becomes exactly 3 output rows:
      - Remote queue  on QM.{prod_app}
      - XmitQ         on QM.{prod_app}  (one per unique consumer, de-duped)
      - Local queue   on QM.{cons_app}
    """
    nb_col      = neighborhood_col(df)
    remote_rows = df[df["q_type"] == "Remote"].copy()
    local_rows  = df[df["q_type"] == "Local"].copy()

    # ── per-app metadata snapshot ──────────────────────────────────────────
    app_meta: dict[str, dict] = {}
    for _, row in df.iterrows():
        aid = row["app_id"].strip()
        if aid and aid not in app_meta:
            app_meta[aid] = {
                "ProducerName":               row.get("ProducerName", ""),
                "ConsumerName":               row.get("ConsumerName", ""),
                "Primary App_Full_Name":      row.get("Primary App_Full_Name", ""),
                "Primary Neighborhood":       row.get("Primary Neighborhood",
                                                       row.get(nb_col, "")),
                "Primary Hosting Type":       row.get("Primary Hosting Type", "Internal"),
                "Primary Data classification":row.get("Primary Data classification", ""),
                "Primary Enterprise Critical":row.get("Primary Enterprise Critical", "No"),
                "Payment Application":        row.get("Payment Application", "No"),
                "Primary PCI":                row.get("Primary PCI", "No"),
                "Primary Publicly Accessible":row.get("Primary Publicly Accessible", "No"),
                "Primary TRTC":               row.get("Primary TRTC", "03"),
                "app_id":                     aid,
                "line_of_business":           row.get("line_of_business", ""),
                "cluster_name":               row.get("cluster_name", ""),
                "cluster_namelist":           row.get("cluster_namelist", ""),
                "def_persistence":            row.get("def_persistence", "Persistent"),
                "def_put_response":           row.get("def_put_response", "Sync"),
                "inhibit_get":                row.get("inhibit_get", "No"),
                "inhibit_put":                row.get("inhibit_put", "No"),
                "Neighborhood":               row.get(nb_col, ""),
            }

    # ── consumer app lookup (Local queue name → app_id) ────────────────────
    local_lookup: dict[str, str] = {}
    for _, row in local_rows.iterrows():
        q   = row["Discrete Queue Name"].strip()
        aid = row["app_id"].strip()
        if q and aid:
            local_lookup[q] = aid

    # ── collect unique flows ───────────────────────────────────────────────
    seen_flows: dict[tuple, dict] = {}
    for _, row in remote_rows.iterrows():
        q_name   = row["Discrete Queue Name"].strip()
        prod_app = row["app_id"].strip()
        rem_q    = row.get("remote_q_name", "").strip() or q_name
        cons_app = local_lookup.get(rem_q, local_lookup.get(q_name, ""))
        if not (prod_app and cons_app):
            continue
        key = (prod_app, cons_app, q_name)
        if key not in seen_flows:
            seen_flows[key] = {
                "queue_name": q_name,
                "prod_app":   prod_app,
                "cons_app":   cons_app,
                "trtc":       str(row.get("Primary TRTC", "03")).strip(),
                "pay_app":    row.get("Payment Application", "No"),
            }

    # ── build output rows ──────────────────────────────────────────────────
    def _base_fields(app_id: str, role: str) -> dict:
        m = app_meta.get(app_id, {})
        return {
            "Primary App_Full_Name":       m.get("Primary App_Full_Name", app_id),
            "Primary Neighborhood":        m.get("Primary Neighborhood", ""),
            "Primary Hosting Type":        m.get("Primary Hosting Type", "Internal"),
            "Primary Data classification": m.get("Primary Data classification", ""),
            "Primary Enterprise Critical": m.get("Primary Enterprise Critical", "No"),
            "Payment Application":         m.get("Payment Application", "No"),
            "Primary PCI":                 m.get("Primary PCI", "No"),
            "Primary Publicly Accessible": m.get("Primary Publicly Accessible", "No"),
            "Primary TRTC":                m.get("Primary TRTC", "03"),
            "Primary Application Id":      app_id,
            "app_id":                      app_id,
            "line_of_business":            m.get("line_of_business", ""),
            "cluster_name":                m.get("cluster_name", ""),
            "cluster_namelist":            m.get("cluster_namelist", ""),
            "def_persistence":             m.get("def_persistence", "Persistent"),
            "def_put_response":            m.get("def_put_response", "Sync"),
            "inhibit_get":                 m.get("inhibit_get", "No"),
            "inhibit_put":                 m.get("inhibit_put", "No"),
            "Neighborhood":                m.get("Neighborhood",
                                                  m.get("Primary Neighborhood", "")),
            "PrimaryAppRole":              role,
        }

    rows_out: list[dict] = []
    xmitq_created: set[tuple] = set()

    for flow in seen_flows.values():
        prod_id   = flow["prod_app"]
        cons_id   = flow["cons_app"]
        q_name    = flow["queue_name"]
        prod_qm   = f"QM.{prod_id}"
        cons_qm   = f"QM.{cons_id}"
        xmit_q    = f"{cons_id}.XMIT"
        prod_meta = app_meta.get(prod_id, {})
        cons_meta = app_meta.get(cons_id, {})

        # Row 1 – Remote queue on producer QM
        r1 = _base_fields(prod_id, "Producer")
        r1.update({
            "Discrete Queue Name": q_name,
            "ProducerName":        prod_meta.get("Primary App_Full_Name",
                                                  prod_meta.get("ProducerName", prod_id)),
            "ConsumerName":        cons_meta.get("Primary App_Full_Name",
                                                  cons_meta.get("ConsumerName", cons_id)),
            "PrimaryAppDisp":      f"{prod_id}.{cons_id}",
            "q_type":              "Remote",
            "queue_manager_name":  prod_qm,
            "remote_q_mgr_name":   cons_qm,
            "remote_q_name":       q_name,
            "xmit_q_name":         xmit_q,
            "usage":               "Normal",
            "Primary TRTC":        flow["trtc"],
            "Payment Application": flow["pay_app"],
        })
        rows_out.append(r1)

        # Row 2 – XmitQ on producer QM (one per unique consumer per producer QM)
        xmit_key = (prod_qm, xmit_q)
        if xmit_key not in xmitq_created:
            xmitq_created.add(xmit_key)
            r2 = _base_fields(prod_id, "Producer")
            r2.update({
                "Discrete Queue Name": xmit_q,
                "ProducerName":        prod_meta.get("Primary App_Full_Name", prod_id),
                "ConsumerName":        "",
                "PrimaryAppDisp":      prod_id,
                "q_type":              "Local",
                "queue_manager_name":  prod_qm,
                "remote_q_mgr_name":   "",
                "remote_q_name":       "",
                "xmit_q_name":         "",
                "usage":               "XMITQ",
            })
            rows_out.append(r2)

        # Row 3 – Local queue on consumer QM
        r3 = _base_fields(cons_id, "Consumer")
        r3.update({
            "Discrete Queue Name": q_name,
            "ProducerName":        prod_meta.get("Primary App_Full_Name", prod_id),
            "ConsumerName":        cons_meta.get("Primary App_Full_Name", cons_id),
            "PrimaryAppDisp":      f"{prod_id}.{cons_id}",
            "q_type":              "Local",
            "queue_manager_name":  cons_qm,
            "remote_q_mgr_name":   "",
            "remote_q_name":       "",
            "xmit_q_name":         "",
            "usage":               "Normal",
            "Primary TRTC":        flow["trtc"],
            "Payment Application": flow["pay_app"],
        })
        rows_out.append(r3)

    target_df = pd.DataFrame(rows_out)
    # Preserve source column order; add any missing columns as empty
    for col in df.columns:
        if col not in target_df.columns:
            target_df[col] = ""
    return target_df[[c for c in df.columns if c in target_df.columns]]


# ─────────────────────────────────────────────────────────────────────────────
# COMPLEXITY METRICS
# ─────────────────────────────────────────────────────────────────────────────

def compute_complexity(df: pd.DataFrame, label: str = "") -> dict:
    """
    Return a dict of complexity dimensions and a weighted composite score.

    Score formula (higher = more complex):
        score = (shared_qms × 5) + (num_channels × 1)
              + (max_fan_out × 2) + (max_fan_in × 2)
              + (num_qms × 0.5) + (apps_per_qm × 3)

    Thresholds: < 50 Low · 50-100 Medium · 100-200 High · > 200 Critical
    """
    nb_col      = neighborhood_col(df)
    remote_rows = df[df["q_type"] == "Remote"]

    num_qms    = df["queue_manager_name"].nunique()
    num_apps   = df["app_id"].nunique()
    num_queues = len(df)
    num_xmitqs = len(df[df["usage"] == "XMITQ"])

    qm_app_map  = df.groupby("queue_manager_name")["app_id"].nunique()
    apps_per_qm = float(qm_app_map.mean()) if len(qm_app_map) else 0.0
    shared_qms  = int((qm_app_map > 1).sum())

    channels: set[tuple]          = set()
    fan_out:  dict[str, set[str]] = defaultdict(set)
    fan_in:   dict[str, set[str]] = defaultdict(set)

    for _, row in remote_rows.iterrows():
        src = row["queue_manager_name"].strip()
        dst = row.get("remote_q_mgr_name", "").strip()
        if src and dst and src != dst:
            channels.add((src, dst))
            fan_out[src].add(dst)
            fan_in[dst].add(src)

    num_channels = len(channels)
    avg_fan_out  = float(np.mean([len(v) for v in fan_out.values()])) if fan_out else 0.0
    avg_fan_in   = float(np.mean([len(v) for v in fan_in.values()]))  if fan_in  else 0.0
    max_fan_out  = max((len(v) for v in fan_out.values()), default=0)
    max_fan_in   = max((len(v) for v in fan_in.values()),  default=0)

    nb_series    = df.get(nb_col, pd.Series(dtype=str))
    neighborhoods = int(nb_series.nunique())

    complexity_score = round(
        shared_qms * 5
        + num_channels * 1
        + max_fan_out * 2
        + max_fan_in * 2
        + num_qms * 0.5
        + apps_per_qm * 3,
        1,
    )

    return {
        "label":            label,
        "num_qms":          num_qms,
        "num_apps":         num_apps,
        "apps_per_qm":      round(apps_per_qm, 2),
        "shared_qms":       shared_qms,
        "num_channels":     num_channels,
        "num_queues":       num_queues,
        "num_xmitqs":       num_xmitqs,
        "avg_fan_out":      round(avg_fan_out, 2),
        "avg_fan_in":       round(avg_fan_in, 2),
        "max_fan_out":      max_fan_out,
        "max_fan_in":       max_fan_in,
        "neighborhoods":    neighborhoods,
        "complexity_score": complexity_score,
    }
