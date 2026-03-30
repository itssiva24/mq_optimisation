"""Data loading and flow extraction."""

from __future__ import annotations

import os
from collections import defaultdict

import pandas as pd
import streamlit as st


@st.cache_data
def load_data(uploaded_file=None) -> pd.DataFrame:
    """Load MQ queue data from an uploaded file or the bundled sample CSV."""
    try:
        if uploaded_file is not None:
            for enc in ("utf-8", "latin-1", "cp1252"):
                try:
                    uploaded_file.seek(0)
                    df = pd.read_csv(uploaded_file, encoding=enc)
                    break
                except UnicodeDecodeError:
                    continue
        else:
            base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            path = os.path.join(base, "data", "sample_mq_data.csv")
            df = pd.read_csv(path, encoding="utf-8")

        df.columns = [c.strip() for c in df.columns]
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].fillna("")
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


def neighborhood_col(df: pd.DataFrame) -> str:
    """Return whichever neighborhood column name is present in *df*."""
    return "Neighborhood" if "Neighborhood" in df.columns else "Primary Neighborhood"


def extract_flows(df: pd.DataFrame) -> list[dict]:
    """
    Return one dict per Remote-queue row describing the producer→consumer flow.

    Each Remote queue row on a producer QM has a ``remote_q_name`` that matches
    the Discrete Queue Name of the corresponding Local queue on the consumer QM.
    We use that mapping to resolve the consumer app_id.
    """
    remote_rows = df[df["q_type"] == "Remote"].copy()
    local_rows  = df[df["q_type"] == "Local"].copy()

    # queue_name → consumer app_id (from Local rows)
    local_lookup: dict[str, str] = {}
    for _, row in local_rows.iterrows():
        q   = row["Discrete Queue Name"].strip()
        aid = row["app_id"].strip()
        if q and aid:
            local_lookup[q] = aid

    nb_col = neighborhood_col(df)
    flows: list[dict] = []
    for _, row in remote_rows.iterrows():
        q_name       = row["Discrete Queue Name"].strip()
        prod_qm      = row["queue_manager_name"].strip()
        cons_qm      = row.get("remote_q_mgr_name", "").strip()
        remote_q     = row.get("remote_q_name", "").strip()
        prod_app     = row["app_id"].strip()
        neighborhood = row.get(nb_col, "").strip()
        trtc         = str(row.get("Primary TRTC", "03")).strip()
        xmit_q       = row.get("xmit_q_name", "").strip()
        lob          = row.get("line_of_business", "").strip()

        cons_app = local_lookup.get(remote_q, local_lookup.get(q_name, ""))

        flows.append({
            "prod_app":    prod_app,
            "prod_qm":     prod_qm,
            "cons_app":    cons_app,
            "cons_qm":     cons_qm,
            "queue_name":  q_name,
            "remote_q":    remote_q,
            "xmit_q":      xmit_q,
            "neighborhood":neighborhood,
            "trtc":        trtc,
            "lob":         lob,
        })
    return flows
