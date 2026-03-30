"""Tab 7 – Architecture Validation."""

from __future__ import annotations

import networkx as nx
import pandas as pd
import streamlit as st


def _check(results: list[dict], name: str, passed: bool, detail: str) -> None:
    results.append({"Check": name, "Pass": passed, "Details": detail})


def run_validation(source_df: pd.DataFrame, target_df: pd.DataFrame) -> list[dict]:
    """Run all 7 validation checks and return a list of result dicts."""
    results: list[dict] = []

    # 1. One QM per app
    t_qm_app  = target_df.groupby("app_id")["queue_manager_name"].nunique()
    violators = t_qm_app[t_qm_app > 1].index.tolist()
    _check(results, "One QM per App",
           len(violators) == 0,
           "All apps use exactly 1 QM" if not violators
           else f"Apps with multiple QMs: {violators}")

    # 2. No new queue names invented (XmitQs with .XMIT suffix are expected new objects)
    src_names    = set(source_df["Discrete Queue Name"].str.strip().unique())
    tgt_names    = set(target_df["Discrete Queue Name"].str.strip().unique())
    tgt_non_xmit = {q for q in tgt_names if not q.endswith(".XMIT")}
    new_names    = tgt_non_xmit - src_names
    _check(results, "No New Queue Names Invented",
           len(new_names) == 0,
           "All queue names originate from source data" if not new_names
           else f"New names (check): {sorted(new_names)[:10]}")

    # 3. XmitQ naming convention: {cons_id}.XMIT
    bad_channels: list[str] = []
    for _, row in target_df[target_df["q_type"] == "Remote"].iterrows():
        dst_qm = row.get("remote_q_mgr_name", "").strip()
        xmit   = row.get("xmit_q_name", "").strip()
        if xmit:
            app_part = dst_qm.replace("QM.", "") if dst_qm.startswith("QM.") else dst_qm
            expected = f"{app_part}.XMIT"
            if xmit != expected:
                bad_channels.append(f"{xmit} (expected {expected})")
    _check(results, "Channel Naming Convention",
           len(bad_channels) == 0,
           "All XmitQ names follow {cons_id}.XMIT pattern" if not bad_channels
           else f"Violations: {bad_channels[:5]}")

    # 4. Every Remote queue has a matching Local queue on the target consumer QM
    t_remote   = target_df[target_df["q_type"] == "Remote"]
    t_local    = target_df[target_df["q_type"] == "Local"]
    local_set  = set(zip(t_local["Discrete Queue Name"].str.strip(),
                         t_local["queue_manager_name"].str.strip()))
    missing_local: list[str] = []
    for _, row in t_remote.iterrows():
        q   = row["Discrete Queue Name"].strip()
        dst = row.get("remote_q_mgr_name", "").strip()
        if dst and (q, dst) not in local_set:
            missing_local.append(f"{q} on {dst}")
    _check(results, "Remote → Local Queue Pairing",
           len(missing_local) == 0,
           "Every remote queue has a local counterpart on the target QM" if not missing_local
           else f"Missing local queues: {missing_local[:5]}")

    # 5. No orphaned XmitQs
    xmitq_names  = set(target_df[target_df["usage"] == "XMITQ"]["Discrete Queue Name"]
                       .str.strip().unique())
    used_xmitqs  = (set(target_df[target_df["q_type"] == "Remote"]["xmit_q_name"]
                        .str.strip().unique()) - {""})
    orphaned     = xmitq_names - used_xmitqs
    _check(results, "No Orphaned XmitQs",
           len(orphaned) == 0,
           "All transmission queues are referenced by a remote queue" if not orphaned
           else f"Orphaned XmitQs: {sorted(orphaned)[:5]}")

    # 6. No circular routing
    G_val = nx.DiGraph()
    for _, row in t_remote.iterrows():
        src = row["queue_manager_name"].strip()
        dst = row.get("remote_q_mgr_name", "").strip()
        if src and dst:
            G_val.add_edge(src, dst)
    try:
        cycles = list(nx.simple_cycles(G_val))
        _check(results, "No Circular Routing",
               len(cycles) == 0,
               "No cycles detected in channel graph" if not cycles
               else f"Cycles found: {cycles[:3]}")
    except Exception as ex:
        _check(results, "No Circular Routing", False, str(ex))

    # 7. All source apps present in target
    src_apps     = set(source_df["app_id"].str.strip().unique()) - {""}
    tgt_apps     = set(target_df["app_id"].str.strip().unique()) - {""}
    missing_apps = src_apps - tgt_apps
    _check(results, "All Source Apps in Target",
           len(missing_apps) == 0,
           "All applications preserved in target" if not missing_apps
           else f"Missing apps: {sorted(missing_apps)}")

    return results


def render(source_df: pd.DataFrame) -> None:
    st.header("Architecture Validation")
    st.markdown(
        "Run automated checks against the generated target architecture to ensure "
        "correctness before migration."
    )

    if "target_df" not in st.session_state:
        st.warning("Generate the target architecture in **Tab 3** first, then run validation.")
        return

    target_df: pd.DataFrame = st.session_state["target_df"]

    if not st.button("▶️ Run Validation Suite", type="primary"):
        return

    results = run_validation(source_df, target_df)

    st.markdown("---")
    all_pass = all(r["Pass"] for r in results)
    if all_pass:
        st.success("✅ All validation checks passed!")
    else:
        fails = sum(1 for r in results if not r["Pass"])
        st.error(f"❌ {fails} check(s) failed – review details below")

    for r in results:
        icon = "✅" if r["Pass"] else "❌"
        with st.expander(f"{icon} {r['Check']}", expanded=not r["Pass"]):
            st.write(r["Details"])

    st.markdown("---")
    st.subheader("Source vs Target Summary")
    sum_df = pd.DataFrame({
        "Item":   ["Apps", "QMs", "Total Queues", "Remote Queues", "Local Queues", "XmitQs"],
        "Source": [
            source_df["app_id"].nunique(),
            source_df["queue_manager_name"].nunique(),
            len(source_df),
            len(source_df[source_df["q_type"] == "Remote"]),
            len(source_df[source_df["q_type"] == "Local"]),
            len(source_df[source_df["usage"] == "XMITQ"]),
        ],
        "Target": [
            target_df["app_id"].nunique(),
            target_df["queue_manager_name"].nunique(),
            len(target_df),
            len(target_df[target_df["q_type"] == "Remote"]),
            len(target_df[target_df["q_type"] == "Local"]),
            len(target_df[target_df["usage"] == "XMITQ"]),
        ],
    })
    sum_df["Delta"] = sum_df["Target"] - sum_df["Source"]
    st.dataframe(sum_df, use_container_width=True)
