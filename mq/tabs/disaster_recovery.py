"""Tab 6 – Disaster Recovery Planning."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from ..constants import DR_SITE_PAIRS, TRTC_LABELS
from ..data import extract_flows, neighborhood_col


def render(df: pd.DataFrame) -> None:
    st.header("Disaster Recovery Planning")
    st.markdown(
        '<div class="info-box">'
        "With dedicated QMs, DR scope is surgical – a QM failure only affects its single "
        "owning application. Standby QMs are paired in a different neighbourhood / data centre."
        "</div>",
        unsafe_allow_html=True,
    )

    nb_col  = neighborhood_col(df)
    qm_list = sorted(df["queue_manager_name"].dropna().unique().tolist())

    # ── DR topology table ──────────────────────────────────────────────────
    st.subheader("DR Topology – Primary / Standby Pairing")
    dr_rows = []
    for _, row in df.drop_duplicates("queue_manager_name").iterrows():
        qm   = row["queue_manager_name"].strip()
        nb   = row.get(nb_col, "").strip()
        trtc = str(row.get("Primary TRTC", "03")).strip()
        apps = ", ".join(sorted(df[df["queue_manager_name"] == qm]["app_id"].unique()))
        dr_rows.append({
            "Primary QM":  qm,
            "Standby QM":  f"{qm}.DR",
            "Neighborhood":nb,
            "DR Site":     DR_SITE_PAIRS.get(nb, "Cloud DR"),
            "Apps Hosted": apps,
            "RTO":         TRTC_LABELS.get(trtc, trtc),
            "RPO":         "0 msgs (sync)" if trtc == "00" else "< 5 msgs",
        })
    st.dataframe(pd.DataFrame(dr_rows), use_container_width=True)

    # ── failure simulator ──────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Failure Impact Simulator")
    failed_qm = st.selectbox("Simulate failure of QM:", qm_list)

    if not failed_qm:
        return

    flows   = extract_flows(df)
    impacted = [f for f in flows if f["prod_qm"] == failed_qm or f["cons_qm"] == failed_qm]
    st.error(f"**{failed_qm} is DOWN** – {len(impacted)} message flow(s) affected")

    if impacted:
        imp_df = pd.DataFrame([{
            "Queue":    f["queue_name"],
            "Producer": f["prod_app"],
            "Prod QM":  f["prod_qm"],
            "Consumer": f["cons_app"],
            "Cons QM":  f["cons_qm"],
            "TRTC":     f["trtc"],
            "Priority": ("🔴 Critical" if f["trtc"] == "00"
                         else "🟠 High" if f["trtc"] == "02"
                         else "🟡 Normal"),
        } for f in sorted(impacted, key=lambda x: x["trtc"])])
        st.dataframe(imp_df, use_container_width=True)

    st.markdown("---")
    st.subheader(f"Recovery Runbook – {failed_qm}")
    affected_apps: set[str] = set()
    for f in impacted:
        if f["prod_qm"] == failed_qm: affected_apps.add(f["prod_app"])
        if f["cons_qm"] == failed_qm: affected_apps.add(f["cons_app"])
    app_str = ", ".join(sorted(affected_apps))

    st.markdown(f"""
**Immediate Response (0-15 min)**
1. Alert on-call MQ administrator
2. Confirm `{failed_qm}` is unreachable via `PING QMGR`
3. Activate standby `{failed_qm}.DR` in alternate data centre
4. Notify application teams for: `{app_str}`

**Channel Recovery (15-30 min)**
5. Start sender channels on surviving peer QMs pointing to `{failed_qm}.DR`
6. Verify channel status: `DIS CHS(*) WHERE(XMITQ EQ *)`
7. Check undelivered messages in dead-letter queue (DLQ)
8. Replay DLQ messages if required

**Application Recovery (30-60 min)**
9. Restart application connections in priority order (TRTC 00 first)
10. Monitor queue depths – ensure producers are consuming
11. Run end-to-end smoke tests for each restored flow

**Post-Recovery (1-4 hr)**
12. Root cause analysis of original failure
13. Update DR test log
14. Schedule next DR drill (target: quarterly)
""")

    # ── TRTC priority table ────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Recovery Priority by TRTC")
    trtc_df = df.drop_duplicates("app_id")[["app_id", "Primary TRTC", nb_col]].copy()
    trtc_df["Priority"] = (
        trtc_df["Primary TRTC"]
        .map({"00": "🔴 Critical", "02": "🟠 High", "03": "🟡 Normal"})
        .fillna("🟡 Normal")
    )
    st.dataframe(
        trtc_df.sort_values("Primary TRTC").rename(columns={nb_col: "Neighborhood"}),
        use_container_width=True,
    )
