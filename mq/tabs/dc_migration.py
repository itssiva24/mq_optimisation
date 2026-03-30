"""Tab 5 – Data Centre Migration Planner."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from ..charts import create_network_graph
from ..constants import TRTC_LABELS
from ..data import extract_flows
from ..graph import build_qm_graph


_DC_TARGETS = [
    "Private PaaS", "Core Banking", "Wholesale Banking",
    "Consumer Lending", "Mainframe", "Cloud DC-East", "Cloud DC-West",
]


def render(df: pd.DataFrame) -> None:
    st.header("Data Center Migration Planner")
    st.markdown(
        '<div class="info-box">'
        "With 1-QM-per-app architecture, migrating an app only impacts its own QM and "
        "its direct channel peers – never unrelated apps on the same QM."
        "</div>",
        unsafe_allow_html=True,
    )

    all_app_ids   = sorted(df["app_id"].dropna().unique().tolist())
    migrating_apps = st.multiselect(
        "Select Apps to Migrate", all_app_ids,
        default=all_app_ids[:3] if len(all_app_ids) >= 3 else all_app_ids,
        help="Choose 3-5 apps for migration planning",
    )
    target_nb = st.selectbox("Target Neighborhood / Data Centre", _DC_TARGETS)

    if not migrating_apps:
        return

    flows          = extract_flows(df)
    affected_flows = [f for f in flows
                      if f["prod_app"] in migrating_apps or f["cons_app"] in migrating_apps]
    cross_boundary = [f for f in affected_flows
                      if (f["prod_app"] in migrating_apps) != (f["cons_app"] in migrating_apps)]

    st.markdown("---")
    am1, am2, am3 = st.columns(3)
    am1.metric("Apps Migrating",         len(migrating_apps))
    am2.metric("Affected Flows",          len(affected_flows))
    am3.metric("Cross-Boundary Channels", len(cross_boundary),
               help="Channels that span migrating and non-migrating apps")

    # Highlight migrating QMs in the network
    G_mig = build_qm_graph(df)
    if len(G_mig.nodes) > 0:
        mig_qms = {
            row["queue_manager_name"].strip()
            for _, row in df[df["app_id"].isin(migrating_apps)].iterrows()
        }
        fig_mig = create_network_graph(
            G_mig,
            "Migration Impact – Red nodes = Migrating QMs",
            highlight_nodes=mig_qms,
        )
        st.plotly_chart(fig_mig, use_container_width=True)

    st.markdown("---")
    st.subheader("Cross-Boundary Channels Requiring Reconfiguration")
    if cross_boundary:
        cb_df = pd.DataFrame([{
            "Queue Name":   f["queue_name"],
            "Producer App": f["prod_app"],
            "Producer QM":  f["prod_qm"],
            "Consumer App": f["cons_app"],
            "Consumer QM":  f["cons_qm"],
            "TRTC":         TRTC_LABELS.get(f["trtc"], f["trtc"]),
        } for f in cross_boundary])
        st.dataframe(cb_df, use_container_width=True)
    else:
        st.success("No cross-boundary channels – migration is fully self-contained!")

    st.markdown("---")
    st.subheader("Migration Steps Checklist")
    app_str = ", ".join(migrating_apps)
    st.markdown(f"""
**Phase 1 – Pre-Migration (T-4 weeks)**
- [ ] Document all current channel definitions for apps: `{app_str}`
- [ ] Identify consumer & producer counterparts via remote queue analysis
- [ ] Provision target Queue Manager(s) in `{target_nb}`
- [ ] Apply security certificates and channel authentication rules

**Phase 2 – Parallel Run (T-2 weeks)**
- [ ] Create shadow QMs in target data centre
- [ ] Configure sender/receiver channels from current QMs to shadow QMs
- [ ] Enable message mirroring for critical queues (TRTC=00)
- [ ] Validate message delivery end-to-end

**Phase 3 – Cut-Over (T-0)**
- [ ] Quiesce producers on current QM
- [ ] Drain in-flight messages (monitor queue depths to zero)
- [ ] Switch DNS / channel endpoints to target QM
- [ ] Re-enable producers pointing to target QM
- [ ] Monitor error logs for 30 minutes post cut-over

**Phase 4 – Post-Migration (T+1 week)**
- [ ] Verify all TRTC SLAs are being met in new environment
- [ ] Decommission old queue definitions
- [ ] Update CMDB and architecture diagrams
- [ ] Close change ticket
""")

    unaffected = [a for a in all_app_ids if a not in migrating_apps]
    st.subheader("Apps Unaffected (benefit of 1-QM-per-app)")
    st.success(
        f"{len(unaffected)} app(s) remain completely unaffected: "
        + ", ".join(unaffected[:15])
        + (" …" if len(unaffected) > 15 else "")
    )
