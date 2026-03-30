Why Queues and Channels Reduce
Queues
In the current architecture, queue objects are inflated by three pathologies:

Scattered ownership — apps connect to multiple QMs to reach their queues. The same logical queue (e.g. 8A.OK.XFER.RQST) may exist as a Remote stub on QM1, a Local queue on QM2, and an Alias on QM3 — three objects for one logical message path.

Alias queues as workarounds — when an app can't reach a queue directly it gets an Alias pointing to another QM. Every Alias is an extra queue object with no intrinsic value; it exists only because the topology is wrong.

Orphaned objects — shared QMs accumulate Local and XmitQ objects that outlive their original app flows (decommissioned producers still have XmitQs, removed consumers still have Local queues). The generator de-duplicates strictly: it iterates unique (prod_app, cons_app, queue_name) triples, so any duplicated or orphaned definition is collapsed to exactly the 3 objects it needs.

Target rule: every message path = exactly 3 queue objects: 1 Remote + 1 XmitQ on the producer QM, 1 Local on the consumer QM. No Aliases, no orphans, no duplicates.

Channels
Current shared QMs create hub-and-spoke concentration and multi-hop routing:

Multi-hop collapse — if the current path is App A → QM_Hub → QM_Relay → App B, that's two channels. The target routes QM.A → QM.B directly — one channel.

Dead channels — shared QMs accumulate sender/receiver channels to QMs that no longer host active counterparts. The target only creates a channel where a Remote→Local pair actually exists.

Over-fan-out on hub QMs — a shared QM hosting 5 apps that each talk to 4 destinations needs up to 20 distinct XmitQ/channel combinations. Target QMs are single-app, so max_fan_out shrinks from potentially high values to exactly the number of unique consumers that app talks to.

The total unique directed (prod_app → cons_app) flows is the same — that's dictated by real business requirements. What drops is the overhead channels created by the shared topology.