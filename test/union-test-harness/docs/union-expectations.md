# What the union must do

This is the behaviour the harness holds `content.tdei_union_dataset` to. There
are two kinds of rule:

- **Connectivity rules always run, whatever the filter.** They keep the
  network routable.
- **Merging rules are controlled by `entity_filters`.** They decide what gets
  deduplicated and whose tags survive.

The principle behind the split: **filters decide what is merged, never what is
connected.** Routability outranks filters. In OSW two edges are connected only
when they share a node id, so blocking a snap because of a filter would cut the
network at that point.

## Connectivity: always runs, whatever the filter

| # | The union must… | Why | Checked by |
|---|---|---|---|
| C1 | **Snap** a DS2 node onto the nearest DS1 node within proximity | DS2 must join the DS1 network where they meet | H, Q, N (`junction`, `connected`) |
| C2 | Snap only within the same **type group**: never a road node onto a sidewalk, never a living street onto a sidewalk | different kinds of way are different physical things | E, F (`apart`) |
| C3 | **Never merge two kerbs** with each other, even within proximity | two kerbs close together are opposite sides of a crossing | D (`kerbs`, `no_kerb_merge`) |
| C4 | **Always join coincident nodes** (same coordinate), even across categories | two nodes at one point are the same physical point | L, N, W; integrity: *two nodes at one coordinate* = 0 |
| C5 | **Split a DS2 edge at a DS1 node** lying on its span | the DS1 node must be reachable from the DS2 edge | J (`junction`) |
| C6 | **Road × crossing:** mint one shared `ixn-` node where they cross, split both. If the crossing ends exactly on a road node, reuse that node | pedestrians cross the road there | C (`ixn` = 1, `junction`); L (`ixn` = 0) |
| C7 | **Split a DS1 edge where a DS2 edge T's onto it** (not roads, ≥ 30°) | a side path must connect mid-span, not dead-end beside it | G, S, R (`junction`) |
| C8 | **Align DS2 endpoints** to their final nodes | every edge end must equal its `_u_id` / `_v_id` node | integrity: *endpoint differs from its node* = 0 |
| C9 | **Stranding guard:** when a duplicate is removed, re-point whatever hung off it onto the surviving node | removing a duplicate must not cut off a crossing or spur | P, R (`junction`); routability `STRANDED` |
| C10 | **Orphan cleanup:** drop nodes that no edge (or zone) uses | no unreferenced nodes in the output | integrity: *orphan nodes* = 0 |

Network-wide, independent of the cases: nothing connected in an input may be
disconnected in the output (routability `BROKEN`, `JUNCTION`, `T-SPAN`).

## Merging: controlled by the filter

| # | The union must… | With no filter | With a filter | Checked by |
|---|---|---|---|---|
| M1 | **Pass 1, node-pair dedup:** drop a DS2 edge whose two nodes match a DS1 edge's | every duplicate removed | removed only if the DS2 edge **and** its DS1 counterpart both pass the edge filter; otherwise both stay | exact duplicates, e.g. A, K, M, V (`count`); scenarios, rule R2 |
| M2 | **Pass 2, coverage dedup:** drop a DS2 edge lying mostly inside the DS1 edge's buffer (`duplicate_buffer_width`, `duplicate_overlap_percentage`) | removed when covered enough | a DS2 edge failing the filter is always kept | near and partial duplicates, e.g. B, U (`count`); `dup_u_overlap50/60`; rule R2 |
| M3 | **Attribute merge** onto a merged node: DS1 values win; DS2-only tags are added | merged | added only when **both** nodes are eligible: on an edge passing the edge filter, and matching the node filter | A, H, W (`node_attr`); scenarios, rule R3 |
| M4 | **Audit** every DS2 value in `ext:union_audit_*`, including the ones DS1 overrode or a filter withheld | always | always, filter or not | A, W (`node_attr`); rule R3 |

All duplicate cases (A, B, D, F, K, M, P, R, U, V) are checked by edge `count` per proximity.

Rules shared by M1–M4:

- **Both sides must pass.** One copy matching the filter is not enough to
  remove a duplicate or merge tags (V under `f_and_surface`).
- **Filter groups OR, keys within a group AND.** See
  [filter-scenarios.md](filter-scenarios.md).
- **A node filter only narrows.** A node must also lie on an edge that passes the
  edge filter (W under `f_crossing n_kerb`).
- **A filter never removes more than the baseline removed.**

## Consequences to expect

An element that **fails** the filter is not removed and its tags are not merged,
but it **still snaps and still gets split** (C1–C10). So it can come out
*modified* rather than *kept*.

- `f_crossing` at 3 m: case B's DS2 sidewalk is kept (it's not a crossing), but
  its ends snap onto the DS1 sidewalk's ends.
- Same run, case D: the DS2 sidewalk is kept with one end snapped 2.5 m.
- `f_crossing n_kerb`: W's kerb still joins both sidewalks (C4), but DS2's
  `tactile_paving` is withheld and audited only (M3, M4).

This is the intended design. A report line such as
`removed → modified (duplicate, but both copies fail the edge filter — both stay)`
is a PASS.

## How the harness checks these rules

- **Default runs** (`runs/prox_1`, `runs/prox_3`): the case assertions, output
  integrity, and routability ([reading-results.md](reading-results.md)).
- **Filter runs:** compared with the same proximity without filters
  ([filter-scenarios.md](filter-scenarios.md)).
  - **R1:** C1–C10 still hold. Every structural assertion passes, and every DS2
    node that merged in the baseline still merges.
  - **R2:** M1–M2 follow the filter.
  - **R3:** M3–M4 follow the filter.

The case setups are described in [test-dataset.md](test-dataset.md).
