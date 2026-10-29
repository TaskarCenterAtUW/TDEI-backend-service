# What union 5.0 must do

The behaviour the harness holds `content.tdei_union_dataset` 5.0 to, rule by
rule, with the cases that check each. The requirement is the "Union 5.0 —
Configurable Operations Requirement Spec"; this page is its testable form.

**Nothing is implicit.** Every snap, dedup and junction comes from the job's
`config`; with no config the output is DS1 + DS2 unchanged (`off` profile). The
stages always run in this order:

```
snap  →  dedup  →  snap topology  →  connect  →  stranding guard  →  zones & extensions  →  output + report
```

## Default rule sets (`mode: default`)

| Stage | Rules, in order (all at the stage `proximity`, 0.5 m if not given) |
|---|---|
| snap | `coincident` on; then `crossing`, `pedestrian`, `living_street`, `road`, `other` — each to its own type, anchor DS1, `merge` |
| dedup | `crossing`, `pedestrian`, `living_street`, `road`, `other` — each against its own type, `min_overlap` 0.8, `max_angle` 30°, anchor DS1 |
| connect | `road-crossing` (crossing → road, point); `crossing-sidewalk`, `crossing-living-street`, `pedestrian-pedestrian`, `living-street-living-street`, `crossing-crossing` (geometry) |
| extensions | 4.3.2 merging: points same type within `proximity`; lines ≥ 70 % in a `line_proximity` buffer (defaults to `proximity`); polygons and zones by 70 % of DS1's area |

Built-in types: **crossing** = `highway=footway` + `footway` in (crossing,
traffic_island); **sidewalk** = `highway=footway` + `footway=sidewalk`;
**pedestrian** = `highway` in (footway, pedestrian, steps), not a crossing;
**living_street**; **road** = `highway` in (primary, secondary, tertiary,
residential, service, unclassified, trunk, motorway); **other** = none of these.
A node matches on its own tags (`barrier=kerb`) and takes the types of every edge
it lies on.

## Snap

| # | The union must… | Checked by |
|---|---|---|
| S1 | Join a DS1 node and a DS2 node of the **same type** within the governing rule's proximity; the non-anchor node moves onto the anchor node | B, H, P, SN1, SN2, SN4, SN5 |
| S2 | Join **coincident** nodes whatever their types when `coincident` is on (default); with snap off they stay apart | A, L, N, W, SN6 (`off`, `connect_only`: apart) |
| S3 | Let the **first rule both nodes match** govern a pair (only its proximity applies); `others` governs pairs no rule matches; with no `others`, unlisted types don't snap | SN3, SN4, SN5 |
| S4 | **Nearest wins** (tie: lowest original `_id`); anchors never move; a node that moved is never a target; never collapse an edge onto one node | all snap cases; integrity |
| S5 | **Never move a kerb**, whatever the anchor; two kerbs join only when coincident | D, W, X3, X4, X5, X7, LS5 |
| S6 | Use the rule's **anchor** (DS1 unless it says `ds2`) | AN1, AN3 |
| S7 | **Attributes:** every mover value that differs from the anchor's is audited as `ext:union_audit_<key>` = `"<original _id>-<value>"`; `merge` adds keys only the mover has, `anchor_only` adds nothing. The node records `ext:union_absorbed` (original ids) and `ext:union_confidence` (1 − d ÷ proximity; 1.0 coincident) | A, H, W, AN1, AN2 |
| S8 | Make the mover's **edges follow**: an edge ending at it is re-pointed; an edge passing through it is split there. An anchor edge is split at a targeted vertex only when, after dedup, an edge ends there | X10, X11; integrity (*edge end differs from its node*) |

## Dedup

| # | The union must… | Checked by |
|---|---|---|
| D1 | Remove an edge joining the **same two nodes** as a `to` anchor edge when at least `min_overlap` of it lies in the buffer, whatever its angle | A, K, M, V |
| D2 | Remove a **near duplicate**: at least `min_overlap` in the buffer and within the rule's `max_angle` of the anchor edge (30° unless set; above 0, at most 90) | B, F, P, R, D (3 m), AN3, DA1, DA2 |
| D3 | Keep a short **T-stub** (angle guard) | G |
| D4 | Remove **whole edges only**; below `min_overlap` the edge is kept whole | U |
| D5 | **Connectivity guard:** an edge whose two ends are both on the anchor network is removed only if the anchor edges inside its buffer already join those ends (or it joins the same two nodes as an anchor edge) — spec D1 | DG2 |
| D6 | **Stranding guard:** after connect, restore a removed duplicate when its removal left a surviving edge hanging at a node not on the anchor network | DG1 (report: `restored` ≥ 1) |
| D7 | Judge an edge by the **first rule whose `from`** it matches, with that rule's anchor | AN3 |

## Connect

A = edges matching `from`, B = edges matching `to`, one from each dataset.
Nothing moves: a node is made on B (B is split there) and, across a gap, a
connector edge joins A's end to it.

| # | The union must… | Checked by |
|---|---|---|
| C1 | **point:** where A crosses B with no shared node, make one node and split both; reuse an existing node at that exact point | C, CC5; L (reuse) |
| C2 | **point, near miss:** an A end short of B by ≤ proximity gets a node on B and a connector | CC5 |
| C3 | **geometry, T onto B's span:** a node on B at the nearest point, B split, a connector from A's end | S, X4, LS5, CC1, CC7 |
| C4 | **Gap to B's end node:** that node is the junction (no split), a connector closes the gap | X5, X6, X7, LS3, LS4 |
| C5 | **Gap of zero:** A's end lies on B, so A's end node is the junction; B is split there, no connector | G, J, X1, X2, X3, LS1, LS2 |
| C6 | **Overshoot:** an A end beyond B meets it where it crosses — one node, A and B split there, no connector — spec G4 | CC14 |
| C7 | **W1:** apply all of a rule's matches in one pass; several splits on one edge | X9, CC13 |
| C8 | **W2:** one junction per A end, to the nearest B (tie: lowest original `_id`) | CC9 |
| C9 | **W3:** two ends facing across one gap make one junction | CC10 |
| C10 | **W4:** only the end of an A edge nearer to B joins it | CC11, X10, LS6 |
| C11 | **W5:** a T onto a span needs 30° or more (a gap to B's end node does not) | CC8, U |
| C12 | **O1:** the first rule listed takes a location; later rules skip it; the report warns about a rule that found only taken locations | CC1, CC2; report warning (`custom_connect`) |
| C13 | **O2:** a later rule landing within reach of an earlier rule's node on B uses that node; existing values stay, differing ones are audited | CC3 |
| C14 | **O3:** connectors and created nodes are never A or B; a later rule never splits them | CC4 |
| C15 | **G1:** lines whose `layer`, `level`, `bridge` or `tunnel` differ never join | CC6 |
| C16 | **G3:** act on DS1 × DS2 pairs only | CC12 |
| C17 | **Tags:** a junction node gets the rule's `node_tags` and a connector its `edge_tags`, never the other set — valid OSW keys for that kind as-is, others as `ext:<key>`; an existing node keeps its own value and audits a differing one — plus `ext:union_rule` (comma list, in rule order, on a node several rules used) and `ext:union_confidence`. Connectors copy A's `highway` / `footway`; pieces of a split edge gain nothing | CC1, CC2, CC3, CC5 |
| C18 | Join nothing no rule asks for (no default rule joins a road and a sidewalk, a sidewalk and a living street, a crossing and a plain footway) | E, F, X8, LS8 |

## Zones and extensions

| # | The union must… | Checked by |
|---|---|---|
| E | `default`: merge same-type points within proximity (DS1 position, DS2-only keys added, every DS2 value audited); lines with ≥ `line_overlap` of their length within `line_proximity` of a DS1 line (DS1 geometry, DS2-only keys added; `line_proximity` defaults to `proximity`, and Points never use it); polygons covering > 70 % of DS1's area (DS1 geometry) or the union of both below that; zones by > 70 % of DS1's area. `points`, `lines`, `polygons`, `zones` (all on by default) switch each type: a type that is off keeps both copies. `off`: pass everything through | E1–E6 |
| Z | Zone rings reference the final nodes (`_w_id`), following any snap | integrity (*zone `_w_id` references a missing node*) |

## Always

| # | The union must… | Checked by |
|---|---|---|
| G-1 | With nothing configured, return DS1 + DS2 unchanged, with no `ext:union_*` tags | `off` unchanged-output check |
| G-2 | Match edge ends to nodes by exact coordinate; keep every edge end equal to its `_u_id`/`_v_id` node | integrity |
| G-3 | Drop only the nodes the union left unreferenced; pass input orphans through | integrity (*orphan nodes the union left behind*) |
| G-4 | Never disconnect what an input connected; never strand a node | routability `BROKEN`, `STRANDED` (every profile) |
| G-5 | With the default rule sets, join every compatible DS1/DS2 end within proximity and every compatible dead end meeting a span at 30°+ | routability `JUNCTION`, `T-SPAN` (default profiles) |
| G-6 | Reject an invalid config before reading data, naming the stage, rule and field; return the effective config (defaults expanded) in the report | `harness/config_cases.py`; report checks |

## Build decisions

Found while building 5.0 against the spec and settled in it (spec edge cases
D1, G4, G5); each is checked by a case.

1. **Connectivity guard (D5).** As written, the spec keeps any edge whose two
   ends are both on the anchor network unless it joins the same two nodes as an
   anchor edge. That keeps a duplicate whenever the two datasets segment a line
   differently (DG2: DS1 has a node mid-way, DS2 doesn't), and connect then splits
   it onto the anchor, leaving two identical lines. The build removes such an
   edge when the anchor edges inside its buffer already join its two ends — the
   guard still keeps an edge that adds a connection.
2. **Overshoot (C6).** The spec says geometry rules split only B. For an A end
   that crosses B and stops beyond it, the build makes the junction where A
   crosses B (both split, no connector) rather than a connector back from the end,
   which would leave A crossing B with no node.
3. **Coincident ends under connect.** With snap off, an A end lying exactly on B's
   end node is joined to it by the connect rule (the gap-of-zero junction), since a
   zero-length connector is impossible. Coincident ends no rule covers stay apart
   (SN6 under `connect_only`).
4. **Case D at 3 m** (carried over, open): the DS2 sidewalk is a duplicate and is
   removed, and its kerb goes with it.
