# Filter scenarios

Filter scenarios run the union with an `entity_filters` value. Each one is
compared with the **baseline**: the same proximity with no filters
(`runs/prox_3`, `runs/prox_1`). The baseline is run first, automatically.

```bash
./run_harness.sh local filters                        # list the scenarios
./run_harness.sh local filters f_crossing             # one scenario
./run_harness.sh local filters f_crossing n_kerb      # ONE run with both filters combined
./run_harness.sh local filters .                      # every scenario, each as its own run
```

Results go to `runs/<scenario>/`. That folder has the same files as any run, plus
`vs_baseline_edges.geojson` and `vs_baseline_nodes.geojson`.

## Filter syntax

```json
{"edge": {"filters": [{"highway": "footway", "footway": "sidewalk"}, {"surface": "concrete"}],
          "duplicate_buffer_width": 3, "duplicate_overlap_percentage": 50},
 "node": {"filters": [{"barrier": "kerb"}]}}
```

Keys within a group are **AND**-ed. Groups are **OR**-ed: a group matches when
the feature's properties contain all of its key/values.

## Combining scenarios

`filters a b …` makes a **single** union run named `a+b` (folder
`runs/f_crossing+n_kerb/`):

- **Within a file type, groups are joined and OR-ed.** `f_sidewalk f_crossing`
  means sidewalk OR crossing.
- **Across file types, both apply.** `f_crossing n_kerb` means *edge: crossing*
  **and** *node: kerb*.
- **Some scenarios can't be combined:** those at different proximities, or with
  conflicting duplicate settings. The error says why.

## What filters control, and what they don't

Filters decide what is **merged**, never what is **connected**. This is the
agreed design: routability outranks filters.

| Always runs, whatever the filter (connectivity) | Controlled by the filter (merging) |
|---|---|
| node snapping within proximity (type guard, kerb×kerb exception; coincident nodes always join) | duplicate removal, node-pair match (Pass 1): only passing DS2 edges are removed, only passing DS1 edges count as the original |
| DS2 edge split at a DS1 node on its span | duplicate removal, coverage match (Pass 2): failing DS2 edges are always kept |
| road × crossing: shared `ixn-` node, both split | node attribute merge: only when both nodes pass (always audited in `ext:union_audit_*`) |
| DS1 edge split where a DS2 edge T's onto it (not roads, ≥ 30°) | |
| DS2 endpoints aligned to their nodes; stranding guard; orphan cleanup | |

So an element that fails the filter is not removed, and its tags are not merged.
It still snaps and still gets split, so it can come out *modified*.

Example: `f_crossing` at 3 m. Case B's DS2 sidewalk is kept (it's not a
crossing), but its ends snap onto the DS1 sidewalk's ends. Case D's is kept with
one end snapped 2.5 m. That is the expected behaviour, and the harness checks
for it.

## Scenarios

| Scenario | Filter | What it shows |
|---|---|---|
| `f_sidewalk` | edge: sidewalk | only sidewalk duplicates are removed; K, F and M are kept |
| `f_crossing` | edge: crossing | only M is removed; every sidewalk duplicate is kept |
| `f_sidewalk_or_crossing` | edge: sidewalk **OR** crossing | two groups are OR-ed |
| `f_and_surface` | edge: sidewalk **AND** surface=concrete | only V's DS1 copy matches, so V keeps both copies (both sides must pass) |
| `f_or_surface` | edge: surface=concrete OR asphalt | V's copies match different groups, so V is deduplicated |
| `n_kerb` | node: barrier=kerb | attributes merge only onto kerbs (W) |
| `n_nomatch` | node: kerb=raised | no attributes merge anywhere; everything still connects |
| `dup_u_overlap50` / `60` | buffer 3 m, overlap 50 / 60 %, at 1 m | the threshold decides U; D's DS2 sidewalk (2.5 m off) falls inside the 3 m corridor either way |

The catalogue lives in `harness/scenarios.py`. To add a scenario, see
[extending.md](extending.md).

## How a scenario is judged

A scenario's expectations are derived from the filter rules and the baseline,
not hand-written per case (`harness/scenarios.py`, `harness/filter_oracle.py`).
New cases therefore need no new scenario expectations.

| Rule | Checked as |
|---|---|
| **R1 connectivity** | every junction / route / apart assertion of the default run still passes; every DS2 node that merged in the baseline still merges; integrity and routability pass |
| **R2 duplicates** | a DS2 duplicate the baseline removed is removed only if it **and** its DS1 counterpart pass the edge filter; otherwise both stay. Nothing the baseline kept is removed. |
| **R3 attributes** | a DS2-only tag is merged only when both nodes are eligible (on an edge passing the edge filter, and matching the node filter); the DS2 value is always audited |

Duplicate-settings scenarios (`dup_*`) deliberately change what counts as a
duplicate. For those, R2 is replaced by explicit per-edge expectations.

The report lists every edge whose fate changed against the baseline, and every
attribute merge, each with PASS/FAIL and the reason:

```
Filter scenario f_crossing+n_kerb: Edge filter: crossings only + Node filter: kerbs only
  vs baseline runs/prox_3: 9 input edges changed fate; duplicates removed 1, kept by the filter 8; attribute merges: 0 merged, 5 withheld
    PASS  A: duplicate sidewalk      removed → kept   (duplicate, but both copies fail the edge filter — both stay)
    PASS  node W: withheld tactile_paving=yes
```

In the `f_crossing n_kerb` run above, W's kerb sits between two sidewalks. It is a
kerb, but it isn't on a crossing, so DS2's `tactile_paving` is audited only. A
node filter narrows the edge filter; it never widens it.

In QGIS, each scenario appears under **Runs** with an extra **vs baseline**
group ([qgis.md](qgis.md)).
