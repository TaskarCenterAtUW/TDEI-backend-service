# Reading the results

`runs/<profile>/check_report.txt` (the key lines are also printed on screen) has
five parts: output integrity, report checks, case results, what changed, and
network-wide routability. Every line is either `PASS`/`ok` or says exactly what
was wrong. The last line sums it up:

```
14/14 cases passed; integrity PASS; report PASS; routability PASS
```

## Case results

One line per case checked under the profile, with the failed expectations
underneath:

```
  PASS  CC9   W2: nearest B only
  FAIL  CC7   Sidewalk T onto a sidewalk
          x connectors by sidewalk-sidewalk: expected 1, got 0
          x node: ext:union_rule='footway-sidewalk' (want 'sidewalk-sidewalk')
```

Read that as: the T junction was made, but by the wrong rule — an earlier,
broader rule took the location (O1).

Connector edges (the edges a connect rule creates; they carry `ext:union_rule`)
count as part of the junction they make: the two nodes a connector joins count as
one junction, and only named edges count towards its degree.

| Line | Means |
|---|---|
| `<edge>: expected N, got M` | M output edges have that name, but there should be N (a duplicate not removed, or a split not made) |
| `junction (x,y): D edges (need K+), not reached by: …` | the named edges should meet at one junction there, and don't |
| `no node at junction (x,y)` | no output node there at all |
| `split into N pieces` | a route that must be continuous is broken |
| `… WRONGLY connected` | two things that must stay separate were joined |
| `kerb nodes: expected N, got M` / `kerb merged into node …` | kerbs were merged (or lost) |
| `new nodes: expected N, got M` | nodes at coordinates no input node has: the junction nodes connect rules create |
| `connectors [by <rule>]: expected N, got M` | connector edges in the case area (optionally by rule) |
| `node: key=… (want …)`, `key missing`, `(should be absent)` | node attributes: anchor values, added keys, `ext:union_audit_*`, `ext:union_rule` |
| `a node remains at (x,y)` | a node that should have moved onto its anchor is still there |
| `connector: key=… (want …)`, `no connector by <rule>` | a connector's tags: A's `highway`/`footway`, the rule's `edge_tags` (OSW keys as-is, others as `ext:`), and none of its `node_tags` |
| `<kind>s: expected N, got M`, `<kind>: key=…` | zones, extension points / lines / polygons in the area, and their attributes |

Coordinates `(x,y)` are metres from the dataset origin. Each case's setup and
expectation is in [test-dataset.md](test-dataset.md) and on its panel of the case
sheet.

## Output integrity

Structural faults. Each count should be 0:

- edge ends that don't equal their `_u_id` / `_v_id` node
- edges or zone rings (`_w_id`) pointing at missing nodes
- duplicate or non-numeric ids
- two nodes at one coordinate — only when the profile joins coincident nodes
  (`n/a` otherwise, e.g. `off`, `connect_only`)
- orphan nodes the union left behind (input orphans are passed through and do not count)

For `off`, an extra block checks that every output file equals the input and that
nothing carries an `ext:union_*` tag.

## Report checks

Checks on `report.json`, the union's own run report: effective rules per stage,
per-rule metrics (`snapped`, `removed`, `junctions`, `connectors`,
`skipped_taken`, …) and warnings. Warnings are listed as `note` lines whether
or not a check asks for them. [profiles.md](profiles.md) lists the checks.

## What changed

A summary of the `changes_*.geojson` layers: each input edge's fate (kept, split,
modified, removed), the output edges' origin (unchanged, split piece, modified,
connector, new) and node events (snapped, created, junction, input node gone).
Worked out by geometry, because output edge ids are renumbered.

## Network-wide routability

Independent of the case list; covers the whole output:

| Tag | Means | Profiles |
|---|---|---|
| `BROKEN` | something connected in an input is disconnected in the output | all |
| `STRANDED` | a node lost an edge and nothing replaced it | all |
| `JUNCTION` | DS1 and DS2 line ends the default rules should join, don't | default |
| `T-SPAN` | a dead end meets the middle of a compatible line of the other dataset at 30°+, unconnected | default |
| `dup-kept` | informational: two same-named ends left apart | default |

## Config validation

`runs/config_report.txt`: one line per config in `harness/config_cases.py`.
Rejected configs show the union's message; a failure says what was expected
(`message lacks '…'`, `accepted, but must be rejected`, or a wrong value in the
effective config).

## When a case fails

1. Look at its panel in `runs/<profile>/case_sheet.png`: inputs, expectation,
   actual output in black, connectors in magenta, and the 4 m close-up.
2. Open the run in QGIS and zoom to the case (Identify on the case box shows
   `expected` and `failures`).
3. Check `report.json` for the rule's counts (`skipped_taken`, `skipped_angle`,
   `skipped_nearer_end`, …) — they say why a location was passed over.
4. Find the phase in `runs/<profile>/union_log.txt`.
