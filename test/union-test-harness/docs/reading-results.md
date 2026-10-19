# Reading the results

`runs/<run>/check_report.txt` (also printed on screen) has three parts: case
results, output integrity, and network-wide routability. Filter runs add a fourth
([filter-scenarios.md](filter-scenarios.md)). Every line is either `PASS`/`ok`
or says exactly what was wrong.

## Case results

One line per case, with the failed expectations underneath:

```
  PASS  E  Road and sidewalk 1.5 m apart
  FAIL  G  Short T-stub onto a span
          x long sidewalk: expected 2, got 1
          x junction (160,30): 1 edges (need 3+), not reached by: long sidewalk
```

Read that as: the DS1 long sidewalk should have been split in two where the stub
meets it. It came out whole, so only one edge reaches the junction instead of three.

| Line | Means |
|---|---|
| `<edge>: expected N, got M` | M output edges have that name, but there should be N (a duplicate not removed, or a split not made) |
| `junction (x,y): D edges (need K+), not reached by: …` | the named edges should meet at one node at that point, and don't |
| `no node at junction (x,y)` | no output node there at all (e.g. no intersection node minted) |
| `split into N pieces` | a route that must be continuous is broken into N disconnected parts |
| `… WRONGLY connected` | two things that must stay separate were joined |
| `kerb nodes: expected N, got M` | kerbs were merged (or lost) |
| `intersection nodes: expected N, got M` | `ixn-` nodes were minted where they shouldn't be, or are missing |
| `wrong/missing: key=…` | merged-node attributes (DS1 authority, `ext:union_audit_*`) are not as expected |

Coordinates `(x,y)` are metres from the dataset origin. Each case's setup and
expectation is in [test-dataset.md](test-dataset.md) and on the reference sheet.

## Output integrity

Structural faults. Each count should be 0:

- edge endpoints that don't equal their `_u_id` / `_v_id` node
- edges pointing at missing nodes
- duplicate node ids
- two nodes at one coordinate (a missed merge)
- orphan nodes

## Network-wide routability

These checks are independent of the case list and cover the whole output:

| Tag | Means |
|---|---|
| `BROKEN` | something connected in an input is disconnected in the output |
| `JUNCTION` | endpoints that should meet don't |
| `STRANDED` | a node lost an edge and nothing replaced it |
| `T-SPAN` | a dead end sits on the middle of another edge, unsplit |
| `dup-kept` | informational only: duplicates left in place (e.g. by a filter) |

## What changed

The report also summarises `changes_*.geojson`: every input edge's fate (kept,
removed, split, modified) and node events (merged, minted, gone). Changes are
worked out by geometry, because output ids are the union's own. QGIS shows the
same information as map layers ([qgis.md](qgis.md)).

## When a case fails

1. Look at its panel in `runs/<run>/case_sheet.png`: inputs, expectation, and
   the actual output in black.
2. Open the run in QGIS and zoom to the case (Identify on the case box shows
   `expected` and `failures`).
3. Find the responsible phase in `runs/<run>/union_log.txt`.
