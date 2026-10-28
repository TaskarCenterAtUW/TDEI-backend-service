# Union 5.0 run report

`content.tdei_union_dataset` returns the combined dataset and a run report
saying what it did. This page explains the report: how to get it, what each key
holds, and how to read it. How to write a config is in
[union-FAQs.md](union-FAQs.md).

---

## Where the report comes from

```sql
content.tdei_union_dataset(
    src_one_tdei_dataset_id  varchar,
    src_two_tdei_dataset_id  varchar,
    config                   jsonb DEFAULT '{}'
) RETURNS TABLE (file_name text, cursor_ref refcursor)
```

The function returns seven rows, one cursor per output:

| `file_name` | Cursor | Each row of the cursor |
|---|---|---|
| `node`, `edge`, `zone`, `point`, `line`, `polygon` | `node_cursor` … `polygon_cursor` | one GeoJSON feature (`jsonb`) |
| `report` | `report_cursor` | **one row, one `jsonb` value: the run report** |

Cursors live only until the transaction ends, so call the function and fetch
from the cursors in the same transaction:

```sql
BEGIN;
SELECT * FROM content.tdei_union_dataset('<dataset id one>', '<dataset id two>',
                                         '{"snap": {"mode": "default", "proximity": 1}}'::jsonb);
FETCH ALL FROM report_cursor;          -- the report
FETCH ALL FROM edge_cursor;            -- the edges, and so on
COMMIT;
```

The TDEI union job writes this value to `union_report.json` in its output zip,
next to the six OpenSidewalks files.

---

## An example

This report is from a union with every stage on its default rules at 1 m,
trimmed: `…` marks rules left out. It has seven keys:

```json
{
  "union_version": "5.0",
  "datasets": { "ds1": "<dataset id one>", "ds2": "<dataset id two>" },

  "effective_config": {
    "snap": {
      "mode": "default", "anchor": "ds1", "attributes": "merge", "coincident": true, "others": null,
      "rules": [
        { "name": "crossing", "type": "crossing", "proximity": 1, "anchor": "ds1", "attributes": "merge" },
        …
      ]
    },
    "dedup": {
      "mode": "default",
      "rules": [
        { "name": "crossing", "from": "crossing", "to": "crossing", "proximity": 1, "min_overlap": 0.8, "anchor": "ds1" },
        …
      ]
    },
    "connect": {
      "mode": "default",
      "rules": [
        { "name": "road-crossing", "from": "crossing", "to": "road", "operation": "point", "proximity": 1, "node_tags": {}, "edge_tags": {} },
        { "name": "crossing-sidewalk", "from": "crossing", "to": "sidewalk", "operation": "geometry", "proximity": 1, "node_tags": {}, "edge_tags": {} },
        …
      ]
    },
    "extensions": { "mode": "default", "proximity": 1, "line_overlap": 0.7, "polygon_overlap": 0.7, "zone_overlap": 0.7 }
  },

  "rules": [
    { "stage": "snap", "rule": "coincident", "metrics": { "snapped": 1, "edges_repointed": 1, "edges_split": 0 } },
    { "stage": "snap", "rule": "road",       "metrics": { "snapped": 3, "edges_repointed": 4, "edges_split": 0 } },
    …
    { "stage": "dedup", "rule": "pedestrian", "metrics": { "candidates": 3, "removed": 2, "restored": 0 } },
    …
    { "stage": "connect", "rule": "crossing-sidewalk", "metrics": {
        "candidates": 4, "junctions": 4, "connectors": 1, "edges_split": 5,
        "nodes_created": 2, "nodes_reused": 2, "joined_earlier_node": 0,
        "skipped_taken": 0, "skipped_nearer_end": 0, "skipped_angle": 0, "skipped_level": 0, "skipped_mutual": 0 } },
    …
  ],

  "counts": {
    "input":  { "nodes": 48, "edges": 41, "zones": 0, "points": 5, "lines": 2, "polygons": 10 },
    "output": { "nodes": 46, "edges": 51, "zones": 0, "points": 4, "lines": 1, "polygons": 9 },
    "nodes_snapped": 5, "edges_removed": 5, "nodes_created": 6, "connectors": 3
  },

  "warnings": [],

  "timings_s": { "load": 0.04, "snap": 0.01, "dedup": 0.03, "snap_topology": 0.0, "connect": 0.09,
                 "stranding_guard": 0.0, "extensions": 0.02, "output": 0.01 }
}
```

The key order in the stored value is not meaningful. PostgreSQL stores `jsonb`
objects with their keys sorted, so read keys by name. Arrays (`rules`,
`warnings`, and the `rules` lists in the config) do keep their order.

| Key | What it holds |
|---|---|
| `union_version` | `"5.0"` |
| `datasets` | the two dataset ids as passed in. `ds1` is `src_one`, `ds2` is `src_two`; the config and the report use these names (`"anchor": "ds1"`) |
| [`effective_config`](#effective_config--the-config-as-applied) | the config as the function applied it |
| [`rules`](#rules--what-each-rule-did) | what each rule did, in run order |
| [`counts`](#counts--in-out-and-totals) | features in and out, and totals |
| [`warnings`](#warnings) | things worth a look, as sentences |
| [`timings_s`](#timings_s--seconds-per-stage) | seconds per stage |

---

## `effective_config` — the config as applied

The function first checks `config` (`content.union5_validate_config`) and fills
in every default; a config it rejects raises an error before any data is read.
The report records the result. Read it to confirm the function understood what
you meant:

- **What `default` expands to:** a `default` stage becomes the named rules it
  stands for, so you can see them.
- **Values you left out:** each gets its default, for example `proximity` 0.5 m
  or `min_overlap` 0.8.
- **Stages you left out:** each appears as off.

| Stage | When on | When off (or left out) |
|---|---|---|
| `snap` | `mode`, `anchor`, `attributes`, `coincident`, `others`, `rules: [{name, type, proximity, anchor, attributes}]` | `{"mode": "off", "coincident": false, "rules": []}` |
| `dedup` | `mode`, `rules: [{name, from, to, proximity, min_overlap, anchor}]` | `{"mode": "off", "rules": []}` |
| `connect` | `mode`, `rules: [{name, from, to, operation, proximity, node_tags, edge_tags}]` | `{"mode": "off", "rules": []}` |
| `extensions` | `mode`, `proximity`, `line_overlap`, `polygon_overlap`, `zone_overlap` | `{"mode": "off"}` |

**How values appear:**
- **Rule defaults are copied in.** Section-level settings such as `anchor`,
  `attributes` and dedup's `min_overlap` appear on every rule, so each rule
  states exactly what it used.
- **`others` expands.** Sent as a number (`"others": 0.5`), it becomes
  `{"proximity": 0.5, "anchor": …, "attributes": …}`. If it isn't set it is
  `null`, and point types no rule lists don't snap.
- **Types stay as you wrote them:** a name (`"sidewalk"`), a tag match
  (`{"highway": "footway"}`) or a list of either.
- **Units:** distances are in metres, and overlaps are fractions from 0 to 1.

A config with no stages gives all four stages off: `{}`, or no `config`
argument at all, since it defaults to `{}`. Then `rules` is `[]`, and the
output is the two datasets combined, unchanged.

---

## `rules` — what each rule did

One entry per rule, `{"stage": …, "rule": <name>, "metrics": {…}}`. Snap
entries come first, then dedup, then connect, each stage in the order its rules
ran.

**Every rule appears, even one that did nothing.** A rule with all zeros is
worth a second look (see [Reading a report](#reading-a-report)).

Snap has two entries besides its listed rules:
- **`coincident`** comes first when on. It is the step that joins points at
  exactly the same spot.
- **`others`** comes last when set. It is the distance for point types no rule
  lists.

Extensions have no entries; their effect shows in [`counts`](#counts--in-out-and-totals).

### Snap

A node from the non-anchor dataset joins the anchor's node of the same type
within the rule's `proximity`.

| Metric | Meaning |
|---|---|
| `snapped` | nodes that joined an anchor node under this rule (they no longer exist on their own) |
| `edges_repointed` | edge ends moved onto the anchor node, because they ended at a node that joined |
| `edges_split` | edges split at a node this rule joined. This happens when a line passes through that node, either in the middle of the line or, once dedup has run, where an edge of the other dataset now ends on it |

### Dedup

A line from the non-anchor dataset is removed when it duplicates an anchor line.
It is removed when at least `min_overlap` of it lies within `proximity` of a
`to` line, running the same way.

| Metric | Meaning |
|---|---|
| `candidates` | non-anchor lines of the `from` type this rule examined. A line is examined by the first rule that covers it only |
| `removed` | lines removed as duplicates, in the end (after any restore) |
| `restored` | lines this rule removed that were then put back, because removing them would have left a surviving line hanging, cut off from the anchor network |

### Connect

The ends of `from` lines are joined to `to` lines within `proximity`:
- **`point`:** where they cross or nearly meet, one shared node.
- **`geometry`:** a node on the `to` line, plus a connector when there is a gap.

Rules run in order, and an end joined by an earlier rule is not taken again.

| Metric | Meaning |
|---|---|
| `candidates` | ends (of `from` lines) the rule found within reach of a `to` line |
| `junctions` | joins made |
| `connectors` | connector edges added (a gap was bridged) |
| `edges_split` | lines split to put a junction node on them |
| `nodes_created` | new junction nodes |
| `nodes_reused` | existing nodes used as the junction, e.g. the `to` line's end node or a kerb already on it |
| `joined_earlier_node` | junctions that joined a node an earlier junction had made |
| `skipped_taken` | ends passed over because an earlier rule already joined them |
| `skipped_nearer_end` | ends passed over because the line's other end is nearer the `to` line and makes the junction |
| `skipped_angle` | ends passed over because the join would meet the `to` line at less than 30° (a gap to its end node is exempt) |
| `skipped_level` | ends passed over because the two lines are on different levels |
| `skipped_mutual` | two ends facing each other across one gap: one junction is made, the other end is counted here |

`junctions` minus `connectors` counts the joins made in place, with no
connector. In those cases the line already touched or crossed the other one,
for example a kerb lying on the sidewalk or a crossing running past it.

---

## `counts` — in, out and totals

| Key | Meaning |
|---|---|
| `input` | features read from both datasets together, per kind: `nodes`, `edges`, `zones`, `points`, `lines`, `polygons` |
| `output` | features in the output cursors, per kind (the row count of each feature cursor) |
| `nodes_snapped` | nodes that joined another (the sum of the snap rules' `snapped`) |
| `edges_removed` | duplicates removed (the sum of the dedup rules' `removed`) |
| `nodes_created` | junction nodes made (the sum of the connect rules' `nodes_created`) |
| `connectors` | connector edges added (the sum of the connect rules' `connectors`) |

Output counts can be higher than input counts:
- **Edges:** splits turn one edge into two or more, and connectors are new
  edges. Removed duplicates go the other way.
- **Nodes:** snapped nodes and the nodes only a removed duplicate used are
  gone; created junction nodes are added.
- **Extensions:** points, lines, polygons and zones fall by the number that
  merged.

---

## `warnings`

Plain sentences, one per rule or condition, never one per feature, so the list
stays short at any data size. An empty list means nothing needed attention.

| Warning | What it means | What to do |
|---|---|---|
| `connect rule "<name>" matched <n> location(s), all taken by earlier rules — it made no junction` | the rule found places to join, but earlier rules had joined every one | if that's intended, nothing. Otherwise move the rule earlier, or narrow the earlier rule's types |
| `<n> edge end(s) had no node at their coordinate; a node was added for each` | the input had edges whose ends had no node at that position | the output is valid, but check the source data: its edges and nodes don't agree |

---

## `timings_s` — seconds per stage

| Key | Stage, in run order |
|---|---|
| `load` | reading both datasets, building the working tables and indexes |
| `snap` | joining nodes |
| `dedup` | removing duplicate lines |
| `snap_topology` | splitting anchor lines where a moved line now ends on them |
| `connect` | the connect rules |
| `stranding_guard` | putting back duplicates whose removal would leave a line hanging |
| `extensions` | zones, points, lines and polygons |
| `output` | building the output features |

Each value is rounded to 0.01 s. A stage that is off shows about 0. Checking
the config and building the report itself are not listed; they take well under
1 % of a large run. Fetching the feature cursors happens after the function
returns, so the time to read them is not included.

---

## Reading a report

| You see | It usually means |
|---|---|
| a rule with all zeros | its types match nothing in this data, or its `proximity` is too small. Compare `effective_config` with what you meant |
| connect `candidates` > 0 but `junctions` = 0 | every candidate was skipped: the `skipped_*` counts say why. All `skipped_taken` also raises a warning |
| dedup `candidates` high, `removed` low | lines are near each other but less than `min_overlap` of them lies within `proximity`, or they run different ways. Raise `proximity` or lower `min_overlap` only if they really are the same feature |
| dedup `restored` > 0 | a duplicate was kept so no line was left hanging. This is expected and protects routing |
| `skipped_angle` > 0 | ends that would meet the `to` line at a shallow angle (under 30°): usually parallel lines, not a T-junction |
| `skipped_level` > 0 | lines on different levels (bridges, tunnels) were correctly not joined |
| `nodes_snapped` much higher than expected | a snap `proximity` is too large for this data: distinct features are being joined |
| output equals input, everything 0 | no config (or `{}`) was passed: all stages were off |

To compare two configs, run the union on the same two datasets with each and
compare their `rules` and `counts`.

---

## Per-feature detail

The report is a summary. To find which features a rule touched, look at the
tags the function puts on the output features
([union-FAQs.md › Properties and provenance](union-FAQs.md#properties-and-provenance)):
- `ext:union_rule`: the connect rule that made a node or connector.
- `ext:union_absorbed`: the original ids that joined a node or merged into a
  feature.
- `ext:union_confidence`: how close the match was, 1.0 at the same spot.
- `ext:union_audit_<key>`: a value from the other dataset that differed and
  was not kept.

---

## Size and cost

The report is a few kilobytes whatever the data size. It holds per-rule counts
and the config, never lists of features, and its warnings are one line per rule
or condition.

It is built once at the end, with about a dozen simple counts over the working
tables. Those are linear passes with no spatial work. The per-rule counts are
kept while each stage runs, from rows that stage already has.

Measured on 2.4 M nodes and 3.7 M edges in, the function took 952 s and its
stages 945 s. That leaves under 7 s for everything else, including building
this report.
