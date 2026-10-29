# Dataset Union — Frequently Asked Questions

*How union 5.0 combines two pedestrian-network datasets, and how to ask for the union you want.*

Written for anyone who runs or reviews a union; no database knowledge is needed. The
precise rules are in the requirement spec ("Union 5.0 — Configurable Operations
Requirement Spec") and are checked by the test harness in
[`test/union-5.0-harness`](../test/union-5.0-harness/README.md).

Related pages:
- [union-configurator.html](union-configurator.html) builds a config without writing JSON.
- [union-report.md](union-report.md) explains the run report each union returns.
- The harness demo ([demo.md](../test/union-5.0-harness/docs/demo.md)) shows what a config
  does on a sample neighbourhood.

---

## The big picture

### What is a "union", briefly?

A union takes **two maps of the same area** and combines them into **one map**. You decide
which of three things it does where the maps overlap:

| Stage | What it does | Example |
|---|---|---|
| **snap** | Joins a point in one map to the same kind of point in the other when they are close | Two sidewalk ends 0.4 m apart become one corner |
| **dedup** | Removes a line from one map that duplicates a line in the other | The second copy of a shared sidewalk is dropped |
| **connect** | Makes junctions between two kinds of way | A crossing that stops 0.5 m short of a sidewalk is joined to it |

A fourth section, **extensions**, handles zones, street furniture, fences and buildings.

### What changed in 5.0?

**Nothing happens unless you ask for it.** In 4.x the union decided by itself what to merge
and connect, and you could only tune a distance (`proximity`) and some filters
(`entity_filters`). In 5.0 every snap, dedup and junction comes from the job's
**`config`**, and the union reports exactly which rules it applied and what each one did.

- `proximity` and `entity_filters` are **removed**. A job that still sends them is rejected
  with a message saying so.
- A job with **no config** returns the two datasets combined, **unchanged**.
- `"mode": "default"` in a section runs a published rule set close to what 4.x did.

### Are the two datasets treated equally?

Not quite. Wherever a point moves or a duplicate is dropped, one side stays fixed: the
**anchor**. It is the first dataset (`tdei_dataset_id_one`) unless a rule says
`"anchor": "ds2"`. The anchor's points never move and its lines are never removed; the
other dataset's are moved onto them or dropped.

Connect rules never move anything. They add a node, split a line, and where there is a gap
add a short **connector** line.

### Does the order of the two datasets matter?

Yes, for snap and dedup: the first dataset is the default anchor, so its version of a shared
feature is the one kept. Connect rules work in both directions between the two datasets.

---

## Asking for a union

### What does a union job take?

| Parameter | Required | Meaning |
|---|---|---|
| `tdei_dataset_id_one` | yes | First dataset (the default anchor) |
| `tdei_dataset_id_two` | yes | Second dataset; must differ from the first |
| `config` | no | What to do, as a JSON object (or a JSON string of one) |

```json
{
  "tdei_dataset_id_one": "<dataset id>",
  "tdei_dataset_id_two": "<dataset id>",
  "config": {
    "snap":       { "mode": "default", "proximity": 1 },
    "dedup":      { "mode": "default", "proximity": 1 },
    "connect":    { "mode": "default", "proximity": 1 },
    "extensions": { "mode": "default", "proximity": 1 }
  }
}
```

That config runs every stage on its published default rules at 1 metre: the closest
equivalent of a 4.x union at `proximity` 1.

### What do I get back?

A zip with the union's output and its run report:

| File | Contents |
|---|---|
| `osw.nodes.geojson`, `osw.edges.geojson`, `osw.zones.geojson`, `osw.points.geojson`, `osw.lines.geojson`, `osw.polygons.geojson` | The combined dataset, OpenSidewalks 0.3 |
| `union_report.json` | What the union did: the effective config, per-rule counts, warnings and totals ([union-report.md](union-report.md)) |

### Can I build a config without writing JSON?

Yes. Open [union-configurator.html](union-configurator.html) in a browser:
- it walks through each case (two sidewalk ends close together, a duplicate crossing, a
  crossing that stops short of a sidewalk, and so on), and you choose a distance or turn the
  case off;
- **Copy configuration** gives the finished `config`, ready for the job;
- **Load configuration** takes an existing config, or a whole union request, so you can edit it.

To see what a config does before running it on real data, paste it into the harness demo's
`demo/input.json` and run `./run_demo.sh local`. The picture shows, for 20 numbered spots on
a sample neighbourhood, what the config did and what the default rules would do
([demo.md](../test/union-5.0-harness/docs/demo.md)).

---

## The configuration

### How is `config` organised?

Up to four sections. Each has a `mode`:

| Mode | Meaning |
|---|---|
| `off` (or the section left out) | The stage does nothing |
| `default` | The published rule set, at the section's `proximity` (0.5 m if not given) |
| `custom` | Only the rules you list, in the order you list them |

`extensions` has `off` and `default` only.

### What does `default` do?

| Stage | Default rules, in order |
|---|---|
| snap | Points at exactly the same spot join, whatever their type; then crossing, pedestrian, living street, road and other points each join their own kind |
| dedup | Crossing, pedestrian, living street, road and other lines: a line is a duplicate of one of its own kind when at least 80 % of it lies within `proximity` of it and its direction is within 30° of it (`max_angle`) |
| connect | `road-crossing` (where a crossing crosses or nearly meets a road), `crossing-sidewalk`, `crossing-living-street`, `pedestrian-pedestrian`, `living-street-living-street`, `crossing-crossing` |
| extensions | Points of the same type within `proximity` merge. A line merges into a first-dataset line when 70 % of it (`line_overlap`) lies within `line_proximity` of it (`line_proximity` defaults to `proximity`). A building or other polygon covering more than 70 % (`polygon_overlap`) of a first-dataset one merges into it; one that overlaps less is joined with it into one shape. A zone covering more than 70 % (`zone_overlap`) of a first-dataset zone is dropped. `points`, `lines`, `polygons` and `zones` (all `true`) switch each type on or off — see [How do I set up extensions?](#how-do-i-set-up-extensions) |

Nothing else is joined: no default rule joins a road and a sidewalk, a sidewalk and a living
street, or a crossing and a plain footway. Add a custom rule if you want one.

### How do I write custom snap rules?

```json
"snap": {
  "mode": "custom",
  "coincident": true,
  "others": 0.5,
  "rules": [
    { "name": "crossings", "type": "crossing", "proximity": 1.5 },
    { "name": "sidewalks", "type": "sidewalk", "proximity": 3, "anchor": "ds2", "attributes": "anchor_only" }
  ]
}
```

| Field | Default | Meaning |
|---|---|---|
| `type` | required | Which points the rule joins; both points must match |
| `proximity` | section's, else 0.5 m | Maximum distance (0 to 5 m; 0 = same spot only) |
| `anchor` | `ds1` | Which dataset's point stays put |
| `attributes` | `merge` | `merge`: the anchor's values win and keys only the other point has are added. `anchor_only`: nothing is added |
| `coincident` (section) | on in `default` | Join points at exactly the same spot, whatever their type |
| `others` (section) | none | Distance for point types no rule lists; left out, those don't snap |

A pair of points is governed by the **first rule both match**. A sidewalk–crossing corner
matches both a crossing and a sidewalk rule; listed first, the crossing rule decides.

### How do I write custom dedup rules?

```json
"dedup": {
  "mode": "custom",
  "rules": [
    { "name": "sidewalks", "from": "sidewalk", "to": "sidewalk", "proximity": 1, "min_overlap": 0.8, "max_angle": 30 }
  ]
}
```

| Field | Default | Meaning |
|---|---|---|
| `from` | required | The kind of line that can be removed |
| `to` | required | The kind of anchor line it duplicates (may differ from `from`) |
| `proximity` | section's, else 0.5 m | Width of the corridor around the `to` line (above 0, at most 5 m) |
| `min_overlap` | 0.8 | Share of the line's length that must lie inside the corridor |
| `max_angle` | section's, else 30 | How many degrees the line's direction may differ from the anchor line's (above 0, at most 90). A line joining the same two nodes as an anchor line is a duplicate at any angle |
| `anchor` | `ds1` | Which dataset's line survives |

Whole lines only: a line is kept whole or removed whole, never trimmed.

### How do I write custom connect rules?

```json
"connect": {
  "mode": "custom",
  "rules": [
    { "name": "kerbed-crossing", "from": "crossing", "to": "sidewalk", "operation": "geometry",
      "proximity": 1,
      "node_tags": { "barrier": "kerb", "kerb": "lowered", "survey_batch": "2026-10" },
      "edge_tags": { "crossing:markings": "zebra", "surface": "asphalt" } },
    { "name": "road-crossing", "from": "crossing", "to": "road", "operation": "point",
      "proximity": 0.3, "node_tags": { "tactile_paving": "yes" } }
  ]
}
```

| Field | Default | Meaning |
|---|---|---|
| `name` | required | Unique; written on everything the rule creates |
| `from` | required | Line A: the line whose end joins, or the line that crosses |
| `to` | required | Line B: the line that is split |
| `operation` | required | `point` or `geometry` (below) |
| `proximity` | section's, else 0.5 m | Point: how far A may stop short of B. Geometry: the largest gap between A's end and B |
| `node_tags` | none | Tags for the junction node the rule makes or reuses |
| `edge_tags` | none | Tags for the connector edges the rule adds |

- **`point`**: where A crosses B with no shared node, a node is made and both lines are split;
  an end of A that stops short of B gets a node on B and a connector.
- **`geometry`**: an end of A within reach of B gets a node on B (B is split there) and a
  connector to it. An end lying exactly on B needs no connector. An end that crosses B and
  stops just beyond it meets B where it crosses.

**Order matters.** The first rule that matches a location takes it; later rules skip it. A
broad rule listed early can hide a narrower one completely — the report warns when that
happens.

### How do I set up extensions?

Extensions are the features around the network: points (benches, hydrants, lamps), lines
(fences, walls), polygons (buildings) and zones (pedestrian areas). They have `off` and
`default` modes and no custom rules; the first dataset's feature is always the one kept.

```json
"extensions": {
  "mode": "default",
  "proximity": 1,
  "line_proximity": 2.5,
  "polygons": false
}
```

| Field | Default | Meaning |
|---|---|---|
| `points` | `true` | A second-dataset point joins a first-dataset point of the same type within `proximity`. Same type means the same tags, ignoring `ext:*` keys; a point with no other tags matches any type |
| `lines` | `true` | A second-dataset line joins a first-dataset line when `line_overlap` of it lies within `line_proximity` of it |
| `polygons` | `true` | A second-dataset polygon covering more than `polygon_overlap` of a first-dataset polygon's area merges into it, keeping the first one's outline. One that overlaps less is joined with it into one shape |
| `zones` | `true` | A second-dataset zone covering more than `zone_overlap` of a first-dataset zone's area is dropped; the first dataset's zone is kept |
| `proximity` | 0.5 m | How far apart two points may be (above 0, at most 5 m) |
| `line_proximity` | `proximity` | Width of the corridor around a first-dataset line (above 0, at most 5 m) |
| `line_overlap` | 0.7 | Share of the second dataset's line that must lie inside the corridor |
| `polygon_overlap` | 0.7 | Share of the first dataset's polygon area the second's must cover |
| `zone_overlap` | 0.7 | Share of the first dataset's zone area the second's must cover |

A type that is `false` keeps both datasets' copies of it. Switching all four off is
rejected: use `"mode": "off"` instead. What happens to the tags is under
[Properties and provenance](#what-happens-to-the-tags-of-merged-extension-features).

### How do I name a type?

The same way in every rule (`type`, `from`, `to`):

| Form | Matches when | Example |
|---|---|---|
| A built-in name | — | `"crossing"` |
| An object | every key matches | `{"highway": "footway", "footway": "sidewalk"}` |
| A list as a value | the key has any of the values | `{"highway": ["primary", "residential"]}` |
| `"*"` as a value | the key is present | `{"kerb": "*"}` |
| A list of the above | any of them matches | `[{"highway": "living_street"}, "sidewalk"]` |

Built-in names: **crossing** (`highway=footway` + `footway` crossing or traffic island),
**sidewalk** (`highway=footway` + `footway=sidewalk`), **pedestrian** (`highway` footway,
pedestrian or steps, not a crossing), **living_street**, **road** (`highway` primary,
secondary, tertiary, residential, service, unclassified, trunk or motorway), **other**
(none of these). A point takes the types of the lines it lies on, plus its own tags
(`barrier=kerb`).

### What are the limits?

| Setting | Allowed |
|---|---|
| Snap `proximity` and `others` | 0 to 5 m (0 = the same spot only) |
| Every other distance: dedup and connect `proximity`, extensions `proximity` and `line_proximity` | above 0, at most 5 m |
| `min_overlap`, `line_overlap`, `polygon_overlap`, `zone_overlap` | above 0, at most 1 |
| `max_angle` | above 0, at most 90 degrees |
| Rules | at most 50 per stage |
| Rule names | 1–64 characters, no commas, unique within a stage. Connect rules need one; snap and dedup rules left unnamed are called `snap-1`, `dedup-1`, … |
| `node_tags`, `edge_tags` | values are plain strings, numbers or booleans; `_id`, `_u_id`, `_v_id`, `_w_id` and `ext:union_*` keys can't be set |
| A section in `off` mode | takes no other settings |
| `extensions` | at least one of `points`, `lines`, `polygons`, `zones` on |
| The whole config | at most 64 KB |

---

## Rules that always hold

Whatever the config, the union:

- **never moves a kerb**, and two kerbs join only when they are at exactly the same spot;
- **never moves anything twice**: a point that moved is never a target for another;
- **never joins lines on different levels** (`layer`, `level`, `bridge`, `tunnel`), even where
  they cross on the map;
- **connects only across the two datasets**; gaps inside one dataset are left as drawn (use
  self-merge for those);
- **never cuts what a rule created**: a later rule never splits a connector an earlier rule
  made;
- **keeps a duplicate that something hangs off**: if removing it would leave another line
  dangling, it is restored;
- **keeps a line that adds a connection**: a line whose ends are both on the anchor network is
  only a duplicate if the anchor already joins those ends inside its corridor;
- **cleans up after itself**: points it left unused are dropped; points that were unused in
  your input are passed through.

---

## Properties and provenance

### When two points join, what happens to their tags?

With `merge`, the anchor's values win and keys only the other point has are added. With
`anchor_only`, nothing is added. Either way, every value of the other point that differs from
the anchor's is kept in `ext:union_audit_<key>` as `"<original _id>-<value>"`, so nothing is
lost silently.

The joined point also carries `ext:union_absorbed` (the original ids that joined it) and
`ext:union_confidence` (1.0 for the same spot, falling towards 0 at the rule's distance).

### What do created nodes and connectors carry?

A junction node gets the rule's `node_tags`; a connector gets the rule's `edge_tags`. Each set
goes only where it is named: `kerb=lowered` in `node_tags` never reaches the connector, and
`crossing:markings=zebra` in `edge_tags` never reaches the node.

- **Keys.** Standard OpenSidewalks keys are written as given (`barrier`, `kerb`,
  `tactile_paving` on a node; `surface`, `width`, `crossing:markings` and the other line keys
  on a connector). Anything else becomes `ext:<key>`: `survey_batch` becomes
  `ext:survey_batch`, and `barrier` in `edge_tags` becomes `ext:barrier`, because barrier is
  not a line key.
- **Existing nodes.** A node that already has a key keeps its own value; a different value
  from the rule is recorded as `ext:union_audit_<key>`.
- **Provenance.** Both kinds get `ext:union_rule` (the rule's name) and
  `ext:union_confidence`. A node several rules used lists them all in `ext:union_rule`, in
  rule order.
- **Connectors** also copy line A's `highway` and `footway`.
- **Split lines.** The pieces keep their own tags and gain nothing.

The single `tags` setting of earlier drafts is rejected, with a message pointing to
`node_tags` and `edge_tags`.

### What happens to the tags of merged extension features?

| Kind | Tags of the merged feature |
|---|---|
| Points | As for joined nodes with `merge`: the first dataset's values win, keys only the second point has are added, and every second-dataset value is kept in `ext:union_audit_<key>` |
| Lines, polygons | The first dataset's values win and keys only the second has are added |
| Zones | The first dataset's zone is kept as it is; the second zone's tags are not added |

---

## The run report

### What is in `union_report.json`?

```json
{
  "union_version": "5.0",
  "datasets": { "ds1": "<dataset id one>", "ds2": "<dataset id two>" },
  "effective_config": { "...": "your config with every default filled in" },
  "rules": [
    { "stage": "connect", "rule": "kerbed-crossing",
      "metrics": { "junctions": 17, "connectors": 10, "nodes_created": 7, "skipped_taken": 0 } }
  ],
  "warnings": [ "connect rule \"duplicate-of-rule-1\" matched 12 location(s), all taken by earlier rules — it made no junction" ],
  "counts": { "input": { "...": "..." }, "output": { "...": "..." }, "nodes_snapped": 5, "edges_removed": 5 },
  "timings_s": { "load": 0.04, "snap": 0.01, "dedup": 0.03, "connect": 0.09, "...": "..." }
}
```

| Stage | Counts per rule |
|---|---|
| snap | `snapped`, `edges_repointed`, `edges_split` |
| dedup | `candidates`, `removed`, `restored` |
| connect | `candidates`, `junctions`, `nodes_created`, `nodes_reused`, `joined_earlier_node`, `connectors`, `edges_split`, and why locations were passed over: `skipped_taken` (an earlier rule had it), `skipped_nearer_end`, `skipped_angle`, `skipped_level`, `skipped_mutual` |

Every rule appears, even one that did nothing — a rule with all zeros is worth a second look.

Every key, count and warning is explained in [union-report.md](union-report.md), with what to
look for when reading one.

---

## When something goes wrong

### What if my config has a mistake?

The job fails before any data is read, with a message naming the stage, the rule and the
field:

| You sent | Message |
|---|---|
| `{"snapping": {...}}` | `Invalid config: unknown section 'snapping'. Allowed sections: snap, dedup, connect, extensions` |
| a connect rule without `operation` | `config.connect.rules[1].operation: is required — one of point, geometry` |
| `"from": "sidewalks"` | `config.dedup.rules[1].from: unknown type name "sidewalks" — use a tag match such as {"highway": "footway"} or one of crossing, sidewalk, pedestrian, living_street, road, other` |
| `"proximity": 9` on a rule | `config.connect.rules[1].proximity: must be greater than 0 and at most 5 (got 9)` |
| `"tags"` on a connect rule | `config.connect.rules[1].tags: a connect rule takes node_tags (for the junction node) and edge_tags (for the connector edge) instead of tags` |
| `"max_angle": 120` | `config.dedup.max_angle: must be greater than 0 and at most 90 (got 120)` |
| `extensions` with all four types `false` | `config.extensions: points, lines, polygons and zones are all off — use "mode": "off" instead` |
| a setting next to `"mode": "off"` | `config.snap: mode off takes no other settings (got "proximity")` |
| `"proximity": 0.5` at the top level (4.x style) | `'proximity' is no longer supported: union 5.0 takes its settings in 'config' (sections snap, dedup, connect, extensions)` |
| the same dataset twice | `tdei_dataset_id_one and tdei_dataset_id_two must be different datasets` |

`rules[1]` is the first rule of the stage.

---

## Common questions

### If both datasets have the same sidewalks, will I see them twice?

Only if dedup is off. With dedup on (`default`, or a rule covering sidewalks), the second
dataset's copy is dropped and the anchor's is kept.

### Will I lose features that only one dataset had?

No. Dedup only removes a line that duplicates an anchor line; anything unique is kept.

### Could the union break a walking route?

No. Nothing connected in either input is disconnected in the output: a duplicate is
restored if removing it would leave something dangling, and joins only ever add
connections. The test harness checks this over the whole output for every profile.

### How do I choose a proximity?

It depends on how precisely the two datasets were drawn. Too large, and distinct features
merge; too small, and genuine matches don't. Start small (0.5–1 m) and raise it per type
where you know the data differs more — for example `sidewalks` at 3 m and `crossings` at 1.5 m.
The report's counts show the effect of each change.

### How large can the datasets be?

Millions of features each. Every step's time grows in proportion to the data: about a quarter
of a millisecond per input edge, with every stage on its default rules. Measured on a small
2-vCPU test machine:

| Edges (both datasets) | Nodes | Time |
|---|---|---|
| 650 thousand | 419 thousand | 2.5 minutes |
| 1.85 million | 1.19 million | 6.9 minutes |
| 3.66 million | 2.36 million | 15.9 minutes |

The database needs about 2 GB of temporary space per million edges, and no time limit
shorter than the job (`statement_timeout`, `idle_in_transaction_session_timeout`). The
details, and how to run the scale tests, are in the harness's
[performance.md](../test/union-5.0-harness/docs/performance.md).

### How do I migrate a 4.x request?

| 4.x | 5.0 |
|---|---|
| `proximity: p` | `config` with `snap`, `dedup`, `connect` and `extensions` all `{"mode": "default", "proximity": p}` |
| `entity_filters` edge filters | Custom snap, dedup and connect rules naming those types |
| `duplicate_buffer_width`, `duplicate_overlap_percentage` | A dedup rule's `proximity` and `min_overlap` (0–1, not 0–100) |
| line / polygon / zone overlap | `extensions` `line_overlap`, `polygon_overlap`, `zone_overlap` (0–1) |
| line buffer distance | `extensions` `line_proximity` (above 0, at most 5 m; defaults to `proximity`) |
| merge one type but not another | `extensions` `points`, `lines`, `polygons`, `zones`: `true` / `false`, all `true` by default; a type that is off keeps both copies |

The defaults are close to 4.x, not identical: connect rules add connector lines instead of
moving ends, and anything 4.x did implicitly (for example road × crossing nodes) is now a
named rule you can see in the report.

### How do we know the union really behaves this way?

The 5.0 test harness runs the union under 11 configurations ("profiles") against 75
small hand-built cases, plus 67 configs checking validation. It also checks every output
for structure (every line end on its node, no missing or duplicate ids) and that nothing
connected in an input is disconnected. Its demo runs any config on one realistic
neighbourhood and checks 20 numbered spots against the default rules. See
[`test/union-5.0-harness`](../test/union-5.0-harness/README.md).
