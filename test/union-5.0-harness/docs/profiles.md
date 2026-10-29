# Profiles

A profile is one complete union `config` — exactly what an API caller would
send — plus the settings the checks need. They live in `harness/profiles.py`;
`python3 harness/profiles.py <name>` prints a profile's config.

Each case lists the profiles it is checked under and what it must produce
under each ([test-dataset.md](test-dataset.md)). A case with nothing for a
profile is not checked in that run.

| Field | Meaning |
|---|---|
| `config` | the union configuration sent with the run |
| `proximity` | resolves per-proximity assertions (`{1.0: 2, 3.0: 1}`) and sets the reach of the routability checks |
| `coincident` | `True` when the config joins coincident nodes; only then must the output have one node per coordinate |
| `routability` | `full` = preservation + junction + T-onto-span checks (the default rule sets); `preserve` = only "nothing connected in an input is disconnected, nothing stranded" |

## The profiles

### `off` — nothing configured

```json
{}
```

Every stage is off. The output must be DS1 + DS2 **unchanged**: the checker
compares every geometry of all six files with the input, and no feature may
carry an `ext:union_*` tag. Cases A, N, W, SN6 and E1–E4 also say what "unchanged"
means for them (e.g. SN6: two nodes at one coordinate stay apart).

### `default_1m`, `default_3m` — the published default rule sets

```json
{"snap": {"mode": "default", "proximity": 1}, "dedup": {"mode": "default", "proximity": 1},
 "connect": {"mode": "default", "proximity": 1}, "extensions": {"mode": "default", "proximity": 1}}
```

(and the same at 3 m). Every network case is checked here, with full
routability. The default rule sets are listed in
[union-expectations.md](union-expectations.md).

### `custom_snap` — per-type snap distances

```json
{"snap": {"mode": "custom", "coincident": true, "others": 0.5,
          "rules": [{"name": "crossings", "type": "crossing", "proximity": 1.5},
                    {"name": "sidewalks", "type": "sidewalk", "proximity": 3}]}}
```

Snap only. Checks: per-type proximity (SN1, SN2), the first rule both nodes
match governs (SN3), `others` for unlisted types (SN4, SN5), coincident joins
any types (SN6).

### `custom_anchor` — DS2 as the anchor

```json
{"snap": {"mode": "custom",
          "rules": [{"name": "sidewalks-ds2", "type": "sidewalk", "proximity": 1, "anchor": "ds2"},
                    {"name": "crossings-ds1", "type": "crossing", "proximity": 1, "attributes": "anchor_only"}]},
 "dedup": {"mode": "custom",
           "rules": [{"name": "sidewalks-ds2", "from": "sidewalk", "to": "sidewalk", "proximity": 1, "anchor": "ds2"}]}}
```

Checks: the DS1 end moves onto DS2's node (AN1), `anchor_only` audits without
adding (AN2), dedup anchored on DS2 removes the DS1 copy (AN3).

### `custom_dedup_angle` — a dedup rule with its own angle

```json
{"dedup": {"mode": "custom", "rules": [
   {"name": "sidewalk-45deg", "from": "sidewalk", "to": "sidewalk", "proximity": 1, "min_overlap": 0.8, "max_angle": 45}]}}
```

Only dedup runs. A short DS2 sidewalk at 40° to a DS1 sidewalk, wholly inside its
buffer, is removed here but kept at the default 30° (DA1); one at 60° is kept
under both (DA2).

### `custom_extensions` — Points and Lines each use their own distance

```json
{"extensions": {"mode": "default", "proximity": 0.5, "line_proximity": 2.5}}
```

Only the extensions stage runs. Points join within `proximity` (0.5 m) and a DS2
Line merges when `line_overlap` of it lies within `line_proximity` (2.5 m) of a
DS1 Line. Checks that the 2 m hedge merges (E5) while the 1.5 m waste baskets
stay apart (E6): the Lines' distance never reaches Points.

### `ext_lines_polygons` and `ext_points_zones` — each type has its own switch

```json
{"extensions": {"mode": "default", "proximity": 3, "points": false, "lines": true, "polygons": true, "zones": false}}
{"extensions": {"mode": "default", "proximity": 3, "points": true, "lines": false, "polygons": false, "zones": true}}
```

Only the extensions stage runs, at 3 m, with two types on and two off. A type
that is off keeps both copies; a type that is on merges as in `default_3m`. The
E cases check each type both ways across the two profiles.

### `connect_only` — stages are independent

```json
{"connect": {"mode": "default", "proximity": 1}}
```

Connect runs with snap and dedup off. Checks that junctions do not depend on
snap (X3, X4), that coincident joining is snap's job (SN6 stays apart), and that
nothing is removed (A keeps both copies).

### `custom_connect` — ordered rules with node_tags / edge_tags

```json
{"snap": {"mode": "default", "proximity": 0.5}, "dedup": {"mode": "default", "proximity": 0.5},
 "connect": {"mode": "custom", "rules": [
   {"name": "kerbed-crossing",     "from": "crossing", "to": "sidewalk", "operation": "geometry", "proximity": 1.0,
    "node_tags": {"barrier": "kerb", "kerb": "lowered", "survey_batch": "2026-10"},
    "edge_tags": {"crossing:markings": "zebra", "surface": "asphalt", "verified": "yes"}},
   {"name": "sidewalk-sidewalk",   "from": "sidewalk", "to": "sidewalk", "operation": "geometry", "proximity": 1.0},
   {"name": "footway-sidewalk",    "from": {"highway": "footway"}, "to": "sidewalk", "operation": "geometry", "proximity": 1.0,
    "node_tags": {"ext:source": "rule2"}},
   {"name": "road-crossing",       "from": "crossing", "to": "road", "operation": "point", "proximity": 0.3,
    "node_tags": {"tactile_paving": "yes"}, "edge_tags": {"crossing:markings": "lines"}},
   {"name": "duplicate-of-rule-1", "from": "crossing", "to": "sidewalk", "operation": "geometry", "proximity": 1.0}]}}
```

The CC cases. Rule order is deliberate:

- `footway-sidewalk` matches every `highway=footway`, which includes sidewalks
  and crossings. Listed **after** the narrower rules, it only gets what they left
  (CC2, CC3). Listed before `sidewalk-sidewalk`, it would take every sidewalk T
  and `sidewalk-sidewalk` would make nothing — the report would warn (O1).
- `duplicate-of-rule-1` repeats rule 1, so every location it matches is already
  taken: it makes no junction and the report carries a warning naming it.

## Report checks

`REPORT_CHECKS` in `profiles.py` checks `report.json` per profile:

| Check | Passes when |
|---|---|
| `("warning", text)` | some warning mentions *text* |
| `("no_warnings",)` | the report has no warnings |
| `("effective_rules", stage, n)` | the effective config lists *n* rules for that stage |
| `("metric", stage, rule, key, op, n)` | a rule's count compares to *n* (`==`, `>=`, `<=`) |

## Adding a profile

1. Add an entry to `PROFILES` (and, if useful, to `REPORT_CHECKS`).
2. Give the cases it should check an entry under its name in `harness/cases.py`.
3. `python3 harness/build_layers.py && python3 harness/case_docs.py && python3 harness/case_sheet.py --profile <name>`
4. `./run_harness.sh local <name>`
