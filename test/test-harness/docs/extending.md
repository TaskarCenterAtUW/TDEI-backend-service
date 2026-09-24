# Extending the harness

## Adding a case

1. Add the geometry in `harness/generate_dataset.py`. Start every edge `name`
   with the case id (`"X: ..."`).
2. Add the case to `harness/cases.py`: setup, expected text, assertions.
3. Regenerate and validate:
   ```bash
   python3 harness/generate_dataset.py     # data/ds1_*, data/ds2_*
   python3 harness/validate_dataset.py     # schema / id / coordinate checks
   python3 harness/build_layers.py         # data/expected/*
   python3 harness/case_sheet.py           # docs/images/reference_sheet.png
   ```
4. Reload and run: `LOAD=1 ./run_harness.sh local`, then `./run_harness.sh local filters .`

Filter scenarios pick up a new case with no further work: their expectations are
derived from the baseline ([filter-scenarios.md](filter-scenarios.md)).

### Assertions in `cases.py`

| Assertion | Passes when |
|---|---|
| `("count", name, n)` | exactly *n* output edges have that name (`n` may be per proximity: `{1.0: 2, 3.0: 1}`) |
| `("junction", (x, y), d, [names])` | a node within 5 cm of *(x, y)* joins ≥ *d* edges, including each listed name |
| `("connected", [names])` | those edges form one connected piece |
| `("apart", a, b)` | the two never share a connected piece |
| `("kerbs", n)` / `("no_kerb_merge",)` | kerb count in the case area / no kerb absorbed another |
| `("ixn", n)` | minted intersection nodes in the case area |
| `("node_attr", (x, y), {k: v})` | node attributes (`None` = the key must exist) |
| `("at", p, assertion)` | the wrapped assertion applies only at proximity *p* |

Coordinates are metres from the dataset origin. In filter scenarios, the
structural assertions (`junction`, `connected`, `apart`, `ixn`, `no_kerb_merge`)
must still hold (rule R1).

## Adding a filter scenario

Add an entry to `SCENARIOS` in `harness/scenarios.py`:

```python
dict(name="f_footway", proximity=3,
     title="Edge filter: plain footways only",
     filters={"edge": {"filters": [{"highway": "footway"}]}},
     description="…what should happen and why…"),
```

- Keep one filter concern per scenario, and combine them on the command line
  (`filters a b`) rather than writing combined entries.
- Edge and node filters need nothing else. The expectations are derived.
- For duplicate settings (`duplicate_buffer_width`,
  `duplicate_overlap_percentage`), set `dedup_rule=False` and list the expected
  fates: `expect=[("U: partly overlapping sidewalk", "removed")]`.

## Code map

| Path | Purpose |
|---|---|
| `run_harness.sh` | the one command: load → union → export → check → sheet |
| `local_db.sh`, `docker-compose.yml`, `docker/` | local PostGIS database: image, TDEI schema, union deploy on first start |
| `union/` | the union `.sql` under test (git-ignored) |
| `sql/union_to_geojson.sql` | export helper `content.tdei_union_dataset_geojson`: drains the union's six cursors into FeatureCollections |
| `sql/export_union.psql` | runs the union once and writes the six GeoJSON files client-side (Azure-safe) |
| `harness/cases.py` | case catalogue: setup, expectation and assertions, in one place |
| `harness/check_cases.py` | the report: per-case checks, integrity, what changed, routability, scenario rules |
| `harness/changes.py` | input-vs-output change detection, by geometry |
| `harness/routability.py` | network-wide connectivity checks (also runnable on its own) |
| `harness/scenarios.py` | filter scenario catalogue; `python3 harness/scenarios.py` lists them |
| `harness/filter_oracle.py` | works out what a scenario must do (R1–R3) from the filter and the baseline |
| `harness/case_sheet.py` | the visual case sheet (per run, or the reference sheet) |
| `harness/common.py` | shared paths and helpers |
| `harness/generate_dataset.py`, `harness/validate_dataset.py` | build and validate `data/` |
| `harness/build_layers.py` | builds `data/expected/` from `cases.py` |
| `harness/load_test_data.py` | writes the SQL that loads `data/` into `content.node` / `content.edge` |
| `qgis/load_in_qgis.py` | loads and styles every run in QGIS |
