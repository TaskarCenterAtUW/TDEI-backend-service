# Extending the harness

## Adding a case

1. Add the geometry in `harness/generate_dataset.py`. Start every edge `name`
   with the case id (`"X: ..."`); give zones and extension features
   `"ext:case": "X"`. Keep the case in its own area of the map.
2. Add the case to `harness/cases.py`: title, setup, expected text, and
   assertions **per profile** (`"default"` covers both default profiles). If it
   checks a rule in [union-expectations.md](union-expectations.md), add the case
   id to that rule's row.
3. Regenerate, validate and rebuild the references:
   ```bash
   python3 harness/generate_dataset.py     # data/ds1_*, data/ds2_*
   python3 harness/validate_dataset.py     # ids, exact edge ends, zone rings, 7 dp, ...
   python3 harness/build_layers.py         # data/expected/*
   python3 harness/case_docs.py            # docs/test-dataset.md
   python3 harness/case_sheet.py --all     # docs/images/reference_<profile>.png
   ```
4. Reload and run: `LOAD=1 ./run_harness.sh local`.

### Assertions in `cases.py`

| Assertion | Passes when |
|---|---|
| `("count", name, n)` | exactly *n* output edges have that name (`n` may be per proximity: `{1.0: 2, 3.0: 1}`) |
| `("junction", (x, y), d, [names])` | a junction within 5 cm of *(x, y)* joins ≥ *d* named edges, including each listed name (nodes a connector joins count as one junction) |
| `("connected", [names])` | those edges form one connected piece |
| `("apart", a, b)` | the two never share a connected piece |
| `("kerbs", n)` / `("no_kerb_merge",)` | kerb count in the case area / no kerb absorbed another |
| `("minted", n)` | output nodes in the area at a coordinate no input node has |
| `("connectors", n[, rule])` | connector edges in the area (optionally made by *rule*) |
| `("node_attr", (x, y), {k: v})` | node attributes (`None` = must exist, `ABSENT` = must not) |
| `("node_absent", (x, y))` | no output node within 5 cm |
| `("edge_attr", rule, {k: v})` | the connector(s) *rule* made in the area carry these values |
| `("features", kind, n)` | zones / points / lines / polygons in the area |
| `("feature_attr", kind, (x, y) or None, {k: v})` | the feature nearest *(x, y)*, or every one in the area |
| `("at", p, assertion)` | the wrapped assertion applies only at proximity *p* |

Coordinates are metres from the dataset origin.

## Adding a profile

See [profiles.md](profiles.md#adding-a-profile).

## Adding a config case

Add a tuple to `CASES` in `harness/config_cases.py`:

```python
("dedup proximity zero", {"dedup": {"mode": "default", "proximity": 0}},
 ("error", "config.dedup.proximity: must be greater than 0")),
("default connect", {"connect": {"mode": "default"}},
 ("ok", [("connect.rules", "len", 6), ("connect.rules[1].name", "==", "road-crossing")])),
```

`("error", fragment)`: the config must be rejected with a message containing
*fragment*. `("ok", [(path, op, value)])`: it must be accepted, and the effective
config at *path* (dotted, `[n]` 1-based) must compare to *value* (`==` or `len`).

## Code map

| Path | Purpose |
|---|---|
| `run_harness.sh` | the one command: load → union per profile → export → check → sheet; config suite |
| `local_db.sh`, `docker-compose.yml`, `docker/` | local PostGIS database: image, TDEI schema, union deploy on first start |
| `union/union_5_0.sql` | the union under test: helpers, `union5_validate_config`, `union5_apply_splits`, `tdei_union_dataset` |
| `sql/union_to_geojson.sql` | export helper `content.tdei_union_dataset_geojson(ds1, ds2, config)`: drains the seven cursors |
| `sql/export_union.psql` | runs the union once and writes the six GeoJSON files and `report.json` client-side (Azure-safe) |
| `harness/cases.py` | case catalogue: setup, expectation and per-profile assertions, in one place |
| `harness/profiles.py` | the run profiles (configs) and their report checks |
| `harness/check_cases.py` | the report: per-case checks, integrity, report checks, what changed, routability |
| `harness/changes.py` | input-vs-output change detection, by geometry |
| `harness/routability.py` | network-wide connectivity checks (also runnable on its own) |
| `harness/config_cases.py` | the config-validation suite |
| `harness/case_sheet.py` | the visual case sheet (per run, or the reference sheets) |
| `harness/case_docs.py` | writes `docs/test-dataset.md` from `cases.py` |
| `harness/build_layers.py` | builds `data/expected/` from `cases.py` |
| `harness/common.py` | shared paths and helpers |
| `harness/generate_dataset.py`, `harness/validate_dataset.py` | build and validate `data/` |
| `harness/load_test_data.py` | writes the SQL that loads `data/` (or `--data-dir`) into the six content tables |
| `qgis/load_in_qgis.py` | loads and styles every run in QGIS |
| `run_demo.sh`, `demo/` | the demo scene, its raw files, checks and picture ([demo.md](demo.md)); `qgis/load_demo.py` loads it in QGIS |
| `perf/generate_scale.sql` | builds a synthetic city of any size as two datasets, for scale tests |
| `perf/run_scale.sh`, `perf/run_once.psql` | time the union on those cities; results in `runs/perf/results.tsv` ([performance.md](performance.md)) |
| `perf/slow_statements.py` | ranks the statements inside a union run by time, from an auto_explain log |
| `perf/compare_runs.py` | checks two sets of harness runs produced identical output |
