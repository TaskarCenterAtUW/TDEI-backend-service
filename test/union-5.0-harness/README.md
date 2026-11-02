# Union 5.0 test harness

Checks `content.tdei_union_dataset` **5.0** (configurable operations) against
69 small, hand-built cases and reports, case by case and profile by profile,
whether the union did what its configuration asked.

```
test data (DS1 + DS2)  ──►  database  ──►  union(config)  ──►  6 GeoJSON + report  ──►  checks  ──►  PASS / FAIL
   data/*.geojson          content.*     one run per         runs/<profile>/         per case,       report, sheet,
                                         PROFILE                                     integrity,      QGIS
                                                                                     routability
```

In 5.0 nothing is implicit: every snap, dedup and junction comes from the job's
`config`. A **profile** is one config (`harness/profiles.py`); each case says
what it expects under each profile it is checked under.

| Profile | Config | Checks |
|---|---|---|
| `off` | `{}` | the output is DS1 + DS2, unchanged |
| `default_1m`, `default_3m` | every stage on its published default rule set | all network cases, full routability |
| `custom_snap` | per-type snap distances, `others`, coincident | first-rule-wins, per-type proximity |
| `custom_anchor` | DS2 as the anchor; `anchor_only` attributes | anchors, audit |
| `connect_only` | connect on its defaults, snap and dedup off | stages are independent |
| `custom_dedup_angle` | dedup only, sidewalks with `max_angle` 45° | the dedup angle is per rule |
| `custom_extensions` | extensions only: `points.proximity` 0.5 m, `lines.proximity` 2.5 m | Points and Lines use separate distances |
| `ext_lines_polygons`, `ext_points_zones` | extensions only, two types on and two off | each type has its own switch |
| `custom_connect` | five ordered connect rules with node_tags / edge_tags | O1–O3, W1–W5, G1, G3, tags |

A separate suite (`harness/config_cases.py`, 48 configs) checks that invalid
configs are rejected with the right message and valid ones expand as specified.

## Demo

`./run_demo.sh local` runs the union on one realistic neighbourhood described
by two sources (a city GIS and a field survey), with the config in
`demo/input.json`. Paste a config from the configurator
(`union-configurator/dist/union-configurator.html`, "Copy configuration") into that file and run
it again to see what that config does. The scene has sidewalks, four crossings,
kerbs, T-junctions, a living street, duplicates and extension features. The
picture shows the applied config with what each rule did, input and output of
the whole scene, and a close-up of each of 13 numbered spots saying what this
run did there. See [docs/demo.md](docs/demo.md).

## Quick start

```bash
./local_db.sh up                     # local PostGIS in Docker, port 5434; deploys union/union_5_0.sql
LOAD=1 ./run_harness.sh local        # load test data, run every profile + the config suite, check
./run_harness.sh local default_3m    # one profile
./run_harness.sh local validate      # the config suite only
```

Each profile writes to `runs/<profile>/`. Start with `check_report.txt`; the
exit code is 0 only when every run passes.

## Documentation

| Read | When you want to… |
|---|---|
| [docs/setup.md](docs/setup.md) | install the prerequisites and get a database (local Docker or your own dev DB) |
| [docs/running.md](docs/running.md) | run the harness: commands, options, output files, steps by hand |
| [docs/profiles.md](docs/profiles.md) | see each profile's config and what it is for; add a profile |
| [docs/reading-results.md](docs/reading-results.md) | understand the report and what to do when something fails |
| [docs/qgis.md](docs/qgis.md) | inspect runs in QGIS and compare input, output, and one profile with another |
| [docs/union-expectations.md](docs/union-expectations.md) | the 5.0 rules the harness holds the union to, and which cases check each |
| [docs/test-dataset.md](docs/test-dataset.md) | every case: setup, expected result, profiles (generated) |
| [docs/demo.md](docs/demo.md) | show the union on one realistic neighbourhood: the demo scene, its raw files and its picture |
| [docs/extending.md](docs/extending.md) | add a case, a profile or a config case, plus a map of the code |
| [docs/performance.md](docs/performance.md) | know how fast the union is at millions of features, what it needs from the database, and run the scale tests |

The requirement is the "Union 5.0 — Configurable Operations Requirement Spec".

## Layout

```
run_harness.sh         the one command: load → union per profile → export → check → sheet; config suite
run_demo.sh            the demo: one neighbourhood, two sources, your config (demo/input.json), one picture
local_db.sh            start / redeploy / status / reset the local database
docker-compose.yml     local PostgreSQL 16 + PostGIS 3 (port 5434)
docker/                image and first-start scripts (TDEI content schema, union deploy)
union/union_5_0.sql    the union under test (tracked here)
sql/                   export helper and the psql script that writes the GeoJSON and report.json
harness/               Python: cases, profiles, checks, config suite, dataset generator
data/                  test datasets (ds1_*, ds2_*: six OSW files each) and expected/ reference layers
qgis/                  load_in_qgis.py, load_demo.py — styled layer trees for QGIS
demo/                  the demo config (input.json), scene (scene.py), raw OSW files (data/), checks and picture
perf/                  scale tests: synthetic city generator, timed runs, slow-statement profiler
docs/                  guides, the union expectations, and images/ (reference sheets)
runs/                  output, one folder per profile, and perf/ for scale runs (not committed)
```
