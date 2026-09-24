# OSW union test harness

Checks `content.tdei_union_dataset` against 21 small, hand-built cases (A–W) and
reports, case by case, whether the union did the right thing: first with default
settings, then under `entity_filters`.

```
test data (DS1 + DS2)  ──►  database  ──►  union  ──►  6 GeoJSON files  ──►  checks  ──►  PASS / FAIL
   data/*.geojson          content.node    run by      runs/<run>/          per case +     report, sheet,
                           content.edge    psql                             network-wide   QGIS
```

## Quick start

```bash
cp /path/to/union_v5_clean.sql union/        # the union version under test
./local_db.sh up                             # local PostGIS in Docker (first time ~1–2 min)
LOAD=1 ./run_harness.sh local                # load test data, run at 1 m and 3 m, check
./run_harness.sh local filters .             # every filter scenario
```

Each run writes to `runs/<run>/`. Start with `check_report.txt`. The exit code
is 0 only when every run passes.

## Documentation

| Read | When you want to… |
|---|---|
| [docs/setup.md](docs/setup.md) | install the prerequisites and get a database (local Docker or your own dev DB) |
| [docs/running.md](docs/running.md) | run the harness: commands, options, output files, steps by hand |
| [docs/reading-results.md](docs/reading-results.md) | understand the report and what to do when a case fails |
| [docs/qgis.md](docs/qgis.md) | inspect runs in QGIS and compare input, output, and one run against another |
| [docs/filter-scenarios.md](docs/filter-scenarios.md) | test `entity_filters`, and see what filters may and may not change |
| [docs/test-dataset.md](docs/test-dataset.md) | see what each case A–W sets up and expects |
| [docs/extending.md](docs/extending.md) | add a case or a scenario, plus a map of the code |

## Layout

```
run_harness.sh         the one command: load → union → export → check → sheet
local_db.sh            start / redeploy / status / reset the local database
docker-compose.yml     local PostgreSQL 16 + PostGIS 3 (port 5433)
docker/                image and first-start scripts (TDEI content schema, union deploy)
union/                 put the union .sql under test here (not committed)
sql/                   export helper and the psql script that writes the GeoJSON
harness/               Python: cases, checks, filter scenarios, dataset generator
data/                  test datasets (ds1_*, ds2_*) and expected/ reference layers
qgis/                  load_in_qgis.py — styled layer tree for QGIS
docs/                  documentation (this folder's guides) and images/
runs/                  output, one folder per run (not committed)
```
