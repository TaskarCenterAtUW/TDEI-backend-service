# Running the harness

Everything goes through one command, run from the harness root.

```bash
LOAD=1 ./run_harness.sh local                   # first time: load the test data, run every profile + the config suite
./run_harness.sh local                          # later: data is already loaded
./run_harness.sh local default_3m custom_snap   # just these profiles
./run_harness.sh local validate                 # the config-validation suite only
./run_harness.sh "$PGCONN"                      # same, against your own dev database
./run_harness.sh list                           # list the profiles
```

`local` is shorthand for the Docker database
(`host=localhost port=5434 dbname=tdei user=tdei password=tdei`). Any other first
argument is passed to psql as the connection string. Arguments after it are
profile names, `validate`, or `all` (the default: every profile and the config
suite). Anything else stops the run before it touches the database.

## Options (environment variables)

| Variable | Default | Effect |
|---|---|---|
| `LOAD=1` | off | (re)load the test data before running |
| `DS1`, `DS2` | `harness5-ds1`, `harness5-ds2` | dataset ids to union, e.g. ids from a TDEI upload |
| `HARNESS_PG_PORT` | `5434` | port used by `local` |

## What a run does

1. **Checks the database.** `content.tdei_union_dataset(varchar, varchar, jsonb)`
   must exist, or the run stops. The 5.0 export helper is deployed if missing.
2. **Loads the test data** (only with `LOAD=1`) via the generated
   `runs/load_test_data.sql`.
3. **Runs the union once per profile**, sending the profile's config
   (`python3 harness/profiles.py <profile>` prints it), and writes the six OSW
   GeoJSON files and `report.json` into `runs/<profile>/`.
4. **Checks the output** — cases, integrity, report checks, routability, what
   changed — and draws `case_sheet.png` (if matplotlib is installed).
5. **Runs the config suite** (all / `validate`): every config in
   `harness/config_cases.py` through `content.union5_validate_config`.

The exit code is 0 only if everything passes in every run.

## After you change the union

Redeploy, then rerun. There is no need to reload the data.

```bash
./local_db.sh deploy union/union_5_0.sql     # or: psql "$PGCONN" -f union/union_5_0.sql
./run_harness.sh local
```

## Output: `runs/<profile>/`

A rerun overwrites its folder.

| File | What it is |
|---|---|
| `check_report.txt` | the PASS/FAIL report. **Start here.** See [reading-results.md](reading-results.md) |
| `case_sheet.png` | one panel per case checked under the profile: inputs, expectation, actual output (connectors magenta), a 4 m close-up, framed green/red |
| `report.json` | the union's own run report: effective config, per-rule counts, warnings, totals |
| `union_log.txt` | the union's `RAISE NOTICE` lines (phases, per-rule progress), for tracing a failure |
| `osw_nodes.geojson`, `osw_edges.geojson`, `osw_zones.geojson`, `osw_points.geojson`, `osw_lines.geojson`, `osw_polygons.geojson` | the union output |
| `case_results.geojson`, `junction_results.geojson` | PASS/FAIL layers for QGIS |
| `changes_*.geojson` | what the union changed, input vs output (by geometry) |

`runs/config_report.txt` holds the config suite's result. To browse runs, see
[qgis.md](qgis.md).

## Running the steps by hand

`run_harness.sh` is only these steps (from the harness root):

```bash
mkdir -p runs && python3 harness/load_test_data.py > runs/load_test_data.sql
psql "$PGCONN" -f runs/load_test_data.sql                                   # load
psql "$PGCONN" -f sql/union_to_geojson.sql                                  # helper, once

mkdir -p runs/default_3m && cd runs/default_3m
psql "$PGCONN" -v ds1=harness5-ds1 -v ds2=harness5-ds2 \
     -v config="$(python3 ../../harness/profiles.py default_3m)" -f ../../sql/export_union.psql
cd ../..

python3 harness/check_cases.py runs/default_3m --profile default_3m        # report
python3 harness/case_sheet.py  runs/default_3m --profile default_3m        # picture
```

Any config can be run the same way: pass it as `-v config='<json>'`. Called
directly, the union is:

```sql
SELECT * FROM content.tdei_union_dataset('harness5-ds1', 'harness5-ds2',
    '{"snap": {"mode": "default"}, "dedup": {"mode": "default"}, "connect": {"mode": "default"}}');
-- seven cursors: node, edge, zone, point, line, polygon, report
SELECT content.union5_validate_config('{"connect": {"mode": "default", "proximity": 2}}');
-- the effective config, or an error naming the field
```

`export_union.psql` writes the files client-side with `\o`, so it works on Azure,
where server-side `COPY TO` a file is not allowed. Don't swap it for `\copy`,
which doubles backslashes and breaks the JSON.
