# Running the harness

Everything goes through one command, run from the repo root.

```bash
LOAD=1 ./run_harness.sh local          # first time: load the test data, run at 1 m and 3 m
./run_harness.sh local                 # later: data is already loaded
./run_harness.sh local 3               # one proximity (any number of them: 1 2 3)
./run_harness.sh "$PGCONN"             # same, against your own dev database

./run_harness.sh local filters                    # list the filter scenarios
./run_harness.sh local filters f_crossing         # one scenario (+ its baseline)
./run_harness.sh local filters f_crossing n_kerb  # ONE run with both filters combined
./run_harness.sh local filters .                  # every scenario, each its own run
```

`local` is shorthand for the Docker database
(`host=localhost port=5433 dbname=tdei user=tdei password=tdei`). Any other first
argument is passed to psql as the connection string.

Arguments after the connection are **proximities** (numbers) or **`filters`**
followed by scenario names. Scenario names given without `filters` are run as
filters, with a note. Anything else that isn't a number stops the run before it
touches the database. Filter scenarios are covered in
[filter-scenarios.md](filter-scenarios.md).

## Options (environment variables)

| Variable | Default | Effect |
|---|---|---|
| `LOAD=1` | off | (re)load the test data before running |
| `DS1`, `DS2` | `harness-ds1`, `harness-ds2` | dataset ids to union, e.g. ids from a TDEI upload |
| `HARNESS_PG_PORT` | `5433` | port used by `local` |

## What a run does

1. **Checks the database.** `content.tdei_union_dataset` must exist, or the run
   stops. The export helper is deployed if missing.
2. **Loads the test data** (only with `LOAD=1`). `data/*.geojson` goes into
   `content.node` / `content.edge` under the two dataset ids, via the generated
   `runs/load_test_data.sql`.
3. **Runs the union** once per proximity or scenario, writing the six OSW GeoJSON
   files into `runs/<run>/`.
4. **Checks the output** and prints the report, then draws `case_sheet.png`
   (if matplotlib is installed).

The exit code is 0 only if every case, integrity check, routability check and
scenario rule passes in every run.

## After you change the union

Redeploy, then rerun. There is no need to reload the data.

```bash
./local_db.sh deploy union/union_v5_clean.sql     # or: psql "$PGCONN" -f <file>
./run_harness.sh local
```

## Output: `runs/<run>/`

Folders are `prox_<p>` for default runs and the scenario name for filter runs
(`f_crossing`, `f_crossing+n_kerb`, …). A rerun overwrites its folder.

| File | What it is |
|---|---|
| `check_report.txt` | the PASS/FAIL report printed on screen. **Start here.** See [reading-results.md](reading-results.md) |
| `case_sheet.png` | one panel per case: inputs, expectation, actual output, framed green/red |
| `union_log.txt` | the union's own `RAISE NOTICE` lines, for tracing a failure to a phase |
| `osw_nodes.geojson`, `osw_edges.geojson`, … | the union output (all six OSW files) |
| `case_results.geojson`, `junction_results.geojson` | PASS/FAIL layers for QGIS |
| `changes_*.geojson` | what the union changed, input vs output |
| `vs_baseline_*.geojson` | filter runs only: what the filter changed against the baseline |

To browse them all at once, see [qgis.md](qgis.md).

## Running the steps by hand

`run_harness.sh` is only these steps (from the repo root):

```bash
mkdir -p runs && python3 harness/load_test_data.py > runs/load_test_data.sql
psql "$PGCONN" -f runs/load_test_data.sql                                   # load
psql "$PGCONN" -f sql/union_to_geojson.sql                                  # helper, once

mkdir -p runs/prox_3 && cd runs/prox_3
psql "$PGCONN" -v ds1=harness-ds1 -v ds2=harness-ds2 -v prox=3 -f ../../sql/export_union.psql
cd ../..

python3 harness/check_cases.py runs/prox_3 --proximity 3                    # report
python3 harness/case_sheet.py  runs/prox_3 --proximity 3                    # picture
```

With filters or duplicate settings, add `-v filters='<json>'` to the psql call.
Then check against the baseline:

```bash
psql "$PGCONN" -v ds1=harness-ds1 -v ds2=harness-ds2 -v prox=3 \
     -v filters='{"edge":{"filters":[{"highway":"footway","footway":"crossing"}]}}' \
     -f ../../sql/export_union.psql
python3 harness/check_cases.py runs/f_crossing --proximity 3 --scenario f_crossing --baseline runs/prox_3
```

`export_union.psql` writes the files client-side with `\o`, so it works on Azure,
where server-side `COPY TO` a file is not allowed. Don't swap it for `\copy`,
which doubles backslashes and breaks the JSON.
