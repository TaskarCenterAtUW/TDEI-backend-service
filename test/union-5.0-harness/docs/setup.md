# Setup

One-time setup: the tools, then a database to run the union against.

## Prerequisites

| You need | Check | Install (macOS) |
|---|---|---|
| `psql` | `psql --version` | `brew install libpq && brew link --force libpq` |
| Python 3 | `python3 --version` | already on macOS |
| matplotlib (only for the case sheets) | `python3 -c "import matplotlib"` | `pip3 install matplotlib` |
| Docker Desktop (for option A) | `docker compose version` | docker.com |
| QGIS (optional) | — | qgis.org |

The harness itself uses only the Python standard library.

## Option A — local Docker database (recommended)

PostgreSQL 16 + PostGIS 3 on **port 5434**, with the TDEI `content` tables
(`docker/initdb/01_tdei_content_schema.sql`, from the TDEI reference DDL). Port
5434 lets it run beside the 4.x harness database (5433).

```bash
./local_db.sh up        # first time: builds the image, creates the schema,
                        # deploys union/union_5_0.sql and the export helper
```

The first start runs everything in `docker/initdb/`. **Postgres only does this on
an empty volume.** After changing the schema file, run `./local_db.sh reset`.

| Command | Does |
|---|---|
| `./local_db.sh up` | start (build + create on first run) and print status |
| `./local_db.sh deploy union/union_5_0.sql` | redeploy the union (and the export helper) after you edit it |
| `./local_db.sh status` | PostgreSQL/PostGIS versions, deployed functions, loaded datasets |
| `./local_db.sh psql` | open a psql prompt |
| `./local_db.sh down` | stop (data kept) |
| `./local_db.sh reset` | **delete** the database and start fresh (asks first) |

Connection for pgAdmin, QGIS, or psql:
`host=localhost port=5434 dbname=tdei user=tdei password=tdei`.
For a different port, set `HARNESS_PG_PORT` for both `local_db.sh` and `run_harness.sh`.

`union/union_5_0.sql` is tracked in this harness: it is the version under test.

## Option B — your own dev database

It needs PostGIS, the TDEI `content` tables, and `union/union_5_0.sql` deployed.
**Not production:** the harness writes test rows into the content tables.

Deploying 5.0 **drops the 4.x union signatures** (`tdei_union_dataset(varchar,
varchar, real[, jsonb])`), and `sql/union_to_geojson.sql` drops the 4.x export
helper. Use a database where that is fine.

Keep the connection string in a variable and the password in `~/.pgpass` (or
`PGPASSWORD`), not on the command line:

```bash
export PGCONN="host=<server>.postgres.database.azure.com port=5432 dbname=<db> user=<user> sslmode=require"
psql "$PGCONN" -v ON_ERROR_STOP=1 -f union/union_5_0.sql
LOAD=1 ./run_harness.sh "$PGCONN"
```

The export helper `content.tdei_union_dataset_geojson` (`sql/union_to_geojson.sql`)
is deployed automatically on the first run if the 5.0 version is missing.

### Loading the test data into your own database

`LOAD=1` inserts only `tdei_dataset_id` and `feature` into `content.node`,
`content.edge`, `content.zone`, `content.extension_point`,
`content.extension_line` and `content.extension_polygon`, exactly as TDEI does;
every other column is generated from the feature. Rows under the harness dataset
ids (`harness5-ds1`, `harness5-ds2`) are deleted first; nothing else is touched.

To upload through TDEI instead, upload `data/ds1_*.geojson` and
`data/ds2_*.geojson` as two datasets and pass their ids:

```bash
DS1=<tdei-dataset-id-1> DS2=<tdei-dataset-id-2> ./run_harness.sh "$PGCONN"
```

Test data ids are numeric, because TDEI stores `_id` / `_u_id` / `_v_id` as
`bigint` ([test-dataset.md](test-dataset.md) lists the ranges).

Next: [running.md](running.md).
