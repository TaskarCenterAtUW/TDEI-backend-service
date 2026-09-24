# Setup

One-time setup: the tools, then a database to run the union against.

## Prerequisites

| You need | Check | Install (macOS) |
|---|---|---|
| `psql` | `psql --version` | `brew install libpq && brew link --force libpq` |
| Python 3 | `python3 --version` | already on macOS |
| matplotlib (only for `case_sheet.png`) | `python3 -c "import matplotlib"` | `pip3 install matplotlib` |
| Docker Desktop (for option A) | `docker compose version` | docker.com |
| QGIS (optional) | — | qgis.org |

The harness itself needs nothing else. It uses only the Python standard library.

## Option A — local Docker database (recommended)

PostgreSQL 16 + PostGIS 3 on **port 5433**, with the TDEI `content` tables
(`docker/initdb/01_tdei_content_schema.sql`, from the TDEI reference DDL).

```bash
cp /path/to/union_v5_clean.sql union/     # the union version you want to test
./local_db.sh up                         # first time: builds the image, creates the schema,
                                         # deploys union/*.sql and the export helper
```

The first start runs everything in `docker/initdb/`. **Postgres only does this on
an empty volume.** After changing the schema file, run `./local_db.sh reset`.

| Command | Does |
|---|---|
| `./local_db.sh up` | start (build + create on first run) and print status |
| `./local_db.sh deploy union/<file>.sql` | redeploy the union after you edit it |
| `./local_db.sh status` | PostgreSQL/PostGIS versions, deployed functions, loaded datasets |
| `./local_db.sh psql` | open a psql prompt |
| `./local_db.sh down` | stop (data kept) |
| `./local_db.sh reset` | **delete** the database and start fresh (asks first) |

Connection for pgAdmin, QGIS, or psql:
`host=localhost port=5433 dbname=tdei user=tdei password=tdei`.
To use a different port, set `HARNESS_PG_PORT=5434` for both `local_db.sh` and
`run_harness.sh`.

`union/*.sql` is git-ignored: the union is versioned with the union code, not
here. Copy in the version you want to test.

## Option B — your own dev database

It needs PostGIS, the TDEI `content` tables, and the union version under test
already deployed. **Not production:** the harness writes test rows into
`content.node` / `content.edge`.

Keep the connection string in a variable and the password in `~/.pgpass` (or
`PGPASSWORD`), not on the command line:

```bash
export PGCONN="host=<server>.postgres.database.azure.com port=5432 dbname=<db> user=<user> sslmode=require"
./run_harness.sh "$PGCONN"
```

The export helper `content.tdei_union_dataset_geojson` (`sql/union_to_geojson.sql`)
is deployed automatically on the first run if it is missing. The union itself is
never modified.

### Loading the test data into your own database

`LOAD=1` inserts only `tdei_dataset_id` and `feature`, exactly as TDEI does. Every
other column is generated from the feature. Rows under the harness dataset ids are
deleted first; nothing else is touched.

To upload through TDEI instead, upload `data/ds1_*.geojson` and
`data/ds2_*.geojson` as two datasets and pass their ids:

```bash
DS1=<tdei-dataset-id-1> DS2=<tdei-dataset-id-2> ./run_harness.sh "$PGCONN"
```

Test data ids are numeric, because TDEI stores `_id` / `_u_id` / `_v_id` as `bigint`:
DS1 nodes `1000001…`, edges `1500001…`; DS2 nodes `2000001…`, edges `2500001…`.

Next: [running.md](running.md).
