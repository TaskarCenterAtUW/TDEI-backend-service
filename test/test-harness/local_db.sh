#!/usr/bin/env bash
# =============================================================================
# local_db.sh — manage the local Docker database for the harness.
#
#   ./local_db.sh up                     start it (builds + creates the schema the first time)
#   ./local_db.sh deploy union/x.sql     (re)deploy a union .sql after you edit it
#   ./local_db.sh psql                   open a psql prompt on it
#   ./local_db.sh status                 what is deployed / loaded
#   ./local_db.sh reset                  DELETE the database and start fresh
#   ./local_db.sh down                   stop it (data kept)
#
#   Details: docs/setup.md
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"
CONN="host=localhost port=${HARNESS_PG_PORT:-5433} dbname=tdei user=tdei password=tdei"

wait_ready() {
    for _ in $(seq 1 60); do
        docker compose exec -T db pg_isready -U tdei -d tdei >/dev/null 2>&1 && \
        docker compose exec -T db psql -U tdei -d tdei -qAt -c "select 1 from pg_proc where proname='tdei_union_dataset_geojson'" 2>/dev/null | grep -q 1 && return 0
        sleep 2
    done
    echo "database did not become ready — see: docker compose logs db"; exit 1
}

case "${1:-}" in
    up)     docker compose up -d --build && wait_ready && "$0" status ;;
    deploy) [[ -f "${2:-}" ]] || { echo "usage: $0 deploy <union .sql file>"; exit 2; }
            psql "$CONN" -X -v ON_ERROR_STOP=1 -q -f "$2" && echo "deployed $2" ;;
    psql)   psql "$CONN" ;;
    status) psql "$CONN" -X -qAt -F ' ' <<'SQL'
SELECT 'PostgreSQL ' || current_setting('server_version') || ', PostGIS ' || postgis_lib_version();
SELECT 'function: content.' || p.oid::regprocedure::text FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
 WHERE n.nspname='content' ORDER BY 1;
SELECT 'data: ' || tdei_dataset_id, count(*) || ' edges' FROM content.edge GROUP BY 1 ORDER BY 1;
SQL
            ;;
    reset)  read -r -p "Delete the local harness database and all its data? [y/N] " a
            [[ "$a" == y || "$a" == Y ]] || exit 0
            docker compose down -v && "$0" up ;;
    down)   docker compose down ;;
    *)      sed -n '3,12p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
