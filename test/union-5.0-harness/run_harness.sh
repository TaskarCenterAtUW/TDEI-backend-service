#!/usr/bin/env bash
# =============================================================================
# run_harness.sh — run union 5.0 on the harness test data and check the result.
#
#   ./run_harness.sh local                          # every profile, on the local Docker database
#   ./run_harness.sh "<psql connection>"            # every profile, on any database
#   ./run_harness.sh local default_3m custom_snap   # just these profiles
#   ./run_harness.sh local validate                 # the config-validation suite only
#   LOAD=1 ./run_harness.sh local                   # (re)load the test data first
#   ./run_harness.sh list                           # list the profiles
#
# A PROFILE is one union config (harness/profiles.py): off, default_1m,
# default_3m, custom_snap, custom_anchor, connect_only, custom_dedup_angle, custom_extensions,
# ext_lines_polygons, ext_points_zones, custom_connect. Each profile runs the
# union once and writes runs/<profile>/: the six OSW files, report.json, the
# union's NOTICE log, check_report.txt, the PASS/FAIL map layers and
# case_sheet.png. Running every profile also runs the validation suite.
#
# Connection: anything psql accepts, e.g.
#   "host=myserver.postgres.database.azure.com port=5432 dbname=tdei user=me sslmode=require"
#   Put the password in ~/.pgpass or export PGPASSWORD — not on the command line.
# Dataset ids default to harness5-ds1 / harness5-ds2 (what harness/load_test_data.py
# uses); if you uploaded the test data through TDEI, pass DS1=<id> DS2=<id>.
#
# Exit code 0 only if every run passes. Full instructions: docs/running.md.
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")"
HARNESS_ROOT="$PWD"

CONN="${1:-}"
if [[ -z "$CONN" ]]; then
    sed -n '3,25p' "$0" | sed 's/^# \{0,1\}//'; exit 2
fi
if [[ "$CONN" == list ]]; then python3 harness/profiles.py; exit 0; fi
shift
[[ "$CONN" == local ]] && CONN="host=localhost port=${HARNESS_PG_PORT:-5434} dbname=tdei user=tdei password=tdei"
DS1="${DS1:-harness5-ds1}"; DS2="${DS2:-harness5-ds2}"

command -v psql    >/dev/null || { echo "psql not found. macOS: brew install libpq && brew link --force libpq"; exit 2; }
command -v python3 >/dev/null || { echo "python3 not found"; exit 2; }
PSQL=(psql "$CONN" -X -v ON_ERROR_STOP=1)

# what to run: profiles and/or the validation suite
ALL_PROFILES=($(python3 -c "import sys; sys.path.insert(0, 'harness'); import profiles; print(' '.join(profiles.PROFILES))"))
RUN_PROFILES=(); RUN_VALIDATE=0
if [[ $# -eq 0 || "${1:-}" == all ]]; then
    RUN_PROFILES=("${ALL_PROFILES[@]}"); RUN_VALIDATE=1
else
    for a in "$@"; do
        if [[ "$a" == validate ]]; then RUN_VALIDATE=1
        elif [[ " ${ALL_PROFILES[*]} " == *" $a "* ]]; then RUN_PROFILES+=("$a")
        else echo "unknown profile '$a' — one of: ${ALL_PROFILES[*]}, validate, all"; exit 2
        fi
    done
fi

echo "== 1. database"
"${PSQL[@]}" -qAt -c "SELECT current_database() || ' on ' || coalesce(inet_server_addr()::text,'local socket')" || exit 2
sig=$("${PSQL[@]}" -qAt -c "SELECT p.oid::regprocedure::text FROM pg_proc p JOIN pg_namespace s ON s.oid=p.pronamespace
                           WHERE s.nspname='content' AND p.proname='tdei_union_dataset'")
if [[ "$sig" != *"jsonb"* || "$sig" == *"real"* ]]; then
    echo "   content.tdei_union_dataset(varchar, varchar, jsonb) is not deployed (found: ${sig:-nothing})"
    echo "   deploy union/union_5_0.sql first:  ./local_db.sh deploy union/union_5_0.sql"; exit 2
fi
echo "   union: $sig"
n=$("${PSQL[@]}" -qAt -c "SELECT count(*) FROM pg_proc p JOIN pg_namespace s ON s.oid=p.pronamespace
                          WHERE s.nspname='content' AND p.proname='tdei_union_dataset_geojson'
                            AND pg_get_function_identity_arguments(p.oid) LIKE '%jsonb%'
                            AND pg_get_function_identity_arguments(p.oid) NOT LIKE '%real%'")
if [[ "$n" == "0" ]]; then
    echo "   deploying content.tdei_union_dataset_geojson (sql/union_to_geojson.sql)"
    "${PSQL[@]}" -q -f sql/union_to_geojson.sql || exit 2
fi

overall=0
if [[ ${#RUN_PROFILES[@]} -gt 0 ]]; then
    if [[ "${LOAD:-0}" == "1" ]]; then
        echo "== 2. loading test data as $DS1 / $DS2"
        mkdir -p runs && python3 harness/load_test_data.py --ds1-id "$DS1" --ds2-id "$DS2" > runs/load_test_data.sql || exit 2
        "${PSQL[@]}" -q -f runs/load_test_data.sql || { echo "   load failed — see docs/setup.md"; exit 2; }
    else
        echo "== 2. test data (set LOAD=1 to reload)"
        "${PSQL[@]}" -qAt -F ' ' -c "SELECT '   '||tdei_dataset_id, count(*) || ' edges' FROM content.edge
                                     WHERE tdei_dataset_id IN ('$DS1','$DS2') GROUP BY 1 ORDER BY 1"
        have=$("${PSQL[@]}" -qAt -c "SELECT count(DISTINCT tdei_dataset_id) FROM content.edge WHERE tdei_dataset_id IN ('$DS1','$DS2')")
        [[ "$have" == "2" ]] || { echo "   $DS1 / $DS2 not both found — rerun with LOAD=1"; exit 2; }
    fi

    for prof in "${RUN_PROFILES[@]}"; do
        out="runs/$prof"; mkdir -p "$out"
        cfg=$(python3 harness/profiles.py "$prof") || exit 2
        echo; echo "== $prof  ->  $out/"
        echo "   config: $cfg"
        ( cd "$out" && "${PSQL[@]}" -v ds1="$DS1" -v ds2="$DS2" -v config="$cfg" -f "$HARNESS_ROOT/sql/export_union.psql" 2>&1 ) \
            | grep --line-buffered -v 'does not exist, skipping' > "$out/union_log.txt"
        if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
            echo "   union failed — see $out/union_log.txt"; tail -3 "$out/union_log.txt"; overall=1; continue
        fi
        python3 harness/check_cases.py "$out" --profile "$prof" > "$out/check_report.txt"
        rc=$?
        grep -E "cases passed|^  FAIL|^    (BROKEN|STRANDED|JUNCTION|T-SPAN)|^          x|^\[  routability\]" "$out/check_report.txt" | sed 's/^/   /'
        [[ $rc -ne 0 ]] && overall=1
        if [[ -f harness/case_sheet.py ]] && python3 -c "import matplotlib" 2>/dev/null; then
            python3 harness/case_sheet.py "$out" --profile "$prof" >/dev/null 2>&1 && echo "   sheet: $out/case_sheet.png"
        fi
    done
fi

if [[ $RUN_VALIDATE -eq 1 ]]; then
    echo; echo "== config validation (harness/config_cases.py)"
    mkdir -p runs
    python3 harness/config_cases.py --sql > runs/config_cases.sql || exit 2
    "${PSQL[@]}" -qAt -f runs/config_cases.sql > runs/config_results.txt 2>&1
    python3 harness/config_cases.py --check runs/config_results.txt > runs/config_report.txt
    rc=$?
    grep -E "passed|^  FAIL|^      " runs/config_report.txt | sed 's/^/   /'
    [[ $rc -ne 0 ]] && overall=1
fi

echo
[[ $overall -eq 0 ]] && echo "ALL RUNS PASSED" || echo "SOME RUNS FAILED — see runs/*/check_report.txt, case_sheet.png, or QGIS"
exit $overall
