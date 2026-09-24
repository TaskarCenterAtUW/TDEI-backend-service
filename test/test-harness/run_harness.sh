#!/usr/bin/env bash
# =============================================================================
# run_harness.sh — run the union on the harness test data and check the result.
#
#   ./run_harness.sh local                            # the local Docker database (docker-compose.yml)
#   ./run_harness.sh "<psql connection>"              # proximity 1 m and 3 m
#   ./run_harness.sh "<psql connection>" 3            # just 3 m
#   LOAD=1 ./run_harness.sh "<psql connection>"       # (re)load the test data first
#   ./run_harness.sh local filters f_crossing         # one filter scenario (harness/scenarios.py) + its baseline
#   ./run_harness.sh local filters f_crossing n_kerb  # ONE run with both scenarios' filters combined
#   ./run_harness.sh local filters .                  # every scenario, each its own run
#
# Connection: anything psql accepts, e.g.
#   "host=myserver.postgres.database.azure.com port=5432 dbname=tdei user=me sslmode=require"
#   Put the password in ~/.pgpass or export PGPASSWORD — not on the command line.
#
# Dataset ids default to harness-ds1 / harness-ds2 (what harness/load_test_data.py uses).
# If you uploaded the test data through TDEI instead, pass its ids:
#   DS1=<id> DS2=<id> ./run_harness.sh "<psql connection>"
#
# Each proximity gets its own folder, runs/prox_<p>/, holding the union output,
# the union's NOTICE log, the PASS/FAIL layers and case_sheet.png. Exit code 0 only if every run passes.
# Full instructions: docs/running.md (setup: docs/setup.md, filters: docs/filter-scenarios.md).
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")"
HARNESS_ROOT="$PWD"

CONN="${1:-}"
if [[ -z "$CONN" ]]; then
    sed -n '3,23p' "$0" | sed 's/^# \{0,1\}//'; exit 2
fi
shift
[[ "$CONN" == local ]] && CONN="host=localhost port=${HARNESS_PG_PORT:-5433} dbname=tdei user=tdei password=tdei"
PROXIMITIES=("$@"); [[ ${#PROXIMITIES[@]} -eq 0 ]] && PROXIMITIES=(1 3)
DS1="${DS1:-harness-ds1}"; DS2="${DS2:-harness-ds2}"

command -v psql    >/dev/null || { echo "psql not found. macOS: brew install libpq && brew link --force libpq"; exit 2; }
command -v python3 >/dev/null || { echo "python3 not found"; exit 2; }
PSQL=(psql "$CONN" -X -v ON_ERROR_STOP=1)

# Arguments are either proximities (numbers) or "filters <scenario...>".
# Scenario names given without "filters" are routed to filter mode; anything
# else that is not a number is rejected before touching the database.
if [[ "${PROXIMITIES[0]}" != filters ]]; then
    bad=(); for p in "${PROXIMITIES[@]}"; do [[ "$p" =~ ^[0-9]+([.][0-9]+)?$ ]] || bad+=("$p"); done
    if [[ ${#bad[@]} -gt 0 ]]; then
        if [[ ${#bad[@]} -eq ${#PROXIMITIES[@]} ]] && python3 harness/scenarios.py "${PROXIMITIES[@]}" >/dev/null 2>&1; then
            echo "note: '${PROXIMITIES[*]}' are filter scenarios — running as: filters ${PROXIMITIES[*]}"
            PROXIMITIES=(filters "${PROXIMITIES[@]}")
        else
            echo "not a proximity: ${bad[*]}"
            echo "  proximities are numbers (e.g. 1 3); filter scenarios need the keyword: filters <name> [<name>...] | filters ."
            python3 harness/scenarios.py >/dev/null   # prints the scenario list
            exit 2
        fi
    fi
fi

echo "== 1. database"
"${PSQL[@]}" -qAt -c "SELECT current_database() || ' on ' || coalesce(inet_server_addr()::text,'local socket')" || exit 2
for fn in tdei_union_dataset tdei_union_dataset_geojson; do
    n=$("${PSQL[@]}" -qAt -c "SELECT count(*) FROM pg_proc p JOIN pg_namespace s ON s.oid=p.pronamespace
                              WHERE s.nspname='content' AND p.proname='$fn'")
    if [[ "$n" == "0" ]]; then
        if [[ $fn == tdei_union_dataset_geojson ]]; then
            echo "   deploying content.$fn (sql/union_to_geojson.sql)"
            "${PSQL[@]}" -q -f sql/union_to_geojson.sql || exit 2
        else
            echo "   content.$fn is not deployed in this database — deploy your union .sql first"; exit 2
        fi
    else
        echo "   content.$fn: present"
    fi
done

if [[ "${LOAD:-0}" == "1" ]]; then
    echo "== 2. loading test data as $DS1 / $DS2"
    mkdir -p runs && python3 harness/load_test_data.py --ds1-id "$DS1" --ds2-id "$DS2" > runs/load_test_data.sql || exit 2
    "${PSQL[@]}" -q -f runs/load_test_data.sql || { echo "   load failed — see docs/setup.md, 'Loading the test data into your own database'"; exit 2; }
else
    echo "== 2. test data (set LOAD=1 to reload)"
    "${PSQL[@]}" -qAt -F ' ' -c "SELECT '   '||tdei_dataset_id, count(*) || ' edges' FROM content.edge
                                 WHERE tdei_dataset_id IN ('$DS1','$DS2') GROUP BY 1 ORDER BY 1"
    have=$("${PSQL[@]}" -qAt -c "SELECT count(DISTINCT tdei_dataset_id) FROM content.edge WHERE tdei_dataset_id IN ('$DS1','$DS2')")
    [[ "$have" == "2" ]] || { echo "   $DS1 / $DS2 not both found — rerun with LOAD=1"; exit 2; }
fi

overall=0
# run_one <out-dir> <proximity> [<filters-json> <scenario> <baseline-dir>]
run_one() {
    local out="$1" p="$2" filt="${3:-}" scen="${4:-}" base="${5:-}"
    local fargs=() cargs=()
    [[ -n "$filt" ]] && fargs=(-v filters="$filt")
    [[ -n "$scen" ]] && cargs=(--scenario "$scen" --baseline "$base")
    mkdir -p "$out"
    echo; echo "== union at ${p} m${scen:+, scenario $scen}  ->  $out/"
    ( cd "$out" && "${PSQL[@]}" -v ds1="$DS1" -v ds2="$DS2" -v prox="$p" "${fargs[@]}" -f "$HARNESS_ROOT/sql/export_union.psql" 2>&1 ) \
        | grep --line-buffered -v 'does not exist, skipping' > "$out/union_log.txt"
    if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
        echo "   union failed — see $out/union_log.txt"; tail -3 "$out/union_log.txt"; overall=1; return
    fi
    python3 harness/check_cases.py "$out" --proximity "$p" "${cargs[@]}" > "$out/check_report.txt"
    local rc=$?
    grep -E "cases passed|Filter scenario|vs baseline|^    (PASS|FAIL)|^  FAIL|^          x" "$out/check_report.txt" | sed 's/^/   /'
    [[ $rc -ne 0 ]] && overall=1
    if python3 -c "import matplotlib" 2>/dev/null; then
        python3 harness/case_sheet.py "$out" --proximity "$p" >/dev/null 2>&1 && echo "   sheet: $out/case_sheet.png"
    fi
}

if [[ "${PROXIMITIES[0]}" == filters ]]; then
    # filters <name>         one scenario
    # filters <name> <name>  ONE run with their filters combined
    # filters .              every scenario, each its own run        (+ baselines)
    scen_lines=$(python3 harness/scenarios.py "${PROXIMITIES[@]:1}") || exit 2
    for p in $(cut -f2 <<< "$scen_lines" | sort -u); do
        run_one "runs/prox_$p" "$p"                       # baseline: same proximity, no filters
    done
    while IFS=$'\t' read -r name p filt; do
        run_one "runs/$name" "$p" "$filt" "$name" "runs/prox_$p"
    done <<< "$scen_lines"
else
    for p in "${PROXIMITIES[@]}"; do run_one "runs/prox_$p" "$p"; done
fi

echo
[[ $overall -eq 0 ]] && echo "ALL RUNS PASSED" || echo "SOME RUNS FAILED — see runs/*/check_report.txt, case_sheet.png, or QGIS"
exit $overall
