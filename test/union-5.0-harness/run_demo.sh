#!/usr/bin/env bash
# =============================================================================
# run_demo.sh — the union 5.0 demo: one realistic neighbourhood, two sources,
# one picture of what the union does with them.
#
#   ./run_demo.sh local                  # default rules at 1 m (default_1m), local Docker DB
#   ./run_demo.sh local off              # same data with nothing configured, for contrast
#   ./run_demo.sh "<psql connection>"    # any database (a dev DB, not production)
#   BUILD=1 ./run_demo.sh local          # rebuild demo/data/*.geojson from demo/scene.py first
#
# Inputs: demo/data/ds1_*.geojson (City GIS) and ds2_*.geojson (Field survey),
# the raw OSW files a TDEI upload would hold. They are loaded as datasets
# demo-ds1 / demo-ds2 (any rows under those ids are replaced; nothing else is
# touched), the union runs once with the profile's config, and runs/demo_<profile>/ gets:
#
#   demo.png                 THE PICTURE: input vs output of the whole scene, numbered
#                            spots, a close-up of each with what happened and PASS/FAIL
#   osw_*.geojson, report.json   the union's six output files and run report
#   demo_results.geojson     the numbered spots with PASS / FAIL (default_1m is checked)
#   changes_*.geojson        what changed, input vs output (as for the harness runs)
#   union_log.txt            the union's NOTICE log
#
# Guide: docs/demo.md. QGIS: qgis/load_demo.py.
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")"
ROOT="$PWD"
CONN="${1:-}"
if [[ -z "$CONN" ]]; then sed -n '3,24p' "$0" | sed 's/^# \{0,1\}//'; exit 2; fi
PROFILE="${2:-default_1m}"
[[ "$CONN" == local ]] && CONN="host=localhost port=${HARNESS_PG_PORT:-5434} dbname=tdei user=tdei password=tdei"
PSQL=(psql "$CONN" -X -v ON_ERROR_STOP=1)
DS1=demo-ds1; DS2=demo-ds2
command -v psql >/dev/null || { echo "psql not found — see docs/setup.md"; exit 2; }
CONFIG=$(python3 harness/profiles.py "$PROFILE") || { echo "unknown profile '$PROFILE' (./run_harness.sh list)"; exit 2; }
OUT="runs/demo_$PROFILE"; mkdir -p "$OUT"

if [[ "${BUILD:-0}" == 1 || ! -f demo/data/ds1_edges.geojson ]]; then
    echo "== building demo/data from demo/scene.py"
    python3 demo/build_demo.py || exit 2
fi

echo "== database"
sig=$("${PSQL[@]}" -qAt -c "SELECT p.oid::regprocedure::text FROM pg_proc p JOIN pg_namespace s ON s.oid=p.pronamespace
                           WHERE s.nspname='content' AND p.proname='tdei_union_dataset'") || exit 2
[[ "$sig" == *jsonb* ]] || { echo "   union 5.0 not deployed — ./local_db.sh deploy union/union_5_0.sql"; exit 2; }
n=$("${PSQL[@]}" -qAt -c "SELECT count(*) FROM pg_proc p JOIN pg_namespace s ON s.oid=p.pronamespace
                          WHERE s.nspname='content' AND p.proname='tdei_union_dataset_geojson'
                            AND pg_get_function_identity_arguments(p.oid) LIKE '%jsonb%'")
[[ "$n" != 0 ]] || "${PSQL[@]}" -q -f sql/union_to_geojson.sql || exit 2

echo "== loading demo/data as $DS1 (City GIS) / $DS2 (Field survey)"
python3 harness/load_test_data.py --data-dir demo/data --ds1-id "$DS1" --ds2-id "$DS2" > "$OUT/load_demo.sql" || exit 2
"${PSQL[@]}" -q -f "$OUT/load_demo.sql" | sed 's/^/   /' || exit 2

echo "== union ($PROFILE)  ->  $OUT/"
echo "   config: $CONFIG"
( cd "$OUT" && "${PSQL[@]}" -v ds1="$DS1" -v ds2="$DS2" -v config="$CONFIG" -f "$ROOT/sql/export_union.psql" 2>&1 ) \
    | grep -v 'does not exist, skipping' > "$OUT/union_log.txt"
if [[ ${PIPESTATUS[0]} -ne 0 ]]; then echo "   union failed — see $OUT/union_log.txt"; tail -3 "$OUT/union_log.txt"; exit 1; fi
python3 harness/changes.py "$OUT" --profile "$PROFILE" --data-dir demo/data | sed 's/^/ /'

echo; echo "== scenarios"
python3 demo/check_demo.py "$OUT" --profile "$PROFILE"; rc=$?

if python3 -c "import matplotlib" 2>/dev/null; then
    python3 demo/demo_sheet.py "$OUT" --profile "$PROFILE" && echo "   picture: $OUT/demo.png"
else
    echo "   (pip install matplotlib for the picture)"
fi
exit $rc
