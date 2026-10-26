#!/usr/bin/env bash
# =============================================================================
# run_demo.sh — the union 5.0 demo: one realistic neighbourhood, two sources,
# one picture of what the union does with them under the config you give it.
#
#   ./run_demo.sh local                      # the config in demo/input.json -> runs/demo_input/
#   ./run_demo.sh local my_config.json       # any config file               -> runs/demo_my_config/
#   ./run_demo.sh local default_1m           # a harness profile             -> runs/demo_default_1m/
#   ./run_demo.sh local off                  #   (nothing configured, for contrast)
#   ./run_demo.sh "<psql connection>" ...    # any database (a dev DB, not production)
#   BUILD=1 ./run_demo.sh local              # rebuild demo/data/*.geojson from demo/scene.py first
#
# Try a config: open docs/union-configurator.html, set it up, press "Copy
# configuration", paste it into demo/input.json (replace the "config" value, or
# the whole file), run ./run_demo.sh local and open runs/demo_input/demo.png.
# A bare config, {"config": {...}} or a whole union request body all work.
#
# Inputs: demo/data/ds1_*.geojson (City GIS) and ds2_*.geojson (Field survey),
# the raw OSW files a TDEI upload would hold. They are loaded as datasets
# demo-ds1 / demo-ds2 (any rows under those ids are replaced; nothing else is
# touched), the union runs once with the config, and the run folder gets:
#
#   demo.png                 THE PICTURE: the applied config with what each rule did,
#                            input vs output of the whole scene, and a close-up of every
#                            numbered spot saying what this run did there
#   osw_*.geojson, report.json   the union's six output files and run report
#   run_config.json          the config as given, as the union applied it, and the defaults
#   demo_results.geojson     the numbered spots: status and what happened (for QGIS)
#   changes_*.geojson        what changed, input vs output (as for the harness runs)
#   union_log.txt            the union's NOTICE log
#
# Guide: docs/demo.md. QGIS: qgis/load_demo.py.
# =============================================================================
set -uo pipefail
CONN="${1:-}"
SRC="${2:-}"
[[ -n "$SRC" && -f "$SRC" ]] && SRC="$(cd "$(dirname "$SRC")" && pwd)/$(basename "$SRC")"   # relative to the caller
cd "$(dirname "$0")"
ROOT="$PWD"
if [[ -z "$CONN" ]]; then sed -n '3,32p' "$0" | sed 's/^# \{0,1\}//'; exit 2; fi
LABEL="${2:-demo/input.json}"
SRC="${SRC:-demo/input.json}"
[[ "$CONN" == local ]] && CONN="host=localhost port=${HARNESS_PG_PORT:-5434} dbname=tdei user=tdei password=tdei"
PSQL=(psql "$CONN" -X -v ON_ERROR_STOP=1)
DS1=demo-ds1; DS2=demo-ds2
command -v psql >/dev/null || { echo "psql not found — see docs/setup.md"; exit 2; }

if [[ -f "$SRC" || "$SRC" == *.json ]]; then
    [[ -f "$SRC" ]] || { echo "no such file: $LABEL"; exit 2; }
    CONFIG=$(python3 demo/read_input.py "$SRC") || exit 2
    NAME=$(basename "$SRC"); NAME="${NAME%.json}"
else
    CONFIG=$(python3 harness/profiles.py "$SRC" 2>/dev/null) \
        || { echo "'$LABEL' is neither a .json config file nor a profile (./run_harness.sh list)"; exit 2; }
    NAME="$SRC"; LABEL="profile $SRC"
fi
OUT="runs/demo_$(printf '%s' "$NAME" | sed 's/[^A-Za-z0-9_.-]/_/g')"

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

echo "== config: $LABEL"
validate() { echo "SELECT content.union5_validate_config(:'cfg'::jsonb);" | "${PSQL[@]}" -qAt -v cfg="$1" 2>&1; }
if ! EFFECTIVE=$(validate "$CONFIG"); then
    echo "   the union rejects this config:"
    echo "$EFFECTIVE" | sed -n 's/^.*ERROR: *//p' | sed 's/^/     /'
    echo "   (sent: ${CONFIG:0:300})"
    exit 2
fi
DEFAULT_EFFECTIVE=$(validate "$(python3 harness/profiles.py default_1m)") || { echo "$DEFAULT_EFFECTIVE"; exit 2; }
echo "   ${CONFIG:0:300}$([[ ${#CONFIG} -gt 300 ]] && echo ' …')"
mkdir -p "$OUT"
python3 - "$OUT/run_config.json" "$LABEL" "$CONFIG" "$EFFECTIVE" "$DEFAULT_EFFECTIVE" <<'EOF' || exit 2
import json, sys
path, source, config, effective, default = sys.argv[1:]
json.dump({"source": source, "config": json.loads(config), "effective": json.loads(effective),
           "default_1m_effective": json.loads(default)}, open(path, "w"), indent=1)
EOF
PROX=$(python3 -c 'import json, sys
s = json.loads(sys.argv[1]).get("snap", {})
p = [r.get("proximity", 0) for r in s.get("rules", [])] + [(s.get("others") or {}).get("proximity", 0)]
print(max(p) or 1)' "$EFFECTIVE")

echo "== loading demo/data as $DS1 (City GIS) / $DS2 (Field survey)"
python3 harness/load_test_data.py --data-dir demo/data --ds1-id "$DS1" --ds2-id "$DS2" > "$OUT/load_demo.sql" || exit 2
"${PSQL[@]}" -q -f "$OUT/load_demo.sql" | sed 's/^/   /' || exit 2

echo "== union  ->  $OUT/"
( cd "$OUT" && "${PSQL[@]}" -v ds1="$DS1" -v ds2="$DS2" -v config="$CONFIG" -f "$ROOT/sql/export_union.psql" 2>&1 ) \
    | grep -v 'does not exist, skipping' > "$OUT/union_log.txt"
if [[ ${PIPESTATUS[0]} -ne 0 ]]; then echo "   union failed — see $OUT/union_log.txt"; tail -3 "$OUT/union_log.txt"; exit 1; fi
python3 harness/changes.py "$OUT" --proximity "$PROX" --data-dir demo/data | sed 's/^/ /'

echo
python3 demo/check_demo.py "$OUT"; rc=$?

if python3 -c "import matplotlib" 2>/dev/null; then
    python3 demo/demo_sheet.py "$OUT" && echo "   picture: $OUT/demo.png"
else
    echo "   (pip install matplotlib for the picture)"
fi
exit $rc
