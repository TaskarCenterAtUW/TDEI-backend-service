#!/usr/bin/env bash
# =============================================================================
# perf/run_scale.sh — time union 5.0 on synthetic cities of growing size.
#
#   perf/run_scale.sh "<psql connection>" <profile> <side> [<side> ...]
#   perf/run_scale.sh local default_1m 50 100 200
#
# For each side: builds the city (perf/generate_scale.sql) as datasets
# scale<side>-ds1 / scale<side>-ds2 unless it is already loaded, runs the union
# once with the profile's config (harness/profiles.py), drains every cursor on
# the server (MOVE ALL) and appends one line to runs/perf/results.tsv:
#
#   side, input edges/nodes, union seconds, drain seconds, output counts,
#   temp-table size, output JSON size, per-stage seconds (report timings_s),
#   output checksum (equal checksums = identical output)
#
# The union's NOTICE log and report go to runs/perf/<profile>_<side>.{log,json}.
# Set UNION_FN=<schema>.tdei_union_dataset to time another build of the union
# deployed under its own schema (e.g. the previous release, for a before/after).
# Set PROFILE_SQL=1 to also log every statement over 200 ms with its plan
# (auto_explain) into runs/perf/<profile>_<side>.plans.log; summarise it with
#   python3 perf/slow_statements.py runs/perf/<profile>_<side>.plans.log
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.."
CONN="${1:?connection}"; PROFILE="${2:?profile}"; shift 2
[[ "$CONN" == local ]] && CONN="host=localhost port=${HARNESS_PG_PORT:-5434} dbname=tdei user=tdei password=tdei"
PSQL=(psql "$CONN" -X -v ON_ERROR_STOP=1)
CONFIG=$(python3 harness/profiles.py "$PROFILE") || exit 2
FN="${UNION_FN:-content.tdei_union_dataset}"     # e.g. UNION_FN=perfbase.tdei_union_dataset to time another build
LABEL="$PROFILE"; [[ "$FN" != content.tdei_union_dataset ]] && LABEL="$PROFILE@${FN%%.*}"
OUT=runs/perf; mkdir -p "$OUT"
RES="$OUT/results.tsv"
[[ -f "$RES" ]] || printf 'when\tprofile\tside\tin_nodes\tin_edges\tunion_s\tdrain_s\tout_nodes\tout_edges\ttemp_tables\toutput_json\ttimings_s\tchecksum\n' > "$RES"

for SIDE in "$@"; do
    DS1="scale${SIDE}-ds1"; DS2="scale${SIDE}-ds2"
    have=$("${PSQL[@]}" -qAt -c "SELECT count(*) FROM content.edge WHERE tdei_dataset_id = '$DS1'")
    if [[ "$have" == 0 ]]; then
        echo "== building side $SIDE"
        "${PSQL[@]}" -q -v ds1="$DS1" -v ds2="$DS2" -v side="$SIDE" -f perf/generate_scale.sql || exit 1
    fi
    LOG="$OUT/${LABEL}_${SIDE}.log"; REP="$OUT/${LABEL}_${SIDE}.json"
    PRE=""
    if [[ "${PROFILE_SQL:-0}" == 1 ]]; then
        PRE="LOAD 'auto_explain'; SET auto_explain.log_min_duration = 200; SET auto_explain.log_nested_statements = on;
             SET auto_explain.log_analyze = on; SET auto_explain.log_timing = off; SET client_min_messages = log;"
    fi
    echo "== union $LABEL on side $SIDE"
    "${PSQL[@]}" -v fn="$FN" -v ds1="$DS1" -v ds2="$DS2" -v config="$CONFIG" -v report="$REP" -v pre="$PRE" \
        -f perf/run_once.psql > "$LOG" 2>&1 || { tail -5 "$LOG"; exit 1; }
    [[ "${PROFILE_SQL:-0}" == 1 ]] && grep -q "LOG:  duration" "$LOG" && cp "$LOG" "$OUT/${LABEL}_${SIDE}.plans.log"
    python3 - "$LOG" "$REP" "$RES" "$LABEL" "$SIDE" <<'PY'
import json, re, sys, datetime
log, rep, res, profile, side = sys.argv[1:]
txt = open(log).read()
times = [float(t) / 1000 for t in re.findall(r'^Time: ([0-9.]+) ms', txt, re.M)]
union_s = times[1] if len(times) > 1 else float('nan')        # [0] is BEGIN
drain_s = sum(times[2:8])
temp = re.search(r'temp_tables\s*\n-+\n\s*(.+?)\n', txt); outj = re.search(r'output_json\s*\n-+\n\s*(.+?)\n', txt)
cks = re.search(r'output_checksum\s*\n-+\n\s*(.+?)\n', txt)
r = json.load(open(rep))
c = r['counts']
row = [datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), profile, side, c['input']['nodes'], c['input']['edges'],
       f'{union_s:.1f}', f'{drain_s:.1f}', c['output']['nodes'], c['output']['edges'],
       temp.group(1).strip() if temp else '', outj.group(1).strip() if outj else '',
       json.dumps(r.get('timings_s', {}), separators=(',', ':')), cks.group(1).strip()[:12] if cks else '']
open(res, 'a').write('\t'.join(map(str, row)) + '\n')
print(f"   input {c['input']['edges']:,} edges / {c['input']['nodes']:,} nodes -> union {union_s:.1f} s, drain {drain_s:.1f} s, "
      f"temp {row[9]}, output {row[10]}, checksum {row[12]}")
if r.get('timings_s'):
    order = ['load', 'types', 'snap', 'dedup', 'snap_topology', 'connect', 'stranding_guard', 'extensions', 'output']
    print('   stages: ' + ', '.join(f'{k} {r["timings_s"][k]:.1f}' for k in order if k in r['timings_s']))
PY
done
