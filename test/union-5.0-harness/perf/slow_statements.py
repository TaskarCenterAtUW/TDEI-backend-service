"""
Rank the statements inside a union run by duration, from a psql log captured
with auto_explain (PROFILE_SQL=1 perf/run_scale.sh ...). Prints each
statement's duration, its first line and its top plan node.

    python3 perf/slow_statements.py runs/perf/<profile>_<side>.plans.log [top_n]
"""
import re
import sys

PLAN_NODE = re.compile(r'\s*(->\s+)?[A-Z][A-Za-z ]+(on|Join|Scan|Loop|Aggregate|Sort|Unique|Append|Result|Hash|'
                       r'Limit|Subquery|WindowAgg|Materialize|Gather|SetOp|Recursive|CTE|Insert|Update|Delete|Function)')

text = open(sys.argv[1]).read()
top = int(sys.argv[2]) if len(sys.argv) > 2 else 25
rows = []
for chunk in re.split(r'\n(?=[^\n]*LOG:  duration: )', text):
    m = re.search(r'LOG:  duration: ([0-9.]+) ms', chunk)
    if not m:
        continue
    q = re.search(r'Query Text: ([^\n]*)', chunk)
    stmt = q.group(1).strip() if q else ''
    if 'tdei_union_dataset(' in stmt and stmt.startswith('SELECT'):
        continue                                   # the whole call, not a statement inside it
    plan = [l.strip() for l in chunk.split('\n') if PLAN_NODE.match(l)]
    first = next((l for l in plan if l.startswith(('Insert', 'Update', 'Delete'))), plan[0] if plan else '')
    rows.append((float(m.group(1)), stmt[:110], first[:90]))

print(f"{len(rows)} statements over the threshold, {sum(r[0] for r in rows) / 1000:.1f} s in all")
for ms, stmt, node in sorted(rows, reverse=True)[:top]:
    print(f"{ms / 1000:8.2f} s  {stmt}\n            {node}")
