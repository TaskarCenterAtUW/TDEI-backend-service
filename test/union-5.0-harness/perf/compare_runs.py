"""
Compare two sets of harness runs file by file: the six OSW files must be
identical, and the reports identical apart from timings_s.

    python3 perf/compare_runs.py <runs dir A> <runs dir B>
"""
import json, os, sys

a, b = sys.argv[1:3]
bad = 0
for prof in sorted(os.listdir(a)):
    pa, pb = os.path.join(a, prof), os.path.join(b, prof)
    if not os.path.isdir(pa) or not os.path.exists(os.path.join(pa, 'report.json')):
        continue
    diffs = []
    for f in ['osw_nodes', 'osw_edges', 'osw_zones', 'osw_points', 'osw_lines', 'osw_polygons']:
        x, y = (open(os.path.join(p, f + '.geojson')).read() for p in (pa, pb))
        if x != y:
            diffs.append(f)
    ra, rb = (json.load(open(os.path.join(p, 'report.json'))) for p in (pa, pb))
    ra.pop('timings_s', None); rb.pop('timings_s', None)
    if ra != rb:
        diffs.append('report')
    print(f"{prof:16s} {'identical' if not diffs else 'DIFFERENT: ' + ', '.join(diffs)}")
    bad += bool(diffs)
sys.exit(1 if bad else 0)
