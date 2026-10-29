"""
Check a demo run spot by spot, and say what it did at each one.

    python3 demo/check_demo.py runs/demo_input           (run_demo.sh runs this for you)

Prints the applied configuration with what every rule did, then each numbered
spot: what this run did there (demo/describe.py) and how that compares with the
default rules. The checks in demo/scene.py describe the published default rules
at 1 m, so:

  run with exactly the default config   each spot PASS or FAIL (a FAIL is a bug)
  run with any other config              each spot "same as default" or
                                         "differs from default" (neither is an error)

"Exactly the default" compares the union's effective config (report.json) with
the default one run_demo.sh stored in run_config.json, so a pasted config that
spells the defaults out in full counts too. When comparing another config, rule
names are not compared (a configurator config names its rules its own way): a
connector or junction made by any rule counts.

Writes <run>/demo_results.geojson (the spots with status and text, for the
picture and QGIS). Exit code 1 only when a default run has a FAIL.
"""
import argparse, json, math, os, sys, textwrap

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import scene, describe
from scene import C, M

ap = argparse.ArgumentParser()
ap.add_argument("run_dir")
ap.add_argument("--data-dir", default=os.path.join(HERE, "data"))
a = ap.parse_args()

run = describe.Run(a.run_dir, a.data_dir)
nodes, edges, points, polys = run.out["nodes"], run.out["edges"], run.out["points"], run.out["polygons"]
out_lines, zones = run.out["lines"], run.out["zones"]
def line_mid(f): return describe._midpoint(describe._xy(f["geometry"]["coordinates"]))
def ring(f): return describe._xy(f["geometry"]["coordinates"][0])
def zone_mid(f): return describe._centroid(ring(f))
rc_path = os.path.join(a.run_dir, "run_config.json")
run_config = json.load(open(rc_path)) if os.path.exists(rc_path) else {}
is_default = bool(run.effective) and run.effective == run_config.get("default_1m_effective")

def dist(p, q): return math.hypot(p[0] - q[0], p[1] - q[1])
def xy(c): return M(*c)

def seg_dist(p, a_, b_):
    (px, py), (ax, ay), (bx, by) = p, a_, b_
    dx, dy = bx - ax, by - ay
    L = dx * dx + dy * dy
    t = 0 if L == 0 else max(0, min(1, ((px - ax) * dx + (py - ay) * dy) / L))
    return math.hypot(px - ax - t * dx, py - ay - t * dy)

def node_at(p):
    near = [n for n in nodes if dist(xy(n["geometry"]["coordinates"]), p) <= 0.05]
    return near[0] if near else None

def has(props, want):
    bad = []
    for k, v in want.items():
        if k == "ext:union_rule" and not is_default and v not in (None, scene.ABSENT):
            v = None                     # another config may name its rules differently: any rule will do
        if v is None and k not in props: bad.append(f"{k} missing")
        elif v == scene.ABSENT and k in props: bad.append(f"{k}={props[k]} present")
        elif v not in (None, scene.ABSENT) and str(props.get(k)) != str(v): bad.append(f"{k}={props.get(k)} (want {v})")
    return bad

def run_check(c):
    kind = c[0]
    if kind == "node":
        n = node_at(c[1])
        return [f"no node at {c[1]}"] if not n else [f"node at {c[1]}: {b}" for b in has(n["properties"], c[2])]
    if kind == "no_node":
        return [f"a node remains at {c[1]}"] if node_at(c[1]) else []
    conns = [e for e in edges if "ext:union_rule" in e["properties"]]
    def near_line(e, p, r):
        pts = [xy(q) for q in e["geometry"]["coordinates"]]
        return min(seg_dist(p, pts[i], pts[i + 1]) for i in range(len(pts) - 1)) <= r
    if kind == "connector":
        ok = [e for e in conns if near_line(e, c[1], 0.5)
              and (not is_default or c[2] in e["properties"]["ext:union_rule"].split(","))]
        return [] if ok else [f"no {c[2]} connector at {c[1]}"]
    if kind == "no_connector":
        bad = [e for e in conns if near_line(e, c[1], c[2])]
        return [f"{len(bad)} connector(s) at {c[1]}"] if bad else []
    if kind == "edges":
        x0, x1, y0, y1 = c[3]
        inside = [e for e in edges if e["properties"].get("name") == c[1]
                  and all(x0 <= x <= x1 and y0 <= y <= y1 for x, y in map(xy, e["geometry"]["coordinates"]))]
        return [] if len(inside) == c[2] else [f"{len(inside)} '{c[1]}' edges there (want {c[2]})"]
    if kind == "points":
        n = sum(1 for f in points if dist(xy(f["geometry"]["coordinates"]), c[1]) <= c[2])
        return [] if n == c[3] else [f"{n} points within {c[2]} m of {c[1]} (want {c[3]})"]
    if kind == "point":
        if not points: return ["no output points"]
        f = min(points, key=lambda f: dist(xy(f["geometry"]["coordinates"]), c[1]))
        return [f"point near {c[1]}: {b}" for b in has(f["properties"], c[2])]
    if kind == "polygon":
        f = [p for p in polys if p["properties"].get("name") == c[1]]
        return [f"no polygon '{c[1]}'"] if not f else [f"{c[1]}: {b}" for b in has(f[0]["properties"], c[2])]
    if kind == "polygons":
        n = sum(1 for p in polys if p["properties"].get("name") == c[1])
        return [] if n == c[2] else [f"{n} polygons named '{c[1]}' (want {c[2]})"]
    if kind == "polygon_covers":
        f = [p for p in polys if p["properties"].get("name") == c[1]]
        return [f"no polygon '{c[1]}'"] if not f else [] if describe._contains(ring(f[0]), c[2]) \
            else [f"'{c[1]}' does not reach {c[2]}"]
    if kind == "lines":
        n = sum(1 for f in out_lines if dist(line_mid(f), c[1]) <= c[2])
        return [] if n == c[3] else [f"{n} lines within {c[2]} m of {c[1]} (want {c[3]})"]
    if kind == "line":
        if not out_lines: return ["no output lines"]
        f = min(out_lines, key=lambda f: dist(line_mid(f), c[1]))
        return [f"line near {c[1]}: {b}" for b in has(f["properties"], c[2])]
    if kind == "zones":
        n = sum(1 for f in zones if dist(zone_mid(f), c[1]) <= c[2])
        return [] if n == c[3] else [f"{n} zones within {c[2]} m of {c[1]} (want {c[3]})"]
    if kind == "zone":
        if not zones: return ["no output zones"]
        f = min(zones, key=lambda f: dist(zone_mid(f), c[1]))
        return [f"zone near {c[1]}: {b}" for b in has(f["properties"], c[2])]
    raise ValueError(kind)

source = run_config.get("source", a.run_dir)
print(f"  config: {source}" + ("  (the published default rules at 1 m)" if is_default else ""))
for w in run.report.get("warnings", []):
    print(f"  warning: {w}")
print("\n== what each rule did")
for stage, head, lines in describe.config_columns(run):
    print(f"  {stage:<10} {head}")
    for l in lines:
        print(f"  {'':<10}   {l}")

print("\n== the numbered spots" + ("" if is_default else "   (checks describe the default rules: compared, not judged)"))
results, n_same = [], 0
for s in scene.SCENARIOS:
    fails = [m for c in s["checks"] for m in run_check(c)]
    if is_default:
        status = "PASS" if not fails else "FAIL"
    else:
        status = "same as default" if not fails else "differs from default"
    n_same += not fails
    events = describe.spot_events(run, s["look"])
    results.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": C(*s["at"])}, "properties": {
        "n": s["n"], "title": s["title"], "status": status,
        "label": f'{s["n"]}. {s["title"]} — {status}',
        "input": s["input"], "this_run": " ".join(events), "default_result": s["result"],
        "differences": "; ".join(fails)}})
    print(f"  {s['n']:2}. {s['title']:<40} {status}" + (f"   {'; '.join(fails)}" if fails and is_default else ""))
    for e in events:
        print(textwrap.fill(e, 110, initial_indent="        • ", subsequent_indent="          "))
json.dump({"type": "FeatureCollection", "features": results},
          open(os.path.join(a.run_dir, "demo_results.geojson"), "w"), indent=1)
total = len(scene.SCENARIOS)
if is_default:
    print(f"\n  {n_same}/{total} spots behave as described")
else:
    print(f"\n  {n_same}/{total} spots give the same result as the default rules, {total - n_same} differ")
sys.exit(1 if is_default and n_same < total else 0)
