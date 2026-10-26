"""
Check a demo run against what each numbered scenario says the union does.

    python3 demo/check_demo.py runs/demo_default_1m --profile default_1m

The checks (demo/scene.py) describe the published default rules at 1 m, so
they run for default_1m only; other profiles are drawn but marked "not
checked". Writes <run>/demo_results.geojson (the scenario spots with PASS / FAIL,
for the picture and QGIS) and prints one line per scenario. Exit code 0 when
every scenario passes (or none is checked).
"""
import argparse, json, math, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import scene
from scene import C, M

ap = argparse.ArgumentParser()
ap.add_argument("run_dir")
ap.add_argument("--profile", required=True)
a = ap.parse_args()

def load(name):
    p = os.path.join(a.run_dir, name)
    return json.load(open(p))["features"] if os.path.exists(p) else []

nodes = load("osw_nodes.geojson")
edges = load("osw_edges.geojson")
points = load("osw_points.geojson")
polys = load("osw_polygons.geojson")

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
        ok = [e for e in conns if near_line(e, c[1], 0.5) and c[2] in e["properties"]["ext:union_rule"].split(",")]
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
    raise ValueError(kind)

checked = a.profile == "default_1m"
results, n_ok = [], 0
for s in scene.SCENARIOS:
    fails = [m for c in s["checks"] for m in run_check(c)] if checked else []
    status = ("PASS" if not fails else "FAIL") if checked else "not checked"
    n_ok += status == "PASS"
    results.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": C(*s["at"])}, "properties": {
        "n": s["n"], "title": s["title"], "status": status, "failures": "; ".join(fails),
        "label": f'{s["n"]}. {s["title"]}' + (f" — {status}" if checked else ""),
        "input": s["input"], "result": s["result"]}})
    print(f"  {s['n']:2}. {s['title']:<40} {status}" + (f"   {'; '.join(fails)}" if fails else ""))
json.dump({"type": "FeatureCollection", "features": results},
          open(os.path.join(a.run_dir, "demo_results.geojson"), "w"), indent=1)
if checked:
    print(f"  {n_ok}/{len(scene.SCENARIOS)} scenarios behave as described")
else:
    print(f"  scenario checks describe default_1m; {a.profile} is drawn, not checked")
sys.exit(0 if not checked or n_ok == len(scene.SCENARIOS) else 1)
