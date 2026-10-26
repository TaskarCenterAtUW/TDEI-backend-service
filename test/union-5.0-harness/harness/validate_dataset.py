"""
Validate the generated test inputs (data/ds1_*.geojson, data/ds2_*.geojson).

    python3 harness/validate_dataset.py

Checks, per dataset: unique ids per file and numeric ids; every edge end
EXACTLY equals its _u_id/_v_id node; every zone's _w_id lists existing nodes,
in ring order, at exactly the ring's vertices; coordinates at 7 dp at most;
edges have 2+ points and no zero-length segment; every edge vertex is a
registered node; rings are closed. Exit code 0 when everything is valid.
"""
import json, os, sys
from collections import Counter

DATA = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
KINDS = ("nodes", "edges", "zones", "points", "lines", "polygons")

def load(ds, kind):
    p = os.path.join(DATA, f"{ds}_{kind}.geojson")
    return json.load(open(p))["features"] if os.path.exists(p) else []

def dp(x):
    s = repr(float(x))
    return len(s.split(".")[1]) if "." in s and "e" not in s else 0

def coords(g):
    t, c = g["type"], g["coordinates"]
    return [c] if t == "Point" else c if t == "LineString" else [p for r in c for p in r]

ok_all = True
def report(label, n):
    global ok_all
    print(f"  {'ok  ' if n == 0 else 'FAIL'}  {n:4}  {label}")
    ok_all &= n == 0

for ds in ("ds1", "ds2"):
    f = {k: load(ds, k) for k in KINDS}
    print(f"\n=== {ds.upper()} : " + ", ".join(f"{len(v)} {k}" for k, v in f.items()) + " ===")
    nid = {str(n["properties"]["_id"]): tuple(n["geometry"]["coordinates"]) for n in f["nodes"]}

    report("duplicate ids (any file)", sum(sum(v - 1 for v in Counter(str(x["properties"]["_id"]) for x in f[k]).values() if v > 1) for k in KINDS))
    report("non-numeric _id", sum(1 for k in KINDS for x in f[k] if not str(x["properties"]["_id"]).isdigit()))
    mism = 0
    for e in f["edges"]:
        p, c = e["properties"], e["geometry"]["coordinates"]
        for coord, key in ((tuple(c[0]), "_u_id"), (tuple(c[-1]), "_v_id")):
            if nid.get(str(p[key])) != coord: mism += 1
    report("edge ends not exactly at their _u_id/_v_id node", mism)
    zbad = 0
    for z in f["zones"]:
        ring_pts = [tuple(c) for r in z["geometry"]["coordinates"] for c in r]
        w = [str(k) for k in z["properties"].get("_w_id", [])]
        if [nid.get(k) for k in w] != ring_pts: zbad += 1
    report("zones whose _w_id does not list the ring's nodes in order", zbad)
    report("coordinates beyond 7 dp", sum(1 for k in KINDS for x in f[k] for lo, la in coords(x["geometry"]) if dp(lo) > 7 or dp(la) > 7))
    short = sum(1 for e in f["edges"] if len(e["geometry"]["coordinates"]) < 2)
    zl = sum(1 for e in f["edges"] for a, b in zip(e["geometry"]["coordinates"], e["geometry"]["coordinates"][1:]) if a == b)
    report("edges with < 2 points or a zero-length segment", short + zl)
    reg = set(nid.values())
    report("edge vertices not registered as nodes", sum(1 for e in f["edges"] for c in e["geometry"]["coordinates"] if tuple(c) not in reg))
    report("polygon / zone rings not closed", sum(1 for k in ("zones", "polygons") for x in f[k] for r in x["geometry"]["coordinates"] if r[0] != r[-1]))

print(f"\n{'ALL VALID' if ok_all else 'VALIDATION FAILED'}")
sys.exit(0 if ok_all else 1)
