"""
Write the demo's raw input files from demo/scene.py:

    python3 demo/build_demo.py

demo/data/ds1_<kind>.geojson (City GIS) and ds2_<kind>.geojson (Field survey),
kind = nodes, edges, zones, points, lines, polygons: the OSW 0.3 files a TDEI
upload holds, ready to load (run_demo.sh does that) or to open in QGIS. Also
demo/data/scenarios.geojson: the numbered spots, with their explanations.

Then checks them as harness/validate_dataset.py does: numeric unique ids, every
edge end exactly at its _u_id / _v_id node, coordinates at 7 dp, no zero-length
segment. Exit code 0 when valid.
"""
import json, os, sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import scene
from scene import C

OUT = os.path.join(HERE, "data")
SCHEMA = "https://sidewalks.washington.edu/opensidewalks/0.3/schema.json"
FIRST_ID = {"ds1": {"nodes": 3000001, "edges": 3500001, "points": 3800001, "lines": 3850001, "polygons": 3900001},
            "ds2": {"nodes": 4000001, "edges": 4500001, "points": 4800001, "lines": 4850001, "polygons": 4900001}}

def feature(geom_type, coords, props):
    return {"type": "Feature", "geometry": {"type": geom_type, "coordinates": coords}, "properties": props}

def write(name, features):
    """One feature per line: small diffs, easy to read and to copy a single feature out."""
    with open(os.path.join(OUT, name), "w") as fh:
        fh.write('{"type": "FeatureCollection", "$schema": "%s", "features": [' % SCHEMA)
        fh.write(",".join("\n" + json.dumps(f, ensure_ascii=False) for f in features))
        fh.write("\n]}\n")

def build(ds):
    ids = dict(FIRST_ID[ds])
    def next_id(kind):
        ids[kind] += 1
        return str(ids[kind] - 1)
    nodes = {}                                       # (x, y) -> node feature, in order of first use
    def node(p):
        if p not in nodes:
            nodes[p] = feature("Point", C(*p), {"_id": next_id("nodes"), **scene.NODE_TAGS[ds].get(p, {})})
        return nodes[p]["properties"]["_id"]
    edges = []
    for name, tags, pts in scene.EDGES[ds]:
        u, v = node(pts[0]), node(pts[-1])
        edges.append(feature("LineString", [C(*p) for p in pts],
                             {"_id": next_id("edges"), "_u_id": u, "_v_id": v, "name": name, **tags}))
    for p in scene.NODE_TAGS[ds]:
        assert p in nodes, f"{ds}: tagged node {p} is not an edge end"
    points = [feature("Point", C(*p), {"_id": next_id("points"), **tags}) for tags, p in scene.POINTS[ds]]
    lines = [feature("LineString", [C(*p) for p in pts], {"_id": next_id("lines"), **tags})
             for tags, pts in scene.LINES[ds]]
    polys = [feature("Polygon", [[C(*p) for p in ring]], {"_id": next_id("polygons"), **tags})
             for tags, ring in scene.POLYGONS[ds]]
    return {"nodes": list(nodes.values()), "edges": edges, "zones": [], "points": points,
            "lines": lines, "polygons": polys}

def validate(ds, f):
    bad = []
    for k, feats in f.items():
        dup = [i for i, c in Counter(x["properties"]["_id"] for x in feats).items() if c > 1]
        if dup: bad.append(f"{k}: duplicate ids {dup}")
        if any(not str(x["properties"]["_id"]).isdigit() for x in feats): bad.append(f"{k}: non-numeric _id")
    at = {n["properties"]["_id"]: n["geometry"]["coordinates"] for n in f["nodes"]}
    for e in f["edges"]:
        c, p = e["geometry"]["coordinates"], e["properties"]
        if at.get(p["_u_id"]) != c[0] or at.get(p["_v_id"]) != c[-1]:
            bad.append(f"edge {p['_id']}: an end is not exactly at its node")
        if any(a == b for a, b in zip(c, c[1:])):
            bad.append(f"edge {p['_id']}: zero-length segment")
    for k, feats in f.items():
        for x in feats:
            g = x["geometry"]
            pts = [g["coordinates"]] if g["type"] == "Point" else g["coordinates"] if g["type"] == "LineString" \
                else [q for r in g["coordinates"] for q in r]
            if any(len(repr(v).split(".")[-1]) > 7 for q in pts for v in q):
                bad.append(f"{k} {x['properties']['_id']}: more than 7 dp")
    print(f"  {ds} ({scene.DATASETS[ds]}): " + ", ".join(f"{len(v)} {k}" for k, v in f.items())
          + ("  — valid" if not bad else ""))
    for b in bad: print("    FAIL", b)
    return not bad

os.makedirs(OUT, exist_ok=True)
ok = True
for ds in ("ds1", "ds2"):
    f = build(ds)
    for kind, feats in f.items():
        write(f"{ds}_{kind}.geojson", feats)
    ok &= validate(ds, f)
write("scenarios.geojson", [feature("Point", C(*s["at"]), {
    "n": s["n"], "title": s["title"], "label": f'{s["n"]}. {s["title"]}', "input": s["input"],
    "result": s["result"]}) for s in scene.SCENARIOS])
print(f"wrote {OUT}/ds1_*.geojson, ds2_*.geojson, scenarios.geojson ({len(scene.SCENARIOS)} scenarios)")
sys.exit(0 if ok else 1)
