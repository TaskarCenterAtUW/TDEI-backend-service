"""Shared helpers: load the test inputs, compute case areas, write GeoJSON."""
import json, os
from cases import CASES, C, M

HERE = os.path.dirname(os.path.abspath(__file__))     # <repo>/harness
ROOT = os.path.dirname(HERE)                          # <repo>
DATA = os.path.join(ROOT, "data")                     # ds1_*/ds2_*.geojson test inputs
LAYERS = os.path.join(DATA, "expected")               # reference layers built from cases.py
IMAGES = os.path.join(ROOT, "docs", "images")         # reference case sheet

def load(path):
    return json.load(open(path))["features"]

def inputs(data_dir=DATA):
    return {ds: {kind: load(os.path.join(data_dir, f"{ds}_{kind}.geojson"))
                 for kind in ("nodes", "edges")} for ds in ("ds1", "ds2")}

def case_edges(inp, cid):
    """Input edges of a case, as (dataset, feature)."""
    out = []
    for ds in ("ds1", "ds2"):
        for f in inp[ds]["edges"]:
            if f["properties"].get("name", "").startswith(cid + ":"):
                out.append((ds, f))
    return out

def case_area(inp, cid, margin=4.0):
    """Rectangle (lon/lat ring) around a case's input edges, with a margin in metres."""
    pts = [M(*c) for _, f in case_edges(inp, cid) for c in f["geometry"]["coordinates"]]
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    x0, x1, y0, y1 = min(xs) - margin, max(xs) + margin, min(ys) - margin, max(ys) + margin
    return [list(C(x0, y0)), list(C(x1, y0)), list(C(x1, y1)), list(C(x0, y1)), list(C(x0, y0))]

def inside(ring, lonlat):
    (x0, y0), (x1, y1) = ring[0], ring[2]
    return min(x0, x1) <= lonlat[0] <= max(x0, x1) and min(y0, y1) <= lonlat[1] <= max(y0, y1)

def write(path, features):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    json.dump({"type": "FeatureCollection", "features": features}, open(path, "w"), indent=1)
