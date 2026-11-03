"""Shared helpers: load the test inputs and outputs, compute case areas, write GeoJSON."""
import json, os
from cases import CASES, C, M

HERE = os.path.dirname(os.path.abspath(__file__))     # <harness>/harness
ROOT = os.path.dirname(HERE)                          # <harness>
DATA = os.path.join(ROOT, "data")                     # ds1_*/ds2_*.geojson test inputs
LAYERS = os.path.join(DATA, "expected")               # reference layers built from cases.py
IMAGES = os.path.join(ROOT, "docs", "images")         # reference case sheets

KINDS = ("nodes", "edges", "zones", "points", "lines", "polygons")
# union output file names, as written by sql/export_union.psql
OUT_FILE = {"nodes": "osw_nodes.geojson", "edges": "osw_edges.geojson", "zones": "osw_zones.geojson",
            "points": "osw_points.geojson", "lines": "osw_lines.geojson", "polygons": "osw_polygons.geojson"}

def load(path):
    if not os.path.exists(path):
        return []
    return json.load(open(path))["features"]

def inputs(data_dir=DATA):
    return {ds: {kind: load(os.path.join(data_dir, f"{ds}_{kind}.geojson")) for kind in KINDS}
            for ds in ("ds1", "ds2")}

def outputs(out_dir):
    return {kind: load(os.path.join(out_dir, OUT_FILE[kind])) for kind in KINDS}

def case_of(props):
    """The case id a feature belongs to: edges by name prefix, other features by ext:case."""
    if props.get("ext:case"):
        return props["ext:case"]
    name = props.get("name", "")
    return name.split(":", 1)[0] if ":" in name else None

def case_edges(inp, cid):
    """Input edges of a case, as (dataset, feature)."""
    return [(ds, f) for ds in ("ds1", "ds2") for f in inp[ds]["edges"]
            if f["properties"].get("name", "").startswith(cid + ":")]

def case_features(inp, cid):
    """Every input feature of a case (edges, zones, points, lines, polygons), as (dataset, kind, feature)."""
    out = []
    for ds in ("ds1", "ds2"):
        for kind in ("edges", "zones", "points", "lines", "polygons"):
            for f in inp[ds][kind]:
                if case_of(f["properties"]) == cid:
                    out.append((ds, kind, f))
    return out

def _coords(geom):
    t, c = geom["type"], geom["coordinates"]
    if t == "Point": return [c]
    if t == "LineString": return c
    if t == "Polygon": return [p for ring in c for p in ring]
    return []

def case_area(inp, cid, margin=4.0):
    """Rectangle (lon/lat ring) around a case's input features, with a margin in metres."""
    pts = [M(*c) for _, _, f in case_features(inp, cid) for c in _coords(f["geometry"])]
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    x0, x1, y0, y1 = min(xs) - margin, max(xs) + margin, min(ys) - margin, max(ys) + margin
    return [list(C(x0, y0)), list(C(x1, y0)), list(C(x1, y1)), list(C(x0, y1)), list(C(x0, y0))]

def inside(ring, lonlat):
    (x0, y0), (x1, y1) = ring[0], ring[2]
    return min(x0, x1) <= lonlat[0] <= max(x0, x1) and min(y0, y1) <= lonlat[1] <= max(y0, y1)

def feature_point(f):
    """A representative coordinate of a feature (its first vertex)."""
    return tuple(_coords(f["geometry"])[0])

def is_connector(props):
    """Connector edges are the only edges a union creates; they carry ext:union_rule."""
    return "ext:union_rule" in props

def write(path, features):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    json.dump({"type": "FeatureCollection", "features": features}, open(path, "w"), indent=1)
