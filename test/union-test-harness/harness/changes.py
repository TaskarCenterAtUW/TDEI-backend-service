"""
What the union changed: compares the input datasets with a union output and
writes three QGIS layers into the run folder, plus a short summary.

    python3 harness/changes.py <union_output_dir> --proximity 3     (check_cases.py runs this for you)

Matching is by GEOMETRY, never by id: the union's output ids are its own
(database row ids, ixn-... nodes), so input and output ids do not correspond.

changes_input_edges.geojson   every DS1/DS2 input edge with its fate
    kept      output has the identical line
    split     the line survives as 2+ pieces lying exactly on it
    modified  it survives as one line, but reshaped (endpoints snapped, trimmed)
    removed   nothing in the output comes from it (dropped as a duplicate)
changes_output_edges.geojson  every output edge with where it came from
    unchanged         identical to an input edge
    split piece       a piece of an input edge (lies exactly on it)
    modified          an input edge reshaped (endpoint(s) snapped)
    new               matches no input edge
changes_nodes.geojson         node-level events
    merged     output node that absorbed a DS2 node (has ext:union_confidence)
    minted     new intersection node (id ixn-...)
    gone       input DS2 node not in the output at its own position — snapped onto
               a DS1 node, or dropped with a removed duplicate (nearest_m says how far
               the nearest output node is)
"""
import argparse, json, math, os
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(os.path.dirname(HERE), "data")   # test inputs
MLON = 111111 * math.cos(math.radians(44.63)); MLAT = 111111.0
SAME = 0.001      # metres: "identical" coordinate (float formatting noise only)
ON = 0.01         # metres: a vertex lies ON an input line


def load(p):
    return json.load(open(p))["features"] if os.path.exists(p) else []

def xy(c):                     # lon/lat -> local metres (small area, fine for a test grid)
    return (c[0] * MLON, c[1] * MLAT)

def seg_dist(p, a, b):
    (px, py), (ax, ay), (bx, by) = p, a, b
    dx, dy = bx - ax, by - ay
    L = dx * dx + dy * dy
    t = 0 if L == 0 else max(0, min(1, ((px - ax) * dx + (py - ay) * dy) / L))
    return math.hypot(px - ax - t * dx, py - ay - t * dy)

def dist_to_line(p, line):
    return min(seg_dist(p, line[i], line[i + 1]) for i in range(len(line) - 1))

def length(line):
    return sum(math.hypot(line[i + 1][0] - line[i][0], line[i + 1][1] - line[i][1]) for i in range(len(line) - 1))

def same_line(a, b):
    if len(a) != len(b): return False
    fwd = all(math.hypot(p[0] - q[0], p[1] - q[1]) <= SAME for p, q in zip(a, b))
    return fwd or all(math.hypot(p[0] - q[0], p[1] - q[1]) <= SAME for p, q in zip(a, reversed(b)))


def run(outdir, proximity, data_dir=DATA, quiet=False):
    inputs = []
    for ds in ("DS1", "DS2"):                                  # DS1 first: it wins ties
        for f in load(os.path.join(data_dir, f"{ds.lower()}_edges.geojson")):
            inputs.append({"ds": ds, "f": f, "line": [xy(c) for c in f["geometry"]["coordinates"]],
                           "name": f["properties"].get("name", ""), "pieces": [], "exact": False})
    outputs = [{"f": f, "line": [xy(c) for c in f["geometry"]["coordinates"]],
                "name": f["properties"].get("name", ""), "src": None, "kind": "new"}
               for f in load(os.path.join(outdir, "osw_edges.geojson"))]

    # 1. identical lines (an input may be claimed once; DS1 before DS2).
    #    Same-name matches first: a kept DS2 duplicate snapped onto its DS1 twin is
    #    geometrically identical to it, and without this the result would depend on
    #    the order the union happens to emit the two output edges.
    for same_name in (True, False):
        for o in outputs:
            if o["src"]: continue
            for i in inputs:
                if same_name and i["name"] != o["name"]: continue
                if not i["exact"] and not i["pieces"] and same_line(o["line"], i["line"]):
                    i["exact"] = True; i["pieces"].append(o); o["src"], o["kind"] = i, "unchanged"; break
    # 2. pieces lying exactly on an input line
    for o in outputs:
        if o["src"]: continue
        for i in inputs:
            if i["exact"] or i["name"] != o["name"]: continue
            if all(dist_to_line(p, i["line"]) <= ON for p in o["line"]):
                i["pieces"].append(o); o["src"], o["kind"] = i, "split piece"; break
    # 3. reshaped: same name, every vertex within proximity (endpoints snapped)
    tol = proximity + ON
    for o in outputs:
        if o["src"]: continue
        best = None
        for i in inputs:
            if i["exact"] or i["name"] != o["name"]: continue
            d = max(dist_to_line(p, i["line"]) for p in o["line"])
            if d <= tol and (best is None or d < best[0]): best = (d, i)
        if best:
            best[1]["pieces"].append(o); o["src"], o["kind"] = best[1], "modified"

    in_feats, fate_count = [], defaultdict(lambda: defaultdict(int))
    for i in inputs:
        n = len(i["pieces"])
        if i["exact"]: fate = "kept"
        elif n == 0: fate = "removed"
        elif n >= 2: fate = "split"
        elif all(o["kind"] == "split piece" for o in i["pieces"]):
            got = sum(length(o["line"]) for o in i["pieces"]) / max(length(i["line"]), 1e-9)
            fate = "kept" if got > 0.999 else "modified"          # one piece covering it all = kept
        else: fate = "modified"
        fate_count[i["ds"]][fate] += 1
        p = i["f"]["properties"]
        in_feats.append({"type": "Feature", "geometry": i["f"]["geometry"], "properties": {
            "dataset": i["ds"], "fate": fate, "pieces": n, "name": i["name"],
            "case": i["name"].split(":", 1)[0], "input_id": p.get("_id"),
            "label": f'{i["ds"]} {fate}' + (f" ×{n}" if fate == "split" else "")}})

    out_feats, kind_count = [], defaultdict(int)
    for o in outputs:
        kind_count[o["kind"]] += 1
        s = o["src"]
        out_feats.append({"type": "Feature", "geometry": o["f"]["geometry"], "properties": {
            "change": o["kind"], "from": s["ds"] if s else "", "from_input_id": s["f"]["properties"].get("_id") if s else "",
            "name": o["name"], "case": o["name"].split(":", 1)[0], "output_id": o["f"]["properties"].get("_id"),
            "label": o["kind"] + (f' ({s["ds"]})' if s else "")}})

    # nodes
    onodes = load(os.path.join(outdir, "osw_nodes.geojson"))
    ocoords = [xy(n["geometry"]["coordinates"]) for n in onodes]
    node_feats, node_count = [], defaultdict(int)
    for n, c in zip(onodes, ocoords):
        p = n["properties"]; oid = str(p.get("_id", ""))
        kind = "minted" if oid.startswith("ixn-") else ("merged" if "ext:union_confidence" in p and
                                                          p.get("ext:union_confidence_status") != "carried" else None)
        if kind:
            node_count[kind] += 1
            audit = {k[len("ext:union_audit_"):]: v for k, v in p.items() if k.startswith("ext:union_audit_")}
            node_feats.append({"type": "Feature", "geometry": n["geometry"], "properties": {
                "change": kind, "output_id": oid, "confidence": p.get("ext:union_confidence"),
                "audit": "; ".join(f"{k}={v}" for k, v in audit.items()), "label": kind}})
    for f in load(os.path.join(data_dir, "ds2_nodes.geojson")):
        c = xy(f["geometry"]["coordinates"])
        near = min((math.hypot(c[0] - o[0], c[1] - o[1]) for o in ocoords), default=1e9)
        if near > SAME:
            node_count["gone"] += 1
            node_feats.append({"type": "Feature", "geometry": f["geometry"], "properties": {
                "change": "gone", "input_id": f["properties"].get("_id"),
                "nearest_m": round(near, 3), "label": f"gone ({near:.2f} m)"}})

    for name, feats in (("changes_input_edges", in_feats), ("changes_output_edges", out_feats),
                        ("changes_nodes", node_feats)):
        json.dump({"type": "FeatureCollection", "features": feats},
                  open(os.path.join(outdir, name + ".geojson"), "w"), indent=1)

    if not quiet:
        print("\nWhat the union changed:")
        for ds in ("DS1", "DS2"):
            c = fate_count[ds]
            print(f"  {ds} edges   kept {c['kept']:3}   split {c['split']:3}   modified {c['modified']:3}   removed {c['removed']:3}")
        print(f"  output edges  unchanged {kind_count['unchanged']}, split pieces {kind_count['split piece']}, "
              f"modified {kind_count['modified']}, new {kind_count['new']}")
        print(f"  nodes         merged {node_count['merged']}, minted {node_count['minted']}, "
              f"DS2 nodes gone {node_count['gone']}")
    return fate_count, kind_count, node_count


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("output_dir")
    ap.add_argument("--proximity", type=float, required=True)
    a = ap.parse_args()
    run(a.output_dir, a.proximity)
