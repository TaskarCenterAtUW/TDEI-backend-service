"""
Check a union output against every case in the catalogue.

    python3 harness/check_cases.py <union_output_dir> --proximity 3

<union_output_dir> holds osw_nodes.geojson and osw_edges.geojson from the union.
Prints a per-case PASS/FAIL table, runs the network-wide routability checks, and
writes two layers for QGIS INTO <union_output_dir>, so every run keeps its own results:
  case_results.geojson      case rectangles with status + what failed
  junction_results.geojson  expected junctions with status + actual degree
Also checks output integrity (endpoint/node mismatch, dangling refs, unmerged
coincident nodes, duplicate ids, orphans).
Exit code 0 when every case, integrity and routability pass, 1 otherwise.

Filter scenarios (scenarios.py):
    python3 harness/check_cases.py runs/f_sidewalk --proximity 3 --scenario f_sidewalk --baseline runs/prox_3
checks the scenario's rules instead of the default-run counts: connectivity
assertions must all still hold, duplicates are removed only where the filter
allows (compared with the baseline), and attribute merges follow the filter.
Also writes vs_baseline_edges.geojson / vs_baseline_nodes.geojson for QGIS.
"""
import argparse, json, math, os, sys
from collections import defaultdict
from cases import CASES, C, MLON, MLAT, value_for
from common import inputs, case_area, inside, load, write, LAYERS, DATA
import routability

ap = argparse.ArgumentParser()
ap.add_argument("output_dir")
ap.add_argument("--proximity", type=float, required=True)
ap.add_argument("--scenario", help="name of a filter scenario in scenarios.py")
ap.add_argument("--baseline", help="baseline run folder (same proximity, no filters)")
args = ap.parse_args()
scenario = None
if args.scenario:
    from scenarios import get as get_scenario
    scenario = get_scenario(args.scenario)
    if not args.baseline or not os.path.exists(os.path.join(args.baseline, "changes_input_edges.geojson")):
        sys.exit(f"--scenario needs --baseline: a checked run at {args.proximity:g} m without filters")
# In a filter scenario only structural assertions carry over from the default
# run (counts, kerbs and attributes legitimately change and are checked by the
# filter rules instead). They are written for 1 m / 3 m only.
STRUCTURAL = {"junction", "connected", "apart", "ixn", "no_kerb_merge"}

nodes = load(os.path.join(args.output_dir, "osw_nodes.geojson"))
edges = load(os.path.join(args.output_dir, "osw_edges.geojson"))
inp = inputs()
coord = {str(n["properties"]["_id"]): tuple(n["geometry"]["coordinates"]) for n in nodes}
nprops = {str(n["properties"]["_id"]): n["properties"] for n in nodes}

parent = {}
def find(a):
    parent.setdefault(a, a)
    while parent[a] != a:
        parent[a] = parent[parent[a]]; a = parent[a]
    return a
degree = defaultdict(int); touching = defaultdict(set); by_name = defaultdict(list)
for e in edges:
    p = e["properties"]; u, v = str(p["_u_id"]), str(p["_v_id"])
    parent[find(u)] = find(v)
    for k in (u, v):
        degree[k] += 1; touching[k].add(p.get("name", ""))
    by_name[p.get("name", "")].append(u)

def dist_m(a, b):
    return math.hypot((a[0] - b[0]) * MLON, (a[1] - b[1]) * MLAT)
def node_near(xy, tol=0.05):
    target = C(*xy)
    best = min(coord, key=lambda k: dist_m(coord[k], target), default=None)
    return best if best is not None and dist_m(coord[best], target) <= tol else None
short = lambda n: n.split(": ", 1)[-1]

def check(a, area):
    kind = a[0]
    if kind == "at":                       # proximity-specific assertion
        return check(a[2], area) if abs(args.proximity - a[1]) < 1e-6 else (None, "n/a at this proximity")
    if kind == "count":
        want = value_for(a[2], args.proximity); got = len(by_name.get(a[1], []))
        return got == want, f"{short(a[1])}: expected {want}, got {got}"
    if kind == "junction":
        (x, y), mind, names = a[1], a[2], a[3]
        k = node_near((x, y))
        if k is None:
            return False, f"no node at junction ({x:g},{y:g})"
        missing = [short(n) for n in names if n not in touching[k]]
        ok = degree[k] >= mind and not missing
        msg = f"junction ({x:g},{y:g}): {degree[k]} edges (need {mind}+)"
        return ok, msg + (f", not reached by: {', '.join(missing)}" if missing else "")
    if kind == "connected":
        comps = {find(u) for n in a[1] for u in by_name.get(n, [])}
        absent = [short(n) for n in a[1] if not by_name.get(n)]
        ok = len(comps) == 1 and not absent
        return ok, "one connected route" if ok else f"split into {len(comps)} pieces" + (
            f"; missing: {', '.join(absent)}" if absent else "")
    if kind == "apart":
        ca = {find(u) for u in by_name.get(a[1], [])}; cb = {find(u) for u in by_name.get(a[2], [])}
        return not (ca & cb), f"{short(a[1])} and {short(a[2])} " + ("kept apart" if not ca & cb else "WRONGLY connected")
    if kind == "kerbs":
        want = value_for(a[1], args.proximity)
        got = sum(1 for k, c in coord.items() if inside(area, c) and nprops[k].get("barrier") == "kerb")
        return got == want, f"kerb nodes: expected {want}, got {got}"
    if kind == "no_kerb_merge":
        bad = [k for k, c in coord.items() if inside(area, c) and nprops[k].get("barrier") == "kerb"
               and "ext:union_audit_barrier" in nprops[k]]
        return not bad, "no kerb absorbed another" if not bad else f"kerb merged into node {bad[0]}"
    if kind == "ixn":
        got = sum(1 for k, c in coord.items() if inside(area, c) and k.startswith("ixn-"))
        return got == a[1], f"intersection nodes: expected {a[1]}, got {got}"
    if kind == "node_attr":
        k = node_near(a[1])
        if k is None:
            return False, f"no node at ({a[1][0]:g},{a[1][1]:g})"
        def wrong(key, val):          # val None -> the key must merely exist
            return key not in nprops[k] if val is None else nprops[k].get(key) != val
        bad = [f"{key}={nprops[k].get(key)!r}" for key, val in a[2].items() if wrong(key, val)]
        return not bad, "attributes as expected" if not bad else "wrong/missing: " + ", ".join(bad)
    raise ValueError(kind)

def applies(a):
    kind = a[2][0] if a[0] == "at" else a[0]
    if scenario is None:
        return True
    return kind in STRUCTURAL and args.proximity in (1.0, 3.0)

areas = {c["id"]: case_area(inp, c["id"]) for c in CASES}
case_fail = {c["id"]: [] for c in CASES}
junction_feats = []
for c in CASES:
    area = areas[c["id"]]
    results = [(a[2] if a[0] == "at" else a, *check(a, area)) for a in c["asserts"] if applies(a)]
    results = [r for r in results if r[1] is not None]        # drop n/a proximity-specific ones
    case_fail[c["id"]] += [m for _, ok, m in results if not ok]
    for a, ok, m in results:
        if a[0] == "junction":
            k = node_near(a[1])
            junction_feats.append({"type": "Feature",
                                   "geometry": {"type": "Point", "coordinates": list(C(*a[1]))},
                                   "properties": {"case": c["id"], "status": "PASS" if ok else "FAIL",
                                                  "degree": degree[k] if k else 0, "min_degree": a[2],
                                                  "label": f'{c["id"]} {"PASS" if ok else "FAIL"} ({degree[k] if k else 0})',
                                                  "detail": m}})

# ---- output integrity: the structural faults seen during development -------
from collections import Counter
print("\nOutput integrity (each should be 0):")
ids = [str(n["properties"]["_id"]) for n in nodes]
used = set()
mismatch = dangling = 0
for e in edges:
    p = e["properties"]; cs = e["geometry"]["coordinates"]
    for key, end in (("_u_id", cs[0]), ("_v_id", cs[-1])):
        k = str(p[key]); used.add(k)
        if k not in coord: dangling += 1
        elif tuple(coord[k]) != tuple(end): mismatch += 1
integrity = [
    ("edge endpoint differs from its _u_id/_v_id node", mismatch),
    ("edge references a node missing from the output", dangling),
    ("duplicate node ids", sum(v - 1 for v in Counter(ids).values() if v > 1)),
    ("two nodes at one coordinate (failed to merge)", sum(v - 1 for v in Counter(coord.values()).values() if v > 1)),
    ("orphan nodes (no edge uses them)", sum(1 for k in coord if k not in used)),
]
bad_integrity = False
for label, n in integrity:
    print(f"  {'ok  ' if n == 0 else 'FAIL'}  {n:4}  {label}")
    bad_integrity |= n > 0

import changes
changes.run(args.output_dir, args.proximity, data_dir=DATA)

# ---- filter scenario rules (R2 duplicates, R3 attributes) --------------------
def case_of_name(name):
    return name.split(":", 1)[0] if ":" in name else None
def case_of_coord(lonlat):
    for cid, area in areas.items():
        if inside(area, lonlat): return cid
    return None

bad_filter = False
if scenario:
    from filter_oracle import Oracle
    orc = Oracle(DATA, scenario)
    fates = lambda d: {(f["properties"]["dataset"], str(f["properties"]["input_id"])): f["properties"]
                       for f in load(os.path.join(d, "changes_input_edges.geojson"))}
    now, base = fates(args.output_dir), fates(args.baseline)
    expected = orc.expected_fates({k: v["fate"] for k, v in base.items()})
    edge_feats = []
    n_dup = {"removed": 0, "kept by filter": 0}
    for (ds, iid), p in now.items():
        b = base.get((ds, iid), {}).get("fate")
        exp = expected.get(iid) if ds == "DS2" else None
        status = "PASS"
        if exp:
            want, why = exp
            ok = (p["fate"] == "removed") if want == "removed" else \
                 (p["fate"] == "kept") if want == "kept" else (p["fate"] != "removed")
            if not ok:
                status = "FAIL"
                case_fail.setdefault(case_of_name(p["name"]), []).append(
                    f'{p["name"].split(": ",1)[-1]}: expected {want}, got {p["fate"]} ({why})')
            if b == "removed" and want == "not removed": n_dup["kept by filter"] += 1
            if want == "removed": n_dup["removed"] += 1
        if p["fate"] != b or status == "FAIL":
            edge_feats.append({"type": "Feature", "geometry": next(
                f["geometry"] for f in load(os.path.join(args.output_dir, "changes_input_edges.geojson"))
                if f["properties"]["dataset"] == ds and str(f["properties"]["input_id"]) == iid),
                "properties": {"dataset": ds, "name": p["name"], "case": p["case"], "baseline_fate": b,
                               "fate": p["fate"], "expected": exp[0] if exp else "", "reason": exp[1] if exp else "",
                               "status": status, "label": f'{b} → {p["fate"]}'}})
    write(os.path.join(args.output_dir, "vs_baseline_edges.geojson"), edge_feats)

    node_feats = []
    # R1 for nodes: every DS2 node that merged in the baseline still merges (snapping is connectivity)
    merged_in = lambda evs: {tuple(e["tags"].items()) for e in evs if e["target"] is not None}
    base_merged = merged_in(orc.attribute_expectations(load(os.path.join(args.baseline, "osw_nodes.geojson")), args.proximity))
    events = orc.attribute_expectations(nodes, args.proximity)
    for ev in events:
        if ev["target"] is None and tuple(ev["tags"].items()) in base_merged:
            case_fail.setdefault(case_of_coord(ev["coord"]), []).append(
                "DS2 node " + ", ".join(f"{k}={v}" for k, v in ev["tags"].items()) +
                " merged in the baseline but not under the filter — a filter must never stop a snap")
    for ev in events:
        if ev["target"] is None or not ev["rows"] and ev.get("audited", True):
            continue
        cid = case_of_coord(ev["coord"])
        bad = [f"{k}: expected {e}, got {g}" for k, v, e, g in ev["rows"] if e != g]
        if not ev["audited"]:
            bad.append("DS2 values not audited (ext:union_audit_* missing)")
        for m in bad:
            case_fail.setdefault(cid, []).append(f"node attribute {m}")
        merged = [f"{k}={v}" for k, v, e, g in ev["rows"] if g == "merged"]
        withheld = [f"{k}={v}" for k, v, e, g in ev["rows"] if g == "withheld"]
        node_feats.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": ev["coord"]},
                           "properties": {"case": cid, "status": "FAIL" if bad else "PASS",
                                          "eligible": ev["eligible"], "merged": ", ".join(merged),
                                          "withheld": ", ".join(withheld), "detail": "; ".join(bad),
                                          "label": ("merged " + ", ".join(merged)) if merged else
                                                   ("withheld " + ", ".join(withheld))}})
    write(os.path.join(args.output_dir, "vs_baseline_nodes.geojson"), node_feats)

    print(f"\nFilter scenario {scenario['name']}: {scenario['title']}")
    print(f"  filters: {json.dumps(scenario['filters'])}")
    print(f"  vs baseline {args.baseline}: {len(edge_feats)} input edges changed fate; "
          f"duplicates removed {n_dup['removed']}, kept by the filter {n_dup['kept by filter']}; "
          f"attribute merges: {sum(1 for f in node_feats if f['properties']['merged'])} merged, "
          f"{sum(1 for f in node_feats if not f['properties']['merged'])} withheld")
    for f in edge_feats:
        q = f["properties"]
        print(f"    {q['status']:4}  {q['name']:38} {q['baseline_fate']} → {q['fate']}"
              + (f"   ({q['reason']})" if q["reason"] else ""))
    for f in node_feats:
        q = f["properties"]
        print(f"    {q['status']:4}  node {q['case'] or '?'}: {q['label']}")

# ---- case boxes (after every rule has had its say) ---------------------------
case_feats, n_fail = [], 0
print(f"\nCase results — {args.output_dir} (proximity {args.proximity:g} m"
      + (f", scenario {scenario['name']}" if scenario else "") + ")\n")
for c in CASES:
    failed = case_fail.get(c["id"], [])
    status = "PASS" if not failed else "FAIL"
    n_fail += bool(failed)
    print(f"  {status}  {c['id']}  {c['title']}")
    for m in failed:
        print(f"          x {m}")
    case_feats.append({"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [areas[c["id"]]]},
                       "properties": {"case": c["id"], "title": c["title"], "status": status,
                                      "label": f'{c["id"]} {status}',
                                      "expected": scenario["description"] if scenario else c["expected"],
                                      "failures": "; ".join(failed)}})
for cid, failed in case_fail.items():                       # failures outside any case area
    if cid is None and failed:
        n_fail += 1; print("  FAIL  (outside cases)"); [print(f"          x {m}") for m in failed]
write(os.path.join(args.output_dir, "case_results.geojson"), case_feats)
write(os.path.join(args.output_dir, "junction_results.geojson"), junction_feats)

print("\nNetwork-wide routability:")
ins = [("DS1", os.path.join(DATA, "ds1_nodes.geojson"), os.path.join(DATA, "ds1_edges.geojson")),
       ("DS2", os.path.join(DATA, "ds2_nodes.geojson"), os.path.join(DATA, "ds2_edges.geojson"))]
bad_route = routability.run(args.output_dir, args.proximity, "  routability", ins)

print(f"\n{len(CASES) - n_fail}/{len(CASES)} cases passed; integrity {'PASS' if not bad_integrity else 'FAIL'}; "
      f"routability {'PASS' if not bad_route else 'FAIL'}")
print(f"QGIS result layers written to {args.output_dir}/")
sys.exit(1 if (n_fail or bad_route or bad_integrity) else 0)
