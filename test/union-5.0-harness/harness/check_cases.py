"""
Check one union 5.0 run against the case catalogue for its profile.

    python3 harness/check_cases.py runs/default_3m --profile default_3m

<run_dir> holds the union's six osw_*.geojson files and report.json. Prints a
per-case PASS/FAIL table, the output-integrity and report checks, and the
network-wide routability checks; writes case_results.geojson and
junction_results.geojson into <run_dir> for QGIS, plus the changes_*.geojson
layers (harness/changes.py: what the union changed, by geometry). Exit code 0 only when
everything passes.

Connector edges (the edges a union creates; they carry ext:union_rule) are
treated as part of the junction they make: the nodes a connector joins count as
one junction, and only named edges count towards its degree.
"""
import argparse, json, math, os, sys
from collections import Counter, defaultdict
from cases import CASES, C, MLON, MLAT, ABSENT, value_for, asserts_for
from common import (inputs, outputs, case_area, inside, write, feature_point, is_connector, DATA)
import profiles, routability, changes

ap = argparse.ArgumentParser()
ap.add_argument("run_dir")
ap.add_argument("--profile", required=True)
args = ap.parse_args()
prof = profiles.get(args.profile)
PROX = prof["proximity"]

inp, out = inputs(), outputs(args.run_dir)
nodes, edges = out["nodes"], out["edges"]
coord = {str(n["properties"]["_id"]): tuple(n["geometry"]["coordinates"]) for n in nodes}
nprops = {str(n["properties"]["_id"]): n["properties"] for n in nodes}
input_coords = {tuple(n["geometry"]["coordinates"]) for ds in ("ds1", "ds2") for n in inp[ds]["nodes"]}

def uf():
    parent = {}
    def find(a):
        parent.setdefault(a, a)
        while parent[a] != a:
            parent[a] = parent[parent[a]]; a = parent[a]
        return a
    return find, parent
find, parent = uf()                 # connectivity: every edge, connectors included
jfind, jparent = uf()               # junctions: nodes joined by a connector count as one
degree = defaultdict(int); touching = defaultdict(set); by_name = defaultdict(list)
for e in edges:
    p = e["properties"]; u, v = str(p["_u_id"]), str(p["_v_id"])
    parent[find(u)] = find(v)
    if is_connector(p):
        jparent[jfind(u)] = jfind(v)
for e in edges:
    p = e["properties"]
    if is_connector(p):
        continue
    u, v = str(p["_u_id"]), str(p["_v_id"])
    for k in (u, v):
        degree[jfind(k)] += 1; touching[jfind(k)].add(p.get("name", ""))
    by_name[p.get("name", "")].append(u)

def dist_m(a, b):
    return math.hypot((a[0] - b[0]) * MLON, (a[1] - b[1]) * MLAT)
def node_near(xy, tol=0.05):
    target = C(*xy)
    best = min(coord, key=lambda k: dist_m(coord[k], target), default=None)
    return best if best is not None and dist_m(coord[best], target) <= tol else None
short = lambda n: n.split(": ", 1)[-1]
def attrs_ok(props, want):
    bad = []
    for key, val in want.items():
        if val is ABSENT:
            if key in props: bad.append(f"{key}={props[key]!r} (should be absent)")
        elif val is None:
            if key not in props: bad.append(f"{key} missing")
        elif str(props.get(key)) != str(val):
            bad.append(f"{key}={props.get(key)!r} (want {val!r})")
    return bad
KIND_OUT = {"point": "points", "line": "lines", "polygon": "polygons", "zone": "zones"}

def check(a, area):
    kind = a[0]
    if kind == "at":
        return check(a[2], area) if abs(PROX - a[1]) < 1e-6 else (None, "n/a at this proximity")
    if kind == "count":
        want = value_for(a[2], PROX); got = len(by_name.get(a[1], []))
        return got == want, f"{short(a[1])}: expected {want}, got {got}"
    if kind == "junction":
        (x, y), mind, names = a[1], a[2], a[3]
        k = node_near((x, y))
        if k is None:
            return False, f"no node at junction ({x:g},{y:g})"
        g = jfind(k)
        missing = [short(n) for n in names if n not in touching[g]]
        ok = degree[g] >= mind and not missing
        msg = f"junction ({x:g},{y:g}): {degree[g]} edges (need {mind}+)"
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
        want = value_for(a[1], PROX)
        got = sum(1 for k, c in coord.items() if inside(area, c) and nprops[k].get("barrier") == "kerb")
        return got == want, f"kerb nodes: expected {want}, got {got}"
    if kind == "no_kerb_merge":
        bad = [k for k, c in coord.items() if inside(area, c) and nprops[k].get("barrier") == "kerb"
               and "ext:union_audit_barrier" in nprops[k]]
        return not bad, "no kerb absorbed another" if not bad else f"kerb merged into node {bad[0]}"
    if kind == "minted":
        got = sum(1 for k, c in coord.items() if inside(area, c) and c not in input_coords)
        return got == a[1], f"new nodes: expected {a[1]}, got {got}"
    if kind == "connectors":
        rule = a[2] if len(a) > 2 else None
        got = sum(1 for e in edges if is_connector(e["properties"])
                  and inside(area, e["geometry"]["coordinates"][0])
                  and (rule is None or rule in e["properties"]["ext:union_rule"].split(",")))
        return got == a[1], f"connectors{' by ' + rule if rule else ''}: expected {a[1]}, got {got}"
    if kind == "node_attr":
        k = node_near(a[1])
        if k is None:
            return False, f"no node at ({a[1][0]:g},{a[1][1]:g})"
        bad = attrs_ok(nprops[k], a[2])
        return not bad, "attributes as expected" if not bad else "node: " + ", ".join(bad)
    if kind == "node_absent":
        k = node_near(a[1])
        return k is None, f"no node at ({a[1][0]:g},{a[1][1]:g})" if k is None else f"a node remains at ({a[1][0]:g},{a[1][1]:g})"
    if kind == "edge_attr":
        cs = [e for e in edges if is_connector(e["properties"]) and inside(area, e["geometry"]["coordinates"][0])
              and a[1] in e["properties"]["ext:union_rule"].split(",")]
        if not cs:
            return False, f"no connector by {a[1]}"
        bad = [b for e in cs for b in attrs_ok(e["properties"], a[2])]
        return not bad, "connector attributes as expected" if not bad else "connector: " + ", ".join(bad)
    if kind == "features":
        fs = [f for f in out[KIND_OUT[a[1]]] if inside(area, feature_point(f))]
        return len(fs) == a[2], f"{a[1]}s: expected {a[2]}, got {len(fs)}"
    if kind == "feature_attr":
        fs = [f for f in out[KIND_OUT[a[1]]] if inside(area, feature_point(f))]
        if a[2] is not None:
            target = C(*a[2])
            fs = sorted(fs, key=lambda f: dist_m(feature_point(f), target))[:1]
        if not fs:
            return False, f"no {a[1]} in the case area"
        bad = [b for f in fs for b in attrs_ok(f["properties"], a[3])]
        return not bad, f"{a[1]} attributes as expected" if not bad else f"{a[1]}: " + ", ".join(bad)
    raise ValueError(kind)

# ---- cases ----------------------------------------------------------------------
areas = {c["id"]: case_area(inp, c["id"]) for c in CASES}
checked = [c for c in CASES if asserts_for(c, args.profile)]
case_fail = {c["id"]: [] for c in checked}
junction_feats = []
for c in checked:
    area = areas[c["id"]]
    for a in asserts_for(c, args.profile):
        ok, msg = check(a, area)
        if ok is None:
            continue
        if not ok:
            case_fail[c["id"]].append(msg)
        aa = a[2] if a[0] == "at" else a
        if aa[0] == "junction":
            k = node_near(aa[1]); g = jfind(k) if k else None
            junction_feats.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": list(C(*aa[1]))},
                                   "properties": {"case": c["id"], "status": "PASS" if ok else "FAIL",
                                                  "degree": degree[g] if g else 0, "min_degree": aa[2],
                                                  "label": f'{c["id"]} {"PASS" if ok else "FAIL"} ({degree[g] if g else 0})',
                                                  "detail": msg}})

# ---- output integrity ---------------------------------------------------------------
print(f"\nOutput integrity — {args.run_dir} (profile {args.profile})")
ids = [str(n["properties"]["_id"]) for n in nodes]
used = set(); mismatch = dangling = 0
for e in edges:
    p = e["properties"]; cs = e["geometry"]["coordinates"]
    for key, end in (("_u_id", cs[0]), ("_v_id", cs[-1])):
        k = str(p[key]); used.add(k)
        if k not in coord: dangling += 1
        elif tuple(coord[k]) != tuple(end): mismatch += 1
zone_bad = 0
for z in out["zones"]:
    for k in z["properties"].get("_w_id", []):
        used.add(str(k))
        if str(k) not in coord: zone_bad += 1
# input orphans (nodes no input edge or zone uses) are passed through, never removed
in_used = set()
for ds in ("ds1", "ds2"):
    for e in inp[ds]["edges"]: in_used |= {(ds, str(e["properties"]["_u_id"])), (ds, str(e["properties"]["_v_id"]))}
    for z in inp[ds]["zones"]: in_used |= {(ds, str(k)) for k in z["properties"].get("_w_id", [])}
in_orphan_coords = {tuple(n["geometry"]["coordinates"]) for ds in ("ds1", "ds2") for n in inp[ds]["nodes"]
                    if (ds, str(n["properties"]["_id"])) not in in_used}
def numeric(x): return str(x).isdigit()
non_numeric = sum(1 for f in nodes + edges + out["zones"] + out["points"] + out["lines"] + out["polygons"]
                  if not numeric(f["properties"]["_id"]))
non_numeric += sum(1 for e in edges if not (numeric(e["properties"]["_u_id"]) and numeric(e["properties"]["_v_id"])))
integrity = [
    ("edge endpoint differs from its _u_id/_v_id node", mismatch, True),
    ("edge references a node missing from the output", dangling, True),
    ("zone _w_id references a node missing from the output", zone_bad, True),
    ("duplicate node ids", sum(v - 1 for v in Counter(ids).values() if v > 1), True),
    ("duplicate edge ids", sum(v - 1 for v in Counter(str(e["properties"]["_id"]) for e in edges).values() if v > 1), True),
    ("non-numeric _id / _u_id / _v_id", non_numeric, True),
    ("two nodes at one coordinate (coincident nodes not joined)",
     sum(v - 1 for v in Counter(coord.values()).values() if v > 1), prof["coincident"]),
    ("orphan nodes the union left behind (no edge or zone uses them)",
     sum(1 for k in coord if k not in used and coord[k] not in in_orphan_coords), True),
]
bad_integrity = False
for label, n, applies in integrity:
    flag = ("ok  " if n == 0 else "FAIL") if applies else "n/a "
    print(f"  {flag}  {n:4}  {label}")
    bad_integrity |= applies and n > 0

# ---- 'off': the output must be the input, unchanged ----------------------------------
bad_off = False
if args.profile == "off":
    print("\nNothing configured — output must equal DS1 + DS2:")
    def geoms(fs): return Counter(json.dumps(f["geometry"]["coordinates"]) for f in fs)
    for kind in ("nodes", "edges", "zones", "points", "lines", "polygons"):
        want = geoms(inp["ds1"][kind] + inp["ds2"][kind]); got = geoms(out[kind])
        ok = want == got
        print(f"  {'ok  ' if ok else 'FAIL'}  {kind:9} input {sum(want.values()):4}  output {sum(got.values()):4}"
              + ("" if ok else "  (geometries differ)"))
        bad_off |= not ok
    tagged = sum(1 for kind in out for f in out[kind] if any(k.startswith("ext:union_") for k in f["properties"]))
    print(f"  {'ok  ' if tagged == 0 else 'FAIL'}  features carrying ext:union_* tags: {tagged}")
    bad_off |= tagged > 0

# ---- report checks ----------------------------------------------------------------------
print("\nReport checks (report.json):")
bad_report = False
rp = os.path.join(args.run_dir, "report.json")
report = json.load(open(rp)) if os.path.exists(rp) else None
if report is None:
    print("  FAIL  report.json missing"); bad_report = True
else:
    def metric(stage, rule):
        for r in report.get("rules", []):
            if r["stage"] == stage and r["rule"] == rule: return r.get("metrics", {})
        return None
    for chk in profiles.REPORT_CHECKS.get(args.profile, []):
        if chk[0] == "warning":
            ok = any(chk[1] in w for w in report.get("warnings", [])); what = f"a warning mentions '{chk[1]}'"
        elif chk[0] == "no_warnings":
            ok = not report.get("warnings"); what = "no warnings" + ("" if ok else f": {report['warnings']}")
        elif chk[0] == "effective_rules":
            rules = (report.get("effective_config", {}).get(chk[1]) or {}).get("rules", [])
            ok = len(rules) == chk[2]; what = f"effective {chk[1]} rules = {chk[2]} (got {len(rules)})"
        elif chk[0] == "metric":
            _, stage, rule, key, op, n = chk
            m = metric(stage, rule); got = None if m is None else m.get(key)
            ok = got is not None and {"==": got == n, ">=": got >= n, "<=": got <= n}[op]
            what = f"{stage}/{rule} {key} {op} {n} (got {got})"
        else:
            raise ValueError(chk)
        print(f"  {'ok  ' if ok else 'FAIL'}  {what}")
        bad_report |= not ok
    for w in report.get("warnings", []):
        print(f"  note  warning: {w}")

# ---- case table ---------------------------------------------------------------------------
case_feats, n_fail = [], 0
print(f"\nCase results — {args.run_dir} (profile {args.profile}: {prof['title']})\n")
for c in checked:
    failed = case_fail[c["id"]]
    status = "PASS" if not failed else "FAIL"
    n_fail += bool(failed)
    print(f"  {status}  {c['id']:5} {c['title']}")
    for m in failed:
        print(f"          x {m}")
    case_feats.append({"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [areas[c["id"]]]},
                       "properties": {"case": c["id"], "title": c["title"], "status": status,
                                      "label": f'{c["id"]} {status}', "expected": c["expected"],
                                      "failures": "; ".join(failed)}})
write(os.path.join(args.run_dir, "case_results.geojson"), case_feats)
write(os.path.join(args.run_dir, "junction_results.geojson"), junction_feats)

changes.run(args.run_dir, PROX)

print("\nNetwork-wide routability:")
bad_route = routability.run(args.run_dir, prof)

print(f"\n{len(checked) - n_fail}/{len(checked)} cases passed; integrity {'PASS' if not bad_integrity else 'FAIL'}; "
      f"report {'PASS' if not bad_report else 'FAIL'}; routability {'PASS' if not bad_route else 'FAIL'}"
      + (f"; unchanged-output {'PASS' if not bad_off else 'FAIL'}" if args.profile == "off" else ""))
print(f"QGIS result layers written to {args.run_dir}/")
sys.exit(1 if (n_fail or bad_route or bad_integrity or bad_report or bad_off) else 0)
