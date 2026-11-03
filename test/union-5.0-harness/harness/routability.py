"""Network-wide routability checks for a union 5.0 run.

    python3 harness/routability.py runs/default_3m --profile default_3m

Run by check_cases.py for every profile. Exit code 0 = routable.

 1. PRESERVATION (every profile) — every edge connected in an input stays
    connected in the output. Catches a union DESTROYING a connection (e.g. a
    duplicate removed while what hung off it is left stranded).
 2. STRANDED (every profile) — a node that joined two or more input edges
    joins fewer in the output: an edge was removed and nothing took its place.
 3. JUNCTIONS (profiles on the default rule sets) — DS1 and DS2 line ends that
    the default rules should join (coincident, or compatible and within the
    profile's proximity) end up in one connected piece. Catches a union
    FAILING to make a connection.
 4. T-ONTO-SPAN (profiles on the default rule sets) — a DS1/DS2 dead end within
    proximity of the middle of a compatible line of the other dataset, meeting
    it at 30° or more, is connected to it.
Compatible = what the default snap and connect rules join: the same category
(crossing, pedestrian, living street, road, other), a crossing and a sidewalk or
living street, a crossing and a road. Two kerbs never join unless coincident.
"""
import math, sys
from collections import defaultdict
from common import inputs, outputs, is_connector
MLON = 111111 * math.cos(math.radians(44.63)); MLAT = 111111.0

def m(a, b): return math.hypot((a[0] - b[0]) * MLON, (a[1] - b[1]) * MLAT)
def cat(p):
    hw, fw = p.get("highway"), p.get("footway")
    if hw == "footway" and fw in ("crossing", "traffic_island"): return "crossing"
    if hw == "living_street": return "living_street"
    if hw in ("footway", "pedestrian", "steps"): return "pedestrian"
    if hw in ("primary", "secondary", "tertiary", "residential", "service", "unclassified", "trunk", "motorway"):
        return "road"
    return "other"
def landing(p):
    return (p.get("highway") == "footway" and p.get("footway") == "sidewalk") or p.get("highway") == "living_street"
def tags(p):
    t = {cat(p)}
    if landing(p): t.add("landing")
    return t
def ends_compatible(a, b):
    """Two line ends the default rules join: same category (snap), or a crossing and a
    landing way / road (connect)."""
    same = (a & b) - {"landing"}
    cross = lambda x, y: "crossing" in x and ("landing" in y or "road" in y)
    return bool(same) or cross(a, b) or cross(b, a)
def t_compatible(end, span):
    """A dead end the default connect rules join onto a span (from -> to)."""
    if "crossing" in end and ({"landing", "road", "crossing"} & span): return True
    return bool(({"pedestrian", "living_street"} & end) & span)

def angle_deg(u, v):
    ux, uy = u; vx, vy = v
    nu, nv = math.hypot(ux, uy), math.hypot(vx, vy)
    if nu == 0 or nv == 0: return 90.0
    c = abs(ux * vx + uy * vy) / (nu * nv)
    return math.degrees(math.acos(min(1.0, c)))

def run(run_dir, prof, label="  routability"):
    prox = prof["proximity"]; full = prof["routability"] == "full"
    inp, out = inputs(), outputs(run_dir)
    on, oe = out["nodes"], out["edges"]
    ocoord = {str(x["properties"]["_id"]): tuple(x["geometry"]["coordinates"]) for x in on}
    by_coord = defaultdict(list)
    for k, c in ocoord.items(): by_coord[c].append(k)
    par = {}
    def f(a):
        par.setdefault(a, a)
        while par[a] != a: par[a] = par[par[a]]; a = par[a]
        return a
    types = defaultdict(set); names = defaultdict(set); odeg = defaultdict(int); last_dir = {}
    for x in oe:
        p = x["properties"]; u, v = str(p["_u_id"]), str(p["_v_id"]); par[f(u)] = f(v)
        odeg[u] += 1; odeg[v] += 1
        if is_connector(p): continue
        cs = x["geometry"]["coordinates"]
        for k, a, b in ((u, cs[0], cs[1]), (v, cs[-1], cs[-2])):
            types[k] |= tags(p); names[k].add(p.get("name", ""))
            last_dir[k] = ((a[0] - b[0]) * MLON, (a[1] - b[1]) * MLAT)
    ds_of = defaultdict(set)                     # dataset(s) an input coordinate belongs to
    for ds in ("ds1", "ds2"):
        for n in inp[ds]["nodes"]: ds_of[tuple(n["geometry"]["coordinates"])].add(ds)
    def out_nodes(c):                            # input coordinate -> candidate output nodes
        if c in by_coord: return by_coord[c]
        best = min(ocoord, key=lambda k: m(ocoord[k], c), default=None)
        return [best] if best is not None and m(ocoord[best], c) <= prox else []

    # 1. preservation
    broken = []
    for ds in ("ds1", "ds2"):
        icoord = {str(x["properties"]["_id"]): tuple(x["geometry"]["coordinates"]) for x in inp[ds]["nodes"]}
        for x in inp[ds]["edges"]:
            p = x["properties"]
            A, B = out_nodes(icoord[str(p["_u_id"])]), out_nodes(icoord[str(p["_v_id"])])
            if A and B and not any(f(a) == f(b) for a in A for b in B):
                broken.append((ds.upper(), p.get("name", "")))
    # 2. stranded
    stranded = []
    for ds in ("ds1", "ds2"):
        icoord = {str(x["properties"]["_id"]): tuple(x["geometry"]["coordinates"]) for x in inp[ds]["nodes"]}
        ideg = defaultdict(int); iname = defaultdict(set)
        for x in inp[ds]["edges"]:
            p = x["properties"]
            for k in (str(p["_u_id"]), str(p["_v_id"])): ideg[k] += 1; iname[k].add(p.get("name", ""))
        for k, dg in ideg.items():
            if dg < 2: continue
            cands = out_nodes(icoord[k])
            if cands and max(odeg[o] for o in cands) < dg:
                stranded.append((ds.upper(), sorted(iname[k])))
    junction, tjunc, dup = [], [], []
    if full:
        kerb = {str(x["properties"]["_id"]) for x in on if x["properties"].get("barrier") == "kerb"}
        # 3. junctions between DS1 and DS2 line ends
        ends = sorted(k for k in types if k in ocoord)
        for i in range(len(ends)):
            for j in range(i + 1, len(ends)):
                a, b = ends[i], ends[j]
                if f(a) == f(b): continue
                da, db = ds_of.get(ocoord[a], set()), ds_of.get(ocoord[b], set())
                if not any(x != y for x in da for y in db): continue          # same dataset only
                d = m(ocoord[a], ocoord[b])
                if d > prox: continue
                if d > 0 and a in kerb and b in kerb: continue
                if d == 0 or ends_compatible(types[a], types[b]):
                    (dup if len(names[a] | names[b]) == 1 else junction).append((round(d, 2), sorted(names[a] | names[b])))
        # 4. a dead end meeting the middle of a compatible line of the other dataset
        def seg_d(pt, a, b):
            dx, dy = (b[0] - a[0]) * MLON, (b[1] - a[1]) * MLAT; px, py = (pt[0] - a[0]) * MLON, (pt[1] - a[1]) * MLAT
            L = dx * dx + dy * dy; t = 0 if L == 0 else max(0, min(1, (px * dx + py * dy) / L))
            return math.hypot(px - t * dx, py - t * dy), t, (dx, dy)
        for k in ends:
            if odeg[k] != 1: continue
            dk = ds_of.get(ocoord[k], set())
            for x in oe:
                p = x["properties"]
                if is_connector(p): continue
                u, v = str(p["_u_id"]), str(p["_v_id"])
                if f(u) == f(k) or not t_compatible(types[k], tags(p)): continue
                c = x["geometry"]["coordinates"]
                dx_ = set().union(*[ds_of.get(tuple(q), set()) for q in c])
                if not any(a != b for a in dk for b in dx_): continue
                for i in range(len(c) - 1):
                    d, t, seg = seg_d(ocoord[k], c[i], c[i + 1])
                    if d <= prox and 0.02 < t < 0.98 and angle_deg(last_dir.get(k, (0, 0)), seg) >= 30:
                        tjunc.append((round(d, 2), sorted(names[k]) + [p.get("name", "")])); break
    bad = len(broken) + len(junction) + len(stranded) + len(tjunc)
    print(f"[{label}]  {'PASS' if bad == 0 else 'FAIL'}  broken={len(broken)} stranded={len(stranded)}"
          + (f" junction-gaps={len(junction)} T-onto-span={len(tjunc)} duplicates-kept={len(dup)}" if full
             else "  (junction checks apply to the default profiles only)"))
    for ds, nm in broken:   print(f"    BROKEN   {ds} edge now disconnected: {nm}")
    for ds, nm in stranded: print(f"    STRANDED {ds} node lost an edge: {' | '.join(x for x in nm if x)}")
    for d, nm in junction:  print(f"    JUNCTION {d} m not connected: {' | '.join(nm)}")
    for d, nm in tjunc:     print(f"    T-SPAN   {d} m dead end meets middle of: {' | '.join(x for x in nm if x)}")
    for d, nm in dup:       print(f"    dup-kept {d} m: {nm[0]}")
    return bad

if __name__ == "__main__":
    import argparse, profiles
    ap = argparse.ArgumentParser(description="Routability check for a union 5.0 run.")
    ap.add_argument("run_dir"); ap.add_argument("--profile", required=True)
    a = ap.parse_args()
    sys.exit(1 if run(a.run_dir, profiles.get(a.profile)) else 0)
