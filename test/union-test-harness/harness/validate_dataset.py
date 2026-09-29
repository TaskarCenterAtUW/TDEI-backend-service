import json, math, os
os.chdir(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"))   # the test inputs
from collections import Counter

def load(f): return json.load(open(f))["features"]
ok_all = True

for ds in ("ds1","ds2"):
    nodes = load(f"{ds}_nodes.geojson"); edges = load(f"{ds}_edges.geojson")
    print(f"\n=== {ds.upper()} : {len(nodes)} nodes, {len(edges)} edges ===")
    nid = {}
    for n in nodes:
        nid[str(n["properties"]["_id"])] = tuple(n["geometry"]["coordinates"])

    # 1. unique ids
    ids = [str(n["properties"]["_id"]) for n in nodes]
    dup = [k for k,v in Counter(ids).items() if v>1]
    eids = [str(e["properties"]["_id"]) for e in edges]
    edup = [k for k,v in Counter(eids).items() if v>1]
    print(f"  duplicate node ids: {len(dup)}   duplicate edge ids: {len(edup)}")
    ok_all &= not dup and not edup

    # 2. every edge endpoint EXACTLY equals its _u_id/_v_id node
    mism = 0
    for e in edges:
        p=e["properties"]; c=e["geometry"]["coordinates"]
        for coord, key in ((tuple(c[0]),"_u_id"), (tuple(c[-1]),"_v_id")):
            target = nid.get(str(p[key]))
            if target is None or target != coord: mism += 1
    print(f"  endpoint/node coordinate mismatches: {mism}")
    ok_all &= mism==0

    # 3. 7 decimal places max
    def dp(x):
        s = repr(float(x))
        return len(s.split(".")[1]) if "." in s and "e" not in s else 0
    bad = 0
    for n in nodes:
        lo,la = n["geometry"]["coordinates"]
        if dp(lo)>7 or dp(la)>7: bad+=1
    for e in edges:
        for lo,la in e["geometry"]["coordinates"]:
            if dp(lo)>7 or dp(la)>7: bad+=1
    print(f"  coordinates beyond 7dp: {bad}")
    ok_all &= bad==0

    # 4. geometry validity: >=2 points, no zero-length, no repeated consecutive pts
    zl=0; short=0
    for e in edges:
        c=e["geometry"]["coordinates"]
        if len(c)<2: short+=1
        for i in range(len(c)-1):
            if c[i]==c[i+1]: zl+=1
    print(f"  edges with <2 points: {short}   zero-length segments: {zl}")
    ok_all &= short==0 and zl==0

    # 5. every vertex is a registered node
    coords = set(nid.values()); missing=0
    for e in edges:
        for c in e["geometry"]["coordinates"]:
            if tuple(c) not in coords: missing+=1
    print(f"  vertices not registered as nodes: {missing}")
    ok_all &= missing==0

print(f"\n{'ALL VALID ✓' if ok_all else 'VALIDATION FAILED ✗'}")
