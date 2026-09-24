"""Routability checks for an OSW union output.

Usage:
  python3 harness/routability.py <union_output_dir> --proximity 3 \
      --ds1 data/ds1_nodes.geojson data/ds1_edges.geojson \
      --ds2 data/ds2_nodes.geojson data/ds2_edges.geojson

Exit code 0 = routable, 1 = a connection was broken or not made.

 1. PRESERVATION — every edge connected in an input must stay connected in the
    output. Catches a union DESTROYING a connection (e.g. a duplicate removed
    while what hung off its endpoint is left stranded).
 2. JUNCTIONS    — endpoints that should meet (coincident, or compatible and
    within proximity) must end up in the same component. Catches a union
    FAILING to create a connection.
Duplicates deliberately left unmerged by a filter are reported separately:
by design they are duplication, not disconnection."""
import json, math, sys
from collections import defaultdict
MLON=111111*math.cos(math.radians(44.63)); MLAT=111111.0
def m(a,b): return math.hypot((a[0]-b[0])*MLON,(a[1]-b[1])*MLAT)
def cat(p):
    hw,fw=p.get("highway"),p.get("footway")
    if hw=="footway" and fw in ("crossing","traffic_island"): return "crossing"
    if hw=="living_street": return "bike"
    if hw in ("footway","pedestrian","steps"): return "pedestrian"
    return "road" if hw else "other"
def load(p): return json.load(open(p))["features"]
def run(outdir, prox, label, inputs):
    on=load(f"{outdir}/osw_nodes.geojson"); oe=load(f"{outdir}/osw_edges.geojson")
    ocoord={str(x["properties"]["_id"]):tuple(x["geometry"]["coordinates"]) for x in on}
    by_coord=defaultdict(list)
    for k,c in ocoord.items(): by_coord[c].append(k)
    par={}
    def f(a):
        par.setdefault(a,a)
        while par[a]!=a: par[a]=par[par[a]]; a=par[a]
        return a
    types=defaultdict(set); names=defaultdict(set)
    for x in oe:
        p=x["properties"]; u,v=str(p["_u_id"]),str(p["_v_id"]); par[f(u)]=f(v)
        for k in (u,v): types[k].add(cat(p)); names[k].add(p.get("name",""))
    def to_out(c):                       # input coordinate -> output node id
        if c in by_coord: return by_coord[c][0]
        best=min(ocoord, key=lambda k:m(ocoord[k],c)); return best if m(ocoord[best],c)<=prox else None
    # 1. preservation
    broken=[]
    for ds,npath,epath in inputs:
        icoord={str(x["properties"]["_id"]):tuple(x["geometry"]["coordinates"]) for x in load(npath)}
        for x in load(epath):
            p=x["properties"]; a,b=to_out(icoord[p["_u_id"]]),to_out(icoord[p["_v_id"]])
            if a and b and f(a)!=f(b): broken.append((ds,p.get("name","")))
    # 3. stranded — a node that joined >=2 edges in an input now joins fewer:
    #    an incident edge was removed and nothing took its place.
    odeg=defaultdict(int)
    for x in oe:
        p=x["properties"]; odeg[str(p["_u_id"])]+=1; odeg[str(p["_v_id"])]+=1
    stranded=[]
    for ds,npath,epath in inputs:
        icoord={str(x["properties"]["_id"]):tuple(x["geometry"]["coordinates"]) for x in load(npath)}
        ideg=defaultdict(int); iname=defaultdict(set)
        for x in load(epath):
            p=x["properties"]
            for k in (p["_u_id"],p["_v_id"]): ideg[k]+=1; iname[k].add(p.get("name",""))
        for k,dg in ideg.items():
            if dg<2: continue
            o=to_out(icoord[k])
            if o is not None and odeg[o]<dg: stranded.append((ds,sorted(iname[k])))
    # 4. T-junction onto an edge interior — a dead end within proximity of the
    #    middle of a compatible edge in another component; no node to meet at.
    tjunc=[]
    def seg_d(pt,a,b):
        dx,dy=(b[0]-a[0])*MLON,(b[1]-a[1])*MLAT; px,py=(pt[0]-a[0])*MLON,(pt[1]-a[1])*MLAT
        L=dx*dx+dy*dy; t=0 if L==0 else max(0,min(1,(px*dx+py*dy)/L))
        return math.hypot(px-t*dx,py-t*dy), t
    for k,dg in odeg.items():
        if dg!=1 or k not in ocoord: continue
        for x in oe:
            p=x["properties"]; u,v=str(p["_u_id"]),str(p["_v_id"])
            if f(u)==f(k) or cat(p) not in types[k]: continue
            c=x["geometry"]["coordinates"]
            for i in range(len(c)-1):
                d,t=seg_d(ocoord[k],c[i],c[i+1])
                if d<=prox and 0.02<t<0.98:
                    tjunc.append((round(d,2),sorted(names[k])+[p.get("name","")])); break
    # 2. junctions
    junction=[]; dup=[]
    ends=sorted(types)
    for i in range(len(ends)):
        for j in range(i+1,len(ends)):
            a,b=ends[i],ends[j]
            if f(a)==f(b): continue
            d=m(ocoord[a],ocoord[b])
            if d>prox: continue
            same_feature = len(names[a]|names[b])==1          # both copies of one feature
            if d==0 or (types[a]&types[b]):
                (dup if same_feature else junction).append((round(d,2),sorted(names[a]|names[b])))
    bad=len(broken)+len(junction)+len(stranded)+len(tjunc)
    print(f"[{label}]  {'PASS' if bad==0 else 'FAIL'}  "
          f"broken={len(broken)} junction-gaps={len(junction)} stranded={len(stranded)} "
          f"T-onto-span={len(tjunc)} duplicates-kept={len(dup)}")
    for ds,nm in broken:   print(f"    BROKEN   {ds} edge now disconnected: {nm}")
    for d,nm in junction:  print(f"    JUNCTION {d} m not connected: {' | '.join(nm)}")
    for ds,nm in stranded: print(f"    STRANDED {ds} node lost an edge: {' | '.join(x for x in nm if x)}")
    for d,nm in tjunc:     print(f"    T-SPAN   {d} m dead end meets middle of: {' | '.join(x for x in nm if x)}")
    for d,nm in dup:       print(f"    dup-kept {d} m (by design under filter): {nm[0]}")
    return bad
if __name__=="__main__":
    import argparse
    ap=argparse.ArgumentParser(description="Routability check for an OSW union output.")
    ap.add_argument("output_dir", help="folder with osw_nodes.geojson + osw_edges.geojson (union output)")
    ap.add_argument("--ds1", nargs=2, metavar=("NODES","EDGES"), required=True)
    ap.add_argument("--ds2", nargs=2, metavar=("NODES","EDGES"), required=True)
    ap.add_argument("--proximity", type=float, required=True, help="metres, as passed to the union")
    ap.add_argument("--label", default="union")
    a=ap.parse_args()
    inp=[("DS1",*a.ds1),("DS2",*a.ds2)]
    sys.exit(1 if run(a.output_dir, a.proximity, a.label, inp) else 0)
