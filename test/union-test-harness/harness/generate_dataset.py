import json, math

# Origin in Albany, OR (their test area) — flat, easy to read on a map
# Required by OSW: every file declares the schema version it complies with.
SCHEMA_URL = "https://sidewalks.washington.edu/opensidewalks/0.3/schema.json"

LON0, LAT0 = -123.1000000, 44.6300000
MLAT = 111111.0
MLON = 111111.0 * math.cos(math.radians(LAT0))   # ~79,100 m per degree lon

def C(x, y):
    """metres east/north of origin -> (lon,lat) rounded to 7dp (system precision)"""
    return (round(LON0 + x / MLON, 7), round(LAT0 + y / MLAT, 7))

class DS:
    # TDEI stores _id / _u_id / _v_id as bigint (generated columns), so ids are
    # numeric strings. Ranges keep the two datasets' ids apart:
    #   DS1 nodes 1000001…, edges 1500001…   DS2 nodes 2000001…, edges 2500001…
    ID_BASE = {"ds1": (1000000, 1500000), "ds2": (2000000, 2500000)}

    def __init__(self, name):
        self.name = name
        self.node_base, self.edge_base = self.ID_BASE[name]
        self.nodes = {}      # coord -> node_id
        self.node_props = {} # node_id -> props
        self.node_seq = 0
        self.edges = []
        self.edge_seq = 0

    def node(self, coord, **props):
        """register (or reuse) a node at an exact coordinate"""
        if coord not in self.nodes:
            self.node_seq += 1
            nid = str(self.node_base + self.node_seq)
            self.nodes[coord] = nid
            self.node_props[nid] = dict(props)
        elif props:
            self.node_props[self.nodes[coord]].update(props)
        return self.nodes[coord]

    def edge(self, coords, **props):
        """every vertex becomes a node (matches real OSW datasets)"""
        ids = [self.node(c) for c in coords]
        self.edge_seq += 1
        eid = str(self.edge_base + self.edge_seq)
        self.edges.append({
            "id": eid, "coords": coords,
            "u": ids[0], "v": ids[-1], "props": dict(props)
        })
        return eid

    def dump(self):
        nodes = {"$schema": SCHEMA_URL, "type": "FeatureCollection", "features": []}
        for coord, nid in self.nodes.items():
            p = {"_id": nid}
            p.update(self.node_props[nid])
            nodes["features"].append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [coord[0], coord[1]]},
                "properties": p})
        edges = {"$schema": SCHEMA_URL, "type": "FeatureCollection", "features": []}
        for e in self.edges:
            p = {"_id": e["id"], "_u_id": e["u"], "_v_id": e["v"]}
            p.update(e["props"])
            edges["features"].append({
                "type": "Feature",
                "geometry": {"type": "LineString",
                             "coordinates": [[c[0], c[1]] for c in e["coords"]]},
                "properties": p})
        return nodes, edges

SIDEWALK = dict(highway="footway", footway="sidewalk")
CROSSING = dict(highway="footway", footway="crossing")
ROAD     = dict(highway="residential")
SERVICE  = dict(highway="service")
LIVING   = dict(highway="living_street")

ds1, ds2 = DS("ds1"), DS("ds2")

# ─────────────────────────────────────────────────────────────────────────────
# CASE A — exact duplicate sidewalk (identical geometry in both datasets)
#          expect: one copy in output, DS2 copy dropped as duplicate
ds1.edge([C(0,0), C(60,0)],  name="A: duplicate sidewalk", **SIDEWALK)
ds2.edge([C(0,0), C(60,0)],  name="A: duplicate sidewalk", **SIDEWALK)

# CASE B — near-parallel sidewalk that drifts (not exactly parallel, offset ~0.6-1.2m)
#          expect: detected as duplicate only with a wide enough buffer
ds1.edge([C(0,20), C(60,20)], name="B: base sidewalk", **SIDEWALK)
ds2.edge([C(0,20.6), C(30,21.2), C(60,20.4)], name="B: drifting sidewalk", **SIDEWALK)

# CASE C — road x crossing intersection (cross-dataset)
#          expect: shared intersection node minted, both edges split at it
ds1.edge([C(100,-20), C(100,80)], name="C: road", **ROAD)
ds2.edge([C(85,30),  C(115,30)],  name="C: crossing", **CROSSING)

# CASE D — two kerbs 2.5 m apart (distinct real features)
#          expect: NOT merged, both kerb nodes survive
d1k = C(140,10); d2k = C(140,12.5)
ds1.edge([C(132,10), d1k], name="D: sidewalk to kerb", **SIDEWALK)
ds1.node(d1k, barrier="kerb")
ds2.edge([C(132,12.5), d2k], name="D: sidewalk to kerb", **SIDEWALK)
ds2.node(d2k, barrier="kerb")

# CASE E — road and sidewalk running 1.5 m apart
#          expect: NOT merged (different category) even inside proximity
ds1.edge([C(0,60), C(60,60)],     name="E: road", **ROAD)
ds2.edge([C(0,61.5), C(60,61.5)], name="E: parallel sidewalk", **SIDEWALK)

# CASE F — living_street pair (isolated 'bike' category)
#          expect: merges only with the other living_street, never the sidewalk
ds1.edge([C(0,90), C(60,90)],     name="F: living street", **LIVING)
ds2.edge([C(0,90.5), C(60,90.5)], name="F: living street", **LIVING)
ds2.edge([C(0,93), C(60,93)],     name="F: nearby sidewalk", **SIDEWALK)

# CASE G — short perpendicular T-connector (1.2 m stub)
#          expect: KEPT — it is a real connector, not a duplicate
ds1.edge([C(160,0), C(160,60)], name="G: long sidewalk", **SIDEWALK)
ds2.edge([C(160,30), C(161.2,30)], name="G: short T stub", **SIDEWALK)

# CASE H — DS2 endpoint 0.36 m from a DS1 endpoint
#          expect: nodes snap together, the two sidewalks join up
ds1.edge([C(0,120), C(40,120)],   name="H: sidewalk west", **SIDEWALK)
ds2.edge([C(40.3,120.2), C(80,120)], name="H: sidewalk east", **SIDEWALK)

# CASE I — road present only in DS2
#          expect: survives untouched (nothing to merge with)
ds2.edge([C(200,0), C(200,80)], name="I: DS2-only service road", **SERVICE)

# CASE J — DS1 node lands on the interior of a DS2 edge
#          expect: DS2 edge split at that node
ds2.edge([C(0,150), C(80,150)], name="J: long DS2 sidewalk", **SIDEWALK)
ds1.edge([C(40,130), C(40,150)], name="J: DS1 spur meeting it", **SIDEWALK)

# CASE K — plain footway (highway=footway with no footway subtype).
#          OSW recommends a plain Footway to connect a Sidewalk to a Crossing.
#          Every OSW edge must carry an identifying field, so a genuinely
#          untagged edge is not valid OSW and cannot be represented here.
ds1.edge([C(200,120), C(260,120)],     name="K: plain footway", highway="footway")
ds2.edge([C(200,120.4), C(260,120.4)], name="K: plain footway", highway="footway")

# CASE N — sidewalk -> plain footway -> crossing chain (the OSW-recommended
#          connection pattern), split across the two datasets.
ds1.edge([C(0,210), C(20,210)],  name="N: sidewalk", **SIDEWALK)
ds1.edge([C(20,210), C(30,210)], name="N: plain footway connector", highway="footway")
ds2.edge([C(30,210), C(50,210)], name="N: crossing", **CROSSING)

# CASE L — crossing whose END lands EXACTLY on an existing road node
#          expect: exact-match REUSE, no new ixn node minted, no micro-edge
road_mid = C(240,40)
ds1.edge([C(240,0), road_mid, C(240,80)], name="L: road with mid node", **ROAD)
ds2.edge([C(225,40), road_mid], name="L: crossing ending on road node", **CROSSING)

# CASE M — both datasets have the same crossing (duplicate crossing)
#          expect: dedup like sidewalks, same-category only
ds1.edge([C(0,180), C(30,180)], name="M: crossing", **CROSSING)
ds2.edge([C(0,180), C(30,180)], name="M: crossing", **CROSSING)

# ── Routability cases (used with node / edge filters) ────────────────────────
# CASE P — DS2 duplicate sidewalk with a DS2 crossing hanging off its end.
#          If the duplicate is removed while its end node fails to snap, the
#          crossing is left attached to a node that is no longer connected.
ds1.edge([C(320,0), C(360,0)],       name="P: sidewalk", **SIDEWALK)
ds2.edge([C(320,0.2), C(360,0.2)],   name="P: duplicate sidewalk", **SIDEWALK)
ds2.edge([C(360,0.2), C(360,15)],    name="P: crossing off duplicate end", **CROSSING)

# CASE Q — a 3-segment DS2 sidewalk chain meeting the DS1 network at BOTH ends.
#          Every junction must survive for the path to stay routable end-to-end.
ds1.edge([C(320,40), C(340,40)],     name="Q: ds1 west", **SIDEWALK)
ds1.edge([C(400,40), C(420,40)],     name="Q: ds1 east", **SIDEWALK)
ds2.edge([C(340.3,40.1), C(360,40)], name="Q: ds2 link a", **SIDEWALK)
ds2.edge([C(360,40), C(380,40)],     name="Q: ds2 link b", **SIDEWALK)
ds2.edge([C(380,40), C(399.7,40.1)], name="Q: ds2 link c", **SIDEWALK)

# CASE R — DS2 duplicate along the MIDDLE of a DS1 sidewalk, with a DS2 spur
#          attached at the duplicate's end. The duplicate's endpoints have no
#          DS1 node to snap to, so removing it as a duplicate strands the spur.
ds1.edge([C(320,80), C(420,80)],      name="R: long ds1 sidewalk", **SIDEWALK)
ds2.edge([C(340,80.2), C(380,80.2)],  name="R: mid-span duplicate", **SIDEWALK)
ds2.edge([C(380,80.2), C(380,95)],    name="R: spur off duplicate", **SIDEWALK)

# CASE S — DS2 sidewalk ending on the INTERIOR of a DS1 sidewalk (a T onto a
#          span with no node). Nothing currently splits the DS1 edge there.
ds1.edge([C(320,120), C(420,120)],    name="S: ds1 sidewalk span", **SIDEWALK)
ds2.edge([C(370,135), C(370,120.2)],  name="S: ds2 T onto span", **SIDEWALK)

# CASE U — PARTIAL overlap: a DS2 sidewalk 2 m off a DS1 sidewalk that runs on
#          well past the DS1 end. Its endpoints are too far to connect, so only
#          the coverage test decides. With buffer 3 m about 53% of it lies in
#          the corridor, so the overlap threshold alone decides its fate.
ds1.edge([C(260,180), C(320,180)], name="U: ds1 sidewalk", **SIDEWALK)
ds2.edge([C(275,182), C(365,182)], name="U: partly overlapping sidewalk", **SIDEWALK)

# CASE V — FILTER case: identical duplicate sidewalk, but the two copies carry
#          different surface tags (DS1 concrete, DS2 asphalt). Default run: DS2
#          removed as a duplicate. Under an edge filter BOTH sides must pass, so a
#          filter only DS1 satisfies (surface=concrete) keeps both copies.
ds1.edge([C(0,240), C(60,240)], name="V: surface-tagged duplicate", surface="concrete", **SIDEWALK)
ds2.edge([C(0,240), C(60,240)], name="V: surface-tagged duplicate", surface="asphalt", **SIDEWALK)

# CASE W — FILTER case: a DS1 sidewalk and a DS2 sidewalk meeting at a
#          coincident kerb node whose two copies carry different attributes.
#          Coincident nodes always merge (connectivity); whether DS2's
#          tactile_paving is MERGED onto the DS1 kerb is what filters decide.
ds1.edge([C(100,240), C(130,240)], name="W: ds1 sidewalk to kerb", **SIDEWALK)
ds2.edge([C(130,240), C(130,260)], name="W: ds2 sidewalk from kerb", **SIDEWALK)

# ── Node attributes, so property merge + ext:union_audit_* is exercised ──────
# Case A: coincident sidewalk endpoints carrying DIFFERENT values for the same
# key, plus a DS2-only key. DS1 wins the shared key; DS2's value is audited;
# the DS2-only key is merged in.
ds1.node(C(0,0),  **{"tactile_paving": "no"})
ds2.node(C(0,0),  **{"tactile_paving": "yes", "ext:surface": "concrete"})

# Case H: the pair that snaps across 0.36 m also carries attributes.
ds1.node(C(40,120), **{"tactile_paving": "no"})
ds2.node(C(40.3,120.2), **{"tactile_paving": "yes", "ext:note": "ds2 survey"})

# Case L: crossing endpoint exactly on the road node — coincident, different types.
ds2.node(C(240,40), **{"ext:survey_date": "2026-05-01"})

# Case N: plain-footway / crossing junction — coincident, different categories.
ds1.node(C(30,210), **{"ext:junction_note": "ds1"})
ds2.node(C(30,210), **{"ext:junction_note": "ds2", "ext:lit": "yes"})

# Case W: coincident kerb — DS1 lowered kerb, DS2 adds tactile_paving.
ds1.node(C(130,240), **{"barrier": "kerb", "kerb": "lowered"})
ds2.node(C(130,240), **{"barrier": "kerb", "kerb": "lowered", "tactile_paving": "yes"})

n1,e1 = ds1.dump(); n2,e2 = ds2.dump()
import os as _os
_OUT = _os.environ.get("OSW_DATASET_DIR",
                      _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "data"))
for fn,obj in [("ds1_nodes.geojson",n1),("ds1_edges.geojson",e1),
               ("ds2_nodes.geojson",n2),("ds2_edges.geojson",e2)]:
    json.dump(obj, open(_os.path.join(_OUT, fn),"w"), indent=1)
    print(f"{fn}: {len(obj['features'])} features")
