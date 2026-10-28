"""
Generate the union 5.0 harness test datasets (DS1 + DS2) from the case geometry.

    python3 harness/generate_dataset.py          # writes data/ds1_*.geojson, data/ds2_*.geojson

Every case's features carry a name (edges) or an ext:case tag (points, lines,
polygons, zones) starting with the case id, so each case can be found on a map.
Coordinates are metres east/north of the origin, rounded to 7 decimal places.
"""
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
    # TDEI stores every _id as bigint (generated columns), so ids are numeric
    # strings. Ranges keep entity types and the two datasets' ids apart.
    ID_BASE = {"ds1": dict(node=1000000, edge=1500000, zone=1700000, point=1800000, line=1850000, polygon=1900000),
               "ds2": dict(node=2000000, edge=2500000, zone=2700000, point=2800000, line=2850000, polygon=2900000)}

    def __init__(self, name):
        self.name = name
        self.base = self.ID_BASE[name]
        self.nodes = {}      # coord -> node_id
        self.node_props = {} # node_id -> props
        self.edges = []
        self.other = {"zone": [], "point": [], "line": [], "polygon": []}

    def _next(self, kind, seq):
        return str(self.base[kind] + seq)

    def node(self, coord, **props):
        """register (or reuse) a node at an exact coordinate"""
        if coord not in self.nodes:
            nid = self._next("node", len(self.nodes) + 1)
            self.nodes[coord] = nid
            self.node_props[nid] = dict(props)
        elif props:
            self.node_props[self.nodes[coord]].update(props)
        return self.nodes[coord]

    def edge(self, coords, **props):
        """every vertex becomes a node (matches real OSW datasets)"""
        ids = [self.node(c) for c in coords]
        eid = self._next("edge", len(self.edges) + 1)
        self.edges.append({"id": eid, "coords": coords, "u": ids[0], "v": ids[-1], "props": dict(props)})
        return eid

    def zone(self, ring, **props):
        """a zone: closed ring whose vertices are registered nodes; _w_id lists them"""
        if ring[0] != ring[-1]: ring = ring + [ring[0]]
        ids = [self.node(c) for c in ring]
        zid = self._next("zone", len(self.other["zone"]) + 1)
        self.other["zone"].append(("Polygon", [[list(c) for c in ring]], dict(props, _w_id=ids), zid))

    def point(self, coord, **props):
        pid = self._next("point", len(self.other["point"]) + 1)
        self.other["point"].append(("Point", list(coord), dict(props), pid))

    def line(self, coords, **props):
        lid = self._next("line", len(self.other["line"]) + 1)
        self.other["line"].append(("LineString", [list(c) for c in coords], dict(props), lid))

    def polygon(self, ring, **props):
        if ring[0] != ring[-1]: ring = ring + [ring[0]]
        gid = self._next("polygon", len(self.other["polygon"]) + 1)
        self.other["polygon"].append(("Polygon", [[list(c) for c in ring]], dict(props), gid))

    def dump(self):
        out = {}
        nodes = {"$schema": SCHEMA_URL, "type": "FeatureCollection", "features": []}
        for coord, nid in self.nodes.items():
            p = {"_id": nid}; p.update(self.node_props[nid])
            nodes["features"].append({"type": "Feature",
                "geometry": {"type": "Point", "coordinates": [coord[0], coord[1]]}, "properties": p})
        edges = {"$schema": SCHEMA_URL, "type": "FeatureCollection", "features": []}
        for e in self.edges:
            p = {"_id": e["id"], "_u_id": e["u"], "_v_id": e["v"]}; p.update(e["props"])
            edges["features"].append({"type": "Feature",
                "geometry": {"type": "LineString", "coordinates": [[c[0], c[1]] for c in e["coords"]]},
                "properties": p})
        out["nodes"], out["edges"] = nodes, edges
        for kind, plural in (("zone", "zones"), ("point", "points"), ("line", "lines"), ("polygon", "polygons")):
            fc = {"$schema": SCHEMA_URL, "type": "FeatureCollection", "features": []}
            for gtype, coords, props, fid in self.other[kind]:
                p = {"_id": fid}; p.update(props)
                fc["features"].append({"type": "Feature", "geometry": {"type": gtype, "coordinates": coords},
                                       "properties": p})
            out[plural] = fc
        return out

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

# ── Crossing ↔ sidewalk junctions (X series) ─────────────────────────────────
# A crossing must connect to the sidewalk it meets. "Sidewalk" here means
# highway=footway + footway=sidewalk only; plain footways are out of scope (X8).
# Rules under test:
#   1. a crossing END connects to the sidewalk it reaches (connect rule
#      crossing-sidewalk, operation geometry);
#   2. the sidewalk gets a node where the crossing meets it and is split there;
#      across a gap a connector edge joins the crossing end — nothing moves;
#   3. a kerb (barrier=kerb) never moves, whichever dataset it is in.
# Every sidewalk here is drawn north-south (constant x), so a crossing end
# given the same x lies exactly on the sidewalk line at 7 dp.

# CASE X1 — DS2 crossing ends ON a DS1 sidewalk span, 0.6 m below the sidewalk
#           corner node (inside proximity at 1 m and 3 m). Split, don't snap.
ds1.edge([C(0,330),  C(20,330)],   name="X1: sidewalk west",  **SIDEWALK)
ds1.edge([C(20,330), C(20,300)],   name="X1: sidewalk south", **SIDEWALK)
ds2.edge([C(40,329.4), C(20,329.4)], name="X1: crossing", **CROSSING)

# CASE X2 — the same geometry with the datasets swapped: DS1 crossing ending on
#           a DS2 sidewalk span 0.6 m below the DS2 corner.
ds2.edge([C(60,330), C(80,330)],   name="X2: sidewalk west",  **SIDEWALK)
ds2.edge([C(80,330), C(80,300)],   name="X2: sidewalk south", **SIDEWALK)
ds1.edge([C(100,329.4), C(80,329.4)], name="X2: crossing", **CROSSING)

# CASE X3 — DS2 crossing ending in a kerb exactly on a DS1 sidewalk span.
x3k = C(140,315)
ds1.edge([C(140,300), C(140,330)], name="X3: sidewalk", **SIDEWALK)
ds2.edge([C(160,315), x3k],        name="X3: crossing to kerb", **CROSSING)
ds2.node(x3k, barrier="kerb")

# CASE X4 — DS2 crossing ending in a kerb 0.4 m short of a DS1 sidewalk span.
#           The kerb does not move: a node is made on the sidewalk and a
#           connector joins the kerb to it.
x4k = C(200.4,315)
ds1.edge([C(200,300), C(200,330)], name="X4: sidewalk", **SIDEWALK)
ds2.edge([C(220,315), x4k],        name="X4: crossing to kerb", **CROSSING)
ds2.node(x4k, barrier="kerb")

# CASE X5 — DS2 crossing ending in a kerb 0.4 m beyond a DS1 sidewalk's END
#           (in line with it). The kerb cannot move; a connector joins it to
#           the sidewalk's end node.
x5k = C(260,330.4)
ds1.edge([C(260,300), C(260,330)], name="X5: sidewalk", **SIDEWALK)
ds2.edge([C(260,345), x5k],        name="X5: crossing to kerb", **CROSSING)
ds2.node(x5k, barrier="kerb")

# CASE X6 — the same as X5 with a bare crossing end: a connector joins it to the sidewalk end.
ds1.edge([C(320,300), C(320,330)],   name="X6: sidewalk", **SIDEWALK)
ds2.edge([C(320,345), C(320,330.4)], name="X6: crossing", **CROSSING)

# CASE X7 — DS1 crossing ending in a kerb; a DS2 sidewalk ends 0.4 m short of it.
#           A connector joins the kerb to the sidewalk end; nothing moves.
x7k = C(380,330)
ds1.edge([C(380,345), x7k],          name="X7: crossing to kerb", **CROSSING)
ds1.node(x7k, barrier="kerb")
ds2.edge([C(380,300), C(380,329.6)], name="X7: sidewalk", **SIDEWALK)

# CASE X8 — scope: a DS2 crossing ending on the span of a DS1 PLAIN footway
#           (highway=footway, no subtype). Out of scope: not split, not joined.
ds1.edge([C(0,360), C(0,390)],  name="X8: plain footway", highway="footway")
ds2.edge([C(20,375), C(0,375)], name="X8: crossing", **CROSSING)

# CASE X9 — two DS2 crossings ending 0.3 m apart on the same DS1 sidewalk span.
#           Each splits the sidewalk at its own end: two junctions, both connected.
ds1.edge([C(60,360), C(60,390)],     name="X9: sidewalk", **SIDEWALK)
ds2.edge([C(80,375), C(60,375)],     name="X9: crossing east", **CROSSING)
ds2.edge([C(75,360), C(60,375.3)],   name="X9: crossing south-east", **CROSSING)

# CASE X10 — a crossing junction near a sidewalk corner (real-data case).
#            DS2 sidewalk is ONE line bending round the corner, with a vertex
#            node 3 m above it. DS1 has three crossings meeting at a junction J
#            2.6 m from the corner; one runs from J to an end exactly ON the
#            corner. J's nearest point on the sidewalk is the corner too, but
#            the crossing already meets the sidewalk there: J must not split it.
ds2.edge([C(140,390), C(140,363), C(140,360), C(170,362)], name="X10: sidewalk", **SIDEWALK)
x10j = C(139,357.6)
ds1.edge([x10j, C(140,360)],  name="X10: crossing to corner", **CROSSING)
ds1.edge([x10j, C(115,355)],  name="X10: crossing west",      **CROSSING)
ds1.edge([x10j, C(125,335)],  name="X10: crossing south",     **CROSSING)

# CASE X11 — X10 with the datasets swapped: DS1 sidewalk, DS2 crossings. J must
#            not snap onto the corner (that would collapse the crossing J-corner).
ds1.edge([C(260,390), C(260,363), C(260,360), C(290,362)], name="X11: sidewalk", **SIDEWALK)
x11j = C(259,357.6)
ds2.edge([x11j, C(260,360)],  name="X11: crossing to corner", **CROSSING)
ds2.edge([x11j, C(235,355)],  name="X11: crossing west",      **CROSSING)
ds2.edge([x11j, C(245,335)],  name="X11: crossing south",     **CROSSING)

# CASES X12 / X13 — a real junction (Albany): a sidewalk bending round a corner
#            as one line, with a vertex S 2.29 m west of the corner; crossings
#            A-B (A exactly on the corner, junction B 0.6 m east), B-east (with a
#            bend vertex 2.7 m from the corner) and B-south (passing 2.3 m from S).
#            Only the crossing END A meets the sidewalk. S must not split the
#            south crossing (X12) and the east crossing's bend must not split the
#            sidewalk (X13).
def x12_13(cid, ox, oy, sidewalk_ds, crossing_ds):
    P = lambda x, y: C(ox + x, oy + y)
    sidewalk_ds.edge([P(-13.39,-0.43), P(-2.29,-0.07), P(0,0), P(-1.39,6.57)], name=f"{cid}: sidewalk", **SIDEWALK)
    crossing_ds.edge([P(0,0), P(0.6,-0.03)],                      name=f"{cid}: crossing A-B",   **CROSSING)
    crossing_ds.edge([P(0.6,-0.03), P(2.69,-0.14), P(14.13,-2.43)], name=f"{cid}: crossing east",  **CROSSING)
    crossing_ds.edge([P(0.6,-0.03), P(0.49,-7.51), P(0.44,-10.8)],  name=f"{cid}: crossing south", **CROSSING)
x12_13("X12", 60, 440, ds1, ds2)    # as in the screenshot: DS1 sidewalk, DS2 crossings
x12_13("X13", 120, 440, ds2, ds1)   # datasets swapped

# ── Crossing ↔ living street junctions (LS series) ───────────────────────────
# A living street (highway=living_street) is a shared space pedestrians walk
# along, so a crossing reaching one must connect to it exactly as it would to a
# sidewalk: the same snap / split / kerb / nearer-end rules as the X series.
# A living street still never joins a sidewalk, footway or road (LS8, case F).

# CASE LS1 — DS2 crossing ends ON the span of a DS1 living street. Split.
ds1.edge([C(0,480), C(0,510)],  name="LS1: living street", **LIVING)
ds2.edge([C(20,495), C(0,495)], name="LS1: crossing",      **CROSSING)

# CASE LS2 — swapped: DS1 crossing ends on the span of a DS2 living street.
ds2.edge([C(60,480), C(60,510)], name="LS2: living street", **LIVING)
ds1.edge([C(80,495), C(60,495)], name="LS2: crossing",      **CROSSING)

# CASE LS3 — bare DS2 crossing end 0.4 m beyond a DS1 living street's END: a connector joins them.
ds1.edge([C(120,480), C(120,510)],   name="LS3: living street", **LIVING)
ds2.edge([C(120,525), C(120,510.4)], name="LS3: crossing",      **CROSSING)

# CASE LS4 — swapped: DS2 living street ends 0.4 m short of a bare DS1 crossing
#            end. A connector joins them; nothing moves.
ds1.edge([C(180,525), C(180,510.4)], name="LS4: crossing",      **CROSSING)
ds2.edge([C(180,480), C(180,510)],   name="LS4: living street", **LIVING)

# CASE LS5 — DS2 crossing ends in a kerb 0.4 m short of a DS1 living street span.
#            The kerb never moves: a node is made on the living street and a
#            connector joins the kerb to it.
ls5k = C(240.4,495)
ds1.edge([C(240,480), C(240,510)], name="LS5: living street",    **LIVING)
ds2.edge([C(260,495), ls5k],       name="LS5: crossing to kerb", **CROSSING)
ds2.node(ls5k, barrier="kerb")

# CASES LS6 / LS7 — the X10 / X11 corner junction with a living street in place
#            of the sidewalk: only the crossing END on the corner meets the living
#            street; the crossing junction J 2.6 m away neither splits it (LS6)
#            nor snaps onto it (LS7).
def ls_corner(cid, ox, street_ds, crossing_ds):
    P = lambda x, y: C(ox + x, y)
    street_ds.edge([P(25,600), P(25,573), P(25,570), P(55,572)], name=f"{cid}: living street", **LIVING)
    j = P(24,567.6)
    crossing_ds.edge([j, P(25,570)],  name=f"{cid}: crossing to corner", **CROSSING)
    crossing_ds.edge([j, P(0,565)],   name=f"{cid}: crossing west",      **CROSSING)
    crossing_ds.edge([j, P(10,545)],  name=f"{cid}: crossing south",     **CROSSING)
ls_corner("LS6", 0,   ds2, ds1)     # DS2 living street, DS1 crossings (as X10)
ls_corner("LS7", 100, ds1, ds2)     # DS1 living street, DS2 crossings (as X11)

# CASE LS8 — scope: a living street never joins a SIDEWALK. One DS2 sidewalk ends
#            on the span of a DS1 living street, another 0.4 m beyond its end.
#            Neither splits nor snaps; all three stay unconnected.
ds1.edge([C(300,480), C(300,510)],   name="LS8: living street",        **LIVING)
ds2.edge([C(320,495), C(300,495)],   name="LS8: sidewalk to span",     **SIDEWALK)
ds2.edge([C(300,525), C(300,510.4)], name="LS8: sidewalk beyond end",  **SIDEWALK)


# ══ Union 5.0 configuration cases ════════════════════════════════════════════
# These rows are checked under the custom profiles (harness/profiles.py). The
# default profiles see them too, and a few are checked there as well.

# ── custom_snap: per-type snap distances (crossings 1.5 m, sidewalks 3 m,
#    everything else 0.5 m, coincident on) ─────────────────────────────────────
# SN1 — sidewalk ends 2.5 m apart: within the 3 m sidewalk rule.
ds1.edge([C(0,640),    C(20,640)],   name="SN1: ds1 sidewalk", **SIDEWALK)
ds2.edge([C(22.5,640), C(42.5,640)], name="SN1: ds2 sidewalk", **SIDEWALK)
# SN2 — crossing ends 2 m apart: beyond the 1.5 m crossing rule.
ds1.edge([C(60,640),   C(80,640)],   name="SN2: ds1 crossing", **CROSSING)
ds2.edge([C(82,640),   C(102,640)],  name="SN2: ds2 crossing", **CROSSING)
# SN3 — two corners 2 m apart, each on a sidewalk AND a crossing. The first rule
#       both corners match is the crossing rule (1.5 m), so they stay apart even
#       though both are also sidewalk nodes (3 m).
ds1.edge([C(120,640), C(140,640)], name="SN3: ds1 sidewalk", **SIDEWALK)
ds1.edge([C(140,640), C(140,625)], name="SN3: ds1 crossing", **CROSSING)
ds2.edge([C(142,640), C(162,640)], name="SN3: ds2 sidewalk", **SIDEWALK)
ds2.edge([C(142,640), C(142,625)], name="SN3: ds2 crossing", **CROSSING)
# SN4 — plain footway ends 0.4 m apart: the 0.5 m 'others' distance applies.
ds1.edge([C(200,640),   C(220,640)], name="SN4: ds1 footway", highway="footway")
ds2.edge([C(220.4,640), C(240,640)], name="SN4: ds2 footway", highway="footway")
# SN5 — plain footway ends 0.7 m apart: beyond 'others'.
ds1.edge([C(260,640),   C(280,640)], name="SN5: ds1 footway", highway="footway")
ds2.edge([C(280.7,640), C(300,640)], name="SN5: ds2 footway", highway="footway")
# SN6 — a footway end and a crossing end at the SAME coordinate: joined by
#       'coincident', whatever their types.
ds1.edge([C(320,640), C(340,640)], name="SN6: ds1 footway", highway="footway")
ds2.edge([C(340,640), C(360,640)], name="SN6: ds2 crossing", **CROSSING)

# ── custom_anchor: DS2 as the anchor, and attributes anchor_only ────────────
# AN1 — sidewalk ends 0.4 m apart, sidewalk rule anchored on DS2: the DS1 end
#       moves onto the DS2 node, which keeps its position and its values.
ds1.edge([C(0,680),    C(20,680)], name="AN1: ds1 sidewalk", **SIDEWALK)
ds2.edge([C(20.4,680), C(40,680)], name="AN1: ds2 sidewalk", **SIDEWALK)
ds1.node(C(20,680),   **{"tactile_paving": "no"})
ds2.node(C(20.4,680), **{"tactile_paving": "yes"})
# AN2 — crossing ends 0.3 m apart, crossing rule anchored on DS1 with
#       attributes anchor_only: DS2's values are audited, never added.
ds1.edge([C(60,680),   C(80,680)],  name="AN2: ds1 crossing", **CROSSING)
ds2.edge([C(80.3,680), C(100,680)], name="AN2: ds2 crossing", **CROSSING)
ds1.node(C(80,680),   **{"ext:note": "ds1"})
ds2.node(C(80.3,680), **{"ext:lit": "yes"})
# AN3 — duplicate sidewalk 0.3 m apart, dedup anchored on DS2: the DS1 copy goes.
ds1.edge([C(120,680),   C(180,680)],   name="AN3: ds1 copy", **SIDEWALK)
ds2.edge([C(120,680.3), C(180,680.3)], name="AN3: ds2 copy", **SIDEWALK)

# ── custom_connect: ordered connect rules with node_tags / edge_tags ──────────────────────────
# CC1 — O1: a crossing end 0.5 m short of a sidewalk. Rule kerbed-crossing takes
#       it; the broader footway-sidewalk rule (highway=footway) skips it.
ds1.edge([C(20,700), C(20,740)],   name="CC1: sidewalk", **SIDEWALK)
ds2.edge([C(0,720),  C(19.5,720)], name="CC1: crossing", **CROSSING)
# CC2 — a plain footway 0.5 m short of a sidewalk: only footway-sidewalk matches.
ds1.edge([C(80,700), C(80,740)],   name="CC2: sidewalk", **SIDEWALK)
ds2.edge([C(60,720), C(79.5,720)], name="CC2: plain footway", highway="footway")
# CC3 — O2: kerbed-crossing makes node J at (140,720); a footway ends 0.78 m
#       from J (0.6 m from the span). footway-sidewalk reuses J.
ds1.edge([C(140,700), C(140,740)],     name="CC3: sidewalk", **SIDEWALK)
ds2.edge([C(120,720), C(139.5,720)],   name="CC3: crossing", **CROSSING)
ds2.edge([C(125,735), C(139.4,720.5)], name="CC3: plain footway", highway="footway")
# CC4 — O3: kerbed-crossing's 0.8 m connector crosses a DS1 road lying 0.4 m
#       off the sidewalk. The later road-crossing rule must not split it.
ds1.edge([C(200,700),   C(200,740)],   name="CC4: sidewalk", **SIDEWALK)
ds1.edge([C(199.6,700), C(199.6,740)], name="CC4: road", **ROAD)
ds2.edge([C(180,720),   C(199.2,720)], name="CC4: crossing", **CROSSING)
# CC5 — road-crossing (point, 0.3 m): one crossing crosses the road, another
#       stops 0.2 m short of it. Both get a node tagged tactile_paving=yes.
ds1.edge([C(260,700), C(260,760)],   name="CC5: road", **ROAD)
ds2.edge([C(245,750), C(275,750)],   name="CC5: crossing through", **CROSSING)
ds2.edge([C(245,715), C(259.8,715)], name="CC5: crossing short", **CROSSING)
# CC6 — G1: a crossing on a bridge passes over the road: never joined.
ds1.edge([C(320,700), C(320,760)], name="CC6: road", **ROAD)
ds2.edge([C(305,730), C(335,730)], name="CC6: bridge crossing", bridge="yes", layer="1", **CROSSING)
# CC7 — sidewalk-sidewalk: a sidewalk ends 0.6 m short of another's span (T).
ds1.edge([C(0,800),  C(40,800)],   name="CC7: ds1 sidewalk", **SIDEWALK)
ds2.edge([C(20,820), C(20,800.6)], name="CC7: ds2 sidewalk T", **SIDEWALK)
# CC8 — W5: a sidewalk running nearly parallel 0.6-0.9 m off another: no T.
ds1.edge([C(60,800),   C(100,800)],  name="CC8: ds1 sidewalk", **SIDEWALK)
ds2.edge([C(70,800.9), C(90,800.6)], name="CC8: ds2 near-parallel", **SIDEWALK)
# CC9 — W2: a crossing end 0.4 m from one sidewalk and 0.9 m from another.
ds1.edge([C(140,780),   C(140,820)],   name="CC9: near sidewalk", **SIDEWALK)
ds1.edge([C(140.5,780), C(140.5,820)], name="CC9: far sidewalk", **SIDEWALK)
ds2.edge([C(120,800),   C(139.6,800)], name="CC9: crossing", **CROSSING)
# CC10 — W3: two sidewalk ends 0.57 m apart, each nearest the other's end.
ds1.edge([C(180,800),   C(200,800)],     name="CC10: ds1 sidewalk", **SIDEWALK)
ds2.edge([C(200.4,780), C(200.4,799.6)], name="CC10: ds2 sidewalk", **SIDEWALK)
# CC11 — W4: a 0.75 m sidewalk stub whose ends are 0.2 m and 0.95 m from a span.
ds1.edge([C(260,780),    C(260,820)],   name="CC11: ds1 sidewalk", **SIDEWALK)
ds2.edge([C(259.05,800), C(259.8,800)], name="CC11: ds2 stub", **SIDEWALK)
# CC12 — G3: a DS1 crossing 0.5 m short of a DS1 sidewalk: same dataset, untouched.
ds1.edge([C(320,780), C(320,820)],   name="CC12: ds1 sidewalk", **SIDEWALK)
ds1.edge([C(300,800), C(319.5,800)], name="CC12: ds1 crossing", **CROSSING)
# CC13 — W1: two crossings 0.5 m and 0.4 m short of one sidewalk: split twice.
ds1.edge([C(380,780), C(380,820)],   name="CC13: sidewalk", **SIDEWALK)
ds2.edge([C(360,790), C(379.5,790)], name="CC13: crossing south", **CROSSING)
ds2.edge([C(360,810), C(379.6,810)], name="CC13: crossing north", **CROSSING)
# CC14 — a crossing that overshoots a sidewalk by 0.4 m: it meets the sidewalk
# where it crosses it (a node there; sidewalk and crossing both split; no connector).
ds1.edge([C(440,780), C(440,820)],   name="CC14: sidewalk", **SIDEWALK)
ds2.edge([C(420,800), C(440.4,800)], name="CC14: crossing", **CROSSING)

# ── Dedup guards ─────────────────────────────────────────────────────────────
# DG1 — stranding guard: a DS2 near-duplicate (0.5 m off, its ends 4 m beyond
# snap reach) with a DS2 footway hanging off its west end. Dedup removes it;
# nothing connects the footway to DS1, so the guard restores it after connect.
ds1.edge([C(0,960), C(40,960)],       name="DG1: ds1 sidewalk", **SIDEWALK)
ds2.edge([C(-4,960.5), C(36,960.5)],  name="DG1: ds2 sidewalk", **SIDEWALK)
ds2.edge([C(-4,960.5), C(-4,980)],    name="DG1: ds2 spur", highway="footway")
# DG2 — connectivity guard: DS1 draws the sidewalk as two edges (a node at x=120),
# DS2 as one edge 0.3 m off. DS2's ends snap onto DS1's ends; both ends are then
# on the anchor network but no single DS1 edge joins them.
ds1.edge([C(100,960), C(120,960)],    name="DG2: ds1 sidewalk", **SIDEWALK)
ds1.edge([C(120,960), C(140,960)],    name="DG2: ds1 sidewalk", **SIDEWALK)
ds2.edge([C(100,960.3), C(140,960.3)], name="DG2: ds2 sidewalk", **SIDEWALK)

# ── Zones and extension features (the 'extensions' section) ─────────────────
# E1 — a bench in both datasets 0.3 m apart, and a DS2 pole 0.5 m away.
ds1.point(C(0,900),     amenity="bench", **{"ext:case": "E1", "ext:color": "green"})
ds2.point(C(0.3,900),   amenity="bench", **{"ext:case": "E1", "ext:color": "red"})
ds2.point(C(0.2,900.5), power="pole",    **{"ext:case": "E1"})
# E2 — the same fence line in both datasets, 0.3 m apart.
ds1.line([C(40,900), C(80,900)],     barrier="fence", **{"ext:case": "E2"})
ds2.line([C(40,900.3), C(80,900.3)], barrier="fence", **{"ext:case": "E2", "ext:height": "1.2"})
# E3 — the same building in both datasets (0.5 m shift) plus a DS2-only kiosk.
ds1.polygon([C(100,890), C(120,890), C(120,910), C(100,910)], building="yes", **{"ext:case": "E3"})
ds2.polygon([C(100.5,890), C(120.5,890), C(120.5,910), C(100.5,910)], building="yes",
            **{"ext:case": "E3", "ext:levels": "2"})
ds2.polygon([C(130,890), C(140,890), C(140,900), C(130,900)], building="kiosk", **{"ext:case": "E3"})
# E4 — the same pedestrian zone in both datasets, 0.2 m apart.
ds1.zone([C(160,890), C(180,890), C(180,910), C(160,910)], highway="pedestrian", **{"ext:case": "E4"})
ds2.zone([C(160.2,890), C(180.2,890), C(180.2,910), C(160.2,910)], highway="pedestrian", **{"ext:case": "E4"})

# E5 — the same hedge in both datasets, 2 m apart: merges only when the Lines'
#      own distance (line_proximity) reaches 2 m.
ds1.line([C(220,900), C(260,900)], barrier="hedge", **{"ext:case": "E5"})
ds2.line([C(220,902), C(260,902)], barrier="hedge", **{"ext:case": "E5", "ext:height": "0.8"})
# E6 — a waste basket in both datasets, 1.5 m apart: Points use proximity only.
ds1.point(C(290,900),   amenity="waste_basket", **{"ext:case": "E6"})
ds2.point(C(291.5,900), amenity="waste_basket", **{"ext:case": "E6", "ext:color": "black"})

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

import os as _os
_OUT = _os.environ.get("OSW_DATASET_DIR",
                      _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "data"))
for name, ds in (("ds1", ds1), ("ds2", ds2)):
    for kind, fc in ds.dump().items():
        fn = f"{name}_{kind}.geojson"
        json.dump(fc, open(_os.path.join(_OUT, fn), "w"), indent=1)
        print(f"{fn}: {len(fc['features'])} features")
