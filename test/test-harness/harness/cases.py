"""
Case catalogue for the OSW union test harness — the SINGLE source of truth.

Every case carries plain-language text (shown as labels in QGIS and on the case
sheet) and machine-checkable assertions (run by check_cases.py). Keeping both in
one definition means what you read on the map is exactly what gets tested.

Expectations are for a DEFAULT run (no filters, no duplicate settings) at
proximity 1 m or 3 m. Where the two differ, an assertion value is a dict
{1.0: ..., 3.0: ...}.

Coordinates are metres east/north of the dataset origin; C() converts them to
lon/lat exactly as the dataset generator does.
"""
import math

LON0, LAT0 = -123.1000000, 44.6300000
MLAT = 111111.0
MLON = MLAT * math.cos(math.radians(LAT0))

def C(x, y):
    """metres -> (lon, lat), rounded to the dataset's 7 dp."""
    return (round(LON0 + x / MLON, 7), round(LAT0 + y / MLAT, 7))

def M(lon, lat):
    """(lon, lat) -> metres, the inverse of C() (for plotting)."""
    return ((lon - LON0) * MLON, (lat - LAT0) * MLAT)

# Assertion vocabulary (see check_cases.py for how each is evaluated):
#   ("count",     edge_name, n)             edges in the output with this name
#   ("junction",  (x, y), min_deg, [names]) a node within 5 cm of (x, y) joining
#                                           at least min_deg edges, touching edges
#                                           with every listed name
#   ("connected", [names])                  all edges with these names form one
#                                           connected piece of network
#   ("apart",     name_a, name_b)           the two never share a connected piece
#   ("kerbs",     n)                        barrier=kerb nodes inside the case area
#   ("no_kerb_merge",)                      no kerb node absorbed another kerb
#   ("ixn",       n)                        minted intersection (ixn-) nodes in area
#   ("node_attr", (x, y), {key: value})     node at (x, y) has these attributes
#                                           (value None = key must merely exist)
#   ("at", proximity, assertion)            the wrapped assertion applies ONLY at that
#                                           proximity (for behaviour that legitimately
#                                           differs, e.g. a node inside 3 m but not 1 m)

CASES = [
 dict(id="A", title="Exact duplicate sidewalk",
      setup="Identical sidewalk in both datasets; the DS1 and DS2 end nodes carry different tactile_paving values.",
      expected="One sidewalk. The merged end node keeps DS1's tactile_paving=no, gains DS2-only ext:surface, and audits DS2's 'yes'.",
      asserts=[("count","A: duplicate sidewalk",1),
               ("node_attr",(0,0),{"tactile_paving":"no","ext:surface":"concrete",
                                   "ext:union_audit_tactile_paving":None})]),
 dict(id="B", title="Drifting near-parallel sidewalk",
      setup="DS2 sidewalk 0.4-1.2 m off the DS1 sidewalk, not quite parallel.",
      expected="DS2 copy removed as a duplicate; only the DS1 sidewalk remains.",
      asserts=[("count","B: base sidewalk",1),("count","B: drifting sidewalk",0)]),
 dict(id="C", title="Road x crossing",
      setup="DS1 road crossed by a DS2 crossing, no node where they meet.",
      expected="A new intersection node where they cross; road and crossing each split in two, all four pieces meeting at it.",
      asserts=[("count","C: road",2),("count","C: crossing",2),("ixn",1),
               ("junction",(100,30),4,["C: road","C: crossing"])]),
 dict(id="D", title="Two kerbs 2.5 m apart",
      setup="Each dataset has a sidewalk ending in a kerb; the kerbs are 2.5 m apart.",
      expected="Kerbs are never merged with each other. At 1 m both sidewalks and kerbs remain. "
               "At 3 m the DS2 sidewalk is removed as a duplicate and its kerb goes with it (open design question).",
      asserts=[("no_kerb_merge",),
               ("count","D: sidewalk to kerb",{1.0:2,3.0:1}),
               ("kerbs",{1.0:2,3.0:1})]),
 dict(id="E", title="Road and sidewalk 1.5 m apart",
      setup="DS1 road with a DS2 sidewalk running alongside, inside proximity.",
      expected="Both kept and NOT connected: a road never merges with a sidewalk.",
      asserts=[("count","E: road",1),("count","E: parallel sidewalk",1),
               ("apart","E: road","E: parallel sidewalk")]),
 dict(id="F", title="Living street vs sidewalk",
      setup="Two living streets 0.5 m apart, plus a DS2 sidewalk 3 m away.",
      expected="The living streets merge into one; the sidewalk stays separate (living street is its own category).",
      asserts=[("count","F: living street",1),("count","F: nearby sidewalk",1),
               ("apart","F: living street","F: nearby sidewalk")]),
 dict(id="G", title="Short T-stub onto a span",
      setup="1.2 m DS2 stub meeting the MIDDLE of a long DS1 sidewalk (no node there).",
      expected="Stub kept (it crosses, it is not a duplicate). The DS1 sidewalk is split where the stub meets it, so all three connect.",
      asserts=[("count","G: long sidewalk",2),("count","G: short T stub",1),
               ("junction",(160,30),3,["G: long sidewalk","G: short T stub"])]),
 dict(id="H", title="Endpoints 0.36 m apart",
      setup="DS2 sidewalk starts 0.36 m from the end of a DS1 sidewalk.",
      expected="The two join at the DS1 node into one continuous path; the merged node keeps DS1's attributes.",
      asserts=[("count","H: sidewalk west",1),("count","H: sidewalk east",1),
               ("junction",(40,120),2,["H: sidewalk west","H: sidewalk east"]),
               ("node_attr",(40,120),{"tactile_paving":"no"})]),
 dict(id="I", title="Road only in DS2",
      setup="Service road present only in DS2.",
      expected="Kept unchanged — nothing to merge with.",
      asserts=[("count","I: DS2-only service road",1)]),
 dict(id="J", title="DS1 node on a DS2 span",
      setup="DS1 spur ends on the middle of a long DS2 sidewalk.",
      expected="The DS2 sidewalk is split at the DS1 node; all three pieces meet there.",
      asserts=[("count","J: long DS2 sidewalk",2),("count","J: DS1 spur meeting it",1),
               ("junction",(40,150),3,["J: long DS2 sidewalk","J: DS1 spur meeting it"])]),
 dict(id="K", title="Plain footway duplicate",
      setup="Two plain footways (highway=footway, no subtype) 0.4 m apart.",
      expected="Merged into one.",
      asserts=[("count","K: plain footway",1)]),
 dict(id="L", title="Crossing ending on a road node",
      setup="DS2 crossing ends EXACTLY on an existing mid node of a DS1 road.",
      expected="The existing node is reused — no new intersection node. Road split there; crossing joins it.",
      asserts=[("count","L: road with mid node",2),("count","L: crossing ending on road node",1),
               ("ixn",0),
               ("junction",(240,40),3,["L: road with mid node","L: crossing ending on road node"])]),
 dict(id="M", title="Duplicate crossing",
      setup="Identical crossing in both datasets.",
      expected="Merged into one.",
      asserts=[("count","M: crossing",1)]),
 dict(id="N", title="Sidewalk - footway - crossing chain",
      setup="DS1 sidewalk and plain-footway connector; DS2 crossing continuing the chain.",
      expected="One continuous route: sidewalk to connector to crossing, including across the dataset boundary.",
      asserts=[("junction",(20,210),2,["N: sidewalk","N: plain footway connector"]),
               ("junction",(30,210),2,["N: plain footway connector","N: crossing"]),
               ("connected",["N: sidewalk","N: plain footway connector","N: crossing"])]),
 dict(id="P", title="Duplicate with a crossing attached",
      setup="DS2 duplicate sidewalk with a DS2 crossing hanging off its end.",
      expected="Duplicate removed; the crossing re-attaches to the DS1 sidewalk and stays connected.",
      asserts=[("count","P: sidewalk",1),("count","P: duplicate sidewalk",0),
               ("count","P: crossing off duplicate end",1),
               ("junction",(360,0),2,["P: sidewalk","P: crossing off duplicate end"])]),
 dict(id="Q", title="Three-segment chain between two DS1 ends",
      setup="DS2 chain of three sidewalk segments bridging two DS1 sidewalks.",
      expected="One continuous route end to end; all four junctions connect.",
      asserts=[("connected",["Q: ds1 west","Q: ds2 link a","Q: ds2 link b","Q: ds2 link c","Q: ds1 east"]),
               ("junction",(340,40),2,["Q: ds1 west","Q: ds2 link a"]),
               ("junction",(360,40),2,["Q: ds2 link a","Q: ds2 link b"]),
               ("junction",(380,40),2,["Q: ds2 link b","Q: ds2 link c"]),
               ("junction",(400,40),2,["Q: ds2 link c","Q: ds1 east"])]),
 dict(id="R", title="Mid-span duplicate with a spur",
      setup="DS2 duplicate along the middle of a DS1 sidewalk, with a DS2 spur off its end.",
      expected="Duplicate removed; DS1 sidewalk split where the spur meets it; spur stays connected.",
      asserts=[("count","R: long ds1 sidewalk",2),("count","R: mid-span duplicate",0),
               ("count","R: spur off duplicate",1),
               ("junction",(380,80),3,["R: long ds1 sidewalk","R: spur off duplicate"])]),
 dict(id="S", title="T onto a DS1 span",
      setup="DS2 sidewalk ends on the middle of a DS1 sidewalk.",
      expected="DS1 sidewalk split at that point; the DS2 sidewalk connects there.",
      asserts=[("count","S: ds1 sidewalk span",2),("count","S: ds2 T onto span",1),
               ("junction",(370,120),3,["S: ds1 sidewalk span","S: ds2 T onto span"])]),
 dict(id="U", title="Partial overlap",
      setup="DS2 sidewalk 2 m off a DS1 sidewalk, continuing 45 m past its end.",
      expected="1 m: kept whole and NOT connected - the DS1 end is 2 m away, beyond proximity. "
               "3 m: the DS1 end is within proximity, so the DS2 sidewalk is split there, the overlapping "
               "half is removed as a duplicate, and the continuing half joins the DS1 sidewalk end - one "
               "continuous route. (With duplicate_buffer_width 3 m at 1 m, the threshold decides: removed "
               "below ~53% overlap.)",
      asserts=[("count","U: ds1 sidewalk",1),("count","U: partly overlapping sidewalk",1),
               ("at",1.0,("apart","U: ds1 sidewalk","U: partly overlapping sidewalk")),
               ("at",3.0,("junction",(320,180),2,["U: ds1 sidewalk","U: partly overlapping sidewalk"])),
               ("at",3.0,("connected",["U: ds1 sidewalk","U: partly overlapping sidewalk"]))]),
 dict(id="V", title="Duplicate, surfaces differ",
      setup="Identical sidewalk in both datasets; DS1 tagged surface=concrete, DS2 surface=asphalt.",
      expected="One sidewalk (the DS2 copy is a duplicate). Filter case: under an edge filter only DS1 "
               "satisfies, both copies stay - a duplicate is removed only when BOTH sides pass.",
      asserts=[("count","V: surface-tagged duplicate",1)]),
 dict(id="W", title="Coincident kerb, attributes differ",
      setup="DS1 and DS2 sidewalks meet at a kerb node present in both; DS2's copy adds tactile_paving=yes.",
      expected="The kerbs merge into one node joining both sidewalks; it keeps DS1's tags, gains "
               "tactile_paving=yes and audits it. Filter case: filters may stop the attribute merge, "
               "never the connection.",
      asserts=[("junction",(130,240),2,["W: ds1 sidewalk to kerb","W: ds2 sidewalk from kerb"]),
               ("kerbs",1),
               ("node_attr",(130,240),{"barrier":"kerb","kerb":"lowered","tactile_paving":"yes",
                                       "ext:union_audit_tactile_paving":None})]),
]

def value_for(v, proximity):
    """Resolve a per-proximity dict to a value."""
    if isinstance(v, dict):
        key = min(v, key=lambda k: abs(k - proximity))
        return v[key]
    return v
