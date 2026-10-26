"""
The demo scene: one realistic neighbourhood where every union behaviour happens
at once, instead of one tiny case per box.

    Harbor Blvd (secondary, east-west) crosses Oak St (residential, north-south).
    Sidewalks run along both sides of both streets, crossings cross every arm of
    the intersection, Market Alley (service) leaves Harbor Blvd to the south and
    Willow Lane, a living street closed to traffic at Harbor Blvd, runs north.
    Buildings, a bus stop with a bench, a fence and a hydrant complete it.

Two sources describe it, as in a real TDEI union:

    DS1  City GIS       roads, every sidewalk, two of the four crossings (with
                        their kerbs), the living street, a park path, a bus stop
    DS2  Field survey   all four crossings with kerbs (one duplicates the city's),
                        part of a sidewalk and part of Harbor Blvd traced again a
                        few decimetres off, a shop entrance path, a hydrant ...

Coordinates are metres east / north of the Harbor Blvd × Oak St centre point;
C() turns them into lon/lat (7 dp, like TDEI data). The scene sits about 500 m
east of the harness test grid, so both can be open in QGIS at once.

SCENARIOS lists the numbered spots the demo picture explains, what DS1 and DS2
hold there, what the union does with the default rules, checks that the output
really does it (demo/check_demo.py runs them), and where to look when describing
what any other config did there (demo/describe.py).
"""
import math

LON0, LAT0 = -123.0880000, 44.6340000
MLAT = 111111.0
MLON = MLAT * math.cos(math.radians(LAT0))

def C(x, y):
    """metres -> [lon, lat] on the 7 dp grid."""
    return [round(LON0 + x / MLON, 7), round(LAT0 + y / MLAT, 7)]

def M(lon, lat):
    """[lon, lat] -> metres (for drawing)."""
    return ((lon - LON0) * MLON, (lat - LAT0) * MLAT)

DATASETS = {"ds1": "City GIS", "ds2": "Field survey"}
EXTENT = (-80, 135, -62, 62)           # x0, x1, y0, y1 of the scene, metres

ROAD = {"highway": "secondary"}
STREET = {"highway": "residential"}
ALLEY = {"highway": "service", "service": "alley"}
LIVING = {"highway": "living_street"}
SIDEWALK = {"highway": "footway", "footway": "sidewalk", "surface": "concrete"}
CROSSING = {"highway": "footway", "footway": "crossing", "crossing": "marked", "crossing:markings": "zebra"}
FOOTWAY = {"highway": "footway"}
KERB = {"barrier": "kerb", "kerb": "lowered", "tactile_paving": "no"}
KERB_SURVEYED = {"barrier": "kerb", "kerb": "lowered", "tactile_paving": "yes"}

# ── edges: (name, tags, [(x, y), ...]); every end is a node, shared exactly ──
EDGES = {
 "ds1": [
    # roads (split where the city's crossings meet them)
    ("Harbor Blvd", ROAD, [(-80, 0), (-10.5, 0)]),
    ("Harbor Blvd", ROAD, [(-10.5, 0), (0, 0)]),
    ("Harbor Blvd", ROAD, [(0, 0), (70, 0)]),
    ("Harbor Blvd", ROAD, [(70, 0), (135, 0)]),
    ("Oak St", STREET, [(0, -62), (0, 0)]),
    ("Oak St", STREET, [(0, 0), (0, 12.5)]),
    ("Oak St", STREET, [(0, 12.5), (0, 62)]),
    ("Market Alley", ALLEY, [(70, 0), (70, -50)]),
    ("Willow Lane", LIVING, [(115, 8.5), (115, 10)]),
    ("Willow Lane", LIVING, [(115, 10), (115, 62)]),
    # sidewalks (10 m off Harbor Blvd, 8 m off Oak St; they meet at the corners)
    ("Harbor Blvd N sidewalk", SIDEWALK, [(-80, 10), (-50, 10)]),
    ("Harbor Blvd N sidewalk", SIDEWALK, [(-50, 10), (-10.5, 10)]),
    ("Harbor Blvd N sidewalk", SIDEWALK, [(-10.5, 10), (-8, 10)]),
    ("Oak St W sidewalk", SIDEWALK, [(-8, 10), (-8, 12.5)]),
    ("Oak St W sidewalk", SIDEWALK, [(-8, 12.5), (-8, 62)]),
    ("Oak St E sidewalk", SIDEWALK, [(8, 10), (8, 12.5)]),
    ("Oak St E sidewalk", SIDEWALK, [(8, 12.5), (8, 62)]),
    ("Harbor Blvd N sidewalk", SIDEWALK, [(8, 10), (115, 10)]),
    ("Harbor Blvd N sidewalk", SIDEWALK, [(115, 10), (135, 10)]),
    ("Harbor Blvd S sidewalk", SIDEWALK, [(-80, -10), (-10.5, -10)]),
    ("Harbor Blvd S sidewalk", SIDEWALK, [(-10.5, -10), (-8, -10)]),
    ("Oak St W sidewalk", SIDEWALK, [(-8, -10), (-8, -62)]),
    ("Oak St E sidewalk", SIDEWALK, [(8, -10), (8, -12.5)]),
    ("Oak St E sidewalk", SIDEWALK, [(8, -12.5), (8, -62)]),
    ("Harbor Blvd S sidewalk", SIDEWALK, [(8, -10), (100, -10)]),
    ("Harbor Blvd S sidewalk", SIDEWALK, [(100, -10), (135, -10)]),
    # the city's two crossings, kerb to kerb, sharing a node with the road
    ("Crossing W (Harbor Blvd)", CROSSING, [(-10.5, 10), (-10.5, 0)]),
    ("Crossing W (Harbor Blvd)", CROSSING, [(-10.5, 0), (-10.5, -10)]),
    ("Crossing N (Oak St)", CROSSING, [(-8, 12.5), (0, 12.5)]),
    ("Crossing N (Oak St)", CROSSING, [(0, 12.5), (8, 12.5)]),
    # footways
    ("Park path", FOOTWAY, [(-50, 10), (-50, 52)]),
    ("Bus stop path", FOOTWAY, [(100, -10), (100, -15)]),
 ],
 "ds2": [
    # the survey's crossings
    ("Crossing W (Harbor Blvd)", CROSSING, [(-10.1, 10.3), (-10.1, -10.3)]),       # the city's, 0.4 m off
    ("Crossing E (Harbor Blvd)", CROSSING, [(10.5, 9.6), (10.5, -10)]),           # new
    ("Crossing S (Oak St)", CROSSING, [(-8.5, -12.5), (8, -12.5)]),               # new; runs 0.5 m past the sidewalk
    ("Crossing (Willow Lane)", CROSSING, [(115, -10), (115, 8)]),                 # new; lands on the living street
    # part of the south sidewalk, traced 0.3-0.4 m off, ending on its own corner
    ("Harbor Blvd S sidewalk", SIDEWALK, [(-60, -10.3), (-10.1, -10.3)]),
    ("Harbor Blvd S sidewalk", SIDEWALK, [(-10.1, -10.3), (-8.4, -10.4)]),
    # part of Harbor Blvd, traced again
    ("Harbor Blvd", ROAD, [(0.3, 0.25), (70.2, -0.3)]),
    ("Harbor Blvd", ROAD, [(70.2, -0.3), (135.3, 0.2)]),
    # a path from a shop door that stops 0.6 m short of the sidewalk
    ("Shop entrance path", FOOTWAY, [(40, -18), (40, -10.6)]),
 ],
}

# node tags by position (every other edge end is a bare node)
NODE_TAGS = {
 "ds1": {(-10.5, 10): KERB, (-10.5, -10): KERB, (-8, 12.5): KERB, (8, 12.5): KERB,
         (8, -12.5): KERB},                                    # a kerb ramp the city recorded, without its crossing
 "ds2": {(-10.1, 10.3): KERB_SURVEYED, (-10.1, -10.3): KERB_SURVEYED,
         (10.5, 9.6): KERB_SURVEYED, (10.5, -10): KERB_SURVEYED,
         (8, -12.5): dict(KERB_SURVEYED, **{"ext:surveyed": "2026-09"}),
         (115, -10): KERB_SURVEYED, (115, 8): KERB_SURVEYED},
}

def rect(x0, y0, x1, y1):
    return [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]

# ── extension features ──
POLYGONS = {
 "ds1": [({"building": "yes", "name": "Harbor Books"}, rect(-76, 18, -56, 50)),
         ({"building": "yes", "name": "Corner Café"}, rect(-42, 18, -16, 46)),
         ({"building": "yes", "name": "Harbor Hall"}, rect(16, 18, 60, 50)),
         ({"building": "yes", "name": "Oak Apartments"}, rect(66, 18, 107, 48)),
         ({"building": "yes", "name": "Bayview Offices"}, rect(-76, -18, -20, -48)),
         ({"building": "retail", "name": "Corner Shops"}, rect(16, -18, 62, -45)),
         ({"building": "yes", "name": "Depot"}, rect(78, -22, 96, -42)),
         ({"building": "yes", "name": "Harbor Market"}, rect(108, -20, 132, -46))],
 "ds2": [({"building": "yes", "name": "Harbor Hall", "building:levels": "2"}, rect(16.6, 18.5, 60.5, 50.4)),
         ({"building": "yes", "name": "Willow Kiosk"}, rect(121, 18, 132, 32))],
}
POINTS = {
 "ds1": [({"amenity": "bench"}, (102, -14.5)),
         ({"highway": "street_lamp"}, (-13, 14))],
 "ds2": [({"amenity": "bench", "ext:condition": "good"}, (102.3, -14.7)),
         ({"amenity": "waste_basket"}, (101.5, -15)),
         ({"emergency": "fire_hydrant"}, (12, -14))],
}
LINES = {
 "ds1": [({"barrier": "fence"}, [(-76, -54), (-20, -54)])],
 "ds2": [({"barrier": "fence", "material": "wood"}, [(-76, -54.4), (-20, -54.3)])],
}

# ── the basemap: carriageway half-widths (metres) for drawing ──
ROAD_WIDTH = {"secondary": 7, "residential": 5, "service": 3, "living_street": 3}

# ── numbered spots the demo explains ──
# at = the spot; badge = where its number sits on the overview; zoom = (x, y, width m)
# of its close-up; look = the boxes (x0, x1, y0, y1) where this spot's action happens:
# demo/describe.py reports what a run did inside them ("This run: ..."), whatever the
# config. input / result say what the sources hold and what the DEFAULT rules at 1 m
# do there; checks test that result (demo/check_demo.py). Under the default config a
# failed check is a FAIL; under any other config it just means "differs from default".
# The checks:
#   ("node", (x, y), {key: value})   an output node within 5 cm; value None = key present,
#                                    ABSENT = key absent
#   ("no_node", (x, y))              no output node within 5 cm
#   ("connector", (x, y), rule)      a connector made by rule passes within 0.5 m of (x, y)
#   ("no_connector", (x, y), r)      no connector within r m of (x, y)
#   ("edges", name, n, box)          n output edges with this name lie inside box (x0, x1, y0, y1)
#   ("points", (x, y), r, n)         n output extension points within r m of (x, y)
#   ("point", (x, y), {key: value})  the output point nearest (x, y) carries these values
#   ("polygon", name, {key: value})  the output polygon with this name carries these values
ABSENT = "<absent>"
SCENARIOS = [
 dict(n=1, title="Snap", at=(-8, -10), badge=(-24, -19), zoom=(-9, -10.5, 7),
      look=[(-8.7, -7.5, -10.8, -9.5)],
      input="The survey's sidewalk corner is 0.57 m from the city's.",
      result="Same type, within 1 m: the survey corner joins the city corner, which keeps its place "
             "and records ext:union_absorbed.",
      checks=[("node", (-8, -10), {"ext:union_absorbed": None}), ("no_node", (-8.4, -10.4))]),
 dict(n=2, title="Duplicate sidewalk removed", at=(-35, -10), badge=(-42, -22), zoom=(-35, -10.2, 7),
      look=[(-40, -30, -11, -9)],
      input="The survey traced 52 m of the south sidewalk again, 0.3 m off.",
      result="Over 80 % of it lies within 1 m of the city's sidewalk, running the same way: "
             "removed. The city's line stays.",
      checks=[("edges", "Harbor Blvd S sidewalk", 2, (-116, -7, -11, -9))]),
 dict(n=3, title="Duplicate crossing removed", at=(-10.3, 5), badge=(-26, 5), zoom=(-10.3, 9.6, 6),
      look=[(-11, -9.6, -1, 11)],
      input="Both sources have the west crossing; the survey's is 0.4 m east, its kerbs 0.45 m from the city's.",
      result="The survey copy is removed. Kerbs never move or merge, so its kerbs go with it "
             "and the city's crossing keeps its own.",
      checks=[("edges", "Crossing W (Harbor Blvd)", 2, (-11, -10, -11, 11)),
              ("node", (-10.5, 10), {"barrier": "kerb"}), ("no_node", (-10.1, 10.3))]),
 dict(n=4, title="Duplicate road removed", at=(40, 0), badge=(48, 5), zoom=(70, -0.1, 7),
      look=[(30, 40, -1, 1), (68.5, 71.5, -1, 1)],
      input="The survey traced Harbor Blvd again; its nodes lie 0.36-0.39 m from the city's.",
      result="The road nodes snap onto the city's, the copy then joins the same two nodes "
             "as a city edge, and is removed.",
      checks=[("no_node", (70.2, -0.3)), ("no_node", (0.3, 0.25)), ("edges", "Harbor Blvd", 4, (-1, 166, -1, 1))]),
 dict(n=5, title="New crossing meets the road", at=(10.5, 0), badge=(26, 4), zoom=(10.5, 0, 6),
      look=[(9.5, 11.5, -1, 1)],
      input="The survey's east crossing crosses Harbor Blvd with no shared node.",
      result="road-crossing: one node where they cross; the crossing and the road are both split there.",
      checks=[("node", (10.5, 0), {"ext:union_rule": "road-crossing"}),
              ("edges", "Crossing E (Harbor Blvd)", 2, (10, 11, -10.5, 10))]),
 dict(n=6, title="Kerb 0.4 m short of the sidewalk", at=(10.5, 9.8), badge=(24, 15), zoom=(10, 9.8, 4),
      look=[(10, 11, 9.5, 10.5)],
      input="The east crossing's north kerb stops 0.4 m short of the sidewalk.",
      result="The kerb never moves. crossing-sidewalk puts a node on the sidewalk, splits it, "
             "and a 0.4 m connector joins the kerb to it.",
      checks=[("connector", (10.5, 9.8), "crossing-sidewalk"), ("node", (10.5, 9.6), {"barrier": "kerb"}),
              ("node", (10.5, 10), {"ext:union_rule": "crossing-sidewalk"})]),
 dict(n=7, title="Kerb on the sidewalk", at=(10.5, -10), badge=(28, -7), zoom=(10, -10.5, 4),
      look=[(10, 11, -10.5, -9.5)],
      input="The east crossing's south kerb lies exactly on the sidewalk line.",
      result="Gap of zero: the sidewalk is split at the kerb, which becomes the junction. No connector.",
      checks=[("node", (10.5, -10), {"barrier": "kerb", "ext:union_rule": "crossing-sidewalk"}),
              ("no_connector", (10.5, -10), 0.3)]),
 dict(n=8, title="Kerbs at the same spot join", at=(8, -12.5), badge=(22, -25), zoom=(8, -12.5, 4),
      look=[(7.5, 8.5, -13, -12)],
      input="The south crossing ends exactly on the kerb ramp the city recorded (no crossing in city data).",
      result="Two kerbs join only when they coincide, as here: one node. The city's "
             "tactile_paving=no stays; the survey's 'yes' is audited, its ext:surveyed added.",
      checks=[("node", (8, -12.5), {"tactile_paving": "no", "ext:union_audit_tactile_paving": None,
                                    "ext:surveyed": "2026-09"})]),
 dict(n=9, title="Crossing runs past the sidewalk", at=(-8, -12.5), badge=(-12, -28), zoom=(-8, -12.5, 4),
      look=[(-8.6, -7.4, -13, -12)],
      input="The south crossing ends 0.5 m beyond the west sidewalk.",
      result="Overshoot: they join where they cross; the crossing and the sidewalk are both split there.",
      checks=[("node", (-8, -12.5), {"ext:union_rule": "crossing-sidewalk"}),
              ("edges", "Crossing S (Oak St)", 3, (-9, 9, -13, -12))]),
 dict(n=10, title="T-junction: shop path", at=(40, -10.3), badge=(44, -25), zoom=(40, -11, 5),
      look=[(39.5, 40.5, -10.8, -9.5)],
      input="A path from a shop door ends 0.6 m short of the south sidewalk.",
      result="pedestrian-pedestrian: a node on the sidewalk, which is split, and a 0.6 m connector.",
      checks=[("connector", (40, -10.3), "pedestrian-pedestrian"),
              ("node", (40, -10), {"ext:union_rule": "pedestrian-pedestrian"})]),
 dict(n=11, title="Crossing lands on a living street", at=(115, 8.2), badge=(125, 4), zoom=(115, 7.5, 6),
      look=[(114.5, 115.5, 7.5, 9), (114.5, 115.5, -0.5, 0.5), (114.5, 115.5, -10.5, -9.5)],
      input="The Willow Lane crossing ends 0.5 m short of the living street, and crosses Harbor Blvd.",
      result="crossing-living-street joins it to Willow Lane's end with a connector; road-crossing "
             "joins it to the road; its south kerb splits the sidewalk.",
      checks=[("connector", (115, 8.25), "crossing-living-street"),
              ("node", (115, 0), {"ext:union_rule": "road-crossing"}),
              ("node", (115, -10), {"barrier": "kerb"})]),
 dict(n=12, title="Not joined: no rule asks", at=(70, -10), badge=(80, -14), zoom=(70, -10, 8),
      look=[(69.5, 70.5, -10.5, -9.5), (-50.5, -49.5, 9.5, 10.5)],
      input="The south sidewalk passes over Market Alley.",
      result="No default rule joins a sidewalk to a road, so neither is split there. The park path "
             "the city already joined to the north sidewalk stays as it was.",
      checks=[("no_node", (70, -10)), ("node", (-50, 10), {"ext:union_rule": ABSENT})]),
 dict(n=13, title="Bench, bin, hydrant, building", at=(102, -14.7), badge=(104, -28), zoom=(101.9, -14.8, 3),
      look=[(101, 103, -15.5, -14), (11.5, 12.5, -14.5, -13.5), (30, 46, 26, 42)],
      input="Both have the bench (0.36 m apart); the survey adds a bin, a hydrant, a storey count.",
      result="Same-type points within 1 m merge (city position, ext:condition added); the bin stays "
             "apart (other type); the hydrant is added; Harbor Hall gains building:levels.",
      checks=[("points", (102, -14.7), 1.2, 2), ("point", (102, -14.5), {"amenity": "bench", "ext:condition": "good"}),
              ("points", (12, -14), 0.5, 1), ("polygon", "Harbor Hall", {"building:levels": "2"})]),
]
