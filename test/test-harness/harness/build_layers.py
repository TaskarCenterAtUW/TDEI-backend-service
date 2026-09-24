"""
Build the QGIS reference layers from the case catalogue.

    python3 harness/build_layers.py

Writes to data/expected/:
  cases.geojson               one labelled rectangle per case (id, title, setup, expected)
  expected_junctions.geojson  points where the union MUST produce a connecting node
  expected_removed.geojson    DS2 edges the union MUST remove as duplicates
"""
import os
from cases import CASES, C
from common import inputs, case_area, case_edges, write, LAYERS

inp = inputs()
cases, junctions, removed = [], [], []
for c in CASES:
    cases.append({"type": "Feature",
                  "geometry": {"type": "Polygon", "coordinates": [case_area(inp, c["id"])]},
                  "properties": {"case": c["id"], "title": c["title"],
                                 "label": f'{c["id"]} - {c["title"]}',
                                 "setup": c["setup"], "expected": c["expected"]}})
    for a in c["asserts"]:
        applies = "all"
        if a[0] == "at":                   # proximity-specific: unwrap and note it
            applies, a = f"{a[1]:g} m only", a[2]
        if a[0] == "junction":
            (x, y), deg, names = a[1], a[2], a[3]
            junctions.append({"type": "Feature",
                              "geometry": {"type": "Point", "coordinates": list(C(x, y))},
                              "properties": {"case": c["id"], "min_degree": deg, "applies_at": applies,
                                             "must_join": " + ".join(n.split(": ", 1)[1] for n in names),
                                             "label": f'{c["id"]}: joins {deg}+ edges' + ("" if applies == "all" else f" ({applies})")}})
        # DS2 edges that must disappear: a name expected 0 times, or a name
        # expected once that exists in BOTH datasets (the DS2 copy is the duplicate).
        if a[0] == "count" and not isinstance(a[2], dict) and a[2] in (0, 1):
            named = [(ds, f) for ds, f in case_edges(inp, c["id"]) if f["properties"]["name"] == a[1]]
            in_both = {"ds1", "ds2"} <= {ds for ds, _ in named}
            if a[2] == 1 and not in_both:
                continue
            for ds, f in named:
                if ds == "ds2":
                    removed.append({"type": "Feature", "geometry": f["geometry"],
                                    "properties": {"case": c["id"], "name": a[1],
                                                   "label": f'{c["id"]}: DS2 copy must be REMOVED'}})
write(os.path.join(LAYERS, "cases.geojson"), cases)
write(os.path.join(LAYERS, "expected_junctions.geojson"), junctions)
write(os.path.join(LAYERS, "expected_removed.geojson"), removed)
print(f"data/expected/cases.geojson               {len(cases)} cases")
print(f"data/expected/expected_junctions.geojson  {len(junctions)} junctions that must connect")
print(f"data/expected/expected_removed.geojson    {len(removed)} duplicates that must be removed")
