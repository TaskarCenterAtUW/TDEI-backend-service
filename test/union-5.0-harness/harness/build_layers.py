"""
Build the QGIS reference layers from the case catalogue.

    python3 harness/build_layers.py

Writes to data/expected/ (one feature per case and profile it is checked under;
filter on "profile" in QGIS — qgis/load_in_qgis.py does this for each run):
  cases.geojson               one labelled rectangle per case (id, title, setup, expected, profiles)
  expected_junctions.geojson  points where a profile's run MUST produce a connecting node
  expected_removed.geojson    edges a profile's run MUST remove as duplicates
"""
import os
from cases import CASES, C, value_for, asserts_for
from common import inputs, case_area, case_edges, write, LAYERS
import profiles

inp = inputs()
cases, junctions, removed = [], [], []
for c in CASES:
    profs = [p for p in profiles.PROFILES if asserts_for(c, p)]
    cases.append({"type": "Feature",
                  "geometry": {"type": "Polygon", "coordinates": [case_area(inp, c["id"])]},
                  "properties": {"case": c["id"], "title": c["title"], "profiles": ", ".join(profs),
                                 "label": f'{c["id"]} - {c["title"]}',
                                 "setup": c["setup"], "expected": c["expected"]}})
    for prof in profs:
        px = profiles.PROFILES[prof]["proximity"]
        for a in asserts_for(c, prof):
            if a[0] == "at":                       # proximity-specific: only when it applies
                if abs(a[1] - px) > 1e-9: continue
                a = a[2]
            if a[0] == "junction":
                (x, y), deg, names = a[1], value_for(a[2], px), a[3]
                junctions.append({"type": "Feature",
                                  "geometry": {"type": "Point", "coordinates": list(C(x, y))},
                                  "properties": {"case": c["id"], "profile": prof, "min_degree": deg,
                                                 "must_join": " + ".join(n.split(": ", 1)[1] for n in names),
                                                 "label": f'{c["id"]}: joins {deg}+ edges'}})
            # edges that must disappear: a name expected 0 times, or a name expected
            # once that exists in BOTH datasets (the DS2 copy is the duplicate)
            if a[0] == "count":
                want = value_for(a[2], px)
                if want not in (0, 1): continue
                named = [(ds, f) for ds, f in case_edges(inp, c["id"]) if f["properties"].get("name") == a[1]]
                in_both = {"ds1", "ds2"} <= {ds for ds, _ in named}
                if want == 1 and not in_both: continue
                for ds, f in named:
                    if want == 0 or ds == "ds2":
                        removed.append({"type": "Feature", "geometry": f["geometry"],
                                        "properties": {"case": c["id"], "profile": prof, "name": a[1],
                                                       "dataset": ds.upper(),
                                                       "label": f'{c["id"]}: {ds.upper()} copy must be REMOVED'}})
write(os.path.join(LAYERS, "cases.geojson"), cases)
write(os.path.join(LAYERS, "expected_junctions.geojson"), junctions)
write(os.path.join(LAYERS, "expected_removed.geojson"), removed)
print(f"data/expected/cases.geojson               {len(cases)} cases")
print(f"data/expected/expected_junctions.geojson  {len(junctions)} junctions that must connect (all profiles)")
print(f"data/expected/expected_removed.geojson    {len(removed)} duplicates that must be removed (all profiles)")
