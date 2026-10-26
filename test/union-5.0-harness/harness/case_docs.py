"""
Write docs/test-dataset.md from the case catalogue, so the case tables never
drift from harness/cases.py.

    python3 harness/case_docs.py
"""
import os, re
from cases import CASES
from common import ROOT
import profiles

SECTIONS = [
    ("Network cases (A–W)", r"^[A-W]$",
     "Carried over from 4.x. Their expectations are now those of the `default` rule sets."),
    ("Crossing ↔ sidewalk (X)", r"^X\d+$",
     "\"Sidewalk\" means `highway=footway` + `footway=sidewalk`. Every sidewalk is drawn north–south, "
     "so a crossing end with the same x lies exactly on the sidewalk line."),
    ("Crossing ↔ living street (LS)", r"^LS\d+$",
     "A living street (`highway=living_street`) is a landing way for crossings, like a sidewalk, but never "
     "joins a sidewalk, footway or road."),
    ("Per-type snap distances (SN)", r"^SN\d+$", "Mostly checked under `custom_snap`."),
    ("Anchors and attributes (AN)", r"^AN\d+$", "Checked under `custom_anchor` (DS2 anchored for sidewalks)."),
    ("Ordered connect rules with tags (CC)", r"^CC\d+$",
     "Checked under `custom_connect`. The ids in titles (O1–O3, W1–W5, G1, G3) are the decided edge cases "
     "of the requirement spec; see [union-expectations.md](union-expectations.md)."),
    ("Dedup guards (DG)", r"^DG\d+$", "The stranding and connectivity guards of the dedup stage."),
    ("Zones and extension features (E)", r"^E\d+$", "The `extensions` section: `default` keeps 4.3.2's merging."),
]

def profs(c):
    out = []
    for k in c["asserts"]:
        out.append("default (1 m, 3 m)" if k == "default" else k)
    return ", ".join(f"`{p}`" if not p.startswith("default (") else p for p in out)

def esc(s): return s.replace("|", "\\|")

lines = ["# Test dataset", "",
         "Synthetic but schema-valid OSW data built to exercise `content.tdei_union_dataset` 5.0, with one small "
         "case per behaviour. **Generated**: the data by `harness/generate_dataset.py`, this page by "
         "`harness/case_docs.py` (both from `harness/cases.py`). Do not edit either by hand.", "",
         "| File | Contents |", "|---|---|",
         "| `data/ds1_*.geojson` | Dataset 1: nodes, edges, zones, extension points / lines / polygons |",
         "| `data/ds2_*.geojson` | Dataset 2: the same six files |",
         "| `data/expected/*.geojson` | reference layers built from the case catalogue, one feature per case and profile: case areas, must-connect points, must-be-removed edges |",
         "",
         "- **Location:** Albany, Oregon, around `-123.10, 44.63`; each case sits in its own area of the map.",
         "- **Finding a case:** every edge `name` starts with its case id (`\"A: duplicate sidewalk\"`); zones and "
         "extension features carry `ext:case`.",
         "- **Ids** are numeric (TDEI stores `_id` as `bigint`). DS1 starts at nodes `1000001`, edges `1500001`, "
         "zones `1700001`, points `1800001`, lines `1850001`, polygons `1900001`; DS2 the same with a leading 2.",
         "- **Profiles:** a case is checked only under the profiles listed for it "
         "([profiles.md](profiles.md)); \"default\" means both `default_1m` and `default_3m`.", "",
         "Reference sheets (inputs and what each profile expects, one panel per case): " +
         ", ".join(f"[{p}](images/reference_{p}.png)" for p in profiles.PROFILES) + ".", "",
         "![default_1m reference sheet](images/reference_default_1m.png)", ""]
seen = set()
for title, pat, intro in SECTIONS:
    rows = [c for c in CASES if re.match(pat, c["id"])]
    if not rows: continue
    lines += [f"## {title}", "", intro, "", "| Case | Setup | Expected | Checked under |", "|---|---|---|---|"]
    for c in rows:
        seen.add(c["id"])
        lines.append(f'| **{c["id"]}** {esc(c["title"])} | {esc(c["setup"])} | {esc(c["expected"])} | {profs(c)} |')
    lines.append("")
missing = [c["id"] for c in CASES if c["id"] not in seen]
if missing:
    lines += ["## Other cases", "", "| Case | Setup | Expected | Checked under |", "|---|---|---|---|"]
    for c in CASES:
        if c["id"] in missing:
            lines.append(f'| **{c["id"]}** {esc(c["title"])} | {esc(c["setup"])} | {esc(c["expected"])} | {profs(c)} |')
path = os.path.join(ROOT, "docs", "test-dataset.md")
open(path, "w").write("\n".join(lines) + "\n")
print("wrote", path, f"({len(CASES)} cases)")
