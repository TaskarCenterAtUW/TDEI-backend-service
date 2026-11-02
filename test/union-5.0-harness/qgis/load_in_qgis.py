"""
Load the union 5.0 test harness into QGIS, grouped so input and output can be
compared and profile runs can be flipped between.

In QGIS: Plugins > Python Console > Show Editor > open qgis/load_in_qgis.py > Run.
Run ./run_harness.sh first — it writes each profile's run into runs/<profile>/.
Full guide: docs/qgis.md

Layer tree (a group per concern; each run is self-contained):

  Union 5.0 harness
  ├─ Runs                       only ONE run visible at a time — click another to flip
  │  ├─ default_3m — Every stage on its published default rule set, 3 m — 56/56 PASS
  │  │  ├─ Checks               case boxes + must-connect rings, green PASS / red FAIL
  │  │  ├─ Expected             what THIS profile must do: must connect / must be removed
  │  │  ├─ What changed         input vs output, computed by geometry:
  │  │  │    nodes              snapped / created / junction / input node gone
  │  │  │    input edges — fate removed / split / modified   (kept: off)
  │  │  │    output edges — origin  connector / split piece / modified   (unchanged: off)
  │  │  └─ Output               the union result: edges (connectors in magenta), nodes,
  │  │                          zones, extension points / lines / polygons
  │  ├─ default_1m — …
  │  └─ custom_connect — …
  ├─ Cases                      every case's box and title (all profiles)
  └─ Input                      DS1 (blue) and DS2 (orange)

Comparing:
  • input vs output  — toggle the Input group on/off over a run's Output group
  • what changed     — the "What changed" group shows only differences
  • profile vs profile — click another run under Runs (they are mutually exclusive)
"""
import json, os, sys
from qgis.core import (QgsProject, QgsVectorLayer, QgsLineSymbol, QgsFillSymbol,
                       QgsMarkerSymbol, QgsCategorizedSymbolRenderer, QgsRendererCategory,
                       QgsSingleSymbolRenderer, QgsPalLayerSettings, QgsTextFormat,
                       QgsTextBufferSettings, QgsVectorLayerSimpleLabeling, QgsRuleBasedRenderer)
from qgis.PyQt.QtGui import QColor

# ---- set these -------------------------------------------------------------
HARNESS_DIR = r""          # harness root; empty = the folder above this script (works once the file is saved)
RUNS = "all"               # "all" = every folder under runs/, or a list: ["default_3m", "custom_connect"]
SHOW_FIRST = "default_1m"  # the run visible when loaded (others one click away)
# -----------------------------------------------------------------------------

def _is_harness(d):
    return bool(d) and os.path.exists(os.path.join(d, "harness", "profiles.py"))
if not _is_harness(HARNESS_DIR):
    try:
        HARNESS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    except NameError:
        pass
if not _is_harness(HARNESS_DIR):
    raise SystemExit("Set HARNESS_DIR at the top of qgis/load_in_qgis.py to the harness root "
                     "(the folder containing run_harness.sh), or save the script before running it.")

RUNS_DIR = os.path.join(HARNESS_DIR, "runs")
DATA = os.path.join(HARNESS_DIR, "data")
LAYERS = os.path.join(DATA, "expected")
sys.path.insert(0, os.path.join(HARNESS_DIR, "harness"))
import profiles
available = [p for p in profiles.PROFILES
             if os.path.exists(os.path.join(RUNS_DIR, p, "osw_edges.geojson"))]
runs = available if RUNS == "all" else [r for r in RUNS if r in available]
if not runs:
    raise SystemExit(f"No runs found under {RUNS_DIR}. Run ./run_harness.sh first. "
                     f"(asked for {RUNS}; available: {', '.join(available) or 'none'})")
if SHOW_FIRST in runs:
    runs.remove(SHOW_FIRST); runs.insert(0, SHOW_FIRST)

project = QgsProject.instance()
root = project.layerTreeRoot()
old = root.findGroup("Union 5.0 harness")
if old: root.removeChildNode(old)
top = root.insertGroup(0, "Union 5.0 harness")

GREEN, RED = "44,160,44", "214,39,40"
BLUE, ORANGE, PURPLE, MAGENTA = "31,95,191", "240,140,0", "123,44,191", "208,24,156"


# ---- helpers ----------------------------------------------------------------
def add(group, path, name, visible=True, subset=None):
    if not os.path.exists(path):
        print("  skipped (not found):", path); return None
    lyr = QgsVectorLayer(path, name, "ogr")
    if not lyr.isValid():
        print("  could not load:", path); return None
    if subset: lyr.setSubsetString(subset)
    project.addMapLayer(lyr, False)
    node = group.addLayer(lyr)                 # appended: earlier = drawn on top
    node.setItemVisibilityChecked(visible)
    node.setExpanded(False)
    return lyr

def line(color, width="0.6", style="solid"):
    return QgsLineSymbol.createSimple({"line_color": color, "line_width": width, "line_style": style})

def fill(color, alpha="30", outline_style="solid"):
    return QgsFillSymbol.createSimple({"color": f"{color},{alpha}", "outline_color": color,
                                       "outline_width": "0.5", "outline_style": outline_style})

def marker(shape, color, size="2", outline="255,255,255", filled=True):
    return QgsMarkerSymbol.createSimple({"name": shape, "color": color if filled else "0,0,0,0",
                                         "outline_color": outline if filled else color,
                                         "outline_width": "0.4" if not filled else "0.2", "size": size})

def categorized(lyr, field, cats):
    """cats: [(value, symbol, legend label, shown?)]"""
    lyr.setRenderer(QgsCategorizedSymbolRenderer(
        field, [QgsRendererCategory(v, s, l, on) for v, s, l, on in cats]))

def rules(lyr, items, base):
    """items: [(expression or 'ELSE', symbol, legend label)]"""
    r = QgsRuleBasedRenderer(base)
    rr = r.rootRule(); rr.removeChildAt(0)
    for expr, sym, name in items:
        rule = QgsRuleBasedRenderer.Rule(sym, 0, 0, "" if expr == "ELSE" else expr, name)
        if expr == "ELSE": rule.setIsElse(True)
        rr.appendChild(rule)
    lyr.setRenderer(r)

def label(lyr, field, size=9, color="black"):
    s = QgsPalLayerSettings(); s.fieldName = field; s.enabled = True
    fmt = QgsTextFormat(); fmt.setSize(size); fmt.setColor(QColor(color))
    buf = QgsTextBufferSettings(); buf.setEnabled(True); buf.setSize(1.2); fmt.setBuffer(buf)
    s.setFormat(fmt)
    lyr.setLabeling(QgsVectorLayerSimpleLabeling(s)); lyr.setLabelsEnabled(True)

def style_nodes(lyr, base_color):
    rules(lyr, [("\"barrier\" = 'kerb'", marker("square", PURPLE, "3", filled=False), "kerb"),
                ("ELSE", marker("circle", base_color, "1.6"), "node")], QgsMarkerSymbol.createSimple({}))

def run_summary(run_dir):
    p = os.path.join(run_dir, "case_results.geojson")
    if not os.path.exists(p): return "not checked"
    st = [f["properties"]["status"] for f in json.load(open(p))["features"]]
    return f"{st.count('PASS')}/{len(st)} PASS"


# ---- Runs (mutually exclusive: one visible at a time) -------------------------
runs_group = top.addGroup("Runs")
for r in runs:
    d = os.path.join(RUNS_DIR, r)
    g = runs_group.addGroup(f"{r} — {profiles.PROFILES[r]['title']} — {run_summary(d)}")
    print(f"run {r}: {d}")

    chk = g.addGroup("Checks")
    j = add(chk, os.path.join(d, "junction_results.geojson"), "must-connect rings")
    if j:
        ring = lambda c: QgsMarkerSymbol.createSimple({"name": "circle", "color": "0,0,0,0",
                                                       "outline_color": c, "outline_width": "0.8", "size": "5"})
        categorized(j, "status", [("PASS", ring(GREEN), "PASS", True), ("FAIL", ring(RED), "FAIL", True)])
        label(j, "label", 8)
    c = add(chk, os.path.join(d, "case_results.geojson"), "case boxes")
    if c:
        box = lambda col: QgsFillSymbol.createSimple({"color": col + ",25", "outline_color": col, "outline_width": "0.6"})
        categorized(c, "status", [("PASS", box(GREEN), "PASS", True), ("FAIL", box(RED), "FAIL", True)])
        label(c, "label", 11, "#222222")

    exp = g.addGroup("Expected (this profile)")
    sub = f"\"profile\" = '{r}'"
    ej = add(exp, os.path.join(LAYERS, "expected_junctions.geojson"), "must connect here", visible=False, subset=sub)
    if ej:
        ej.setRenderer(QgsSingleSymbolRenderer(QgsMarkerSymbol.createSimple(
            {"name": "circle", "color": "0,0,0,0", "outline_color": GREEN, "outline_width": "0.5", "size": "6"})))
    er = add(exp, os.path.join(LAYERS, "expected_removed.geojson"), "must be removed (duplicates)", visible=False, subset=sub)
    if er: er.setRenderer(QgsSingleSymbolRenderer(line("214,39,40,90", "2.4")))
    exp.setExpanded(False)

    ch = g.addGroup("What changed")
    n = add(ch, os.path.join(d, "changes_nodes.geojson"), "nodes")
    if n:
        categorized(n, "change", [
            ("snapped", marker("diamond", GREEN, "3.2"), "snapped (other nodes joined it)", True),
            ("created", marker("star", MAGENTA, "4.5"), "created by a connect rule", True),
            ("junction", marker("star", BLUE, "4"), "existing node used as a junction", True),
            ("gone", marker("cross2", RED, "3", filled=False), "input node gone (joined another / removed)", True)])
    ie = add(ch, os.path.join(d, "changes_input_edges.geojson"), "input edges — fate")
    if ie:
        categorized(ie, "fate", [
            ("removed", line(RED, "1.6", "dash"), "removed (duplicate)", True),
            ("split", line(PURPLE, "1.2"), "split into pieces", True),
            ("modified", line(ORANGE, "1.2"), "modified (an end snapped)", True),
            ("kept", line("150,150,150", "0.4"), "kept unchanged", False)])
    oe = add(ch, os.path.join(d, "changes_output_edges.geojson"), "output edges — origin", visible=False)
    if oe:
        categorized(oe, "change", [
            ("connector", line(MAGENTA, "1.2"), "connector (made by a connect rule)", True),
            ("split piece", line(PURPLE, "0.8"), "piece of a split input edge", True),
            ("modified", line(ORANGE, "0.8"), "reshaped input edge", True),
            ("new", line("0,170,170", "1.0"), "new (matches no input)", True),
            ("unchanged", line("150,150,150", "0.4"), "unchanged", False)])

    out = g.addGroup("Output")
    on = add(out, os.path.join(d, "osw_nodes.geojson"), "nodes")
    if on: style_nodes(on, "90,90,90")
    oe2 = add(out, os.path.join(d, "osw_edges.geojson"), "edges")
    if oe2:
        rules(oe2, [("\"ext:union_rule\" IS NOT NULL", line(MAGENTA, "1.0"), "connector"),
                    ("ELSE", line("0,0,0", "0.35"), "edge")], QgsLineSymbol.createSimple({}))
    for kind, sym in (("zones", fill("0,0,0", "15")), ("polygons", fill("90,90,90", "20")),
                      ("lines", line("90,90,90", "0.5")), ("points", marker("circle", "60,60,60", "2.2"))):
        lyr = add(out, os.path.join(d, f"osw_{kind}.geojson"), kind)
        if lyr: lyr.setRenderer(QgsSingleSymbolRenderer(sym))
    g.setExpanded(r == runs[0])
runs_group.setIsMutuallyExclusive(True, 0)          # first run visible, flip by clicking another

# ---- Cases (every case, every profile) -----------------------------------------
cg = top.addGroup("Cases")
cl = add(cg, os.path.join(LAYERS, "cases.geojson"), "case boxes (all profiles)", visible=False)
if cl:
    cl.setRenderer(QgsSingleSymbolRenderer(QgsFillSymbol.createSimple(
        {"color": "0,0,0,0", "outline_color": "120,120,120", "outline_width": "0.4"})))
    label(cl, "label", 9, "#444444")
cg.setExpanded(False)

# ---- Input ---------------------------------------------------------------------
inp = top.addGroup("Input")
for ds, col, style, width in (("DS2", ORANGE, "dash", "0.9"), ("DS1", BLUE, "solid", "1.4")):
    dg = inp.addGroup(ds)
    nl = add(dg, os.path.join(DATA, f"{ds.lower()}_nodes.geojson"), f"{ds} nodes", visible=False)
    if nl: style_nodes(nl, col)
    el = add(dg, os.path.join(DATA, f"{ds.lower()}_edges.geojson"), f"{ds} edges")
    if el: el.setRenderer(QgsSingleSymbolRenderer(line(col, width, style)))
    for kind, sym in (("zones", fill(col, "15", style)), ("polygons", fill(col, "20", style)),
                      ("lines", line(col, "0.6", style)), ("points", marker("circle", col, "2.2"))):
        lyr = add(dg, os.path.join(DATA, f"{ds.lower()}_{kind}.geojson"), f"{ds} {kind}", visible=False)
        if lyr: lyr.setRenderer(QgsSingleSymbolRenderer(sym))
    dg.setExpanded(False)

# zoom to the test grid
try:
    ext = None
    for lyr in project.mapLayers().values():
        if lyr.name() == "DS1 edges":
            ext = lyr.extent(); break
    if ext:
        canvas = iface.mapCanvas()
        ext = canvas.mapSettings().layerExtentToOutputExtent(lyr, ext)   # layer CRS -> map CRS
        ext.scale(1.1); canvas.setExtent(ext); canvas.refresh()
except NameError:
    pass
print(f"Union 5.0 harness loaded: runs {', '.join(runs)} (showing {runs[0]}).")
