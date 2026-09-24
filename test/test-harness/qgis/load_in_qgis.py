"""
Load the OSW union test harness into QGIS, grouped so input and output can be
compared and runs can be flipped between.

In QGIS: Plugins > Python Console > Show Editor > open qgis/load_in_qgis.py > Run.
Run ./run_harness.sh first — it writes each run into runs/<name>/.
Full guide: docs/qgis.md

Layer tree (a group per concern; each run is self-contained):

  OSW union harness
  ├─ Runs                       only ONE run visible at a time — click another to flip
  │  ├─ prox_3 — 19/19 PASS
  │  │  ├─ Checks               case boxes + must-connect rings, green PASS / red FAIL
  │  │  ├─ What changed         input vs output, computed by geometry:
  │  │  │    nodes              merged / minted ixn / DS2 node gone
  │  │  │    input edges — fate removed / split / modified   (kept: off)
  │  │  │    output edges — origin  split piece / modified / new   (unchanged: off)
  │  │  └─ Output               the union result: edges, nodes, kerbs
  │  ├─ prox_1 — …
  │  └─ f_sidewalk — Edge filter: sidewalks only @ 3 m — 21/21 PASS   (filter scenarios)
  │     ├─ vs baseline        what the FILTER changed vs the same run without filters:
  │     │    node attributes   green = DS2 tags merged, orange = withheld (audited only)
  │     │    edges             teal = duplicate kept by the filter, red dashed = now removed
  │     └─ Checks / What changed / Output   as for any run
  ├─ Expected                   what SHOULD happen: must be removed, must connect
  └─ Input                      DS1 (blue) and DS2 (orange): edges and nodes

Comparing:
  • input vs output  — toggle the Input group on/off over a run's Output group
  • what changed     — the "What changed" group shows only differences
  • run vs run       — click another run under Runs (they are mutually exclusive)
"""
import json, os
from qgis.core import (QgsProject, QgsVectorLayer, QgsLineSymbol, QgsFillSymbol,
                       QgsMarkerSymbol, QgsCategorizedSymbolRenderer, QgsRendererCategory,
                       QgsSingleSymbolRenderer, QgsPalLayerSettings, QgsTextFormat,
                       QgsTextBufferSettings, QgsVectorLayerSimpleLabeling, QgsRuleBasedRenderer)
from qgis.PyQt.QtGui import QColor

# ---- set these -------------------------------------------------------------
HARNESS_DIR = r""        # repo root; empty = the folder above this script (works once the file is saved)
RUNS = "all"            # "all" = every folder under runs/, or a list: ["prox_3", "prox_1"]
SHOW_FIRST = "prox_3"   # the run visible when loaded (others one click away)
# -----------------------------------------------------------------------------

# QGIS runs an unsaved, edited script from a temp copy, so the script's own
# location can't always be trusted: use HARNESS_DIR if set, else <this file>/..
def _is_harness(d):
    return bool(d) and os.path.exists(os.path.join(d, "harness", "cases.py"))
if not _is_harness(HARNESS_DIR):
    try:
        HARNESS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    except NameError:
        pass
if not _is_harness(HARNESS_DIR):
    raise SystemExit("Set HARNESS_DIR at the top of qgis/load_in_qgis.py to the harness repo root "
                     "(the folder containing run_harness.sh), or save the script before running it.")

RUNS_DIR = os.path.join(HARNESS_DIR, "runs")
DATA = os.path.join(HARNESS_DIR, "data")
LAYERS = os.path.join(DATA, "expected")
available = sorted(d for d in (os.listdir(RUNS_DIR) if os.path.isdir(RUNS_DIR) else [])
                   if os.path.exists(os.path.join(RUNS_DIR, d, "osw_edges.geojson")))
runs = available if RUNS == "all" else [r for r in RUNS if r in available]
if not runs:
    raise SystemExit(f"No runs found under {RUNS_DIR}. Run ./run_harness.sh first. "
                     f"(asked for {RUNS}; available: {', '.join(available) or 'none'})")
import sys
sys.path.insert(0, os.path.join(HARNESS_DIR, "harness"))
try:
    from scenarios import SCENARIOS, get as get_scenario
except Exception:
    SCENARIOS, get_scenario = [], None
SCEN = {}
for r in runs:                                          # single or combined ("a+b") scenario runs
    try:
        SCEN[r] = get_scenario(r) if get_scenario and not r.startswith("prox_") else None
    except Exception:
        SCEN[r] = None
SCEN = {k: v for k, v in SCEN.items() if v}
# default runs first (prox_1, prox_3, ...), then scenarios in catalogue order
order = {x["name"]: i for i, x in enumerate(SCENARIOS)}
runs.sort(key=lambda r: (r in SCEN, "+" in r, order.get(r.split("+")[0], 0), r))
if SHOW_FIRST in runs:                                  # the one to show goes first
    runs.remove(SHOW_FIRST); runs.insert(0, SHOW_FIRST)

project = QgsProject.instance()
root = project.layerTreeRoot()
old = root.findGroup("OSW union harness")
if old: root.removeChildNode(old)
top = root.insertGroup(0, "OSW union harness")

GREEN, RED = "44,160,44", "214,39,40"
BLUE, ORANGE, PURPLE = "31,95,191", "240,140,0", "123,44,191"


# ---- helpers ----------------------------------------------------------------
def add(group, path, name, visible=True):
    if not os.path.exists(path):
        print("  skipped (not found):", path); return None
    lyr = QgsVectorLayer(path, name, "ogr")
    if not lyr.isValid():
        print("  could not load:", path); return None
    project.addMapLayer(lyr, False)
    node = group.addLayer(lyr)                 # appended: earlier = drawn on top
    node.setItemVisibilityChecked(visible)
    node.setExpanded(False)
    return lyr

def line(color, width="0.6", style="solid"):
    return QgsLineSymbol.createSimple({"line_color": color, "line_width": width, "line_style": style})

def marker(shape, color, size="2", outline="255,255,255", fill=True):
    return QgsMarkerSymbol.createSimple({"name": shape, "color": color if fill else "0,0,0,0",
                                         "outline_color": outline if fill else color,
                                         "outline_width": "0.4" if not fill else "0.2", "size": size})

def categorized(lyr, field, cats):
    """cats: [(value, symbol, legend label, shown?)]"""
    lyr.setRenderer(QgsCategorizedSymbolRenderer(
        field, [QgsRendererCategory(v, s, l, on) for v, s, l, on in cats]))

def label(lyr, field, size=9, color="black"):
    s = QgsPalLayerSettings(); s.fieldName = field; s.enabled = True
    fmt = QgsTextFormat(); fmt.setSize(size); fmt.setColor(QColor(color))
    buf = QgsTextBufferSettings(); buf.setEnabled(True); buf.setSize(1.2); fmt.setBuffer(buf)
    s.setFormat(fmt)
    lyr.setLabeling(QgsVectorLayerSimpleLabeling(s)); lyr.setLabelsEnabled(True)

def style_nodes(lyr, base_color):
    r = QgsRuleBasedRenderer(QgsMarkerSymbol.createSimple({}))
    rules = r.rootRule(); rules.removeChildAt(0)
    for expr, sym, name in [("\"barrier\" = 'kerb'", marker("square", PURPLE, "3", fill=False), "kerb"),
                            ("ELSE", marker("circle", base_color, "1.6"), "node")]:
        rule = QgsRuleBasedRenderer.Rule(sym, 0, 0, "" if expr == "ELSE" else expr, name)
        if expr == "ELSE": rule.setIsElse(True)
        rules.appendChild(rule)
    lyr.setRenderer(r)

def run_summary(run_dir):
    p = os.path.join(run_dir, "case_results.geojson")
    if not os.path.exists(p): return "not checked"
    st = [f["properties"]["status"] for f in json.load(open(p))["features"]]
    return f"{st.count('PASS')}/{len(st)} PASS"


# ---- Runs (mutually exclusive: one visible at a time) -------------------------
runs_group = top.addGroup("Runs")
for r in runs:
    d = os.path.join(RUNS_DIR, r)
    sc = SCEN.get(r)
    title = f"{r} — {sc['title']} @ {sc['proximity']:g} m" if sc else f"{r} — default, no filters"
    g = runs_group.addGroup(f"{title} — {run_summary(d)}")
    print(f"run {r}: {d}")

    # Checks
    chk = g.addGroup("Checks")
    j = add(chk, os.path.join(d, "junction_results.geojson"), "must-connect rings")
    if j:
        ring = lambda c: QgsMarkerSymbol.createSimple({"name": "circle", "color": "0,0,0,0",
                                                       "outline_color": c, "outline_width": "0.8", "size": "5"})
        categorized(j, "status", [("PASS", ring(GREEN), "PASS", True), ("FAIL", ring(RED), "FAIL", True)])
        label(j, "label", 8)
    c = add(chk, os.path.join(d, "case_results.geojson"), "case boxes")
    if c:
        box = lambda col: QgsFillSymbol.createSimple({"color": col + ",25", "outline_color": col,
                                                      "outline_width": "0.6"})
        categorized(c, "status", [("PASS", box(GREEN), "PASS", True), ("FAIL", box(RED), "FAIL", True)])
        label(c, "label", 11, "#222222")

    # vs baseline (filter scenarios only): what the filter changed compared with no filter
    if sc:
        vb = g.addGroup(f"vs baseline (prox_{sc['proximity']:g}, no filters)")
        vn = add(vb, os.path.join(d, "vs_baseline_nodes.geojson"), "node attributes: merged / withheld")
        if vn:
            r_ = QgsRuleBasedRenderer(QgsMarkerSymbol.createSimple({}))
            root_rule = r_.rootRule(); root_rule.removeChildAt(0)
            for expr, sym, name in [
                ("\"status\" = 'FAIL'", marker("diamond", RED, "4.5"), "FAIL — not as the filter requires"),
                ("\"merged\" <> ''", marker("diamond", GREEN, "3.5"), "DS2 tags merged"),
                ("ELSE", marker("diamond", ORANGE, "3.5", fill=False), "DS2 tags withheld (audited only)")]:
                rule = QgsRuleBasedRenderer.Rule(sym, 0, 0, "" if expr == "ELSE" else expr, name)
                if expr == "ELSE": rule.setIsElse(True)
                root_rule.appendChild(rule)
            vn.setRenderer(r_); label(vn, "label", 8, "#333333")
        ve = add(vb, os.path.join(d, "vs_baseline_edges.geojson"), "edges whose fate changed")
        if ve:
            r_ = QgsRuleBasedRenderer(QgsLineSymbol.createSimple({}))
            root_rule = r_.rootRule(); root_rule.removeChildAt(0)
            for expr, sym, name in [
                ("\"status\" = 'FAIL'", line(RED, "2.4"), "FAIL — not as the filter requires"),
                ("\"fate\" = 'removed'", line(RED, "1.6", "dash"), "now removed (baseline kept it)"),
                ("ELSE", line("0,170,170", "1.8"), "duplicate kept by the filter (baseline removed it)")]:
                rule = QgsRuleBasedRenderer.Rule(sym, 0, 0, "" if expr == "ELSE" else expr, name)
                if expr == "ELSE": rule.setIsElse(True)
                root_rule.appendChild(rule)
            ve.setRenderer(r_); label(ve, "label", 8, "#006666")

    # What changed
    ch = g.addGroup("What changed")
    n = add(ch, os.path.join(d, "changes_nodes.geojson"), "nodes")
    if n:
        categorized(n, "change", [
            ("merged", marker("diamond", GREEN, "3.2"), "merged (DS2 node joined a DS1 node)", True),
            ("minted", marker("star", BLUE, "4.5"), "minted (new ixn intersection node)", True),
            ("gone", marker("cross2", RED, "3", fill=False), "DS2 node gone (snapped or dropped)", True)])
    ie = add(ch, os.path.join(d, "changes_input_edges.geojson"), "input edges — fate")
    if ie:
        categorized(ie, "fate", [
            ("removed", line(RED, "1.6", "dash"), "removed (dropped as duplicate)", True),
            ("split", line(PURPLE, "1.2"), "split into pieces", True),
            ("modified", line(ORANGE, "1.2"), "modified (endpoints snapped)", True),
            ("kept", line("150,150,150", "0.4"), "kept unchanged", False)])
    oe = add(ch, os.path.join(d, "changes_output_edges.geojson"), "output edges — origin", visible=False)
    if oe:
        categorized(oe, "change", [
            ("split piece", line(PURPLE, "0.8"), "piece of a split input edge", True),
            ("modified", line(ORANGE, "0.8"), "reshaped input edge", True),
            ("new", line("0,170,170", "1.0"), "new (matches no input)", True),
            ("unchanged", line("150,150,150", "0.4"), "unchanged", False)])

    # Output
    out = g.addGroup("Output")
    on = add(out, os.path.join(d, "osw_nodes.geojson"), "nodes")
    if on: style_nodes(on, "90,90,90")
    oe2 = add(out, os.path.join(d, "osw_edges.geojson"), "edges")
    if oe2: oe2.setRenderer(QgsSingleSymbolRenderer(line("0,0,0", "0.35")))

    g.setExpanded(r == runs[0])
runs_group.setIsMutuallyExclusive(True, 0)          # first run visible, flip by clicking another

# ---- Expected (reference, same for every run) ---------------------------------
exp = top.addGroup("Expected")
ej = add(exp, os.path.join(LAYERS, "expected_junctions.geojson"), "must connect here", visible=False)
if ej:
    ej.setRenderer(QgsSingleSymbolRenderer(QgsMarkerSymbol.createSimple(
        {"name": "circle", "color": "0,0,0,0", "outline_color": GREEN, "outline_width": "0.5", "size": "6"})))
er = add(exp, os.path.join(LAYERS, "expected_removed.geojson"), "must be removed (DS2 duplicates)")
if er: er.setRenderer(QgsSingleSymbolRenderer(line("214,39,40,90", "2.4")))
exp.setExpanded(False)

# ---- Input ---------------------------------------------------------------------
inp = top.addGroup("Input")
for ds, col, style, width in (("DS2", ORANGE, "dash", "0.9"), ("DS1", BLUE, "solid", "1.4")):
    dg = inp.addGroup(f"{ds} ({'authoritative' if ds == 'DS1' else 'stitched in'})")
    nl = add(dg, os.path.join(DATA, f"{ds.lower()}_nodes.geojson"), f"{ds} nodes", visible=False)
    if nl: style_nodes(nl, col)
    el = add(dg, os.path.join(DATA, f"{ds.lower()}_edges.geojson"), f"{ds} edges")
    if el: el.setRenderer(QgsSingleSymbolRenderer(line(col, width, style)))
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
print(f"OSW union harness loaded: runs {', '.join(runs)} (showing {runs[0]}).")
