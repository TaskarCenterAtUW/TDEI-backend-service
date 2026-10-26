"""
Load the union 5.0 demo into QGIS: the Harbor Blvd × Oak St neighbourhood from
two sources, and what the union made of it under each config you ran.

In QGIS: Plugins > Python Console > Show Editor > open qgis/load_demo.py > Run.
Run ./run_demo.sh first — it writes runs/demo_<name>/ (demo/input.json ->
runs/demo_input/). Guide: docs/demo.md

  Union 5.0 demo
  ├─ Runs               one visible at a time (e.g. demo_input, demo_default_1m, demo_off)
  │  └─ demo_input
  │     ├─ Numbered spots   label + status (PASS / FAIL for the default config,
  │     │                   same as / differs from default for any other); Identify
  │     │                   shows the input, what this run did there, the default result
  │     ├─ What changed nodes snapped / created / used, input edge fate, output edge origin
  │     └─ Output       edges (connectors in purple), nodes (kerbs as squares),
  │                     buildings, points, lines
  └─ Input              DS1 City GIS (blue), DS2 Field survey (orange dashed)
"""
import json, os
from qgis.core import (QgsProject, QgsVectorLayer, QgsLineSymbol, QgsFillSymbol, QgsMarkerSymbol,
                       QgsCategorizedSymbolRenderer, QgsRendererCategory, QgsSingleSymbolRenderer,
                       QgsPalLayerSettings, QgsTextFormat, QgsTextBufferSettings,
                       QgsVectorLayerSimpleLabeling, QgsRuleBasedRenderer)
from qgis.PyQt.QtGui import QColor

# ---- set these -------------------------------------------------------------
HARNESS_DIR = r""            # harness root; empty = the folder above this script (works once the file is saved)
SHOW_FIRST = "demo_input"
# -----------------------------------------------------------------------------

def _is_harness(d):
    return bool(d) and os.path.exists(os.path.join(d, "demo", "scene.py"))
if not _is_harness(HARNESS_DIR):
    try:
        HARNESS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    except NameError:
        pass
if not _is_harness(HARNESS_DIR):
    raise SystemExit("Set HARNESS_DIR at the top of qgis/load_demo.py to the harness root, "
                     "or save the script before running it.")
DATA = os.path.join(HARNESS_DIR, "demo", "data")
RUNS_DIR = os.path.join(HARNESS_DIR, "runs")
runs = sorted(d for d in os.listdir(RUNS_DIR) if d.startswith("demo_")
              and os.path.exists(os.path.join(RUNS_DIR, d, "osw_edges.geojson"))) if os.path.isdir(RUNS_DIR) else []
if not runs:
    raise SystemExit(f"No demo runs under {RUNS_DIR}. Run ./run_demo.sh first.")
if SHOW_FIRST in runs:
    runs.remove(SHOW_FIRST); runs.insert(0, SHOW_FIRST)

project = QgsProject.instance()
root = project.layerTreeRoot()
old = root.findGroup("Union 5.0 demo")
if old: root.removeChildNode(old)
top = root.insertGroup(0, "Union 5.0 demo")

GREEN, RED, BLUE, ORANGE = "46,125,50", "198,40,40", "31,95,191", "240,140,0"
PINK, PURPLE, KERB, GOLD = "224,86,124", "106,27,154", "123,44,191", "255,179,0"

def add(group, path, name, visible=True):
    if not os.path.exists(path):
        print("  skipped (not found):", path); return None
    lyr = QgsVectorLayer(path, name, "ogr")
    if not lyr.isValid():
        print("  could not load:", path); return None
    project.addMapLayer(lyr, False)
    node = group.addLayer(lyr); node.setItemVisibilityChecked(visible); node.setExpanded(False)
    return lyr

def line(color, width="0.6", style="solid"):
    return QgsLineSymbol.createSimple({"line_color": color, "line_width": width, "line_style": style})

def marker(shape, color, size="2", outline="255,255,255", filled=True):
    return QgsMarkerSymbol.createSimple({"name": shape, "color": color if filled else "0,0,0,0",
                                         "outline_color": outline if filled else color,
                                         "outline_width": "0.4" if not filled else "0.2", "size": size})

def fill(color, alpha="60", outline="160,140,120", style="solid"):
    return QgsFillSymbol.createSimple({"color": f"{color},{alpha}", "outline_color": outline,
                                       "outline_width": "0.3", "outline_style": style})

def categorized(lyr, field, cats):
    lyr.setRenderer(QgsCategorizedSymbolRenderer(field, [QgsRendererCategory(v, s, l, on) for v, s, l, on in cats]))

def rules(lyr, items, base):
    r = QgsRuleBasedRenderer(base); rr = r.rootRule(); rr.removeChildAt(0)
    for expr, sym, name in items:
        rule = QgsRuleBasedRenderer.Rule(sym, 0, 0, "" if expr == "ELSE" else expr, name)
        if expr == "ELSE": rule.setIsElse(True)
        rr.appendChild(rule)
    lyr.setRenderer(r)

def label(lyr, field, size=9, color="black"):
    s = QgsPalLayerSettings(); s.fieldName = field; s.enabled = True
    fmt = QgsTextFormat(); fmt.setSize(size); fmt.setColor(QColor(color))
    buf = QgsTextBufferSettings(); buf.setEnabled(True); buf.setSize(1.2); fmt.setBuffer(buf)
    s.setFormat(fmt); lyr.setLabeling(QgsVectorLayerSimpleLabeling(s)); lyr.setLabelsEnabled(True)

def style_nodes(lyr, color):
    rules(lyr, [("\"barrier\" = 'kerb'", marker("square", KERB, "3", filled=False), "kerb"),
                ("ELSE", marker("circle", color, "1.6"), "node")], QgsMarkerSymbol.createSimple({}))

# ---- Runs ------------------------------------------------------------------------
rg = top.addGroup("Runs")
for r in runs:
    d = os.path.join(RUNS_DIR, r)
    g = rg.addGroup(r)
    sc = add(g, os.path.join(d, "demo_results.geojson"), "Numbered spots")
    if sc:
        ring = lambda c: marker("circle", "0,0,0,0", "6", outline=c, filled=False)
        categorized(sc, "status", [("PASS", ring(GREEN), "PASS", True), ("FAIL", ring(RED), "FAIL", True),
                                   ("same as default", ring("85,139,47"), "same as default", True),
                                   ("differs from default", ring("230,81,0"), "differs from default", True)])
        label(sc, "label", 10, "#263238")
    ch = g.addGroup("What changed")
    n = add(ch, os.path.join(d, "changes_nodes.geojson"), "nodes")
    if n:
        categorized(n, "change", [
            ("snapped", marker("diamond", GREEN, "3.2"), "snapped (other nodes joined it)", True),
            ("created", marker("star", GOLD, "4.5", outline="93,64,55"), "created by a connect rule", True),
            ("junction", marker("circle", "0,0,0,0", "4.5", outline=PURPLE, filled=False), "existing node a rule used", True),
            ("gone", marker("cross2", RED, "3", filled=False), "input node gone (joined another / removed)", True)])
    ie = add(ch, os.path.join(d, "changes_input_edges.geojson"), "input edges — fate")
    if ie:
        categorized(ie, "fate", [
            ("removed", line(RED, "1.6", "dash"), "removed (duplicate)", True),
            ("split", line(KERB, "1.2"), "split into pieces", True),
            ("modified", line(ORANGE, "1.2"), "modified (an end snapped)", True),
            ("kept", line("150,150,150", "0.4"), "kept unchanged", False)])
    oe = add(ch, os.path.join(d, "changes_output_edges.geojson"), "output edges — origin", visible=False)
    if oe:
        categorized(oe, "change", [
            ("connector", line(PURPLE, "1.2"), "connector", True), ("split piece", line(KERB, "0.8"), "piece of a split edge", True),
            ("modified", line(ORANGE, "0.8"), "reshaped edge", True), ("unchanged", line("150,150,150", "0.4"), "unchanged", False)])
    out = g.addGroup("Output")
    on = add(out, os.path.join(d, "osw_nodes.geojson"), "nodes")
    if on: style_nodes(on, "61,220,132")
    e = add(out, os.path.join(d, "osw_edges.geojson"), "edges")
    if e:
        rules(e, [("\"ext:union_rule\" IS NOT NULL", line(PURPLE, "1.0"), "connector"),
                  ("ELSE", line(PINK, "0.6"), "edge")], QgsLineSymbol.createSimple({}))
    for kind, sym in (("polygons", fill("220,210,201")), ("lines", line("90,90,90", "0.4")),
                      ("points", marker("circle", "60,60,60", "2.2")), ("zones", fill("0,0,0", "15"))):
        lyr = add(out, os.path.join(d, f"osw_{kind}.geojson"), kind)
        if lyr: lyr.setRenderer(QgsSingleSymbolRenderer(sym))
    g.setExpanded(r == runs[0])
rg.setIsMutuallyExclusive(True, 0)

# ---- Input -----------------------------------------------------------------------
ig = top.addGroup("Input")
for ds, title, col, style, width in (("ds2", "DS2 Field survey", ORANGE, "dash", "0.9"),
                                     ("ds1", "DS1 City GIS", BLUE, "solid", "1.4")):
    dg = ig.addGroup(title)
    nl = add(dg, os.path.join(DATA, f"{ds}_nodes.geojson"), "nodes", visible=False)
    if nl: style_nodes(nl, col)
    el = add(dg, os.path.join(DATA, f"{ds}_edges.geojson"), "edges")
    if el: el.setRenderer(QgsSingleSymbolRenderer(line(col, width, style)))
    for kind, sym in (("polygons", fill(col, "25", col, style)), ("lines", line(col, "0.6", style)),
                      ("points", marker("circle", col, "2.2"))):
        lyr = add(dg, os.path.join(DATA, f"{ds}_{kind}.geojson"), kind, visible=False)
        if lyr: lyr.setRenderer(QgsSingleSymbolRenderer(sym))
    dg.setExpanded(False)
ig.setItemVisibilityChecked(False)

try:
    lyr = next(l for l in project.mapLayers().values() if l.source().endswith(os.path.join("demo", "data", "ds1_edges.geojson")))
    canvas = iface.mapCanvas()
    ext = canvas.mapSettings().layerExtentToOutputExtent(lyr, lyr.extent()); ext.scale(1.05)
    canvas.setExtent(ext); canvas.refresh()
except (NameError, StopIteration):
    pass
print(f"Union 5.0 demo loaded: {', '.join(runs)} (showing {runs[0]}). Input is off: tick it to compare.")
