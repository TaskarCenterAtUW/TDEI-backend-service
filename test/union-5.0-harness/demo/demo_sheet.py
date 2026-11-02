"""
The demo picture: what one config did to the whole neighbourhood, in one image.

    python3 demo/demo_sheet.py runs/demo_input          (run_demo.sh runs this for you)

Top: the applied configuration, stage by stage, with what every rule did in
this run. Then the scene as the two sources describe it (left) and as the
union outputs it (right), drawn like a street map, with numbered badges.
Below: a close-up of every numbered spot (input faint underneath, output on
top) saying what the sources hold, what this run did there, and what the
default rules do: PASS / FAIL for a default run, "= default" / "≠ default"
for any other config. Run demo/check_demo.py first (it writes the statuses).
Writes <run>/demo.png.
"""
import argparse, json, os, sys, textwrap
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon, Circle, Patch
from matplotlib.lines import Line2D

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import scene, describe
from scene import M

ap = argparse.ArgumentParser()
ap.add_argument("run_dir")
ap.add_argument("--data-dir", default=os.path.join(HERE, "data"))
ap.add_argument("--out", help="image path (default <run>/demo.png)")
a = ap.parse_args()

run = describe.Run(a.run_dir, a.data_dir)
inp, out, report = run.inp, run.out, run.report
removed = [f for f in run.ch_in if f["properties"]["fate"] == "removed"]
status = {f["properties"]["n"]: f["properties"]
          for f in describe._load(os.path.join(a.run_dir, "demo_results.geojson"))}
rc_path = os.path.join(a.run_dir, "run_config.json")
run_config = json.load(open(rc_path)) if os.path.exists(rc_path) else {}
is_default = bool(run.effective) and run.effective == run_config.get("default_1m_effective")
in_coords = {tuple(n["geometry"]["coordinates"]) for ds in inp for n in inp[ds]["nodes"]}

# ── colours ──
BLOCK, ROADC, LIVINGC, WALKC = "#e4e2df", "#ffffff", "#f4eee2", "#eeece8"
BLDG, BLDG_EDGE, LABEL = "#dcd2c9", "#c3b5a8", "#8d8279"
ZONEC, ZONE_EDGE = "#efe7d3", "#a1887f"
DS1C, DS2C = "#1f5fbf", "#f08c00"
OUTC, CONNC, REMC = "#e0567c", "#6a1b9a", "#d62728"
NODEC, KERBC, NEWC, SNAPC = "#3ddc84", "#7b2cbf", "#ffb300", "#1b5e20"

def xy(coords): return [M(*c) for c in coords]

def strip(p, q, w):
    """A rectangle of half-width w along segment p-q (a road or sidewalk surface)."""
    (x0, y0), (x1, y1) = p, q
    L = ((x1 - x0) ** 2 + (y1 - y0) ** 2) ** 0.5
    nx, ny = -(y1 - y0) / L * w, (x1 - x0) / L * w
    return [(x0 + nx, y0 + ny), (x1 + nx, y1 + ny), (x1 - nx, y1 - ny), (x0 - nx, y0 - ny)]

def basemap(ax, polys, poly_style):
    ax.set_facecolor(BLOCK)
    for name, tags, pts in scene.EDGES["ds1"]:
        if tags.get("footway") == "sidewalk":
            for p, q in zip(pts, pts[1:]):
                ax.add_patch(Polygon(strip(p, q, 1.2), closed=True, fc=WALKC, ec="none", zorder=0.5))
    for name, tags, pts in scene.EDGES["ds1"]:
        w = scene.ROAD_WIDTH.get(tags.get("highway"))
        if w:
            for p, q in zip(pts, pts[1:]):
                ax.add_patch(Polygon(strip(p, q, w), closed=True, zorder=0.6,
                                     fc=LIVINGC if tags["highway"] == "living_street" else ROADC, ec="none"))
    for f, (fc, ec, ls, lw) in polys:
        ring = xy(f["geometry"]["coordinates"][0])
        ax.add_patch(Polygon(ring, closed=True, fc=fc, ec=ec, ls=ls, lw=lw, zorder=0.8))

def street_names(ax, size):
    kw = dict(color="#9a9a9a", fontsize=size, ha="center", va="center", zorder=0.9, fontstyle="italic")
    ax.text(-62, -3.6, "Harbor Blvd", **kw); ax.text(92, -3.6, "Harbor Blvd", **kw)
    ax.text(-2.7, 45, "Oak St", rotation=90, **kw); ax.text(-2.7, -45, "Oak St", rotation=90, **kw)
    ax.text(72.5, -32, "Market Alley", rotation=90, **kw)
    ax.text(117.3, 40, "Willow Lane (living street)", rotation=90, **kw)

def building_labels(ax, polys, size):
    for f in polys:
        name = f["properties"].get("name")
        if name:
            r = xy(f["geometry"]["coordinates"][0])
            cx, cy = sum(p[0] for p in r[:-1]) / (len(r) - 1), sum(p[1] for p in r[:-1]) / (len(r) - 1)
            ax.text(cx, cy, name, color=LABEL, fontsize=size, ha="center", va="center", zorder=0.9)

POINT_MARK = {"bench": "s", "waste_basket": "v", "fire_hydrant": "^", "street_lamp": "*"}
def point_marker(p):
    return POINT_MARK.get(p.get("amenity") or p.get("emergency") or p.get("highway"), "o")

def draw_input(ax, scale=1.0, faint=False):
    al = 0.55 if faint else 1.0
    for ds, col, ls, lw in (("ds1", DS1C, "-", 2.2), ("ds2", DS2C, (0, (3.5, 2)), 1.9)):
        for e in inp[ds]["edges"]:
            pts = xy(e["geometry"]["coordinates"])
            ax.plot(*zip(*pts), color=col, ls=ls, lw=lw * scale, alpha=al, zorder=3 if ds == "ds1" else 3.2,
                    solid_capstyle="round")
        if faint:      # close-ups: each source's buildings and zones as outlines over the output
            for f in inp[ds]["polygons"] + inp[ds]["zones"]:
                ax.add_patch(Polygon(xy(f["geometry"]["coordinates"][0]), closed=True, fc="none", ec=col,
                                     ls=ls, lw=1.1 * scale, alpha=0.9, zorder=2.4))
        else:
            for z in inp[ds]["zones"]:
                ax.add_patch(Polygon(xy(z["geometry"]["coordinates"][0]), closed=True, zorder=0.85,
                                     fc=(col if ds == "ds1" else "none"), alpha=(0.12 if ds == "ds1" else 1.0),
                                     ec=col, ls=ls, lw=1.2 * scale))
        for l in inp[ds]["lines"]:
            ax.plot(*zip(*xy(l["geometry"]["coordinates"])), color=col, ls=ls, lw=1.0 * scale, alpha=al, zorder=2)
        if faint:
            for n in inp[ds]["nodes"]:
                if n["properties"].get("barrier") == "kerb":
                    x, y = M(*n["geometry"]["coordinates"])
                    ax.plot(x, y, "s", ms=5 * scale, mfc="none", mec=col, mew=1.1, alpha=0.8, zorder=5.5)
            for p in inp[ds]["points"]:
                x, y = M(*p["geometry"]["coordinates"])
                ax.plot(x, y, point_marker(p["properties"]), ms=6 * scale, mfc="none", mec=col, mew=1.3, zorder=5.5)
            continue
        for n in inp[ds]["nodes"]:
            x, y = M(*n["geometry"]["coordinates"])
            if n["properties"].get("barrier") == "kerb":
                ax.plot(x, y, "s", ms=6.5 * scale, mfc="white", mec=KERBC, mew=1.6, zorder=6)
            else:
                ax.plot(x, y, "o", ms=3.6 * scale, mfc=col, mec="white", mew=0.5, zorder=5)
        for p in inp[ds]["points"]:
            x, y = M(*p["geometry"]["coordinates"])
            ax.plot(x, y, point_marker(p["properties"]), ms=6 * scale, mfc=col, mec="white", mew=0.6, zorder=6)

def draw_output(ax, scale=1.0):
    for e in removed:
        ax.plot(*zip(*xy(e["geometry"]["coordinates"])), color=REMC, lw=6.5 * scale, alpha=0.22,
                solid_capstyle="round", zorder=2.5)
    for e in out["edges"]:
        conn = "ext:union_rule" in e["properties"]
        ax.plot(*zip(*xy(e["geometry"]["coordinates"])), color=CONNC if conn else OUTC,
                lw=(3.2 if conn else 2.1) * scale, zorder=4 if conn else 3.5, solid_capstyle="round")
    for z in out["zones"]:
        ax.add_patch(Polygon(xy(z["geometry"]["coordinates"][0]), closed=True, fc=ZONEC, ec=ZONE_EDGE,
                             lw=1.0 * scale, hatch="..", zorder=0.85))
    for l in out["lines"]:
        ax.plot(*zip(*xy(l["geometry"]["coordinates"])), color="#6d6d6d", lw=1.0 * scale, zorder=2)
    for n in out["nodes"]:
        p = n["properties"]; x, y = M(*n["geometry"]["coordinates"])
        if p.get("barrier") == "kerb":
            ax.plot(x, y, "s", ms=6.5 * scale, mfc="white", mec=KERBC, mew=1.6, zorder=6)
        elif tuple(n["geometry"]["coordinates"]) not in in_coords:
            ax.plot(x, y, "*", ms=11 * scale, mfc=NEWC, mec="#5d4037", mew=0.6, zorder=7)
        elif "ext:union_absorbed" in p:
            ax.plot(x, y, "D", ms=6 * scale, mfc=SNAPC, mec="white", mew=0.6, zorder=7)
        else:
            ax.plot(x, y, "o", ms=4.2 * scale, mfc=NODEC, mec="#1e7a46", mew=0.6, zorder=5)
        if "ext:union_rule" in p and tuple(n["geometry"]["coordinates"]) in in_coords:
            ax.plot(x, y, "o", ms=12 * scale, mfc="none", mec=CONNC, mew=1.4, zorder=6.5)
    for p in out["points"]:
        x, y = M(*p["geometry"]["coordinates"])
        ax.plot(x, y, point_marker(p["properties"]), ms=6 * scale, mfc="#424242", mec="white", mew=0.6, zorder=6)

def badges(ax, size):
    for s in scene.SCENARIOS:
        (ax_, ay), (bx, by) = s["at"], s["badge"]
        ax.plot([ax_, bx], [ay, by], color="#37474f", lw=0.8, zorder=8)
        ax.add_patch(Circle((bx, by), size, fc="white", ec="#263238", lw=1.4, zorder=9))
        ax.text(bx, by, str(s["n"]), ha="center", va="center", fontsize=9.5, fontweight="bold", color="#263238", zorder=10)

def frame(ax, x0, x1, y0, y1):
    ax.set_xlim(x0, x1); ax.set_ylim(y0, y1); ax.set_aspect("equal")
    ax.set_xticks([]); ax.set_yticks([])
    for sp in ax.spines.values(): sp.set_edgecolor("#9e9e9e")

def scale_bar(ax, length, label, x0, x1, y0, y1, color="#424242", right=False):
    bx = x1 - (x1 - x0) * 0.04 - length if right else x0 + (x1 - x0) * 0.04
    by = y0 + (y1 - y0) * (0.03 if right else 0.05)
    ax.plot([bx, bx + length], [by, by], color=color, lw=2.5, zorder=11, solid_capstyle="butt")
    ax.text(bx + length / 2, by + (y1 - y0) * 0.025, label, ha="center", va="bottom", fontsize=8, color=color, zorder=11)

# ── layout (inches) ──
W = 26.0
side, gap = 0.3, 0.3
main_w = (W - 2 * side - gap) / 2
x0, x1, y0, y1 = scene.EXTENT
main_h = main_w * (y1 - y0) / (x1 - x0)
cols, tile, tile_gap = 7, 3.3, 0.37
tile_w = (W - 2 * side - (cols - 1) * tile_gap) / cols
PT = 1 / 72
DPI = 100
_scratch = plt.figure(figsize=(4, 4), dpi=DPI)
def measure(lines, fs, ls):
    """Height in inches of these lines as matplotlib will draw them."""
    t = _scratch.text(0, 0, "\n".join(lines), fontsize=fs, linespacing=ls)
    h = t.get_window_extent(_scratch.canvas.get_renderer()).height / DPI
    t.remove()
    return h

# the configuration band: one column per stage
band_cols = describe.config_columns(run)
band_w = W - 2 * side - 3 * gap - 0.4
col_ws = [band_w * f for f in (0.22, 0.27, 0.32, 0.19)]          # connect has the longest lines
CFG_FS, CFG_LH = 9.4, 9.4 * 1.32 * PT
cfg_text, MAX_LINES = [], 14
for (stage, head, lines), col_w in zip(band_cols, col_ws):
    wrapped = []
    for l in lines:
        wrapped += textwrap.wrap(l, int(col_w * 14.6), subsequent_indent="      ") or [""]
    if len(wrapped) > MAX_LINES:
        more = len(wrapped) - (MAX_LINES - 1)
        wrapped = wrapped[:MAX_LINES - 1] + [f"… {more} more lines in report.json"]
    cfg_text.append((stage, head, wrapped))
warnings = report.get("warnings", [])
warn_lines = [w for x in warnings for w in textwrap.wrap("⚠ " + x, int((W - 2 * side - 0.4) * 14.6))][:4]
band_h = (0.5 + 1.3 * CFG_LH + max(measure(w, CFG_FS, 1.32) for _, _, w in cfg_text)
          + (measure(warn_lines, CFG_FS, 1.32) + 0.1 if warn_lines else 0) + 0.2)

# the close-up texts, so each row is as tall as its longest text
TXT_FS = 8.4
WRAP = int(tile_w * 13.4)
STATUS_LOOK = {"PASS": ("✓ PASS", "#2e7d32"), "FAIL": ("✗ FAIL", "#c62828"),
               "same as default": ("= default", "#558b2f"), "differs from default": ("≠ default", "#e65100")}
def tile_text(s):
    st = status.get(s["n"], {})
    paras = [(textwrap.wrap("Input: " + s["input"], WRAP), "#263238"),
             (["This run:"] + [l for e in describe.spot_events(run, s["look"])
                               for l in textwrap.wrap("• " + e, WRAP, subsequent_indent="   ")], "#0d47a1"),
             (textwrap.wrap("Default rules: " + s["result"], WRAP), "#607d8b")]
    if st.get("status") == "FAIL":
        paras.append((textwrap.wrap("FAILED: " + st.get("differences", ""), WRAP), "#c62828"))
    return paras
texts = [tile_text(s) for s in scene.SCENARIOS]
PARA_GAP = 0.06
def text_h(paras): return sum(measure(p, TXT_FS, 1.27) for p, _ in paras) + (len(paras) - 1) * PARA_GAP + 0.1
n_slots = len(scene.SCENARIOS) + 1
rows = (n_slots + cols - 1) // cols
row_text = [max([text_h(texts[i]) for i in range(r * cols, min((r + 1) * cols, len(texts)))] + [1.2])
            for r in range(rows)]

band_top = 1.2
main_top = band_top + band_h + 0.5
tiles_top = main_top + main_h + 0.55
H = tiles_top + sum(0.32 + tile + t + 0.3 for t in row_text) + 0.1
plt.close(_scratch)
fig = plt.figure(figsize=(W, H))
def axes_at(left, top, w, h):                 # inches from the top-left corner
    return fig.add_axes([left / W, 1 - (top + h) / H, w / W, h / H])
def text_at(left, top, s, **kw):
    return fig.text(left / W, 1 - top / H, s, **kw)

# ── header ──
total = len(scene.SCENARIOS)
same = sum(1 for v in status.values() if v["status"] in ("PASS", "same as default"))
if is_default:
    verdict = f"{same}/{total} spots behave as described"
elif status:
    verdict = f"Same result as the default rules at {same} of {total} spots, different at {total - same}"
else:
    verdict = "run demo/check_demo.py for the spot statuses"
source = run_config.get("source", os.path.basename(os.path.normpath(a.run_dir)))
text_at(side, 0.38, "Union 5.0 demo — Harbor Blvd × Oak St, one neighbourhood from two sources",
        fontsize=22, fontweight="bold", va="center")
text_at(side, 0.85, f"DS1 = {scene.DATASETS['ds1']}, DS2 = {scene.DATASETS['ds2']}.   Config: {source}"
        + (" — the published default rules at 1 m" if is_default else "") + f".   {verdict}.",
        fontsize=13.5, va="center", color="#37474f")

# ── the applied configuration ──
bax = axes_at(side, band_top, W - 2 * side, band_h)
bax.set_xticks([]); bax.set_yticks([]); bax.set_facecolor("#f4f6f8")
for sp in bax.spines.values(): sp.set_edgecolor("#b0bec5")
text_at(side + 0.2, band_top + 0.25, "APPLIED CONFIGURATION — how the union read this config, and what each rule did "
        "in this run (report.json)", fontsize=12, fontweight="bold", va="center", color="#263238")
for i, (stage, head, wrapped) in enumerate(cfg_text):
    left = side + 0.2 + sum(col_ws[:i]) + i * gap
    off = head == "off"
    text_at(left, band_top + 0.5, f"{stage}", fontsize=10.5, fontweight="bold", va="top",
            color="#9e9e9e" if off else "#263238")
    text_at(left + 0.11 * len(stage) + 0.25, band_top + 0.5, head, fontsize=10, va="top",
            color="#9e9e9e" if off else "#455a64")
    text_at(left, band_top + 0.5 + 1.3 * CFG_LH, "\n".join(wrapped), fontsize=CFG_FS, va="top",
            color="#9e9e9e" if off else "#263238", linespacing=1.32)
if warn_lines:
    text_at(side + 0.2, band_top + band_h - 0.2 - measure(warn_lines, CFG_FS, 1.32), "\n".join(warn_lines),
            fontsize=CFG_FS, va="top", color="#e65100", linespacing=1.32)

# ── the two overviews ──
inp_polys = [(f, (BLDG, DS1C, "-", 0.9)) for f in inp["ds1"]["polygons"]] + \
            [(f, ("none", DS2C, (0, (3, 2)), 1.2)) for f in inp["ds2"]["polygons"]]
out_polys = [(f, (BLDG, BLDG_EDGE, "-", 0.7)) for f in out["polygons"]]

ax = axes_at(side, main_top, main_w, main_h)
basemap(ax, inp_polys, None); street_names(ax, 9); building_labels(ax, inp["ds1"]["polygons"], 8)
draw_input(ax); badges(ax, 4.2); frame(ax, x0, x1, y0, y1); scale_bar(ax, 20, "20 m", x0, x1, y0, y1, right=True)
ax.set_title("INPUT — what the two sources hold", fontsize=15, fontweight="bold", loc="left", pad=6)
c = report.get("counts", {}).get("input", {})
ax.text(0.99, 0.985, f"DS1 + DS2: {c.get('edges', '?')} edges, {c.get('nodes', '?')} nodes, "
        f"{c.get('points', 0) + c.get('lines', 0) + c.get('polygons', 0)} extension features",
        transform=ax.transAxes, ha="right", va="top", fontsize=10, color="#37474f",
        bbox=dict(fc="white", ec="#b0bec5", alpha=0.92, boxstyle="round,pad=0.35"), zorder=12)

ax = axes_at(side + main_w + gap, main_top, main_w, main_h)
basemap(ax, out_polys, None); street_names(ax, 9); building_labels(ax, out["polygons"], 8)
draw_output(ax); badges(ax, 4.2); frame(ax, x0, x1, y0, y1); scale_bar(ax, 20, "20 m", x0, x1, y0, y1, right=True)
ax.set_title("OUTPUT — the union with this config", fontsize=15, fontweight="bold", loc="left", pad=6)
co = report.get("counts", {})
ax.text(0.99, 0.985, f"{co.get('output', {}).get('edges', '?')} edges, {co.get('output', {}).get('nodes', '?')} nodes   ·   "
        f"{co.get('nodes_snapped', 0)} nodes snapped   ·   {co.get('edges_removed', 0)} duplicates removed   ·   "
        f"{co.get('nodes_created', 0)} junction nodes made   ·   {co.get('connectors', 0)} connectors",
        transform=ax.transAxes, ha="right", va="top", fontsize=10, color="#37474f",
        bbox=dict(fc="white", ec="#b0bec5", alpha=0.92, boxstyle="round,pad=0.35"), zorder=12)

# ── close-ups ──
def slot(i):
    r, cidx = divmod(i, cols)
    return side + cidx * (tile_w + tile_gap), tiles_top + sum(0.32 + tile + t + 0.3 for t in row_text[:r]), r

for i, s in enumerate(scene.SCENARIOS):
    left, ytop, r = slot(i)
    ax = axes_at(left, ytop + 0.32, tile_w, tile)
    cx, cy, size = s["zoom"]
    hx, hy = size / 2 * tile_w / tile, size / 2
    basemap(ax, [(f, (BLDG, BLDG_EDGE, "-", 0.7)) for f in out["polygons"]], None)
    draw_input(ax, scale=1.7, faint=True)
    draw_output(ax, scale=1.7)
    frame(ax, cx - hx, cx + hx, cy - hy, cy + hy)
    bar = 1 if size <= 5 else 2 if size <= 8 else 5 if size <= 30 else 10
    scale_bar(ax, bar, f"{bar} m", cx - hx, cx + hx, cy - hy, cy + hy)
    st = status.get(s["n"], {}).get("status")
    word, col = STATUS_LOOK.get(st, ("", "#757575"))
    for sp in ax.spines.values(): sp.set_edgecolor(col); sp.set_linewidth(2.2 if st else 1)
    text_at(left, ytop + 0.14, f"{s['n']}", fontsize=13, fontweight="bold", va="center",
            color="white", bbox=dict(boxstyle="circle,pad=0.25", fc="#263238", ec="none"))
    text_at(left + 0.36, ytop + 0.14, s["title"], fontsize=11.3, fontweight="bold", va="center")
    if word:
        ax.text(0.97, 0.96, word, transform=ax.transAxes, ha="right", va="top", fontsize=10, fontweight="bold",
                color=col, zorder=12, bbox=dict(fc="white", ec=col, boxstyle="round,pad=0.25"))
    y = ytop + 0.32 + tile + 0.1
    for lines, colr in texts[i]:
        text_at(left, y, "\n".join(lines), fontsize=TXT_FS, va="top", color=colr, linespacing=1.27)
        y += measure(lines, TXT_FS, 1.27) + PARA_GAP

# legend in the free tile slot
left, ytop, r = slot(len(scene.SCENARIOS))
lax = axes_at(left, ytop + 0.32, tile_w, tile + row_text[r] - 0.2); lax.set_axis_off()
handles = [
    Line2D([], [], color=DS1C, lw=2.2, label="DS1 edge (City GIS)"),
    Line2D([], [], color=DS2C, lw=1.9, ls=(0, (3.5, 2)), label="DS2 edge (Field survey)"),
    Line2D([], [], color=OUTC, lw=2.1, label="output edge"),
    Line2D([], [], color=CONNC, lw=3.2, label="connector (made by a rule)"),
    Line2D([], [], color=REMC, lw=6.5, alpha=0.22, label="removed duplicate"),
    Patch(fc=ZONEC, ec=ZONE_EDGE, hatch="..", label="pedestrian zone (output)"),
    Line2D([], [], marker="o", ls="", ms=6, mfc=NODEC, mec="#1e7a46", label="node"),
    Line2D([], [], marker="s", ls="", ms=7, mfc="white", mec=KERBC, mew=1.6, label="kerb (never moves)"),
    Line2D([], [], marker="s", ls="", ms=6, mfc="none", mec=DS2C, mew=1.1, label="input kerb (close-ups)"),
    Line2D([], [], marker="D", ls="", ms=6, mfc=SNAPC, mec="white", label="node others snapped onto"),
    Line2D([], [], marker="*", ls="", ms=12, mfc=NEWC, mec="#5d4037", label="junction node a rule made"),
    Line2D([], [], marker="o", ls="", ms=11, mfc="none", mec=CONNC, mew=1.4, label="existing node a rule used"),
    Line2D([], [], marker="s", ls="", ms=7, mfc="#9e9e9e", mec="white", label="bench  ▼ bin  ▲ hydrant  ★ lamp"),
]
lax.legend(handles=handles, loc="upper left", fontsize=9.6, frameon=True, title="Legend", title_fontsize=11,
           borderpad=0.8, labelspacing=0.75, handlelength=2.6)
key = ("✓ PASS / ✗ FAIL: a default run does / does not do\nwhat the spot describes."
       if is_default else
       "= default: this config gives the same result here\nas the default rules.  ≠ default: it does not\n"
       "(not an error: compare 'This run' with 'Default rules').")
lax.text(0.02, 0.0, key + "\nClose-ups: input faint underneath, output on top.\nOverview: blocks grey, roads white, "
         "living street sand.", transform=lax.transAxes, fontsize=8.6, va="bottom", color="#455a64")

path = a.out or os.path.join(a.run_dir, "demo.png")
fig.savefig(path, dpi=DPI)
print("wrote", path)
