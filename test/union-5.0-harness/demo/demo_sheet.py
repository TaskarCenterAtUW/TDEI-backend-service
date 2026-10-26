"""
The demo picture: the whole neighbourhood before and after the union, in one
image.

    python3 demo/demo_sheet.py runs/demo_default_1m --profile default_1m

Top: the scene as the two sources describe it (left) and as the union outputs
it (right), drawn like a street map, with numbered badges. Below: a close-up of
every numbered spot — input drawn faint underneath, output on top — with what
the sources hold there, what the union did, and whether the run did it (PASS).
Writes <run>/demo.png.
"""
import argparse, json, os, sys, textwrap
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon, Rectangle, Circle
from matplotlib.lines import Line2D

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "harness"))
import scene
from scene import M
import profiles

ap = argparse.ArgumentParser()
ap.add_argument("run_dir")
ap.add_argument("--profile", required=True)
ap.add_argument("--out", help="image path (default <run>/demo.png)")
a = ap.parse_args()
prof = profiles.get(a.profile)

def load(path):
    return json.load(open(path))["features"] if os.path.exists(path) else []
DATA = os.path.join(HERE, "data")
inp = {ds: {k: load(os.path.join(DATA, f"{ds}_{k}.geojson")) for k in ("nodes", "edges", "points", "lines", "polygons")}
       for ds in ("ds1", "ds2")}
out = {k: load(os.path.join(a.run_dir, f"osw_{k}.geojson")) for k in ("nodes", "edges", "points", "lines", "polygons")}
removed = [f for f in load(os.path.join(a.run_dir, "changes_input_edges.geojson")) if f["properties"]["fate"] == "removed"]
changed_nodes = load(os.path.join(a.run_dir, "changes_nodes.geojson"))
status = {f["properties"]["n"]: f["properties"] for f in load(os.path.join(a.run_dir, "demo_results.geojson"))}
report = json.load(open(os.path.join(a.run_dir, "report.json"))) if os.path.exists(os.path.join(a.run_dir, "report.json")) else {}
in_coords = {tuple(n["geometry"]["coordinates"]) for ds in inp for n in inp[ds]["nodes"]}

# ── colours ──
BLOCK, ROADC, LIVINGC, WALKC = "#e4e2df", "#ffffff", "#f4eee2", "#eeece8"
BLDG, BLDG_EDGE, LABEL = "#dcd2c9", "#c3b5a8", "#8d8279"
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
main_w, main_gap, side = 12.55, 0.3, 0.3
x0, x1, y0, y1 = scene.EXTENT
main_h = main_w * (y1 - y0) / (x1 - x0)
tile, tile_gap, text_h, cols = 3.3, 0.37, 1.55, 7
rows = (len(scene.SCENARIOS) + 1 + cols - 1) // cols
H = 1.55 + main_h + 0.55 + rows * (tile + text_h + 0.35) + 0.25
fig = plt.figure(figsize=(W, H))
def axes_at(left, top, w, h):                 # inches from the top-left corner
    return fig.add_axes([left / W, 1 - (top + h) / H, w / W, h / H])

ds_names = f"DS1 = {scene.DATASETS['ds1']}, DS2 = {scene.DATASETS['ds2']}"
checked = [v for v in status.values() if v["status"] != "not checked"]
verdict = (f"{sum(v['status'] == 'PASS' for v in checked)}/{len(checked)} scenarios behave as described"
           if checked else "Scenario checks describe default_1m: this run is drawn, not checked")
fig.text(side / W, 1 - 0.38 / H, "Union 5.0 demo — Harbor Blvd × Oak St, one neighbourhood from two sources",
         fontsize=22, fontweight="bold", va="center")
fig.text(side / W, 1 - 0.85 / H, f"{ds_names}.   Config: {a.profile} — {prof['title']}.   {verdict}.",
         fontsize=13.5, va="center", color="#37474f")

top = 1.55
inp_polys = [(f, (BLDG, DS1C, "-", 0.9)) for f in inp["ds1"]["polygons"]] + \
            [(f, ("none", DS2C, (0, (3, 2)), 1.2)) for f in inp["ds2"]["polygons"]]
out_polys = [(f, (BLDG, BLDG_EDGE, "-", 0.7)) for f in out["polygons"]]

ax = axes_at(side, top, main_w, main_h)
basemap(ax, inp_polys, None); street_names(ax, 9); building_labels(ax, inp["ds1"]["polygons"], 8)
draw_input(ax); badges(ax, 4.2); frame(ax, x0, x1, y0, y1); scale_bar(ax, 20, "20 m", x0, x1, y0, y1, right=True)
ax.set_title("INPUT — what the two sources hold", fontsize=15, fontweight="bold", loc="left", pad=6)
c = report.get("counts", {}).get("input", {})
ax.text(0.99, 0.985, f"DS1 + DS2: {c.get('edges', '?')} edges, {c.get('nodes', '?')} nodes, "
        f"{c.get('points', 0) + c.get('lines', 0) + c.get('polygons', 0)} extension features",
        transform=ax.transAxes, ha="right", va="top", fontsize=10, color="#37474f",
        bbox=dict(fc="white", ec="#b0bec5", alpha=0.92, boxstyle="round,pad=0.35"), zorder=12)

ax = axes_at(side + main_w + main_gap, top, main_w, main_h)
basemap(ax, out_polys, None); street_names(ax, 9); building_labels(ax, out["polygons"], 8)
draw_output(ax); badges(ax, 4.2); frame(ax, x0, x1, y0, y1); scale_bar(ax, 20, "20 m", x0, x1, y0, y1, right=True)
ax.set_title("OUTPUT — the union", fontsize=15, fontweight="bold", loc="left", pad=6)
co = report.get("counts", {})
ax.text(0.99, 0.985, f"{co.get('output', {}).get('edges', '?')} edges, {co.get('output', {}).get('nodes', '?')} nodes   ·   "
        f"{co.get('nodes_snapped', 0)} nodes snapped   ·   {co.get('edges_removed', 0)} duplicates removed   ·   "
        f"{co.get('nodes_created', 0)} junction nodes made   ·   {co.get('connectors', 0)} connectors",
        transform=ax.transAxes, ha="right", va="top", fontsize=10, color="#37474f",
        bbox=dict(fc="white", ec="#b0bec5", alpha=0.92, boxstyle="round,pad=0.35"), zorder=12)

# ── close-ups ──
tile_w = (W - 2 * side - (cols - 1) * tile_gap) / cols
ttop = top + main_h + 0.55
for i, s in enumerate(scene.SCENARIOS):
    r, cidx = divmod(i, cols)
    left = side + cidx * (tile_w + tile_gap)
    ytop = ttop + r * (tile + text_h + 0.35)
    ax = axes_at(left, ytop + 0.32, tile_w, tile)
    cx, cy, size = s["zoom"]
    hx, hy = size / 2 * tile_w / tile, size / 2
    basemap(ax, [(f, (BLDG, BLDG_EDGE, "-", 0.7)) for f in out["polygons"]], None)
    draw_input(ax, scale=1.7, faint=True)
    draw_output(ax, scale=1.7)
    frame(ax, cx - hx, cx + hx, cy - hy, cy + hy)
    bar = 1 if size <= 5 else 2 if size <= 8 else 5
    scale_bar(ax, bar, f"{bar} m", cx - hx, cx + hx, cy - hy, cy + hy)
    st = status.get(s["n"], {}).get("status", "not checked")
    col = {"PASS": "#2e7d32", "FAIL": "#c62828"}.get(st, "#757575")
    for sp in ax.spines.values(): sp.set_edgecolor(col); sp.set_linewidth(2.2 if st != "not checked" else 1)
    fig.text((left) / W, 1 - (ytop + 0.14) / H, f"{s['n']}", fontsize=13, fontweight="bold", va="center",
             color="white", bbox=dict(boxstyle="circle,pad=0.25", fc="#263238", ec="none"))
    fig.text((left + 0.36) / W, 1 - (ytop + 0.14) / H, s["title"], fontsize=11.3, fontweight="bold", va="center")
    if st != "not checked":
        ax.text(0.97, 0.96, "✓ PASS" if st == "PASS" else "✗ FAIL", transform=ax.transAxes, ha="right", va="top",
                fontsize=10, fontweight="bold", color=col, zorder=12,
                bbox=dict(fc="white", ec=col, boxstyle="round,pad=0.25"))
    width = int(tile_w * 13.2)
    body = ["Input: " + s["input"], ("Union: " if st != "not checked" else "With default_1m: ") + s["result"]]
    if st == "FAIL":
        body.append("FAILED: " + status[s["n"]]["failures"])
    fig.text(left / W, 1 - (ytop + 0.32 + tile + 0.08) / H,
             "\n".join(textwrap.fill(b, width) for b in body), fontsize=8.6, va="top", color="#263238",
             linespacing=1.25)

# legend in the free tile slot
i = len(scene.SCENARIOS); r, cidx = divmod(i, cols)
left = side + cidx * (tile_w + tile_gap); ytop = ttop + r * (tile + text_h + 0.35)
lax = axes_at(left, ytop + 0.32, tile_w, tile + text_h - 0.2); lax.set_axis_off()
handles = [
    Line2D([], [], color=DS1C, lw=2.2, label="DS1 edge (City GIS)"),
    Line2D([], [], color=DS2C, lw=1.9, ls=(0, (3.5, 2)), label="DS2 edge (Field survey)"),
    Line2D([], [], color=OUTC, lw=2.1, label="output edge"),
    Line2D([], [], color=CONNC, lw=3.2, label="connector (made by a rule)"),
    Line2D([], [], color=REMC, lw=6.5, alpha=0.22, label="removed duplicate"),
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
lax.text(0.02, 0.0, "Close-ups: input faint underneath, output on top.\nOverview: blocks grey, roads white, "
         "living street sand,\nsidewalk paving light grey.", transform=lax.transAxes, fontsize=8.6, va="bottom",
         color="#455a64")

path = a.out or os.path.join(a.run_dir, "demo.png")
fig.savefig(path, dpi=100)
print("wrote", path)
