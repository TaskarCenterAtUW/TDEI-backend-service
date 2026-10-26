"""
Visual case sheet: one panel per case — the reference to hold next to QGIS.

    python3 harness/case_sheet.py --profile default_1m                 # inputs + what that profile expects
    python3 harness/case_sheet.py --all                                # a reference sheet for every profile
    python3 harness/case_sheet.py runs/default_3m --profile default_3m # ...plus the actual output of a run

Only the cases checked under the profile get a panel.

Legend
  blue solid        DS1 edge                    orange dashed     DS2 edge
  red, faded        edge that MUST be removed as a duplicate
  green ring        point where the union MUST produce a connecting node
  purple square     kerb node
  thin outlines     zones / polygons; dots = extension points
  black thin        ACTUAL union output (when a run folder is given)
  magenta           connector edge a connect rule made (actual output)
  green star        node a connect rule created (actual output)
  inset             a 4 m close-up of the first must-connect point, where gaps and connectors show
  panel frame       green = case passed, red = failed (when a run folder is given)
Writes docs/images/reference_<profile>.png, or <run>/case_sheet.png when a run folder is given.
"""
import argparse, json, os, subprocess, sys, textwrap
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from cases import CASES, M, asserts_for
from common import inputs, case_features, HERE, LAYERS, IMAGES, load
import profiles

ap = argparse.ArgumentParser()
ap.add_argument("output_dir", nargs="?")
ap.add_argument("--profile")
ap.add_argument("--all", action="store_true", help="write a reference sheet for every profile")
args = ap.parse_args()
if args.all:
    for p in profiles.PROFILES:
        subprocess.run([sys.executable, os.path.abspath(__file__), "--profile", p], check=True)
    sys.exit(0)
if not args.profile:
    sys.exit("--profile is required (one of: " + ", ".join(profiles.PROFILES) + ")")
prof = profiles.get(args.profile)

inp = inputs()
jn = [f for f in load(os.path.join(LAYERS, "expected_junctions.geojson")) if f["properties"]["profile"] == args.profile]
rm = [f for f in load(os.path.join(LAYERS, "expected_removed.geojson")) if f["properties"]["profile"] == args.profile]
input_coords = {tuple(n["geometry"]["coordinates"]) for ds in ("ds1", "ds2") for n in inp[ds]["nodes"]}
status, out_edges, out_nodes = {}, [], []
if args.output_dir:
    if not os.path.exists(os.path.join(args.output_dir, "case_results.geojson")):
        subprocess.run([sys.executable, os.path.join(HERE, "check_cases.py"), args.output_dir,
                        "--profile", args.profile], stdout=subprocess.DEVNULL)
    status = {f["properties"]["case"]: f["properties"]
              for f in load(os.path.join(args.output_dir, "case_results.geojson"))}
    out_edges = load(os.path.join(args.output_dir, "osw_edges.geojson"))
    out_nodes = load(os.path.join(args.output_dir, "osw_nodes.geojson"))

def xy(coords): return zip(*[M(*c) for c in coords])

shown = [c for c in CASES if asserts_for(c, args.profile)]

def draw(ax, cid, inset=False):
    """Inputs, expectations and actual output of one case onto ax."""
    lw = (lambda w: w * 0.6) if inset else (lambda w: w)
    for ds, kind, f in case_features(inp, cid):
        col = "#1f5fbf" if ds == "ds1" else "#f08c00"
        g = f["geometry"]
        if kind == "edges":
            xs, ys = xy(g["coordinates"])
            if ds == "ds1": ax.plot(xs, ys, color=col, lw=lw(3.2), solid_capstyle="round", zorder=2)
            else:           ax.plot(xs, ys, color=col, lw=lw(2.0), ls=(0, (4, 2.5)), zorder=3)
        elif kind in ("zones", "polygons"):
            for ring in g["coordinates"]:
                xs, ys = xy(ring); ax.plot(xs, ys, color=col, lw=1.2, ls="-" if ds == "ds1" else "--", zorder=2)
        elif kind == "lines":
            xs, ys = xy(g["coordinates"]); ax.plot(xs, ys, color=col, lw=1.4, ls="-" if ds == "ds1" else "--", zorder=2)
        elif kind == "points":
            px, py = M(*g["coordinates"]); ax.plot(px, py, "o", ms=6, color=col, zorder=3)
    for f in rm:
        if f["properties"]["case"] == cid:
            xs, ys = xy(f["geometry"]["coordinates"])
            ax.plot(xs, ys, color="#d62728", lw=lw(7), alpha=0.25, zorder=1)
    for e in out_edges:
        if e["properties"].get("name", "").startswith(cid + ":"):
            xs, ys = xy(e["geometry"]["coordinates"]); ax.plot(xs, ys, color="black", lw=0.9, zorder=4)

def near_view(ax, pad=5):
    x0, x1 = ax.get_xlim(); y0, y1 = ax.get_ylim()
    return lambda x, y: x0 - pad <= x <= x1 + pad and y0 - pad <= y <= y1 + pad

def draw_points(ax, cid, near, inset=False):
    """Connectors, created nodes, kerbs and must-connect rings within view."""
    for e in out_edges:                                   # connectors carry no name: draw those in view
        if "ext:union_rule" in e["properties"]:
            pts = [M(*q) for q in e["geometry"]["coordinates"]]
            if all(near(*q) for q in pts):
                ax.plot([q[0] for q in pts], [q[1] for q in pts], color="#d0189c", lw=2.2, zorder=5)
    for n in out_nodes:
        if tuple(n["geometry"]["coordinates"]) not in input_coords:
            nx, ny = M(*n["geometry"]["coordinates"])
            if near(nx, ny): ax.plot(nx, ny, "*", ms=11, color="#2ca02c", mec="black", mew=0.4, zorder=6)
    for ds in ("ds1", "ds2"):
        for n in inp[ds]["nodes"]:
            if n["properties"].get("barrier") == "kerb":
                kx, ky = M(*n["geometry"]["coordinates"])
                if near(kx, ky): ax.plot(kx, ky, "s", ms=9, mfc="none", mec="#7b2cbf", mew=2, zorder=5)
    for f in jn:
        if f["properties"]["case"] == cid:
            jx, jy = M(*f["geometry"]["coordinates"])
            if near(jx, jy): ax.plot(jx, jy, "o", ms=15 if not inset else 22, mfc="none", mec="#2ca02c", mew=2.5, zorder=7)

cols = 5; rows = (len(shown) + cols - 1) // cols
fig, axes = plt.subplots(rows, cols, figsize=(cols * 5.2, rows * 5.9), squeeze=False)
for ax in axes.flat: ax.set_axis_off()
for ax, c in zip(axes.flat, shown):
    cid = c["id"]; ax.set_axis_on(); ax.set_xticks([]); ax.set_yticks([])
    draw(ax, cid)
    ax.relim(); ax.autoscale_view()
    draw_points(ax, cid, near_view(ax))
    x0, x1 = ax.get_xlim(); y0, y1 = ax.get_ylim()
    span = max(x1 - x0, y1 - y0, 12) * 0.62
    cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
    ax.set_xlim(cx - span, cx + span); ax.set_ylim(cy - span, cy + span); ax.set_aspect("equal")
    # a 4 m close-up of the first must-connect point (or created node), where gaps and connectors show
    focus = [M(*f["geometry"]["coordinates"]) for f in jn if f["properties"]["case"] == cid]
    if not focus:
        focus = [M(*e["geometry"]["coordinates"][0]) for e in out_edges if "ext:union_rule" in e["properties"]
                 and abs(M(*e["geometry"]["coordinates"][0])[0] - cx) < span and abs(M(*e["geometry"]["coordinates"][0])[1] - cy) < span]
    if focus and span > 6:
        fx, fy = focus[0]
        ins = ax.inset_axes([0.62, 0.02, 0.36, 0.36])
        ins.set_xticks([]); ins.set_yticks([])
        draw(ins, cid, inset=True)
        ins.set_xlim(fx - 2, fx + 2); ins.set_ylim(fy - 2, fy + 2); ins.set_aspect("equal")
        draw_points(ins, cid, near_view(ins, 0), inset=True)
        ins.set_xlim(fx - 2, fx + 2); ins.set_ylim(fy - 2, fy + 2)
        for sp in ins.spines.values(): sp.set_edgecolor("#888888")
        ins.text(0.03, 0.97, "4 m", transform=ins.transAxes, va="top", fontsize=7, color="#555555")
    ax.set_title(f'{cid} — {c["title"]}', fontsize=12, fontweight="bold", loc="left")
    body = "Setup: " + c["setup"] + "\nExpected: " + c["expected"]
    ax.text(0.0, -0.02, "\n".join(textwrap.fill(p, 62) for p in body.split("\n")),
            transform=ax.transAxes, va="top", fontsize=7.6, family="DejaVu Sans")
    if cid in status:
        ok = status[cid]["status"] == "PASS"
        for sp in ax.spines.values(): sp.set_edgecolor("#2ca02c" if ok else "#d62728"); sp.set_linewidth(4)
        ax.text(0.98, 0.96, status[cid]["status"], transform=ax.transAxes, ha="right", va="top",
                fontsize=13, fontweight="bold", color="#2ca02c" if ok else "#d62728")
        if not ok:
            ax.text(0.02, 0.04, textwrap.fill(status[cid]["failures"], 55), transform=ax.transAxes,
                    fontsize=7, color="#d62728", va="bottom", bbox=dict(fc="white", ec="#d62728", alpha=0.9))
handles = [plt.Line2D([], [], color="#1f5fbf", lw=3.2, label="DS1 edge"),
           plt.Line2D([], [], color="#f08c00", lw=2, ls="--", label="DS2 edge"),
           plt.Line2D([], [], color="#d62728", lw=7, alpha=0.25, label="must be removed"),
           plt.Line2D([], [], marker="o", ls="", ms=12, mfc="none", mec="#2ca02c", mew=2.5, label="must connect here"),
           plt.Line2D([], [], marker="s", ls="", ms=9, mfc="none", mec="#7b2cbf", mew=2, label="kerb node")]
if out_edges:
    handles += [plt.Line2D([], [], color="black", lw=0.9, label="actual output"),
                plt.Line2D([], [], color="#d0189c", lw=2.2, label="connector (actual)"),
                plt.Line2D([], [], marker="*", ls="", ms=11, color="#2ca02c", mec="black", label="created node (actual)")]
fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, 0.985), ncol=len(handles), fontsize=13, frameon=False)
title = f"Union 5.0 harness — profile {args.profile}: {prof['title']}"
if args.output_dir:
    n_ok = sum(1 for v in status.values() if v["status"] == "PASS")
    title += f"   |   actual: {n_ok}/{len(shown)} passed"
fig.suptitle(title, fontsize=17, fontweight="bold", y=0.999)
fig.tight_layout(rect=(0, 0.02, 1, 1 - 0.9 / (rows * 5.9)), h_pad=7.5)
name = (os.path.join(args.output_dir, "case_sheet.png") if args.output_dir
        else os.path.join(IMAGES, f"reference_{args.profile}.png"))
os.makedirs(os.path.dirname(name), exist_ok=True)
fig.savefig(name, dpi=80)
print("wrote", name)
