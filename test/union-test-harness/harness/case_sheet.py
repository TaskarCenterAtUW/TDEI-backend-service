"""
Visual case sheet: one panel per case — the reference to hold next to QGIS.

    python3 harness/case_sheet.py                                  # inputs + expectations
    python3 harness/case_sheet.py <union_output_dir> --proximity 3 # ...plus the actual output

Legend
  blue solid        DS1 edge (authoritative)
  orange dashed     DS2 edge (stitched in)
  red, faded        DS2 edge that MUST be removed as a duplicate
  green ring        point where the union MUST produce a connecting node
  purple square     kerb node
  black thin        ACTUAL union output (when an output dir is given)
  panel frame       green = case passed, red = failed (when an output dir is given)
Writes docs/images/reference_sheet.png, or <union_output_dir>/case_sheet.png when checking an output.
"""
import argparse, json, os, subprocess, sys, textwrap
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from cases import CASES, M
from common import inputs, case_edges, HERE, LAYERS, IMAGES

ap = argparse.ArgumentParser()
ap.add_argument("output_dir", nargs="?")
ap.add_argument("--proximity", type=float, default=3.0)
args = ap.parse_args()

inp = inputs()
jn = json.load(open(os.path.join(LAYERS, "expected_junctions.geojson")))["features"]
rm = json.load(open(os.path.join(LAYERS, "expected_removed.geojson")))["features"]
status, out_edges = {}, []
if args.output_dir:
    # use the results check_cases.py already wrote (a filter scenario's results
    # differ from a default check, so never re-check here if they exist)
    if not os.path.exists(os.path.join(args.output_dir, "case_results.geojson")):
        subprocess.run([sys.executable, os.path.join(HERE, "check_cases.py"), args.output_dir,
                        "--proximity", str(args.proximity)], stdout=subprocess.DEVNULL)
    res = json.load(open(os.path.join(args.output_dir, "case_results.geojson")))["features"]
    status = {f["properties"]["case"]: f["properties"] for f in res}
    out_edges = json.load(open(os.path.join(args.output_dir, "osw_edges.geojson")))["features"]

def xy(coords): return zip(*[M(*c) for c in coords])

cols = 5; rows = (len(CASES) + cols - 1) // cols
fig, axes = plt.subplots(rows, cols, figsize=(cols * 5.2, rows * 5.9))
for ax in axes.flat: ax.set_axis_off()
for ax, c in zip(axes.flat, CASES):
    cid = c["id"]; ax.set_axis_on(); ax.set_xticks([]); ax.set_yticks([])
    kerbs = []
    for ds, f in case_edges(inp, cid):
        xs, ys = xy(f["geometry"]["coordinates"])
        if ds == "ds1": ax.plot(xs, ys, color="#1f5fbf", lw=3.2, solid_capstyle="round", zorder=2)
        else:           ax.plot(xs, ys, color="#f08c00", lw=2.0, ls=(0, (4, 2.5)), zorder=3)
    for ds in ("ds1", "ds2"):
        for n in inp[ds]["nodes"]:
            if n["properties"].get("barrier") == "kerb":
                kerbs.append(M(*n["geometry"]["coordinates"]))
    for f in rm:
        if f["properties"]["case"] == cid:
            xs, ys = xy(f["geometry"]["coordinates"])
            ax.plot(xs, ys, color="#d62728", lw=7, alpha=0.25, zorder=1)
    for e in out_edges:
        if e["properties"].get("name", "").startswith(cid + ":"):
            xs, ys = xy(e["geometry"]["coordinates"])
            ax.plot(xs, ys, color="black", lw=0.9, zorder=4)
    ax.relim(); ax.autoscale_view()
    x0, x1 = ax.get_xlim(); y0, y1 = ax.get_ylim()
    for kx, ky in kerbs:
        if x0 - 5 <= kx <= x1 + 5 and y0 - 5 <= ky <= y1 + 5:
            ax.plot(kx, ky, "s", ms=9, mfc="none", mec="#7b2cbf", mew=2, zorder=5)
    for f in jn:
        if f["properties"]["case"] == cid:
            jx, jy = M(*f["geometry"]["coordinates"])
            ax.plot(jx, jy, "o", ms=15, mfc="none", mec="#2ca02c", mew=2.5, zorder=6)
    span = max(x1 - x0, y1 - y0, 12) * 0.62
    cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
    ax.set_xlim(cx - span, cx + span); ax.set_ylim(cy - span, cy + span); ax.set_aspect("equal")
    ax.set_title(f'{cid} — {c["title"]}', fontsize=12, fontweight="bold", loc="left")
    body = "Setup: " + c["setup"] + "\nExpected: " + c["expected"]
    ax.text(0.0, -0.02, "\n".join(textwrap.fill(p, 62) for p in body.split("\n")),
            transform=ax.transAxes, va="top", fontsize=7.6, family="DejaVu Sans")
    if cid in status:
        ok = status[cid]["status"] == "PASS"
        for s in ax.spines.values(): s.set_edgecolor("#2ca02c" if ok else "#d62728"); s.set_linewidth(4)
        ax.text(0.98, 0.96, status[cid]["status"], transform=ax.transAxes, ha="right", va="top",
                fontsize=13, fontweight="bold", color="#2ca02c" if ok else "#d62728")
        if not ok:
            ax.text(0.02, 0.04, textwrap.fill(status[cid]["failures"], 55), transform=ax.transAxes,
                    fontsize=7, color="#d62728", va="bottom",
                    bbox=dict(fc="white", ec="#d62728", alpha=0.9))
handles = [plt.Line2D([], [], color="#1f5fbf", lw=3.2, label="DS1 edge"),
           plt.Line2D([], [], color="#f08c00", lw=2, ls="--", label="DS2 edge"),
           plt.Line2D([], [], color="#d62728", lw=7, alpha=0.25, label="must be removed"),
           plt.Line2D([], [], marker="o", ls="", ms=12, mfc="none", mec="#2ca02c", mew=2.5, label="must connect here"),
           plt.Line2D([], [], marker="s", ls="", ms=9, mfc="none", mec="#7b2cbf", mew=2, label="kerb node")]
if out_edges: handles.append(plt.Line2D([], [], color="black", lw=0.9, label="actual union output"))
fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, 0.975), ncol=len(handles), fontsize=13, frameon=False)
title = "OSW union test cases — inputs and expectations (default run)"
if args.output_dir:
    n_ok = sum(1 for v in status.values() if v["status"] == "PASS")
    title += f"   |   actual: {os.path.basename(os.path.normpath(args.output_dir))} @ {args.proximity:g} m — {n_ok}/{len(CASES)} passed"
fig.suptitle(title, fontsize=17, fontweight="bold", y=0.998)
fig.tight_layout(rect=(0, 0.035, 1, 0.95), h_pad=7.5)
name = (os.path.join(args.output_dir, "case_sheet.png") if args.output_dir
        else os.path.join(IMAGES, "reference_sheet.png"))
fig.savefig(name, dpi=90)
print("wrote", name)
