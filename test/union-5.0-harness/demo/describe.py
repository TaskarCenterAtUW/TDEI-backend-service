"""
Say in plain words what one demo run did, read only from the run's own files,
so it describes whatever config ran:

  config_columns(run)     the applied configuration, stage by stage, every rule
                          with its settings and what it did (report.json)
  spot_events(run, look)  what happened inside one numbered spot's look boxes:
                          nodes snapped, duplicates removed, edges split,
                          connectors and junction nodes made, points and
                          buildings merged

check_demo.py writes the spot text into demo_results.geojson ("this_run");
demo_sheet.py draws both.

    python3 demo/describe.py runs/demo_input        # print both
"""
import json, math, os, sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import scene
from scene import M

KINDS = ("nodes", "edges", "points", "lines", "polygons")
SOURCE = {"ds1": scene.DATASETS["ds1"], "ds2": scene.DATASETS["ds2"], "DS1": scene.DATASETS["ds1"],
          "DS2": scene.DATASETS["ds2"]}
NOTHING = "Nothing changed here: both sources' features pass through as they are."


def _load(path):
    return json.load(open(path, encoding="utf-8"))["features"] if os.path.exists(path) else []

def _key(c):
    return (round(c[0], 7), round(c[1], 7))

def _dist(p, q):
    return math.hypot(p[0] - q[0], p[1] - q[1])

def _xy(line):
    return [M(*c) for c in line]

def _length(pts):
    return sum(_dist(pts[i], pts[i + 1]) for i in range(len(pts) - 1))

def _midpoint(pts):
    half, run = _length(pts) / 2, 0.0
    for p, q in zip(pts, pts[1:]):
        d = _dist(p, q)
        if run + d >= half and d > 0:
            t = (half - run) / d
            return (p[0] + t * (q[0] - p[0]), p[1] + t * (q[1] - p[1]))
        run += d
    return pts[0]

def _inside(p, boxes):
    return any(x0 <= p[0] <= x1 and y0 <= p[1] <= y1 for x0, x1, y0, y1 in boxes)

def _tags(props):
    return {k: v for k, v in props.items() if k != "_id" and not k.startswith("ext:union_")}

def _kind(tags):
    v = tags.get("amenity") or tags.get("emergency") or tags.get("highway") or "point"
    return v.replace("_", " ")

def _and(items):
    items = list(items)
    return items[0] if len(items) == 1 else ", ".join(items[:-1]) + " and " + items[-1]

def _m(d):
    return f"{d:.2f} m"

def _n(k, word):
    return f"{k} {word}" + ("" if k == 1 else "s")

def _by_source(items):
    """[(ds, thing)] -> "the City GIS's 'a' and 'b', and the Field survey's 'c'" """
    groups = {}
    for ds, thing in items:
        groups.setdefault(SOURCE[ds], [])
        if thing not in groups[SOURCE[ds]]:
            groups[SOURCE[ds]].append(thing)
    parts = [f"the {src}'s {_and(things)}" for src, things in groups.items()]
    if len(parts) == 1:
        return parts[0]
    sep = ", and " if len(parts) > 2 or " and " in parts[0] else " and "
    return ", ".join(parts[:-1]) + sep + parts[-1]


class Run:
    """One run's input (demo/data) and output (the run folder), indexed."""

    def __init__(self, run_dir, data_dir=os.path.join(HERE, "data")):
        self.dir = run_dir
        self.inp = {ds: {k: _load(os.path.join(data_dir, f"{ds}_{k}.geojson")) for k in KINDS}
                    for ds in ("ds1", "ds2")}
        self.out = {k: _load(os.path.join(run_dir, f"osw_{k}.geojson")) for k in KINDS}
        self.ch_nodes = _load(os.path.join(run_dir, "changes_nodes.geojson"))
        self.ch_in = _load(os.path.join(run_dir, "changes_input_edges.geojson"))
        self.ch_out = _load(os.path.join(run_dir, "changes_output_edges.geojson"))
        rp = os.path.join(run_dir, "report.json")
        self.report = json.load(open(rp)) if os.path.exists(rp) else {}

        self.in_node = {}                         # id -> (ds, xy, props)
        self.in_nodes_at = defaultdict(list)      # coordinate -> [(ds, id, props)]
        self.in_edge = {}                         # id -> (ds, name, xy line)
        self.in_point = {}                        # id -> (ds, xy, props)
        self.in_points_at = defaultdict(list)
        for ds in ("ds1", "ds2"):
            for f in self.inp[ds]["nodes"]:
                c, p = f["geometry"]["coordinates"], f["properties"]
                self.in_node[str(p["_id"])] = (ds, M(*c), p)
                self.in_nodes_at[_key(c)].append((ds, str(p["_id"]), p))
            for f in self.inp[ds]["edges"]:
                p = f["properties"]
                self.in_edge[str(p["_id"])] = (ds, p.get("name", "edge"), _xy(f["geometry"]["coordinates"]))
            for f in self.inp[ds]["points"]:
                c, p = f["geometry"]["coordinates"], f["properties"]
                self.in_point[str(p["_id"])] = (ds, M(*c), p)
                self.in_points_at[_key(c)].append((ds, str(p["_id"]), p))
        self.out_node = {str(f["properties"]["_id"]): f for f in self.out["nodes"]}
        self.out_edge_from = {str(f["properties"]["output_id"]): f["properties"]["from"] for f in self.ch_out}
        self.absorbed_by = {}                     # input edge id -> source of the output edge that absorbed it
        for f in self.out["edges"]:
            for i in str(f["properties"].get("ext:union_absorbed", "")).split(","):
                if i:
                    self.absorbed_by[i] = self.out_edge_from.get(str(f["properties"]["_id"]), "")
        self.pieces = defaultdict(list)           # input edge id -> its split pieces (xy lines)
        for f in self.ch_out:
            if f["properties"]["change"] == "split piece":
                self.pieces[str(f["properties"]["from_input_id"])].append(_xy(f["geometry"]["coordinates"]))

    @property
    def effective(self):
        return self.report.get("effective_config", {})


# ── what happened at one spot ─────────────────────────────────────────────────────
def spot_events(run, look):
    """Plain sentences, in stage order, for everything the run did inside the boxes."""
    ev = []

    # snap: output nodes other nodes joined
    for f in run.ch_nodes:
        p = f["properties"]
        if p["change"] != "snapped" or not _inside(M(*f["geometry"]["coordinates"]), look):
            continue
        node = run.out_node.get(str(p["output_id"]))
        if not node:
            continue
        props, pos = node["properties"], M(*node["geometry"]["coordinates"])
        anchors = run.in_nodes_at.get(_key(node["geometry"]["coordinates"]), [])
        anchor_ds, anchor_tags = (anchors[0][0], _tags(anchors[0][2])) if anchors else (None, {})
        joined = [run.in_node[i] for i in str(props.get("ext:union_absorbed", "")).split(",") if i in run.in_node]
        if not joined:
            continue
        kerbs = props.get("barrier") == "kerb" and all(j[2].get("barrier") == "kerb" for j in joined)
        dists = [_dist(j[1], pos) for j in joined]
        if kerbs and max(dists) < 0.005:
            text = "Snap: the two kerbs are at the same spot, so they join into one node"
        else:
            src = _and(sorted({SOURCE[j[0]] for j in joined}))
            what = "node" if len(joined) == 1 else f"{len(joined)} nodes"
            onto = f"the {SOURCE[anchor_ds]} node" if anchor_ds else "another node"
            text = f"Snap: the {src} {what} {', '.join(_m(d) for d in dists)} away joined {onto}"
        notes = []
        out_tags = _tags(props)
        for k, v in props.items():
            if not k.startswith("ext:union_audit_"):
                continue
            short = k[len("ext:union_audit_"):]
            tk = short if short in out_tags or short in anchor_tags else "ext:" + short
            audited = str(v).split("-", 1)[1] if "-" in str(v) else str(v)
            if tk in anchor_tags and str(anchor_tags[tk]) != audited:
                notes.append(f"{tk} stays '{anchor_tags[tk]}' ('{audited}' audited)")
        for k, v in out_tags.items():
            if anchors and k not in anchor_tags:
                notes.append(f"{k}={v} added")
        ev.append((0, text + ("; " + "; ".join(notes) if notes else "") + "."))

    # dedup / snap: input edges removed or reshaped
    removed, modified = defaultdict(lambda: [0.0, set()]), defaultdict(float)
    for f in run.ch_in:
        p = f["properties"]
        if p["fate"] not in ("removed", "modified"):
            continue
        pts = _xy(f["geometry"]["coordinates"])
        if not _inside(_midpoint(pts), look):
            continue
        key = (p["dataset"], p.get("name") or "edge")
        if p["fate"] == "removed":
            removed[key][0] += _length(pts)
            if run.absorbed_by.get(str(p["input_id"])):
                removed[key][1].add(run.absorbed_by[str(p["input_id"])])
        else:
            modified[key] += _length(pts)
    for (ds, name), (length, by) in sorted(removed.items()):
        of = f" of the {_and(sorted(SOURCE[b] for b in by))}'s" if by else ""
        ev.append((1, f"Dedup: the {SOURCE[ds]}'s '{name}' ({length:.0f} m here) removed as a duplicate{of}."))
    for (ds, name), length in sorted(modified.items()):
        ev.append((1, f"The {SOURCE[ds]}'s '{name}' kept, reshaped: an end moved with a snapped node."))

    # connect: junction nodes made or used, connectors, splits
    for f in run.ch_nodes:
        p = f["properties"]
        if p["change"] not in ("created", "junction") or not _inside(M(*f["geometry"]["coordinates"]), look):
            continue
        rules = _and(p["rules"].split(",")) if p.get("rules") else "a connect rule"
        if p["change"] == "created":
            ev.append((2, f"Connect: {rules} made a junction node."))
        else:
            node = run.out_node.get(str(p["output_id"]))
            what = "kerb" if node and node["properties"].get("barrier") == "kerb" else "node"
            ev.append((2, f"Connect: {rules} used the existing {what} as the junction."))
    for f in run.ch_out:
        p = f["properties"]
        if p["change"] != "connector":
            continue
        pts = _xy(f["geometry"]["coordinates"])
        if _inside(_midpoint(pts), look):
            ev.append((3, f"Connect: {p['rule']} added a {_m(_length(pts))} connector."))
    split = []
    for f in run.ch_in:
        p = f["properties"]
        if p["fate"] != "split":
            continue
        line = _xy(f["geometry"]["coordinates"])
        ends = (line[0], line[-1])
        cuts = [q for piece in run.pieces.get(str(p["input_id"]), []) for q in (piece[0], piece[-1])
                if min(_dist(q, e) for e in ends) > 0.01]
        if any(_inside(q, look) for q in cuts):
            split.append((p["dataset"], f"'{p.get('name') or 'edge'}'"))
    if split:
        ev.append((4, f"Split here: {_by_source(split)}."))

    # extensions: points
    out_at = {_key(f["geometry"]["coordinates"]): f for f in run.out["points"]}
    merged_away, anchors_of_merge = set(), set()
    for f in run.out["points"]:
        props, pos = f["properties"], M(*f["geometry"]["coordinates"])
        ids = {str(v).split("-", 1)[0] for k, v in props.items() if k.startswith("ext:union_audit_")}
        ids = {i for i in ids if i in run.in_point}
        here = run.in_points_at.get(_key(f["geometry"]["coordinates"]), [])
        ids -= {i for _, i, _ in here}
        if not ids:
            continue
        merged_away |= ids
        anchors_of_merge.add(_key(f["geometry"]["coordinates"]))
        if not _inside(pos, look):
            continue
        anchor_ds, anchor_tags = (here[0][0], _tags(here[0][2])) if here else (None, {})
        joined = [run.in_point[i] for i in sorted(ids)]
        src = _and(sorted({SOURCE[j[0]] for j in joined}))
        text = (f"Extensions: the {src} {_kind(joined[0][2])} {', '.join(_m(_dist(j[1], pos)) for j in joined)} "
                f"away merged into the {SOURCE[anchor_ds] if anchor_ds else 'other'} one")
        added = [f"{k}={v} added" for k, v in _tags(props).items() if here and k not in anchor_tags]
        ev.append((5, text + ("; " + "; ".join(added) if added else "") + "."))
    kept = []
    for ds in ("ds1", "ds2"):
        for f in run.inp[ds]["points"]:
            k, p = _key(f["geometry"]["coordinates"]), f["properties"]
            if not _inside(M(*f["geometry"]["coordinates"]), look) or str(p["_id"]) in merged_away:
                continue
            if k in out_at and k not in anchors_of_merge:
                kept.append((ds, _kind(p)))
            elif k not in out_at:
                ev.append((5, f"The {SOURCE[ds]} {_kind(p)} is not in the output."))
    if kept:
        ev.append((6, f"Points kept as they are: {_by_source(kept)}."))

    # extensions: buildings and other polygons, by name
    def centroid(f):
        r = _xy(f["geometry"]["coordinates"][0])[:-1]
        return (sum(p[0] for p in r) / len(r), sum(p[1] for p in r) / len(r))
    names = {}
    for ds in ("ds1", "ds2"):
        for f in run.inp[ds]["polygons"]:
            if _inside(centroid(f), look) and f["properties"].get("name"):
                names.setdefault(f["properties"]["name"], []).append((ds, f))
    for name, srcs in sorted(names.items()):
        outs = [f for f in run.out["polygons"] if f["properties"].get("name") == name]
        if len({ds for ds, _ in srcs}) < 2:
            if not outs:
                ev.append((7, f"'{name}' is not in the output."))
            continue
        if len(outs) >= 2:
            ev.append((7, f"'{name}': both outlines kept (not merged)."))
        elif len(outs) == 1:
            ring = outs[0]["geometry"]["coordinates"][0]
            base = next(((ds, f) for ds, f in srcs if _key(f["geometry"]["coordinates"][0][0]) == _key(ring[0])), srcs[0])
            added = [f"{k}={v} added" for k, v in _tags(outs[0]["properties"]).items()
                     if k not in _tags(base[1]["properties"])]
            ev.append((7, f"Extensions: the two '{name}' outlines merged into the {SOURCE[base[0]]} one"
                          + ("; " + "; ".join(added) if added else "") + "."))
        else:
            ev.append((7, f"'{name}' is not in the output."))

    ev.sort(key=lambda e: e[0])
    return [t for _, t in ev] or [NOTHING]


# ── the applied configuration ─────────────────────────────────────────────────────
def _type(t):
    if isinstance(t, str):
        return t
    if isinstance(t, dict):
        return "{" + ", ".join(f"{k}={v}" for k, v in t.items()) + "}"
    if isinstance(t, list):
        return " | ".join(_type(x) for x in t)
    return str(t)

def _p(v):
    return f"{v:g} m" if isinstance(v, (int, float)) else "?"

def config_columns(run):
    """[(STAGE, summary line, [rule lines])] for snap, dedup, connect, extensions."""
    eff = run.effective
    metrics = {(r["stage"], r["rule"]): r["metrics"] for r in run.report.get("rules", [])}
    cols = []

    st = eff.get("snap", {"mode": "off"})
    if st.get("mode", "off") == "off":
        cols.append(("SNAP", "off", ["Nodes stay where each source put them."]))
    else:
        lines = []
        if st.get("coincident"):
            m = metrics.get(("snap", "coincident"), {})
            lines.append(f"coincident: nodes at the same position join  →  {m.get('snapped', 0)} snapped")
        for r in st.get("rules", []):
            m = metrics.get(("snap", r["name"]), {})
            extra = "".join([f", anchor {r['anchor'].upper()}" if r.get("anchor") != st.get("anchor") else "",
                             f", {r['attributes']}" if r.get("attributes") != st.get("attributes") else ""])
            lines.append(f"{r['name']}: {_type(r['type'])} within {_p(r['proximity'])}{extra}"
                         f"  →  {m.get('snapped', 0)} snapped")
        if st.get("others"):
            o, m = st["others"], metrics.get(("snap", "others"), {})
            lines.append(f"others: every other type within {_p(o.get('proximity'))}  →  {m.get('snapped', 0)} snapped")
        cols.append(("SNAP", f"{st['mode']} · anchor {str(st.get('anchor', 'ds1')).upper()} · "
                             f"attributes {st.get('attributes', 'merge')}", lines))

    st = eff.get("dedup", {"mode": "off"})
    if st.get("mode", "off") == "off":
        cols.append(("DEDUP", "off", ["Duplicates from both sources are all kept."]))
    else:
        lines = []
        for r in st.get("rules", []):
            m = metrics.get(("dedup", r["name"]), {})
            what = _type(r["from"]) if r["from"] == r["to"] else f"{_type(r['from'])} → {_type(r['to'])}"
            lines.append(f"{r['name']}: {what} within {_p(r['proximity'])}, ≥ {r.get('min_overlap', 0.8) * 100:.0f} % "
                         f"overlap, anchor {str(r.get('anchor', 'ds1')).upper()}  →  {m.get('removed', 0)} removed"
                         f" of {m.get('candidates', 0)}")
        cols.append(("DEDUP", st["mode"], lines))

    st = eff.get("connect", {"mode": "off"})
    if st.get("mode", "off") == "off":
        cols.append(("CONNECT", "off", ["Gaps stay open; nothing is joined or split."]))
    else:
        lines = []
        for i, r in enumerate(st.get("rules", []), 1):
            m = metrics.get(("connect", r["name"]), {})
            tags = ", tags " + " ".join(f"{k}={v}" for k, v in r["tags"].items()) if r.get("tags") else ""
            skipped = sum(v for k, v in m.items() if k.startswith("skipped_"))
            lines.append(f"{i}. {r['name']}: {_type(r['from'])} → {_type(r['to'])}, {r['operation']} within "
                         f"{_p(r['proximity'])}{tags}  →  {_n(m.get('junctions', 0), 'junction')}, "
                         f"{_n(m.get('connectors', 0), 'connector')}" + (f", {skipped} skipped" if skipped else ""))
        cols.append(("CONNECT", f"{st['mode']} · rules apply in order", lines))

    st = eff.get("extensions", {"mode": "off"})
    c = run.report.get("counts", {})
    io = "; ".join(f"{k} {c.get('input', {}).get(k, 0)} → {c.get('output', {}).get(k, 0)}"
                   for k in ("points", "lines", "polygons", "zones") if c.get("input", {}).get(k))
    if st.get("mode", "off") == "off":
        cols.append(("EXTENSIONS", "off", ["Points, lines, buildings and zones from both sources are all kept.",
                                           f"in → out: {io}" if io else ""]))
    else:
        cols.append(("EXTENSIONS", f"default within {_p(st.get('proximity'))}", [
            "same-type points within reach merge; lines, buildings and zones merge when they overlap",
            f"overlap ≥ line {st.get('line_overlap', 0.7) * 100:.0f} %, polygon "
            f"{st.get('polygon_overlap', 0.7) * 100:.0f} %, zone {st.get('zone_overlap', 0.7) * 100:.0f} %",
            f"in → out: {io}" if io else ""]))
    return [(s, h, [l for l in ls if l]) for s, h, ls in cols]


if __name__ == "__main__":
    run = Run(sys.argv[1])
    for stage, head, lines in config_columns(run):
        print(f"{stage} — {head}")
        for l in lines:
            print("   ", l)
    for s in scene.SCENARIOS:
        print(f"{s['n']:2}. {s['title']}")
        for t in spot_events(run, s["look"]):
            print("     ", t)
