"""
Expected behaviour of a filtered union run, derived from the filter rules
(R1-R3 in scenarios.py) and the baseline run at the same proximity.

Everything is matched by geometry, never by output id.
"""
import json, math, os
from changes import xy, dist_to_line, load, SAME, ON
from routability import cat as type_group

# ---- filter matching: jsonb @> semantics on a flat property dict ----------------
def matches(props, groups):
    """groups: list of {key: value} (AND within, OR across); None = no filter."""
    if groups is None:
        return True
    return any(all(k in props and props[k] == v for k, v in g.items()) for g in groups)

def groups_of(filters, ftype):
    g = (filters or {}).get(ftype, {}).get("filters")
    return g or None                                   # [] behaves like no filter


class Oracle:
    def __init__(self, data_dir, scenario):
        self.s = scenario
        self.f_edge = groups_of(scenario["filters"], "edge")
        self.f_node = groups_of(scenario["filters"], "node")
        self.edges = {ds: load(os.path.join(data_dir, f"{ds}_edges.geojson")) for ds in ("ds1", "ds2")}
        self.nodes = {ds: load(os.path.join(data_dir, f"{ds}_nodes.geojson")) for ds in ("ds1", "ds2")}
        self.lines = {ds: [[xy(c) for c in e["geometry"]["coordinates"]] for e in self.edges[ds]]
                      for ds in ("ds1", "ds2")}

    # eligibility ---------------------------------------------------------------
    def edge_ok(self, props):
        return matches(props, self.f_edge)

    def node_ok(self, ds, node):
        """A node may merge attributes when it lies on an edge passing the edge
        filter (any node, if no edge filter) and matches the node filter."""
        p = xy(node["geometry"]["coordinates"])
        on_edges = [e for e, l in zip(self.edges[ds], self.lines[ds]) if dist_to_line(p, l) <= ON]
        if self.f_edge is None:
            edge_side = True
        else:
            edge_side = any(self.edge_ok(e["properties"]) for e in on_edges)
        return edge_side and matches(node["properties"], self.f_node)

    def witness(self, ds2_edge):
        """The DS1 edge a DS2 duplicate duplicates: same type group, nearest by mean vertex distance."""
        l2 = [xy(c) for c in ds2_edge["geometry"]["coordinates"]]
        t = type_group(ds2_edge["properties"])
        best = None
        for e, l in zip(self.edges["ds1"], self.lines["ds1"]):
            if type_group(e["properties"]) != t:
                continue
            d = sum(dist_to_line(p, l) for p in l2) / len(l2)
            if best is None or d < best[0]:
                best = (d, e)
        return best[1] if best else None

    # R2: fate of each DS2 input edge -------------------------------------------
    def expected_fates(self, baseline_fates):
        """baseline_fates: {(dataset, input_id): fate}. Returns
        {input_id: (expected, reason)} for DS2 edges, expected in {'removed','not removed','kept'}."""
        out = {}
        explicit = {name: fate for name, fate in self.s.get("expect", [])}
        for e in self.edges["ds2"]:
            p = e["properties"]; iid = str(p["_id"]); name = p.get("name", "")
            if name in explicit:
                want = explicit[name]
                if want == "removed" and self.f_edge is not None:
                    w = self.witness(e)
                    if not (self.edge_ok(p) and w is not None and self.edge_ok(w["properties"])):
                        out[iid] = ("not removed", "a duplicate under these settings, but the edge filter "
                                                   "keeps it — both stay")
                        continue
                out[iid] = (want, "scenario expectation")
                continue
            if not self.s.get("dedup_rule", True):
                continue
            base = baseline_fates.get(("DS2", iid))
            if base != "removed":
                out[iid] = ("not removed", f"baseline {base}: a filter never removes more")
                continue
            w = self.witness(e)
            ok2, ok1 = self.edge_ok(p), (w is not None and self.edge_ok(w["properties"]))
            if ok2 and ok1:
                out[iid] = ("removed", "duplicate; both copies pass the edge filter")
            else:
                failing = [x for x, ok in (("DS2 copy", ok2), ("DS1 copy", ok1)) if not ok]
                who = "both copies fail" if len(failing) == 2 else f"the {failing[0]} fails"
                out[iid] = ("not removed", f"duplicate, but {who} the edge filter — both stay")
        return out

    # R3: attribute merge on merged nodes --------------------------------------
    def attribute_expectations(self, out_nodes, proximity):
        """For each DS2 node carrying tags that merged into an output node, which
        DS2-only tags must be present (merged) or absent (withheld).
        Returns list of dicts: coord, merged_into (output props), rows[(key, value, expect, got)]."""
        onodes = [(xy(n["geometry"]["coordinates"]), n) for n in out_nodes]
        ds1_at = {}
        for n in self.nodes["ds1"]:
            ds1_at[xy(n["geometry"]["coordinates"])] = n
        events = []
        for n2 in self.nodes["ds2"]:
            tags = {k: v for k, v in n2["properties"].items() if k != "_id"}
            if not tags:
                continue
            c2 = xy(n2["geometry"]["coordinates"])
            # the output node that audited this DS2 node's values
            target = None
            for c, o in onodes:
                if math.hypot(c[0] - c2[0], c[1] - c2[1]) > proximity + ON:
                    continue
                op = o["properties"]
                if any(str(op.get(f"ext:union_audit_{k}", "")).endswith(f"-{v}") or f"-{v}," in str(op.get(f"ext:union_audit_{k}", ""))
                       for k, v in tags.items()):
                    target = (c, o); break
            if target is None:
                events.append(dict(coord=n2["geometry"]["coordinates"], target=None, tags=tags, rows=[]))
                continue
            c, o = target
            n1 = min(ds1_at.items(), key=lambda kv: math.hypot(kv[0][0] - c[0], kv[0][1] - c[1]))
            if math.hypot(n1[0][0] - c[0], n1[0][1] - c[1]) > SAME:
                n1 = None
            n1 = n1[1] if n1 else None
            elig = n1 is not None and self.node_ok("ds1", n1) and self.node_ok("ds2", n2)
            rows = []
            for k, v in tags.items():
                if n1 is not None and k in n1["properties"]:
                    continue                                   # DS1 authoritative: never merged
                got = o["properties"].get(k) == v
                rows.append((k, v, "merged" if elig else "withheld", "merged" if got else "withheld"))
            audited = all(f"ext:union_audit_{k}" in o["properties"] for k in tags)
            events.append(dict(coord=o["geometry"]["coordinates"], target=o, tags=tags, rows=rows,
                               eligible=elig, audited=audited))
        return events
