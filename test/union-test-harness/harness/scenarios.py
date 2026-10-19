"""
Filter scenarios — the catalogue of entity_filters runs, and the single source
of truth for what each is expected to do.

Every scenario runs at a proximity and is compared with the BASELINE run at the
same proximity (runs/prox_<p>, no filters). Expectations are derived from the
union's filter rules rather than hard-coded per case, so they stay correct when
cases are added (see filter_oracle.py):

  R1 connectivity   filters never change connectivity: every junction/route
                    assertion of the default run still holds, the same DS2 nodes
                    snap as in the baseline, routability and integrity pass.
  R2 duplicates     a DS2 edge the baseline removed as a duplicate is removed
                    only if it AND its DS1 witness pass the edge filter;
                    otherwise both copies stay. A filter never removes anything
                    the baseline kept.
  R3 attributes     on a merged node, a DS2-only tag is merged only if BOTH
                    nodes are eligible (on an edge passing the edge filter, and
                    matching the node filter if one is given). The DS2 value is
                    audited (ext:union_audit_*) either way.

Filter syntax (your union's entity_filters): within a group key/values are
AND-ed, groups are OR-ed ({"a":1} matches when properties contain a=1).

Duplicate-setting scenarios change what counts as a duplicate on purpose, so
R2 is replaced by explicit per-edge expectations ("expect").
"""

SIDEWALK = {"highway": "footway", "footway": "sidewalk"}
CROSSING = {"highway": "footway", "footway": "crossing"}

SCENARIOS = [
    dict(name="f_sidewalk", proximity=3,
         title="Edge filter: sidewalks only",
         filters={"edge": {"filters": [SIDEWALK]}},
         description="Only sidewalk duplicates are removed. Plain footway (K), living street (F) and "
                     "crossing (M) duplicates stay. Nodes on crossings/roads don't merge attributes."),
    dict(name="f_crossing", proximity=3,
         title="Edge filter: crossings only",
         filters={"edge": {"filters": [CROSSING]}},
         description="Only the crossing duplicate (M) is removed; every sidewalk duplicate stays. "
                     "Road x crossing (C) is still split — structural repair is not filtered."),
    dict(name="f_sidewalk_or_crossing", proximity=3,
         title="Edge filter: sidewalk OR crossing (two groups)",
         filters={"edge": {"filters": [SIDEWALK, CROSSING]}},
         description="Groups are OR-ed: sidewalk and crossing duplicates removed; K and F stay."),
    dict(name="f_and_surface", proximity=3,
         title="Edge filter: sidewalk AND surface=concrete",
         filters={"edge": {"filters": [dict(SIDEWALK, surface="concrete")]}},
         description="Pairs are AND-ed: only V's DS1 copy matches. V's DS2 copy (asphalt) does not, "
                     "so V keeps both copies — a duplicate needs BOTH sides eligible. Nothing else is removed."),
    dict(name="f_or_surface", proximity=3,
         title="Edge filter: surface=concrete OR surface=asphalt",
         filters={"edge": {"filters": [{"surface": "concrete"}, {"surface": "asphalt"}]}},
         description="Both V copies match different groups, so V is deduplicated; no other edge has a surface."),
    dict(name="n_kerb", proximity=3,
         title="Node filter: kerbs only",
         filters={"node": {"filters": [{"barrier": "kerb"}]}},
         description="Edges dedupe as default. Attributes merge only onto kerbs: W gains tactile_paving; "
                     "A's ext:surface and N's ext:lit are audited but not merged."),
    dict(name="n_nomatch", proximity=3,
         title="Node filter matching nothing (kerb=raised)",
         filters={"node": {"filters": [{"kerb": "raised"}]}},
         description="No node is eligible: no DS2 attribute is merged anywhere, every DS2 value is still "
                     "audited, and every node still connects."),
    dict(name="dup_u_overlap50", proximity=1,
         title="Duplicate settings: buffer 3 m, overlap 50%",
         filters={"edge": {"duplicate_buffer_width": 3, "duplicate_overlap_percentage": 50}},
         dedup_rule=False, expect=[("U: partly overlapping sidewalk", "removed"),
                                   ("D: sidewalk to kerb", "removed")],
         description="~53% of U's DS2 sidewalk lies in the 3 m corridor: at a 50% threshold it is a duplicate. "
                     "Side effect: D's DS2 sidewalk, 2.5 m from the DS1 one, is inside a 3 m corridor too "
                     "and is removed with its kerb — the corridor is wider than the kerb spacing."),
    dict(name="dup_u_overlap60", proximity=1,
         title="Duplicate settings: buffer 3 m, overlap 60%",
         filters={"edge": {"duplicate_buffer_width": 3, "duplicate_overlap_percentage": 60}},
         dedup_rule=False, expect=[("U: partly overlapping sidewalk", "kept"),
                                   ("D: sidewalk to kerb", "removed")],
         description="Same corridor, 60% threshold: U's DS2 sidewalk is not a duplicate and stays whole. "
                     "D's DS2 sidewalk lies wholly in the corridor, so it is still removed."),
]

BY_NAME = {s["name"]: s for s in SCENARIOS}


def combine(names):
    """One scenario from several: their filters applied together in ONE run.

    Per file type, filter groups are concatenated (so they OR, exactly as groups
    do in entity_filters); different file types each apply (an edge filter and a
    node filter both hold). Duplicate settings are merged; the same setting with
    two different values, or two different proximities, is an error.
    Explicit fate expectations are kept, but only where the combined edge filter
    still lets that edge be removed (see filter_oracle.expected_fates)."""
    parts = []
    for n in names:
        if n not in BY_NAME:
            raise KeyError(f'unknown scenario "{n}" — known: {", ".join(BY_NAME)}')
        parts.append(BY_NAME[n])
    if len(parts) == 1:
        return parts[0]
    prox = {p["proximity"] for p in parts}
    if len(prox) > 1:
        raise ValueError(f"cannot combine scenarios at different proximities: "
                         + ", ".join(f'{p["name"]} @ {p["proximity"]:g} m' for p in parts))
    filters = {}
    for p in parts:
        for ftype, cfg in p["filters"].items():
            into = filters.setdefault(ftype, {})
            for key, val in cfg.items():
                if key == "filters":
                    into.setdefault("filters", [])
                    into["filters"] += [g for g in val if g not in into["filters"]]
                elif key in into and into[key] != val:
                    raise ValueError(f"cannot combine: {ftype}.{key} is {into[key]} in one scenario and {val} in another")
                else:
                    into[key] = val
    return dict(name="+".join(p["name"] for p in parts), proximity=prox.pop(),
                title=" + ".join(p["title"] for p in parts),
                filters=filters,
                dedup_rule=all(p.get("dedup_rule", True) for p in parts),
                expect=[e for p in parts for e in p.get("expect", [])],
                description="Combined in one run: " + " | ".join(p["description"] for p in parts),
                parts=[p["name"] for p in parts])


def get(name):
    """A scenario by run name: 'f_crossing' or a combination 'f_crossing+n_kerb'."""
    return combine(name.split("+"))


if __name__ == "__main__":
    # Used by run_harness.sh:  name <TAB> proximity <TAB> filters-json   (one line per run)
    #   <none>      list the scenarios
    #   .           every scenario, each its own run
    #   a           scenario a
    #   a b ...     ONE run with a, b, ... combined
    import json, sys
    args = sys.argv[1:]
    if not args:
        print("Scenarios (harness/scenarios.py):", file=sys.stderr)
        for s in SCENARIOS:
            print(f'  {s["name"]:22} @ {s["proximity"]:g} m  {s["title"]}', file=sys.stderr)
        print("\nUsage: filters <name>          one scenario\n"
              "       filters <name> <name>   one run, filters combined\n"
              "       filters .               every scenario, each separately", file=sys.stderr)
        sys.exit(2)
    try:
        runs = SCENARIOS if args == ["."] else [combine(args)]
    except (KeyError, ValueError) as e:
        print(e.args[0], file=sys.stderr); sys.exit(2)
    for s in runs:
        print(f'{s["name"]}\t{s["proximity"]:g}\t{json.dumps(s["filters"])}')
