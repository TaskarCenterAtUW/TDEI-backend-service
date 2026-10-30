"""
Config validation suite: every check content.union5_validate_config makes,
exercised with a config that must be rejected (and the message it must give)
or accepted (and what the effective config must contain).

    python3 harness/config_cases.py --sql > runs/config_cases.sql     # SQL that validates each config
    psql "$PGCONN" -qAt -f runs/config_cases.sql > runs/config_results.txt 2>&1
    python3 harness/config_cases.py --check runs/config_results.txt   # PASS/FAIL per case

run_harness.sh does all three (`./run_harness.sh local validate`). Validation
reads no data, so this needs only the union deployed.

Each case: (name, config, expectation)
  ("error", "fragment")         rejected, the message contains fragment
  ("ok", [(path, op, value)])   accepted; the effective config at path (dotted;
                                [n] indexes a list) compares to value. op: ==, len, absent
"""
import argparse, json, re, sys

R = lambda **kw: kw                                  # a rule, for readability
def many(n): return [R(name=f"r{i}", **{"from": "crossing", "to": "sidewalk"}, operation="geometry") for i in range(n)]

CASES = [
    # ── structure and modes ─────────────────────────────────────────────────
    ("not an object",            [],                                                     ("error", "config: must be a JSON object")),
    ("unknown section",          {"snapp": {"mode": "default"}},                          ("error", 'unknown section "snapp"')),
    ("mode missing",             {"snap": {"proximity": 1}},                              ("error", "config.snap.mode: is required")),
    ("mode not known",           {"dedup": {"mode": "on"}},                               ("error", "config.dedup.mode: expected one of off, default, custom")),
    ("off takes nothing else",   {"connect": {"mode": "off", "proximity": 1}},           ("error", "config.connect: mode off takes no other settings")),
    ("default takes no rules",   {"snap": {"mode": "default", "rules": []}},             ("error", 'config.snap: unknown setting "rules"')),
    ("extensions type not an object", {"extensions": {"mode": "default", "lines": True}},
                                 ("error", 'config.extensions.lines: must be an object, e.g. {"enabled": true, "proximity": 0.5, "min_overlap": 0.7}')),
    ("extensions enabled not boolean", {"extensions": {"mode": "default", "zones": {"enabled": "yes"}}},
                                 ("error", "config.extensions.zones.enabled: must be true or false")),
    ("extensions all disabled",  {"extensions": {"mode": "default", "points": {"enabled": False}, "lines": {"enabled": False},
                                                 "polygons": {"enabled": False}, "zones": {"enabled": False}}},
                                 ("error", "config.extensions: points, lines, polygons and zones are all disabled")),
    ("extensions off takes no types", {"extensions": {"mode": "off", "points": {"enabled": True}}},
                                 ("error", "config.extensions")),
    ("polygons take no proximity", {"extensions": {"mode": "default", "polygons": {"proximity": 1}}},
                                 ("error", 'config.extensions.polygons: unknown setting "proximity"')),
    ("old flat line_overlap",    {"extensions": {"mode": "default", "line_overlap": 0.7}},
                                 ("error", "config.extensions.line_overlap: each type has its own block now — use extensions.lines.min_overlap")),
    ("old flat proximity",       {"extensions": {"mode": "default", "proximity": 1}},
                                 ("error", "config.extensions.proximity: each type has its own block now — use extensions.points.proximity")),
    ("max_angle 0",              {"dedup": {"mode": "default", "max_angle": 0}},
                                 ("error", "config.dedup.max_angle: must be greater than 0")),
    ("max_angle above 90",       {"dedup": {"mode": "custom", "rules": [R(**{"from": "sidewalk", "to": "sidewalk"}, max_angle=91)]}},
                                 ("error", "config.dedup.rules[1].max_angle: must be greater than 0 and at most 90")),
    ("lines proximity 0",        {"extensions": {"mode": "default", "lines": {"proximity": 0}}},
                                 ("error", "config.extensions.lines.proximity: must be greater than 0")),
    ("points proximity above 5", {"extensions": {"mode": "default", "points": {"proximity": 6}}},
                                 ("error", "config.extensions.points.proximity: must be greater than 0 and at most 5")),
    ("zones min_overlap above 1", {"extensions": {"mode": "default", "zones": {"min_overlap": 1.5}}},
                                 ("error", "config.extensions.zones.min_overlap: must be greater than 0 and at most 1")),
    ("extensions has no custom", {"extensions": {"mode": "custom"}},                     ("error", "config.extensions.mode: expected one of off, default")),
    ("section not an object",    {"snap": "default"},                                    ("error", "config.snap: must be a JSON object")),
    ("custom snap needs a rule", {"snap": {"mode": "custom"}},                           ("error", "custom mode needs at least one rule")),
    ("custom dedup needs rules", {"dedup": {"mode": "custom", "rules": []}},             ("error", "config.dedup.rules: custom mode needs a non-empty list")),
    ("custom connect needs rules", {"connect": {"mode": "custom"}},                      ("error", "config.connect.rules: custom mode needs a non-empty list")),
    ("at most 50 rules",         {"connect": {"mode": "custom", "rules": many(51)}},     ("error", "at most 50 rules per stage (got 51)")),
    # ── ranges (V2) ─────────────────────────────────────────────────────────
    ("snap proximity above 5",   {"snap": {"mode": "default", "proximity": 6}},          ("error", "config.snap.proximity: must be at least 0 and at most 5")),
    ("snap proximity negative",  {"snap": {"mode": "default", "proximity": -1}},         ("error", "config.snap.proximity: must be at least 0")),
    ("dedup proximity zero",     {"dedup": {"mode": "default", "proximity": 0}},         ("error", "config.dedup.proximity: must be greater than 0")),
    ("connect proximity text",   {"connect": {"mode": "default", "proximity": "1"}},     ("error", "config.connect.proximity: must be a number")),
    ("min_overlap above 1",      {"dedup": {"mode": "default", "min_overlap": 1.5}},     ("error", "config.dedup.min_overlap: must be greater than 0 and at most 1")),
    ("rule proximity above 5",   {"connect": {"mode": "custom", "rules": [R(name="a", **{"from": "crossing", "to": "road"}, operation="point", proximity=9)]}},
                                 ("error", "config.connect.rules[1].proximity: must be greater than 0 and at most 5")),
    # ── enumerations ────────────────────────────────────────────────────────
    ("anchor not known",         {"snap": {"mode": "default", "anchor": "ds3"}},         ("error", "config.snap.anchor: expected one of ds1, ds2")),
    ("attributes not known",     {"snap": {"mode": "default", "attributes": "keep"}},    ("error", "config.snap.attributes: expected one of merge, anchor_only")),
    ("operation missing",        {"connect": {"mode": "custom", "rules": [R(name="a", **{"from": "crossing", "to": "road"})]}},
                                 ("error", "config.connect.rules[1].operation: is required")),
    ("operation not known",      {"connect": {"mode": "custom", "rules": [R(name="a", **{"from": "crossing", "to": "road"}, operation="split")]}},
                                 ("error", "config.connect.rules[1].operation: expected one of point, geometry")),
    ("coincident not boolean",   {"snap": {"mode": "custom", "coincident": "yes"}},      ("error", "config.snap.coincident: must be true or false")),
    # ── rule fields and names ───────────────────────────────────────────────
    ("unknown rule field",       {"snap": {"mode": "custom", "rules": [R(type="crossing", distance=1)]}},
                                 ("error", 'config.snap.rules[1]: unknown setting "distance"')),
    ("connect name required",    {"connect": {"mode": "custom", "rules": [R(**{"from": "crossing", "to": "road"}, operation="point")]}},
                                 ("error", "config.connect.rules[1].name: a rule name is required")),
    ("connect names unique",     {"connect": {"mode": "custom", "rules": [R(name="x", **{"from": "crossing", "to": "road"}, operation="point"),
                                                                          R(name="x", **{"from": "crossing", "to": "sidewalk"}, operation="geometry")]}},
                                 ("error", 'config.connect.rules[2].name: "x" is used by an earlier connect rule')),
    ("name with a comma",        {"connect": {"mode": "custom", "rules": [R(name="a,b", **{"from": "crossing", "to": "road"}, operation="point")]}},
                                 ("error", "without commas")),
    ("rule not an object",       {"dedup": {"mode": "custom", "rules": ["crossing"]}},  ("error", "config.dedup.rules[1]: must be a JSON object")),
    # ── types (V3) ──────────────────────────────────────────────────────────
    ("type required",            {"snap": {"mode": "custom", "rules": [R(name="a")]}},  ("error", "config.snap.rules[1].type: a type is required")),
    ("unknown type name",        {"connect": {"mode": "custom", "rules": [R(name="a", **{"from": "sidewalks", "to": "road"}, operation="point")]}},
                                 ("error", 'unknown type name "sidewalks"')),
    ("empty tag match",          {"dedup": {"mode": "custom", "rules": [R(**{"from": {}, "to": "road"})]}},
                                 ("error", "an empty tag match {} would match everything")),
    ("empty type list",          {"dedup": {"mode": "custom", "rules": [R(**{"from": [], "to": "road"})]}},
                                 ("error", "an empty list of types matches nothing")),
    ("tag value a number",       {"snap": {"mode": "custom", "rules": [R(type={"highway": 5})]}},
                                 ("error", "config.snap.rules[1].type.highway: a tag value must be a string")),
    ("type list of numbers",     {"snap": {"mode": "custom", "rules": [R(type=[1])]}},  ("error", "each list entry must be a type name or a tag match")),
    # ── node_tags / edge_tags (V1) ─────────────────────────────────────────
    ("tags replaced",            {"connect": {"mode": "custom", "rules": [R(name="a", **{"from": "crossing", "to": "road"}, operation="point", tags={"kerb": "lowered"})]}},
                                 ("error", "config.connect.rules[1].tags: a connect rule takes node_tags (for the junction node) and edge_tags (for the connector edge) instead of tags")),
    ("node_tags not an object",  {"connect": {"mode": "custom", "rules": [R(name="a", **{"from": "crossing", "to": "road"}, operation="point", node_tags=["x"])]}},
                                 ("error", "config.connect.rules[1].node_tags: tags must be an object")),
    ("edge_tags not an object",  {"connect": {"mode": "custom", "rules": [R(name="a", **{"from": "crossing", "to": "road"}, operation="point", edge_tags="zebra")]}},
                                 ("error", "config.connect.rules[1].edge_tags: tags must be an object")),
    ("reserved node tag _id",    {"connect": {"mode": "custom", "rules": [R(name="a", **{"from": "crossing", "to": "road"}, operation="point", node_tags={"_id": "1"})]}},
                                 ("error", "config.connect.rules[1].node_tags._id: this key is reserved")),
    ("reserved edge tag _u_id",  {"connect": {"mode": "custom", "rules": [R(name="a", **{"from": "crossing", "to": "road"}, operation="point", edge_tags={"_u_id": "1"})]}},
                                 ("error", "config.connect.rules[1].edge_tags._u_id: this key is reserved")),
    ("reserved tag ext:union_",  {"connect": {"mode": "custom", "rules": [R(name="a", **{"from": "crossing", "to": "road"}, operation="point", edge_tags={"ext:union_rule": "x"})]}},
                                 ("error", "this key is reserved")),
    ("nested node tag value",    {"connect": {"mode": "custom", "rules": [R(name="a", **{"from": "crossing", "to": "road"}, operation="point", node_tags={"kerb": {"a": 1}})]}},
                                 ("error", "config.connect.rules[1].node_tags.kerb: a tag value must be a string, number or boolean")),
    ("nested edge tag value",    {"connect": {"mode": "custom", "rules": [R(name="a", **{"from": "crossing", "to": "road"}, operation="point", edge_tags={"surface": ["asphalt"]})]}},
                                 ("error", "config.connect.rules[1].edge_tags.surface: a tag value must be a string, number or boolean")),

    # ── accepted configs, and what they expand to ───────────────────────────
    ("nothing configured",       {},  ("ok", [("snap.mode", "==", "off"), ("dedup.mode", "==", "off"),
                                              ("connect.mode", "==", "off"), ("extensions.mode", "==", "off"),
                                              ("snap.rules", "len", 0), ("connect.rules", "len", 0)])),
    ("sections off",             {"snap": {"mode": "off"}, "dedup": {"mode": "off"}, "connect": {"mode": "off"}, "extensions": {"mode": "off"}},
                                 ("ok", [("snap.coincident", "==", False), ("dedup.rules", "len", 0)])),
    ("default snap",             {"snap": {"mode": "default"}},
                                 ("ok", [("snap.coincident", "==", True), ("snap.rules", "len", 5),
                                         ("snap.rules[1].name", "==", "crossing"), ("snap.rules[1].proximity", "==", 0.5),
                                         ("snap.rules[5].name", "==", "other"), ("snap.rules[1].anchor", "==", "ds1")])),
    ("default snap 2 m, no coincident", {"snap": {"mode": "default", "proximity": 2, "coincident": False}},
                                 ("ok", [("snap.coincident", "==", False), ("snap.rules[3].proximity", "==", 2)])),
    ("default dedup",            {"dedup": {"mode": "default", "proximity": 1}},
                                 ("ok", [("dedup.rules", "len", 5), ("dedup.rules[2].from", "==", "pedestrian"),
                                         ("dedup.rules[2].min_overlap", "==", 0.8), ("dedup.rules[2].proximity", "==", 1)])),
    ("default connect",          {"connect": {"mode": "default"}},
                                 ("ok", [("connect.rules", "len", 6), ("connect.rules[1].name", "==", "road-crossing"),
                                         ("connect.rules[1].operation", "==", "point"),
                                         ("connect.rules[2].name", "==", "crossing-sidewalk"),
                                         ("connect.rules[6].name", "==", "crossing-crossing"),
                                         ("connect.rules[4].proximity", "==", 0.5)])),
    ("default dedup max_angle 30", {"dedup": {"mode": "default"}},
                                 ("ok", [("dedup.rules[1].max_angle", "==", 30), ("dedup.rules[5].max_angle", "==", 30)])),
    ("dedup max_angle per rule", {"dedup": {"mode": "custom", "max_angle": 20, "rules": [
                                    R(name="a", **{"from": "sidewalk", "to": "sidewalk"}), R(name="b", **{"from": "road", "to": "road"}, max_angle=60)]}},
                                 ("ok", [("dedup.rules[1].max_angle", "==", 20), ("dedup.rules[2].max_angle", "==", 60)])),
    ("default extensions",       {"extensions": {"mode": "default"}},
                                 ("ok", [("extensions.points", "==", {"enabled": True, "proximity": 0.5}),
                                         ("extensions.lines", "==", {"enabled": True, "proximity": 0.5, "min_overlap": 0.7}),
                                         ("extensions.polygons", "==", {"enabled": True, "min_overlap": 0.7}),
                                         ("extensions.zones", "==", {"enabled": True, "min_overlap": 0.7})])),
    ("each type its own values", {"extensions": {"mode": "default", "points": {"proximity": 1}, "lines": {"proximity": 3, "min_overlap": 0.5},
                                                 "polygons": {"enabled": False}, "zones": {"min_overlap": 0.9}}},
                                 ("ok", [("extensions.points.proximity", "==", 1), ("extensions.lines.proximity", "==", 3),
                                         ("extensions.lines.min_overlap", "==", 0.5), ("extensions.polygons.enabled", "==", False),
                                         ("extensions.zones.min_overlap", "==", 0.9), ("extensions.zones.enabled", "==", True)])),
    ("custom snap: others, anchors", {"snap": {"mode": "custom", "proximity": 1, "anchor": "ds2", "others": 0.5,
                                               "rules": [R(type={"highway": "footway", "footway": ["sidewalk", "crossing"]}),
                                                         R(name="kerbs", type={"kerb": "*"}, proximity=0, anchor="ds1")]}},
                                 ("ok", [("snap.rules", "len", 2), ("snap.rules[1].name", "==", "snap-1"),
                                         ("snap.rules[1].anchor", "==", "ds2"), ("snap.rules[1].proximity", "==", 1),
                                         ("snap.rules[2].proximity", "==", 0), ("snap.rules[2].anchor", "==", "ds1"),
                                         ("snap.others.proximity", "==", 0.5), ("snap.coincident", "==", False)])),
    ("custom connect: node_tags, edge_tags, lists of types", {"connect": {"mode": "custom", "proximity": 1, "rules": [
                                    R(name="kerbed", **{"from": "crossing", "to": [{"footway": "sidewalk"}, "living_street"]},
                                      operation="geometry", node_tags={"barrier": "kerb", "survey_batch": 2026, "verified": True},
                                      edge_tags={"crossing:markings": "zebra", "width": 2.5})]}},
                                 ("ok", [("connect.rules[1].proximity", "==", 1), ("connect.rules[1].node_tags.survey_batch", "==", 2026),
                                         ("connect.rules[1].edge_tags.crossing:markings", "==", "zebra"),
                                         ("connect.rules[1].edge_tags.width", "==", 2.5),
                                         ("connect.rules[1].tags", "absent", None)])),
    ("custom connect: no tag sets", {"connect": {"mode": "custom", "rules": [R(name="plain", **{"from": "crossing", "to": "road"}, operation="point")]}},
                                 ("ok", [("connect.rules[1].node_tags", "==", {}), ("connect.rules[1].edge_tags", "==", {})])),
    ("default connect: empty tag sets", {"connect": {"mode": "default"}},
                                 ("ok", [("connect.rules[2].node_tags", "==", {}), ("connect.rules[2].edge_tags", "==", {})])),
    ("50 rules is the limit",    {"connect": {"mode": "custom", "rules": many(50)}},     ("ok", [("connect.rules", "len", 50)])),
]


def sql():
    out = ["\\set ON_ERROR_STOP off", "SET client_min_messages = notice;"]
    for name, cfg, _ in CASES:
        lit = json.dumps(cfg).replace("'", "''")
        nm = name.replace("'", "''")
        out.append(f"""DO $$ DECLARE eff jsonb; BEGIN
  eff := content.union5_validate_config('{lit}'::jsonb);
  RAISE NOTICE 'CASE|{nm}|ok|%', eff::text;
EXCEPTION WHEN others THEN RAISE NOTICE 'CASE|{nm}|error|%', SQLERRM; END $$;""")
    return "\n".join(out)


def get(obj, path):
    for part in re.findall(r"[^.\[\]]+|\[\d+\]", path):
        if part.startswith("["):
            obj = obj[int(part[1:-1]) - 1]
        else:
            obj = obj[part]
    return obj


def check(results_file):
    got = {}
    for line in open(results_file):
        m = re.search(r"CASE\|(.*?)\|(ok|error)\|(.*)$", line.rstrip("\n"))
        if m: got[m.group(1)] = (m.group(2), m.group(3))
    n_fail = 0
    print("Config validation (content.union5_validate_config):")
    for name, cfg, want in CASES:
        res = got.get(name)
        problems = []
        if res is None:
            problems.append("no result (did the SQL run? is union_5_0.sql deployed?)")
        elif want[0] == "error":
            if res[0] != "error": problems.append("accepted, but must be rejected")
            elif want[1] not in res[1]: problems.append(f"message lacks '{want[1]}': {res[1]}")
        else:
            if res[0] != "ok":
                problems.append(f"rejected: {res[1]}")
            else:
                eff = json.loads(res[1])
                for path, op, val in want[1]:
                    if op == "absent":
                        try:
                            get(eff, path); problems.append(f"{path} present, want it absent")
                        except (KeyError, IndexError, TypeError):
                            pass
                        continue
                    try:
                        v = get(eff, path)
                    except (KeyError, IndexError, TypeError):
                        problems.append(f"{path} missing"); continue
                    if op == "len" and len(v) != val: problems.append(f"len({path}) = {len(v)}, want {val}")
                    if op == "==" and v != val: problems.append(f"{path} = {v!r}, want {val!r}")
        status = "FAIL" if problems else "ok  "
        n_fail += bool(problems)
        print(f"  {status}  {name}" + ("" if problems or want[0] == "ok" else f"  →  {res[1]}"))
        for p in problems:
            print(f"      x {p}")
    print(f"\n{len(CASES) - n_fail}/{len(CASES)} config cases passed")
    return n_fail


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sql", action="store_true", help="print the SQL that validates every case")
    g.add_argument("--check", metavar="RESULTS", help="check the psql output of that SQL")
    a = ap.parse_args()
    if a.sql:
        print(sql())
    else:
        sys.exit(1 if check(a.check) else 0)
