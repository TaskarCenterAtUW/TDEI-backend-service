"""
Run profiles for the union 5.0 harness: one union configuration per run.

Every profile is a complete `config` object for content.tdei_union_dataset, so a
run is exactly what an API caller would send. Cases (harness/cases.py) say which
profiles they are checked under and what each one must produce.

    python3 harness/profiles.py                 # list profiles
    python3 harness/profiles.py default_1m      # print one profile's config (JSON)

Profile fields
  config      the union configuration sent with the run
  proximity   the distance case assertions resolve {1.0: …, 3.0: …} values with,
              and the reach of the routability checks (metres)
  coincident  True when the config joins coincident nodes (snap 'coincident');
              only then does the integrity check require one node per coordinate
  routability 'full' = preservation + junction checks (the default rule sets);
              'preserve' = only "nothing connected in an input is disconnected"
"""
import json, sys

def _default(p):
    return {"snap":       {"mode": "default", "proximity": p},
            "dedup":      {"mode": "default", "proximity": p},
            "connect":    {"mode": "default", "proximity": p},
            "extensions": {"mode": "default", "proximity": p}}

PROFILES = {
    "off": dict(
        title="Nothing configured: the output is DS1 + DS2, unchanged",
        config={}, proximity=1.0, coincident=False, routability="preserve"),
    "default_1m": dict(
        title="Every stage on its published default rule set, 1 m",
        config=_default(1), proximity=1.0, coincident=True, routability="full"),
    "default_3m": dict(
        title="Every stage on its published default rule set, 3 m",
        config=_default(3), proximity=3.0, coincident=True, routability="full"),
    "custom_snap": dict(
        title="Snap only: crossings 1.5 m, sidewalks 3 m, other types 0.5 m, coincident on",
        config={"snap": {"mode": "custom", "coincident": True, "others": 0.5,
                         "rules": [{"name": "crossings", "type": "crossing", "proximity": 1.5},
                                   {"name": "sidewalks", "type": "sidewalk", "proximity": 3}]}},
        proximity=3.0, coincident=True, routability="preserve"),
    "custom_anchor": dict(
        title="DS2 as the anchor for sidewalks (snap and dedup); crossings anchor_only",
        config={"snap": {"mode": "custom",
                         "rules": [{"name": "sidewalks-ds2", "type": "sidewalk", "proximity": 1, "anchor": "ds2"},
                                   {"name": "crossings-ds1", "type": "crossing", "proximity": 1,
                                    "attributes": "anchor_only"}]},
                "dedup": {"mode": "custom",
                          "rules": [{"name": "sidewalks-ds2", "from": "sidewalk", "to": "sidewalk",
                                     "proximity": 1, "anchor": "ds2"}]}},
        proximity=1.0, coincident=False, routability="preserve"),
    "connect_only": dict(
        title="Connect on its default rules (1 m) with snap and dedup off: stages are independent",
        config={"connect": {"mode": "default", "proximity": 1}},
        proximity=1.0, coincident=False, routability="preserve"),
    "custom_extensions": dict(
        title="Extensions only: Points join within 0.5 m, Lines use their own 2.5 m buffer",
        config={"extensions": {"mode": "default", "proximity": 0.5, "line_proximity": 2.5}},
        proximity=0.5, coincident=False, routability="preserve"),
    "ext_lines_polygons": dict(
        title="Extensions at 3 m with only Lines and Polygons on (points and zones off)",
        config={"extensions": {"mode": "default", "proximity": 3, "points": False, "lines": True,
                               "polygons": True, "zones": False}},
        proximity=3.0, coincident=False, routability="preserve"),
    "ext_points_zones": dict(
        title="Extensions at 3 m with only Points and Zones on (lines and polygons off)",
        config={"extensions": {"mode": "default", "proximity": 3, "points": True, "lines": False,
                               "polygons": False, "zones": True}},
        proximity=3.0, coincident=False, routability="preserve"),
    "custom_connect": dict(
        title="Ordered connect rules with node_tags / edge_tags; snap and dedup on their defaults at 0.5 m",
        config={"snap":  {"mode": "default", "proximity": 0.5},
                "dedup": {"mode": "default", "proximity": 0.5},
                "connect": {"mode": "custom", "rules": [
                    {"name": "kerbed-crossing", "from": "crossing", "to": "sidewalk",
                     "operation": "geometry", "proximity": 1.0,
                     "node_tags": {"barrier": "kerb", "kerb": "lowered", "survey_batch": "2026-10"},
                     "edge_tags": {"crossing:markings": "zebra", "surface": "asphalt", "verified": "yes"}},
                    {"name": "sidewalk-sidewalk", "from": "sidewalk", "to": "sidewalk",
                     "operation": "geometry", "proximity": 1.0},
                    {"name": "footway-sidewalk", "from": {"highway": "footway"}, "to": "sidewalk",
                     "operation": "geometry", "proximity": 1.0, "node_tags": {"ext:source": "rule2"}},
                    {"name": "road-crossing", "from": "crossing", "to": "road",
                     "operation": "point", "proximity": 0.3,
                     "node_tags": {"tactile_paving": "yes"}, "edge_tags": {"crossing:markings": "lines"}},
                    {"name": "duplicate-of-rule-1", "from": "crossing", "to": "sidewalk",
                     "operation": "geometry", "proximity": 1.0}]}},
        proximity=1.0, coincident=True, routability="preserve"),
}

# Run-level checks on the union's report output (report.json), per profile.
#   ("warning", text)                      some warning mentions text
#   ("no_warnings",)                       the report has no warnings
#   ("metric", stage, rule, key, op, n)    a rule's count compares to n (op: == >= <=)
#   ("effective_rules", stage, n)          the effective config lists n rules for that stage
REPORT_CHECKS = {
    "off": [("no_warnings",), ("effective_rules", "snap", 0), ("effective_rules", "connect", 0)],
    "default_1m": [("effective_rules", "connect", 6), ("effective_rules", "dedup", 5),
                   ("metric", "connect", "road-crossing", "nodes_created", ">=", 1),
                   ("metric", "connect", "crossing-sidewalk", "connectors", ">=", 4),
                   ("metric", "dedup", "pedestrian", "restored", ">=", 1)],
    "default_3m": [("effective_rules", "connect", 6), ("metric", "dedup", "pedestrian", "restored", ">=", 1)],
    "custom_snap": [("metric", "snap", "sidewalks", "snapped", ">=", 1),
                    ("metric", "snap", "crossings", "snapped", ">=", 1)],
    "custom_anchor": [("metric", "dedup", "sidewalks-ds2", "removed", ">=", 1)],
    "connect_only": [("effective_rules", "snap", 0), ("effective_rules", "dedup", 0), ("effective_rules", "connect", 6),
                     ("metric", "connect", "crossing-sidewalk", "junctions", ">=", 5)],
    "custom_extensions": [("effective_rules", "snap", 0), ("effective_rules", "dedup", 0), ("effective_rules", "connect", 0)],
    "custom_connect": [("warning", "duplicate-of-rule-1"),
                       ("metric", "connect", "kerbed-crossing", "junctions", ">=", 5),
                       ("metric", "connect", "footway-sidewalk", "skipped_taken", ">=", 1),
                       ("metric", "connect", "sidewalk-sidewalk", "junctions", ">=", 3),
                       ("metric", "connect", "duplicate-of-rule-1", "junctions", "==", 0)],
}

def get(name):
    if name not in PROFILES:
        sys.exit(f"unknown profile '{name}' — one of: {', '.join(PROFILES)}")
    return dict(PROFILES[name], name=name)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        for n, p in PROFILES.items():
            print(f"  {n:16} {p['title']}")
    else:
        print(json.dumps(get(sys.argv[1])["config"]))
