# The demo: one neighbourhood, two sources

The harness cases test one behaviour each, in its own small box. The demo shows
them working together. It is one realistic street corner, described by two
sources, and one picture of what the union makes of it. Use it to show someone
what the union does, or as a starting point for your own example.

```bash
./run_demo.sh local              # default rules at 1 m → runs/demo_default_1m/demo.png
./run_demo.sh local off          # the same data with nothing configured, for contrast
```

![The demo: input and output of the whole scene, with a close-up of each numbered spot](images/demo_default_1m.png)

## The scene

Harbor Blvd (`highway=secondary`, east–west) crosses Oak St
(`highway=residential`, north–south). The rest of the corner:

- **Sidewalks** run along both sides of both streets, 10 m from Harbor Blvd's
  centre and 8 m from Oak St's, meeting at the four corners.
- **Crossings** cross all four arms of the intersection, kerb to kerb.
- **Market Alley** (`highway=service`) leaves Harbor Blvd to the south.
- **Willow Lane** (`highway=living_street`), a shared street closed to traffic
  at Harbor Blvd, runs north.
- **Extension features:** buildings, a bus stop with a bench, a fence, a street
  lamp and a hydrant.

Two sources describe it:

| | Holds |
|---|---|
| **DS1 — City GIS** | roads, every sidewalk, two of the four crossings (west and north) with their kerbs, Willow Lane, a park path, a bus stop path, a kerb ramp on Oak St with no crossing yet |
| **DS2 — Field survey** | all four crossings with kerbs (the west one duplicates the city's), part of the south sidewalk and part of Harbor Blvd traced again a few decimetres off, a path from a shop door, a bench, a bin, a hydrant, a storey count for Harbor Hall, a new kiosk |

## What happens, spot by spot

The numbers match the picture. The scene is checked under `default_1m`, the
published default rule sets at 1 m; `run_demo.sh` reports PASS or FAIL for each.

| # | Spot | The sources | The union (default rules, 1 m) |
|---|---|---|---|
| 1 | Snap | survey sidewalk corner 0.57 m from the city's | joins the city corner, which records `ext:union_absorbed` |
| 2 | Duplicate sidewalk | 52 m of sidewalk traced again, 0.3 m off | removed: over 80 % lies within 1 m of the city's, running the same way |
| 3 | Duplicate crossing | both have the west crossing, 0.4 m apart; kerbs 0.45 m apart | survey copy removed with its kerbs; kerbs never move or merge |
| 4 | Duplicate road | Harbor Blvd traced again, nodes 0.36–0.39 m off | road nodes snap, then the copy joins the same two nodes as a city edge: removed |
| 5 | Crossing meets road | new crossing crosses Harbor Blvd, no shared node | `road-crossing`: one node where they cross, both split |
| 6 | Kerb short of sidewalk | crossing's kerb 0.4 m short of the sidewalk | `crossing-sidewalk`: node on the sidewalk, split, 0.4 m connector; the kerb stays |
| 7 | Kerb on sidewalk | crossing's kerb exactly on the sidewalk line | gap of zero: sidewalk split at the kerb, no connector |
| 8 | Same-spot kerbs | crossing ends exactly on the city's kerb ramp | coincident kerbs join; city `tactile_paving=no` stays, survey's `yes` audited |
| 9 | Overshoot | crossing ends 0.5 m beyond the sidewalk | they join where they cross; both split |
| 10 | T-junction | shop path ends 0.6 m short of the sidewalk | `pedestrian-pedestrian`: node on the sidewalk, split, 0.6 m connector |
| 11 | Living street | crossing ends 0.5 m short of Willow Lane, and crosses Harbor Blvd | `crossing-living-street` connector to Willow Lane's end; `road-crossing` node on the road; its south kerb splits the sidewalk |
| 12 | Not joined | sidewalk passes over Market Alley | no rule joins a sidewalk to a road: untouched. The city's own park-path junction stays as it was |
| 13 | Extensions | bench in both (0.36 m apart), survey bin, hydrant, storey count | bench merged (city position, `ext:condition` added); bin kept apart (other type); hydrant added; Harbor Hall gains `building:levels` |

Running `./run_demo.sh local off` shows the contrast: both copies of every
duplicate stay, and every gap stays open.

## The picture (`runs/demo_<profile>/demo.png`)

- **Top:** the whole scene twice, drawn like a street map (blocks grey, roads
  white, the living street sand-coloured). Left is the input: DS1 in blue,
  DS2 in orange dashes. Right is the union's output: edges in pink, nodes as
  green dots, kerbs as purple squares, connectors in purple, removed
  duplicates as a faint red band.
- **Below:** a close-up of each numbered spot. The input is drawn faint
  underneath and the output on top. Each close-up says what the sources hold,
  what the union did, and PASS or FAIL. Most of the action is under a metre,
  invisible at full scale, which is why the close-ups are there.
- **Legend:** in the last tile.

`docs/images/demo_default_1m.png` and `demo_off.png` are reference copies.

## The raw files

`demo/data/` holds what a TDEI upload would hold: the OSW 0.3 GeoJSON files for
each source, one feature per line.
- `ds1_nodes`, `ds1_edges`, `ds1_zones`, `ds1_points`, `ds1_lines` and
  `ds1_polygons` hold DS1, the City GIS.
- `ds2_*.geojson`, the same six files, hold DS2, the Field survey.
- `scenarios.geojson` holds the numbered spots and their explanations.

To show one behaviour on its own, copy the few features of that spot (and their
nodes) into a new pair of files. Or upload `demo/data` through TDEI as two
datasets and run the real job on them. The coordinates are near Albany, Oregon,
about 500 m east of the harness test grid.

## Changing the scene

The scene is written in `demo/scene.py`, in metres around the intersection.
- edges with their names and tags;
- tagged nodes (kerbs);
- extension features;
- the numbered spots with their text and checks.

After editing it:

```bash
BUILD=1 ./run_demo.sh local      # rebuild demo/data from scene.py, then run
```

The checks use the vocabulary described at the top of `scene.py`, for example
`("connector", (x, y), rule)` or `("node", (x, y), {key: value})`.
`demo/check_demo.py` runs them.

| File | Purpose |
|---|---|
| `run_demo.sh` | the one command: load `demo/data` as `demo-ds1` / `demo-ds2`, union, export, check, draw |
| `demo/scene.py` | the scene and the numbered spots (the single source of truth) |
| `demo/build_demo.py` | writes and validates `demo/data/*.geojson` from the scene |
| `demo/check_demo.py` | checks a run against the numbered spots; writes `demo_results.geojson` |
| `demo/demo_sheet.py` | draws `demo.png` |
| `qgis/load_demo.py` | loads the demo inputs and runs into QGIS (Plugins ▸ Python Console ▸ Show Editor ▸ Run) |
