# Looking at runs in QGIS

`qgis/load_in_qgis.py` loads every profile run under `runs/`, styled and grouped
so you can compare input with output, and one profile with another.

## Load

1. Run the harness first ([running.md](running.md)).
2. QGIS → **Plugins ▸ Python Console ▸ Show Editor** → open `qgis/load_in_qgis.py`.
3. Press **Run**.

Settings at the top of the script:

| Setting | Default | Meaning |
|---|---|---|
| `HARNESS_DIR` | `""` | harness root. Empty means the folder above the script. Set it only if QGIS can't find it. |
| `RUNS` | `"all"` | every profile run under `runs/`, or a list such as `["default_3m", "custom_connect"]` |
| `SHOW_FIRST` | `"default_1m"` | the run that is visible when loaded |

If you edit the script, **save it (⌘S) before Run**. QGIS runs unsaved edits
from a temporary copy, and then the script can't locate the harness.

## Layer tree

```
Union 5.0 harness
├─ Runs                           only ONE run visible at a time — click another to flip
│  ├─ default_1m — Every stage on its published default rule set, 1 m — 58/58 PASS
│  │  ├─ Checks                   case boxes + must-connect rings, green PASS / red FAIL
│  │  ├─ Expected (this profile)  (off) rings = must connect, faded red = must be removed
│  │  ├─ What changed             input vs output, only the differences
│  │  │    nodes                    ◆ snapped   ★ created (magenta)   ★ junction (blue)   ✕ input node gone
│  │  │    input edges — fate       red dashed removed · purple split · orange modified
│  │  │    output edges — origin    (off) connector · split piece · modified · new
│  │  └─ Output                   edges (connectors magenta), nodes (kerbs purple), zones,
│  │                              extension points / lines / polygons
│  ├─ default_3m — …
│  ├─ custom_connect — …
│  └─ …
├─ Cases                          (off) every case box and title, all profiles
└─ Input                          DS1 blue solid, DS2 orange dashed (nodes and extension features off)
```

## Ways to compare

| Question | Do this |
|---|---|
| What did the union do? | Look at **What changed** only (untick Output and Input). Every mark is a change. |
| Did it do what the profile expects? | Turn on **Expected (this profile)**. A red dashed "removed" line should sit on every faded-red band, and every ring should be green in **Checks**. |
| Input vs output | Toggle the **Input** group on and off over a run's **Output**. |
| 1 m vs 3 m | Click the other default run under **Runs**. Case D shows the difference: at 3 m the DS2 sidewalk and its kerb are removed; at 1 m both are kept. |
| Rule order | Flip between `custom_connect` and `default_1m` on the CC cases: the node tags (`ext:union_rule`, `barrier`, `ext:source`) show which rule made each junction. |
| Stages on their own | Flip between `connect_only` and `default_1m`: junctions are made either way; duplicates and coincident joins only with snap and dedup on. |

## What the marks mean

| Mark | Meaning |
|---|---|
| magenta line | **connector** a connect rule made (Identify: `ext:union_rule`, `ext:union_confidence`, the rule's `edge_tags`) |
| red dashed line | input edge **removed**: nothing in the output comes from it (a duplicate) |
| purple line | input edge **split** into 2+ pieces lying exactly on it |
| orange line | input edge **modified**: one line, reshaped (an end snapped) |
| green ◆ | output node other nodes **snapped** onto (Identify → `absorbed`, `audit`) |
| magenta ★ | node a connect rule **created** |
| blue ★ | existing node a connect rule used as a **junction** (`rules` lists them, in rule order) |
| red ✕ | input node **gone** from its own position: snapped onto its anchor, or went with a removed duplicate (`nearest_m`) |
| green / red box | case area, PASS / FAIL. Identify shows `expected` and `failures` |
| ring `CC9 PASS (3)` | a must-connect point, and how many named edges meet there |

Gaps and connectors are under a metre: zoom right in, or use the 4 m close-up on
each panel of `case_sheet.png`.

A good case: black follows the lines that should survive, every ring is green,
connectors are short and magenta, and no black covers faded red.
