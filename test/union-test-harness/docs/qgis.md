# Looking at runs in QGIS

`qgis/load_in_qgis.py` loads every run under `runs/`, styled and grouped so you can
compare input with output, and one run with another.

## Load

1. Run the harness first ([running.md](running.md)).
2. QGIS → **Plugins ▸ Python Console ▸ Show Editor** → open `qgis/load_in_qgis.py`.
3. Press **Run**.

Settings at the top of the script:

| Setting | Default | Meaning |
|---|---|---|
| `HARNESS_DIR` | `""` | repo root. Empty means the folder above the script. Set it only if QGIS can't find the repo. |
| `RUNS` | `"all"` | every run under `runs/`, or a list such as `["prox_3", "f_crossing"]` |
| `SHOW_FIRST` | `"prox_3"` | the run that is visible when loaded |

If you edit the script, **save it (⌘S) before Run**. QGIS runs unsaved edits
from a temporary copy, and then the script can't locate the repo.

## Layer tree

```
OSW union harness
├─ Runs                           only ONE run visible at a time — click another to flip
│  ├─ prox_3 — default, no filters — 21/21 PASS
│  │  ├─ Checks                   case boxes + must-connect rings, green PASS / red FAIL
│  │  ├─ What changed             input vs output, only the differences
│  │  │    nodes                    ◆ merged   ★ minted ixn   ✕ DS2 node gone
│  │  │    input edges — fate       red dashed removed · purple split · orange modified
│  │  │    output edges — origin    (off) split piece · modified · new
│  │  └─ Output                   the union result: black edges, grey nodes, purple kerbs
│  ├─ prox_1 — default, no filters — 21/21 PASS
│  └─ f_crossing — Edge filter: crossings only @ 3 m — 21/21 PASS
│     ├─ vs baseline              what the FILTER changed vs the same proximity without filters
│     └─ Checks / What changed / Output
├─ Expected                       what SHOULD happen: faded red = must be removed, rings = must connect
└─ Input                          DS1 blue solid, DS2 orange dashed (nodes off)
```

## Ways to compare

| Question | Do this |
|---|---|
| What did the union do? | Look at **What changed** only (untick Output and Input). Every mark is a change; unchanged lines are hidden. |
| Did it do what was expected? | Turn on **Expected**. A red dashed "removed" line should sit on every faded-red band, and nowhere else. |
| Input vs output | Toggle the **Input** group on and off over a run's **Output**. |
| 1 m vs 3 m | Click the other run under **Runs**. Case D shows the difference clearly: at 3 m the DS2 sidewalk and its kerb are removed; at 1 m both are kept. |
| Filter vs no filter | Click between a scenario and `prox_3`, and read its **vs baseline** group. |

## What the marks mean

| Mark | Meaning |
|---|---|
| red dashed line | input edge **removed**: nothing in the output comes from it (a duplicate) |
| purple line | input edge **split** into 2+ pieces lying exactly on it |
| orange line | input edge **modified**: survives as one line but reshaped (endpoint snapped) |
| green ◆ | output node that **merged** a DS2 node into a DS1 node (Identify → `audit` shows the DS2 values) |
| blue ★ | **minted** intersection node (`ixn-…`) |
| red ✕ | DS2 node **gone** from its own position: snapped onto a DS1 node, or dropped with a removed duplicate (`nearest_m`) |
| green / red box | case area, PASS / FAIL. Identify shows `expected` and `failures` |
| ring `C PASS (4)` | a must-connect point, and how many edges meet there |

In **vs baseline** (filter runs):

| Mark | Meaning |
|---|---|
| teal line | duplicate **kept by the filter** (the baseline removed it) |
| red dashed line | removed only under these settings |
| green ◆ | DS2 tags **merged** onto the node |
| orange ◇ | DS2 tags **withheld**: audited in `ext:union_audit_*` only |

In a categorized layer, untick a legend entry to hide just that kind (tick
"kept unchanged" to see everything). **Identify** on any feature shows its case,
name and ids.

A good case: black follows the lines that should survive, every ring is green, and
no black covers faded red.
