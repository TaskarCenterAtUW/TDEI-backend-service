# Self-Merge — What It Does

*A plain-language guide to `content.tdei_self_merge_dataset`. This explains the behaviour and the reasoning, not the SQL. If you maintain the code, read this first to understand intent; the implementation lives in `selfmerge_v0_1.sql`.*

---

## The one-sentence version

Self-merge takes a **single** OSW dataset and closes the small gaps inside it — where two paths *should* connect but their endpoints stop just short of each other — by pulling those near-miss endpoints onto a single shared node, so the network becomes routable.

Its guiding rule: **connect gaps, never delete anything.**

---

## Why it exists

Pedestrian network data is drawn by many hands and imported from many sources. A very common defect is a **near-miss**: two sidewalk segments that clearly meet on the ground are stored with their endpoints a few centimetres — or a metre — apart. To a person looking at a map they look connected. To a routing engine they are two dead ends, and you cannot walk from one to the other.

Multiply that by every junction in a city and large parts of the network silently become unreachable.

Self-merge finds those near-misses within one dataset and stitches them shut.

```
        BEFORE                          AFTER
   sidewalk A ───●                 sidewalk A ───●
                  · gap                          │  (one shared node)
   sidewalk B     ●───            sidewalk B ─────●───
   (two dead ends)                 (you can now walk A → B)
```

---

## The core idea: proximity, witness, absorb

Three words describe the whole behaviour.

**Proximity** — how close two endpoints must be before they're considered "the same place." It's a distance you pass in (default 0.5 metres). Endpoints closer than this are candidates to merge; endpoints farther apart are left alone.

**Witness** — when a group of endpoints merges, one of them is chosen to *survive*. That surviving node is the **witness**. It keeps its own identity and its own exact location.

**Absorb** — the other endpoints in the group are **absorbed** into the witness: they disappear as separate nodes, and every path that used to end on them is redirected to end on the witness instead. No path is deleted — it just gets a new, shared endpoint.

```
   ●  n1        n2 ●          n1 and n2 are within proximity
      \          /            → they merge
       \        /             → n1 chosen as witness (survives)
        \      /              → n2 absorbed (removed, its edge re-pointed to n1)
   ─────●══════            result: one node, both edges connected through it
```

---

## Who wins? The witness rules

When endpoints merge, the choice of survivor is not random. In order:

1. **A kerb wins.** If one endpoint is a kerb (`barrier=kerb`) and the other is a plain node, the kerb survives. Kerbs mark the sidewalk/street boundary and carry accessibility information (raised, lowered, flush) — that meaning must not be dissolved into an ordinary node.

2. **Otherwise, the lowest ID wins.** A stable, predictable tiebreak so the same input always produces the same output.

The winner keeps its exact coordinate. Any extra descriptive tags the absorbed node had — and that the winner didn't — are carried over so no information is lost.

---

## What it will NOT do

These are deliberate limits, not missing features.

**It never deletes an edge.** The entire purpose is connection. If a merge would ever collapse a path down to nothing, the merge is undone instead. Edge count out always equals edge count in.

**It never merges two kerbs together.** Two `barrier=kerb` nodes near each other are treated as *real, distinct features* — for example the two opposite corners of a street crossing, or a raised kerb and a ramp a couple of metres apart. Collapsing them would invent a junction that doesn't exist on the ground, so self-merge leaves both in place.

**It never connects things that belong to different networks.** A road and a sidewalk that happen to pass near each other are *not* fused — pedestrians don't step off a kerb into traffic. Roads and sidewalks connect only through a **crossing**, and self-merge respects that: it will connect a crossing to a sidewalk, and a crossing to a road, but never a road directly to a sidewalk.

**It never moves your geometry to "clean it up."** Coordinates are treated as the source of truth. The only coordinate that ever changes is an absorbed endpoint being set to the witness's *existing* location — and even that is an exact copy of a real coordinate, never a rounded or averaged one.

---

## Worked examples

### Example 1 — the simple gap

Two sidewalks, endpoints 0.3 m apart, proximity set to 0.5 m.

```
   A ───────●   ●─────── B        →      A ───────●─────── B
             0.3m
```

They're within proximity and both are sidewalks, so they merge. One node survives, both sidewalks now share it. **Result: connected.**

---

### Example 2 — a four-way junction

Four path-ends converge near one spot, all within proximity.

```
          │                                    │
      ●   │   ●                                 │
        \ │ /                                   │
   ●─────┼┼┼─────●          →           ●───────●───────●
        / │ \                                   │
      ●   │   ●                                 │
          │                                    │
   (several endpoints, all near)        (one shared node — all connected)
```

All four collapse onto a **single** witness in one step. This is the case that a naïve "merge nearest pairs" approach gets wrong — it would leave you with two half-merged nodes still sitting apart. Self-merge pulls the whole cluster to one point.

---

### Example 3 — the kerb keeps its place

A sidewalk endpoint sits 0.25 m from a kerb node.

```
   sidewalk ───●   ⬤ kerb (raised)     →     sidewalk ───⬤ kerb (raised)
                0.25m
```

They merge, and the **kerb** is the witness — even though it might have a higher ID. The surviving node stays exactly where the kerb was and keeps its `raised` status. The sidewalk now ends on the kerb. **Accessibility information is preserved.**

---

### Example 4 — two kerbs are left alone

Two kerb nodes sit 2 m apart at a crossing — one on each side of the street.

```
   ⬤ kerb ·············· kerb ⬤          →     ⬤ kerb ·············· kerb ⬤
        (2 m apart)                                    (unchanged)
```

Even though they're relatively close, self-merge does **nothing** here. Two kerbs are two real features. Merging them would fabricate a mid-street junction that doesn't exist. **Both survive, untouched.**

---

### Example 5 — road and sidewalk are NOT fused

A sidewalk runs parallel to a road; their nodes come within proximity.

```
   road      ═══════●═══════
                    · (near, but different networks)
   sidewalk  ───────●───────
```

Self-merge leaves them separate. A pedestrian can't cross into a road except at a crossing, so fusing these would create a false, unsafe connection. **They stay independent** — unless a crossing feature links them, in which case the crossing is allowed to connect to each.

---

## Beyond the network: other feature types

Self-merge also tidies **duplicate** non-path features within the dataset, using the same witness-and-absorb idea:

- **Points** (benches, poles, hydrants, …) — two points of the **same kind** sitting on top of each other are treated as one feature captured twice; one survives, the other's extra details are folded in. Two points of *different* kinds are never merged.
- **Lines and polygons** (fences, building footprints, landuse, …) — near-identical duplicates (heavily overlapping) collapse to a single feature.
- **Zones** — heavily overlapping duplicate zones collapse to one, and any zone corner that referenced a merged node is updated to point at the surviving node.

The principle is identical throughout: **keep one real feature, lose nothing, invent nothing.**

---

## What you get back

Self-merge returns the cleaned dataset as the usual six OSW layers — **nodes, edges, zones, points, lines, polygons** — ready to load or route on. Merged features carry a small provenance trail (an `ext:selfmerge_*` note recording what was absorbed and why) so any merge can be audited after the fact.

---

## Frequently asked questions

**Q: Isn't this the same as running the union function on one dataset?**
No. The union function is built to merge *two* datasets and treats the first as authoritative — every node effectively finds its own exact copy and nothing internal gets stitched. Running it on a single dataset closes no gaps. Self-merge is purpose-built to look *inward* and connect near-misses within the one dataset.

**Q: I ran it and nothing merged. Why?**
Almost always the **proximity** value. If your gaps are ~1 m and you ran with the 0.5 m default, nothing is close enough to qualify. Raise proximity to just above your typical gap and re-run. (Also check the two endpoints aren't a road and a sidewalk, or two kerbs — those are intentionally left alone.)

**Q: I set a large proximity and now too much merged together.**
Proximity is a blunt instrument — a bigger radius pulls in more, including things that were only coincidentally close. Start small (a little above your real gap size) and increase only if genuine gaps remain open. Merging is bounded so nothing drifts wildly, but a generous radius still merges generously.

**Q: Will it ever delete one of my paths?**
No. That's a hard guarantee. If a merge would reduce a path to zero length, the merge is reversed and the path is kept. The number of edges you put in is the number you get out (barring paths that were already zero-length loops in your source).

**Q: Two sidewalk segments that meet a crossing didn't connect to it. Bug?**
Check whether the crossing's endpoints are tagged `barrier=kerb`. Two kerbs are never merged with each other by design, so a kerb-to-kerb gap stays open on purpose. A kerb *to a plain sidewalk endpoint* will connect. If neither endpoint is a kerb and they still didn't connect, then it's worth reporting.

**Q: My road and sidewalk should connect here — why won't it join them?**
By design. Roads and pedestrian ways are different networks and only meet at crossings. If they genuinely should connect, the data needs a crossing feature between them; self-merge will then connect the crossing to both.

**Q: Two kerbs really are the same point in my data (a duplicate). Self-merge won't merge them — what do I do?**
Correct, it won't — it can't safely tell a true duplicate from two legitimately-close kerbs, and guessing wrong invents a junction. A genuine duplicate kerb is a data-authoring fix, not something self-merge will decide for you.

**Q: Does it change the shape or coordinates of my features?**
No. Coordinates are treated as ground truth. The only thing that moves is an absorbed endpoint, which is set to the surviving node's *existing* coordinate — an exact copy, never rounded, averaged, or snapped to a grid. Everything else is emitted exactly as authored.

**Q: Is running it twice safe?**
Yes. After one pass there are no remaining near-misses within proximity, so a second run finds nothing to do and leaves the data unchanged.

**Q: What's the difference between "proximity" and how it decides two things are the same node?**
Two different questions. **Proximity** decides whether two *separate* endpoints are close enough to *merge* (near, not identical). Separately, when rebuilding a path, an endpoint is matched to its node by **exact** coordinate — no tolerance there. Near-things merge; a path's own vertex is tied to its node exactly.

---

## Glossary

| Term | Meaning |
|------|---------|
| **Node** | A point where paths connect — the junctions of the network. |
| **Edge** | A path segment (sidewalk, crossing, etc.) running between two nodes. |
| **Endpoint** | The start or end of an edge — the bit that needs to meet its neighbour. |
| **Proximity** | The distance threshold (in metres) for "close enough to merge." |
| **Witness** | The node that survives a merge and keeps its location and identity. |
| **Absorb** | To be merged into the witness and removed as a separate node. |
| **Kerb** | A `barrier=kerb` node at the sidewalk/street edge, carrying accessibility info. |
| **Crossing** | The pedestrian feature that links the sidewalk network to the road network. |
| **Zone / Point / Line / Polygon** | Non-path OSW features also carried through and de-duplicated. |