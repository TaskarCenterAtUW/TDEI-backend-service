# Dataset Union — Frequently Asked Questions

*A guide to how two pedestrian-network datasets are combined.*

This document explains what happens when we "union" two datasets, written for a general audience. No technical background is needed. Where helpful, small examples are given.

---

## The big picture

### What is a "union", briefly?

A union takes **two maps of the same area** and combines them into **one complete map**, without creating duplicates.

Think of two people who each walked the same neighbourhood and drew their own map. One person carefully mapped all the sidewalks and crossings. The other mapped the sidewalks too, but also added the roads. If you simply stacked both maps on top of each other, you would see every sidewalk drawn twice. The union is the smart way of laying them together: it keeps one clean copy of the shared sidewalks, adds the roads that only one map had, and makes sure everything connects properly.

### Are the two datasets treated equally?

No — and this is the single most important idea. One dataset is chosen as the **authoritative** one (we call it the *primary* dataset). The other is the *secondary* dataset.

- The **primary** dataset is treated as the source of truth. Its features are kept exactly as they are.
- The **secondary** dataset is *stitched into* the primary one. Where the secondary repeats something the primary already has, the duplicate is dropped. Where the secondary adds something new, it is kept and connected in.

**Example.** If the sidewalk-and-crossing map is primary and the sidewalk-and-road map is secondary, the result keeps the primary's sidewalks and crossings untouched, drops the secondary's duplicate sidewalks, and brings in the secondary's roads as new additions.

### Does the order of the two datasets matter?

Yes. Because the primary dataset is kept as-is and the secondary is the one stitched in, choosing which dataset is primary affects the result. The primary's version of any shared feature is the one that survives. This is intentional — you decide which dataset you trust more, and that one becomes the backbone.

---

## How each type of feature is combined

A pedestrian network is made of several kinds of features. The union handles each one appropriately.

### How does the union work for nodes (points where lines meet)?

A **node** is a point — the end of a sidewalk, a corner, a crossing point, a street lamp, and so on.

When a node from the secondary dataset sits very close to a node in the primary dataset, and they represent the same kind of thing, they are treated as **the same real-world point** and merged into one. The primary dataset's node is the one that stays; the secondary node folds into it.

If a secondary node has **no matching primary node nearby**, it is kept as a brand-new node.

### How does the union work for edges (the lines: sidewalks, roads, crossings)?

An **edge** is a line segment — a stretch of sidewalk, a road, a crossing.

- Every edge from the **primary** dataset is kept exactly as drawn.
- An edge from the **secondary** dataset is kept **only if it adds something new**. If it merely retraces a line the primary already has, it is recognised as a duplicate and dropped.
- Secondary edges that represent genuinely new connections (like roads the primary never had) are reconnected to the combined network and kept.

**Example.** Two datasets both drew the same sidewalk along Main Street. The union keeps the primary's copy and drops the secondary's duplicate — you end up with one sidewalk, not two overlapping ones. But a road that only the secondary dataset had is brand new, so it stays.

### How does the union decide an edge is a "duplicate"?

It measures how much of a secondary edge runs alongside a primary edge **of the same type**. If most of the secondary edge (a large majority of its length) is already covered by a same-type primary edge running right next to it, it is considered a duplicate and removed.

Two important safeguards:

1. **Same type only.** A sidewalk is only ever compared against sidewalks, a road against roads, a crossing against crossings. A road is never treated as a duplicate of a sidewalk, even if they run side by side.
2. **Connection safety.** If a small piece of edge is the *only* link holding two parts of the network together, it is kept even if it looks like a duplicate — so the union never accidentally breaks a route.

### What are the "types" or categories of edges?

Every edge is sorted into one of a small set of categories, based on its standard descriptive labels. The category controls how it behaves during the union — which things it may merge with and which it must stay separate from.

| Category | What it is | Examples |
|---|---|---|
| **Pedestrian** | Walking paths | Sidewalks, footways, pedestrian ways, steps |
| **Road** | Vehicle roads | Residential, service, primary, secondary, tertiary, trunk, motorway, unclassified streets |
| **Crossing** | Where pedestrians cross a road | Marked crossings, traffic islands |
| **Bike / living street** | Low-speed shared streets | Living streets (kept in their own isolated category) |
| **Other** | Anything without a recognised type label | Features whose category can't be confirmed |

**The key rule:** a feature only merges (or is treated as a duplicate) within compatible categories. Sidewalks stitch to sidewalks, roads to roads, crossings to crossings. Roads and sidewalks stay separate. Living streets stay in their own lane entirely — they only ever merge with another living street.

This category system is the backbone of how the union keeps the map meaningful: it's *why* a road never dissolves into a nearby sidewalk, and *why* two different kinds of features are never mistaken for one another.

**Example.** A residential road and a sidewalk run parallel a metre apart. The road is category *road*, the sidewalk is category *pedestrian* — different categories, so they are never merged, no matter how close they are.

### How does the union know a feature's category?

It reads the feature's **standard descriptive labels** — the same labels a mapping expert would use to say "this is a sidewalk" or "this is a crossing." Only these standard, meaningful labels decide the category. Extra descriptive detail attached to a feature (supplementary tags) is never used to decide what type it is — it's treated as information *about* the feature, not as the definition of what the feature *is*.

### What happens to zones, points, lines, and polygons?

The same philosophy applies to every feature type:

- **Zones** (areas like plazas or open pedestrian spaces): the primary's zones are kept; a secondary zone is merged only if it substantially overlaps an existing one, otherwise it is added as new.
- **Extension points** (street furniture — lamps, benches, hydrants, poles): merged only with the *same kind* of point nearby (a pole merges with a pole, never with a bench). Otherwise kept as new. *(More on this below.)*
- **Extension lines** (features like fences or walls) and **polygons** (like building footprints or wooded areas): kept from the primary; a secondary one is merged only if it clearly overlaps a matching one, otherwise added as new.

In every case the rule is the same: **keep the primary, drop true duplicates, add what's genuinely new.**

### How much overlap is needed before things are considered "the same"?

Different feature types use a "how much do they overlap?" test to decide whether a secondary feature is a duplicate of a primary one. The exact threshold varies a little by feature type:

| Feature type | Overlap needed to be treated as the same | What happens otherwise |
|---|---|---|
| **Edges** (sidewalks, roads, crossings) | A secondary edge is dropped as a duplicate if **at least 80%** of its length runs alongside a *same-type* primary edge | Kept as a new edge |
| **Extension lines** (fences, walls) | Merged into the primary line if they overlap by **70% or more** | Added as a new line |
| **Polygons** (building footprints, wooded areas) | Merged if they overlap by **more than 70%** | Added as a new polygon |
| **Zones** (plazas, open pedestrian areas) | Merged if the secondary substantially overlaps an existing zone (**around 70%**) | Added as a new zone |

**Why the thresholds are high (70–80%):** they are set deliberately high so that only features that *genuinely* represent the same real-world thing are merged. Two features that merely clip or touch at the edges (a low overlap) are treated as distinct and both kept. You need a large majority of overlap before the union concludes "these are the same feature drawn twice."

**Example (extension line).** The primary dataset has a fence along a property. The secondary dataset drew the same fence, and the two versions overlap by 90%. Since that's well above 70%, they're merged into one fence (primary's version kept, secondary's extra details preserved in the audit trail). But if the secondary "fence" only overlapped the primary's by 30%, it would be treated as a *different* fence and kept separately.

**Example (polygon).** Two datasets both mapped the same building, overlapping by 85% — merged into one. A second building that only clips the first by 20% is kept as its own separate polygon.

---

## When features are close together

### What happens when a road and a sidewalk are within proximity of each other?

**They are kept separate. A road and a sidewalk are never merged into each other.**

Even though a sidewalk usually runs right alongside a road — often just a metre or two away — they are different kinds of features serving different purposes. The union recognises this and keeps both. A pedestrian using the combined map still sees the sidewalk as a sidewalk and the road as a road.

This is deliberate. Merging a road into a nearby sidewalk (or vice versa) would corrupt the map — pedestrians would appear to be routed down a road, or a road would vanish into a footpath. The union's type-awareness prevents this.

**Example.** A sidewalk runs 2 metres from a road for a whole block. Even at close proximity, the union keeps them as two distinct parallel features.

### What happens when two nodes are close enough to merge?

When two nodes (one from each dataset) are within the proximity distance you set **and** they are the same kind of point, the union merges them into a single node. This is how the two datasets get "stitched" together — shared points become one shared point, so the networks connect.

Only the secondary node moves; it snaps onto the primary node's exact location. The primary node stays put, because the primary dataset is authoritative.

**Example.** Both datasets have a node at the end of the same sidewalk, a few centimetres apart (each person's map was slightly imprecise). The union recognises these as the same point and merges them into one, so the two sidewalk pieces join up seamlessly.

### What decides whether two nearby nodes actually merge?

Two conditions must both be true:

1. **They are close enough** — within the proximity distance chosen for the run.
2. **They are compatible types** — for example two pedestrian points, or a point that shares a role with the other. Incompatible points (like a pure road point and a pure sidewalk point) are left separate.

If either condition fails, the nodes are left alone as two distinct points.

---

## What happens to properties (the information attached to features)

### When two nodes merge, what happens to their properties?

Every feature carries **properties** — little labels describing it (for a kerb: `barrier = kerb`; for a lamp: `highway = street_lamp`; and so on). When two nodes merge, their properties are combined using these rules:

1. **The primary dataset wins.** Wherever both nodes describe the same property, the primary dataset's value is kept. (The primary is authoritative.)
2. **New information is added.** If the secondary node has a property the primary didn't have, that property is added to the merged node — so no information is thrown away needlessly.
3. **Overwritten values are recorded, not lost.** Where the primary "won" and the secondary's value was set aside, the secondary's value is still saved in a special **audit** label (see below), so nothing disappears silently.

**Example.** A primary lamp node says `height = 9`. A matching secondary lamp node says `height = 9.1` and also `material = steel`. The merged node keeps `height = 9` (primary wins), adds `material = steel` (new information), and records `height = 9.1` in the audit trail (the secondary's value, preserved for reference).

### How is the audit done — and why?

Whenever the secondary dataset contributes to a merged feature, we write down **exactly what it contributed** in special audit labels. Each audit label records which secondary feature the value came from and what the value was.

**Why this matters:** the union never silently discards information. If the primary and secondary disagreed on a value, or the secondary carried extra detail, you can always look at the audit trail and see the full history of what came from where. It makes the whole process transparent and reviewable.

You can recognise these labels by their `union_audit` marking. They sit alongside the normal properties and don't interfere with how the map is used — they're there purely as a record.

---

## Special handling

### How are kerbs handled?

Kerbs get special treatment because of what they represent in the real world.

**Two kerbs that are close together are left alone — they are *not* merged.**

Here's why. A kerb marks the edge of a footpath at a crossing point. At a single crossing you often have **two separate kerbs** — one on each side of the road — and they can legitimately sit just 2–3 metres apart. These are genuinely different features (the kerb you step down from, and the kerb you step up onto). If the union merged them, it would collapse two real, distinct kerbs into one and create a misleading map.

So the rule is: **when two kerb points are near each other, leave them both exactly as they are.** A kerb still connects normally to non-kerb points around it — the exception only applies when *both* points are kerbs.

**Example.** A crossing has a kerb on the north side and a kerb on the south side, 3 metres apart. The union keeps both kerbs as separate points, correctly representing the two sides of the crossing.

### What happens where a road and a crossing meet?

Where a road and a pedestrian crossing intersect, they should share a common point (so the network knows the crossing actually meets the road there). The union creates or reuses a **shared intersection point** at that spot.

If there's already a point at the intersection, the union reuses it. If there isn't, it creates a new one. This keeps the road and crossing properly joined so that routing across the intersection works.

### How are living streets (shared pedestrian/vehicle streets) handled?

A *living street* is a special low-speed street shared by people and vehicles. Because it behaves differently from both ordinary sidewalks and ordinary roads, the union keeps it in **its own category**. A living street merges only with another living street — it is kept separate from sidewalks, roads, and crossings. This prevents it from being mistakenly folded into a regular footpath or road.

### How are street-furniture points (poles, benches, hydrants) matched?

Points like poles, benches, hydrants, and lamps are merged **only with the same kind of point**. The union looks at what each point *is* and only merges like with like.

- A pole merges with another pole.
- A bench merges with another bench.
- A pole is **never** merged with a bench, even if they sit close together.

If a secondary point has no matching same-type point nearby, it is kept as a new point. And as with nodes, any differing details from the secondary point are preserved in the audit trail.

---

## The confidence score

### What is the confidence score?

Whenever the union merges a secondary point into a primary point, it records a **confidence score** — a number between 0 and 1 that answers the question: *"How sure are we that these two points really are the same thing?"*

- A score **near 1** means high confidence — the points were almost on top of each other and clearly the same kind of thing.
- A score **near 0** means low confidence — the points were near the edge of the allowed distance, or their types were a weak match.

This lets you review merges: high-confidence merges can be trusted at a glance, while low-confidence ones can be flagged for a closer look.

### How is the confidence score calculated?

It blends **two ingredients**:

1. **Closeness (weighted 70%).** How close the two points were, relative to the maximum allowed distance. Points almost touching score near 1 on this part; points near the far edge of the allowed distance score near 0.
2. **Type match (weighted 30%).** Whether the two points were clearly the same kind of thing. A clear type match scores full marks; when the type couldn't be confirmed, this part scores a half.

The final score is: **70% × closeness + 30% × type-match**, rounded to three decimals.

**Example.** Two points almost exactly on top of each other, both clearly the same type:
closeness ≈ 1.0, type-match = 1.0 → confidence ≈ **0.93**.

**Example.** Two points near the far edge of the allowed distance, type not confirmed:
closeness ≈ 0.1, type-match = 0.5 → confidence ≈ **0.22**.

### What do the confidence-related labels mean?

Alongside the score, merged features carry a few plain labels:

- A **source** label — which two datasets were combined.
- A **status** label with one of three values:
  - **merged** — this feature was scored during *this* union.
  - **carried** — this feature already carried a score from an *earlier* union, and we preserved it.
  - **none / new** — this is a brand-new feature that wasn't the result of a merge, so it has no score.

This means if you union datasets that were themselves the result of earlier unions, the history is preserved rather than overwritten.

---

## Common questions

### If both datasets have the same sidewalks, will I see them twice?

No. That's exactly what the union prevents. Shared sidewalks appear **once** in the result — the primary dataset's copy is kept and the secondary's duplicate is dropped.

### Will I lose any roads or features that only one dataset had?

No. Anything that exists in only one of the two datasets is kept. Duplicates are removed; unique features are always preserved. If the secondary dataset had roads the primary didn't, those roads appear in the result.

### Could the union accidentally break a walking route?

No. The union has a built-in connection safeguard: if a piece of edge is the only thing linking two parts of the network, it is kept even if it otherwise looks like a duplicate. Routes stay connected.

### Why does the primary dataset get to "win" every disagreement?

Because you chose it as the authoritative source. In any data-merging task, you need a source of truth to resolve conflicts. Picking the primary dataset is how you tell the union which data you trust more. And even when the primary "wins," the secondary's values are never thrown away — they're kept in the audit trail.

### What is the "proximity" setting and how do I choose it?

Proximity is the maximum distance at which two features are considered "the same place" and eligible to merge. A larger proximity merges features that are further apart; a smaller one only merges features that are very close.

- **Too large**, and you risk merging features that are actually distinct.
- **Too small**, and genuine matches (drawn slightly differently in each dataset) might not merge.

The right value depends on how precisely your two datasets were drawn. A common practical starting point is a small handful of metres.

### Does the union change the shape or position of the primary dataset's features?

No. The primary dataset's geometry is preserved as-is. The only place the union adjusts positions is where a road and crossing must share an intersection point — a deliberate, contained exception needed to keep the network connected.

### What kinds of things count as the "type" of a feature?

Type is read from each feature's standard descriptive labels — for edges, whether it's a footway/sidewalk, a road, a crossing, or a living street; for points, what the point *is* (pole, bench, hydrant, and so on). Only these standard, meaningful labels are used to decide type. Extra descriptive detail attached to a feature is never used to decide what type it is.

### What if a feature has no recognisable type?

If the union can't confirm a feature's type from its labels, it errs on the side of caution and doesn't block a merge purely on type grounds — but closeness still has to be satisfied. Type-matching mainly acts as a *guard* to stop clearly-different things (like a road and a sidewalk) from merging.

### Can I trace where every piece of the final map came from?

Yes. Between the source labels, the status labels, and the audit trail, every merged feature records which datasets it came from, whether and when it was scored, and what the secondary dataset contributed. The union is designed to be fully transparent and reviewable.

---

*For questions not covered here, or for the technical details behind any of these behaviours, please reach out to the data engineering team.*