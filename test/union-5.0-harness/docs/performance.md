# Performance at scale

Can union 5.0 run on two datasets of millions of features each? **Yes.** Every
step's cost grows in proportion to the data, so time is predictable from the
input size. On a small 2-vCPU test machine:

- **1.85 million edges and 1.19 million nodes** (plus 0.46 million extension
  features and zones) take **6.9 minutes**;
- **3.66 million edges and 2.36 million nodes** (plus 0.92 million) take
  **15.9 minutes**.

## Measured

A synthetic city (`perf/generate_scale.sql`) at growing sizes, run with every
stage on its default rule set at 1 m (`default_1m`). Times are the union call;
reading the output cursors on the server adds under 1 %.

| City (blocks) | Edges (DS1 + DS2) | Nodes | Extension features + zones | Before | After | Temp tables | Output GeoJSON |
|---|---|---|---|---|---|---|---|
| 20 × 20 | 10k (5.2k + 4.9k) | 6.6k | 2.5k | 10.2 s | 2.7 s | 23 MB | 3.5 MB |
| 40 × 40 | 40k (21k + 20k) | 26k | 10k | 26.4 s | 9.9 s | 87 MB | 14 MB |
| 80 × 80 | 162k (84k + 78k) | 105k | 41k | 118.5 s | 37.8 s | 335 MB | 55 MB |
| 160 × 160 | 650k (336k + 314k) | 419k | 163k | 1,160 s (19.3 min) | 147 s (2.5 min) | 1.3 GB | 221 MB |
| 270 × 270 | 1.85M (0.96M + 0.89M) | 1.19M | 464k | — | 414 s (6.9 min) | 3.7 GB | 633 MB |
| 380 × 380 | 3.66M (1.89M + 1.77M) | 2.36M | 918k | — | 952 s (15.9 min) | 7.3 GB | 1.26 GB |

"Before" is the union before the changes under *What changed*. Its time grew much faster
than the data, because a few steps compared items pairwise. Each 4× more data took about 10× longer, and at 650k edges 14 of
its 19 minutes went on merging extension features. On that trend the
1.85M-edge city would have taken hours. "After" grows linearly: about
**0.22 ms per input edge** from 160k to 1.85M edges. At 3.66M edges it was
0.26 ms, because the working set no longer fits in the 8 GB test machine's
memory.

Where the time goes (seconds, from the report's `timings_s`):

| Edges | load | snap | dedup | snap topology | connect | stranding guard | zones + extensions | output |
|---|---|---|---|---|---|---|---|---|
| 1.85M | 67 | 49 | 118 | 1 | 94 | 22 | 28 | 35 |
| 3.66M | 157 | 119 | 247 | 1 | 204 | 60 | 85 | 72 |

Reading the output cursors afterwards took 3 s and 7.5 s on the server; sending
the rows to the backend and writing them to storage comes on top.

The output is unchanged, apart from the four ties under *Determinism* where the
old build's choice was arbitrary. All seven harness profiles produce
byte-identical files, and on the scale cities each optimisation step left the
output checksum unchanged.

### The test city

Each block (100 m) has two road segments, eight sidewalk edges (four sides
split at the middle), about four crossings kerb to kerb across the streets, a
diagonal footway in some blocks, a building, a bench, a hydrant, a fence in a
quarter of the blocks and a pedestrian zone in a tenth. DS2 is the same city
shifted 30 % east (70 % of it overlaps DS1), every location moved up to
±0.25 m, a third of its sidewalk sides unsplit, and different features missing
in each dataset. Half of DS2's block corners are kerbs, which never move, so
connect has gaps to close.

That is close to the worst case: heavy overlap means most edges are snap,
dedup and connect candidates. Two neighbouring datasets that overlap only along
their border run faster, because edges with nothing nearby cost almost nothing.

## Planning a run

| Input edges (DS1 + DS2) | Union time on the test machine | Temp space | Output GeoJSON |
|---|---|---|---|
| 1 million | ~4–4.5 min | ~2 GB | ~0.35 GB |
| 2 million | ~8–9 min | ~4 GB | ~0.7 GB |
| 4 million | ~16–17 min | ~8 GB | ~1.4 GB |
| 10 million | ~40–45 min | ~20 GB | ~3.5 GB |

Rule of thumb: **about 4 minutes and 2 GB of temp space per million input
edges** on one core of the test machine (2 vCPU, 8 GB, PostgreSQL 16, PostGIS
3). A production server's core will usually be faster. The figures assume a
dense overlap like the test city.

**One core.** The union runs as one PostgreSQL function in one transaction, so
it uses one CPU core. More cores let several jobs run side by side; they do not
make one job faster. Core speed matters.

**Temp space.** The union keeps its working tables as temp tables, which live on
the database server's disk (or in `temp_tablespaces`): about 2 GB per million
input edges at the peak. They are dropped when the job's transaction ends,
including when it fails.

**Memory.** The union raises `work_mem` to 128 MB and `maintenance_work_mem` to
512 MB for its own transaction (it never lowers a higher server setting) and
switches JIT off. Peak backend memory in the 3.66M-edge run was
about 0.8 GB. Raising `temp_buffers` gained only about 2 %, so no server
setting needs to change.

**Timeouts.** One job is one transaction that stays open while the backend
reads the output. `statement_timeout` and `idle_in_transaction_session_timeout`
must be off (the default) or longer than the job. The open transaction also
holds back vacuum's clean-up horizon for that long, which is harmless at these
durations.

## What changed

Every statement in the union was checked for how its cost grows with the data,
and each stage was timed on cities of 10k to 3.66M edges.

**Pairwise comparisons, now index lookups** (these made the old build's time
grow much faster than the data):

1. **Connectivity guard (dedup).** Its walk ran for every dedup candidate, and
   the result was joined back with a nested loop over all candidates. It now
   runs only for the edges it can decide, with no join.
2. **Extension points and lines.** Each DS1 feature re-scanned the whole match
   table to merge its DS2 matches. The match tables are now indexed.
3. **Extension polygons and zones.** DS1 × DS2 overlaps were found by comparing
   every pair. They now use a spatial index.

**Linear but wasteful:**

4. **Stranding guard.** Each round re-checked every removed duplicate (8+ rounds
   on the test city). Later rounds now check only edges next to one the round
   before restored.
5. **Indexes after updates.** PostgreSQL cannot use an index built after a row
   of its table was updated earlier in the same transaction. When edge ends had
   no node, the old build fixed the edges first and indexed them after, so every
   later stage scanned the whole edge table. Every working table is now filled
   in one statement and indexed before anything updates it.
6. **Type matching** rewrote every edge and node row (and its indexes) after
   loading. Types are now set during the load, and an unused spatial index on
   the edge table is gone.
7. **Dedup** buffered every anchor edge. It now buffers only anchor edges a
   candidate can reach. A candidate that copies one anchor edge is settled by
   that edge's buffer, without merging buffers. The same-two-nodes test uses an
   index.
8. **Smaller fixes.** Snap topology's OR join became two index joins. The
   zone-node chain follows only zone nodes, not every node. The output's
   mixed-type check uses a hash instead of sorting every property. Working
   memory and JIT are set per transaction. Staging tables are dropped once used,
   and loaded rows are no longer rewritten (30 % less temp space).

**Determinism.** Four ties were broken arbitrarily, so the same input could
give different output from one run to the next. Each is now broken by lowest
original `_id` (or lowest id):
- which A edge's tags a connector copies when several A edges end at one node;
- the anchor edge dedup compares against when two are equally near;
- the crossing an overshooting end meets when it crosses twice;
- the DS2 zone merged when two cover a DS1 zone equally.

Duplicate keys in merged tags now resolve in key order.

**Report.** `timings_s` gives the seconds spent in each stage.

## Beyond the database: the backend job

The TDEI backend streams each output cursor to blob storage, then zips the
files. At millions of features, check these before relying on it:

- **The zip is built in memory.** `zipStream` reads every output file fully
  into memory and builds the zip as one buffer. The 1.85M-edge union's output is
  633 MB of GeoJSON and the 3.66M-edge union's 1.26 GB, so this step alone
  needs about 1.5–3 GB of memory at these sizes. Streaming the zip (for example
  with `archiver`, piping blob streams in and the zip stream out to blob
  storage) would keep memory flat.
- **No backpressure.** Each row is pushed into the upload stream whether or not
  the upload has caught up. If storage is slower than the database, unsent rows
  pile up in memory.
- **Quiet connection.** During long stages the database sends nothing to the
  backend; at 4M edges dedup is several minutes long. An idle-timeout on a NAT or
  load balancer between them (often 4 minutes on Azure) could drop the
  connection. Setting `keepAlive: true` on the `pg` Pool avoids that.
- **The message lock is fine.** The Service Bus lock is renewed every 25
  seconds for as long as the job runs.

## If it ever needs to be faster

The remaining time is mostly PostGIS work each feature needs anyway. Examples
are intersecting a candidate edge with its anchor buffers, and finding the B
lines near each end. The next step would be parallelism. PostgreSQL can split
the heavy statements across cores, but only on regular (unlogged) tables, not
temp tables. That means per-job table names and clean-up, so it is a bigger
change, worth it only if jobs regularly exceed ~10 million edges.

## Running the scale tests

```bash
# a city of side × side blocks (side 100 ≈ 130k edges per dataset), built once
# per side, then a timed union with a profile's config; results append to
# runs/perf/results.tsv
perf/run_scale.sh local default_1m 50 100 200

# also log each statement over 200 ms with its plan, then rank them
PROFILE_SQL=1 perf/run_scale.sh local default_1m 100
python3 perf/slow_statements.py runs/perf/default_1m_100.plans.log

# time another build deployed under its own schema (for a before/after)
UNION_FN=perfbase.tdei_union_dataset perf/run_scale.sh local default_1m 100

# after changing the union: harness outputs must be identical to a saved copy
python3 perf/compare_runs.py <saved runs dir> runs
```

Sizes: side 270 ≈ 1.85M edges (about 4 GB of disk for the data plus 4 GB of
temp space); side 380 ≈ 3.66M edges, about twice that. Building side 380 takes
about 30 minutes. The checksum column changes only if the output changes.
