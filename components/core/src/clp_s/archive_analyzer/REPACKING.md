# Value-aware archive packing

## Problem

Suppose column values are indexed at the database level: for a predicate `col = v`, the index
prunes every archive that does not contain `v`. When `v` appears in only a few percent of
archives, most archives are never opened.

Now group archives into packs of K (e.g. K = 128, the pack size used for merged dictionaries).
Reads happen at pack granularity: a pack must be opened if *any* of its archives matches. If
archives are assigned to packs without regard to their contents (arrival order, or effectively at
random), the probability that a pack of K archives contains at least one match is

    P(open pack) = 1 - (1 - s)^K

for a value present in a fraction `s` of archives. Even s = 5% gives 1 - 0.95^128 ≈ 99.9%: an
index that pruned 95% of archives prunes almost no packs. The archive-level index survives, but
its effect is destroyed by the packing.

## Which columns are actually prunable (it is not "low cardinality")

It is tempting to say "index the low-cardinality columns". Cardinality — distinct values divided
by total values — is what makes a column *cheap* to index, but it says nothing about whether the
index prunes archives, and on real data the two can point in opposite directions.

The quantity that matters follows from a counting identity. Summing over values, the number of
archives containing each value equals, summing over archives, the number of distinct values each
holds. So for a column:

    mean archive-frequency = (distinct values per archive) / (distinct values corpus-wide)
    mean archive-level pruning = 1 - that ratio

A column is selective exactly when **its value universe grows as archives are added** — when a
typical archive holds only a small slice of the values that exist across the corpus. Cardinality
does not capture this. Measured on the mongodb corpus (1,037 archives):

| column | cardinality | distinct per archive | union | mean pruning |
|---|---:|---:|---:|---:|
| `s` (severity) | 0.0034% | 6.0 | 7 | 14.3% |
| `ctx` | 0.0056% | 10.1 | 55 | 81.7% |
| `id` | 0.0147% | 26.4 | 126 | 79.1% |
| `attr.message.category` | 0.5421% | 2.7 | 7 | 62.0% |

`s` has the lowest cardinality of any column and the worst pruning: 7 severity values exist and 6
appear in every archive, so the universe does not grow. `attr.message.category` has 160x the
cardinality and prunes four times better. Index candidates should therefore be ranked by
`1 - per-archive/union`, which the simulator reports directly; columns whose universe is bounded
by the schema (severity, boolean flags, small enums) will never become selective no matter how
much the corpus grows, while columns whose universe grows with the deployment (tenant, service,
host, build id) get *better* with scale.

## Never judge a column by its mean

Per-column averages are averages over populations that behave nothing alike, and they routinely
describe no value in the column. The severity column is the clearest case:

| value | archives (of 1,037) | pruning |
|---|---:|---:|
| `D1`, `D2`, `D3`, `D4`, `I` | 1037 | 0.0% |
| `D5` | 1036 | 0.1% |
| `W` | 2 | 99.8% |

The column's 14.3% mean is `(0+0+0+0+0+0.1+99.8)/7`. Six values prune nothing and one value —
the single most selective predicate in the whole corpus — prunes 99.8%. A column that looks
useless in aggregate is worth indexing entirely because of one value the aggregate hides, and
"should we index column X?" is the wrong granularity for the question. The simulator therefore
reports the median and a dead-value count next to every mean, and breaks results down by
per-value selectivity; read those, not the means.

## Objective

For an equality workload that queries each distinct value v of each indexed column, the expected
number of pack opens is proportional to

    Σ_v |P(v)|,   P(v) = the set of packs containing at least one archive that contains v.

Value-agnostic packing maximizes this sum (every value is scattered everywhere); the goal of
value-aware packing is to minimize it. The unbeatable floor for one value is |P(v)| ≥
ceil(n_v / K), where n_v is the number of archives containing v — reached when the archives
containing v are stored contiguously. Minimizing the sum across *all* values of *several* columns
at once is a hypergraph-partitioning problem (archives = vertices, values = hyperedges, packs =
parts, objective = hyperedge fanout), which is NP-hard in general — hence heuristics below.

Two properties of the data make cheap heuristics work well:

1. Archives carry value *sets*, not single values, and those sets overlap heavily for archives
   that are close in time (a deployment keeps logging the same components/ids for a while). So
   arrival order is already far better than random, and clustering only has to tighten it.
2. Values co-occur (a workload phase determines severity mix, components, and ids together), so
   packing well for one column tends to pack well for correlated columns too.

## Strategies

* `original` — arrival (time) order, cut into consecutive packs. The do-nothing baseline; it
  benefits from temporal locality but keeps no guarantees.
* `random` — the adversarial baseline; models content-agnostic placement (e.g. hash-ordered
  object listing).
* `sorted` — sort archives by their value-set signature (per indexed column, most discriminating
  column first), then cut into consecutive packs. Archives with identical sets become adjacent;
  the first sort key gets near-optimal contiguity, later keys benefit only within ties. This is
  the ClickHouse `ORDER BY` trade-off in miniature.
* `greedy` — grow each pack by repeatedly adding the unassigned archive that grows the pack's
  per-column value-set unions the least (growth normalized by each column's universe size so no
  column dominates). Directly minimizes the objective, balances all columns instead of
  privileging the first, and costs O(n²) set operations — fine for thousands of archives. A
  production system could get the same effect incrementally by routing each new archive to the
  open pack whose union it grows the least.

  Growth alone does not rank candidates well, and getting this wrong costs most of the benefit.
  Once a value is in a pack's union, *every* archive containing it grows the union by zero, so
  growth cannot tell an archive that uses the committed value from one that merely adds nothing
  new — and the pack fills with unrelated archives while the value's other archives spill into
  later packs. Because the objective counts packs per value, an archive holding an
  already-committed value is nearly free to this pack and would cost a whole extra pack
  elsewhere, so ties on growth are broken by **maximum overlap** with the union, and only then by
  arrival order (preserving temporal affinity). On a single column this correction took greedy
  from 44.4% to 77.8% pack-level pruning against an 88.9% floor.

`simulate_repacking.py` measures all four against the floor, using the per-archive distinct-value
fingerprints that `archive-analyzer` records for low-cardinality columns (`--value-fingerprints`,
on by default): for each pack size and each indexed column it reports the archive-level pruning
(what the index gives before packing), the pack-level pruning per strategy, the fraction of all
archives read when reads happen at pack granularity, and — most importantly — a breakdown of
pruning by per-value selectivity.

## Practical notes

* Repacking does not require re-encoding archives: pack membership is metadata (which archives'
  dictionaries get merged / which archives are co-located). Clustering can therefore run as a
  background job once enough archives exist, or incrementally as archives are sealed.
* Only values that are rare at the *archive* level can be helped: a value present in every
  archive (e.g. an "info" severity) is unprunable at any granularity, and no packing changes
  that. The index columns worth clustering by are the ones whose value sets differ across
  archives.
* Clustering by content trades against pure time-range locality: a time-range query touches more
  packs if adjacent-in-time archives are scattered. Keeping arrival order as the tie-breaker (as
  `greedy` does) preserves most temporal affinity within clusters; measuring the time-range cost
  is future work.

## Usage

```bash
# Collect per-archive value fingerprints (on by default when column stats are enabled)
archive-analyzer --json /path/to/archives > analysis.json

# Simulate
python3 simulate_repacking.py analysis.json --pack-sizes 32,128,512
```

## Results: mongodb dataset

Setup: the public mongodb dataset (69.6 GB of mongod JSON logs, 260 time-ordered rotation
files) split into 64 MiB line-aligned parts and compressed into **1,037 archives** in time order
(`prepare_dataset.py`), analyzed with value fingerprints, then simulated at pack sizes 32/128/512.
Two index-column sets: the natural mongod filter columns
(`s`, `c`, `id`, `ctx`, `msg`, `attr.db`, `attr.ns`, `attr.remote`, `attr.message.category`) and
the auto-selected most-discriminating set.

Per-column means are reported below only because they are what a reader expects; the
per-value breakdown is the result that matters. Natural columns, pack size 128 (9 packs):

| granularity            | random | original (time) | sorted | greedy | floor |
|------------------------|-------:|----------------:|-------:|-------:|------:|
| archives (no packing)  | 59.4%  | 59.4%           | 59.4%  | 59.4%  | -     |
| packs of 128           | 37.9%  | 40.6%           | 46.9%  | 48.2%  | 54.2% |

Broken down by per-value selectivity (all index-column values pooled), which separates
populations the mean blends together:

| values present in    | count | random | original | sorted | greedy | floor |
|----------------------|------:|-------:|---------:|-------:|-------:|------:|
| <=1% of archives     | 231   | 86.1%  | 86.2%    | 88.8%  | 86.2%  | 88.9% |
| 5-20% of archives    | 14    | 4.8%   | 4.0%     | 50.8%  | 61.9%  | 88.9% |
| 20-50% of archives   | 37    | 0.1%   | 14.7%    | 33.3%  | 34.5%  | 66.7% |
| >50% of archives     | 81    | 0.0%   | 1.6%     | 1.0%   | 2.1%   | 4.3%  |

At the finer pack size 32 (33 packs) the same bands read 96.2 / 12.0 / 0.0 / 0.0% for random
against 96.2 / **83.1** / 55.0 / 3.4% for greedy, with floors of 97.0 / 91.3 / 65.8 / 4.2%.

Reading the rows:

* **5-20% — the motivating scenario** (an archive-level index prunes 80-95%): random packing
  collapses it to ~5% of packs, matching `1-(1-s)^K` to within a point (the small residual is the
  partial last pack, not noise). Value-aware packing recovers it to 62% at K=128 and 83% at
  K=32. This band is where repacking pays, and it is the only band where it pays.
* **<=1%**: values in a handful of archives can only reach a handful of packs however they are
  placed, so every strategy is already near the floor. Nothing to win.
* **>50%**: unprunable at any granularity (floor 4.3%). No strategy helps, as predicted.
* Time order (`original`) beats random — workload phases give real temporal locality — but in
  the mid bands it captures a fraction of what explicit clustering gets, and it can be *worse*
  than random when the values are periodic (see below).

On the auto-selected columns (mean archive-level pruning 88.6%), pack size 128 gives: random
54.9%, original 64.0%, sorted 59.9%, greedy 65.2%, floor 80.4% — the same shape.

### Why the mid band exists here: periodicity, not phases

The 14 values in the 5-20% band are almost entirely one recurring event. Each appears in ~70 of
1,037 archives, spread across the whole corpus in 69 separate runs with a median gap of 17
archives; ten of the fourteen occupy an identical archive set (pairwise Jaccard 1.00). They are
MongoDB's `LogicalSessionCacheRefresh` / `LogicalSessionCacheReap`, which run on a fixed
~5-minute timer.

That periodicity is the worst case for arrival order — a 128-archive time-contiguous window
catches roughly eight occurrences, so every pack matches and `original` prunes 4% — and the best
case for clustering, since those 70 archives fit comfortably in one pack. It is a useful
reminder that assumption (1) above (temporal locality) fails exactly for timer-driven events,
which are common in real systems.

Caveats worth carrying into any production design:

* **The corpus barely populates the band this work is about.** Across the nine natural columns
  there are 231 values in <=5 archives, **zero** values between 5 and 59 archives, 14 at ~70, and
  118 above 300. A single mongod instance running one continuous benchmark produces threads that
  either run once at boot (`initandlisten`, `conn1`) or run forever (`JournalFlusher`,
  `TimestampMonitor`); nothing sits in between. So the aggregate benefit measured here is small
  even though the mechanism is demonstrated cleanly. Multi-tenant or multi-service deployments —
  where "a database-level index prunes 95% of archives" is realistic — fill that band.
* **The remaining gap to the floor is not a limit of clustering.** On a *single* column, `sorted`
  reaches 88.9%, exactly the floor. The 50-62% in the nine-column runs is the cost of satisfying
  nine columns with one linear archive order, not evidence that clustering tops out there.
  Clustering on the one or two columns a workload actually filters on recovers essentially
  everything.
* `greedy` still trails `sorted` on single columns (77.8% vs 88.9%) for a boundary reason rather
  than a modelling one: it packs non-matching archives first, and when their count is not a
  multiple of K the leftovers force one mixed pack. A balancing pass or lookahead would close it.
* Repacking by content trades against time-range locality; `greedy`'s arrival-order tie-break
  keeps clusters mostly contiguous in time, but time-range query cost isn't measured here yet.

Reproduction:

```bash
python3 prepare_dataset.py mongodb.tar.gz mongo-run \
    --clp-s build/core/clp-s --timestamp-key 't.$date' --jobs 3
archive-analyzer --json $(ls -d mongo-run/archives/part_* | sort) > analysis.json
python3 simulate_repacking.py analysis.json --pack-sizes 32,128,512 \
    --columns "s,c,id,ctx,msg,attr.db,attr.ns,attr.remote,attr.message.category"
```
