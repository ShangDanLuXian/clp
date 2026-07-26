# Value-aware archive packing

## Problem

Suppose the values of low-cardinality columns (severity, component, log id, ...) are indexed at
the database level: for a predicate `col = v`, the index prunes every archive that does not
contain `v`. On real data such an index is highly selective — a value often appears in only a few
percent of archives — so most archives are never opened.

Now group archives into packs of K (e.g. K = 128, the pack size used for merged dictionaries).
Reads happen at pack granularity: a pack must be opened if *any* of its archives matches. If
archives are assigned to packs without regard to their contents (arrival order, or effectively at
random), the probability that a pack of K archives contains at least one match is

    P(open pack) = 1 - (1 - s)^K

for a value present in a fraction `s` of archives. Even s = 5% gives 1 - 0.95^128 ≈ 99.9%: an
index that pruned 95% of archives prunes almost no packs. The archive-level index survives, but
its effect is destroyed by the packing.

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
  column dominates; ties broken in arrival order). Directly minimizes the objective, balances all
  columns instead of privileging the first, and costs O(n²) set operations — fine for thousands
  of archives. A production system could get the same effect incrementally by routing each new
  archive to the open pack whose union it grows the least.

`simulate_repacking.py` measures all four against the floor, using the per-archive distinct-value
fingerprints that `archive-analyzer` records for low-cardinality columns (`--value-fingerprints`,
on by default): for each pack size and each indexed column it reports the archive-level pruning
(what the index gives before packing), the pack-level pruning per strategy, and the fraction of
all archives read when reads happen at pack granularity.

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

Headline (natural columns, pack size 128 -> 9 packs; "pruning" = fraction skipped, averaged over
one equality query per distinct value):

| granularity            | random | original (time) | sorted | greedy | floor |
|------------------------|-------:|----------------:|-------:|-------:|------:|
| archives (no packing)  | 59.4%  | 59.4%           | 59.4%  | 59.4%  | -     |
| packs of 128           | 37.9%  | 40.6%           | 46.9%  | 46.4%  | 54.2% |

The averages understate the effect because they mix three very different populations, which the
per-value selectivity breakdown separates (pack size 128, all index-column values pooled):

| values present in    | count | random | original | sorted | greedy | floor |
|----------------------|------:|-------:|---------:|-------:|-------:|------:|
| <=1% of archives     | 231   | 86.1%  | 86.2%    | 88.8%  | 86.1%  | 88.9% |
| 5-20% of archives    | 14    | 4.8%   | 4.0%     | 50.8%  | 55.6%  | 88.9% |
| 20-50% of archives   | 37    | 0.1%   | 14.7%    | 33.3%  | 30.3%  | 66.7% |
| >50% of archives     | 81    | 0.0%   | 1.6%     | 1.0%   | 1.6%   | 4.3%  |

Reading the rows:

* **5-20% - the motivating scenario** (an archive-level index prunes 80-95%): random packing
  collapses that to ~5% of packs, exactly the `1-(1-s)^K` prediction. Value-aware packing
  recovers 51-56%, and the floor (88.9%) shows a smarter partitioner has room to recover nearly
  all of it. This band is where repacking pays.
* **<=1%**: values so rare (a handful of archives) that even random packing can only put them in
  a few packs - clustering is barely needed, though it still closes most of the small gap to the
  floor.
* **>50%**: values in most archives are unprunable at any granularity (floor 4.3%); no packing
  strategy can help, as predicted.
* Time order (`original`) is consistently better than random - workload phases give real
  temporal locality - but for the mid-selectivity bands it captures only a fraction of what
  explicit clustering gets.

On the auto-selected columns (which include high-churn numeric columns such as
`attr.durationMillis`; mean archive-level pruning 88.6%), pack size 128 gives: random 54.9%,
original 64.0%, sorted 59.9%, greedy 65.2%, floor 80.4% - the same shape.

Caveats worth carrying into any production design:

* This dataset is a single mongod instance running one long benchmark, so many values (the
  `>50%` row: default severities, components, steady-state ids) recur in nearly every archive.
  In multi-tenant or multi-service deployments - the setting where "a database-level index
  prunes 95% of archives" is realistic - far more values sit in the 1-20% band where clustering
  matters most.
* `sorted` and `greedy` trade places depending on pack size and column correlations; both are
  cheap. The gap to the floor (e.g. 51-56% vs 88.9%) is the price of heuristics - a
  hypergraph-partitioning pass could close much of it.
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
