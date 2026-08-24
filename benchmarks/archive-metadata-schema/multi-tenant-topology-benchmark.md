# Multi-tenant metadata topologies: benchmark design and results

This document describes the design, execution, and results of the table-scope topology
benchmark (`bench_topology.py`): the experiment that decides how CLP's metadata tables should
be organized when one deployment serves many users. It is self-contained -- every term it
uses is defined in section 1.0, the exact setup is in section 3.0, and every number quoted
comes from the run recorded in `topology_results_20260824-010411.txt`.

The one-line conclusion: **one unified table set for everyone, with dataset_id leading every
primary key.** Retention in CLP is a background row-wise delete job keyed on each dataset's
policy; its own cost is explicitly not a decision metric. Neither is query latency, as it
turns out: every query shape CLP actually serves is scoped to a single dataset, and on those
shapes all five configurations tie. The decision is therefore made entirely on the builder
and operational axes -- storage, write amplification, and footprint -- and `unified` wins
every one of them. The current one-set-of-tables-per-dataset layout pays a 100x idle-storage
floor (only where partitions stay sparse -- 5.1) and 36% more physical write bandwidth
(about 9 KB/s in absolute terms, a rounding error -- 5.2). Converted to absolutes most of
the margins are small, and the recommendation rests instead on one structural fact and one
negative result: `per_dataset`'s tablespace count grows with tenant count toward hard
ceilings that hardware cannot raise, and co-locating tenants in one B-tree costs a
single-tenant query nothing measurable.
Section 5.4 records what happens IF retention were executed by partition drops; under the
row-wise model it decides nothing, and the `tiered` variant it motivates is kept only as a
contingency.

---

## 1.0 Terminology

**Topology / configuration.** A rule for how many physical tables hold the metadata of N
datasets, and what the primary key of those tables looks like. The benchmark compares five
(section 2.0). Nothing else varies: same rows, same partitioning, same server.

**Table kind.** The schema has several logical tables per dataset -- the archives table, the
filter-postings side table, and in production also files, tags, and so on. Each is a "kind".
Per-table costs multiply by the number of kinds, so the benchmark builds two representative
kinds and the results scale by K, the kind count:

- `arch`: one row per archive (id, begin/end timestamp, size). Narrow, low row count.
- `side`: the filter posting list, ~45 rows per archive. Wide, dominates bytes and rows.

**Partition / tablespace file.** Every table is `PARTITION BY RANGE (begin_timestamp)` into
hourly partitions. InnoDB stores each partition as its own file on disk. A partition file
has a fixed floor of 64 KB even when it holds zero rows, and the server holds an open file
handle per populated partition, bounded by `innodb_open_files`.

**dataset_id-leading primary key.** In any configuration where one table holds more than one
dataset, `dataset_id` is the FIRST column of the primary key, e.g. for the side table:

    PRIMARY KEY (dataset_id, column_id, value, begin_timestamp, archive_id)

Leading with it keeps each dataset's rows one contiguous key range, so a single-dataset query
is one index seek. Configurations whose tables hold exactly one dataset omit the column --
it would be a constant.

**Retention tier.** How long a user's data must be kept before deletion. Users differ: the
benchmark assigns tiers of 24 h, 72 h, and 168 h round-robin across users. The P3
experiment executes retention by dropping whole expired partitions (`ALTER TABLE ... DROP
PARTITION`) to expose the structural differences between topologies; CLP's operative model
is a background row-wise delete job (see 5.4).

**Rows past policy (over-retention).** After an expiry cycle, the number of rows still
present whose owning dataset's retention says they should be gone. A table that holds only
one tier can always drop its expired partitions, so it scores zero. A table that MIXES tiers
cannot drop a partition until the LONGEST tier in it expires, so every shorter-tier tenant's
rows in that partition are retained past policy. This matters only under DROP-based
retention: CLP's operative model is a background row-wise delete job, which meets every
policy exactly in any topology, so this metric describes the drop-based alternative.

**Logical payload.** The bytes the data itself represents: rows multiplied by their raw
column widths, with zero storage overhead. For this run, 1,301.2 MB. It is the denominator
for both amplification figures, so they measure pure overhead.

**Space amplification.** Stored bytes (`data_length + index_length`, i.e. filled pages)
divided by logical payload. How many bytes on disk each payload byte costs.

**Write amplification.** (`Innodb_data_written` + `Innodb_os_log_written`) across the build,
divided by logical payload. How many bytes InnoDB physically pushed to storage -- page
writes plus redo log -- per payload byte. Measured by differencing global counters around
the load, so it captures what actually hit the disk, not what was requested.

**Warm / cold / phys_rd.** A warm query finds its pages in the buffer pool; a cold one must
read storage. Instead of forcing an empty pool (unreliable: MariaDB 10.6 refuses online pool
shrink, and a table scan cannot evict InnoDB's scan-resistant LRU), the benchmark records
`phys_rd` = `Innodb_buffer_pool_reads` per query: the count of pages actually fetched from
storage. See section 6.2 for why this still under-measured cold in this run.

**parts (partition pruning).** The number of partitions the optimizer visits for a query,
taken from `EXPLAIN PARTITIONS`. Confirms that time-range predicates prune, rather than
assuming it.

**open_f.** `Innodb_num_open_files`: how many tablespace files the server holds open. It
equals `min(tablespaces holding data, innodb_open_files)` -- files are opened at server
startup, not by the queries that read them (measured in 5.1), so this gauge describes the
INSTANCE and never a query's time window. Pinned at the cap means the instance has more
tablespaces than cache slots, and hence that accesses outside the cached set must evict and
reopen. It does not by itself mean anything is slow: 5.1 measures that cost at roughly 10%.

---

## 2.0 The five configurations

| config            | tables total | dataset_id in key | what it models                          |
|-------------------|--------------|-------------------|-----------------------------------------|
| `per_dataset`     | K x N        | no                | today's design: tables named by dataset |
| `schema_per_user` | K x N        | no                | same tables, one SCHEMA per user        |
| `per_user`        | K x U        | yes               | one table set per user                  |
| `unified`         | K            | yes               | one table set for everyone              |
| `tiered`          | K x R        | yes               | one table set per RETENTION TIER        |

N = datasets (100 here), U = users (20), R = retention tiers (3), K = table kinds (2 here;
more in production, so table counts scale up accordingly).

`schema_per_user` is deliberately a placebo: it changes only naming/namespacing, which fixes
the SQL-construction problems of dataset-named tables (identifiers built by string
interpolation, the 64-char identifier limit, sanitization collisions) while changing nothing
physical. Its results test whether those problems can be fixed without the operational
refactor.

`tiered` groups tenants by retention contract instead of identity. It exists because of the
retention finding (section 5.4): grouping by anything that mixes retention periods in one
partition forces over-retention, and retention tier is the one grouping for which whole
partitions always expire together.

---

## 3.0 Benchmark setup

**Scale.** 20 users x 5 datasets x 12,000 archives = 100 datasets, 1.2 M archive rows and
54,000,000 side-table postings per configuration. Every table has 168 hourly partitions plus
a floor and a future catch-all (170 files per table). Retention tiers 24/72/168 h assigned
round-robin by user (20 users over 3 tiers = a 7/7/6 split -- this matters in 5.4).

**Data shape.** Each archive contributes 45 postings across 8 filter columns whose widths
and cardinalities follow the same `c8` shape used in every earlier round (hosts at 1,000
distinct values, envs at 12, severities at 40, modules at 300), so per-archive byte figures
are comparable across rounds. The 45 is a cardinality-WITHIN-archive assumption -- 3
host-like columns at 1 distinct value each, 2 env-like at 3, 2 severity-like at 8, 1
module-like at 20 -- not a size ceiling.

**Where `c8` sits in the admissible range, which every absolute figure below depends on.**
The service admits a filter configuration only if its payload stays under 0.1% of the
compressed archive, which at 256 MB raw and 50x compression is 5,368 B per archive (see
README). `c8` uses 577 B: about 11% of that allowance. The widest admissible shape, `wide`,
is 340 values per archive at 5,117 B -- 8.9x more. So a deployment whose users configure to
the policy limit would produce a side table roughly 8.9x larger than every size number in
section 5.2, and the storage projections scale with it. The topology COMPARISON is
unaffected, since all five configurations ran the identical shape; the absolute magnitudes
are a conservative point in the admissible space, not an upper bound.

**Write path.** 16 worker processes run concurrently, each owning a slice of the datasets,
issuing per-archive INSERTs interleaved in timestamp order across their datasets. This
matters: tenants ingest simultaneously in production, so writes land in many key ranges at
once. (An earlier round loaded tenant-by-tenant, which resembles a sorted bulk load and
flatters whichever design has the fewest B-trees.) Ingestion throughput is reported for
context only; it is not a decision metric.

**Server.** MariaDB 10.6.23, 4 GB buffer pool, `innodb_open_files = 2000`, one shared
instance. Each configuration is created, loaded, queried, expired, and dropped in sequence,
so configurations never share cache state.

**Measurement sources.** Sizes from `information_schema` after `ANALYZE TABLE` (the
statistics are cached and read stale otherwise); file counts and allocated bytes from
`INNODB_SYS_TABLESPACES.FILE_SIZE`; rows examined from `Handler_read_*` after `FLUSH
STATUS`; physical reads, opened tables, and write volumes from global status counters
differenced around each operation; partitions visited from `EXPLAIN PARTITIONS`.

---

## 4.0 What is measured

- **P0 structure**: tables, partitions, open files, and the bytes a configuration costs
  while completely EMPTY -- allocated-but-unused space, measured after CREATE and before any
  row exists. See 5.1 for why this is a steady-state floor rather than a day-one cost.
- **P1 size and amplification**: stored bytes split by table kind; space amplification;
  write amplification across the concurrent build.
- **P2 query matrix**: eight query shapes, each isolating one thing topology could change --
  Q1 selective point lookup, 24 h window; Q2 same, 7-day window; Q3 low-selectivity value;
  Q4 prefix wildcard; Q5 two-predicate AND (self-join binding archive_id AND
  begin_timestamp, per the ts-equality rule); Q6 join back to the archives table for
  metadata; Q7 archives-table time-range scan; Q8 the same lookup across ALL tenants
  (an N-way UNION for per-table configs, one IN-list range for shared-table configs) --
  a table-count DIAGNOSTIC, not a workload CLP serves; see section 5.3.
  Each records wall time, rows examined, physical reads, partitions visited, and
  table-cache misses.
- **P3 retention**: one full expiry cycle under the heterogeneous tiers -- wall time,
  DROP PARTITION statements issued, bytes freed, and rows past policy.

---

## 5.0 Results

### 5.1 Structure and idle cost (P0)

| config            | tables | partitions | open_f    | empty (MB) |
|-------------------|-------:|-----------:|----------:|-----------:|
| `per_dataset`     |    200 |     34,000 | **2,000** |    2,125.0 |
| `schema_per_user` |    200 |     34,000 | **2,000** |    2,125.0 |
| `per_user`        |     40 |      6,800 | **2,000** |      425.0 |
| `unified`         |      2 |        340 |       344 |       21.2 |
| `tiered`          |      6 |      1,020 |     1,024 |       63.8 |

Empty cost is pure partition-file floor: a new InnoDB tablespace allocates 64 KB before it
holds anything (at the default 16 KB page size), so the column is exactly 64 KB x partition
count -- 2,125 MB / 34,000 = 64 KB. Per dataset under the current design that is ~21 MB,
times however many kinds production adds. At 1,000 datasets it is ~21 GB of floor.

**The floor is only a lasting cost where partitions stay SPARSE.** 64 KB is an initial
allocation; a partition holding real data grows past it, and the floor becomes the first
64 KB of a file that is earning its keep. At this run's ~81 stored bytes per posting row,
64 KB holds ~810 rows, or about **18 archives** -- so a dataset producing more than ~18
archives per hourly partition absorbs the floor entirely. The benchmark's datasets produce
71 archives/hour, comfortably above that line, which means the 2,125 MB above is a starting
state that this run's own data had absorbed by the time it finished. It is NOT ongoing waste
in this scenario.

Where it becomes ongoing waste is many SMALL tenants -- the regime where a fixed cost per
(dataset x partition) has nothing to amortize against. A near-idle dataset under
`per_dataset` with hourly partitions and K=6 allocates 6 x 170 x 64 KB, about 65 MB, to hold
almost nothing; ten thousand such datasets is ~640 GB of near-empty files. Note this is a
hazard of creating datasets that carry no data, not a cost of scale: ten thousand REAL
datasets at this run's density hold ~434 GB of actual metadata with the floor absorbed
inside it, so the two figures are alternatives, never a sum. This is the
sharpest statement of what `unified` does: it amortizes partition overhead ACROSS tenants
rather than multiplying it BY them, so a partition is dense whenever the aggregate rate is
dense and an idle tenant costs only its rows. Coarser partitioning is the other lever --
daily partitions move the threshold from ~18 archives/hour to ~18 archives/day.

So this row matters if the deployment expects many small or dormant datasets, and barely
matters if it is a modest number of busy ones.

**This is also a steady-state figure, not a day-one cost.** Partitions are created by DDL, never
by data arriving -- a row outside every declared boundary is rejected, not accommodated. The
benchmark can declare all 170 boundaries at CREATE TABLE because it generates its own data
and therefore knows the time range in advance; production has no such knowledge and would
create a floor plus some hours forward of now, with a scheduled job extending the leading
edge as retention removes the trailing one. The count climbs to `retention_hours + 2` over
one retention period and then holds. So read this column as what a configuration costs once
a dataset has been alive for a full retention window, which is the state it spends its life
in. The ratios between configurations are unaffected: all five were measured identically.

Note that under `per_dataset` this floor is paid even by a dataset shipping NO data, since
the partition-maintenance job extends the window on a schedule and has no reason to know a
dataset is idle.

The `open_f` column shows file-handle saturation: all three many-table configurations sit
pinned at exactly the `innodb_open_files` cap of 2,000, so the server must evict and reopen
tablespace files to touch anything outside the cached 2,000. `unified` and `tiered` hold
every file they need open at once, with headroom.

**What the gauge means, and what saturation costs, were both measured separately** on a
throwaway instance by `check_file_handles.sh`, because this run reported the pinned gauge as
a finding without establishing either.

*The gauge is a property of the instance, not of a query.* Counting the server's open file
descriptors by name in `/proc/<pid>/fd`, 60 of a table's 61 partition files were already
open after a clean restart with the table untouched -- and with buffer-pool dump and restore
both disabled, so the pool was not the cause. Queries pruned to one partition, to ten, and a
full sweep each added nothing. So:

    Innodb_num_open_files = min(tablespaces holding data, innodb_open_files)

A query's time window does not enter into it. Thrash is therefore an instance-level
condition -- total tablespaces exceeding the cap -- not something a wide query triggers.

*Saturation costs about 10%, not a cliff.* Identical data, identical 32 MB pool, identical
sweep of 200 partitions, varying only the cap: 841 ms at cap=1000 (all files open), 839 ms
at 300, 823 ms at 30, and 935 ms at cap=12. Physical page reads were equal across all four
(~42,200), so the difference is evict/reopen rather than I/O volume. The worst row is 17x
oversubscribed -- the same ratio as `per_dataset`'s 34,000 tablespaces against a 2,000 cap
-- and costs roughly 11%. The sweep is I/O bound, which under-states churn as a fraction; on
cached data it would weigh more.

This also explains why section 5.3 finds no query penalty, and why that is not a
contradiction: the query shapes touch the same 24 partitions repeatedly, those stay resident
in the handle LRU, and nothing churns however many tablespaces exist elsewhere.

**The consequence is that file-handle saturation is a modest tax, not the load-bearing
finding this document originally made it.** What the file count is measured to cost is idle
allocation (the table above). Startup cost scales with tablespace count for the same reason
the gauge does -- the files are opened then -- though this run did not time it (6.2). The
write-amplification gap in 5.2 should not be attributed to file handles either; B-tree count
is the likelier mechanism. The remaining
file-count argument is structural rather than empirical, and it is a hard wall rather than a
gradient: `K x N x (hours + 2)` grows without bound in N, reaching 4.3 M files at 1,000
datasets with K=6 and 30-day retention -- past the process fd limit and past what backup,
DDL, and filesystem directories handle at all.

### 5.2 Size and write amplification (P1)

| config            | arch (MB) | side (MB) | total (MB) | space amp | write amp |
|-------------------|----------:|----------:|-----------:|----------:|----------:|
| `per_dataset`     |     265.6 |   4,074.6 |    4,340.2 |     3.34x |    15.91x |
| `schema_per_user` |     265.6 |   4,074.6 |    4,340.2 |     3.34x |    16.21x |
| `per_user`        |     158.1 |   5,093.1 |    5,251.2 |     3.73x |    12.64x |
| `unified`         |     131.6 |   4,166.2 |    4,297.7 |     3.06x |    11.71x |
| `tiered`          |      96.7 |   4,276.5 |    4,373.2 |     3.11x |    11.99x |

Logical payload: 1,301.2 MB. Load wall times (informational): per_dataset 396 s,
schema_per_user 400 s, per_user 263 s, unified 118 s, tiered 119 s.

**Both of those spreads must be read in absolute terms, or they mislead.** The 1.2 M
archives represent 168 h of ingest -- 604,800 s of real time -- loaded in 396 s, so the
required steady-state rate is ~2 archives/s against a demonstrated ~3,030/s. The worst
configuration is over-provisioned for its own ingest by roughly 1,500x, and the 3.4x spread
between configurations is 3.4x on a quantity with three orders of magnitude of slack.

The write-amplification ratio deflates the same way once converted: 15.91x of 1,301.2 MB is
20,700 MB over seven days against `unified`'s 15,240 MB, a difference of 5,465 MB, or about
**9 KB/s sustained** -- ~285 GB/year, against SSD endurance measured in petabytes. The 36%
figure is real and reproducible, and at this scale it is an operational rounding error.

Neither is therefore a reason to prefer any configuration. They are reported because they
scale linearly with tenant count and because they corroborate the mechanism (many B-trees
dirtied concurrently versus few), not because their magnitudes matter here. Section 7.0
weighs them accordingly.

Two findings. First, **write amplification is 36% higher for the per-dataset layout** (15.9x
vs 11.7x): identical rows, but flushing them through 34,000 small partitions writes more
partially-filled pages. Every payload byte costs ~12-16 bytes of physical writes either way
-- the per-archive INSERT path pays redo per commit -- but topology alone moves it by a
third. Second, `schema_per_user` matches `per_dataset` on every physical number, confirming
it is a naming-only change: it fixes SQL construction and fixes nothing else.

`per_dataset`'s side table is slightly smaller than the shared ones (4,074 vs 4,166 MB)
because its rows genuinely omit the 2-byte dataset_id; the 92 MB difference is that column.
The `per_user` row is anomalous -- see section 6.1.

### 5.3 Query matrix (P2)

Single-tenant shapes (Q1-Q7) are a five-way tie: identical rows examined (104 / 816 / 417 /
1,714) and identical partitions visited (24 for the 24 h window, 168 for 7 days, 48 for the
joins), with wall times all inside 2.5-4.1 ms -- though section 6.3 explains why the
structural counters, not the timer, are what carry that conclusion. Sharing a table with 99
other tenants costs a single-tenant query nothing measurable: the dataset_id key prefix and
partition pruning isolate it as effectively as a private table does.

Q8 -- the same value looked up across all 100 datasets, ~11,000 rows -- is the only shape
that separates them:

| config            | Q8 warm (ms) | partitions visited |
|-------------------|-------------:|-------------------:|
| `unified`         |         16.5 |                 24 |
| `tiered`          |         22.2 |                 72 |
| `schema_per_user` |         28.3 |              2,400 |
| `per_dataset`     |         29.9 |              2,400 |
| `per_user`        |         66.6 |                480 |

**Q8 is a diagnostic, not a workload, and it decides nothing.** CLP does not issue queries
spanning multiple tenants: a query is scoped to its owner, and crossing that boundary is
excluded by authorization before it is excluded by performance. Q8 was built as the widest
possible fan-out to isolate one variable -- what it costs to answer a question from N tables
instead of one -- and it does isolate it: per-table configurations must issue a 100-branch
UNION visiting 2,400 partitions to answer what `unified` answers in one range over 24. But
no user of the system asks that question, so this table is evidence about a mechanism, not
about a workload, and the recommendation in section 7.0 does not rest on it.

The shape the matrix does not cover is the middle: one tenant querying across the several
datasets that tenant owns. Q1-Q7 all pin a single dataset; Q8 jumps to all 100. If that
middle shape occurs in production it is worth measuring, because it is the one fan-out that
survives the authorization boundary -- and under `per_dataset` it still requires a generated
UNION over table names, at whatever width the tenant's dataset count happens to be. See
section 8.0.

### 5.4 Retention under heterogeneous tiers (P3)

| config            | expiry (s) | statements | freed (MB) | rows past policy |
|-------------------|-----------:|-----------:|-----------:|-----------------:|
| `per_dataset`     |     440.79 |     16,800 |    2,166.8 |                0 |
| `schema_per_user` |     452.34 |     16,800 |    2,166.8 |                0 |
| `per_user`        |      93.00 |      3,360 |    2,625.0 |                0 |
| `unified`         |       0.00 |          0 |        0.0 |   **27,000,585** |
| `tiered`          |      13.94 |        480 |    2,195.4 |                0 |

This table describes the partition-drop retention model only. Under CLP's operative model
-- a background row-wise delete whose cost is not a decision metric -- every configuration
meets every policy exactly and nothing below disqualifies anything. It is recorded because
it is the strongest structural difference between the topologies, and because it prices the
`tiered` contingency should drop-based retention ever become a requirement.

`unified` issues zero statements not because it is efficient but because it cannot legally
drop anything: every hourly partition mixes 24 h, 72 h, and 168 h tenants, and a partition
can only be dropped when its longest tier has expired. The result is 27,000,585 postings --
exactly 50.0% of all data -- retained past their owners' policy. (The figure is arithmetic,
not noise: 20 users round-robin over 3 tiers is a 7/7/6 split, and 0.35 x 144/168 + 0.35 x
96/168 = 0.500.) Honoring the short tiers under a drop-only model would require row-wise
DELETEs -- which is what the operative retention model does anyway, so in practice this
disqualifies nothing.

`tiered` expires correctly with 480 statements in 13.9 s -- 32x faster than `per_dataset`'s
440.8 s across 16,800 statements, with zero rows past policy. Grouping by retention tier is
precisely what makes every partition drop-eligible the moment it expires.

---

## 6.0 Anomalies and limitations

### 6.1 The per_user anomaly (flagged, unexplained)

`per_user` shows two results that do not follow from its position between per_dataset and
unified: its side table is 22% larger than unified's despite a byte-identical row format
(5,093 vs 4,166 MB), and its Q8 is the slowest of all five configurations (66.6 ms) despite
visiting 5x fewer partitions than per_dataset. Both are single measurements; the size
suggests worse page fill under its particular write interleaving; the latency has no
explanation that survives scrutiny. Since per_user is not a candidate recommendation,
neither changes any decision -- but these cells should not be quoted without a rerun.

### 6.2 Cold latency was not captured, by construction

`phys_rd` is zero in 39 of 40 cells. The reason is sequencing, not pool size: each
configuration is queried immediately after its own build, so the load itself warms exactly
the pages the probes touch. No pool size fixes that. The honest cold measurement is a
targeted follow-up: restart the server (with the buffer-pool dump/reload disabled) and rerun
the matrix for `unified` and `tiered` only -- their 344 and 1,024 files reopen in seconds.
The current design is impractical to cold-benchmark this way, because tablespaces are
opened at startup (measured in 5.1), so a server holding 34,000 of them does 100x the
startup work of one holding 340. The wall time of that was NOT measured in this run; only
the mechanism is established.
Single-tenant shapes tie warm across all configurations, so the open cold question affects
only Q8 -- which section 5.3 excludes from the decision anyway. Nothing the recommendation
rests on is waiting on a cold number.

### 6.3 What the millisecond column actually times

Every query runs as a fresh `mysql -e "..."` subprocess, so each reported wall time includes
process spawn, client startup, socket connect, and the auth handshake -- a floor of several
milliseconds before the server does any work. Server-side execution for these shapes is
almost certainly sub-millisecond and is buried under that floor. The 2.5-4.1 ms figures are
therefore not a measurement of index work, and the five-way tie should NOT be read off them.

It should be read off the structural counters, which are server-side and exact: rows
examined identical at 104 / 816 / 417 / 1,714 and partitions visited identical at 24 / 168 /
48 across all five configurations. Identical rows touched and identical partitions opened is
direct evidence that the index work is the same; the timer only fails to contradict it.

This does not weaken the conclusion, because of what section 4.0's query step is FOR. The
side-table lookup is the candidate-reduction prefix of a user query, not the query: the user
waits on opening and searching each candidate archive, measured in earlier rounds at roughly
200 ms per archive. A metadata step of one millisecond or five is invisible against that. No
topology choice can move end-user query latency; the decision is a builder-side one.

### 6.4 Other limitations

- One engine, one machine, one run. Earlier rounds showed MySQL 8 amplifies every DDL and
  partition-count cost (up to 10x slower DDL, DROP PARTITION cost growing with table size),
  so these gaps should widen there, but that is extrapolation, not measurement.
- Ingestion throughput is deliberately out of scope; wall times are reported for context.
- Instance-per-user topologies (one database server per user) are out of scope here: their
  cost is a fixed per-instance floor (system tablespace, redo, buffer pool, monitoring),
  which is better measured as a small side study than simulated on one box.
- The 7/7/6 tier imbalance is an artifact of 20 users over 3 tiers; it changes the expected
  over-retention percentage but not any conclusion.

---

## 7.0 Recommendation

**One unified table set for everyone, with dataset_id leading every primary key.** Table
count is K, independent of datasets, users, and retention policies. Retention stays what it
operationally is: a background row-wise delete driven by each dataset's policy value in a
config table -- which also makes retention arbitrary and mutable per dataset for free (a
policy change is an UPDATE; the next delete cycle applies it, retroactively included).

**This does not make `unified` immune to the file-handle ceiling; it makes the ceiling
predictable.** Tablespace files are `K x (retention_hours + 2)`, so `unified` still crosses
`innodb_open_files = 2000` at a long enough retention or a wide enough schema -- K=6 kinds
at 30-day retention is 4,332 files, over the cap with a single tenant. The difference is
which input moves it. Under `unified` the inputs are retention length and schema breadth:
both chosen at design time, both computable exactly before deployment, neither growing on
its own. Under `per_dataset` the same formula carries a factor of N, so the input that moves
it is onboarding a customer -- crossing is not a design decision but a consequence of
growth, and it continues without bound (1,000 datasets at K=6 and 30 days is 4.3 M files,
beyond any workable fd limit). When `unified` does cross, the levers are raising the cap
against a known bounded number, coarsening partitions to daily (that same K=6 30-day case
becomes 192 files), or reducing K. Those levers exist for `per_dataset` too, but they are
also divided by N, so they do not rescue it.

Compared with today's per-dataset layout the differences are 100x fewer tablespace files,
100x less idle allocation, 36% less physical write bandwidth, and 3.4x less ingest wall
time. Ordered by whether they can matter -- which is not the same as ordered by ratio:

| cost of `per_dataset`  | at 100 datasets | at 10,000 datasets, K=6 | does it matter?      |
|------------------------|-----------------|-------------------------|----------------------|
| tablespace files       | 34,000          | 10.2 M                  | **YES -- a ceiling** |
| idle allocation        | 2,125 MB        | 637 GB IF near-empty    | only if tenants tiny |
| extra write bandwidth  | ~9 KB/s         | ~900 KB/s               | no                   |
| extra ingest wall time | 278 s / 7 days  | ~7.7 h / 7 days         | no -- buy hardware   |

Only the first row survives scrutiny, and the honest reading of this table is that the
recommendation rests on it almost alone.

The idle-allocation row assumes every partition sits at the 64 KB floor, i.e. that all
10,000 datasets are nearly empty. At this run's density each (dataset, partition) holds
~251 KB of real data, so the floor is absorbed and contributes nothing; the figures are not
additive with actual storage, and at 10,000 REAL datasets the metadata is ~434 GB of which
the floor is noise. That row is a hazard of careless dataset creation, not a property of the
topology. The bottom two rows are rounding errors in absolute terms (5.2) and in any case
are things hardware solves.

The first row is different in kind: file descriptors and the 8,192-partitions-per-table
limit are ceilings, not gradients, and `per_dataset` approaches them by signing customers,
independent of how much data anyone sends. `K x N x (hours + 2)` reaches the maximum
grantable `ulimit -n` of 1,048,576 at roughly 1,030 datasets under 7-day hourly partitions
with K=6, at ~242 datasets under 30-day hourly, and at ~68 datasets under 7-year daily.
Those are ordinary configurations, not asymptotes. `unified` needs `K x (hours + 2)` for the
entire deployment -- 1,020 files at 7-day hourly, 15,354 at 7-year daily -- forever
independent of tenant count.

The recommendation therefore does not rest on any figure's magnitude at this run's scale --
converted to absolutes, most of them are small or conditional. It rests on two things: that
`unified` removes the factor of N from the one quantity that has a hard ceiling, and on this
benchmark's central NEGATIVE result -- that co-locating 100 tenants in one B-tree costs a
single-tenant query nothing measurable (5.3). The ceiling could have been derived on paper.
The negative result could not, and without it `unified` would be unviable whatever its file
count.

The case is entirely a builder-side and operational one. Query latency does not choose
between these topologies: every shape CLP actually serves is scoped to one dataset, and on
those shapes all five configurations tie at 2.5-4.1 ms. `unified` is not recommended because
it queries faster -- it does not -- but because it is the only configuration whose file
count, idle footprint, and table count stop growing with the number of tenants, while
costing a single-tenant query nothing.

The refactor must carry three invariants:

1. **dataset_id is mandatory in every query.** It leads the key, so omitting it forfeits the
   index seek: the earlier tenancy round measured the omission at 25-50x. This belongs in
   the query builder's signature, not in code review.
2. **Archive identity is (dataset_id, archive_id).** Every join additionally binds
   begin_timestamp (the ts-equality rule from the side-table rounds). Getting this wrong
   returns another tenant's rows, not a slow query.
3. **Offboarding is the same delete job.** A departed tenant is a retention policy of zero.

Contingency: if drop-based retention ever becomes a requirement (e.g. a compliance regime
that forbids relying on a delete job), section 5.4 shows `tiered` -- one table set per
retention value in use -- is the cheap correct shape, and partitioning by expiration time
instead of begin time achieves the same without table proliferation. Neither is part of
this recommendation.

The naming refactor alone (`schema_per_user`, or renaming tables by dataset id) is measured
here as physically null: adopt it only as an interim step for SQL hygiene, never as the fix.

---

## 8.0 Open questions

1. **Concurrent multi-tenant query load.** Every query in this run executed alone on an idle
   server. The effect that can only appear under concurrency is the one specific to the
   design being recommended: latch contention on a single shared B-tree's hot pages, where
   `unified` concentrates every tenant's writes and reads into one tree that `per_dataset`
   spreads across 200. Nothing here measures it, and it is the most valuable missing
   experiment. (File-handle saturation is no longer in this list: 5.1 measures it.)
2. **Delete-job interference.** The retention job's own cost is out of scope, but a
   sustained background delete churns the buffer pool and purge, which can surface in
   foreground query latency -- the one retention-adjacent effect that touches a metric we
   care about. Unmeasured.
3. **Same-tenant, multi-dataset fan-out.** Does a tenant ever query across the several
   datasets it owns in one request? If so it is the only fan-out shape that survives the
   authorization boundary, and the matrix has no measurement of it (5.3). Adding it is
   small: Q8's structure at width `datasets-per-user` instead of width 100. The expected
   result is a latency tie, leaving the generated-UNION construction cost as the only
   difference -- but that is a prediction, not a measurement.
4. The per_user anomalies (6.1) deserve one rerun before that configuration is described in
   any external document.
5. The targeted cold pass on `unified` (6.2).
6. The same matrix on MySQL 8, where partition-count costs are known to be larger.
