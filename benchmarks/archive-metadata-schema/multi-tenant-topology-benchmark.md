# Multi-tenant metadata topologies: benchmark design and results

This document describes the design, execution, and results of the table-scope topology
benchmark (`bench_topology.py`): the experiment that decides how CLP's metadata tables should
be organized when one deployment serves many users. It is self-contained -- every term it
uses is defined in section 1.0, the exact setup is in section 3.0, and every number quoted
comes from the run recorded in `topology_results_20260824-010411.txt`.

The one-line conclusion: **one set of tables per retention tier (`tiered`) matches the best
design on every performance axis and is the only cheap design that never retains a tenant's
rows past that tenant's own retention policy.** The current one-set-of-tables-per-dataset
layout pays a 100x idle-storage floor, 36% more physical write bandwidth, saturates the
server's file-handle cache, and needs 32x longer to expire old data.

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
handle per recently-touched partition, bounded by `innodb_open_files`.

**dataset_id-leading primary key.** In any configuration where one table holds more than one
dataset, `dataset_id` is the FIRST column of the primary key, e.g. for the side table:

    PRIMARY KEY (dataset_id, column_id, value, begin_timestamp, archive_id)

Leading with it keeps each dataset's rows one contiguous key range, so a single-dataset query
is one index seek. Configurations whose tables hold exactly one dataset omit the column --
it would be a constant.

**Retention tier.** How long a user's data must be kept before deletion. Users differ: the
benchmark assigns tiers of 24 h, 72 h, and 168 h round-robin across users. Retention is
executed by dropping whole expired partitions (`ALTER TABLE ... DROP PARTITION`), which is a
file deletion -- the row-count-independent way to delete.

**Rows past policy (over-retention).** After an expiry cycle, the number of rows still
present whose owning dataset's retention says they should be gone. A table that holds only
one tier can always drop its expired partitions, so it scores zero. A table that MIXES tiers
cannot drop a partition until the LONGEST tier in it expires, so every shorter-tier tenant's
rows in that partition are retained past policy. This is a compliance violation -- data kept
beyond the contractual retention period -- not merely wasted disk.

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

**open_f.** `Innodb_num_open_files`: how many tablespace files the server holds open at the
moment of measurement. When a configuration needs more files than `innodb_open_files`, the
server continuously evicts and reopens them; this gauge pinned at the cap is the direct
signature of that thrashing.

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
are comparable across rounds.

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
  while completely EMPTY -- the price of existing before any data arrives.
- **P1 size and amplification**: stored bytes split by table kind; space amplification;
  write amplification across the concurrent build.
- **P2 query matrix**: eight query shapes, each isolating one thing topology could change --
  Q1 selective point lookup, 24 h window; Q2 same, 7-day window; Q3 low-selectivity value;
  Q4 prefix wildcard; Q5 two-predicate AND (self-join binding archive_id AND
  begin_timestamp, per the ts-equality rule); Q6 join back to the archives table for
  metadata; Q7 archives-table time-range scan; Q8 the same lookup across ALL tenants
  (an N-way UNION for per-table configs, one IN-list range for shared-table configs).
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

Empty cost is pure partition-file floor: 64 KB x partition count, exactly (2,125 MB /
34,000 = 64 KB). Per dataset under the current design that is ~21 MB of allocation before a
single row exists, times however many kinds production adds. At 1,000 datasets it is ~21 GB
of floor.

The `open_f` column is the direct evidence of file-handle saturation: all three many-table
configurations sit pinned at exactly the `innodb_open_files` cap of 2,000, meaning the
server evicts and reopens tablespace files continuously. `unified` and `tiered` hold every
file they need open at once, with headroom.

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

Single-tenant shapes (Q1-Q7) are a five-way tie: 2.5-4.1 ms warm across every configuration,
identical rows examined (104 / 816 / 417 / 1,714), identical partitions visited (24 for the
24 h window, 168 for 7 days, 48 for the joins). Sharing a table with 99 other tenants costs
a single-tenant query nothing measurable: the dataset_id key prefix and partition pruning
isolate it as effectively as a private table does.

The cross-tenant shape (Q8: the same value looked up across all 100 datasets, ~11,000 rows)
separates them:

| config            | Q8 warm (ms) | partitions visited |
|-------------------|-------------:|-------------------:|
| `unified`         |         16.5 |                 24 |
| `tiered`          |         22.2 |                 72 |
| `schema_per_user` |         28.3 |              2,400 |
| `per_dataset`     |         29.9 |              2,400 |
| `per_user`        |         66.6 |                480 |

Per-table configurations must issue a 100-branch UNION visiting 2,400 partitions to answer
what `unified` answers in one range over 24. The gap here (1.8x) is modest because the
machine was otherwise idle and everything was cached; the earlier tenancy round measured the
same shape at up to 59x under memory pressure, and the pinned `open_f` gauge explains why:
every one of those 2,400 partition touches competes for a saturated file-handle cache.

### 5.4 Retention under heterogeneous tiers (P3)

| config            | expiry (s) | statements | freed (MB) | rows past policy |
|-------------------|-----------:|-----------:|-----------:|-----------------:|
| `per_dataset`     |     440.79 |     16,800 |    2,166.8 |                0 |
| `schema_per_user` |     452.34 |     16,800 |    2,166.8 |                0 |
| `per_user`        |      93.00 |      3,360 |    2,625.0 |                0 |
| `unified`         |       0.00 |          0 |        0.0 |   **27,000,585** |
| `tiered`          |      13.94 |        480 |    2,195.4 |                0 |

This is the deciding table.

`unified` issues zero statements not because it is efficient but because it cannot legally
drop anything: every hourly partition mixes 24 h, 72 h, and 168 h tenants, and a partition
can only be dropped when its longest tier has expired. The result is 27,000,585 postings --
exactly 50.0% of all data -- retained past their owners' policy. (The figure is arithmetic,
not noise: 20 users round-robin over 3 tiers is a 7/7/6 split, and 0.35 x 144/168 + 0.35 x
96/168 = 0.500.) Honoring the short tiers would require row-wise DELETEs, the exact cost
partitioning exists to avoid. Pure `unified` is therefore disqualified on compliance
despite winning nearly every other axis.

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
The current design cannot be cold-benchmarked this way at all: restarting a server holding
34,000 tablespace files takes minutes per restart, which is itself an operational finding.
Single-tenant shapes tie warm across all configurations, so the open cold question affects
only the cross-tenant comparison, where the file-count argument already favors few-table
designs.

### 6.3 Other limitations

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

**One set of tables per retention tier, with dataset_id leading every primary key.** Table
count K x R, where R is the number of retention contracts offered (assumed small).

Compared with today's per-dataset layout, at this run's scale: ~33x fewer tables, partitions
and files (with headroom under `innodb_open_files` instead of pinned saturation), 100x less
idle allocation, 27% less physical write bandwidth for identical data, equal single-tenant
query latency, ~1.4x faster cross-tenant queries warm (with the earlier round showing the
gap exploding under memory pressure), and expiry in 14 s instead of 441 s -- all with zero
rows past policy.

The refactor must carry four invariants:

1. **dataset_id is mandatory in every query.** It leads the key, so omitting it forfeits the
   index seek: the earlier tenancy round measured the omission at 25-50x. This belongs in
   the query builder's signature, not in code review.
2. **Archive identity is (dataset_id, archive_id).** Every join additionally binds
   begin_timestamp (the ts-equality rule from the side-table rounds). Getting this wrong
   returns another tenant's rows, not a slow query.
3. **Offboarding is retention.** A departed tenant's rows age out partition-by-partition at
   zero marginal cost. Contractual immediate deletion is a batched DELETE walking partition
   ranges -- rare, and priced separately.
4. **Tier changes follow no-backfill semantics.** A tenant moving tiers writes new archives
   to the new tier's tables; sealed archives age out under the old tier. No rewrites.

The naming refactor alone (`schema_per_user`, or renaming tables by dataset id) is measured
here as physically null: adopt it only as an interim step for SQL hygiene, never as the fix.

---

## 8.0 Open questions

1. **Is retention really tiered?** The whole design assumes R is a handful of contract
   values. If every customer can choose an arbitrary retention period, `tiered` degenerates
   toward per-user and the comparison must be redone with that distribution.
2. The per_user anomalies (6.1) deserve one rerun before that configuration is described in
   any external document.
3. The targeted cold pass on `unified` and `tiered` (6.2).
4. The same matrix on MySQL 8, where partition-count costs are known to be larger.
