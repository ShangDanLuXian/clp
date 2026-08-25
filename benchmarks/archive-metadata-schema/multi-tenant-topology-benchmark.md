# Multi-tenant metadata topologies: benchmark design and results

How should CLP's metadata tables be organized when one deployment serves many users? This
records the experiment that answers it (`bench_topology.py`), run at 100 datasets and 54 M
postings; every number comes from `topology_results_20260824-010411.txt`.

**Conclusion: one table set for everything, with `dataset_id` leading every primary key.**
Two results carry it:

- **One table set per dataset walks into hard ceilings.** Its tablespace count is
  `K x N x (partitions + 2)`: it grows by onboarding datasets, independent of how much data
  anyone sends, toward the process file-descriptor limit and the engine's 8,192-partitions-
  per-table cap (section 5.0). The shared set needs `K x (partitions + 2)` for the entire
  deployment, independent of tenant count.
- **Co-locating 100 tenants in one B-tree costs a single-tenant query nothing measurable**
  (section 4.2). The ceiling argument could have been derived on paper; this one could not,
  and without it the shared design would be unviable whatever its file count.

Everything else measured -- write amplification, ingest time, file-handle churn, idle
allocation -- turns out to be small, conditional, or solvable with hardware once converted
from ratios to absolutes.

---

## 1.0 Terminology

**Topology / configuration.** A rule for how many physical tables hold the metadata of N
datasets, and what their primary key looks like. Four are compared (section 2.0); nothing
else varies -- same rows, same partitioning, same server.

**Table kind (K).** The schema has several logical tables per dataset: the archives table,
the filter-postings side table, and in production files, tags and others. Per-table costs
multiply by K. The benchmark builds two representative kinds:

- `arch` -- one row per archive (id, begin/end timestamp, size). Narrow, low row count.
- `side` -- the filter posting list, ~45 rows per archive. Dominates bytes and rows.

**Partition / tablespace file.** Every table is `PARTITION BY RANGE (begin_timestamp)` into
hourly partitions, and InnoDB stores each partition as its own file. A fresh tablespace
allocates 64 KB before holding anything, and the server holds an open file handle per
populated partition, bounded by `innodb_open_files`.

**dataset_id-leading primary key.** Where one table holds more than one dataset,
`dataset_id` is the FIRST key column:

    PRIMARY KEY (dataset_id, column_id, value, begin_timestamp, archive_id)

This keeps each dataset's rows in one contiguous key range, so a single-dataset query is one
index seek. Configurations whose tables hold exactly one dataset omit it as a constant.

**Logical payload.** Rows times raw column widths, zero storage overhead: 1,301.2 MB for
this run. It is the denominator of both amplification figures, so they measure pure
overhead. **Space amplification** is stored bytes (`data_length + index_length`) over
payload. **Write amplification** is (`Innodb_data_written` + `Innodb_os_log_written`) across
the build over payload -- what InnoDB actually pushed to storage, page writes plus redo.

**Open files.** `Innodb_num_open_files`, which equals
`min(tablespaces holding data, innodb_open_files)`. Files are opened at server startup, not
by the queries that read them (measured separately by `check_file_handles.sh`), so this
describes the INSTANCE, never a query's time window.

---

## 2.0 The four configurations

| configuration                | tables | dataset_id in key | what it models                     |
|------------------------------|--------|-------------------|------------------------------------|
| one set per dataset          | K x N  | no                | today's design, tables named by ds |
| one set per user             | K x U  | yes               | tables scoped to the owner         |
| one set for everything       | K      | yes               | a single shared table set          |
| one set per retention period | K x R  | yes               | grouped by how long data lives     |

N = datasets (100 here), U = users (20), R = distinct retention periods (3), K = table kinds
(2 here; more in production, so table counts scale accordingly).

Not compared: one database instance per user holding a shared, dataset_id-keyed table set.
Its per-instance behaviour is the shared configuration at U=1; what it adds -- a fixed
per-instance floor (system tablespace, redo, buffer pool, monitoring) times U -- was not
measured here.

**One set per retention period** is included for completeness only. It is unworkable in
practice: retention is a per-dataset, user-mutable setting, and under this grouping a policy
change means migrating every row the dataset owns into a different table set. See 4.3.

---

## 3.0 Setup

**Scale.** 20 users x 5 datasets x 12,000 archives = 100 datasets, 1.2 M archive rows and
54,000,000 side-table postings per configuration. Every table has 168 hourly partitions plus
a floor and a future catch-all: 170 files. Retention periods of 24/72/168 h assigned
round-robin by user.

**Data shape.** Each archive contributes 45 postings across 8 filter columns following the
`c8` shape used in every earlier round -- hosts at 1,000 distinct values, envs at 12,
severities at 40, modules at 300. The 45 is a cardinality-WITHIN-archive assumption, not a
size ceiling. `c8`'s payload is 577 B per archive, about 11% of the 5,368 B admission
budget (0.1% of the compressed archive); the widest admissible shape is 8.9x larger, so the
absolute sizes below are a conservative corner of the space. The comparison is unaffected --
all configurations ran identical data.

**No compression.** Tables were created without `ROW_FORMAT` or `KEY_BLOCK_SIZE`, so
everything ran at InnoDB's default uncompressed `DYNAMIC`. A separate round measured
`ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8` at ~61% smaller at these value widths (see
`dictionary-encoding.md`), so the sizes in 4.1 would fall by roughly two-thirds under the
recommended row format.

**Write path.** 16 worker processes run concurrently, each owning a slice of the datasets,
issuing per-archive INSERTs interleaved in timestamp order -- tenants ingest simultaneously
in production, and a tenant-by-tenant load resembles a sorted bulk load that flatters
whichever design has the fewest B-trees.

**Server and measurement.** MariaDB 10.6.23, 4 GB buffer pool, `innodb_open_files = 2000`.
Each configuration is created, loaded, queried, expired and dropped in sequence. Sizes from
`information_schema` after `ANALYZE TABLE`; file counts from `INNODB_SYS_TABLESPACES`; rows
examined from `Handler_read_*` after `FLUSH STATUS`; partitions visited from
`EXPLAIN PARTITIONS`.

---

## 4.0 Results

### 4.1 Structure, size, and amplification

| configuration | tables | partitions | open files | empty MB | arch MB | side MB | space | write |
|---------------|-------:|-----------:|-----------:|---------:|--------:|--------:|------:|------:|
| per dataset   |    200 |     34,000 |  **2,000** |  2,125.0 |   265.6 | 4,074.6 | 3.34x | 15.91x |
| per user      |     40 |      6,800 |  **2,000** |    425.0 |   158.1 | 5,093.1 | 3.73x | 12.64x |
| shared        |      2 |        340 |        344 |     21.2 |   131.6 | 4,166.2 | 3.06x | 11.71x |
| per retention |      6 |      1,020 |      1,024 |     63.8 |    96.7 | 4,276.5 | 3.11x | 11.99x |

(Row labels abbreviate the configuration names of section 2.0; "space"/"write" are the two
amplification figures.)

Load wall times, informational: 396 s, 263 s, 118 s, 119 s in table order.

**Empty cost** is exactly 64 KB x partition count -- the initial allocation of a fresh
tablespace. It is a steady-state floor (production reaches this partition count after one
retention period, not on day one) and it lasts only where partitions stay sparse: at ~81
stored bytes per posting, 64 KB is absorbed by ~18 archives per partition, and these
datasets produce 71 per hour. It bites only with many near-empty datasets (~65 MB each at
K=6), which is a hazard of creating empty datasets, not a cost of scale.

**Open files**: the three many-table configurations sit pinned at the 2,000 cap, meaning
more tablespaces exist than cache slots. `check_file_handles.sh` measured what that costs:
an identical scan at 17x oversubscription -- the same ratio as 34,000 tablespaces against a
2,000 cap -- runs about 11% slower from handle evict/reopen, with physical page reads equal.
A modest tax, not a cliff. The real file-count problem is the ceiling in section 5.0.

**Write amplification** is 36% higher for the per-dataset layouts (15.9x vs 11.7x):
identical rows, but flushing through 34,000 small partitions writes more partially-filled
pages than flushing through 340. Converted to absolutes it stops mattering: the difference
is 5,465 MB over the seven days the data represents, ~9 KB/s sustained, ~285 GB/year against
SSD endurance measured in petabytes. Load wall times deflate the same way -- the 1.2 M
archives represent 168 h of ingest loaded in 396 s, so even the slowest configuration is
over-provisioned for its own ingest by ~1,500x.

One structural note: the per-dataset layout stores slightly LESS than the shared ones
(4,074.6 vs 4,166.2 MB side). Its rows genuinely omit the 2-byte `dataset_id`, and the 92 MB
difference is exactly that column -- the shared design does not pack data better.

### 4.2 Query matrix

Eight query shapes. Rows examined and partitions visited are identical across all four
configurations for Q1-Q7 (Q7 within 1.4%), which is the evidence that sharing a table with
99 other tenants costs a single-dataset query nothing: the `dataset_id` key prefix and
partition pruning isolate it as effectively as a private table.

| query                        | rows examined | partitions visited |
|------------------------------|--------------:|-------------------:|
| Q1 point lookup, 24 h        |           104 |                 24 |
| Q2 point lookup, 7 d         |           816 |                168 |
| Q3 low-selectivity value     |           417 |                 24 |
| Q4 prefix wildcard           |           104 |                 24 |
| Q5 two-predicate AND (join)  |           104 |                 48 |
| Q6 join for archive metadata |           104 |                 48 |
| Q7 archives time range       |   1,714-1,738 |                 24 |
| Q8 same lookup, ALL tenants  |       ~11,050 |     24 up to 2,400 |

Wall times per configuration, milliseconds:

| query | per-dataset | per-user | shared | per-retention |
|-------|------------:|---------:|-------:|--------------:|
| Q1    |         2.6 |      2.6 |    3.1 |           2.5 |
| Q2    |         3.6 |      3.5 |    4.1 |           3.7 |
| Q3    |         3.3 |      2.8 |    2.8 |           2.8 |
| Q4    |         2.7 |      2.7 |    2.6 |           3.0 |
| Q5    |         3.1 |      2.8 |    3.2 |           2.9 |
| Q6    |         2.8 |      2.8 |    3.0 |           2.8 |
| Q7    |         2.9 |      2.7 |    2.7 |           2.7 |
| Q8    |        29.9 |     66.6 |   16.5 |          22.2 |

Read the Q1-Q7 comparison from the counters, not the timer: each query runs as a fresh
`mysql -e` subprocess, so every wall time carries several milliseconds of client startup and
connection before the server does any work, and server-side execution for these shapes is
buried under that floor. Identical rows examined and partitions visited is the direct
evidence. Two counter details worth noting: the only nonzero table-cache miss in the matrix
lands on Q5 for the 200-table layout (the self-join opens a second table reference, and only
the layout with 200 tables misses the cache on it), and Q7 examines 24 more rows under it
(1,738 vs 1,714), one per partition visited, from the different archives-table key layout.

**Q8 is a diagnostic, not a workload, and decides nothing.** CLP does not issue queries
spanning tenants -- authorization excludes them before performance enters into it. The shape
exists to isolate what answering from N tables costs versus one: the per-dataset layout
needs a 100-branch UNION over 2,400 partitions (29.9 ms) where the shared table answers in
one range over 24 (16.5 ms). Also relevant per-query cost: a metadata lookup is the candidate-
generation prefix of a user query, and the user's wait is dominated by opening and searching
each candidate archive (~200 ms per archive in earlier rounds), so no per-query difference
at this scale is user-visible under any topology.

The matrix does not cover the one fan-out that survives authorization: a tenant querying
across the several datasets it owns. Under the per-dataset layout that still requires a
generated UNION over table names. See 6.0.

### 4.3 Retention

Retention was exercised once, under heterogeneous periods, using partition drops -- and it
decides nothing. CLP expires data with a background row-wise DELETE keyed on each dataset's
policy value, whose cost is out of scope; under that model every configuration meets every
policy exactly, and a policy change is an UPDATE that the next delete cycle applies. For the
record: under drop-based expiry the shared table cannot legally drop mixed partitions and
over-retained 27.0 M postings (exactly 50.0% -- the arithmetic of the 7/7/6 period split),
while per-retention grouping expired cleanly (480 statements, 13.9 s, vs 16,800 statements
and 440.8 s for per-dataset). That grouping is still unworkable, because user-mutable
retention would migrate whole datasets between table sets on every policy change.

---

## 5.0 Partition granularity, long retention, and the hard limits

Two limits bound the design regardless of topology, and they interact with retention length.

**The engine caps a table at 8,192 partitions.** At hourly granularity that is 341 days:
7-year retention (61,368 hours, needing 61,370 partitions) is not expressible -- the CREATE
TABLE fails. **The finest granularity that can express 7 years is 12-hour partitions**
(5,116 partitions, 1.6x headroom); 6-hourly still exceeds the cap at 10,230. Daily gives
2,559 partitions with 3.2x headroom, enough to extend to ~22 years without repartitioning.

**Coarser partitions cost queries almost nothing**, because `begin_timestamp` is a key
column: pruning is only the coarse first cut, and the index does the fine time filtering
inside each partition. A 24 h query touches 2 twelve-hour partitions instead of 24 hourly
ones -- fewer B-trees to seek, not more rows to scan. What coarsening does trade away is the
empty-floor threshold (4.1), which scales with the partition window: ~18 archives per hour
becomes ~18 per day.

**File counts at long retention decide the `innodb_open_files` setting, and the topology
decides whether that number is bounded.** At K=6 kinds, 7-year retention:

| granularity | partitions/table | files, one shared set | files, per dataset x 1,000 |
|-------------|-----------------:|----------------------:|---------------------------:|
| 12-hourly   |            5,116 |                30,696 |                     30.7 M |
| daily       |            2,559 |                15,354 |                     15.4 M |

Under the shared set the total is `K x (partitions + 2)` -- computable exactly at design
time, set the cap above it once, and it never grows. Under one set per dataset the same
formula carries a factor of N: it reaches the maximum grantable file-descriptor limit
(`ulimit -n` = 1,048,576) at roughly 1,030 datasets with 7-day hourly partitions, ~242 at
30-day hourly, ~68 at 7-year daily -- ordinary configurations, reached by onboarding
datasets rather than by data volume, and past what backup, DDL, and filesystem directories
handle long before the kernel refuses.

---

## 6.0 Open questions

1. **Concurrent multi-tenant query load.** Every query here executed alone on an idle
   server. The effect that can only appear under concurrency is the one specific to the
   recommended design: latch contention on a single shared B-tree's hot pages, which one
   table set concentrates and per-dataset tables spread across 200 trees.
2. **Same-tenant, multi-dataset fan-out.** The only fan-out shape that survives
   authorization, and the matrix has no measurement of it. Cheap to add: Q8's structure at
   width `datasets-per-user` instead of width 100.
3. **Compression.** All sizes here are uncompressed. Whether the recommended row format
   changes the write-amplification comparison, and whether the 64 KB partition floor holds
   at an 8 KB page size, are unmeasured.
4. The same matrix on MySQL 8, where partition-count costs are known to be larger.
