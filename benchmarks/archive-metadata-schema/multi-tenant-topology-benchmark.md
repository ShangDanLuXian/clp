# Multi-tenant metadata topologies: benchmark design and results

How should CLP's metadata tables be organized when one deployment serves many users? This
records the experiment that answers it (`bench_topology.py`), run at 100 datasets and 54 M
postings; every number comes from `topology_results_20260824-010411.txt`.

**Conclusion: one table set for everything, with `dataset_id` leading every primary key.**

The decision rests on two things, and deliberately not on the rest:

- **One table set per dataset walks into a hard ceiling.** Its tablespace count is
  `K x N x (retention_hours + 2)` -- it grows by signing customers, independent of how much
  data anyone sends, and reaches the maximum grantable file-descriptor limit at ordinary
  configurations (~1,030 datasets at 7-day hourly partitions with 6 table kinds). File
  descriptors and the 8,192-partitions-per-table engine limit are ceilings, not gradients.
  One table set for everything needs `K x (retention_hours + 2)` for the entire deployment,
  independent of tenant count forever.
- **Co-locating 100 tenants in one B-tree costs a single-tenant query nothing measurable.**
  Identical rows examined and identical partitions visited against a private table. This is
  the benchmark's central result: the ceiling argument could have been derived on paper, but
  without this the design would be unviable whatever its file count.

Everything else the run measured -- write amplification, ingest time, file-handle churn,
idle allocation -- turns out to be small, conditional, or solvable with hardware once
converted from ratios to absolutes. Section 5 gives the numbers; section 7 ranks them.

---

## 1.0 Terminology

**Topology / configuration.** A rule for how many physical tables hold the metadata of N
datasets, and what their primary key looks like. Five are compared (section 2.0); nothing
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
by the queries that read them (measured in 5.1), so this describes the INSTANCE, never a
query's time window. Pinned at the cap means more tablespaces exist than cache slots, so
accesses outside the cached set must evict and reopen -- which 5.1 prices at roughly 10%,
not a cliff.

---

## 2.0 The five configurations

| configuration                | tables | dataset_id in key | what it models                     |
|------------------------------|--------|-------------------|------------------------------------|
| one set per dataset          | K x N  | no                | today's design, tables named by ds |
| per dataset, schema per user | K x N  | no                | same tables, one SCHEMA per user   |
| one set per user             | K x U  | yes               | tables scoped to the owner         |
| one set for everything       | K      | yes               | a single shared table set          |
| one set per retention period | K x R  | yes               | grouped by how long data lives     |

N = datasets (100 here), U = users (20), R = distinct retention periods (3), K = table kinds
(2 here; more in production, so table counts scale accordingly).

**Per dataset, schema per user** is deliberately a placebo. It changes only namespacing,
which fixes the SQL-construction problems of dataset-named tables -- identifiers built by
string interpolation, the 64-character identifier limit, sanitization collisions -- while
changing nothing physical. Its results test whether those problems can be fixed without the
operational refactor.

**One set per retention period** groups tenants by how long their data must be kept rather
than by who owns it. It exists only because of the retention finding in 5.4: any grouping
that mixes retention periods inside one partition forces over-retention under partition-drop
expiry, and retention period is the one grouping where whole partitions always expire
together.

---

## 3.0 Setup

**Scale.** 20 users x 5 datasets x 12,000 archives = 100 datasets, 1.2 M archive rows and
54,000,000 side-table postings per configuration. Every table has 168 hourly partitions plus
a floor and a future catch-all: 170 files. Retention periods of 24/72/168 h assigned
round-robin by user (20 over 3 gives a 7/7/6 split, which matters in 5.4).

**Data shape.** Each archive contributes 45 postings across 8 filter columns following the
`c8` shape used in every earlier round -- hosts at 1,000 distinct values, envs at 12,
severities at 40, modules at 300. The 45 is a cardinality-WITHIN-archive assumption, not a
size ceiling.

**Where `c8` sits in the admissible range.** Every absolute size figure below depends on
this. The service admits a filter configuration only if its payload stays under 0.1% of the
compressed archive -- 5,368 B per archive at 256 MB raw and 50x compression. `c8` uses
577 B, about 11% of that allowance; the widest admissible shape uses 5,117 B, 8.9x more. So
a deployment configured to the policy limit produces a side table roughly 8.9x larger than
section 5.2 reports. The topology COMPARISON is unaffected, since all five ran identical
data, but the magnitudes are a conservative corner of the space, not an upper bound.

**No compression.** The tables were created without `ROW_FORMAT` or `KEY_BLOCK_SIZE`, so
everything ran at InnoDB's default uncompressed `DYNAMIC`. A separate round measured
`ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8` at ~61% smaller at these value widths (see
`dictionary-encoding.md`), so the sizes in 5.2 would fall by roughly two-thirds under the
configuration an earlier round recommended. Whether compression changes the write-
amplification comparison is unmeasured.

**Write path.** 16 worker processes run concurrently, each owning a slice of the datasets,
issuing per-archive INSERTs interleaved in timestamp order. This matters: tenants ingest
simultaneously in production, so writes land in many key ranges at once. An earlier round
loaded tenant-by-tenant, which resembles a sorted bulk load and flatters whichever design
has the fewest B-trees.

**Server.** MariaDB 10.6.23, 4 GB buffer pool, `innodb_open_files = 2000`, one shared
instance. Each configuration is created, loaded, queried, expired and dropped in sequence,
so they never share cache state.

**Measurement sources.** Sizes from `information_schema` after `ANALYZE TABLE` (statistics
are cached and read stale otherwise); file counts and allocated bytes from
`INNODB_SYS_TABLESPACES.FILE_SIZE`; rows examined from `Handler_read_*` after `FLUSH
STATUS`; physical reads and write volumes from global counters differenced around each
operation; partitions visited from `EXPLAIN PARTITIONS`.

---

## 4.0 What is measured

- **Structure**: tables, partitions, open files, and allocated-but-unused bytes after CREATE
  and before any row exists.
- **Size and amplification**: stored bytes by table kind, space and write amplification
  across the concurrent build.
- **Query matrix**: eight shapes -- selective point lookup over 24 h and 7 d; a
  low-selectivity value; a prefix wildcard; a two-predicate AND (self-join binding both
  `archive_id` AND `begin_timestamp`); a join back to the archives table; an archives-table
  time-range scan; and one lookup spanning all tenants. Each records wall time, rows
  examined, physical reads, partitions visited and table-cache misses.
- **Retention**: one expiry cycle under heterogeneous retention periods -- wall time,
  statements issued, bytes freed, and rows still present past their owner's policy.

---

## 5.0 Results

### 5.1 Structure and idle cost

| configuration                | tables | partitions | open files | empty (MB) |
|------------------------------|-------:|-----------:|-----------:|-----------:|
| one set per dataset          |    200 |     34,000 |  **2,000** |    2,125.0 |
| per dataset, schema per user |    200 |     34,000 |  **2,000** |    2,125.0 |
| one set per user             |     40 |      6,800 |  **2,000** |      425.0 |
| one set for everything       |      2 |        340 |        344 |       21.2 |
| one set per retention period |      6 |      1,020 |      1,024 |       63.8 |

Empty cost is exactly 64 KB x partition count (2,125 MB / 34,000 = 64 KB): the initial
allocation of a fresh tablespace at the default 16 KB page size.

**Two qualifications narrow this row to near-irrelevance for real datasets.** It is a
steady-state figure rather than a day-one cost: partitions are created by DDL, never by data
arriving, and production grows the window forward and reaches this count only after one
retention period. And the floor lasts only where partitions stay SPARSE -- at ~81 stored
bytes per posting, 64 KB holds about 18 archives, and these datasets produce 71 per hour, so
their own data had absorbed it entirely by the end of the run.

It bites only with many SMALL tenants, where a fixed per-(dataset, partition) cost has
nothing to amortize against: a near-idle dataset at K=6 allocates ~65 MB to hold almost
nothing. The two figures are alternatives, never a sum -- 10,000 REAL datasets at this
density hold ~434 GB of actual metadata with the floor absorbed inside it. So this is a
hazard of creating empty datasets, not a cost of scale.

**Open files, measured separately.** The three many-table configurations sit pinned at the
2,000 cap. `check_file_handles.sh` establishes what that does and does not mean:

- *The gauge describes the instance.* Counting the server's open descriptors by name in
  `/proc/<pid>/fd`, 60 of a table's 61 partition files were already open after a clean
  restart with the table untouched, with buffer-pool dump and restore both disabled. Queries
  pruned to one partition, to ten, and a full sweep each added nothing. Thrash is therefore
  an instance-level condition -- total tablespaces exceeding the cap -- not something a wide
  query triggers.
- *Saturation costs about 10%, not a cliff.* Identical data, identical 32 MB pool, identical
  sweep of 200 partitions, varying only the cap: 841 ms at 1,000 (all files open), 839 ms at
  300, 823 ms at 30, 935 ms at 12. Physical page reads were equal throughout (~42,200), so
  the difference is evict/reopen and not I/O volume. The worst row is 17x oversubscribed --
  the same ratio as 34,000 tablespaces against a 2,000 cap -- for roughly 11%. That sweep is
  I/O bound, which under-states churn as a fraction.

This also explains why 5.3 finds no query penalty, and why that is not a contradiction: the
query shapes touch the same 24 partitions repeatedly, those stay resident in the handle LRU,
and nothing churns however many tablespaces exist elsewhere.

So file-handle saturation is a modest tax, not the load-bearing finding. What the file count
costs is the ceiling in section 7.0, plus startup work that scales with tablespace count for
the same reason the gauge does -- though this run never timed that.

### 5.2 Size and write amplification

| configuration                | arch (MB) | side (MB) | total (MB) | space amp | write amp |
|------------------------------|----------:|----------:|-----------:|----------:|----------:|
| one set per dataset          |     265.6 |   4,074.6 |    4,340.2 |     3.34x |    15.91x |
| per dataset, schema per user |     265.6 |   4,074.6 |    4,340.2 |     3.34x |    16.21x |
| one set per user             |     158.1 |   5,093.1 |    5,251.2 |     3.73x |    12.64x |
| one set for everything       |     131.6 |   4,166.2 |    4,297.7 |     3.06x |    11.71x |
| one set per retention period |      96.7 |   4,276.5 |    4,373.2 |     3.11x |    11.99x |

Logical payload 1,301.2 MB. Load wall times, informational: 396 s, 400 s, 263 s, 118 s,
119 s in table order.

**Both spreads mislead as ratios and must be converted.** The 1.2 M archives represent 168 h
of ingest -- 604,800 s of real time -- loaded in 396 s. The required steady-state rate is ~2
archives/s against a demonstrated ~3,030/s, so the worst configuration is over-provisioned
for its own ingest by roughly 1,500x, and the 3.4x spread is 3.4x on a quantity with three
orders of magnitude of slack.

Write amplification deflates the same way: 15.91x of 1,301.2 MB is 20,700 MB over seven days
against 15,240 MB, a difference of 5,465 MB, or about **9 KB/s sustained** -- ~285 GB/year
against SSD endurance measured in petabytes. The 36% figure is real and reproducible, and at
this scale it is an operational rounding error.

Neither is a reason to prefer any configuration. They are reported because they scale
linearly with tenant count and because they corroborate the mechanism -- many B-trees
dirtied concurrently versus few -- not because their magnitudes matter here.

Two structural notes. Today's layout stores slightly LESS data than the shared ones (4,074
vs 4,166 MB) because its rows genuinely omit the 2-byte `dataset_id`; that 92 MB difference
is exactly that column, so the shared design does not pack data better. And the schema-per-
user variant matches it on every physical number, confirming it is naming-only: it fixes SQL
construction and fixes nothing else. The one-set-per-user row is anomalous -- see 6.1.

### 5.3 Query matrix

**The single-dataset shapes are a five-way tie**, and this is the benchmark's most important
result. Identical rows examined (104 / 816 / 417 / 1,714), identical partitions visited (24
for the 24 h window, 168 for 7 days, 48 for the joins), wall times all inside 2.5-4.1 ms.
Sharing a table with 99 other tenants costs a single-tenant query nothing measurable: the
`dataset_id` key prefix and partition pruning isolate it as effectively as a private table.

Read that off the structural counters rather than the timer -- see 6.2 for why the
millisecond column cannot resolve this.

**The all-tenant shape separates them, and decides nothing.**

| configuration                | warm (ms) | partitions visited |
|------------------------------|----------:|-------------------:|
| one set for everything       |      16.5 |                 24 |
| one set per retention period |      22.2 |                 72 |
| per dataset, schema per user |      28.3 |              2,400 |
| one set per dataset          |      29.9 |              2,400 |
| one set per user             |      66.6 |                480 |

CLP does not issue queries spanning multiple tenants -- a query is scoped to its owner, and
crossing that boundary is excluded by authorization before performance enters into it. This
shape was built as the widest possible fan-out to isolate one variable, what it costs to
answer a question from N tables instead of one, and it does isolate it: the per-table
layouts must issue a 100-branch UNION over 2,400 partitions to answer what one shared table
answers in a single range over 24. It is evidence about a mechanism, not about a workload.

The matrix does not cover the middle: one tenant querying across the several datasets that
tenant owns. That is the only fan-out shape surviving the authorization boundary, and under
today's layout it still requires a generated UNION over table names. See 8.0.

### 5.4 Retention

| configuration                | expiry (s) | statements | freed (MB) | rows past policy |
|------------------------------|-----------:|-----------:|-----------:|-----------------:|
| one set per dataset          |     440.79 |     16,800 |    2,166.8 |                0 |
| per dataset, schema per user |     452.34 |     16,800 |    2,166.8 |                0 |
| one set per user             |      93.00 |      3,360 |    2,625.0 |                0 |
| one set for everything       |       0.00 |          0 |        0.0 |   **27,000,585** |
| one set per retention period |      13.94 |        480 |    2,195.4 |                0 |

**This table describes an alternative CLP does not use, and decides nothing.** Retention is
a background row-wise DELETE driven by each dataset's policy value, and its cost is out of
scope; under that model every configuration meets every policy exactly. It is kept only
because it is the strongest structural difference between the topologies, and because it
prices the per-retention-period contingency.

The shared table issues zero statements not because it is efficient but because it cannot
legally drop anything: every hourly partition mixes 24 h, 72 h and 168 h tenants, and a
partition survives until its longest-lived contents expire. The 27,000,585 postings past
policy is exactly 50.0% of all data, and it is arithmetic rather than noise -- a 7/7/6 split
over three periods gives 0.35 x 144/168 + 0.35 x 96/168 = 0.500.

---

## 6.0 Limitations

### 6.1 The one-set-per-user anomaly

Two results do not follow from its position between the per-dataset and fully-shared
layouts: its side table is 22% larger than the shared one despite a byte-identical row
format (5,093 vs 4,166 MB), and its all-tenant query is the slowest of all five (66.6 ms)
despite visiting 5x fewer partitions than the per-dataset layout. Both are single
measurements. The size suggests worse page fill under its particular write interleaving; the
latency has no explanation that survives scrutiny. It is not a candidate, so neither changes
a decision -- but these cells should not be quoted without a rerun.

### 6.2 What the millisecond column times

Every query runs as a fresh `mysql -e "..."` subprocess, so each wall time includes process
spawn, client startup, socket connect and the auth handshake -- a floor of several
milliseconds before the server does any work. Server-side execution for these shapes is
almost certainly sub-millisecond and buried under it. **The 2.5-4.1 ms figures are not a
measurement of index work, and the five-way tie must not be read off them.** Read it off the
structural counters, which are server-side and exact: identical rows examined and identical
partitions visited is direct evidence that the index work is the same.

This costs the conclusion nothing, because of what the query step is FOR. The side-table
lookup is the candidate-reduction prefix of a user query, not the query -- the user waits on
opening and searching each candidate archive, roughly 200 ms each in earlier rounds. A
metadata step of one millisecond or five is invisible against that. No topology choice can
move end-user query latency.

### 6.3 Cold latency was never captured

`phys_rd` is zero in 39 of 40 cells, because each configuration is queried immediately after
its own build, so the load warms exactly the pages the probes touch. No pool size fixes a
sequencing flaw. Since the single-dataset shapes tie warm and the all-tenant shape is
excluded from the decision anyway, nothing the recommendation rests on is waiting on a cold
number.

### 6.4 Other

- One engine, one machine, one run, no repetition. Earlier rounds showed MySQL 8 amplifies
  DDL and partition-count costs (up to 10x slower DDL), so gaps should widen there -- but
  that is extrapolation.
- Every query ran alone on an idle server. See open question 1.
- No compression (section 3.0), so all sizes are roughly 3x what the recommended row format
  would produce.
- Instance-per-user topologies are out of scope: their cost is a fixed per-instance floor
  better measured as a side study than simulated on one box.

---

## 7.0 Recommendation

**One table set for everything, with `dataset_id` leading every primary key.** Table count
is K, independent of datasets, users and retention policies. Retention stays what it
operationally is -- a background row-wise delete driven by each dataset's policy value in a
config table -- which also makes retention arbitrary and mutable per dataset for free: a
policy change is an UPDATE, and the next delete cycle applies it retroactively.

### 7.1 Ranked by whether it can matter

| cost of one-set-per-dataset | at 100 datasets | at 10,000 datasets, K=6 | matters?     |
|-----------------------------|-----------------|-------------------------|--------------|
| tablespace files            | 34,000          | 10.2 M                  | **a ceiling**|
| idle allocation             | 2,125 MB        | 637 GB if near-empty    | only if tiny |
| extra write bandwidth       | ~9 KB/s         | ~900 KB/s               | no           |
| extra ingest wall time      | 278 s / 7 days  | ~7.7 h / 7 days         | no           |

Only the first row survives scrutiny. Idle allocation assumes every partition sits at the
floor, which is only true if the datasets are nearly empty (5.1). The bottom two are
rounding errors in absolute terms and are things hardware solves.

The first row is different in kind. `K x N x (hours + 2)` reaches the maximum grantable
`ulimit -n` of 1,048,576 at roughly **1,030 datasets** under 7-day hourly partitions with
K=6, at **~242 datasets** under 30-day hourly, and at **~68 datasets** under 7-year daily.
Those are ordinary configurations, not asymptotes, and file descriptors and the
8,192-partitions-per-table engine limit cannot be raised with money. One shared set needs
`K x (hours + 2)` for the whole deployment: 1,020 files at 7-day hourly, 15,354 at 7-year
daily, independent of tenant count.

**A shared set is not immune to that ceiling; it makes it predictable.** K=6 at 30-day
retention is 4,332 files, over a stock 2,000 cap with a single tenant. The difference is
which input moves it: retention length and schema breadth are chosen at design time and
computable exactly before deployment, whereas the factor of N is moved by onboarding a
customer. When a shared set does cross, the levers are raising the cap against a known
bounded number, coarsening partitions (that same case becomes 192 files at daily), or
reducing K. The same levers exist for the per-dataset layout but are also divided by N, so
they do not rescue it.

### 7.2 Invariants the refactor must carry

1. **`dataset_id` is mandatory in every query.** It leads the key, so omitting it forfeits
   the index seek -- an earlier round measured the omission at 25-50x. This belongs in the
   query builder's signature, not in code review.
2. **Archive identity is `(dataset_id, archive_id)`,** and every join additionally binds
   `begin_timestamp`. Getting this wrong returns another tenant's rows, not a slow query.
3. **Offboarding is the same delete job.** A departed tenant is a retention policy of zero.

### 7.3 Not recommended

The naming refactor alone -- one schema per user, or renaming tables by dataset id -- is
measured here as physically null. Adopt it only as an interim step for SQL hygiene, never as
the fix.

Grouping by retention period is a contingency, not a recommendation. If partition-drop
expiry ever becomes a requirement (a compliance regime forbidding reliance on a delete job),
5.4 shows it is the cheap correct shape -- though partitioning by expiration time instead of
begin time achieves the same without table proliferation.

---

## 8.0 Open questions

1. **Concurrent multi-tenant query load.** Every query here executed alone on an idle
   server. The effect that can only appear under concurrency is the one specific to the
   design being recommended: latch contention on a single shared B-tree's hot pages, where
   one table set concentrates every tenant's reads and writes into a tree that the
   per-dataset layout spreads across 200. This is the most valuable missing experiment.
2. **Same-tenant, multi-dataset fan-out.** Does a tenant ever query across the several
   datasets it owns in one request? It is the only fan-out shape surviving the authorization
   boundary and the matrix has no measurement of it. Cheap to add: the all-tenant query at
   width `datasets-per-user` rather than width 100. Expected result is a latency tie leaving
   the generated-UNION construction cost as the only difference -- but that is a prediction.
3. **Delete-job interference.** A sustained background delete churns the buffer pool and
   purge, which can surface in foreground query latency. The one retention-adjacent effect
   touching a metric that matters. Unmeasured.
4. **Compression.** All sizes here are uncompressed (3.0). Whether the recommended row
   format changes the write-amplification comparison, and whether the 64 KB partition floor
   still holds at an 8 KB page size, are both unmeasured.
5. The one-set-per-user anomalies (6.1) deserve a rerun before that row is quoted anywhere.
6. The same matrix on MySQL 8, where partition-count costs are known to be larger.
