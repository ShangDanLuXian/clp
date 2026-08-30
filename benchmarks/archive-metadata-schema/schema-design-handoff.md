# CLP archive-metadata schema — design handoff

Context for someone picking this up cold. Covers what the work is, the schema decisions
reached, the evidence behind each, the current DDL, and what is still open.

## 0. What this is

CLP compresses logs into **archives**. Metadata about those archives lives in a relational
database so a search can decide which archives to open without touching object storage. This
work designs the replacement for that metadata schema.

**What exists today.** `components/clp-py-utils/clp_py_utils/clp_metadata_db_utils.py` creates
one `archives` table per dataset: unpartitioned, keyed on a `pagination_id` auto-increment, with
a UUID string as the archive identity. There is no filter index at all — narrowing a search to
candidate archives by field value is not something the metadata database can do, so the time
range is the only filter available.

**What is proposed.** Four things, all of which the benchmarks in this directory exist to
validate:

1. **Partition by time.** `PARTITION BY RANGE (begin_timestamp)` so retention is
   `DROP PARTITION` (6 ms) rather than a row-wise `DELETE` (1.77 s), and so time-bounded queries
   prune instead of scanning history.
2. **A filter-posting index.** A side table mapping `(column, value) -> archive`, so a search
   with a field predicate narrows to candidate archives in the database rather than opening
   archives to find out. This is the largest table in the design by a wide margin.
3. **Dataset-leading keys.** `dataset_id` first in every primary key, keeping a dataset's rows
   in one contiguous range and making a cross-dataset query an `IN`-list rather than a generated
   `UNION` over per-dataset tables.
4. **Packs.** Archives are grouped ~128 at a time into a single object-storage object, so the
   object count stays manageable. Packs are mutable — repacking supersedes old packs — which is
   why they carry a soft-delete rather than living in a partitioned table.

**Deployment target.** One table set per user; a user has many datasets, so `dataset_id`
remains a column. Retention horizon up to 7 years. Engine is **MySQL 8** (earlier benchmarks
ran MariaDB 10.6/10.11; wherever the two differ it is called out below).

**How the benchmarks got here.** `README.md` indexes the earlier series — pushdown predicate,
partitioning, join and repack cost, online DDL, column sizing — against a synthetic one-year
corpus. Later rounds added: table-scope topology (`multi-tenant-topology-benchmark.md`),
oversized filter values (`bench_oversize.py`), dictionary encoding
(`dictionary-encoding.md`), and low-cardinality value encodings (`bench_lc.py`). The decisions
in §2 draw on those; every DDL statement here was re-verified to create on MySQL 8.0.46.

**Conventions.** Commits authored as
`Xuhui Chen <90581933+ShangDanLuXian@users.noreply.github.com>`, short conventional messages,
100-char lines. Do not edit `multi-tenant-topology-benchmark.md` unless explicitly asked.

## 1. Current DDL

### 1.1 `clp_archives`

```sql
CREATE TABLE `clp_archives` (
    `id`                           BIGINT unsigned NOT NULL AUTO_INCREMENT,
    `dataset_id`                   SMALLINT unsigned NOT NULL,
    `uuid`                         BINARY(16) NOT NULL,
    `timestamp_range_begin_millis` BIGINT NOT NULL,
    `timestamp_range_end_millis`   BIGINT NOT NULL,
    `num_uncompressed_bytes`       BIGINT unsigned NOT NULL,
    `num_compressed_bytes`         BIGINT unsigned NOT NULL,
    `pack_id`                      BIGINT unsigned NULL,   -- NULL while unpacked
    PRIMARY KEY (`dataset_id`, `timestamp_range_begin_millis`, `id`),
    KEY `auto_inc` (`id`),
    KEY `archive_uuid` (`dataset_id`, `uuid`),
    KEY `archive_pack` (`dataset_id`, `pack_id`)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8
PARTITION BY RANGE (`timestamp_range_begin_millis`) (
    PARTITION `p_floor`  VALUES LESS THAN (<epoch of oldest retained window>),
    PARTITION `p_000000` VALUES LESS THAN (<+1 window>),
    -- ... one per retention window, extended by a maintenance job ...
    PARTITION `p_future` VALUES LESS THAN MAXVALUE
);
```

### 1.2 `clp_packs` — object-storage packing

```sql
CREATE TABLE `clp_packs` (
    `id`                           BIGINT unsigned NOT NULL AUTO_INCREMENT,
    `dataset_id`                   SMALLINT unsigned NOT NULL,
    `object_uuid`                  BINARY(16) NOT NULL,
    `tier`                         TINYINT unsigned NOT NULL,
    `window_id`                    BIGINT NOT NULL,
    `num_archives`                 INT unsigned NOT NULL,
    `num_compressed_bytes`         BIGINT unsigned NOT NULL,
    `timestamp_range_begin_millis` BIGINT NOT NULL,
    `timestamp_range_end_millis`   BIGINT NOT NULL,
    `creation_time_millis`         BIGINT NOT NULL,
    `is_deleted`                   BOOLEAN NOT NULL DEFAULT FALSE,
    `deleted_at_millis`            BIGINT NULL,
    PRIMARY KEY (`id`),
    KEY `pack_window` (`dataset_id`, `window_id`),
    KEY `pack_time` (`dataset_id`, `is_deleted`, `timestamp_range_begin_millis`),
    KEY `pack_object` (`dataset_id`, `object_uuid`)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;
```

Modernized from `schema.sql:1189`'s `packs_logs`. `object_id_high`/`object_id_low` collapse into
one `object_uuid BINARY(16)` — same 128 bits, one column, matching `clp_archives.uuid`.

**Deliberately unpartitioned.** A pack covers ~128 archives, so the table is ~1/128 the row
count. More importantly the hot join is `ON p.id = a.pack_id`; partitioning would force
`timestamp_range_begin_millis` into the primary key, degrading that join into a local-index
probe across every partition (the same trap as `auto_inc` on `clp_archives`). `PRIMARY KEY (id)`
keeps it a single dive. Cost: expiring packs is a row-wise `DELETE`, which is acceptable because
`is_deleted`/`deleted_at_millis` already force row-level lifecycle handling for repack
supersession. `pack_time` leads with `is_deleted` because superseded packs accumulate until GC
runs and every live query wants only current ones — an equality column, so it precedes the range.

"Which archives are in pack P" goes through `archive_pack`, which is a **local** index on a
partitioned table, so supply the pack's own time range to prune first:

```sql
SELECT a.id, a.uuid FROM clp_archives a
 WHERE a.dataset_id = ? AND a.pack_id = ?
   AND a.timestamp_range_begin_millis >= ?    -- p.timestamp_range_begin_millis
   AND a.timestamp_range_begin_millis <= ?;   -- p.timestamp_range_end_millis
```

The E6 repack flip (insert new pack, repoint 128 archives, supersede the old packs) was
re-verified as a single transaction against these definitions.

### 1.3 `clp_long_span_archives` — accelerator for the overlap query

```sql
CREATE TABLE `clp_long_span_archives` (
    `dataset_id`                   SMALLINT unsigned NOT NULL,
    `timestamp_range_begin_millis` BIGINT NOT NULL,
    `timestamp_range_end_millis`   BIGINT NOT NULL,
    `archive_id`                   BIGINT unsigned NOT NULL,
    PRIMARY KEY (`dataset_id`, `timestamp_range_end_millis`, `archive_id`),
    KEY `by_begin` (`dataset_id`, `timestamp_range_begin_millis`)
) ENGINE=InnoDB;   -- deliberately unpartitioned; small by construction
```

Holds a **duplicate** row for every archive whose span exceeds `MAX_SPAN`. Not a move — the
archive stays in `clp_archives` so identity is never split across two tables. See §2.6.

### 1.4 Postings tables

These still use the older naming (`begin_timestamp`, not `timestamp_range_begin_millis`) and
have **not** been re-issued in the new convention. Pending mechanical edit.

```sql
CREATE TABLE `clp_string_postings` (
    `dataset_id`      SMALLINT unsigned NOT NULL,
    `column_id`       SMALLINT unsigned NOT NULL,
    `value`           VARBINARY(1024) NOT NULL,
    `begin_timestamp` BIGINT NOT NULL,
    `archive_id`      BIGINT unsigned NOT NULL,
    PRIMARY KEY (`dataset_id`, `column_id`, `value`, `begin_timestamp`, `archive_id`)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8
PARTITION BY RANGE (`begin_timestamp`) ( /* one per retention window */ );

CREATE TABLE `clp_string_postings_long` (
    `dataset_id`      SMALLINT unsigned NOT NULL,
    `column_id`       SMALLINT unsigned NOT NULL,
    `begin_timestamp` BIGINT NOT NULL,
    `archive_id`      BIGINT unsigned NOT NULL,
    `long_value_ix`   TINYINT unsigned NOT NULL,
    `value`           VARBINARY(8192) NOT NULL,      -- set to the app's own length cap
    PRIMARY KEY (`dataset_id`, `column_id`, `begin_timestamp`, `archive_id`, `long_value_ix`),
    KEY `value_prefix` (`dataset_id`, `column_id`, `value`(1024))   -- see open item 3
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8
PARTITION BY RANGE (`begin_timestamp`) ( /* same boundaries */ );

CREATE TABLE `clp_string_postings_rejects` (
    `dataset_id`      SMALLINT unsigned NOT NULL,
    `column_id`       SMALLINT unsigned NOT NULL,
    `begin_timestamp` BIGINT NOT NULL,
    `archive_id`      BIGINT unsigned NOT NULL,
    PRIMARY KEY (`dataset_id`, `column_id`, `begin_timestamp`, `archive_id`)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8
PARTITION BY RANGE (`begin_timestamp`) ( /* same boundaries */ );
```

## 2. Decisions and the evidence

### 2.1 Key column order: equality columns first, then the range column

The governing rule for every table here. Partitioning does not change it — pruning is a separate
mechanism that runs before the index and selects partitions; key order decides what happens
*inside* one.

Measured on `clp_archives` (MySQL 8, 320k rows, daily partitions, 1-hour window):

| PK | rows read | matched |
|---|---:|---:|
| `(dataset_id, id, begin_timestamp)` | 2,000 | 83 |
| `(dataset_id, begin_timestamp, id)` | 83 | 83 |

`EXPLAIN` shows `type: ref, key_len: 2` versus `type: range, key_len: 10`. With `id` in the
middle the timestamp is unreachable in the key, so the scan reads every row the dataset has in
the partition. Also removes a filesort on `ORDER BY begin_timestamp`.

Measured on `clp_string_postings` (256k rows), moving the timestamp *earlier* is the mistake:

| PK | point lookup, rows read |
|---|---:|
| `(dataset_id, column_id, value, begin_timestamp, archive_id)` | 20 |
| `(dataset_id, column_id, begin_timestamp, value, archive_id)` | 3,958 |

Same rule, different outcome, because in the postings table `value` is the selective equality
and in the archives table the timestamp is all the selectivity there is after `dataset_id`.

Write-side cost of the archives swap (20k inserts, `innodb_metrics index_page_splits`): normal
append is a wash (198 vs 213 splits); backfill into an old partition is +26% splits and +48%
time. Rare path, accepted.

**The existing topology benchmark results are unaffected by this finding.** It partitions
hourly and queries 24-hour windows aligned to partition boundaries, so pruning consumes the
whole timestamp predicate and no residual filtering remains — verified: aligned windows read
8,000 rows under both orderings. The key order is also identical across all four benchmarked
configurations, so it cancels from every between-config ratio.

### 2.2 Dropped columns

- **`creator_id` / `creation_ix`** — carried over from CLP's existing archives table. In the
  unstructured `clp` path `creator_id` is a per-process UUID (`clp/compression.cpp:90`) and
  `creation_ix` increments per archive split (`streaming_archive/writer/utils.cpp:17`); the pair
  serves `GlobalMySQLMetadataDB::get_archive_iterator()`'s `ORDER BY`. In the **clp-s** path
  `compression_task.py:119-123` hardcodes `("", 0)` on every row, so the column pair and its
  index were constant. Dropped.
- **`index_config_id`** — removed by the user, from both archives and packs. Consequence:
  archive→config resolution goes back through `clp_index_configurations`'s `min_archive_id`
  range lookup, which has a known ordering hazard (the AUTO_INCREMENT id is assigned at
  INSERT/job-end while the config is read at job start, so a job spanning a config change lands
  on the wrong side). Worth a comment on that table. A pack is built under exactly one
  configuration, so there is a reasonable case for keeping it on `clp_packs` specifically.
- **`expiration_time`** — present in the earlier `schema.sql`, not carried forward. It is not
  purely a retention field: `e4c.sql` filters on it to hide archives that are logically expired
  but not yet dropped. Flagged, not resolved.

### 2.3 The three secondary keys on `clp_archives`

- `auto_inc (id)` is **mandatory**, not a query path: without it, `ERROR 1075 — there can be
  only one auto column and it must be defined as a key`. Secondary indexes on a partitioned
  table are **local**, so `WHERE id = ?` probes every partition (measured 8 probes on 8
  partitions, 1 with a timestamp supplied). Never resolve an archive by `id` alone.
- `archive_uuid (dataset_id, uuid)` serves external-identity lookups. It **cannot** be UNIQUE:
  `ERROR 1503 — a UNIQUE INDEX must include all columns in the table's partitioning function`.
  UUID uniqueness is therefore the writer's responsibility. With `BINARY(16)` the key is 18
  bytes (was 66 with `VARCHAR(64)`).
- `archive_pack (dataset_id, pack_id)` — also local; see §1.2 for how to prune around that.

Index cost measured on 320k rows: PRIMARY 47.1 MB, `archive_uuid` 34.1 MB (when it was
VARCHAR(64)), `auto_inc` 14.1 MB — the secondaries together exceeded the table.

### 2.4 Oversized filter values: long-value table, **not** an overflow table

From `bench_oversize.py` (MariaDB 10.11, cap 1024, oversized values 3,000 B, 20k archives). Raw
output is committed as `results/oversize_20260820_warm.txt` and `..._cold.txt`. Four policies,
pass condition truth ⊆ candidates:

```
query shape                    policy   truth   cands   extra   verdict
equality, short term           trunc      796     796       0   pass
equality, short term           marker     796  11,923  11,127   pass
equality, short term           ltab       796     873      77   pass
equality, term > cap           naive      826       0       0   FALSE NEGATIVES
infix '%…%' past the cap       trunc      826  11,535  10,709   pass
infix '%…%' past the cap       marker     826  11,535  10,709   pass
infix '%…%' past the cap       ltab       826     892      66   pass
```

Candidate inflation as the oversized fraction rises:

```
oversized  trunc_MB mark_MB ltab_MB   eq tr  eq mk  eq lt   ifx tr  ifx mk  ifx lt
0%             13.3    13.3    21.3   1.00x  1.00x  1.00x    1.00x   1.00x   1.00x
1%             17.2    13.3    28.8   1.00x  2.93x  1.00x    2.93x   2.93x   1.00x
5%             29.9    13.3    62.7   1.00x  9.30x  1.01x    9.30x   9.30x   1.01x
```

Decision: **`ltab`** — short postings + `_long` (values stored whole) + `_rejects` (per-archive
valve). The benchmark's `ltab` row already *is* that combination; the table named `marker` in
that script is just the short-postings table shared by both policies.

Rejected: the `marker`/overflow policy, because its `UNION` is unconditional and so inflates
*every* shape including plain equality (796 → 11,923). Rejected: `naive`, which loses rows.
Not chosen: `trunc`, which is perfect on equality but forces the cap-length rule — a stored
value of exactly 1024 bytes must match every wildcard unconditionally — which is precisely what
poisons infix and suffix, the shapes this deployment cares about.

Consequence: the cap-length rule is **gone**. `clp_string_postings` now holds only values that
genuinely fit, and no truncation semantics are needed at write or query time.

The candidate query is a union of the reachable branches plus the rejects, unconditionally.
A term longer than the cap can never match the inline table; a shorter one can still match a
long value's prefix — skip unreachable branches application-side.

Note `results/lc_bench_results.txt` settles the related question of whether filter values could
be packed inline on the archive row instead of normalized into a postings table: the delimited
and JSON encodings lose a rare-value lookup by ~200x (174 ms vs 0.5 ms) at roughly 1/4 the
storage. The postings table is the right structure.

### 2.5 The 3,072-byte index key limit

Why `value` is in the postings PK but not the long table's. The limit is checked at
`CREATE TABLE` against the **declared** length, not stored data:

```
VARBINARY(1024)  total key 1,044  OK
VARBINARY(3052)  total key 3,072  OK
VARBINARY(3053)  total key 3,073  ERROR 1071: max key length is 3072 bytes
VARBINARY(8192)  total key 8,212  ERROR 1071
```

So **1024 is a policy choice, not an engine limit** — there is ~2 KB of headroom. `value(1024)`
in `value_prefix` is the standard workaround, and is exactly why that index cannot see an infix
match past byte 1024.

`long_value_ix` (renamed from `seq`) exists only because `value` cannot be in that PK: one
archive can hold several different oversized values for one column, and without a tiebreaker
they collide (`ERROR 1062: Duplicate entry '1-3-1000-77'`), silently losing postings under
`INSERT IGNORE`. It counts 0,1,2… within each `(dataset, column, timestamp, archive)` group and
never exceeds the reject valve's K, hence `TINYINT`.

### 2.6 Long-span archives

The problem: an archive is *stored* in the partition of its begin timestamp but *covers*
`[begin, end]`, so overlap queries cannot simply prune to the window's partitions. Demonstrated
on 180k archives over 90 days plus one archive spanning day 3 → day 85, querying day 80:

```
approach                                  found  rows_read  parts
1. bounded, no split   (fast but WRONG)     376      2,333      2
2. unbounded, no split (right but SLOW)     377    161,168     81
3. split                                    377      2,334     81
```

Approach 1 silently returns a wrong answer. Approach 2's cost grows with retained history
forever. The split keeps 2's correctness at 1's price.

Original design (`schema.sql:13`) used a generated `is_long_span` column plus
`archives_time_lookup`. **Superseded** by `clp_long_span_archives`, because: the flag's long
branch still probes every partition (81 probes to read 1 row) whereas a small unpartitioned
table is one seek; and the threshold becomes cheap to change, which matters while its value is
unknown.

Query shape — two branches, independent, each pruning on its own, `UNION ALL` with per-branch
`ORDER BY … LIMIT k` (the outer-only form does not early-terminate: `e4c.sql` measured 189 ms /
97K rows against 10.8 s / 19.7M):

```sql
(SELECT id, uuid, timestamp_range_begin_millis, timestamp_range_end_millis, num_compressed_bytes
   FROM clp_archives
  WHERE dataset_id = ?
    AND timestamp_range_begin_millis >= ? - :max_span     -- W1 - MAX_SPAN, this is what prunes
    AND timestamp_range_begin_millis <  ?                 -- W2
    AND timestamp_range_end_millis   >  ?                 -- W1
    AND timestamp_range_end_millis - timestamp_range_begin_millis <= :max_span   -- disjointness
  ORDER BY timestamp_range_begin_millis LIMIT 100)
UNION ALL
(SELECT a.id, a.uuid, a.timestamp_range_begin_millis, a.timestamp_range_end_millis,
        a.num_compressed_bytes
   FROM clp_long_span_archives l
   JOIN clp_archives a
     ON a.dataset_id = l.dataset_id
    AND a.timestamp_range_begin_millis = l.timestamp_range_begin_millis
    AND a.id = l.archive_id
  WHERE l.dataset_id = ?
    AND l.timestamp_range_end_millis   > ?                -- W1
    AND l.timestamp_range_begin_millis < ?                -- W2
  ORDER BY a.timestamp_range_begin_millis LIMIT 100)
ORDER BY timestamp_range_begin_millis LIMIT 100;
```

The span predicate on the normal branch is **required**: the boolean column used to guarantee
the branches were disjoint, and nothing else does now. Without it a long-span archive beginning
within `MAX_SPAN` of the window appears in both and is double-counted.

`MAX_SPAN` is data-driven. Cost model:

```
normal branch  ≈ arrival_rate × (MAX_SPAN + window_width)          -- bounded
long branch    ≈ P(span > MAX_SPAN) × all archives ever retained   -- unbounded in retention
```

Note **partition width cancels out of the normal branch's row count** — wider partitions mean
fewer of them with proportionally more rows each. Partition width is a retention/file-count
dial, not a lever on this problem. Lower bound on `MAX_SPAN` is roughly one partition width
(below that it saves at most one partition); above that, pick the knee of the span CDF. Since
the long branch's cost grows with retention and the normal branch's does not, err large.

## 3. Facts established by measurement (MySQL 8.0.46 unless noted)

- `ROW_FORMAT=COMPRESSED` **blocks `ALGORITHM=INSTANT`** for `ADD COLUMN`. Isolated against
  controls: `compressed_nogen` and `compressed_stored` both fail with `ERROR 1845`;
  `dynamic_nogen`, `dynamic_stored`, `dynamic_virtual` all succeed. The earlier E7 conclusion
  that an *indexed VIRTUAL column* blocks INSTANT was a MariaDB result and does not reproduce.
  **Every table in this design uses COMPRESSED, so all of them forfeit instant `ADD COLUMN`** —
  each future column is a full table rebuild. Decide deliberately.
- Foreign keys are impossible on partitioned tables (`ERROR 1506`); `archive_id` and `pack_id`
  referential integrity is the writer's job.
- A partitioned table allocates one tablespace file **per partition**, 128 KB before holding a
  row on MySQL 8 (the topology doc says 64 KB; that was MariaDB 10.6). Two extra 168-partition
  tables cost 336 files / ~8 MB accounted / 40 MB on disk while empty. At 8,192 partitions this
  is ~400 MB against TB-scale data — **judged immaterial and explicitly dropped as a concern.**
- File-handle churn from exceeding `innodb_open_files` requires **both** more tablespaces than
  the cap **and** a working set that keeps missing the buffer pool. A warm query touches no
  files at all regardless of how many partitions it spans (0 physical reads → ~0 `openat`).
  When both conditions hold the tax is 5–8% with identical physical page reads (63.4 → 68.5 ms;
  10 vs 1,014 `openat` over 5 sweeps). This **confirms** the ~11% the topology doc attributed to
  evict/reopen, which had previously been asserted without proof. The practical lever is buffer
  pool vs working set, not `innodb_open_files`.
- `AUTO_INCREMENT` counters persist across restart on MySQL 8 (were lost on 5.7).
- Range gaps in `clp_index_configurations` silently unmap archives — an archive id falling in a
  gap resolves to the wrong config with no error.

## 4. Open items

1. **Measure the span distribution in production** — `timestamp_range_end_millis -
   timestamp_range_begin_millis`, plus the count of long-span archives *concurrently overlapping*
   windows of a few widths. This single measurement settles `MAX_SPAN`, whether the postings
   tables also need a long-span split, and whether a valve is needed for absurd spans (an
   archive spanning years overlaps every window, so a few of those are worse than thousands of
   3-day ones). Currently the plan is that the long-span archive set resolves first and the
   postings are then probed by full key for just those archives — which holds only if the
   concurrent-overlap count is small.
2. **Dictionary encoding is upstream of several of these decisions.** Per
   `dictionary-encoding.md` §3.0, moving to `value_id INT` deletes the `VARBINARY(1024)`, the
   key-length ceiling, `clp_string_postings_long`, `clp_string_postings_rejects`, and the whole
   oversized-value apparatus. If that is still live, settle it before building the above.
3. **Keep or drop `value_prefix`?** Measured on 40k rows × 3,000 B: it turns equality and prefix
   lookups from 5,000 rows read into 1, but does **nothing** for infix/suffix (5,000 either way,
   because the match sits past the indexed prefix), and the equality win is largely unreachable
   since a term shorter than the cap can never equal a long value. It costs +37% on the long
   table. Justified only if prefix wildcards are a real query shape.
4. **Raise the inline cap above 1024?** There is headroom to 3,052. Larger cap → fewer rows in
   the long table, but a fatter key on the hot table means fewer entries per page and a deeper
   B-tree for the short values that dominate. Needs measurement.
5. **Re-issue the three postings tables** in the `timestamp_range_begin_millis` naming
   convention; they currently use `begin_timestamp`.
6. **Decide on `expiration_time`** — dropped, but `e4c.sql` uses it to hide logically-expired
   archives that are not yet dropped.
7. **`COMPRESSED` vs instant DDL** — see §3. Not yet a deliberate decision.
8. **`index_configuration_id` on `clp_packs`** — dropped for consistency with `clp_archives`,
   but a pack maps to exactly one configuration, so it may belong there.

## 5. Things previously believed that turned out wrong

Recorded so they are not re-derived:

- "Indexed VIRTUAL columns block `ALGORITHM=INSTANT`" — MariaDB-only; on MySQL 8 the blocker is
  `ROW_FORMAT=COMPRESSED`.
- "The 11% open-files penalty is from handle evict/reopen" — was an unverified attribution; now
  confirmed, but only under repeated physical I/O, and it is ~0 for warm queries.
- "Empty-partition floor and tablespace file count are a design constraint" — at TB scale they
  are ~0.04% of storage. Dropped.
- "`begin_timestamp` should move earlier in every key because we query by partition" — false;
  pruning is independent of key order. Only `clp_archives` needed the move, and by the
  equality-first rule, not because of partitioning.
