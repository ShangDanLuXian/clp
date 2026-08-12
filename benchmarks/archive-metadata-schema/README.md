# Archive/Pack metadata schema benchmarks

A MariaDB benchmark harness for the proposed per-dataset `archives_*` / `packs_*` metadata
schema. It validates the load-bearing claims of the design (pushdown predicate, partitioning,
join and repack costs, online DDL, column sizing) against a synthetic one-year corpus shaped
like the proposal's DDL.

## Setup

On a machine with no database installed (including WSL, where systemd may be absent):

```bash
sudo ./setup_db.sh        # installs MariaDB if needed, configures it, starts it, grants you access
sudo ./setup_mysql8.sh    # optional: adds MySQL 8 on port 3307 for the cross-engine comparison
```

`setup_db.sh` sizes the buffer pool to half of RAM (capped at 4 GB), enables `local_infile`,
starts the server via systemd/`service`/`mariadbd-safe` depending on what the box has, and
grants your user socket authentication plus the global `RELOAD` privilege that `FLUSH STATUS`
needs. After it prints `Ready.` no further sudo is required.

## Environment

- MariaDB 10.11 (InnoDB), `innodb_buffer_pool_size = 4G`, `local_infile = 1`.
- The scripts connect over the unix socket with no credentials. On a stock Ubuntu install only
  `root` is socket-authenticated, so give your OS user an account first:
  `sudo mysql -e "CREATE USER IF NOT EXISTS '$USER'@'localhost' IDENTIFIED VIA unix_socket;
  GRANT ALL PRIVILEGES ON bench.* TO '$USER'@'localhost';"`
- ~20 GB free disk for the corpus plus one full-table-copy rebuild (E7).
- All reported latencies are warm: each query runs twice and the second timing is kept.
  Rows examined = `Handler_read_next` after `FLUSH STATUS`.

## Corpus

`gen.py ROWS SEED OUT.tsv` emits archive rows spanning 2024: arrival-ordered
`begin_timestamp` (ns), lognormal spans (median ~10 min, p99 < 1 day) with 0.1% long-span
rows (2-30 days), `pack_id` assigned K=128 in arrival order (newest 2% unpacked), and
`expiration_time` set so the whole corpus is unexpired at the snapshot instant.

`schema.sql` creates database `bench` with `archives_logs` (30M rows, 366 daily partitions
on `begin_timestamp` plus `p_floor`/`p_future`), an unpartitioned twin `archives_logs_nopart`
for E2, `archives_metrics` / `archives_traces` (5M rows each) for E4, and `packs_logs`.
Tables follow the proposed DDL: `PRIMARY KEY (id, begin_timestamp)`, VIRTUAL `is_long_span`,
`archives_time_lookup(is_long_span, begin_timestamp, end_timestamp, expiration_time)`,
`archives_expiration(expiration_time, begin_timestamp)`.

`load.sh` generates all three TSVs, sources the schema, bulk-loads the tables, and derives
one `packs_logs` row per assigned `pack_id` (229,688 Packs).

## Experiments

| script | measures | headline result |
|---|---|---|
| `e1e2.sql` | E1: naive overlap predicate vs the two-branch pushdown+quarantine form, at three window positions; E2: partitioned vs unpartitioned, `EXPLAIN PARTITIONS` on the OR-form | naive scans half the table mid-history (15.08M rows, 4.9 s) and all of it on recent windows (21 s); cookbook form is ~100K rows / 105-238 ms everywhere; partitioning adds ~5% warm; OR-form touches 183/368 partitions |
| `e2b.sql` | warm rerun of the unpartitioned twin (fair E2 comparison) and the UNION ALL class-split form that restores partition pruning | warm nopart 166 ms vs 158 ms partitioned; split normal-span branch prunes to 2 partitions, index-only, but is slower warm (241 ms, two passes) |
| `e3e6.sql` | E3: `LEFT JOIN packs_logs` on a ~40K-archive result vs no join; E6: repack flip of 128 archives (`FOR UPDATE` + insert + membership update + supersede, one transaction) | join effectively free (195 vs 189 ms); flip commits in 235 ms |
| `e4.sql` | E4: multi-dataset UNION ALL top-k, outer-only ORDER BY/LIMIT vs naive per-branch bare LIMIT | 10.8 s / 19.7M rows examined; bare per-branch LIMIT does not early-terminate (same 19.7M) |
| `e4c.sql` | E4: streamed form -- class-split branches, per-branch ORDER BY `begin_timestamp` LIMIT k, outer merge | 189 ms / 97K rows examined, 57x over the outer-only rule |
| `e7_ddl.sql` | E7: `ALGORITHM=INSTANT` ADD COLUMN vs the indexed VIRTUAL `is_long_span`, with controls (unindexed VIRTUAL, indexed VIRTUAL, indexed STORED) | INSTANT refused on the proposed table; STORED variant restores 5 ms INSTANT; forced INPLACE = full-table-copy rebuild, 2m01s idle with table-sized peak disk |
| `e_retention.sql` | retention: drop one day (~82K rows) via `DROP PARTITION` vs row-wise `DELETE` (destructive -- run last) | 6 ms vs 1.77 s |

## Low-cardinality column encodings (`bench_lc.py`)

A second, self-contained harness for the multi-value filter column, run across **both**
MariaDB and MySQL because the two engines diverge sharply here. One archive row stores the
set of distinct values a column takes anywhere in that archive, so the planner can ask "does
this archive contain value v?"; this compares the candidate encodings for that set.

This harness uses its own database (`lcbench`) and needs the global RELOAD privilege, because
it counts rows scanned via `FLUSH STATUS`. A database-scoped grant alone fails with
`ERROR 1227`:

```bash
sudo mysql -e "GRANT ALL PRIVILEGES ON lcbench.* TO '$USER'@'localhost';
               GRANT RELOAD ON *.* TO '$USER'@'localhost';"
```

```bash
python3 bench_lc.py                       # auto-detects local servers, writes lc_bench_results.txt
python3 bench_lc.py --profile id-like     # one profile only
python3 bench_lc.py --rows 50000          # smaller/faster
python3 bench_lc.py --engine mariadb="mysql -u root" \
                    --engine mysql="mysql -u root -h 127.0.0.1 -P 3307 --protocol=TCP"
```

Output is a plain-text numbers table (not a document): storage, bytes/row, load time, warm
latency for a rare (~1%) and a common (~20%) value, rows scanned, matching archives, index
chosen, and status. Both engines are optional — it benchmarks whichever it can reach.

Variants: `varchar_delim` (VARCHAR(1024), the proposal as written), `text_delim` (TEXT),
`text_hash` (TEXT of fixed-width digests, delimiter-safe by construction), `json`,
`json_mvi` (JSON + MySQL multi-valued index), `side_table` (normalized
`(val_hash, archive_id)`) and `side_table_raw` (normalized `(value, archive_id)`, keeping
the value so the B-tree can serve prefix ranges). Two profiles, both taken from the real
per-archive distinct-value counts measured on the mongodb dataset: `id-like` (125 values x
7 chars = 1,001 B, grazing the VARCHAR limit) and `msg-like` (115 x 60 = ~7 KB, far over it).

Three measurements, because they pull in different directions:

- **Storage**, absolute and per archive.
- **Query latency** for an exact value and for a **prefix wildcard** (`service: web-*`).
  Hash-based encodings report `n/a` for the wildcard: hashing is not order-preserving, so
  they cannot express the query at all. This is the discriminating case between keeping raw
  values and digesting them.
- **Sustained per-archive INSERT throughput** (`insert_arch/s`), one transaction per archive,
  which is the shape the ingest path actually writes. Bulk `LOAD DATA` (`bulk_s`) is also
  reported but is the friendliest possible write pattern and hides the cost of random key
  order, so it should not be used to compare designs.

The `hits` columns are the correctness check: every variant must report the same number of
matching archives. A lower count means the encoding lost data and the filter is silently
returning false negatives.

## Round 2: inline vs side table at scale (`bench_lc2.py`)

Round 1 eliminated the alternatives (VARCHAR loses data, hashes cannot express wildcards,
JSON is slow everywhere, MVI cannot serve prefix queries). Round 2 stress-tests the real
decision -- inline `text_delim` vs the raw-value side table -- under production conditions:

```bash
./run_all.sh                     # clean + verify setup + tune + run, in one command
./run_all.sh --with-mysql        # same, plus install/configure MySQL 8 for both engines
./run_all.sh --scale 100000      # fast pass; extra args go through to bench_lc2.py
./run_all.sh --clean-only        # just reclaim space from a previous run
```

`run_all.sh` is the entry point: it installs and starts a server if needed, raises the redo
log (see below), drops `lcbench` on every engine, removes staging dirs orphaned by aborted
runs, then runs the benchmark into a timestamped results file with a matching progress log.
Run it as your normal user -- it sudo's only for the privileged steps.

**Redo log capacity is the setting that decides whether this run takes minutes or hours.**
A 125M-row random-key load fills the default log (96M on MariaDB, 100M on MySQL 8) faster
than the checkpointer can reclaim it, at which point InnoDB throttles the writing thread and
the load appears hung. `run_all.sh` raises it to 4G automatically; note that this *allocates*
4 GB of disk for the log files, so budget for it on a small volume. The benchmark header
prints the setting and warns when it is below 1G.

To drive the benchmark directly instead:

```bash
python3 bench_lc2.py                          # both profiles, 100K then 1M archives
python3 bench_lc2.py --scale 100000           # quick pass only
python3 bench_lc2.py --scale 5000000          # if you have ~60 GB free disk
```

Progress goes to stderr with a per-phase breakdown (load / optimize / queries / inserts / gc)
per variant, so a long run shows where the time is actually going.

What it measures, per engine and scale: combined **time-window AND value** queries at
1h/1d/7d windows (the shape production actually runs); the side table **partitioned daily on
begin_timestamp vs unpartitioned** (the write-amplification hypothesis -- partition-local
B-trees keep the hot write set one-day-sized, so any benefit appears only once the
unpartitioned tree outgrows the buffer pool, i.e. at the 1M+ scales); **denormalized
timestamp vs join-back** (`side_nots`); table size **before and after OPTIMIZE TABLE**
(how much of the side table's overhead is recoverable page waste); insert rates at **1 and
10 archives per transaction**; and **DROP PARTITION vs row-wise DELETE** GC on the side
table itself.

Index-configuration changes apply only to archives sealed after the change; existing archives
are never backfilled. So the per-archive insert rates are the *only* production write numbers,
and the bulk load exists purely to construct a realistic-size table for the query and GC
measurements. A query window that starts before a filter column's config point cannot use that
filter for the older span -- that fallback is planner behaviour, outside this benchmark's
scope.

## Round 3: full-shape benchmark at 31 archives/s (`bench3.py`)

The corpus models the mongodb archive-analyzer report: filter columns whose per-archive
distinct counts follow the measured shape classes, arrival-rate timestamps, hourly
side-table partitions. Designs under test: `inline` (delimited TEXT columns), `side_shared`
(one side table, column_id in the key), `side_percol` (one table per column), `inline_json`
(one JSON document per archive, plus per-path multi-valued-index attempts on MySQL with
failures recorded as results).

### The storage budget is an admission policy, not a benchmark variable

Users nominate which columns to index, and the service **rejects a configuration whose total
filter payload exceeds 0.1% of the compressed archive** (`--budget-pct`). At 256 MB raw and
50x compression that ceiling is **5,368 bytes per archive**, which is what makes the size
question answerable up front rather than discovered after loading. `python3 bench3.py plan`
evaluates every configuration against the policy without touching a database:

| archetype | distinct/archive | payload | how many fit the budget |
|---|---:|---:|---:|
| host / pod / region | 1 | 13 B | 400 |
| env / tier | 3 | 27 B | 190 |
| severity / method | 8 | 88 B | 59 |
| module / service | 20 | 300 B | 17 |
| endpoint / error code | 64 | 1,216 B | 4 |

Anything with hundreds of distinct values per archive is inadmissible on its own -- a single
326-distinct column is 6.8 KB, above the entire budget -- so the policy excludes exactly the
shapes that made storage a problem. Configurations under test (`--config`), all bounded by
the 64-column schema limit:

| config | columns | values/archive | payload | % of compressed | verdict |
|---|---:|---:|---:|---:|---|
| `c8` (default) | 8 | 45 | 577 B | 0.011% | admitted |
| `c32` | 32 | 142 | 1,810 B | 0.034% | admitted |
| `c64` | 64 | 328 | 4,536 B | 0.085% | admitted |
| `wide` | 17 | 340 | 5,117 B | 0.095% | admitted (95% of budget) |
| `over` | 15 | 520 | 9,095 B | 0.169% | **rejected** |

Because size is capped by policy, the sweep measures what varies with the *number* of
configured columns: query cost, write throughput, and partition count. That last one is a
real constraint on `side_percol`, whose partitions multiply by column count -- 64 columns x
674 hourly partitions is 43,136 files, past the process open-file limit and near the
8,192-per-table engine cap. `--partition daily` trades partition-pruning granularity for
feasibility; `side_shared` is unaffected. The planner prints these counts and warns.

Three entry points, so the database persists and query experiments can iterate on it:

```bash
python3 bench3.py plan                        # admission verdicts only; no server needed
./bench3_setup.sh                             # clean + tune + BUILD (c8, 0.05 arch/s x 28d)
./bench3_setup.sh --config c64 --partition daily      # 64 columns; daily avoids 43K files
./bench3_setup.sh --rate 0.5                  # full scale: hours of build, ~100 GB disk
./bench3_query.sh                             # query/write/interference on the existing db
./bench3_query.sh --query Q3_sel_hot --window 7d --design side_percol
./bench3_all.sh                               # both
```

The build writes `lc3_manifest.json` (scale, seed, probe set, per-probe ground-truth hit
counts when the corpus is small enough to compute them) and keeps database `lc3`. Query runs
check every hit count against ground truth and across designs. Metrics per design: data/index
size and % of compressed data (256 MB raw/archive at 50x), build rate, add-a-column DDL cost,
GC drop-vs-delete, insert rate at 1/10 archives per txn and 4 connections, physical bytes
written per archive, cold+warm query ms / hits / rows scanned / pruning power for 11 queries
(1-2 predicates, equality and prefix wildcard, hot to rare) x 3 windows, and query latency
while the ingest path runs concurrently. For true cold numbers, restart the server and run
`./bench3_query.sh` immediately -- run1_ms of that pass is a cold measurement.

The header prints each engine's binlog and flush settings and warns if the binary log is on:
MySQL 8 enables it by default with `sync_binlog=1`, which caps single-transaction write rates
at the disk's fsync rate and invalidates cross-engine write comparisons. `setup_mysql8.sh`
now disables it (`skip-log-bin`); if you set MySQL up before this change, add that line under
`[mysqld]` in `/etc/my8.cnf` and restart. The 1M-archive step loads 125M side rows and takes
tens of minutes per engine; the toy scales are for validation only.

### Running both engines on one machine

`apt install mysql-server` **removes MariaDB** — the packages conflict — so install MySQL
either in a container or from extracted binaries in a private prefix:

```bash
docker run -d --name mysql8-bench -p 3307:3306 -e MYSQL_ALLOW_EMPTY_PASSWORD=1 \
  mysql:8.0 --innodb-buffer-pool-size=4G --local-infile=1     # simplest, if docker is available
sudo ./setup_mysql8.sh                                        # otherwise: native, port 3307
```

`setup_mysql8.sh` downloads the MySQL packages without installing them, apt-installs only
their shared-library dependencies, extracts the binaries to `/opt/mysql8`, and initializes a
separate datadir on port 3307. It leaves the existing MariaDB untouched and prints the
`--engine` flags to use. Give both engines the same `innodb_buffer_pool_size` or the
comparison is meaningless.

## Running

```bash
./load.sh                      # generate + load (~10 min)
mysql -vvv < e1e2.sql          # then e2b, e3e6, e4, e4c, e7_ddl
mysql -vvv < e_retention.sql   # destructive; run last
```

`results/` holds the raw outputs of the runs behind the reported numbers (`*.out`, plus the
E7 INPLACE logs: the first attempt exhausting disk after 94 s, and the timed 2m01s rerun).
