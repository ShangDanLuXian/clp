# Archive/Pack metadata schema benchmarks

A MariaDB benchmark harness for the proposed per-dataset `archives_*` / `packs_*` metadata
schema. It validates the load-bearing claims of the design (pushdown predicate, partitioning,
join and repack costs, online DDL, column sizing) against a synthetic one-year corpus shaped
like the proposal's DDL.

## Environment

- MariaDB 10.11 (InnoDB), `innodb_buffer_pool_size = 4G`, `local_infile = 1`.
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

E8 (sizing of `lc_multi_val VARCHAR(1024)`) is not part of this harness: it is an offline
analysis of real per-archive distinct-value counts from the mongodb dataset (see the
archive-analyzer output on the `simulate_repacking` branch), where a 7-char numeric column
with 125 distinct values per archive already needs ~1,001 bytes delimited.

## Running

```bash
./load.sh                      # generate + load (~10 min)
mysql -vvv < e1e2.sql          # then e2b, e3e6, e4, e4c, e7_ddl
mysql -vvv < e_retention.sql   # destructive; run last
```

`results/` holds the raw outputs of the runs behind the reported numbers (`*.out`, plus the
E7 INPLACE logs: the first attempt exhausting disk after 94 s, and the timed 2m01s rerun).
