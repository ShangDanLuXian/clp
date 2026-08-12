#!/usr/bin/env python3
"""Round 2: text_delim (inline) vs the raw-value side table, under production conditions.

Round 1 (bench_lc.py) eliminated the other encodings: VARCHAR(1024) silently loses data,
hashed variants cannot express wildcards, JSON is slow on MariaDB and pathological on MySQL,
and MySQL's multi-valued index cannot serve prefix queries. What remains is the real decision,
so this round stops comparing encodings and stress-tests the survivor's weaknesses:

  * the REAL query shape: time-window AND value predicate, at 1h / 1d / 7d windows;
  * a scale curve (default 100K and 1M archives) so B-tree depth and buffer-pool residency
    show up, instead of measuring a fully-cached toy;
  * the side table partitioned by begin_timestamp (mirroring the archives table) vs
    unpartitioned -- the write-amplification hypothesis;
  * denormalized timestamp vs join-back-to-archives (side_nots);
  * space remediation: table size before and after OPTIMIZE TABLE;
  * GC: DROP PARTITION vs row-wise DELETE of one day, on the side table itself.

Variants:
  inline       archives row carries the delimited TEXT set; time index + LIKE residual
  side_unpart  side table PK (value, begin_timestamp, archive_id), one table
  side_part    same, RANGE-partitioned daily on begin_timestamp
  side_nots    side table PK (value, archive_id), no timestamp; joins archives for time

Write rates are one transaction per archive (ins1/s) and ten per transaction (ins10/s); for
side variants each transaction also writes the base archives row, so rates are end-to-end.
The header prints each engine's binlog/flush settings: with log_bin=ON every commit pays a
binlog fsync (MySQL 8 default; MariaDB default is OFF) and write rates measure that policy,
not the schema. setup_mysql8.sh disables it; the header warns if it is on.
"""

import argparse
import os
import shlex
import subprocess
import sys
import tempfile
import time

DB = "lcbench"
SEP = chr(31)
T0 = 1704067200 * 10**9          # 2024-01-01 UTC, ns
DAY = 86400 * 10**9
DAYS = 366

# (values per archive, value width, distinct-value pool size)
PROFILES = {
    "id-like": (125, 7, 12500),      # the mongodb id column profile from round 1
    "severity-like": (3, 6, 8),      # few, tiny values: the side table's best case
}
VARIANTS = ["inline", "side_unpart", "side_part", "side_nots"]
WINDOWS = [("1h", T0 + 180 * DAY + 12 * 3600 * 10**9, T0 + 180 * DAY + 13 * 3600 * 10**9),
           ("1d", T0 + 180 * DAY, T0 + 181 * DAY),
           ("7d", T0 + 180 * DAY, T0 + 187 * DAY)]
GC_DAY = 10                       # day whose data the GC comparison removes (after queries)


def sh(cmd, sql, db=None, local_infile=False, timeout=7200):
    argv = list(cmd)
    if local_infile:
        argv.append("--local-infile=1")
    argv += ["-N", "-B"]
    if db:
        argv.append(db)
    p = subprocess.run(argv, input=sql, capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


def detect(specs):
    found, failed, seen = [], [], set()
    for spec in specs:
        label, _, cmdline = spec.partition("=")
        cmd = shlex.split(cmdline)
        try:
            rc, out, err = sh(cmd, "SELECT VERSION();", timeout=15)
        except FileNotFoundError:
            failed.append((cmdline, "client program not found"))
            continue
        except (OSError, subprocess.SubprocessError) as exc:
            failed.append((cmdline, str(exc)[:70]))
            continue
        if rc != 0 or not out.strip():
            failed.append((cmdline, (err.strip().splitlines()[-1] if err.strip()
                                     else "no response")[:70]))
            continue
        version = out.strip().splitlines()[0]
        flavour = "mariadb" if "mariadb" in version.lower() else "mysql"
        if flavour in seen:
            failed.append((cmdline, f"duplicate: another command already serves {flavour}"))
            continue
        seen.add(flavour)
        e = {"label": flavour if label == "auto" else label, "cmd": cmd,
             "version": version, "flavour": flavour}
        rc, _, err = sh(cmd, f"CREATE DATABASE IF NOT EXISTS {DB};")
        if rc == 0:
            rc, _, err = sh(cmd, "CREATE TABLE __probe (i INT); FLUSH STATUS; "
                                 "DROP TABLE __probe;", DB)
        e["error"] = err.strip().splitlines()[-1] if rc != 0 else None
        if not e["error"]:
            _, out, _ = sh(cmd, "SELECT @@log_bin, @@sync_binlog, "
                                "@@innodb_flush_log_at_trx_commit, "
                                "ROUND(@@innodb_buffer_pool_size/1048576);")
            try:
                lb, sb, fl, bp = out.split()
                e["cfg"] = (f"log_bin={'ON' if lb == '1' else 'OFF'} sync_binlog={sb} "
                            f"flush_log_at_trx_commit={fl} buffer_pool={bp}M")
                e["binlog"] = lb == "1"
            except ValueError:
                e["cfg"], e["binlog"] = "(settings unavailable)", False
        found.append(e)
    return found, failed


DEFAULT_ENGINES = [
    "auto=mysql",
    "auto=mariadb",
    "auto=mysql -h 127.0.0.1 -P 3306 --protocol=TCP",
    "auto=mysql -h 127.0.0.1 -P 3307 --protocol=TCP",
    "auto=/opt/mysql8/usr/bin/mysql --defaults-file=/etc/my8.cnf -u root",
]


def part_clause():
    parts = [f"PARTITION p_floor VALUES LESS THAN ({T0})"]
    parts += [f"PARTITION p_d{d:03d} VALUES LESS THAN ({T0 + d * DAY})"
              for d in range(1, DAYS + 1)]
    parts.append("PARTITION p_future VALUES LESS THAN MAXVALUE")
    return "PARTITION BY RANGE (begin_timestamp) (" + ",".join(parts) + ")"


DDL = {
    "inline": "CREATE TABLE t_inline (id INT UNSIGNED NOT NULL PRIMARY KEY, "
              "begin_timestamp BIGINT NOT NULL, v TEXT NULL, KEY ix_time (begin_timestamp)) "
              "ENGINE=InnoDB DEFAULT CHARSET=ascii",
    "base": "CREATE TABLE t_base (id INT UNSIGNED NOT NULL PRIMARY KEY, "
            "begin_timestamp BIGINT NOT NULL) ENGINE=InnoDB",
    "side_unpart": "CREATE TABLE {t} (value VARCHAR(255) NOT NULL, "
                   "begin_timestamp BIGINT NOT NULL, archive_id INT UNSIGNED NOT NULL, "
                   "PRIMARY KEY (value, begin_timestamp, archive_id)) "
                   "ENGINE=InnoDB DEFAULT CHARSET=ascii",
    "side_part": "CREATE TABLE {t} (value VARCHAR(255) NOT NULL, "
                 "begin_timestamp BIGINT NOT NULL, archive_id INT UNSIGNED NOT NULL, "
                 "PRIMARY KEY (value, begin_timestamp, archive_id)) "
                 "ENGINE=InnoDB DEFAULT CHARSET=ascii " + part_clause(),
    "side_nots": "CREATE TABLE {t} (value VARCHAR(255) NOT NULL, "
                 "archive_id INT UNSIGNED NOT NULL, PRIMARY KEY (value, archive_id)) "
                 "ENGINE=InnoDB DEFAULT CHARSET=ascii",
}


def generate(profile, archives, seed, outdir):
    """Streams the corpus to TSVs. Returns (paths, needles, batch_values)."""
    import random
    n_vals, width, pool_size = PROFILES[profile]
    rng = random.Random(seed)
    pool = [(str(i) + "x" * width)[:width] for i in range(pool_size)]
    hot, rare = pool[0], pool[pool_size // 2]
    prefix = rare[:1]
    for k in range(width, 0, -1):
        if sum(1 for v in pool if v.startswith(rare[:k])) >= 5:
            prefix = rare[:k]
            break

    def row_values():
        vals = rng.sample(pool, n_vals)
        if rng.random() < 0.20 and hot not in vals:
            vals[0] = hot
        return vals

    paths = {k: os.path.join(outdir, f"{k}.tsv")
             for k in ("inline", "base", "side_ts", "side")}
    step = (DAYS * DAY) // archives
    with open(paths["inline"], "w") as fi, open(paths["base"], "w") as fb, \
         open(paths["side_ts"], "w") as ft, open(paths["side"], "w") as fs:
        for i in range(archives):
            ts = T0 + i * step + rng.randrange(0, max(1, step))
            vals = row_values()
            fi.write(f"{i}\t{ts}\t{SEP}{SEP.join(vals)}{SEP}\n")
            fb.write(f"{i}\t{ts}\n")
            for v in vals:
                ft.write(f"{v}\t{ts}\t{i}\n")
                fs.write(f"{v}\t{i}\n")
    batch = None  # built lazily per insert test from the same generator settings
    return paths, {"rare": rare, "common": hot, "prefix": prefix}, \
        [row_values() for _ in range(2000)]


def predicate(variant, tbl, literal, kind, w1, w2):
    val = f"value LIKE '{literal}%'" if kind == "prefix" else f"value = '{literal}'"
    if variant == "inline":
        pat = (f"CONCAT('%',CHAR(31 USING ascii),'{literal}','%')" if kind == "prefix" else
               f"CONCAT('%',CHAR(31 USING ascii),'{literal}',CHAR(31 USING ascii),'%')")
        return (f"SELECT COUNT(*) FROM t_inline WHERE begin_timestamp >= {w1} "
                f"AND begin_timestamp < {w2} AND v LIKE {pat}")
    if variant == "side_nots":
        return (f"SELECT COUNT(DISTINCT s.archive_id) FROM {tbl} s "
                f"JOIN t_base a ON a.id = s.archive_id WHERE s.{val} "
                f"AND a.begin_timestamp >= {w1} AND a.begin_timestamp < {w2}")
    return (f"SELECT COUNT(DISTINCT archive_id) FROM {tbl} WHERE {val} "
            f"AND begin_timestamp >= {w1} AND begin_timestamp < {w2}")


def insert_txn(variant, tbl, aid, ts, vals):
    stmts = []
    if variant == "inline":
        stmts.append(f"INSERT INTO t_inline (id,begin_timestamp,v) "
                     f"VALUES ({aid},{ts},'{SEP}{SEP.join(vals)}{SEP}');")
        return stmts
    stmts.append(f"INSERT INTO t_base (id,begin_timestamp) VALUES ({aid},{ts});")
    if variant == "side_nots":
        rows = ",".join(f"('{v}',{aid})" for v in vals)
        stmts.append(f"INSERT INTO {tbl} (value,archive_id) VALUES {rows};")
    else:
        rows = ",".join(f"('{v}',{ts},{aid})" for v in vals)
        stmts.append(f"INSERT INTO {tbl} (value,begin_timestamp,archive_id) VALUES {rows};")
    return stmts


def timed(engine, sql):
    script = (sql + ";\nFLUSH STATUS;\nSET @t=NOW(6);\n" + sql + ";\n"
              "SELECT CONCAT('MS=',TIMESTAMPDIFF(MICROSECOND,@t,NOW(6))/1000);\n"
              "SHOW SESSION STATUS WHERE Variable_name IN "
              "('Handler_read_next','Handler_read_rnd_next','Handler_read_key');\n")
    rc, out, err = sh(engine["cmd"], script, DB)
    if rc != 0:
        return None, None, None, (err.strip().splitlines()[-1] if err.strip() else "error")
    ms, scanned, matched = None, 0, None
    for line in out.splitlines():
        if line.startswith("MS="):
            ms = float(line[3:])
        elif "\t" in line:
            k, v = line.split("\t", 1)
            if k in ("Handler_read_next", "Handler_read_rnd_next"):
                scanned += int(v)
        elif line.strip().isdigit():
            matched = int(line.strip())
    return ms, scanned, matched, None


def insert_rate(engine, variant, tbl, batch_vals, start_id, per_txn):
    """Archives/second, per_txn archives per transaction, timestamps in the newest day."""
    n = len(batch_vals)
    parts = ["SET @t=NOW(6);"]
    ts0 = T0 + (DAYS - 1) * DAY + 3600 * 10**9
    for g in range(0, n, per_txn):
        parts.append("BEGIN;")
        for j in range(g, min(g + per_txn, n)):
            parts += insert_txn(variant, tbl, start_id + j, ts0 + j * 1000, batch_vals[j])
        parts.append("COMMIT;")
    parts.append("SELECT CONCAT('MS=',TIMESTAMPDIFF(MICROSECOND,@t,NOW(6))/1000);")
    rc, out, err = sh(engine["cmd"], "\n".join(parts), DB)
    if rc != 0:
        return None
    for line in out.splitlines():
        if line.startswith("MS="):
            ms = float(line[3:])
            return round(n / (ms / 1000.0), 1) if ms > 0 else None
    return None


def partitions_touched(engine, sql):
    stmt = ("EXPLAIN PARTITIONS " if engine["flavour"] == "mariadb" else "EXPLAIN ") + sql
    rc, out, _ = sh(engine["cmd"], stmt + ";", DB)
    if rc != 0 or not out.strip():
        return "-"
    fields = out.splitlines()[0].split("\t")
    if len(fields) > 3 and fields[3] not in ("NULL", ""):
        return str(len(fields[3].split(",")))
    return "-"


def table_mb(engine, tbl, rows):
    sh(engine["cmd"], f"ANALYZE TABLE {tbl};", DB)
    fresh = ("SET SESSION information_schema_stats_expiry=0;\n"
             if engine["flavour"] == "mysql" else "")
    _, out, _ = sh(engine["cmd"], fresh +
                   "SELECT ROUND((data_length+index_length)/1048576,1), "
                   "data_length+index_length FROM information_schema.tables "
                   f"WHERE table_schema='{DB}' AND table_name='{tbl}';", DB)
    try:
        return float(out.split()[0]), round(int(out.split()[1]) / rows)
    except (ValueError, IndexError):
        return 0.0, 0


def bench_variant(engine, variant, paths, needles, batch_vals, archives, n_vals,
                  do_optimize, log):
    tbl = "t_inline" if variant == "inline" else f"t_{variant}"
    r = {"engine": engine["label"], "variant": variant, "status": "ok"}
    sh(engine["cmd"], f"DROP TABLE IF EXISTS {tbl}; " + DDL[variant].format(t=tbl) + ";", DB)
    src = paths["inline"] if variant == "inline" else \
        paths["side"] if variant == "side_nots" else paths["side_ts"]
    cols = {"inline": "(id,begin_timestamp,v)", "side_nots": "(value,archive_id)"} \
        .get(variant, "(value,begin_timestamp,archive_id)")
    t0 = time.time()
    rc, _, err = sh(engine["cmd"],
                    f"LOAD DATA LOCAL INFILE '{src}' INTO TABLE {tbl} {cols};",
                    DB, local_infile=True)
    r["bulk_s"] = round(time.time() - t0, 1)
    if rc != 0:
        r["status"] = "LOAD FAILED"
        r["note"] = (err.strip().splitlines()[-1] if err.strip() else "error")[:70]
        return r
    r["bulk_arch_s"] = round(archives / r["bulk_s"], 1) if r["bulk_s"] else None
    r["store_mb"], r["b_arch"] = table_mb(engine, tbl, archives)
    if do_optimize and variant in ("side_unpart", "side_part"):
        t0 = time.time()
        sh(engine["cmd"], f"OPTIMIZE TABLE {tbl};", DB)
        r["opt_s"] = round(time.time() - t0, 1)
        r["opt_mb"], _ = table_mb(engine, tbl, archives)
    for kind in ("exact", "prefix"):
        lit = needles["rare" if kind == "exact" else "prefix"]
        for wname, w1, w2 in WINDOWS:
            sql = predicate(variant, tbl, lit, kind, w1, w2)
            ms, scanned, hits, err = timed(engine, sql)
            r[f"{kind}_{wname}_ms"], r[f"{kind}_{wname}_hits"] = ms, hits
            if wname == "7d":
                r[f"{kind}_scan"] = scanned
                if variant == "side_part":
                    r[f"{kind}_parts"] = partitions_touched(engine, sql)
            if err:
                r["status"], r["note"] = "QUERY FAILED", err[:70]
    vidx = VARIANTS.index(variant)
    base_id = archives + vidx * 2 * len(batch_vals)
    r["ins1_s"] = insert_rate(engine, variant, tbl, batch_vals, base_id, 1)
    r["ins10_s"] = insert_rate(engine, variant, tbl, batch_vals,
                               base_id + len(batch_vals), 10)
    # GC comparison: remove one mid-year day from the side table both ways.
    if variant == "side_part":
        t0 = time.time()
        rc, _, _ = sh(engine["cmd"], f"ALTER TABLE {tbl} DROP PARTITION p_d{GC_DAY:03d};", DB)
        r["gc_ms"] = round((time.time() - t0) * 1000, 1) if rc == 0 else None
    elif variant == "side_unpart":
        w1, w2 = T0 + (GC_DAY - 1) * DAY, T0 + GC_DAY * DAY
        t0 = time.time()
        rc, _, _ = sh(engine["cmd"],
                      f"DELETE FROM {tbl} WHERE begin_timestamp >= {w1} "
                      f"AND begin_timestamp < {w2};", DB)
        r["gc_ms"] = round((time.time() - t0) * 1000, 1) if rc == 0 else None
    sh(engine["cmd"], f"DROP TABLE IF EXISTS {tbl};", DB)
    return r


def fmt(rows_out, profile, archives, n_vals, needles, out):
    def p(s):
        print(s)
        out.write(s + "\n")

    def num(r, k):
        v = r.get(k)
        if isinstance(v, float):
            return f"{v:,.1f}"
        if isinstance(v, int):
            return f"{v:,}"
        return "-"

    p("")
    p(f"PROFILE {profile} @ {archives:,} archives ({n_vals} values/archive; "
      f"side rows = {archives * n_vals:,})")
    p(f"  probes: exact='{needles['rare']}'  prefix='{needles['prefix']}*'  "
      f"windows anchored at day 180 of 366")
    p("")
    p("  BUILD AND WRITE")
    p("  " + "-" * 100)
    p(f"  {'engine':<9}{'variant':<13}{'store_MB':>10}{'B/arch':>8}{'opt_s':>8}"
      f"{'opt_MB':>9}{'bulk_s':>8}{'bulk_a/s':>10}{'ins1/s':>9}{'ins10/s':>9}"
      f"  {'status':<10}")
    p("  " + "-" * 100)
    for r in rows_out:
        p(f"  {r['engine']:<9}{r['variant']:<13}{num(r,'store_mb'):>10}{num(r,'b_arch'):>8}"
          f"{num(r,'opt_s'):>8}{num(r,'opt_mb'):>9}{num(r,'bulk_s'):>8}"
          f"{num(r,'bulk_arch_s'):>10}{num(r,'ins1_s'):>9}{num(r,'ins10_s'):>9}"
          f"  {r['status']:<10}")
    p("")
    p("  QUERY: time-window AND value predicate (warm ms / matching archives)")
    p("  " + "-" * 104)
    p(f"  {'engine':<9}{'variant':<13}{'probe':<8}{'1h_ms':>8}{'hits':>7}{'1d_ms':>8}"
      f"{'hits':>7}{'7d_ms':>9}{'hits':>7}{'scan7d':>10}{'parts':>7}")
    p("  " + "-" * 104)
    for r in rows_out:
        for kind in ("exact", "prefix"):
            p(f"  {r['engine']:<9}{r['variant']:<13}{kind:<8}"
              f"{num(r,f'{kind}_1h_ms'):>8}{num(r,f'{kind}_1h_hits'):>7}"
              f"{num(r,f'{kind}_1d_ms'):>8}{num(r,f'{kind}_1d_hits'):>7}"
              f"{num(r,f'{kind}_7d_ms'):>9}{num(r,f'{kind}_7d_hits'):>7}"
              f"{num(r,f'{kind}_scan'):>10}{str(r.get(f'{kind}_parts','-')):>7}")
    gc = [r for r in rows_out if r.get("gc_ms") is not None]
    if gc:
        p("")
        p("  GC (remove one mid-year day from the side table):")
        for r in gc:
            how = ("DROP PARTITION" if r["variant"] == "side_part"
                   else "row-wise DELETE")
            p(f"    {r['engine']:<9}{r['variant']:<13}{how:<16}{num(r,'gc_ms'):>10} ms")
    notes = [r for r in rows_out if r.get("note")]
    if notes:
        p("")
        for r in notes:
            p(f"  ! {r['engine']}/{r['variant']}: {r['note']}")
    for kind in ("exact", "prefix"):
        for wname, _, _ in WINDOWS:
            key = f"{kind}_{wname}_hits"
            vals = [r for r in rows_out if isinstance(r.get(key), int)]
            if not vals:
                continue
            truth = max(r[key] for r in vals)
            wrong = [r for r in vals if r[key] != truth]
            if wrong:
                p("")
                p(f"  *** DISAGREEMENT ({kind} {wname}): expected {truth} archives:")
                for r in wrong:
                    p(f"      {r['engine']}/{r['variant']}: {r[key]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", action="append", default=[], metavar="LABEL=COMMAND")
    ap.add_argument("--profile", action="append", default=[], choices=list(PROFILES))
    ap.add_argument("--scale", action="append", type=int, default=[],
                    help="archive counts (default: 100000 then 1000000)")
    ap.add_argument("--insert-batch", type=int, default=300)
    ap.add_argument("--skip-optimize", action="store_true")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default="lc2_bench_results.txt")
    ap.add_argument("--tmpdir", default=tempfile.gettempdir())
    a = ap.parse_args()

    engines, unreachable = detect(a.engine or DEFAULT_ENGINES)
    broken = [e for e in engines if e.get("error")]
    engines = [e for e in engines if not e.get("error")]
    for e in broken:
        sys.stderr.write(f"SKIPPING {e['label']} ({e['version']}): {e['error']}\n")
    if not engines:
        for cmdline, why in unreachable:
            sys.stderr.write(f"  tried: {cmdline}\n         -> {why}\n")
        user = os.environ.get("USER", "<you>")
        sys.exit(
            "\nNo usable server. The benchmark needs a database-scoped grant plus the\n"
            f"global RELOAD privilege (for FLUSH STATUS):\n\n"
            f"  sudo mysql -e \"GRANT ALL PRIVILEGES ON {DB}.* TO '{user}'@'localhost';\n"
            f"                 GRANT RELOAD ON *.* TO '{user}'@'localhost';\"\n")
    scales = a.scale or [100_000, 1_000_000]
    profiles = a.profile or list(PROFILES)

    out = open(a.out, "w")

    def line(s):
        print(s)
        out.write(s + "\n")

    line("=" * 104)
    line(" LOW-CARDINALITY FILTER - ROUND 2: inline vs side table at scale")
    line(" " + time.strftime("%Y-%m-%d %H:%M:%S"))
    for e in engines:
        line(f"   {e['label']:<9} {e['version']}")
        line(f"   {'':<9} {e['cfg']}")
        if e.get("binlog"):
            line(f"   {'':<9} WARNING: binary log is ON -- every commit pays a binlog "
                 f"fsync, so write rates")
            line(f"   {'':<9} measure that policy, not the schema. Add skip-log-bin "
                 f"(see setup_mysql8.sh) and restart.")
    missing = {"mariadb", "mysql"} - {e["flavour"] for e in engines}
    for m in sorted(missing):
        line(f"   MISSING: no {m} server reached; results cover only "
             f"{'/'.join(sorted(e['flavour'] for e in engines))}. "
             f"Run `sudo ./setup_mysql8.sh` or pass --engine.")
    line("=" * 104)

    for prof in profiles:
        n_vals, _, _ = PROFILES[prof]
        # The severity-like profile is a space/write shape check; the scale curve belongs
        # to the main profile.
        prof_scales = scales if prof == "id-like" else scales[:1]
        for archives in prof_scales:
            with tempfile.TemporaryDirectory(dir=a.tmpdir) as td:
                sys.stderr.write(f"[{prof} @ {archives:,}] generating corpus ...\n")
                paths, needles, batch_vals = generate(prof, archives, a.seed, td)
                batch_vals = batch_vals[:a.insert_batch]
                os.chmod(td, 0o755)
                for p in paths.values():
                    os.chmod(p, 0o644)
                results = []
                for e in engines:
                    sh(e["cmd"], f"CREATE DATABASE IF NOT EXISTS {DB};")
                    sys.stderr.write(f"  {e['label']}: loading base table ...\n")
                    sh(e["cmd"], "DROP TABLE IF EXISTS t_base; " + DDL["base"] + ";", DB)
                    sh(e["cmd"], f"LOAD DATA LOCAL INFILE '{paths['base']}' "
                                 f"INTO TABLE t_base (id,begin_timestamp);",
                       DB, local_infile=True)
                    for v in VARIANTS:
                        sys.stderr.write(f"  [{prof} @ {archives:,}] {e['label']}/{v} ...\n")
                        results.append(bench_variant(
                            e, v, paths, needles, batch_vals, archives, n_vals,
                            not a.skip_optimize, sys.stderr))
                    sh(e["cmd"], "DROP TABLE IF EXISTS t_base;", DB)
                fmt(results, prof, archives, n_vals, needles, out)
    for e in engines:
        sh(e["cmd"], f"DROP DATABASE IF EXISTS {DB};")
    line("")
    line(f"written to {a.out}")
    out.close()


if __name__ == "__main__":
    main()
