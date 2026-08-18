#!/usr/bin/env python3
"""Multi-tenancy: one side table PER dataset vs ONE unified table with dataset_id in the key.

B7 compared the two designs at 3 datasets and found a latency tie, which settles nothing
operationally. The costs that actually separate them are table-COUNT costs, and they only
appear at many tenants:

  T1  build      DDL time for N partitioned tables vs 1; partition-file count; load; bytes
                 -- including what an EMPTY tenant costs just to exist (per-file floor)
  T2  query      single-tenant lookup, and a cross-tenant lookup (N-way UNION ALL vs one
                 IN-list range) -- plus the unified table's trap: omitting dataset_id
  T3  retention  expiring one hour: N ALTER ... DROP PARTITION vs 1
  T4  tenant lifecycle  onboarding (CREATE) and offboarding (DROP TABLE vs DELETE)

The unified layout leads its PK with dataset_id:
    PRIMARY KEY (dataset_id, column_id, value, begin_timestamp, archive_id)
so one tenant's postings stay one contiguous key range, and hourly partitioning by
begin_timestamp is unchanged.

Usage:
    python3 bench_tenancy.py                              # 100 tenants x 2,000 archives
    python3 bench_tenancy.py --datasets 300 --per 1000
    python3 bench_tenancy.py --engine mariadb=mysql \\
        --engine mysql="/opt/mysql8/usr/bin/mysql --defaults-file=/etc/my8.cnf -u root"
"""
import argparse
import os
import shlex
import subprocess
import sys
import tempfile
import time

DB = "tenancy"
T0 = 1704067200 * 10**9
HOUR = 3600 * 10**9

# The c8 posting shape from bench4, so per-archive numbers stay comparable across rounds.
COLS = [("h0", 12, 1, 1000), ("h1", 12, 1, 1000), ("h2", 12, 1, 1000),
        ("e0", 8, 3, 12), ("e1", 8, 3, 12),
        ("s0", 10, 8, 40), ("s1", 10, 8, 40),
        ("m0", 14, 20, 300)]
POSTINGS = sum(c[2] for c in COLS)
SIDE_COLS = ("column_id TINYINT UNSIGNED NOT NULL, value VARCHAR(64) NOT NULL, "
             "begin_timestamp BIGINT NOT NULL, archive_id INT UNSIGNED NOT NULL")


def sh(e, sql, db=None, local_infile=False, timeout=3600):
    cmd = shlex.split(e["cmd"])
    if local_infile:
        cmd.append("--local-infile=1")
    if db:
        cmd.append(db)
    try:
        p = subprocess.run(cmd + ["-e", sql], capture_output=True, text=True, timeout=timeout)
    except OSError as ex:
        return 127, "", str(ex)
    except subprocess.TimeoutExpired:
        return 124, "", "timed out"
    return p.returncode, p.stdout, p.stderr


def detect(specs):
    out, seen = [], set()
    for spec in specs:
        label, _, cmd = spec.partition("=")
        if not cmd:
            label, cmd = os.path.basename(shlex.split(spec)[0]), spec
        e = {"label": label, "cmd": cmd}
        rc, ident, err = sh(e, "SELECT CONCAT_WS('|', @@socket, @@port, VERSION());")
        if rc != 0:
            sys.stderr.write(f"  skip {label}: {err.strip().splitlines()[-1][:70]}\n")
            continue
        ident = ident.strip().splitlines()[-1]
        if ident in seen:
            continue
        rc, _, err = sh(e, f"DROP DATABASE IF EXISTS {DB}; CREATE DATABASE {DB};")
        if rc != 0:
            sys.stderr.write(f"  skip {label}: {err.strip().splitlines()[-1][:70]}\n")
            continue
        seen.add(ident)
        e["version"] = ident.rsplit("|", 1)[-1]
        e["flavour"] = "mariadb" if "mariadb" in e["version"].lower() else "mysql"
        out.append(e)
    return out


def parts(hours):
    p = [f"PARTITION p_floor VALUES LESS THAN ({T0})"]
    p += [f"PARTITION p_h{h:06d} VALUES LESS THAN ({T0 + (h + 1) * HOUR})"
          for h in range(hours)]
    p.append("PARTITION p_future VALUES LESS THAN MAXVALUE")
    return "PARTITION BY RANGE (begin_timestamp) (" + ",".join(p) + ")"


def val(tag, width, i):
    return (tag + str(i)).ljust(width, "x")[:width]


def gen(a, outdir):
    """One TSV per tenant (for the split loads) plus one unified TSV with dataset_id."""
    import random
    rng = random.Random(7)
    pools = {c[0]: [val(c[0], c[1], i) for i in range(c[3])] for c in COLS}
    span = max(1, a.hours * HOUR // a.per)
    upath = os.path.join(outdir, "unified.tsv")
    dpaths = []
    with open(upath, "w") as fu:
        for k in range(a.datasets):
            dp = os.path.join(outdir, f"d{k:04d}.tsv")
            dpaths.append(dp)
            with open(dp, "w") as fd:
                for i in range(a.per):
                    ts = T0 + i * span + rng.randrange(0, span)
                    for n, c in enumerate(COLS):
                        for v in rng.sample(pools[c[0]], c[2]):
                            fd.write(f"{n}\t{v}\t{ts}\t{i}\n")
                            fu.write(f"{k}\t{n}\t{v}\t{ts}\t{i}\n")
    for p in dpaths + [upath]:
        os.chmod(p, 0o644)
    return dpaths, upath


def file_bytes(e, like):
    """Allocated file bytes for tablespaces matching db/<like>. This is the number that
    exposes the per-partition floor: an EMPTY partition still owns a 64 KB (or larger)
    file, which information_schema.tables' data_length never shows."""
    view = ("INNODB_SYS_TABLESPACES" if e["flavour"] == "mariadb" else "INNODB_TABLESPACES")
    rc, out, _ = sh(e, f"SELECT COALESCE(SUM(FILE_SIZE),0), COUNT(*) FROM "
                       f"information_schema.{view} WHERE NAME LIKE '{DB}/{like}';")
    nums = [int(x) for x in out.split() if x.isdigit()]
    return (nums[-2], nums[-1]) if len(nums) >= 2 else (0, 0)


def data_bytes(e, like):
    """Page-accurate bytes (data_length + index_length) summed over matching tables.
    Complements file_bytes: .ibd files grow in 4 MB steps once they pass ~1.5 MB, so at
    small scale FILE_SIZE is quantization, not data. ANALYZE first -- the stats are cached."""
    rc, out, _ = sh(e, f"SELECT GROUP_CONCAT(table_name) FROM information_schema.tables "
                       f"WHERE table_schema='{DB}' AND table_name LIKE '{like}';")
    names = (out.strip().splitlines() or [""])[-1]
    if names and names != "NULL":
        sh(e, "ANALYZE TABLE " + ", ".join(names.split(",")) + ";", DB)
    fresh = "SET SESSION information_schema_stats_expiry=0;\n" if e["flavour"] == "mysql" else ""
    rc, out, _ = sh(e, fresh + "SELECT COALESCE(SUM(data_length + index_length),0) "
                               f"FROM information_schema.tables WHERE table_schema='{DB}' "
                               f"AND table_name LIKE '{like}';")
    nums = [int(x) for x in out.split() if x.isdigit()]
    return nums[-1] if nums else 0


def timed(e, sql, timeout_s=300):
    """Warm wall-clock: runs twice, reports the second."""
    guard = (f"SET SESSION max_statement_time={timeout_s};\n" if e["flavour"] == "mariadb"
             else f"SET SESSION max_execution_time={timeout_s * 1000};\n")
    sh(e, guard + sql, DB, timeout=timeout_s + 60)
    t0 = time.time()
    rc, out, err = sh(e, guard + sql, DB, timeout=timeout_s + 60)
    ms = (time.time() - t0) * 1000
    if rc != 0:
        return None, (err.strip().splitlines() or ["?"])[-1][:60]
    return ms, None


def wall(fn):
    t0 = time.time()
    fn()
    return time.time() - t0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", action="append", default=[])
    ap.add_argument("--datasets", type=int, default=100)
    ap.add_argument("--per", type=int, default=2000, help="archives per dataset")
    ap.add_argument("--hours", type=int, default=168)
    ap.add_argument("--tmpdir", default=tempfile.gettempdir())
    ap.add_argument("--out", default="")
    a = ap.parse_args()

    engines = detect(a.engine or ["mariadb=mysql", "mariadb=mariadb",
                                  "mysql=/opt/mysql8/usr/bin/mysql "
                                  "--defaults-file=/etc/my8.cnf -u root"])
    if not engines:
        sys.exit(f"no usable server (needs GRANT ALL ON {DB}.*)")

    outpath = a.out or f"tenancy_results_{time.strftime('%Y%m%d-%H%M%S')}.txt"
    out = open(outpath, "w")

    def line(s):
        print(s, flush=True)
        out.write(s + "\n")
        out.flush()

    td = tempfile.mkdtemp(dir=a.tmpdir)
    os.chmod(td, 0o755)
    total_arch = a.datasets * a.per
    sys.stderr.write(f"generating {total_arch * POSTINGS:,} postings for "
                     f"{a.datasets} tenants ...\n")
    dpaths, upath = gen(a, td)

    line(f" MULTI-TENANCY  {time.strftime('%Y-%m-%d %H:%M:%S')}")
    line(f"   {a.datasets} tenants x {a.per:,} archives x {POSTINGS} postings = "
         f"{total_arch * POSTINGS:,} rows per design; {a.hours} hourly partitions/table")
    for e in engines:
        line(f"   {e['label']:<9} {e['version']}")

    pdef = parts(a.hours)
    nfiles = a.hours + 2
    for e in engines:
        sh(e, f"DROP DATABASE IF EXISTS {DB}; CREATE DATABASE {DB};")
        line("")
        line("=" * 96)
        line(f" T1  build  [{e['label']}]")
        line("=" * 96)

        def create_split():
            for k in range(a.datasets):
                if k % 25 == 0:
                    sys.stderr.write(f"    [{e['label']}] create split {k}/{a.datasets}\n")
                sh(e, f"CREATE TABLE side_d{k:04d} ({SIDE_COLS}, PRIMARY KEY "
                      f"(column_id, value, begin_timestamp, archive_id)) "
                      f"ENGINE=InnoDB DEFAULT CHARSET=ascii {pdef};", DB)
        t_cs = wall(create_split)
        eb_s, ef_s = file_bytes(e, "side_d%")

        def create_uni():
            sh(e, f"CREATE TABLE side_all (dataset_id SMALLINT UNSIGNED NOT NULL, "
                  f"{SIDE_COLS}, PRIMARY KEY (dataset_id, column_id, value, "
                  f"begin_timestamp, archive_id)) ENGINE=InnoDB DEFAULT CHARSET=ascii "
                  f"{pdef};", DB)
        t_cu = wall(create_uni)
        eb_u, ef_u = file_bytes(e, "side_all%")

        line(f"  {'':<26}{'split (N tables)':>18}{'unified (1 table)':>19}")
        line("  " + "-" * 66)
        line(f"  {'CREATE time':<26}{t_cs:>16.1f} s{t_cu:>17.1f} s")
        line(f"  {'partition files':<26}{ef_s:>18,}{ef_u:>19,}")
        line(f"  {'bytes while EMPTY':<26}{eb_s / 1048576:>15.1f} MB{eb_u / 1048576:>16.1f} MB")
        line(f"  {'  = per empty tenant':<26}{eb_s / a.datasets / 1024:>15.1f} KB"
             f"{'(none)':>19}")

        def load_split():
            for k, dp in enumerate(dpaths):
                if k % 25 == 0:
                    sys.stderr.write(f"    [{e['label']}] load split {k}/{a.datasets}\n")
                sh(e, f"LOAD DATA LOCAL INFILE '{dp}' INTO TABLE side_d{k:04d} "
                      f"(column_id,value,begin_timestamp,archive_id);", DB, True)
        t_ls = wall(load_split)
        fb_s, _ = file_bytes(e, "side_d%")

        def load_uni():
            sys.stderr.write(f"    [{e['label']}] load unified\n")
            sh(e, f"LOAD DATA LOCAL INFILE '{upath}' INTO TABLE side_all "
                  f"(dataset_id,column_id,value,begin_timestamp,archive_id);", DB, True)
        t_lu = wall(load_uni)
        fb_u, _ = file_bytes(e, "side_all%")
        db_s = data_bytes(e, "side\\_d%")
        db_u = data_bytes(e, "side\\_all")

        line(f"  {'load time':<26}{t_ls:>16.1f} s{t_lu:>17.1f} s")
        line(f"  {'file bytes loaded':<26}{fb_s / 1048576:>15.1f} MB{fb_u / 1048576:>16.1f} MB")
        line(f"  {'data bytes (pages)':<26}{db_s / 1048576:>15.1f} MB{db_u / 1048576:>16.1f} MB")
        line(f"  {'  data B/archive':<26}{db_s / total_arch:>18,.0f}{db_u / total_arch:>19,.0f}")
        line("  (file bytes are what the disk loses: per-partition floors plus 4 MB extension")
        line("   steps. data bytes are the pages actually filled -- the number comparable to")
        line("   earlier rounds. At small --per the gap between them is quantization, not data.)")

        line("")
        line("=" * 96)
        line(f" T2  query (warm, ms)  [{e['label']}]")
        line("=" * 96)
        w1 = T0 + (a.hours - 24) * HOUR
        w2 = T0 + a.hours * HOUR
        tw = f"begin_timestamp >= {w1} AND begin_timestamp < {w2}"
        probe = val("m0", 14, 7)
        pred = f"column_id=7 AND value='{probe}' AND {tw}"
        k = a.datasets // 2
        line(f"  {'query':<44}{'split':>10}{'unified':>10}")
        line("  " + "-" * 66)
        m1, err1 = timed(e, f"SELECT COUNT(DISTINCT archive_id) FROM side_d{k:04d} "
                            f"WHERE {pred};")
        m2, err2 = timed(e, f"SELECT COUNT(DISTINCT archive_id) FROM side_all "
                            f"WHERE dataset_id={k} AND {pred};")
        line(f"  {'one tenant, 24h window':<44}"
             f"{m1 if m1 is not None else err1:>10.1f}{m2 if m2 is not None else err2:>10.1f}")
        union = " UNION ALL ".join(
            f"SELECT COUNT(DISTINCT archive_id) c FROM side_d{j:04d} WHERE {pred}"
            for j in range(a.datasets))
        m3, err3 = timed(e, f"SELECT SUM(c) FROM ({union}) u;")
        inlist = ",".join(str(j) for j in range(a.datasets))
        m4, err4 = timed(e, f"SELECT COUNT(DISTINCT dataset_id, archive_id) FROM side_all "
                            f"WHERE dataset_id IN ({inlist}) AND {pred};")
        line(f"  {'ALL tenants (UNION ALL vs IN-list)':<44}"
             f"{m3 if m3 is not None else err3:>10.1f}{m4 if m4 is not None else err4:>10.1f}")
        m5, err5 = timed(e, f"SELECT COUNT(DISTINCT dataset_id, archive_id) FROM side_all "
                            f"WHERE {pred};")
        line(f"  {'unified, dataset_id OMITTED (the trap)':<44}{'--':>10}"
             f"{m5 if m5 is not None else err5:>10.1f}")
        line("    (omitting dataset_id forfeits the PK prefix: the index cannot seek on")
        line("     column_id/value alone, so the predicate scans the whole time window)")

        line("")
        line("=" * 96)
        line(f" T3  retention: expire the oldest hour  [{e['label']}]")
        line("=" * 96)

        def drop_split():
            for j in range(a.datasets):
                sh(e, f"ALTER TABLE side_d{j:04d} DROP PARTITION p_h000000;", DB)
        t_ds = wall(drop_split)

        def drop_uni():
            sh(e, "ALTER TABLE side_all DROP PARTITION p_h000000;", DB)
        t_du = wall(drop_uni)
        line(f"  split:   {a.datasets} x ALTER ... DROP PARTITION = {t_ds:,.2f} s "
             f"({t_ds / a.datasets * 1000:,.0f} ms per tenant)")
        line(f"  unified: 1 x ALTER ... DROP PARTITION = {t_du:,.2f} s")

        line("")
        line("=" * 96)
        line(f" T4  tenant lifecycle  [{e['label']}]")
        line("=" * 96)
        t_on = wall(lambda: sh(e, f"CREATE TABLE side_dnew ({SIDE_COLS}, PRIMARY KEY "
                                  f"(column_id, value, begin_timestamp, archive_id)) "
                                  f"ENGINE=InnoDB DEFAULT CHARSET=ascii {pdef};", DB))
        sh(e, "DROP TABLE side_dnew;", DB)
        line(f"  onboard  split:   CREATE TABLE ({nfiles} partitions) = {t_on:,.2f} s")
        line(f"  onboard  unified: nothing to create = 0 s")
        t_offs = wall(lambda: sh(e, f"DROP TABLE side_d{a.datasets - 1:04d};", DB))
        t_offu = wall(lambda: sh(e, f"DELETE FROM side_all "
                                    f"WHERE dataset_id={a.datasets - 1};", DB,
                                 timeout=1800))
        line(f"  offboard split:   DROP TABLE = {t_offs:,.2f} s (space back immediately)")
        line(f"  offboard unified: DELETE {a.per * POSTINGS:,} rows = {t_offu:,.2f} s "
             f"(space returns only after purge/OPTIMIZE)")

        sh(e, f"DROP DATABASE IF EXISTS {DB};")

    import shutil
    shutil.rmtree(td, ignore_errors=True)
    line(f"\nwritten to {outpath}")
    out.close()


if __name__ == "__main__":
    main()
