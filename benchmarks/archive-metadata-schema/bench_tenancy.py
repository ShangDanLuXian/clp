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
    python3 bench_tenancy.py --datasets 100 --per 3000    # the <=1h configuration
    python3 bench_tenancy.py                              # 100 tenants x 10,000 archives
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


def limits(e, need_files):
    """Reports the server's file-handle headroom against what the split design demands.

    Every partition is its own tablespace file, so N tenants x P partitions is N*P files.
    Once that exceeds innodb_open_files the server evicts and reopens tablespaces
    continuously, which shows up as load and query time rather than as an error -- so the
    ratio has to be printed alongside the results or they cannot be interpreted."""
    got = {}
    for v in ("open_files_limit", "table_open_cache", "innodb_open_files"):
        rc, out, _ = sh(e, f"SHOW GLOBAL VARIABLES LIKE '{v}';")
        nums = [int(x) for x in out.split() if x.isdigit()]
        got[v] = nums[-1] if nums else 0
    got["files_needed_by_split"] = need_files
    got["thrashing"] = need_files > got["innodb_open_files"]
    return got


def _bp_settle(e):
    """Waits for an online buffer-pool resize to finish and returns the ACTUAL size the
    server settled on. Never assumes the requested size was honored: servers clamp
    out-of-range requests, and an exact-match wait then spins forever."""
    prev = -1
    for _ in range(480):
        _, o, _ = sh(e, "SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_resize_status';")
        busy = "resizing" in o.lower() or "completing" in o.lower()
        _, o, _ = sh(e, "SELECT @@innodb_buffer_pool_size;")
        n = [int(x) for x in o.split() if x.isdigit()]
        cur = n[-1] if n else -1
        if not busy and cur == prev and cur > 0:
            return cur
        prev = cur
        time.sleep(0.5)
    return prev


def bp_cold(e):
    """Evicts the buffer pool so the next query reads storage: shrink to one 128 MB chunk,
    sweep the evictor table through the shrunken pool, restore and VERIFY the original size.

    Returns "evict" on success, or a string starting with "unavailable:" naming the ACTUAL
    reason. Two failures are distinct and must not be conflated (an earlier version reported
    both as "pool too small", which printed '4 GB is at or below 128 MB'):
      - the pool already sits at the floor, so there is nothing to cycle;
      - the server refused or clamped the shrink, so the pool never emptied.
    Sweeping alone is never a fallback: scanned pages enter the OLD sublist
    (innodb_old_blocks_pct, default 37%) and are evicted from there without displacing the
    YOUNG pages, so a sweep churns a third of the pool and leaves arbitrary residue."""
    if "bp" not in e:
        _, o, _ = sh(e, "SELECT @@innodb_buffer_pool_size;")
        n = [int(x) for x in o.split() if x.isdigit()]
        e["bp"] = n[-1] if n else 0
        _, o, _ = sh(e, "SELECT @@innodb_flush_method;")
        e["flush"] = (o.split() or ["?"])[-1]
    size = e["bp"]
    if size <= 134217728:
        return (f"unavailable: pool is {size:,} B, already at/below the 128 MB floor this "
                f"evicts to -- nothing to cycle")
    rc, _, err = sh(e, "SET GLOBAL innodb_buffer_pool_size=134217728;")
    got = _bp_settle(e)
    shrunk = 0 < got < size
    sh(e, "SELECT COALESCE(SUM(begin_timestamp % 7), 0) FROM evictor;", DB)
    sh(e, f"SET GLOBAL innodb_buffer_pool_size={size};")
    if _bp_settle(e) != size:
        sh(e, f"SET GLOBAL innodb_buffer_pool_size={size};")
        if _bp_settle(e) != size:
            return "evict (POOL RESTORE FAILED -- restore it manually)"
    if shrunk:
        return "evict"
    detail = (err.strip().splitlines() or [""])[-1][:60] if rc != 0 else \
        f"requested 128 MB, server settled at {got:,} B"
    return f"unavailable: shrink refused/clamped ({detail})"


def timed_cw(e, sql, do_cold, timeout_s=300):
    """do_cold False means the first column is merely run1, not cold."""
    """(cold_ms, warm_ms, err): evict, run once cold, re-run immediately for warm.
    With do_cold False the first run is merely run1 (load-warmed), kept for the column."""
    if do_cold:
        bp_cold(e)
    guard = (f"SET SESSION max_statement_time={timeout_s};\n" if e["flavour"] == "mariadb"
             else f"SET SESSION max_execution_time={timeout_s * 1000};\n")
    t0 = time.time()
    rc, _, err = sh(e, guard + sql, DB, timeout=timeout_s + 60)
    cold = (time.time() - t0) * 1000
    if rc != 0:
        return None, None, (err.strip().splitlines() or ["?"])[-1][:60]
    t0 = time.time()
    rc, _, err = sh(e, guard + sql, DB, timeout=timeout_s + 60)
    warm = (time.time() - t0) * 1000
    if rc != 0:
        return cold, None, (err.strip().splitlines() or ["?"])[-1][:60]
    return cold, warm, None


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


def fmt2(cold, warm, err):
    """A 'cold/warm' cell; a failed query prints its error instead of crashing the row."""
    if err is not None and cold is None:
        return f"{err[:19]:>21}"
    w = f"{warm:,.0f}" if warm is not None else "ERR"
    return f"{cold:>12,.0f} /{w:>7}"


def fmt(ms, err):
    """Formats a timing cell. A failed query returns an error STRING, and applying a float
    format code to it raises ValueError mid-report -- which is how the first run died."""
    return f"{ms:>10.1f}" if ms is not None else f"{(err or 'ERR')[:10]:>10}"


def wall(fn):
    t0 = time.time()
    fn()
    return time.time() - t0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", action="append", default=[])
    ap.add_argument("--datasets", type=int, default=100)
    ap.add_argument("--per", type=int, default=10_000,
                    help="archives per dataset (default 100 x 10,000 = 1M archives)")
    ap.add_argument("--hours", type=int, default=168)
    ap.add_argument("--keep", action="store_true",
                    help="leave the tenancy database in place for external cold probing")
    ap.add_argument("--no-cold", action="store_true",
                    help="skip buffer-pool eviction; first latency column is then run1")
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
        lim = limits(e, a.datasets * nfiles)
        line("")
        line("=" * 96)
        line(f" T0  file-handle headroom  [{e['label']}]")
        line("=" * 96)
        line(f"  split needs {lim['files_needed_by_split']:,} tablespace files "
             f"({a.datasets} tenants x {nfiles} partitions); unified needs {nfiles:,}")
        line(f"  innodb_open_files={lim['innodb_open_files']:,}  "
             f"table_open_cache={lim['table_open_cache']:,}  "
             f"open_files_limit={lim['open_files_limit']:,}")
        if lim["thrashing"]:
            line("  -> split EXCEEDS innodb_open_files: the server will evict and reopen")
            line("     tablespaces continuously. This is a real cost of the design, not a")
            line("     misconfiguration, but read T1/T2 split timings with it in mind.")
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

        # Load failures must stop the run. Ignoring them produced a report whose sizes and
        # timings were all zero while still looking like results -- worse than a crash.
        fails = []

        def load_split():
            for k, dp in enumerate(dpaths):
                if k % 25 == 0:
                    sys.stderr.write(f"    [{e['label']}] load split {k}/{a.datasets}\n")
                rc, _, err = sh(e, f"LOAD DATA LOCAL INFILE '{dp}' INTO TABLE side_d{k:04d} "
                                   f"(column_id,value,begin_timestamp,archive_id);", DB, True)
                if rc != 0 and len(fails) < 3:
                    fails.append(f"split tenant {k}: "
                                 f"{(err.strip().splitlines() or ['?'])[-1][:70]}")
        t_ls = wall(load_split)
        fb_s, _ = file_bytes(e, "side_d%")

        def load_uni():
            sys.stderr.write(f"    [{e['label']}] load unified\n")
            rc, _, err = sh(e, f"LOAD DATA LOCAL INFILE '{upath}' INTO TABLE side_all "
                               f"(dataset_id,column_id,value,begin_timestamp,archive_id);",
                            DB, True)
            if rc != 0:
                fails.append(f"unified: {(err.strip().splitlines() or ['?'])[-1][:70]}")
        t_lu = wall(load_uni)
        fb_u, _ = file_bytes(e, "side_all%")
        db_s = data_bytes(e, "side\\_d%")
        db_u = data_bytes(e, "side\\_all")

        if fails or db_s == 0 or db_u == 0:
            line("")
            line("  !! LOAD FAILED -- everything below this point would be meaningless.")
            for f in fails:
                line(f"     {f}")
            if not fails and (db_s == 0 or db_u == 0):
                line("     loads reported success but the tables measure 0 bytes;")
                line("     the server is likely wedged (disk full leaves InnoDB read-only).")
            line(f"     split measured {db_s:,} B, unified measured {db_u:,} B")
            line("     Free disk (this run needs ~15 GB at the default scale) or lower")
            line("     --per, then re-run. T1 CREATE/empty-size numbers above are still valid.")
            sh(e, f"DROP DATABASE IF EXISTS {DB};")
            continue

        line(f"  {'load time':<26}{t_ls:>16.1f} s{t_lu:>17.1f} s")
        line(f"  {'file bytes loaded':<26}{fb_s / 1048576:>15.1f} MB{fb_u / 1048576:>16.1f} MB")
        line(f"  {'data bytes (pages)':<26}{db_s / 1048576:>15.1f} MB{db_u / 1048576:>16.1f} MB")
        line(f"  {'  data B/archive':<26}{db_s / total_arch:>18,.0f}{db_u / total_arch:>19,.0f}")
        line("  (file bytes are what the disk loses: per-partition floors plus 4 MB extension")
        line("   steps. data bytes are the pages actually filled -- the number comparable to")
        line("   earlier rounds. At small --per the gap between them is quantization, not data.)")

        line("")
        line("=" * 96)
        line(f" T2  query latency, cold / warm ms  [{e['label']}]")
        line("=" * 96)
        # The evictor exists so a shrunken pool can be swept clean of BOTH designs'
        # pages; timed queries never touch it. ~200 MB, larger than the shrink target.
        do_cold = not a.no_cold
        mode = None
        if do_cold:
            sh(e, "DROP TABLE IF EXISTS evictor; CREATE TABLE evictor AS "
                  "SELECT * FROM side_all LIMIT 3000000;", DB)
            mode = bp_cold(e)
            do_cold = not str(mode).startswith("unavailable:")
        if do_cold:
            line(f"  cold = first run after buffer-pool eviction (mode: {mode}; "
                 f"flush_method={e.get('flush', '?')}).")
            line("  O_DIRECT means cold reads truly hit storage; a buffered flush_method "
                 "can still")
            line("  serve them from the OS page cache. warm = same statement re-issued.")
        elif a.no_cold:
            line("  (--no-cold: first column is run1, load-warmed, NOT cold)")
        else:
            line(f"  COLD UNAVAILABLE -- {str(mode).replace('unavailable: ', '')}.")
            line("  A scan-only sweep does NOT evict InnoDB (scan-resistant LRU), so cold")
            line("  numbers would be arbitrary. The first column is run1 instead: it is")
            line("  PARTIALLY cold (the evictor sweep and the resize attempt do displace")
            line("  pages) but it is not a controlled measurement -- do not quote it as cold.")
        w1 = T0 + (a.hours - 24) * HOUR
        w2 = T0 + a.hours * HOUR
        tw = f"begin_timestamp >= {w1} AND begin_timestamp < {w2}"
        probe = val("m0", 14, 7)
        pred = f"column_id=7 AND value='{probe}' AND {tw}"
        k = a.datasets // 2
        c0 = "cold/warm" if do_cold else "run1/warm"
        line(f"  {'query':<42}{'split ' + c0:>21}{'unified ' + c0:>22}")
        line("  " + "-" * 86)
        c1, w1m, e1 = timed_cw(e, f"SELECT COUNT(DISTINCT archive_id) FROM side_d{k:04d} "
                                  f"WHERE {pred};", do_cold)
        c2, w2m, e2 = timed_cw(e, f"SELECT COUNT(DISTINCT archive_id) FROM side_all "
                                  f"WHERE dataset_id={k} AND {pred};", do_cold)
        line(f"  {'one tenant, 24h window':<42}{fmt2(c1, w1m, e1)}{fmt2(c2, w2m, e2):>22}")
        union = " UNION ALL ".join(
            f"SELECT COUNT(DISTINCT archive_id) c FROM side_d{j:04d} WHERE {pred}"
            for j in range(a.datasets))
        c3, w3m, e3 = timed_cw(e, f"SELECT SUM(c) FROM ({union}) u;", do_cold)
        inlist = ",".join(str(j) for j in range(a.datasets))
        c4, w4m, e4 = timed_cw(e, f"SELECT COUNT(DISTINCT dataset_id, archive_id) "
                                  f"FROM side_all WHERE dataset_id IN ({inlist}) "
                                  f"AND {pred};", do_cold)
        line(f"  {'ALL tenants (UNION ALL vs IN-list)':<42}{fmt2(c3, w3m, e3)}"
             f"{fmt2(c4, w4m, e4):>22}")
        c5, w5m, e5 = timed_cw(e, f"SELECT COUNT(DISTINCT dataset_id, archive_id) "
                                  f"FROM side_all WHERE {pred};", do_cold)
        line(f"  {'unified, dataset_id OMITTED (the trap)':<42}{'--':>21}"
             f"{fmt2(c5, w5m, e5):>22}")
        line("    (omitting dataset_id forfeits the PK prefix: the index cannot seek on")
        line("     column_id/value alone, so the predicate scans the whole time window.")
        line("     cold split pays first-touch on ONE tenant table; cold unified pays it")
        line("     on the one big tree -- the comparison production users actually feel.)")

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

        if not a.keep:
            sh(e, f"DROP DATABASE IF EXISTS {DB};")
        else:
            line(f"\n  --keep: database `{DB}` left in place for cold probing "
                 f"(./cold_probe.sh)")

    import shutil
    shutil.rmtree(td, ignore_errors=True)
    line(f"\nwritten to {outpath}")
    out.close()


if __name__ == "__main__":
    main()
