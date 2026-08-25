#!/usr/bin/env python3
"""Cold/warm query probe against an EXISTING topology database.

The topology run reported one wall-time sample per query (a third execution, through a
fresh mysql subprocess with a ~2 ms client-startup floor) and no cold numbers at all. This
probe fixes both against whatever tables still exist, without rebuilding anything:

  - N repetitions per query (default 7), reporting min / median / max.
  - Server-side timing: every batch runs in ONE mysql session, each execution bracketed by
    SELECT NOW(6) markers, so neither process spawn nor connection setup is in the number.
  - A real cold pass, if --restart-cmd is given. Online buffer-pool eviction does not work
    on MariaDB 10.6 -- SET GLOBAL innodb_buffer_pool_size refuses or clamps an online
    shrink, and a table scan cannot evict the pool because scanned pages enter the LRU's
    old sublist and never displace the young ones -- so cold means a restart, with two
    traps handled here: the buffer-pool dump/reload (defaults ON; a naive restart comes
    back WARM) is disabled first, and the default innodb_flush_method=fsync does buffered
    I/O, so the OS page cache still holds file pages after a restart. Pass --drop-caches
    (needs root) to flush that too; without it the cold column is InnoDB-cold, OS-warm,
    and is labelled as such.
  - Coldness is VERIFIED, not assumed: each cold sample records the Innodb_buffer_pool_reads
    delta, and a cold row with zero physical reads is flagged "not cold" instead of
    reported as a cold time.

Discovery: the benchmark drops each configuration's schemas when its cycle ends, so this
probe first lists which side tables still exist and probes only those. If none survive,
rebuild one configuration with bench_topology.py --keep.

Usage (match the flags to the original run; defaults match the 100-dataset run):
    python3 query_probe.py --engine mariadb=mysql
    python3 query_probe.py --engine mariadb=mysql --reps 9 \\
        --restart-cmd "service mariadb restart" --drop-caches
"""
import argparse
import os
import shlex
import statistics
import subprocess
import sys
import tempfile
import time

DB = "topo"
T0 = 1704067200 * 10**9
HOUR = 3600 * 10**9


def sh(e, sql, db=None, timeout=600):
    cmd = shlex.split(e["cmd"]) + ([db] if db else [])
    try:
        p = subprocess.run(cmd + ["-N", "-B", "-e", sql], capture_output=True, text=True,
                           timeout=timeout)
    except OSError as ex:
        return 127, "", str(ex)
    except subprocess.TimeoutExpired:
        return 124, "", "timed out"
    return p.returncode, p.stdout, p.stderr


def val(tag, width, i):
    return (tag + str(i)).ljust(width, "x")[:width]


def exists(e, schema, table):
    _, o, _ = sh(e, "SELECT COUNT(*) FROM information_schema.tables WHERE "
                    f"table_schema='{schema}' AND table_name='{table}';")
    return o.strip().splitlines()[-1:] == ["1"]


def queries(a, ds, at, st):
    """The single-dataset shapes Q1-Q7, byte-identical to the benchmark's."""
    w24 = T0 + (a.hours - 24) * HOUR
    w7d = T0 + max(0, a.hours - 168) * HOUR
    wend = T0 + a.hours * HOUR
    tw24 = f"begin_timestamp >= {w24} AND begin_timestamp < {wend}"
    tw7d = f"begin_timestamp >= {w7d} AND begin_timestamp < {wend}"
    sel, hot, other = val("m0", 14, 7), val("e0", 8, 3), val("s0", 10, 3)
    d = f"dataset_id={a.k} AND " if ds else ""
    dx = f"x.dataset_id={a.k} AND " if ds else ""
    dss = f"s.dataset_id={a.k} AND " if ds else ""
    j = " AND y.dataset_id=x.dataset_id" if ds else ""
    ja = " AND a.dataset_id=s.dataset_id" if ds else ""
    return [
        ("Q1 point, 24h", f"SELECT COUNT(DISTINCT archive_id) FROM {st} "
                          f"WHERE {d}column_id=7 AND value='{sel}' AND {tw24}"),
        ("Q2 point, 7d", f"SELECT COUNT(DISTINCT archive_id) FROM {st} "
                         f"WHERE {d}column_id=7 AND value='{sel}' AND {tw7d}"),
        ("Q3 hot value, 24h", f"SELECT COUNT(DISTINCT archive_id) FROM {st} "
                              f"WHERE {d}column_id=3 AND value='{hot}' AND {tw24}"),
        ("Q4 prefix wildcard", f"SELECT COUNT(DISTINCT archive_id) FROM {st} "
                               f"WHERE {d}column_id=7 AND value LIKE '{sel[:5]}%' AND {tw24}"),
        ("Q5 two predicates", f"SELECT COUNT(*) FROM {st} x JOIN {st} y "
                              f"ON y.archive_id=x.archive_id "
                              f"AND y.begin_timestamp=x.begin_timestamp{j} "
                              f"WHERE {dx}x.column_id=7 AND x.value='{sel}' "
                              f"AND y.column_id=5 AND y.value='{other}' "
                              f"AND x.begin_timestamp >= {w24} AND x.begin_timestamp < {wend}"),
        ("Q6 join metadata", f"SELECT COUNT(*), MAX(a.size_bytes) FROM {st} s JOIN {at} a "
                             f"ON a.archive_id=s.archive_id "
                             f"AND a.begin_timestamp=s.begin_timestamp{ja} "
                             f"WHERE {dss}s.column_id=7 AND s.value='{sel}' "
                             f"AND s.begin_timestamp >= {w24} AND s.begin_timestamp < {wend}"),
        ("Q7 archives range", f"SELECT COUNT(*), SUM(size_bytes) FROM {at} WHERE {d}{tw24}"),
    ]


def batch(e, schema, sql, reps):
    """Run `sql` reps times in ONE session; return ([ms...], [phys...]) measured server-side.

    Each repetition is bracketed by NOW(6) markers and Innodb_buffer_pool_reads samples, so
    the times exclude client startup and the phys column verifies where pages came from."""
    probe = ("SELECT CONCAT('##P,', VARIABLE_VALUE) FROM information_schema.GLOBAL_STATUS "
             "WHERE VARIABLE_NAME='Innodb_buffer_pool_reads';")
    parts = []
    for _ in range(reps):
        parts += [probe, "SELECT CONCAT('##T,', UNIX_TIMESTAMP(NOW(6)));", sql + ";",
                  "SELECT CONCAT('##T,', UNIX_TIMESTAMP(NOW(6)));"]
    parts.append(probe)
    rc, o, err = sh(e, "\n".join(parts), schema)
    if rc != 0:
        return None, None, (err.strip().splitlines() or ["?"])[-1][:70]
    ts = [float(l.split(",")[1]) for l in o.splitlines() if l.startswith("##T,")]
    ph = [int(float(l.split(",")[1])) for l in o.splitlines() if l.startswith("##P,")]
    ms = [(ts[i + 1] - ts[i]) * 1000 for i in range(0, len(ts) - 1, 2)]
    phys = [ph[i + 1] - ph[i] for i in range(len(ph) - 1)]
    return ms, phys, None


def go_cold(e, a):
    """Restart the server with the pool dump/reload off; optionally drop the OS cache.

    NEVER capture_output on the restart: the service starts a daemon that inherits the
    pipes, and waiting for EOF on them hangs forever. DEVNULL plus a new session keeps it
    detached and bounded."""
    sh(e, "SET GLOBAL innodb_buffer_pool_dump_at_shutdown=OFF;")
    try:
        subprocess.run(a.restart_cmd, shell=True, stdin=subprocess.DEVNULL,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       timeout=300, start_new_session=True)
    except subprocess.TimeoutExpired:
        return False
    if a.drop_caches:
        try:
            subprocess.run("sync; echo 3 > /proc/sys/vm/drop_caches", shell=True,
                           timeout=60)
        except Exception:
            pass
    for _ in range(180):
        rc, _, _ = sh(e, "SELECT 1;")
        if rc == 0:
            return True
        time.sleep(2)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", default="mariadb=mysql")
    ap.add_argument("--reps", type=int, default=7)
    ap.add_argument("--restart-cmd", default="", help="e.g. 'service mariadb restart'; "
                    "without it the cold pass is skipped, and nothing cold is reported")
    ap.add_argument("--drop-caches", action="store_true",
                    help="also flush the OS page cache after each restart (needs root)")
    ap.add_argument("--hours", type=int, default=168)
    ap.add_argument("--users", type=int, default=20)
    ap.add_argument("--datasets-per-user", type=int, dest="dpu", default=5)
    ap.add_argument("--tiers", default="24,72,168")
    a = ap.parse_args()
    a.k = (a.users * a.dpu) // 2
    u = a.k // a.dpu
    tier = u % len(a.tiers.split(","))

    label, _, cmd = a.engine.partition("=")
    e = {"label": label or "db", "cmd": cmd or a.engine}
    rc, o, err = sh(e, "SELECT VERSION();")
    if rc != 0:
        sys.stderr.write(f"cannot reach server: {err.strip().splitlines()[-1][:70]}\n")
        return 1
    print(f"server {o.strip().splitlines()[-1]}   probing dataset k={a.k}\n")

    candidates = [
        ("per dataset", DB, f"arch_d{a.k:04d}", f"side_d{a.k:04d}", False),
        ("per user", DB, f"arch_u{u:03d}", f"side_u{u:03d}", True),
        ("shared", DB, "arch_all", "side_all", True),
        ("per retention", DB, f"arch_t{tier}", f"side_t{tier}", True),
    ]
    found = [(n, sc, at, st, ds) for n, sc, at, st, ds in candidates if exists(e, sc, st)]
    for n, sc, at, st, ds in candidates:
        mark = "found" if (n, sc, at, st, ds) in found else "absent"
        print(f"  {n:<14} {sc}.{st:<12} {mark}")
    if not found:
        print("\nNothing to probe: the benchmark drops each configuration when its cycle")
        print("ends. Rebuild one with bench_topology.py --keep, then rerun this.")
        return 1
    if not a.restart_cmd:
        print("\nno --restart-cmd: warm-only. Cold is a restart; online pool eviction")
        print("does not work on MariaDB 10.6 (shrink refused; scans do not evict).")
    print()

    hdr = (f"  {'config':<14} {'query':<20} {'cold_ms':>9} {'c_phys':>7} "
           f"{'warm_min':>9} {'warm_med':>9} {'warm_max':>9} {'w_phys':>7}")
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    out = [hdr.strip()]
    for name, schema, at, st, ds in found:
        for qname, sql in queries(a, ds, at, st):
            cold_s, cphys = "-", "-"
            if a.restart_cmd:
                if not go_cold(e, a):
                    print("  restart failed; aborting cold pass")
                    return 1
                cms, cph, err = batch(e, schema, sql, 1)
                if err:
                    print(f"  {name:<14} {qname:<20} error: {err}")
                    continue
                cphys = cph[0]
                cold_s = f"{cms[0]:.1f}" if cphys > 0 else f"{cms[0]:.1f}*"
            ms, ph, err = batch(e, schema, sql, a.reps)
            if err:
                print(f"  {name:<14} {qname:<20} error: {err}")
                continue
            row = (f"  {name:<14} {qname:<20} {cold_s:>9} {cphys:>7} "
                   f"{min(ms):>9.1f} {statistics.median(ms):>9.1f} {max(ms):>9.1f} "
                   f"{sum(ph):>7}")
            print(row)
            out.append(row.strip())
        print("  " + "-" * (len(hdr) - 2))
    print("\n  cold_ms marked * had ZERO physical reads: the data came from cache, so it")
    print("  is not a cold number. Without --drop-caches, cold means InnoDB-cold but")
    print("  OS-page-cache-warm (innodb_flush_method=fsync does buffered I/O).")
    path = os.path.join(tempfile.gettempdir(),
                        f"query_probe_{time.strftime('%Y%m%d-%H%M%S')}.txt")
    with open(path, "w") as f:
        f.write("\n".join(out) + "\n")
    print(f"  written to {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
