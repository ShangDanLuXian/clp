#!/usr/bin/env python3
"""Table-scope topologies for multi-tenant metadata, compared on one shared server.

bench_tenancy.py compared only per-dataset tables against one unified table, loaded
tenant-by-tenant under uniform retention. Both of those simplifications flattered the
unified design. This benchmark fixes them and widens the field:

  per_dataset      one table set per dataset, one schema      (today's design)
  per_user         one table set per user, dataset_id in key
  unified          one table set for everyone, dataset_id leading the PK
  tiered           one table set PER RETENTION TIER, dataset_id leading the PK
  schema_per_user  per-dataset tables, but one SCHEMA per user (namespacing only)

Two table KINDS are built for each, because the per-table costs multiply by the number of
kinds a dataset owns: `arch` (one row per archive) and `side` (the posting list, ~45 rows
per archive). Everything is partitioned hourly on begin_timestamp.

What this measures that the earlier one did not:

  P0  inventory   tables, partitions, tablespace FILES, and the bytes an EMPTY tenant
                  costs before holding any data
  P1  build       CONCURRENT INTERLEAVED writes -- W worker processes issuing batched
                  INSERTs for their own tenants at the same time, which is how tenants
                  really ingest, unlike the old sequential tenant-by-tenant bulk load.
  P2  query       per-STEP breakdown (candidate generation / two-predicate intersection /
                  join back for metadata), each with wall time, rows examined, PHYSICAL
                  page reads, and table-cache misses
  P3  retention   HETEROGENEOUS retention. Users are assigned tiers, and the metric that
                  matters is rows still present past their own policy: a time-partitioned
                  unified table cannot drop a partition until the LONGEST-retention tenant
                  in it expires, so short-retention tenants are over-retained.

Physical reads are the cold-cost proxy: `Innodb_buffer_pool_reads` counts pages actually
fetched from storage, so it is deterministic and machine-independent, where forcing a cold
buffer pool is neither (MariaDB 10.6 refuses online shrink, and a scan does not evict).

Usage:
    python3 bench_topology.py --engine mariadb=mysql              # small default
    python3 bench_topology.py --users 20 --datasets-per-user 5 --archives 3000 \\
        --hours 168 --tiers 24,72,168 --workers 8 --engine mariadb=mysql
    python3 bench_topology.py --configs unified,tiered            # subset
"""
import argparse
import os
import shlex
import subprocess
import sys
import tempfile
import time

DB = "topo"
T0 = 1704067200 * 10**9
HOUR = 3600 * 10**9
CONFIGS = ["per_dataset", "per_user", "unified", "tiered", "schema_per_user"]

# (tag, value width, postings per archive, distinct values) -- the c8 shape from bench4.
COLS = [("h0", 12, 1, 1000), ("h1", 12, 1, 1000), ("h2", 12, 1, 1000),
        ("e0", 8, 3, 12), ("e1", 8, 3, 12),
        ("s0", 10, 8, 40), ("s1", 10, 8, 40),
        ("m0", 14, 20, 300)]
POSTINGS = sum(c[2] for c in COLS)

SIDE_COLS = ("column_id TINYINT UNSIGNED NOT NULL, value VARBINARY(64) NOT NULL, "
             "begin_timestamp BIGINT NOT NULL, archive_id INT UNSIGNED NOT NULL")
ARCH_COLS = ("archive_id INT UNSIGNED NOT NULL, begin_timestamp BIGINT NOT NULL, "
             "end_timestamp BIGINT NOT NULL, size_bytes BIGINT NOT NULL")
# Global counters worth diffing around a query. Physical reads stand in for cold cost;
# the table-cache pair is what actually bites the many-table configs.
GLOBALS = ("Innodb_buffer_pool_reads", "Opened_tables", "Table_open_cache_misses")


def sh(e, sql, db=None, timeout=3600):
    cmd = shlex.split(e["cmd"]) + (["--local-infile=1"] if "LOAD DATA" in sql else [])
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
        seen.add(ident)
        e["version"] = ident.rsplit("|", 1)[-1]
        e["flavour"] = "mariadb" if "mariadb" in e["version"].lower() else "mysql"
        out.append(e)
    return out


def parts(hours):
    p = [f"PARTITION p_floor VALUES LESS THAN ({T0})"]
    p += [f"PARTITION p_h{h:04d} VALUES LESS THAN ({T0 + (h + 1) * HOUR})"
          for h in range(hours)]
    p.append("PARTITION p_future VALUES LESS THAN MAXVALUE")
    return "PARTITION BY RANGE (begin_timestamp) (" + ",".join(p) + ")"


def val(tag, width, i):
    return (tag + str(i)).ljust(width, "x")[:width]


# --------------------------------------------------------------------------- topology map
def plan(cfg, k, a):
    """Where dataset k lives under `cfg`.

    Returns (schema, arch_table, side_table, needs_dataset_id). A config whose tables span
    more than one dataset must carry dataset_id in the key; one whose tables are already
    per-dataset must not, because the column would be a constant."""
    u = k // a.dpu
    tier = u % len(a.tiers)
    if cfg == "per_dataset":
        return DB, f"arch_d{k:04d}", f"side_d{k:04d}", False
    if cfg == "per_user":
        return DB, f"arch_u{u:03d}", f"side_u{u:03d}", True
    if cfg == "unified":
        return DB, "arch_all", "side_all", True
    if cfg == "tiered":
        return DB, f"arch_t{tier}", f"side_t{tier}", True
    if cfg == "schema_per_user":
        return f"{DB}_u{u:03d}", f"arch_d{k:04d}", f"side_d{k:04d}", False
    raise ValueError(cfg)


def all_tables(cfg, a):
    """Distinct (schema, arch, side, needs_ds) tuples the config requires."""
    seen, out = set(), []
    for k in range(a.datasets):
        t = plan(cfg, k, a)
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def schemas(cfg, a):
    return sorted({t[0] for t in all_tables(cfg, a)})


def tier_of(cfg, k, a):
    """Retention tier (index into a.tiers) governing dataset k's rows under `cfg`.

    Only `unified` mixes tiers in one table; there a partition survives until the LONGEST
    tier in it expires, which is exactly the over-retention this benchmark measures."""
    return (k // a.dpu) % len(a.tiers)


def create(e, cfg, a):
    for s in schemas(cfg, a):
        sh(e, f"DROP DATABASE IF EXISTS {s}; CREATE DATABASE {s};")
    for schema, arch, side, needs_ds in all_tables(cfg, a):
        ds = "dataset_id SMALLINT UNSIGNED NOT NULL, " if needs_ds else ""
        dsk = "dataset_id, " if needs_ds else ""
        sh(e, f"CREATE TABLE {arch} ({ds}{ARCH_COLS}, "
              f"PRIMARY KEY ({dsk}archive_id, begin_timestamp)) "
              f"ENGINE=InnoDB {parts(a.hours)};", schema)
        sh(e, f"CREATE TABLE {side} ({ds}{SIDE_COLS}, "
              f"PRIMARY KEY ({dsk}column_id, value, begin_timestamp, archive_id)) "
              f"ENGINE=InnoDB {parts(a.hours)};", schema)


# --------------------------------------------------------------------------- inventory
def inventory(e, cfg, a):
    """Tables, partitions, tablespace files and allocated bytes for this config."""
    like = " OR ".join(f"table_schema='{s}'" for s in schemas(cfg, a))
    _, o, _ = sh(e, f"SELECT COUNT(*) FROM information_schema.tables WHERE {like};")
    tabs = int(([x for x in o.split() if x.isdigit()] or [0])[-1])
    _, o, _ = sh(e, f"SELECT COUNT(*) FROM information_schema.partitions "
                    f"WHERE ({like}) AND partition_name IS NOT NULL;")
    prts = int(([x for x in o.split() if x.isdigit()] or [0])[-1])
    view = "INNODB_SYS_TABLESPACES" if e["flavour"] == "mariadb" else "INNODB_TABLESPACES"
    nlike = " OR ".join(f"NAME LIKE '{s}/%'" for s in schemas(cfg, a))
    _, o, _ = sh(e, f"SELECT COALESCE(SUM(FILE_SIZE),0), COUNT(*) FROM "
                    f"information_schema.{view} WHERE {nlike};")
    n = [int(x) for x in o.split() if x.isdigit()]
    fbytes, files = (n[-2], n[-1]) if len(n) >= 2 else (0, 0)
    return tabs, prts, files, fbytes


def data_bytes(e, cfg, a):
    tot = 0
    for s in schemas(cfg, a):
        _, o, _ = sh(e, f"SELECT GROUP_CONCAT(table_name) FROM information_schema.tables "
                        f"WHERE table_schema='{s}';")
        names = (o.strip().splitlines() or [""])[-1]
        if names and names != "NULL":
            sh(e, "ANALYZE TABLE " + ", ".join(names.split(",")) + ";", s)
        fresh = ("SET SESSION information_schema_stats_expiry=0;\n"
                 if e["flavour"] == "mysql" else "")
        _, o, _ = sh(e, fresh + "SELECT COALESCE(SUM(data_length+index_length),0) "
                                f"FROM information_schema.tables WHERE table_schema='{s}';")
        n = [int(x) for x in o.split() if x.isdigit()]
        tot += n[-1] if n else 0
    return tot


# --------------------------------------------------------------------------- build
def rows_for(k, a, rng, pools):
    """Yields (arch_row, [side_rows]) for every archive of dataset k, in timestamp order."""
    span = max(1, a.hours * HOUR // a.archives)
    for i in range(a.archives):
        ts = T0 + i * span + rng.randrange(0, span)
        arch = (i, ts, ts + 10**9, 5_000_000)
        side = [(n, v, ts, i) for n, c in enumerate(COLS)
                for v in rng.sample(pools[c[0]], c[2])]
        yield arch, side


def build_sql(cfg, a, ks, path):
    """Writes one worker's INSERT stream: its datasets, interleaved by timestamp.

    Interleaving is the point. Real tenants seal archives concurrently, so the writes hit
    many key ranges at once; the earlier benchmark filled one tenant's range completely
    before starting the next, which is close to a sorted bulk load and flatters whichever
    design has the fewest trees."""
    import random
    rng = random.Random(7)
    pools = {c[0]: [val(c[0], c[1], i) for i in range(c[3])] for c in COLS}
    streams = []
    for k in ks:
        streams.append((k, list(rows_for(k, a, rng, pools))))
    with open(path, "w") as f:
        f.write("SET autocommit=1;\n")
        for i in range(a.archives):                     # timestamp-major across datasets
            for k, rs in streams:
                arch, side = rs[i]
                schema, at, st, needs_ds = plan(cfg, k, a)
                dsv = f"{k}," if needs_ds else ""
                f.write(f"INSERT INTO {schema}.{at} VALUES "
                        f"({dsv}{arch[0]},{arch[1]},{arch[2]},{arch[3]});\n")
                vals = ",".join(f"({dsv}{n},'{v}',{ts},{aid})" for n, v, ts, aid in side)
                f.write(f"INSERT INTO {schema}.{st} VALUES {vals};\n")
    os.chmod(path, 0o644)


def build(e, cfg, a, tmpdir):
    """Concurrent interleaved load: W workers, each owning a slice of the datasets."""
    files = []
    for w in range(a.workers):
        ks = [k for k in range(a.datasets) if k % a.workers == w]
        if not ks:
            continue
        p = os.path.join(tmpdir, f"{cfg}_w{w}.sql")
        build_sql(cfg, a, ks, p)
        files.append(p)
    t0 = time.time()
    procs = []
    for p in files:
        cmd = shlex.split(e["cmd"])
        procs.append(subprocess.Popen(cmd, stdin=open(p), stdout=subprocess.DEVNULL,
                                      stderr=subprocess.PIPE))
    errs = []
    for pr in procs:
        _, err = pr.communicate()
        if pr.returncode != 0:
            errs.append((err or b"").decode()[:70])
    for p in files:
        os.unlink(p)
    return time.time() - t0, errs


# --------------------------------------------------------------------------- query
def gstat(e):
    _, o, _ = sh(e, "SHOW GLOBAL STATUS WHERE Variable_name IN "
                    "(" + ",".join(f"'{g}'" for g in GLOBALS) + ");")
    d = {}
    for ln in o.splitlines():
        p = ln.split("\t")
        if len(p) == 2 and p[1].strip().isdigit():
            d[p[0]] = int(p[1])
    return d


def measure(e, sql, schema):
    """One query: wall ms, rows examined, physical page reads, table-cache misses.

    Physical reads are the portable cold-cost proxy -- pages fetched from storage rather
    than served by the pool -- so the number does not depend on how warm this machine
    happens to be right now."""
    before = gstat(e)
    body = "FLUSH STATUS;\n" + sql + "\nSHOW SESSION STATUS LIKE 'Handler_read%';"
    t0 = time.time()
    rc, o, err = sh(e, body, schema)
    ms = (time.time() - t0) * 1000
    after = gstat(e)
    if rc != 0:
        return None
    scanned = 0
    for ln in o.splitlines():
        p = ln.split("\t")
        if len(p) == 2 and p[0] in ("Handler_read_next", "Handler_read_rnd_next") \
                and p[1].strip().isdigit():
            scanned += int(p[1])
    sh(e, sql, schema)                                   # warm the statement
    t0 = time.time()
    sh(e, sql, schema)
    warm = (time.time() - t0) * 1000
    return {"ms": ms, "warm": warm, "scanned": scanned,
            "phys": max(0, after.get(GLOBALS[0], 0) - before.get(GLOBALS[0], 0)),
            "opens": max(0, after.get(GLOBALS[1], 0) - before.get(GLOBALS[1], 0)),
            "miss": max(0, after.get(GLOBALS[2], 0) - before.get(GLOBALS[2], 0))}


def steps(e, cfg, a):
    """The three SQL stages of a real filter query, measured separately."""
    k = a.datasets // 2
    schema, at, st, needs_ds = plan(cfg, k, a)
    w1, w2 = T0 + (a.hours - 24) * HOUR, T0 + a.hours * HOUR
    v1, v2 = val("m0", 14, 7), val("s0", 10, 3)

    def tw(al):
        """Both halves of the window must carry the alias -- qualifying only the first
        leaves the second ambiguous the moment the query self-joins (ERROR 1052)."""
        q = f"{al}." if al else ""
        return f"{q}begin_timestamp >= {w1} AND {q}begin_timestamp < {w2}"

    def dsp(al):
        if not needs_ds:
            return ""
        return f"{f'{al}.' if al else ''}dataset_id={k} AND "

    out = {}
    out["S1 candidates"] = measure(
        e, f"SELECT COUNT(DISTINCT archive_id) FROM {st} "
           f"WHERE {dsp('')}column_id=7 AND value='{v1}' AND {tw('')};", schema)
    # The ts-equality rule: bind BOTH key columns or the inner side cannot seek.
    out["S2 two predicates"] = measure(
        e, f"SELECT COUNT(*) FROM {st} x JOIN {st} y "
           f"ON y.archive_id=x.archive_id AND y.begin_timestamp=x.begin_timestamp"
           + (" AND y.dataset_id=x.dataset_id" if needs_ds else "")
           + f" WHERE {dsp('x')}x.column_id=7 AND x.value='{v1}' "
             f"AND y.column_id=5 AND y.value='{v2}' AND {tw('x')};", schema)
    out["S3 join for metadata"] = measure(
        e, f"SELECT COUNT(*), MAX(m.size_bytes) FROM {st} s JOIN {at} m "
           f"ON m.archive_id=s.archive_id AND m.begin_timestamp=s.begin_timestamp"
           + (" AND m.dataset_id=s.dataset_id" if needs_ds else "")
           + f" WHERE {dsp('s')}s.column_id=7 AND s.value='{v1}' AND {tw('s')};", schema)
    # Cross-tenant. Every config gets the same shape: one branch per table, each naming
    # exactly the datasets that live in it. Dropping the dataset_id predicate where the
    # column leads the PK would forfeit the seek and make the comparison unfair.
    br = []
    for s2, a2, t2, nd in all_tables(cfg, a):
        ks = [j for j in range(a.datasets) if plan(cfg, j, a)[:3] == (s2, a2, t2)]
        pred = f"dataset_id IN ({','.join(str(j) for j in ks)}) AND " if nd else ""
        br.append(f"SELECT COUNT(DISTINCT archive_id) c FROM {s2}.{t2} "
                  f"WHERE {pred}column_id=7 AND value='{v1}' AND {tw('')}")
    out["X  all tenants"] = measure(
        e, "SELECT SUM(c) FROM (" + " UNION ALL ".join(br) + ") u;", schemas(cfg, a)[0])
    return out


# --------------------------------------------------------------------------- retention
def retention(e, cfg, a):
    """One expiry cycle under HETEROGENEOUS retention.

    A table holding exactly one tier drops its expired partitions outright. `unified` mixes
    tiers, so a partition survives until the longest tier in it expires -- everything a
    shorter-retention tenant owns in that partition is retained past its own policy, which
    is a compliance breach, not merely wasted space."""
    longest = max(a.tiers)
    stmts = 0
    t0 = time.time()
    for schema, arch, side, _ in all_tables(cfg, a):
        # Which tiers live in this table? unified holds all of them; the rest hold one.
        ks = [k for k in range(a.datasets)
              if plan(cfg, k, a)[:3] == (schema, arch, side)]
        tiers_here = {tier_of(cfg, k, a) for k in ks}
        keep = max(a.tiers[t] for t in tiers_here) if tiers_here else longest
        cutoff = a.hours - keep
        for h in range(max(0, cutoff)):
            for tbl in (arch, side):
                rc, _, _ = sh(e, f"ALTER TABLE {tbl} DROP PARTITION p_h{h:04d};", schema)
                stmts += 1
    el = time.time() - t0
    # Rows still present that this dataset's own policy says should be gone.
    over = 0
    for k in range(a.datasets):
        schema, _, side, needs_ds = plan(cfg, k, a)
        keep = a.tiers[tier_of(cfg, k, a)]
        cut = T0 + (a.hours - keep) * HOUR
        ds = f"dataset_id={k} AND " if needs_ds else ""
        _, o, _ = sh(e, f"SELECT COUNT(*) FROM {side} WHERE {ds}begin_timestamp < {cut};",
                     schema)
        over += int(([x for x in o.split() if x.isdigit()] or [0])[-1])
    return el, stmts, over


# --------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", action="append", default=[])
    ap.add_argument("--users", type=int, default=6)
    ap.add_argument("--datasets-per-user", type=int, dest="dpu", default=3)
    ap.add_argument("--archives", type=int, default=100, help="archives per dataset")
    ap.add_argument("--hours", type=int, default=72, help="hourly partitions per table")
    ap.add_argument("--tiers", default="24,48,72",
                    help="retention hours per tier, assigned to users round-robin")
    ap.add_argument("--workers", type=int, default=4, help="concurrent writer processes")
    ap.add_argument("--configs", default=",".join(CONFIGS))
    ap.add_argument("--tmpdir", default=tempfile.gettempdir())
    ap.add_argument("--out", default="")
    a = ap.parse_args()
    a.tiers = [int(x) for x in a.tiers.split(",")]
    a.datasets = a.users * a.dpu
    cfgs = [c for c in a.configs.split(",") if c in CONFIGS]

    engines = detect(a.engine or ["mariadb=mysql", "mariadb=mariadb"])
    if not engines:
        sys.exit(f"no usable server (needs GRANT ALL ON {DB}%.*)")
    outpath = a.out or f"topology_results_{time.strftime('%Y%m%d-%H%M%S')}.txt"
    out = open(outpath, "w")

    def line(s):
        print(s, flush=True)
        out.write(s + "\n")
        out.flush()

    line(f" TABLE-SCOPE TOPOLOGIES  {time.strftime('%Y-%m-%d %H:%M:%S')}")
    line(f"   {a.users} users x {a.dpu} datasets x {a.archives:,} archives "
         f"= {a.datasets} datasets, {a.datasets * a.archives * POSTINGS:,} postings")
    line(f"   {a.hours} hourly partitions/table, retention tiers {a.tiers} h "
         f"(round-robin by user), {a.workers} concurrent writers")
    for e in engines:
        line(f"   {e['label']:<9} {e['version']}")

    td = tempfile.mkdtemp(dir=a.tmpdir)
    os.chmod(td, 0o755)
    for e in engines:
        res = {}
        for cfg in cfgs:
            sys.stderr.write(f"  [{e['label']}] {cfg}: create\n")
            create(e, cfg, a)
            tabs, prts, files, empty = inventory(e, cfg, a)
            sys.stderr.write(f"  [{e['label']}] {cfg}: load\n")
            secs, errs = build(e, cfg, a, td)
            _, _, files2, fbytes = inventory(e, cfg, a)
            res[cfg] = {"tabs": tabs, "prts": prts, "files": files, "empty": empty,
                        "load": secs, "errs": errs, "fbytes": fbytes,
                        "dbytes": data_bytes(e, cfg, a)}
            sys.stderr.write(f"  [{e['label']}] {cfg}: query\n")
            res[cfg]["steps"] = steps(e, cfg, a)
            sys.stderr.write(f"  [{e['label']}] {cfg}: retention\n")
            res[cfg]["ret"] = retention(e, cfg, a)
            for s in schemas(cfg, a):
                sh(e, f"DROP DATABASE IF EXISTS {s};")

        line("")
        line("=" * 98)
        line(f" P0/P1  inventory, build and size  [{e['label']}]")
        line("=" * 98)
        line(f"  {'config':<17}{'tables':>8}{'parts':>8}{'files':>8}{'empty_MB':>10}"
             f"{'load_s':>9}{'data_MB':>10}{'file_MB':>10}")
        line("  " + "-" * 80)
        for cfg in cfgs:
            r = res[cfg]
            line(f"  {cfg:<17}{r['tabs']:>8,}{r['prts']:>8,}{r['files']:>8,}"
                 f"{r['empty'] / 1048576:>10,.1f}{r['load']:>9,.1f}"
                 f"{r['dbytes'] / 1048576:>10,.1f}{r['fbytes'] / 1048576:>10,.1f}")
            if r["errs"]:
                line(f"    !! load errors: {r['errs'][0]}")
        line("  (empty_MB is measured after CREATE and before any row exists: the price of")
        line("   simply having the tables. load is CONCURRENT and interleaved across")
        line("   tenants -- W workers writing their own datasets at the same time.)")

        line("")
        line("=" * 98)
        line(f" P2  query, per step  [{e['label']}]")
        line("=" * 98)
        line(f"  {'config':<17}{'step':<22}{'ms':>8}{'warm':>8}{'scanned':>10}"
             f"{'phys_rd':>9}{'tbl_miss':>10}")
        line("  " + "-" * 84)
        for cfg in cfgs:
            for name, m in res[cfg]["steps"].items():
                if m is None:
                    line(f"  {cfg:<17}{name:<22}{'query failed':>8}")
                    continue
                line(f"  {cfg:<17}{name:<22}{m['ms']:>8,.1f}{m['warm']:>8,.1f}"
                     f"{m['scanned']:>10,}{m['phys']:>9,}{m['miss']:>10,}")
            line("  " + "-" * 84)
        line("  (phys_rd = pages fetched from storage, the portable cold-cost proxy.")
        line("   tbl_miss = table-cache misses, which is what many-table configs pay.)")

        line("")
        line("=" * 98)
        line(f" P3  retention with HETEROGENEOUS tiers  [{e['label']}]")
        line("=" * 98)
        line(f"  {'config':<17}{'expiry_s':>10}{'statements':>12}"
             f"{'rows past policy':>19}")
        line("  " + "-" * 60)
        for cfg in cfgs:
            el, stmts, over = res[cfg]["ret"]
            line(f"  {cfg:<17}{el:>10,.2f}{stmts:>12,}{over:>19,}")
        line("  (rows past policy = rows still present that the owning dataset's own")
        line("   retention says should be gone. A table mixing tiers cannot drop a")
        line("   partition until its LONGEST tier expires, so shorter tenants over-retain.")
        line("   Non-zero here is a compliance problem, not a storage inefficiency.)")

    import shutil
    shutil.rmtree(td, ignore_errors=True)
    line(f"\nwritten to {outpath}")
    out.close()


if __name__ == "__main__":
    main()
