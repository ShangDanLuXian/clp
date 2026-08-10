#!/usr/bin/env python3
"""Benchmarks encodings for the low-cardinality (multi-value) archive filter column.

One archive row stores the SET of distinct values a filter column takes anywhere in that
archive, so the planner can ask "does this archive contain value v?". This script compares
the candidate encodings for that set, on MariaDB and MySQL, and prints a numbers summary.

Three things are measured, because they pull in different directions:
  * storage, both absolute and per archive;
  * query latency for an exact value AND for a prefix wildcard (`service: web-*`), which
    hash-based encodings cannot answer at all;
  * sustained per-archive INSERT throughput, which is how the ingest path actually writes.
    Bulk LOAD DATA is the friendliest possible write pattern and hides the cost of random
    key order, so it is reported but is not the write metric that matters.

Run with no arguments to auto-detect local servers:
    python3 bench_lc.py
Point it at specific servers (repeatable; the label is free-form):
    python3 bench_lc.py --engine mariadb="mysql -u root" \
                        --engine mysql="mysql -u root -h 127.0.0.1 -P 3307 --protocol=TCP"
"""

import argparse
import os
import shlex
import subprocess
import sys
import tempfile
import time
import zlib

DB = "lcbench"
SEP = chr(31)  # ASCII unit separator

# (values per archive, value width, default row count). Both profiles come from the real
# per-archive distinct-value counts measured on the mongodb dataset (1,037 archives).
# Row counts default to the ~100K candidate set a time-filtered query hands to this filter.
PROFILES = {
    "id-like": (125, 7, 100_000),  # 125 x 7 chars -> 1,001 B payload: grazes VARCHAR(1024)
    "msg-like": (115, 60, 25_000),  # 115 x 60 chars -> ~7 KB payload: far over VARCHAR(1024)
}

VARIANTS = ["varchar_delim", "text_delim", "text_hash", "json", "json_mvi",
            "side_table", "side_table_raw"]

DEFAULT_ENGINES = [
    # Portable candidates. "auto" labels are replaced by the detected flavour. Anything not
    # reachable here is reported, not skipped silently -- pass --engine for custom setups.
    "auto=mysql",  # whatever the default socket serves
    "auto=mariadb",  # MariaDB 11+ ships the client under this name
    "auto=mysql -h 127.0.0.1 -P 3306 --protocol=TCP",
    "auto=mysql -h 127.0.0.1 -P 3307 --protocol=TCP",  # second engine, per README setup
    "auto=/opt/mysql8/usr/bin/mysql --defaults-file=/etc/my8.cnf -u root",  # README's prefix
]


def sh(cmd, sql, db=None, local_infile=False, timeout=3600):
    """Runs SQL through a mysql-family client, returning (returncode, stdout, stderr)."""
    argv = list(cmd)
    if local_infile:
        argv.append("--local-infile=1")
    argv += ["-N", "-B"]
    if db:
        argv.append(db)
    p = subprocess.run(argv, input=sql, capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


def detect(specs):
    """Probes each label=command spec. Returns (usable engines, [(command, why it failed)])."""
    found, failed, seen_flavours = [], [], set()
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
            why = err.strip().splitlines()[-1] if err.strip() else "no response"
            failed.append((cmdline, why[:70]))
            continue
        version = out.strip().splitlines()[0]
        flavour = "mariadb" if "mariadb" in version.lower() else "mysql"
        if flavour in seen_flavours:
            failed.append((cmdline, f"duplicate: another command already serves {flavour}"))
            continue  # first working command per flavour wins
        seen_flavours.add(flavour)
        e = {"label": flavour if label == "auto" else label,
             "cmd": cmd, "version": version, "flavour": flavour}
        # Probe every privilege the run needs BEFORE generating hundreds of MB of data.
        # FLUSH STATUS needs the global RELOAD privilege, which a database-scoped grant
        # does not cover, so check it explicitly rather than failing per-variant later.
        rc, _, err = sh(cmd, f"CREATE DATABASE IF NOT EXISTS {DB};")
        if rc == 0:
            rc, _, err = sh(cmd, "CREATE TABLE __probe (i INT); FLUSH STATUS; "
                                 "DROP TABLE __probe;", DB)
        e["error"] = err.strip().splitlines()[-1] if rc != 0 else None
        found.append(e)
    return found, failed


def digest(value):
    """Fixed-width, restricted-alphabet stand-in for a raw value (delimiter-safe by design)."""
    return f"{zlib.crc32(value.encode()) & 0xFFFFFFFF:08x}"


def u32(value):
    return zlib.crc32(value.encode()) & 0xFFFFFFFF


def generate(profile, rows, seed, outdir, insert_batch):
    """Writes one TSV per storage shape. Returns (paths, needles, payload_bytes, batch)."""
    import random

    n_vals, width, _ = PROFILES[profile]
    rng = random.Random(seed)
    pool_size = max(n_vals * 100, 1000)
    pool = [(str(i) + "x" * width)[:width] for i in range(pool_size)]  # fixed-width, distinct
    hot = pool[0]  # injected into ~20% of archives to give a "common value" probe
    rare = pool[pool_size // 2]
    # Most selective prefix that still spans several distinct values, so the wildcard probe
    # exercises a real index range rather than degenerating into a single point lookup.
    prefix = rare[:1]
    for k in range(width, 0, -1):
        if sum(1 for v in pool if v.startswith(rare[:k])) >= 5:
            prefix = rare[:k]
            break

    def row_values(i):
        vals = rng.sample(pool, n_vals)
        if rng.random() < 0.20 and hot not in vals:
            vals[0] = hot
        if rare in vals and rng.random() < 0.5:
            vals[vals.index(rare)] = pool[1]  # thin the rare value toward ~1%
        return vals

    paths = {k: os.path.join(outdir, f"{k}.tsv")
             for k in ("delim", "hash", "json", "side", "side_raw")}
    payload = 0
    with open(paths["delim"], "w") as fd, open(paths["hash"], "w") as fh, \
         open(paths["json"], "w") as fj, open(paths["side"], "w") as fs, \
         open(paths["side_raw"], "w") as fr:
        for i in range(rows):
            vals = row_values(i)
            line = SEP + SEP.join(vals) + SEP
            payload = max(payload, len(line))
            fd.write(f"{i}\t{line}\n")
            fh.write(f"{i}\t" + SEP + SEP.join(digest(v) for v in vals) + SEP + "\n")
            fj.write(f"{i}\t[" + ",".join(f'"{v}"' for v in vals) + "]\n")
            for v in vals:
                fs.write(f"{i}\t{u32(v)}\n")
                fr.write(f"{i}\t{v}\n")
    # Archives appended afterwards to measure sustained per-archive insert throughput.
    batch = [(rows + j, row_values(rows + j)) for j in range(insert_batch)]
    return paths, {"rare": rare, "common": hot, "prefix": prefix}, payload, batch


# VARCHAR(255) on the raw side table stands in for the prefix index a production deployment
# would need: InnoDB caps index keys at 3072 bytes, so unbounded values cannot be keyed whole.
DDL = {
    "varchar_delim": "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(1024) NULL) "
                     "ENGINE=InnoDB DEFAULT CHARSET=ascii",
    "text_delim": "CREATE TABLE t (id INT PRIMARY KEY, v TEXT NULL) "
                  "ENGINE=InnoDB DEFAULT CHARSET=ascii",
    "text_hash": "CREATE TABLE t (id INT PRIMARY KEY, v TEXT NULL) "
                 "ENGINE=InnoDB DEFAULT CHARSET=ascii",
    "json": "CREATE TABLE t (id INT PRIMARY KEY, v JSON NULL) ENGINE=InnoDB",
    "json_mvi": "CREATE TABLE t (id INT PRIMARY KEY, v JSON NULL) ENGINE=InnoDB",
    "side_table": "CREATE TABLE t (archive_id INT UNSIGNED NOT NULL, "
                  "val_hash INT UNSIGNED NOT NULL, PRIMARY KEY (val_hash, archive_id)) "
                  "ENGINE=InnoDB",
    "side_table_raw": "CREATE TABLE t (archive_id INT UNSIGNED NOT NULL, "
                      "value VARCHAR(255) NOT NULL, PRIMARY KEY (value, archive_id)) "
                      "ENGINE=InnoDB DEFAULT CHARSET=ascii",
}
SOURCE = {"varchar_delim": "delim", "text_delim": "delim", "text_hash": "hash",
          "json": "json", "json_mvi": "json", "side_table": "side",
          "side_table_raw": "side_raw"}
COLUMNS = {"side_table": "(archive_id,val_hash)", "side_table_raw": "(archive_id,value)"}
IS_SIDE = ("side_table", "side_table_raw")
IS_HASHED = ("text_hash", "side_table")  # cannot express a prefix wildcard, by construction


def predicate(variant, literal, kind):
    """SQL for 'which archives contain this value' (kind='exact') or 'contain a value with
    this prefix' (kind='prefix'). Returns None where the encoding cannot express the query."""
    sep = "CHAR(31 USING ascii)"
    if kind == "prefix":
        if variant in IS_HASHED:
            return None  # hashing is not order- or prefix-preserving
        if variant in ("varchar_delim", "text_delim"):
            return f"SELECT COUNT(*) FROM t WHERE v LIKE CONCAT('%',{sep},'{literal}','%')"
        if variant in ("json", "json_mvi"):
            # JSON_SEARCH takes LIKE patterns; a multi-valued index cannot serve it.
            return f"SELECT COUNT(*) FROM t WHERE JSON_SEARCH(v,'one','{literal}%') IS NOT NULL"
        return f"SELECT COUNT(DISTINCT archive_id) FROM t WHERE value LIKE '{literal}%'"
    if variant in ("varchar_delim", "text_delim"):
        return (f"SELECT COUNT(*) FROM t WHERE v LIKE CONCAT('%',{sep},'{literal}',{sep},'%')")
    if variant == "text_hash":
        return (f"SELECT COUNT(*) FROM t WHERE v LIKE "
                f"CONCAT('%',{sep},'{digest(literal)}',{sep},'%')")
    if variant == "json":
        return f"SELECT COUNT(*) FROM t WHERE JSON_CONTAINS(v, '\"{literal}\"')"
    if variant == "json_mvi":
        return f"SELECT COUNT(*) FROM t WHERE '{literal}' MEMBER OF (v)"
    if variant == "side_table":
        return f"SELECT COUNT(DISTINCT archive_id) FROM t WHERE val_hash = {u32(literal)}"
    return f"SELECT COUNT(DISTINCT archive_id) FROM t WHERE value = '{literal}'"


def insert_sql(variant, aid, vals):
    """One archive's worth of rows, as the ingest path would write it."""
    if variant in ("varchar_delim", "text_delim"):
        return f"INSERT INTO t (id,v) VALUES ({aid},'{SEP}{SEP.join(vals)}{SEP}');"
    if variant == "text_hash":
        body = SEP + SEP.join(digest(v) for v in vals) + SEP
        return f"INSERT INTO t (id,v) VALUES ({aid},'{body}');"
    if variant in ("json", "json_mvi"):
        return f"INSERT INTO t (id,v) VALUES ({aid},'[" + ",".join(f'\"{v}\"' for v in vals) \
               + "]');"
    if variant == "side_table":
        return "INSERT INTO t (archive_id,val_hash) VALUES " \
               + ",".join(f"({aid},{u32(v)})" for v in vals) + ";"
    return "INSERT INTO t (archive_id,value) VALUES " \
           + ",".join(f"({aid},'{v}')" for v in vals) + ";"


def timed(engine, sql):
    """Runs sql twice; returns (warm_ms, rows_scanned, matched, error) with server-side timing."""
    script = (
        sql + ";\n"                      # warm-up pass, not counted
        "FLUSH STATUS;\n"
        "SET @t=NOW(6);\n" + sql + ";\n"
        "SELECT CONCAT('MS=',TIMESTAMPDIFF(MICROSECOND,@t,NOW(6))/1000);\n"
        "SHOW SESSION STATUS WHERE Variable_name IN "
        "('Handler_read_next','Handler_read_rnd_next','Handler_read_key');\n"
    )
    rc, out, err = sh(engine["cmd"], script, DB)
    if rc != 0:
        return None, None, None, err.strip().splitlines()[-1] if err.strip() else "error"
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


def insert_rate(engine, variant, batch):
    """Archives inserted per second, one transaction per archive (the real ingest shape)."""
    parts = ["SET @t=NOW(6);"]
    for aid, vals in batch:
        parts += ["BEGIN;", insert_sql(variant, aid, vals), "COMMIT;"]
    parts.append("SELECT CONCAT('MS=',TIMESTAMPDIFF(MICROSECOND,@t,NOW(6))/1000);")
    rc, out, _ = sh(engine["cmd"], "\n".join(parts), DB)
    if rc != 0:
        return None
    for line in out.splitlines():
        if line.startswith("MS="):
            ms = float(line[3:])
            return round(len(batch) / (ms / 1000.0), 1) if ms > 0 else None
    return None


def index_used(engine, sql):
    """Reads the chosen index out of EXPLAIN FORMAT=JSON (portable across both engines)."""
    rc, out, _ = sh(engine["cmd"], f"EXPLAIN FORMAT=JSON {sql};", DB)
    if rc != 0:
        return "?"
    for token in out.replace(",", "\n").splitlines():
        if '"key"' in token:
            val = token.split(":", 1)[1].strip().strip('" ')
            return "-" if val in ("null", "") else val
    return "-"


def bench(engine, variant, paths, needles, rows, batch):
    r = {"engine": engine["label"], "variant": variant, "status": "ok"}
    sh(engine["cmd"], f"DROP TABLE IF EXISTS t; {DDL[variant]};", DB)
    if variant == "json_mvi":
        rc, _, err = sh(engine["cmd"],
                        "ALTER TABLE t ADD INDEX mvi ((CAST(v AS CHAR(64) ARRAY)));", DB)
        if rc != 0:
            r["status"] = "UNSUPPORTED"
            r["note"] = (err.strip().splitlines()[-1][:60] if err.strip() else "rejected")
            sh(engine["cmd"], "DROP TABLE IF EXISTS t;", DB)
            return r
    src = paths[SOURCE[variant]]
    cols = COLUMNS.get(variant, "(id,v)")
    t0 = time.time()
    rc, _, err = sh(engine["cmd"],
                    f"LOAD DATA LOCAL INFILE '{src}' INTO TABLE t {cols};", DB,
                    local_infile=True)
    r["load_s"] = round(time.time() - t0, 2)
    if rc != 0:
        r["status"] = "LOAD FAILED"
        r["note"] = err.strip().splitlines()[-1][:60] if err.strip() else "error"
        sh(engine["cmd"], "DROP TABLE IF EXISTS t;", DB)
        return r
    # InnoDB serves information_schema sizes from cached stats; refresh them or sizes read 0.
    sh(engine["cmd"], "ANALYZE TABLE t;", DB)
    fresh = ("SET SESSION information_schema_stats_expiry=0;\n"
             if engine["flavour"] == "mysql" else "")
    _, out, _ = sh(engine["cmd"], fresh +
                   "SELECT ROUND((data_length+index_length)/1048576,1), "
                   "data_length+index_length FROM information_schema.tables "
                   f"WHERE table_schema='{DB}' AND table_name='t';", DB)
    try:
        r["store_mb"], r["b_row"] = float(out.split()[0]), round(int(out.split()[1]) / rows)
    except (ValueError, IndexError):
        r["store_mb"], r["b_row"] = 0.0, 0
    # Truncation check: VARCHAR(1024) silently clips oversized payloads on non-strict loads.
    if variant == "varchar_delim":
        _, out, _ = sh(engine["cmd"], "SELECT COUNT(*) FROM t WHERE LENGTH(v)>=1024;", DB)
        clipped = int(out.strip() or 0)
        if clipped:
            r["status"] = f"TRUNCATED({clipped})"
    for label, key, kind in (("rare", "rare", "exact"), ("common", "common", "exact"),
                             ("pfx", "prefix", "prefix")):
        sql = predicate(variant, needles[key], kind)
        if sql is None:
            r[f"{label}_ms"] = r[f"{label}_scan"] = r[f"{label}_hits"] = None
            r[f"{label}_na"] = True
            continue
        ms, scanned, matched, err = timed(engine, sql)
        r[f"{label}_ms"], r[f"{label}_scan"], r[f"{label}_hits"] = ms, scanned, matched
        if err:
            r["status"], r["note"] = "QUERY FAILED", err[:60]
    exact = predicate(variant, needles["rare"], "exact")
    r["index"] = index_used(engine, exact)
    pfx = predicate(variant, needles["prefix"], "prefix")
    r["pfx_index"] = index_used(engine, pfx) if pfx else "n/a"
    r["ins_arch_s"] = insert_rate(engine, variant, batch)
    sh(engine["cmd"], "DROP TABLE IF EXISTS t;", DB)
    return r


def fmt(rows_out, profile, rows, n_vals, width, payload, needles, batch_n, out):
    def p(s):
        print(s)
        out.write(s + "\n")

    def num(r, k):
        v = r.get(k)
        if r.get(k.split("_")[0] + "_na"):
            return "n/a"
        if isinstance(v, float):
            return f"{v:,.1f}"
        if isinstance(v, int):
            return f"{v:,}"
        return "-"

    p("")
    p(f"PROFILE {profile}: archives={rows:,}  values/archive={n_vals}  value_width={width}"
      f"  delimited_payload={payload}B")
    p(f"  probes: exact rare='{needles['rare']}'  exact common='{needles['common']}'"
      f"  prefix='{needles['prefix']}*'   insert batch={batch_n:,} archives")

    p("")
    p("  STORAGE AND WRITE")
    p("  " + "-" * 84)
    p(f"  {'engine':<9}{'variant':<16}{'store_MB':>10}{'B/archive':>11}{'bulk_s':>9}"
      f"{'insert_arch/s':>15}  {'status':<12}")
    p("  " + "-" * 84)
    for r in rows_out:
        if r.get("store_mb") is None:
            continue
        p(f"  {r['engine']:<9}{r['variant']:<16}{num(r,'store_mb'):>10}{num(r,'b_row'):>11}"
          f"{num(r,'load_s'):>9}{num(r,'ins_arch_s'):>15}  {r['status']:<12}")

    p("")
    p("  QUERY (warm ms; hits = matching archives, which must agree across variants)")
    p("  " + "-" * 112)
    p(f"  {'engine':<9}{'variant':<16}{'exact_ms':>10}{'hits':>8}{'scanned':>10}{'idx':>9}"
      f"{'common_ms':>11}{'prefix_ms':>10}{'hits':>8}{'scanned':>10}{'idx':>9}")
    p("  " + "-" * 112)
    for r in rows_out:
        if r.get("store_mb") is None:
            continue
        p(f"  {r['engine']:<9}{r['variant']:<16}{num(r,'rare_ms'):>10}{num(r,'rare_hits'):>8}"
          f"{num(r,'rare_scan'):>10}{str(r.get('index','-'))[:8]:>9}"
          f"{num(r,'common_ms'):>11}{num(r,'pfx_ms'):>10}{num(r,'pfx_hits'):>8}"
          f"{num(r,'pfx_scan'):>10}{str(r.get('pfx_index','-'))[:8]:>9}")

    notes = [r for r in rows_out if r.get("note")]
    if notes:
        p("")
        for r in notes:
            p(f"  ! {r['engine']}/{r['variant']}: {r['note']}")
    na = [r for r in rows_out if r.get("pfx_na")]
    if na:
        p("")
        p("  n/a = encoding cannot express a prefix wildcard (hashing is not "
          "order-preserving):")
        p("      " + ", ".join(f"{r['engine']}/{r['variant']}" for r in na))

    # Every variant must agree on hit counts. A lower count means the encoding lost data and
    # the filter is silently returning false negatives, which is a correctness bug.
    for label, what in (("rare", "exact"), ("pfx", "prefix")):
        vals = [r for r in rows_out if isinstance(r.get(f"{label}_hits"), int)]
        if not vals:
            continue
        truth = max(r[f"{label}_hits"] for r in vals)
        wrong = [r for r in vals if r[f"{label}_hits"] != truth]
        if wrong:
            p("")
            p(f"  *** FALSE NEGATIVES ({what}): expected {truth} matching archives;"
              f" these returned fewer:")
            for r in wrong:
                p(f"      {r['engine']:<9}{r['variant']:<16}{r[f'{label}_hits']:>8} "
                  f"({truth - r[f'{label}_hits']} missed)")

    for label, what in (("rare", "exact"), ("pfx", "prefix")):
        base = {r["engine"]: r[f"{label}_ms"] for r in rows_out
                if r["variant"] == "text_delim" and isinstance(r.get(f"{label}_ms"), float)}
        if not base:
            continue
        p("")
        p(f"  {what} lookup, relative to text_delim on the same engine (lower is better):")
        for r in rows_out:
            if isinstance(r.get(f"{label}_ms"), float) and r["engine"] in base:
                p(f"    {r['engine']:<9}{r['variant']:<16}"
                  f"{r[f'{label}_ms']/base[r['engine']]:>8.2f}x")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", action="append", default=[], metavar="LABEL=COMMAND")
    ap.add_argument("--profile", action="append", default=[], choices=list(PROFILES))
    ap.add_argument("--rows", type=int, default=0, help="override row count for all profiles")
    ap.add_argument("--insert-batch", type=int, default=500,
                    help="archives inserted one-transaction-each to measure write throughput")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default="lc_bench_results.txt")
    # Generated TSVs are large (hundreds of MB); keep them out of the working tree.
    ap.add_argument("--tmpdir", default=tempfile.gettempdir(),
                    help="where to stage generated data (default: system temp)")
    a = ap.parse_args()

    engines, unreachable = detect(a.engine or DEFAULT_ENGINES)
    if not engines:
        for cmdline, why in unreachable:
            sys.stderr.write(f"  tried: {cmdline}\n         -> {why}\n")
        sys.exit("No reachable server. Pass --engine LABEL='mysql -u root ...'")
    broken = [e for e in engines if e["error"]]
    engines = [e for e in engines if not e["error"]]
    for e in broken:
        sys.stderr.write(f"SKIPPING {e['label']} ({e['version']}): {e['error']}\n")
    if broken and not engines:
        user = os.environ.get("USER", "<you>")
        sys.exit(
            "\nNo usable server, so nothing was run. The benchmark needs both a\n"
            f"database-scoped grant and the global RELOAD privilege (for FLUSH STATUS):\n\n"
            f"  sudo mysql -e \"GRANT ALL PRIVILEGES ON {DB}.* TO '{user}'@'localhost';\n"
            f"                 GRANT RELOAD ON *.* TO '{user}'@'localhost';\"\n")
    profiles = a.profile or list(PROFILES)

    out = open(a.out, "w")

    def line(s):
        print(s)
        out.write(s + "\n")

    line("=" * 104)
    line(" LOW-CARDINALITY FILTER COLUMN - ENCODING BENCHMARK")
    line(" " + time.strftime("%Y-%m-%d %H:%M:%S"))
    for e in engines:
        line(f"   {e['label']:<9} {e['version']}")
    # Say why an engine is absent. Silence here reads as "MySQL has no results" rather than
    # "MySQL was never reached", which is the more useful thing to know.
    for cmdline, why in unreachable:
        if not why.startswith("duplicate"):
            line(f"   {'(none)':<9} not reached via `{cmdline}`: {why}")
    missing = {"mariadb", "mysql"} - {e["flavour"] for e in engines}
    for m in sorted(missing):
        line(f"   NOTE: no {m} server was reached, so these results cover only "
             f"{'/'.join(sorted(e['flavour'] for e in engines))}.")
        line("         The two engines diverge sharply on this workload -- see README for")
        line("         running both side by side, or pass --engine to point at yours.")
    line("=" * 104)

    for prof in profiles:
        n_vals, width, default_rows = PROFILES[prof]
        rows = a.rows or default_rows
        with tempfile.TemporaryDirectory(dir=a.tmpdir) as td:
            paths, needles, payload, batch = generate(prof, rows, a.seed, td, a.insert_batch)
            os.chmod(td, 0o755)
            for p in paths.values():
                os.chmod(p, 0o644)
            results = []
            for e in engines:
                sh(e["cmd"], f"CREATE DATABASE IF NOT EXISTS {DB};")
                for v in VARIANTS:
                    sys.stderr.write(f"  [{prof}] {e['label']}/{v} ...\n")
                    results.append(bench(e, v, paths, needles, rows, batch))
            fmt(results, prof, rows, n_vals, width, payload, needles, len(batch), out)

    for e in engines:
        sh(e["cmd"], f"DROP DATABASE IF EXISTS {DB};")
    line("")
    line(f"written to {a.out}")
    out.close()


if __name__ == "__main__":
    main()
