#!/usr/bin/env python3
"""Round 3: full-shape benchmark of low-cardinality filter designs, at 31-archives/s scale.

The corpus models the mongodb archive-analyzer report: 8 filter columns whose per-archive
distinct counts follow the measured shape (~2,500 side rows per archive), timestamps at a
configurable arrival rate over a configurable history, hourly partitions for side tables.

Designs:  inline      8 delimited TEXT columns in the archives table (baseline)
          side_shared one side table, PK (column_id, value, begin_timestamp, archive_id)
          side_percol one side table per column, PK (value, begin_timestamp, archive_id)
          inline_json one JSON document per archive holding all 8 columns
                      (+ per-path multi-valued-index attempts on MySQL, failures recorded)

Subcommands:
  build   drop+recreate the lc3 database, generate the corpus, load every design, measure
          build rate, sizes (steady-state, i.e. NOT compacted), config-change DDL cost, GC;
          write lc3_manifest.json so later query runs know what exists.
  query   run the query matrix, write-path tests, and interference test against the
          EXISTING database. Re-runnable; supports --design/--query/--window filters.
  all     build then query.

Metrics: size (data/index split, B/archive, % of compressed data at 50x), build rate,
per-archive insert rate at 1/10 archives per txn and 4 connections, physical bytes written
per archive (Innodb_data_written + os_log delta), query cold+warm ms / hits / rows scanned,
pruning power vs the time-only baseline, correctness vs generator ground truth (when
computed) and across designs, GC drop-vs-delete, add-a-column DDL cost per design.
"""

import argparse
import json
import os
import shlex
import subprocess
import sys
import tempfile
import time

DB = "lc3"
SEP = chr(31)
T0 = 1704067200 * 10**9
HOUR = 3600 * 10**9
DAY = 24 * HOUR
MANIFEST = "lc3_manifest.json"
RAW_BYTES_PER_ARCHIVE = 256 * 1024 * 1024
COMPRESSION = 50

# (name, tag, width, distinct/archive, pool size) -- shape from the mongodb report's
# per-archive distinct classes: constants, 2-8, 9-64, 65-512, 513-4K.
ALL_COLUMNS = [
    ("host",     "h1", 12, 1,    1000),
    ("flag1",    "f1", 6,  3,    8),
    ("flag2",    "f2", 6,  3,    8),
    ("module1",  "m1", 12, 20,   200),
    ("module2",  "m2", 12, 20,   200),
    ("entity1",  "e1", 20, 326,  30000),
    ("entity2",  "e2", 20, 326,  30000),
    ("session1", "s1", 30, 1800, 500000),
]
# How permissive the index configuration is. Storage is dominated by the highest-distinct
# column configured -- session1 alone is ~77% of the "full" tier's bytes -- so the tier is a
# parameter, not a constant: the point is the cost curve, not any single configuration.
TIERS = {
    "lean":     ["host", "flag1", "flag2", "module1", "module2"],
    "standard": ["host", "flag1", "flag2", "module1", "module2", "entity1", "entity2"],
    "full":     [c[0] for c in ALL_COLUMNS],
}
COLUMNS = [c for c in ALL_COLUMNS if c[0] in TIERS["standard"]]
DESIGNS = ["inline", "side_shared", "side_percol", "inline_json"]


def set_tier(tier):
    global COLUMNS
    COLUMNS = [c for c in ALL_COLUMNS if c[0] in TIERS[tier]]
    return COLUMNS


def val(tag, width, i):
    # Digit count adapts to the column width so short values stay distinct: a fixed 6-digit
    # format truncated to width 6 would collapse an entire pool onto one string.
    digits = min(6, width - len(tag))
    return (tag + format(i, f"0{digits}d") + "x" * width)[:width]


for _c in ALL_COLUMNS:
    assert _c[4] <= 10 ** min(6, _c[2] - len(_c[1])), f"pool too large for width: {_c[0]}"


def make_probes():
    """The query matrix, restricted to columns the active tier configures as filters.
    Selectivities span hot (a flag value, ~37% of archives) to rare (a session value)."""
    active = {c[0] for c in COLUMNS}
    eq = {c[0]: val(c[1], c[2], 7 % c[4]) for c in COLUMNS}
    eq.setdefault("entity1", val("e1", 20, 500))
    if "entity1" in active:
        eq["entity1"] = val("e1", 20, 500)
    if "session1" in active:
        eq["session1"] = val("s1", 30, 123456)
    narrow = "e100001"              # entity1 values 10..19 -> 10 pool values
    broad = "s11234"                # session1 values 123400..123499 -> 100 pool values
    candidates = {
        "Q1_host":    [("host", "eq", eq.get("host"))],
        "Q1_flag":    [("flag1", "eq", eq.get("flag1"))],
        "Q1_module":  [("module1", "eq", eq.get("module1"))],
        "Q1_entity":  [("entity1", "eq", eq.get("entity1"))],
        "Q1_session": [("session1", "eq", eq.get("session1"))],
        "Q2_narrow":  [("entity1", "pfx", narrow)],
        "Q2_broad":   [("session1", "pfx", broad)],
        "Q3_sel_sel": [("entity1", "eq", eq.get("entity1")),
                       ("session1", "eq", eq.get("session1"))],
        "Q3_sel_hot": [("entity1", "eq", eq.get("entity1")),
                       ("flag1", "eq", eq.get("flag1"))],
        "Q3_hot_hot": [("flag1", "eq", eq.get("flag1")),
                       ("flag2", "eq", val("f2", 6, 3))],
        "Q4_hot_pfx": [("flag1", "eq", eq.get("flag1")), ("entity1", "pfx", narrow)],
    }
    # A lean tier has no entity/session columns, so those probes simply do not exist.
    return {q: p for q, p in candidates.items() if all(c in active for c, _, _ in p)}


def tier_cost(columns):
    """Payload bytes and value count per archive, before storage overhead."""
    rows = sum(c[3] for c in columns)
    payload = sum(c[3] * (c[2] + 1) for c in columns) + len(columns)
    return rows, payload


def windows(days):
    a = T0 + (days - 7) * DAY        # 7d window ends 1 day before history ends
    return {"1h": (a + 6 * DAY + 12 * HOUR, a + 6 * DAY + 13 * HOUR),
            "1d": (a + 6 * DAY, a + 7 * DAY),
            "7d": (a, a + 7 * DAY)}


# ----------------------------------------------------------------------------- sql plumbing
def sh(cmd, sql, db=None, local_infile=False, timeout=14400):
    argv = list(cmd) + (["--local-infile=1"] if local_infile else []) + ["-N", "-B"]
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
            failed.append((cmdline, (err.strip().splitlines() or ["no response"])[-1][:70]))
            continue
        version = out.strip().splitlines()[0]
        flavour = "mariadb" if "mariadb" in version.lower() else "mysql"
        if flavour in seen:
            continue
        seen.add(flavour)
        rc, _, err = sh(cmd, f"CREATE DATABASE IF NOT EXISTS {DB};")
        if rc == 0:
            rc, _, err = sh(cmd, "CREATE TABLE IF NOT EXISTS __p (i INT); FLUSH STATUS; "
                                 "DROP TABLE __p;", DB)
        if rc != 0:
            failed.append((cmdline, (err.strip().splitlines() or ["?"])[-1][:70]))
            continue
        redo = "@@innodb_redo_log_capacity" if flavour == "mysql" else "@@innodb_log_file_size"
        _, out, _ = sh(cmd, "SELECT @@log_bin, @@innodb_flush_log_at_trx_commit, "
                            f"ROUND(@@innodb_buffer_pool_size/1048576), ROUND({redo}/1048576);")
        lb, fl, bp, rd = (out.split() + ["?"] * 4)[:4]
        found.append({"label": flavour if label == "auto" else label, "cmd": cmd,
                      "cmdline": cmdline, "version": version, "flavour": flavour,
                      "cfg": f"log_bin={'ON' if lb == '1' else 'OFF'} flush={fl} "
                             f"buffer_pool={bp}M redo={rd}M",
                      "binlog": lb == "1", "redo_mb": int(rd) if rd.isdigit() else 0})
    return found, failed


DEFAULT_ENGINES = [
    "auto=mysql", "auto=mariadb",
    "auto=mysql -h 127.0.0.1 -P 3307 --protocol=TCP",
    "auto=/opt/mysql8/usr/bin/mysql --defaults-file=/etc/my8.cnf -u root",
]


def status_bytes(engine):
    _, out, _ = sh(engine["cmd"], "SHOW GLOBAL STATUS WHERE Variable_name IN "
                                  "('Innodb_data_written','Innodb_os_log_written');", DB)
    return sum(int(x.split("\t")[1]) for x in out.splitlines() if "\t" in x)


def table_size(engine, tbl):
    sh(engine["cmd"], f"ANALYZE TABLE {tbl};", DB)
    fresh = ("SET SESSION information_schema_stats_expiry=0;\n"
             if engine["flavour"] == "mysql" else "")
    _, out, _ = sh(engine["cmd"], fresh +
                   "SELECT COALESCE(data_length,0), COALESCE(index_length,0) "
                   f"FROM information_schema.tables WHERE table_schema='{DB}' "
                   f"AND table_name='{tbl}';", DB)
    try:
        d, i = out.split()
        return int(d), int(i)
    except ValueError:
        return 0, 0


# ----------------------------------------------------------------------------- ddl
def hourly_parts(days):
    parts = [f"PARTITION p_floor VALUES LESS THAN ({T0})"]
    parts += [f"PARTITION p_h{h:04d} VALUES LESS THAN ({T0 + (h + 1) * HOUR})"
              for h in range(days * 24)]
    parts.append("PARTITION p_future VALUES LESS THAN MAXVALUE")
    return "PARTITION BY RANGE (begin_timestamp) (" + ",".join(parts) + ")"


def daily_parts(days):
    parts = [f"PARTITION p_floor VALUES LESS THAN ({T0})"]
    parts += [f"PARTITION p_d{d:03d} VALUES LESS THAN ({T0 + (d + 1) * DAY})"
              for d in range(days)]
    parts.append("PARTITION p_future VALUES LESS THAN MAXVALUE")
    return "PARTITION BY RANGE (begin_timestamp) (" + ",".join(parts) + ")"


def ddl(design, days):
    side_cols = ("value VARCHAR(64) NOT NULL, begin_timestamp BIGINT NOT NULL, "
                 "archive_id INT UNSIGNED NOT NULL")
    if design == "inline":
        cols = ",".join(f"lc_{c[0]} TEXT NULL" for c in COLUMNS)
        return {"t_inline": f"CREATE TABLE t_inline (id INT UNSIGNED NOT NULL, "
                            f"begin_timestamp BIGINT NOT NULL, {cols}, "
                            f"PRIMARY KEY (id, begin_timestamp), KEY ix_time (begin_timestamp)"
                            f") ENGINE=InnoDB DEFAULT CHARSET=ascii " + daily_parts(days)}
    if design == "inline_json":
        return {"t_json": f"CREATE TABLE t_json (id INT UNSIGNED NOT NULL, "
                          f"begin_timestamp BIGINT NOT NULL, v JSON NULL, "
                          f"PRIMARY KEY (id, begin_timestamp), KEY ix_time (begin_timestamp)"
                          f") ENGINE=InnoDB " + daily_parts(days)}
    if design == "side_shared":
        return {"t_side": f"CREATE TABLE t_side (column_id TINYINT UNSIGNED NOT NULL, "
                          f"{side_cols}, PRIMARY KEY (column_id, value, begin_timestamp, "
                          f"archive_id)) ENGINE=InnoDB DEFAULT CHARSET=ascii "
                          + hourly_parts(days)}
    return {f"t_lc_{c[0]}": f"CREATE TABLE t_lc_{c[0]} ({side_cols}, PRIMARY KEY (value, "
                            f"begin_timestamp, archive_id)) ENGINE=InnoDB "
                            f"DEFAULT CHARSET=ascii " + hourly_parts(days)
            for c in COLUMNS}


# ----------------------------------------------------------------------------- generation
def generate(archives, days, seed, outdir, want_gt, probes, wins, log):
    import random
    rng = random.Random(seed)
    pools = {c[0]: [val(c[1], c[2], i) for i in range(c[4])] for c in COLUMNS}
    spacing = days * DAY // archives
    gt = {}
    single = {}                       # per-column single predicates needing ground truth
    if want_gt:
        for q, preds in probes.items():
            for col, kind, lit in preds:
                key = (col, kind, lit)
                if key not in single:
                    single[key] = (frozenset(v for v in pools[col] if v.startswith(lit))
                                   if kind == "pfx" else lit)
        gt = {q: {w: 0 for w in wins} for q in probes}
        gt["Q0"] = {w: 0 for w in wins}

    files = {"base": open(os.path.join(outdir, "base.tsv"), "w"),
             "inline": open(os.path.join(outdir, "inline.tsv"), "w"),
             "json": open(os.path.join(outdir, "json.tsv"), "w")}
    for c in COLUMNS:
        files[c[0]] = open(os.path.join(outdir, f"lc_{c[0]}.tsv"), "w")
    t_start = time.time()
    for i in range(archives):
        ts = T0 + i * spacing + rng.randrange(0, max(1, spacing))
        sets = {c[0]: rng.sample(pools[c[0]], c[3]) for c in COLUMNS}
        files["base"].write(f"{i}\t{ts}\n")
        files["inline"].write(f"{i}\t{ts}\t" + "\t".join(
            SEP + SEP.join(sets[c[0]]) + SEP for c in COLUMNS) + "\n")
        files["json"].write(f"{i}\t{ts}\t" + json.dumps(
            {c[0]: sets[c[0]] for c in COLUMNS}, separators=(",", ":")) + "\n")
        for c in COLUMNS:
            f = files[c[0]]
            for v in sets[c[0]]:
                f.write(f"{v}\t{ts}\t{i}\n")
        if want_gt:
            in_wins = [w for w, (a, b) in wins.items() if a <= ts < b]
            if in_wins:
                for w in in_wins:
                    gt["Q0"][w] += 1
                hits = {}
                for (col, kind, lit), probe in single.items():
                    s = sets[col]
                    hits[(col, kind, lit)] = (not probe.isdisjoint(s) if kind == "pfx"
                                              else lit in s) if isinstance(probe, frozenset) \
                        else probe in s
                for q, preds in probes.items():
                    if all(hits[(c, k, l)] for c, k, l in preds):
                        for w in in_wins:
                            gt[q][w] += 1
        if i and i % 200000 == 0:
            log.write(f"    generated {i:,}/{archives:,} "
                      f"({(time.time()-t_start)/60:.0f} min)\n")
            log.flush()
    for f in files.values():
        f.close()
    return gt


# ----------------------------------------------------------------------------- predicates
def q_time(w1, w2, alias=""):
    a = alias + "." if alias else ""
    return f"{a}begin_timestamp >= {w1} AND {a}begin_timestamp < {w2}"


def q_inline(preds, w1, w2):
    like = []
    for col, kind, lit in preds:
        pat = (f"CONCAT('%',CHAR(31 USING ascii),'{lit}','%')" if kind == "pfx" else
               f"CONCAT('%',CHAR(31 USING ascii),'{lit}',CHAR(31 USING ascii),'%')")
        like.append(f"lc_{col} LIKE {pat}")
    return (f"SELECT COUNT(*) FROM t_inline WHERE {q_time(w1, w2)} AND "
            + " AND ".join(like))


def q_json(preds, w1, w2):
    conds = []
    for col, kind, lit in preds:
        if kind == "eq":
            conds.append(f"JSON_CONTAINS(v, '\"{lit}\"', '$.{col}')")
        else:
            conds.append(f"JSON_SEARCH(v, 'one', '{lit}%', NULL, '$.{col}[*]') IS NOT NULL")
    return (f"SELECT COUNT(*) FROM t_json WHERE {q_time(w1, w2)} AND "
            + " AND ".join(conds))


def side_ref(design, col):
    if design == "side_shared":
        cid = next(n for n, c in enumerate(COLUMNS) if c[0] == col)
        return "t_side", f"column_id = {cid} AND "
    return f"t_lc_{col}", ""


def q_side(design, preds, w1, w2, form="join"):
    refs = []
    for n, (col, kind, lit) in enumerate(preds):
        tbl, cid = side_ref(design, col)
        vcond = f"value LIKE '{lit}%'" if kind == "pfx" else f"value = '{lit}'"
        refs.append((f"s{n}", tbl, cid, vcond))
    a0, t0_, c0, v0 = refs[0]
    if len(refs) == 1:
        return (f"SELECT COUNT(DISTINCT {a0}.archive_id) FROM {t0_} {a0} "
                f"WHERE {c0.replace('column_id', a0 + '.column_id')}{a0}.{v0} "
                f"AND {q_time(w1, w2, a0)}")
    if form == "exists":
        inner = []
        for al, tb, ci, vc in refs[1:]:
            inner.append(f"EXISTS (SELECT 1 FROM {tb} {al} WHERE "
                         f"{ci.replace('column_id', al + '.column_id')}{al}.{vc} "
                         f"AND {q_time(w1, w2, al)} AND {al}.archive_id = {a0}.archive_id)")
        return (f"SELECT COUNT(DISTINCT {a0}.archive_id) FROM {t0_} {a0} WHERE "
                f"{c0.replace('column_id', a0 + '.column_id')}{a0}.{v0} "
                f"AND {q_time(w1, w2, a0)} AND " + " AND ".join(inner))
    joins, conds = [f"{t0_} {a0}"], [
        f"{c0.replace('column_id', a0 + '.column_id')}{a0}.{v0}", q_time(w1, w2, a0)]
    for al, tb, ci, vc in refs[1:]:
        joins.append(f"JOIN {tb} {al} ON {al}.archive_id = {a0}.archive_id")
        conds += [f"{ci.replace('column_id', al + '.column_id')}{al}.{vc}",
                  q_time(w1, w2, al)]
    return (f"SELECT COUNT(DISTINCT {a0}.archive_id) FROM " + " ".join(joins)
            + " WHERE " + " AND ".join(conds))


def q_base_count(w1, w2):
    return f"SELECT COUNT(*) FROM t_base WHERE {q_time(w1, w2)}"


def build_query(design, preds, w1, w2, form="join"):
    if design == "inline":
        return q_inline(preds, w1, w2)
    if design == "inline_json":
        return q_json(preds, w1, w2)
    return q_side(design, preds, w1, w2, form)


# ----------------------------------------------------------------------------- measurement
def timed(engine, sql):
    """Two executions: (run1_ms, warm_ms, hits, scanned_on_warm)."""
    script = ("FLUSH STATUS;\nSET @t=NOW(6);\n" + sql + ";\n"
              "SELECT CONCAT('R1=',TIMESTAMPDIFF(MICROSECOND,@t,NOW(6))/1000);\n"
              "FLUSH STATUS;\nSET @t=NOW(6);\n" + sql + ";\n"
              "SELECT CONCAT('R2=',TIMESTAMPDIFF(MICROSECOND,@t,NOW(6))/1000);\n"
              "SHOW SESSION STATUS WHERE Variable_name IN "
              "('Handler_read_next','Handler_read_rnd_next');\n")
    rc, out, err = sh(engine["cmd"], script, DB)
    if rc != 0:
        return None, None, None, None, (err.strip().splitlines() or ["error"])[-1][:80]
    r1 = r2 = hits = None
    scanned = 0
    for line in out.splitlines():
        if line.startswith("R1="):
            r1 = float(line[3:])
        elif line.startswith("R2="):
            r2 = float(line[3:])
        elif "\t" in line:
            scanned += int(line.split("\t")[1])
        elif line.strip().isdigit():
            hits = int(line.strip())      # both executions print it; last wins
    return r1, r2, hits, scanned, None


def insert_txns(design, aid, ts, sets):
    stmts = [f"INSERT INTO t_base (id,begin_timestamp) VALUES ({aid},{ts});"]
    if design == "inline":
        # Columns named explicitly: the build's config-change test adds lc_extra, so a
        # positional VALUES list would no longer match the table.
        cols = ",".join(f"lc_{c[0]}" for c in COLUMNS)
        vals = ",".join("'" + SEP + SEP.join(sets[c[0]]) + SEP + "'" for c in COLUMNS)
        return [f"INSERT INTO t_inline (id,begin_timestamp,{cols}) "
                f"VALUES ({aid},{ts},{vals});"]
    if design == "inline_json":
        doc = json.dumps({c[0]: sets[c[0]] for c in COLUMNS}, separators=(",", ":"))
        return [f"INSERT INTO t_json (id,begin_timestamp,v) VALUES ({aid},{ts},'{doc}');"]
    if design == "side_shared":
        rows = ",".join(f"({n},'{v}',{ts},{aid})"
                        for n, c in enumerate(COLUMNS) for v in sets[c[0]])
        return stmts + [f"INSERT INTO t_side VALUES {rows};"]
    out = list(stmts)
    for c in COLUMNS:
        rows = ",".join(f"('{v}',{ts},{aid})" for v in sets[c[0]])
        out.append(f"INSERT INTO t_lc_{c[0]} VALUES {rows};")
    return out


def make_insert_script(design, ids, ts0, seed, per_txn):
    import random
    rng = random.Random(seed)
    pools = {c[0]: [val(c[1], c[2], i) for i in range(c[4])] for c in COLUMNS}
    parts = []
    for g in range(0, len(ids), per_txn):
        parts.append("BEGIN;")
        for j, aid in enumerate(ids[g:g + per_txn]):
            sets = {c[0]: rng.sample(pools[c[0]], c[3]) for c in COLUMNS}
            parts += insert_txns(design, aid, ts0 + (g + j) * 10**6, sets)
        parts.append("COMMIT;")
    return "\n".join(parts)


def next_id(engine):
    """Highest id across every archive-bearing table, so repeated query runs never
    collide -- inline/json writes do not touch t_base, so t_base alone is not enough."""
    hi = 0
    for tbl in ("t_base", "t_inline", "t_json"):
        rc, out, _ = sh(engine["cmd"], f"SELECT COALESCE(MAX(id),0) FROM {tbl};", DB)
        if rc == 0 and out.strip().isdigit():
            hi = max(hi, int(out.strip()))
    return hi + 1 if hi else 10**8


def write_tests(engine, design, days, batch, seed, log):
    r = {}
    ts0 = T0 + days * DAY - HOUR // 2
    base = next_id(engine)
    for tag, per_txn, n in (("ins1", 1, batch), ("ins10", 10, batch)):
        ids = list(range(base, base + n))
        base += n
        script = make_insert_script(design, ids, ts0, seed + per_txn, per_txn)
        b0 = status_bytes(engine)
        t0 = time.time()
        rc, _, err = sh(engine["cmd"], script, DB)
        el = time.time() - t0
        if rc != 0:
            r[tag] = None
            r["note"] = (err.strip().splitlines() or ["?"])[-1][:70]
            continue
        r[tag] = round(n / el, 1)
        if tag == "ins1":
            r["bytes_arch"] = round((status_bytes(engine) - b0) / n)
    # 4 concurrent connections, ins1 shape. Scripts are fed from files, not pipes: a client
    # that aborts on error closes stdin early and a pipe write would then kill this process.
    n4 = max(batch // 2, 40)
    procs, tmps = [], []
    t0 = time.time()
    for k in range(4):
        ids = list(range(base + k * n4, base + (k + 1) * n4))
        script = make_insert_script(design, ids, ts0, seed + 100 + k, 1)
        tf = tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False)
        tf.write(script)
        tf.close()
        tmps.append(tf.name)
        argv = list(engine["cmd"]) + ["-N", "-B", DB]
        procs.append(subprocess.Popen(argv, stdin=open(tf.name), text=True,
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    ok = all(p.wait(timeout=3600) == 0 for p in procs)
    for t in tmps:
        os.unlink(t)
    r["ins4c"] = round(4 * n4 / (time.time() - t0), 1) if ok else None
    base += 4 * n4
    return r


def interference(engine, design, days, wins, probes, seed, log):
    """Query latency while the ingest path runs, plus concurrent insert rate."""
    w1, w2 = wins["1d"]
    sql = build_query(design, probes["Q1_entity"], w1, w2) + ";"
    solo = []
    for _ in range(5):
        t0 = time.time()
        sh(engine["cmd"], sql, DB)
        solo.append((time.time() - t0) * 1000)
    n = 150
    ids = list(range(next_id(engine), next_id(engine) + n))
    script = make_insert_script(design, ids, T0 + days * DAY - HOUR // 2, seed + 999, 1)
    tf = tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False)
    tf.write(script)
    tf.close()
    argv = list(engine["cmd"]) + ["-N", "-B", DB]
    t0 = time.time()
    p = subprocess.Popen(argv, stdin=open(tf.name), text=True,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    during = []
    while p.poll() is None:
        t1 = time.time()
        sh(engine["cmd"], sql, DB)
        during.append((time.time() - t1) * 1000)
        if time.time() - t0 > 300:
            p.kill()
            break
    el = time.time() - t0
    os.unlink(tf.name)
    med = lambda xs: sorted(xs)[len(xs) // 2] if xs else None
    return {"solo_ms": round(med(solo), 1), "during_ms": round(med(during), 1),
            "conc_ins": round(n / el, 1), "queries_ran": len(during),
            "ok": p.returncode == 0}


# ----------------------------------------------------------------------------- build
def do_build(a, engines, out):
    line = lambda s: (print(s), out.write(s + "\n"), out.flush())
    archives = int(a.rate * a.days * 86400)
    probes = make_probes()
    wins = windows(a.days)
    want_gt = archives <= 200_000
    designs = a.design or DESIGNS
    rows_per, payload = tier_cost(COLUMNS)
    comp_arch = RAW_BYTES_PER_ARCHIVE / COMPRESSION
    line(f"archives={archives:,} (rate {a.rate}/s x {a.days}d)  tier={a.columns}  "
         f"side rows/archive={rows_per:,}  "
         f"ground truth={'yes' if want_gt else 'no (too large; cross-design check only)'}")
    line(f"filter payload {payload:,} B/archive = {100*payload/comp_arch:.3f}% of a "
         f"compressed archive ({comp_arch/1048576:.2f} MB at {COMPRESSION}x); by column:")
    for c in sorted(COLUMNS, key=lambda x: -x[3] * (x[2] + 1)):
        b = c[3] * (c[2] + 1)
        line(f"    {c[0]:<10}{c[3]:>6,} distinct x {c[2]:>3}B = {b:>8,} B "
             f"({100*b/payload:>5.1f}% of filter payload)")
    manifest = {"created": time.strftime("%Y-%m-%d %H:%M:%S"), "seed": a.seed,
                "rate": a.rate, "days": a.days, "archives": archives, "columns": a.columns,
                "designs": designs, "engines": [], "gt": None, "build": {}}
    with tempfile.TemporaryDirectory(dir=a.tmpdir) as td:
        line("generating corpus ...")
        t0 = time.time()
        gt = generate(archives, a.days, a.seed, td, want_gt, probes, wins, sys.stderr)
        gen_s = round(time.time() - t0, 1)
        line(f"  generation: {gen_s}s")
        if want_gt:
            manifest["gt"] = {q: dict(v) for q, v in gt.items()}
        os.chmod(td, 0o755)
        for f in os.listdir(td):
            os.chmod(os.path.join(td, f), 0o644)
        for e in engines:
            manifest["engines"].append({"label": e["label"], "cmdline": e["cmdline"],
                                        "version": e["version"]})
            line(f"\n[{e['label']}] {e['version']}")
            sh(e["cmd"], f"DROP DATABASE IF EXISTS {DB}; CREATE DATABASE {DB};")
            sh(e["cmd"], "CREATE TABLE t_base (id INT UNSIGNED NOT NULL, "
                         "begin_timestamp BIGINT NOT NULL, PRIMARY KEY (id, begin_timestamp)"
                         ") ENGINE=InnoDB " + daily_parts(a.days) + ";", DB)
            sh(e["cmd"], f"LOAD DATA LOCAL INFILE '{td}/base.tsv' INTO TABLE t_base;",
               DB, local_infile=True)
            for design in designs:
                bres = {"engine": e["label"], "design": design}
                t0 = time.time()
                for tbl, stmt in ddl(design, a.days).items():
                    rc, _, err = sh(e["cmd"], stmt + ";", DB)
                    if rc != 0:
                        line(f"  ! {design}/{tbl}: "
                             f"{(err.strip().splitlines() or ['?'])[-1][:80]}")
                bres["create_s"] = round(time.time() - t0, 1)
                t0 = time.time()
                if design == "inline":
                    cols = ",".join(f"lc_{c[0]}" for c in COLUMNS)
                    sh(e["cmd"], f"LOAD DATA LOCAL INFILE '{td}/inline.tsv' INTO TABLE "
                                 f"t_inline (id,begin_timestamp,{cols});",
                       DB, local_infile=True)
                elif design == "inline_json":
                    sh(e["cmd"], f"LOAD DATA LOCAL INFILE '{td}/json.tsv' INTO TABLE "
                                 f"t_json (id,begin_timestamp,v);", DB, local_infile=True)
                elif design == "side_shared":
                    for n, c in enumerate(COLUMNS):
                        sh(e["cmd"], f"LOAD DATA LOCAL INFILE '{td}/lc_{c[0]}.tsv' INTO "
                                     f"TABLE t_side (value,begin_timestamp,archive_id) "
                                     f"SET column_id={n};", DB, local_infile=True)
                else:
                    for c in COLUMNS:
                        sh(e["cmd"], f"LOAD DATA LOCAL INFILE '{td}/lc_{c[0]}.tsv' INTO "
                                     f"TABLE t_lc_{c[0]} (value,begin_timestamp,archive_id);",
                           DB, local_infile=True)
                bres["load_s"] = round(time.time() - t0, 1)
                d_b = i_b = 0
                for tbl in ddl(design, a.days):
                    d, i = table_size(e, tbl)
                    d_b += d
                    i_b += i
                bres["data_mb"] = round(d_b / 1048576, 1)
                bres["index_mb"] = round(i_b / 1048576, 1)
                bres["b_arch"] = round((d_b + i_b) / archives)
                comp = archives * RAW_BYTES_PER_ARCHIVE / COMPRESSION
                bres["pct_comp"] = round(100 * (d_b + i_b) / comp, 3)
                # config-change cost: add a 9th filter column, per design semantics
                t0 = time.time()
                if design == "inline":
                    sh(e["cmd"], "ALTER TABLE t_inline ADD COLUMN lc_extra TEXT NULL;", DB)
                elif design == "side_percol":
                    sh(e["cmd"], "CREATE TABLE t_lc_extra (value VARCHAR(64) NOT NULL, "
                                 "begin_timestamp BIGINT NOT NULL, archive_id INT UNSIGNED "
                                 "NOT NULL, PRIMARY KEY (value, begin_timestamp, archive_id))"
                                 " ENGINE=InnoDB DEFAULT CHARSET=ascii "
                                 + hourly_parts(a.days) + ";", DB)
                bres["addcol_s"] = round(time.time() - t0, 2)
                # GC: one early-morning hour, far from the query windows
                gc_tbl = {"side_shared": "t_side", "side_percol": "t_lc_entity1"}.get(design)
                if gc_tbl:
                    t0 = time.time()
                    sh(e["cmd"], f"ALTER TABLE {gc_tbl} DROP PARTITION p_h0027;", DB)
                    bres["gc_drop_ms"] = round((time.time() - t0) * 1000, 1)
                    w1 = T0 + 29 * HOUR
                    t0 = time.time()
                    sh(e["cmd"], f"DELETE FROM {gc_tbl} WHERE begin_timestamp >= {w1} "
                                 f"AND begin_timestamp < {w1 + HOUR};", DB)
                    bres["gc_del_ms"] = round((time.time() - t0) * 1000, 1)
                if design == "inline_json" and e["flavour"] == "mysql":
                    mvi = {}
                    for c in COLUMNS:
                        rc, _, err = sh(e["cmd"],
                                        f"ALTER TABLE t_json ADD INDEX mvi_{c[0]} "
                                        f"((CAST(v->'$.{c[0]}' AS CHAR(64) ARRAY)));", DB)
                        mvi[c[0]] = "ok" if rc == 0 else \
                            (err.strip().splitlines() or ["failed"])[-1][:60]
                    bres["mvi"] = mvi
                manifest["build"][f"{e['label']}/{design}"] = bres
                line(f"  {design:<12} create {bres['create_s']:>7.1f}s  "
                     f"load {bres['load_s']:>8.1f}s  data {bres['data_mb']:>10,.1f}M  "
                     f"index {bres['index_mb']:>10,.1f}M  {bres['b_arch']:>8,}B/arch  "
                     f"{bres['pct_comp']:>6.3f}% of compressed  "
                     f"addcol {bres['addcol_s']}s")
                if bres.get("gc_drop_ms") is not None:
                    line(f"  {'':<12} GC: drop partition {bres['gc_drop_ms']} ms vs "
                         f"row-wise delete {bres['gc_del_ms']} ms")
                if bres.get("mvi"):
                    for c, s in bres["mvi"].items():
                        line(f"  {'':<12} mvi[{c}]: {s}")
    with open(MANIFEST, "w") as f:
        json.dump(manifest, f, indent=1)
    line(f"\nmanifest written to {MANIFEST}; database '{DB}' kept for query runs")


# ----------------------------------------------------------------------------- query
def do_query(a, engines, manifest, out):
    line = lambda s: (print(s), out.write(s + "\n"), out.flush())
    probes = make_probes()
    wins = windows(manifest["days"])
    days, seed = manifest["days"], manifest["seed"]
    gt = manifest.get("gt")
    designs = [d for d in (a.design or manifest["designs"])]
    sel_q = a.query or list(probes)
    sel_w = a.window or list(wins)
    comp_pct = 100.0 / (RAW_BYTES_PER_ARCHIVE / COMPRESSION)
    for e in engines:
        line(f"\n[{e['label']}] {e['version']}   {e['cfg']}")
        if e.get("binlog"):
            line("  WARNING: binary log ON -- write rates measure fsync policy, not schema")
        line(f"  Q0 candidates (time-only):")
        q0 = {}
        for w in sel_w:
            w1, w2 = wins[w]
            _, _, hits, _, _ = timed(e, q_base_count(w1, w2))
            q0[w] = hits
            gtq = f"  (gt {gt['Q0'][w]:,})" if gt else ""
            line(f"    {w:<4} {hits:>12,}{gtq}")
        line("")
        line(f"  {'design':<12}{'query':<12}{'win':<5}{'run1_ms':>9}{'warm_ms':>9}"
             f"{'hits':>10}{'scanned':>12}{'prune%':>8}  {'check':<8}")
        line("  " + "-" * 92)
        agreement = {}
        for design in designs:
            for q in sel_q:
                preds = probes[q]
                for w in sel_w:
                    w1, w2 = wins[w]
                    sql = build_query(design, preds, w1, w2, a.join_form)
                    r1, r2, hits, scanned, err = timed(e, sql)
                    if err:
                        line(f"  {design:<12}{q:<12}{w:<5} ! {err}")
                        continue
                    check = ""
                    if gt:
                        check = "OK" if hits == gt[q][w] else f"GT={gt[q][w]}!"
                    key = (q, w)
                    agreement.setdefault(key, {})[design] = hits
                    prune = (100 * (1 - hits / q0[w])) if q0.get(w) else 0
                    line(f"  {design:<12}{q:<12}{w:<5}{r1:>9,.1f}{r2:>9,.1f}"
                         f"{hits:>10,}{scanned:>12,}{prune:>7.1f}%  {check:<8}")
        bad = {k: v for k, v in agreement.items() if len(set(v.values())) > 1}
        if bad:
            line("\n  *** CROSS-DESIGN DISAGREEMENT:")
            for (q, w), v in bad.items():
                line(f"      {q}/{w}: " + ", ".join(f"{d}={h}" for d, h in v.items()))
        else:
            line("\n  all designs agree on every hit count")
        if not a.skip_writes:
            line(f"\n  WRITE PATH (archives/s; target {a.target_rate}/s fleet-wide)")
            line(f"  {'design':<12}{'ins1/s':>9}{'ins10/s':>9}{'4conn/s':>9}"
                 f"{'phys_KB/arch':>14}")
            for design in designs:
                try:
                    r = write_tests(e, design, days, a.insert_batch, seed, sys.stderr)
                except Exception as exc:                      # keep one failure contained
                    line(f"  {design:<12} ! {str(exc)[:80]}")
                    continue
                kb = f"{r['bytes_arch']/1024:,.0f}" if r.get("bytes_arch") else "-"
                f_ = lambda k: f"{r[k]:,.1f}" if r.get(k) else "-"
                line(f"  {design:<12}{f_('ins1'):>9}{f_('ins10'):>9}{f_('ins4c'):>9}"
                     f"{kb:>14}" + (f"   ! {r['note']}" if r.get("note") else ""))
        if not a.skip_interference:
            line(f"\n  INTERFERENCE (Q1_entity/1d while ingesting)")
            for design in designs:
                try:
                    r = interference(e, design, days, wins, probes, seed, sys.stderr)
                except Exception as exc:
                    line(f"  {design:<12} ! {str(exc)[:80]}")
                    continue
                note = "" if r.get("ok") else "  (ingest script reported an error)"
                line(f"  {design:<12} solo {r['solo_ms']} ms -> during ingest "
                     f"{r['during_ms']} ms; concurrent insert {r['conc_ins']} arch/s "
                     f"({r['queries_ran']} queries overlapped){note}")


# ----------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["build", "query", "all"])
    ap.add_argument("--engine", action="append", default=[], metavar="LABEL=COMMAND")
    ap.add_argument("--rate", type=float, default=0.05,
                    help="archives/second of simulated history (default 0.05; full=0.5)")
    ap.add_argument("--days", type=int, default=28)
    ap.add_argument("--columns", choices=list(TIERS), default="standard",
                    help="how permissive the index configuration is; storage is dominated "
                         "by the highest-distinct column included (default: standard)")
    ap.add_argument("--design", action="append", choices=DESIGNS, default=[])
    ap.add_argument("--query", action="append", default=[])
    ap.add_argument("--window", action="append", default=[], choices=["1h", "1d", "7d"])
    ap.add_argument("--join-form", choices=["join", "exists"], default="join")
    ap.add_argument("--insert-batch", type=int, default=200)
    ap.add_argument("--target-rate", type=float, default=31.0)
    ap.add_argument("--skip-writes", action="store_true")
    ap.add_argument("--skip-interference", action="store_true")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default="")
    ap.add_argument("--tmpdir", default=tempfile.gettempdir())
    a = ap.parse_args()
    set_tier(a.columns)

    engines, failed = detect(a.engine or DEFAULT_ENGINES)
    if not engines:
        for c, why in failed:
            sys.stderr.write(f"  tried: {c}\n         -> {why}\n")
        sys.exit("no usable server (needs grants on lc3.* plus global RELOAD)")

    stamp = time.strftime("%Y%m%d-%H%M%S")
    outpath = a.out or f"lc3_{a.mode}_{stamp}.txt"
    out = open(outpath, "w")
    line = lambda s: (print(s), out.write(s + "\n"))
    line("=" * 100)
    line(f" LC FILTER ROUND 3 ({a.mode})   {time.strftime('%Y-%m-%d %H:%M:%S')}")
    for e in engines:
        line(f"   {e['label']:<9} {e['version']}")
        line(f"   {'':<9} {e['cfg']}")
        if e.get("redo_mb", 0) and e["redo_mb"] < 1024:
            line(f"   {'':<9} WARNING: redo only {e['redo_mb']}M -- large loads will stall; "
                 f"raise to 4G")
    missing = {"mariadb", "mysql"} - {e["flavour"] for e in engines}
    for m in sorted(missing):
        line(f"   MISSING: no {m} reached -- results cover only "
             f"{'/'.join(e['flavour'] for e in engines)}")
    line("=" * 100)

    if a.mode in ("build", "all"):
        do_build(a, engines, out)
    if a.mode in ("query", "all"):
        if not os.path.exists(MANIFEST):
            sys.exit(f"{MANIFEST} not found -- run build first")
        manifest = json.load(open(MANIFEST))
        # The tier defines which tables exist, so query runs must use the built one.
        built = manifest.get("columns", "full")
        if built != a.columns:
            line(f"  (using tier '{built}' from the manifest, not '{a.columns}')")
            set_tier(built)
        bad = [q for q in a.query if q not in make_probes()]
        if bad:
            sys.exit(f"query {bad} not available in tier '{built}'; "
                     f"choose from {list(make_probes())}")
        do_query(a, engines, manifest, out)
    line(f"\nwritten to {outpath}")
    out.close()


if __name__ == "__main__":
    main()
