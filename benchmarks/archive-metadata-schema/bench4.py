#!/usr/bin/env python3
"""Round 4: focused follow-up experiments on the shared side table design.

  B1 charset      utf8mb4 vs ascii vs VARBINARY storage, incl. multibyte values, and the
                  index-key-length ceiling (VARCHAR(1024) utf8mb4 cannot be a PK).
  B2 compression  ROW_FORMAT=COMPRESSED (both engines) and PAGE_COMPRESSED (MariaDB) on the
                  side table: size, load, query, insert.
  B3 index share  data_length vs index_length for every design (side tables are clustered-
                  PK-only, so their "index" IS the data; inline pays a secondary index).
  B4 column cost  marginal storage per archive for single columns of 1/8/15/20/64 distinct
                  values per archive -- including the requested 15-value case.
  B5 column_id    TINYINT vs SMALLINT key width. (Per-configuration numbering is a design
                  question answered in the report text; this measures the insurance cost.)
  B6 join-back    filter-only vs filter + join to the archives table returning metadata --
                  the full database-side query path.
  B7 unified      per-dataset tables vs one unified table with dataset_id: single-dataset
                  and all-datasets queries.
  B8 min/max      numeric range filtering via per-archive min/max columns in the archives
                  table: storage and query cost.
  B9 d1 fast path single-cardinality columns as a plain archives-table column + secondary
                  index, vs postings in the side table.

Storage experiments (B1/B2/B4/B5) use unpartitioned tables so per-row costs are not
polluted by partition fixed overhead; query experiments (B6-B9) use the design's hourly
partitions. Run everything: ./bench4.sh   One item: python3 bench4.py --exp B2 ...
"""

import argparse
import os
import shlex
import subprocess
import sys
import tempfile
import time

DB = "lc4"
T0 = 1704067200 * 10**9
HOUR = 3600 * 10**9
DAY = 24 * HOUR
DAYS = 28

# (name, tag, width, distinct/archive, pool)
C8 = [("host0", "h0", 12, 1, 1000), ("host1", "h1", 12, 1, 1000), ("host2", "h2", 12, 1, 1000),
      ("env0", "e0", 8, 3, 12), ("env1", "e1", 8, 3, 12),
      ("sev0", "s0", 10, 8, 40), ("sev1", "s1", 10, 8, 40),
      ("mod0", "m0", 14, 20, 300)]
SINGLES = [("d1", "a1", 12, 1, 1000), ("d8", "a8", 10, 8, 40), ("d15", "b5", 16, 15, 500),
           ("d20", "c0", 14, 20, 300), ("d64", "c4", 18, 64, 5000)]


def val(tag, width, i, mb=False):
    digits = min(6, width - len(tag))
    v = (tag + format(i, f"0{digits}d") + "x" * width)[:width]
    # Multibyte variant: swap the trailing pad for CJK characters (3 bytes each in utf8mb4).
    # Character length is unchanged, so VARCHAR(width) still fits; byte length grows.
    return v[:-2] + "日本" if mb else v


def sh(cmd, sql, db=None, local_infile=False, timeout=7200):
    argv = list(cmd) + (["--local-infile=1"] if local_infile else []) + ["-N", "-B"]
    if db:
        argv.append(db)
    p = subprocess.run(argv, input=sql, capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


def detect(specs):
    found, seen = [], set()
    for spec in specs:
        label, _, cmdline = spec.partition("=")
        cmd = shlex.split(cmdline)
        try:
            rc, out, _ = sh(cmd, "SELECT VERSION();", timeout=15)
        except (OSError, subprocess.SubprocessError):
            continue
        if rc != 0 or not out.strip():
            continue
        version = out.strip().splitlines()[0]
        flavour = "mariadb" if "mariadb" in version.lower() else "mysql"
        if flavour in seen:
            continue
        seen.add(flavour)
        rc, _, _ = sh(cmd, f"CREATE DATABASE IF NOT EXISTS {DB};")
        if rc == 0:
            found.append({"label": flavour, "cmd": cmd, "version": version,
                          "flavour": flavour})
    return found


DEFAULT_ENGINES = [
    "auto=mysql", "auto=mariadb",
    "auto=/opt/mysql8/usr/bin/mysql --defaults-file=/etc/my8.cnf -u root",
]


def allocated_bytes(e, tbl):
    """On-disk file size. Needed for PAGE_COMPRESSED, whose savings are invisible to
    data_length (it reports uncompressed page sizes; compression happens at the file)."""
    src = ("INNODB_SYS_TABLESPACES" if e["flavour"] == "mariadb" else "INNODB_TABLESPACES")
    _, out, _ = sh(e["cmd"], f"SELECT COALESCE(ALLOCATED_SIZE, FILE_SIZE) FROM "
                             f"information_schema.{src} WHERE NAME='{DB}/{tbl}';", DB)
    return int(out.strip()) if out.strip().isdigit() else 0


def tbl_bytes(e, tbl):
    sh(e["cmd"], f"ANALYZE TABLE {tbl};", DB)
    fresh = ("SET SESSION information_schema_stats_expiry=0;\n"
             if e["flavour"] == "mysql" else "")
    _, out, _ = sh(e["cmd"], fresh + "SELECT COALESCE(data_length,0), "
                   "COALESCE(index_length,0) FROM information_schema.tables "
                   f"WHERE table_schema='{DB}' AND table_name='{tbl}';", DB)
    try:
        d, i = out.split()
        return int(d), int(i)
    except ValueError:
        return 0, 0


def timed(e, sql, timeout_s=180):
    cap = (f"SET SESSION max_statement_time={timeout_s};\n" if e["flavour"] == "mariadb"
           else f"SET SESSION max_execution_time={timeout_s * 1000};\n")
    script = (cap + sql + ";\nFLUSH STATUS;\nSET @t=NOW(6);\n" + sql + ";\n"
              "SELECT CONCAT('MS=',TIMESTAMPDIFF(MICROSECOND,@t,NOW(6))/1000);\n"
              "SHOW SESSION STATUS WHERE Variable_name IN "
              "('Handler_read_next','Handler_read_rnd_next');\n")
    rc, out, err = sh(e["cmd"], script, DB)
    if rc != 0:
        return None, None, None, (err.strip().splitlines() or ["error"])[-1][:80]
    ms = hits = None
    scanned = 0
    for line in out.splitlines():
        if line.startswith("MS="):
            ms = float(line[3:])
        elif "\t" in line:
            scanned += int(line.split("\t")[1])
        elif line.strip().isdigit():
            hits = int(line.strip())
    return ms, hits, scanned, None


def hourly_parts():
    parts = [f"PARTITION p_floor VALUES LESS THAN ({T0})"]
    parts += [f"PARTITION p_h{h:04d} VALUES LESS THAN ({T0 + (h + 1) * HOUR})"
              for h in range(DAYS * 24)]
    parts.append("PARTITION p_future VALUES LESS THAN MAXVALUE")
    return "PARTITION BY RANGE (begin_timestamp) (" + ",".join(parts) + ")"


def gen_corpus(archives, cols, outdir, seed, mb=False, base_extra=None):
    """Writes side postings (col_id,value,ts,id) and a base table (id,ts[,extra...])."""
    import random
    rng = random.Random(seed)
    pools = {c[0]: [val(c[1], c[2], i, mb and i % 5 == 0) for i in range(c[4])]
             for c in cols}
    spacing = DAYS * DAY // archives
    sp = os.path.join(outdir, "side.tsv")
    bp = os.path.join(outdir, "base.tsv")
    with open(sp, "w") as fs, open(bp, "w") as fb:
        for i in range(archives):
            ts = T0 + i * spacing + rng.randrange(0, max(1, spacing))
            extra = base_extra(rng, i) if base_extra else []
            fb.write("\t".join([str(i), str(ts)] + [str(x) for x in extra]) + "\n")
            for n, c in enumerate(cols):
                for v in rng.sample(pools[c[0]], c[3]):
                    fs.write(f"{n}\t{v}\t{ts}\t{i}\n")
    return sp, bp


def load(e, tbl, path, columns):
    sys.stderr.write(f"    loading {tbl} [{e['label']}] ...\n")
    t0 = time.time()
    rc, _, err = sh(e["cmd"], f"LOAD DATA LOCAL INFILE '{path}' INTO TABLE {tbl} "
                              f"({columns});", DB, local_infile=True)
    el = round(time.time() - t0, 1)
    sys.stderr.write(f"      ... {el}s\n")
    if rc != 0:
        return None, (err.strip().splitlines() or ["?"])[-1][:70]
    return el, None


SIDE_COLS = ("column_id TINYINT UNSIGNED NOT NULL, value VARCHAR(64) NOT NULL, "
             "begin_timestamp BIGINT NOT NULL, archive_id INT UNSIGNED NOT NULL")
SIDE_PK = "PRIMARY KEY (column_id, value, begin_timestamp, archive_id)"


def section(out, title):
    line = lambda s: (print(s), out.write(s + "\n"), out.flush())
    line("")
    line("=" * 100)
    line(f" {title}")
    line("=" * 100)
    return line


# --------------------------------------------------------------------------- B1 charset
def b1(engines, a, out, paths):
    line = section(out, "B1  Value charset: ascii vs utf8mb4 vs VARBINARY "
                        f"({a.small:,} archives, {sum(c[3] for c in C8)} postings/archive)")
    variants = [
        ("ascii_a", "VARCHAR(64)", "DEFAULT CHARSET=ascii", False),
        ("utf8_a", "VARCHAR(64)", "DEFAULT CHARSET=utf8mb4", False),
        ("utf8_mb", "VARCHAR(64)", "DEFAULT CHARSET=utf8mb4", True),
        ("varbin_mb", "VARBINARY(255)", "", True),
    ]
    line(f"  {'variant':<11}{'value type':<16}{'charset':<10}{'multibyte':<11}"
         f"{'B/arch':>8}{'load_s':>8}{'eq_ms':>8}{'pfx_ms':>8}")
    line("  " + "-" * 88)
    for name, vtype, charset, mb in variants:
        cols = SIDE_COLS.replace("VARCHAR(64)", vtype)
        sh(engines[0]["cmd"], "", DB)
        for e in engines:
            sh(e["cmd"], f"DROP TABLE IF EXISTS b1_{name}; CREATE TABLE b1_{name} "
                         f"({cols}, {SIDE_PK}) ENGINE=InnoDB {charset};", DB)
            src = paths["mb" if mb else "plain"]
            ls, err = load(e, f"b1_{name}", src, "column_id,value,begin_timestamp,archive_id")
            if err:
                line(f"  {name:<11} [{e['label']}] ! {err}")
                continue
            d, i = tbl_bytes(e, f"b1_{name}")
            probe = val("m0", 14, 7, False)
            eq, _, _, _ = timed(e, f"SELECT COUNT(DISTINCT archive_id) FROM b1_{name} "
                                   f"WHERE column_id=7 AND value='{probe}'")
            pfx, _, _, _ = timed(e, f"SELECT COUNT(DISTINCT archive_id) FROM b1_{name} "
                                    f"WHERE column_id=7 AND value LIKE '{probe[:7]}%'")
            cs = charset.replace("DEFAULT CHARSET=", "") or "binary"
            line(f"  {name:<11}{vtype:<16}{cs:<10}"
                 f"{('20% CJK' if mb else 'no'):<11}{round((d + i) / a.small):>8,}"
                 f"{ls:>8}{eq:>8.1f}{pfx:>8.1f}   [{e['label']}]")
            sh(e["cmd"], f"DROP TABLE b1_{name};", DB)
    line("")
    line("  key-length ceiling probes (PRIMARY KEY containing the value column):")
    for e in engines:
        for vtype, charset in (("VARCHAR(1024)", "utf8mb4"), ("VARCHAR(768)", "utf8mb4"),
                               ("VARCHAR(1024)", "ascii"), ("VARBINARY(1024)", "binary")):
            cs = "" if charset == "binary" else f"DEFAULT CHARSET={charset}"
            rc, _, err = sh(e["cmd"],
                            f"DROP TABLE IF EXISTS b1k; CREATE TABLE b1k "
                            f"({SIDE_COLS.replace('VARCHAR(64)', vtype)}, {SIDE_PK}) "
                            f"ENGINE=InnoDB {cs};", DB)
            verdict = "ok" if rc == 0 else (err.strip().splitlines() or ["?"])[-1][:60]
            line(f"    [{e['label']}] {vtype:<16}{charset:<9} -> {verdict}")
            sh(e["cmd"], "DROP TABLE IF EXISTS b1k;", DB)


# --------------------------------------------------------------------------- B2 compression
def b2(engines, a, out, paths):
    line = section(out, f"B2  InnoDB compression on the side table ({a.small:,} archives)")
    line("  (B/arch here is ALLOCATED file bytes, so PAGE_COMPRESSED savings are visible.")
    line("   PAGE_COMPRESSED uses filesystem hole-punching: its ratio is filesystem-")
    line("   dependent and looks exaggerated on tiny corpora -- trust it only at --small")
    line("   50K+ on the target filesystem.)")
    line(f"  {'variant':<22}{'B/arch':>8}{'vs plain':>9}{'load_s':>8}{'eq_ms':>8}"
         f"{'pfx_ms':>8}{'ins1/s':>9}")
    line("  " + "-" * 84)
    for e in engines:
        variants = [("plain", ""),
                    ("compressed_kbs8", "ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8")]
        if not a.skip_kbs4:
            # kbs4 is the most expensive load in the suite (~6x plain) and loses on every
            # axis; skippable once its verdict is established at smaller scale.
            variants.append(("compressed_kbs4", "ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4"))
        if e["flavour"] == "mariadb":
            variants.append(("page_compressed", "PAGE_COMPRESSED=1"))
        base_b = None
        for name, opts in variants:
            rc, _, err = sh(e["cmd"], f"DROP TABLE IF EXISTS b2_{name}; CREATE TABLE "
                                      f"b2_{name} ({SIDE_COLS}, {SIDE_PK}) ENGINE=InnoDB "
                                      f"DEFAULT CHARSET=ascii {opts};", DB)
            if rc != 0:
                line(f"  {name:<22} [{e['label']}] ! "
                     f"{(err.strip().splitlines() or ['?'])[-1][:60]}")
                continue
            ls, err = load(e, f"b2_{name}", paths["plain"],
                           "column_id,value,begin_timestamp,archive_id")
            if err:
                line(f"  {name:<22} [{e['label']}] ! {err}")
                continue
            d, i = tbl_bytes(e, f"b2_{name}")
            alloc = allocated_bytes(e, f"b2_{name}")
            per = round((alloc or d + i) / a.small)
            base_b = base_b or per
            probe = val("m0", 14, 7)
            eq, _, _, _ = timed(e, f"SELECT COUNT(DISTINCT archive_id) FROM b2_{name} "
                                   f"WHERE column_id=7 AND value='{probe}'")
            pfx, _, _, _ = timed(e, f"SELECT COUNT(DISTINCT archive_id) FROM b2_{name} "
                                    f"WHERE column_id=7 AND value LIKE '{probe[:7]}%'")
            rows = "),(".join(f"7,'{val('m0', 14, k % 300)}',{T0 + k},{10**8 + k}"
                              for k in range(2000))
            t0 = time.time()
            sh(e["cmd"], f"INSERT INTO b2_{name} VALUES ({rows});", DB)
            ins = round(2000 / 45 / (time.time() - t0), 1)   # ~archives/s at 45 postings
            line(f"  {name:<22}{per:>8,}{per / base_b:>8.2f}x{ls:>8}{eq:>8.1f}"
                 f"{pfx:>8.1f}{ins:>9,.1f}   [{e['label']}]")
            sh(e["cmd"], f"DROP TABLE b2_{name};", DB)


# --------------------------------------------------------------------------- B4 column cost
def b4(engines, a, out, tmpdir, seed):
    line = section(out, f"B4  Marginal side-table cost per column shape "
                        f"({a.small:,} archives, unpartitioned)")
    line(f"  {'shape':<8}{'distinct':>9}{'width':>7}{'postings':>10}{'B/arch':>9}"
         f"{'B/posting':>11}")
    line("  " + "-" * 60)
    e = engines[0]
    for c in SINGLES:
        with tempfile.TemporaryDirectory(dir=tmpdir) as td:
            sp, _ = gen_corpus(a.small, [c], td, seed)
            os.chmod(td, 0o755)
            os.chmod(sp, 0o644)
            sh(e["cmd"], f"DROP TABLE IF EXISTS b4; CREATE TABLE b4 ({SIDE_COLS}, "
                         f"{SIDE_PK}) ENGINE=InnoDB DEFAULT CHARSET=ascii;", DB)
            _, err = load(e, "b4", sp, "column_id,value,begin_timestamp,archive_id")
            if err:
                line(f"  {c[0]:<8} ! {err}")
                continue
            d, i = tbl_bytes(e, "b4")
            per = (d + i) / a.small
            line(f"  {c[0]:<8}{c[3]:>9}{c[2]:>7}{c[3]:>10}{round(per):>9,}"
                 f"{round(per / c[3]):>11,}   [{e['label']}]")
            sh(e["cmd"], "DROP TABLE b4;", DB)


# --------------------------------------------------------------------------- B5 column_id
def b5(engines, a, out, paths):
    line = section(out, f"B5  column_id width: TINYINT vs SMALLINT ({a.small:,} archives)")
    e = engines[0]
    for name, typ in (("tiny", "TINYINT UNSIGNED"), ("small", "SMALLINT UNSIGNED")):
        cols = SIDE_COLS.replace("TINYINT UNSIGNED", typ)
        sh(e["cmd"], f"DROP TABLE IF EXISTS b5_{name}; CREATE TABLE b5_{name} ({cols}, "
                     f"{SIDE_PK}) ENGINE=InnoDB DEFAULT CHARSET=ascii;", DB)
        _, err = load(e, f"b5_{name}", paths["plain"],
                      "column_id,value,begin_timestamp,archive_id")
        d, i = tbl_bytes(e, f"b5_{name}")
        line(f"  {typ:<20}{round((d + i) / a.small):>8,} B/arch   [{e['label']}]")
        sh(e["cmd"], f"DROP TABLE b5_{name};", DB)
    line("  (design note: numbering column_ids per configuration, resolved through the")
    line("   ID-range fence, keeps TINYINT sufficient indefinitely -- see report text)")


# ------------------------------------------------------------------- B6/B7/B8/B9 (query)
def build_query_corpus(engines, a, _out, tmpdir, seed):
    """One partitioned c8 corpus + base table with extras, shared by B6-B9."""
    import random

    def extras(rng, i):
        # host value (for B9) + three numeric min/max pairs (for B8): each archive covers a
        # narrow slice of a 0..1e9 domain; 1% of archives are wide (cover 10% of it).
        host = val("h0", 12, rng.randrange(1000))
        nums = []
        for _ in range(3):
            lo = rng.randrange(0, 10**9)
            span = 10**8 if rng.random() < 0.01 else rng.randrange(10**4, 10**6)
            nums += [lo, min(lo + span, 10**9)]
        return [host] + nums
    td = tempfile.mkdtemp(dir=tmpdir)
    sp, bp = gen_corpus(a.archives, C8, td, seed, base_extra=extras)
    os.chmod(td, 0o755)
    os.chmod(sp, 0o644)
    os.chmod(bp, 0o644)
    for e in engines:
        sh(e["cmd"], f"DROP DATABASE IF EXISTS {DB}; CREATE DATABASE {DB};")
        sh(e["cmd"], "CREATE TABLE t_base (id INT UNSIGNED NOT NULL, "
                     "begin_timestamp BIGINT NOT NULL, host VARCHAR(64) NOT NULL, "
                     "n1_min BIGINT NOT NULL, n1_max BIGINT NOT NULL, "
                     "n2_min BIGINT NOT NULL, n2_max BIGINT NOT NULL, "
                     "n3_min BIGINT NOT NULL, n3_max BIGINT NOT NULL, "
                     "PRIMARY KEY (id, begin_timestamp), "
                     "KEY ix_host (host, begin_timestamp)) "
                     "ENGINE=InnoDB DEFAULT CHARSET=ascii " + hourly_parts() + ";", DB)
        load(e, "t_base", bp, "id,begin_timestamp,host,n1_min,n1_max,n2_min,n2_max,"
                              "n3_min,n3_max")
        sh(e["cmd"], f"CREATE TABLE t_side ({SIDE_COLS}, {SIDE_PK}) ENGINE=InnoDB "
                     f"DEFAULT CHARSET=ascii " + hourly_parts() + ";", DB)
        load(e, "t_side", sp, "column_id,value,begin_timestamp,archive_id")
    return sp, bp


def windows():
    aa = T0 + (DAYS - 7) * DAY
    return {"1d": (aa + 6 * DAY, aa + 7 * DAY), "7d": (aa, aa + 7 * DAY)}


def b6(engines, a, out):
    line = section(out, f"B6  Filter-only vs filter + join back to the archives table "
                        f"({a.archives:,} archives)")
    line(f"  {'query':<26}{'win':<5}{'filter_ms':>10}{'with_join_ms':>13}{'join_cost':>10}")
    line("  " + "-" * 70)
    probes = [("selective (host eq)", 0, val("h0", 12, 7)),
              ("hot (env eq)", 3, val("e0", 8, 7))]
    for e in engines:
        for pname, cid, v in probes:
            for w, (w1, w2) in windows().items():
                base = (f"FROM t_side s WHERE s.column_id={cid} AND s.value='{v}' AND "
                        f"s.begin_timestamp >= {w1} AND s.begin_timestamp < {w2}")
                f_ms, _, _, _ = timed(e, f"SELECT COUNT(DISTINCT s.archive_id) {base}")
                # The production shape: return archive metadata for matches. The join binds
                # BOTH PK columns (the ts-equality rule), so each match is one point probe.
                j_ms, _, _, _ = timed(
                    e, f"SELECT COUNT(*), MAX(a.n1_max), MIN(a.n1_min) FROM t_side s "
                       f"JOIN t_base a ON a.id = s.archive_id "
                       f"AND a.begin_timestamp = s.begin_timestamp "
                       f"WHERE s.column_id={cid} AND s.value='{v}' "
                       f"AND s.begin_timestamp >= {w1} AND s.begin_timestamp < {w2}")
                if f_ms is None or j_ms is None:
                    continue
                line(f"  {pname:<26}{w:<5}{f_ms:>10,.1f}{j_ms:>13,.1f}"
                     f"{j_ms - f_ms:>+10,.1f}   [{e['label']}]")


def b7(engines, a, out, tmpdir, seed, sp, bp):
    line = section(out, f"B7  Per-dataset tables vs one unified table with dataset_id "
                        f"(3 datasets x {a.archives // 3:,} archives)")
    third = a.archives // 3
    for e in engines:
        # Split: reuse t_side/t_base as dataset 0; two more copies as datasets 1 and 2.
        for k in (1, 2):
            sh(e["cmd"], f"DROP TABLE IF EXISTS t_side_{k}; CREATE TABLE t_side_{k} LIKE "
                         f"t_side; INSERT INTO t_side_{k} SELECT * FROM t_side "
                         f"WHERE archive_id < {third};", DB)
        # Unified: dataset_id leads the PK.
        sh(e["cmd"], "DROP TABLE IF EXISTS t_side_all; CREATE TABLE t_side_all ("
                     "dataset_id TINYINT UNSIGNED NOT NULL, " + SIDE_COLS + ", "
                     "PRIMARY KEY (dataset_id, column_id, value, begin_timestamp, "
                     "archive_id)) ENGINE=InnoDB DEFAULT CHARSET=ascii "
                     + hourly_parts() + ";", DB)
        for k in (0, 1, 2):
            src = "t_side" if k == 0 else f"t_side_{k}"
            lim = "" if k == 0 else f" WHERE archive_id < {third}"
            sh(e["cmd"], f"INSERT INTO t_side_all SELECT {k}, s.* FROM t_side s{lim};", DB)
        v = val("h0", 12, 7)
        w1, w2 = windows()["7d"]
        tw = f"begin_timestamp >= {w1} AND begin_timestamp < {w2}"
        one_split, _, _, _ = timed(e, "SELECT COUNT(DISTINCT archive_id) FROM t_side "
                                      f"WHERE column_id=0 AND value='{v}' AND {tw}")
        one_uni, _, _, _ = timed(e, "SELECT COUNT(DISTINCT archive_id) FROM t_side_all "
                                    f"WHERE dataset_id=0 AND column_id=0 AND value='{v}' "
                                    f"AND {tw}")
        all_split, _, _, _ = timed(
            e, "SELECT SUM(c) FROM (" + " UNION ALL ".join(
                f"SELECT COUNT(DISTINCT archive_id) c FROM {t} WHERE column_id=0 "
                f"AND value='{v}' AND {tw}"
                for t in ("t_side", "t_side_1", "t_side_2")) + ") u")
        all_uni, _, _, _ = timed(e, "SELECT COUNT(DISTINCT dataset_id, archive_id) "
                                    f"FROM t_side_all WHERE dataset_id IN (0,1,2) AND "
                                    f"column_id=0 AND value='{v}' AND {tw}")
        line(f"  [{e['label']}] single-dataset 7d:  split {one_split:,.1f} ms   "
             f"unified {one_uni:,.1f} ms")
        line(f"  [{e['label']}] all-3-datasets 7d:  split(UNION ALL) {all_split:,.1f} ms  "
             f" unified(IN) {all_uni:,.1f} ms")
        for k in (1, 2):
            sh(e["cmd"], f"DROP TABLE IF EXISTS t_side_{k};", DB)
        sh(e["cmd"], "DROP TABLE IF EXISTS t_side_all;", DB)


def b8(engines, a, out):
    line = section(out, f"B8  Numeric range filtering via per-archive min/max columns "
                        f"({a.archives:,} archives; 3 numeric columns = 48 B/archive)")
    line(f"  {'probe':<34}{'win':<5}{'ms':>9}{'hits':>9}{'prune%':>8}")
    line("  " + "-" * 72)
    for e in engines:
        for pname, cond in (
                ("point v inside typical spans", f"{5 * 10**8} BETWEEN n1_min AND n1_max"),
                ("narrow range overlap (1e6 wide)",
                 f"n1_max >= {5 * 10**8} AND n1_min < {5 * 10**8 + 10**6}"),
                ("wide range overlap (1e8 wide)",
                 f"n1_max >= {45 * 10**7} AND n1_min < {55 * 10**7}")):
            for w, (w1, w2) in windows().items():
                ms, hits, _, err = timed(
                    e, f"SELECT COUNT(*) FROM t_base WHERE begin_timestamp >= {w1} AND "
                       f"begin_timestamp < {w2} AND {cond}")
                if err:
                    continue
                cand = a.archives * (1 if w == "7d" else 1 / 7) * 7 / DAYS
                line(f"  {pname:<34}{w:<5}{ms:>9,.1f}{hits:>9,}"
                     f"{100 * (1 - hits / cand):>7.1f}%   [{e['label']}]")


def b9(engines, a, out):
    line = section(out, f"B9  Single-cardinality fast path: plain column + secondary index "
                        f"vs side-table postings ({a.archives:,} archives)")
    line(f"  {'path':<34}{'win':<5}{'ms':>9}{'scanned':>10}")
    line("  " + "-" * 66)
    v = val("h0", 12, 7)
    for e in engines:
        for w, (w1, w2) in windows().items():
            tw = f"begin_timestamp >= {w1} AND begin_timestamp < {w2}"
            m1, _, s1, _ = timed(e, f"SELECT COUNT(*) FROM t_base WHERE host='{v}' AND {tw}")
            m2, _, s2, _ = timed(e, "SELECT COUNT(DISTINCT archive_id) FROM t_side "
                                    f"WHERE column_id=0 AND value='{v}' AND {tw}")
            line(f"  {'archives column + ix_host':<34}{w:<5}{m1:>9,.1f}{s1:>10,}"
                 f"   [{e['label']}]")
            line(f"  {'side-table posting':<34}{w:<5}{m2:>9,.1f}{s2:>10,}"
                 f"   [{e['label']}]")
    d, i = tbl_bytes(engines[0], "t_base")
    line(f"  storage: ix_host secondary index adds ~26 B/archive (measured in B3 split); a")
    line(f"  d1 posting costs ~44 B/archive -- and the plain column itself must exist for")
    line(f"  the archive row anyway, so the fast path saves the posting entirely.")


def b3(engines, a, out):
    line = section(out, "B3  Data vs index share per structure (from the query corpus)")
    line(f"  {'table':<12}{'data_MB':>9}{'index_MB':>10}{'index%':>8}")
    line("  " + "-" * 44)
    for e in engines:
        for tbl in ("t_base", "t_side"):
            d, i = tbl_bytes(e, tbl)
            tot = d + i
            line(f"  {tbl:<12}{d / 1048576:>9,.1f}{i / 1048576:>10,.1f}"
                 f"{100 * i / tot if tot else 0:>7.1f}%   [{e['label']}]")
    line("  (a side table is a clustered primary key with no secondaries: its index IS its")
    line("   data, so index_length ~ 0. inline/base pay secondaries as index_length.)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", action="append", default=[])
    ap.add_argument("--exp", action="append", default=[],
                    choices=["B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9"])
    ap.add_argument("--small", type=int, default=100_000,
                    help="archives for storage experiments (unpartitioned)")
    ap.add_argument("--archives", type=int, default=300_000,
                    help="archives for the partitioned query corpus (B3, B6-B9)")
    ap.add_argument("--skip-kbs4", action="store_true",
                    help="skip the KEY_BLOCK_SIZE=4 variant in B2 (slowest load in the "
                         "suite; loses on every axis at every scale measured)")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default="")
    ap.add_argument("--tmpdir", default=tempfile.gettempdir())
    a = ap.parse_args()
    exps = a.exp or ["B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9"]

    engines = detect(a.engine or DEFAULT_ENGINES)
    if not engines:
        sys.exit("no usable server (needs grants on lc4.*)")
    if a.small < 50_000:
        sys.stderr.write("NOTE: --small below 50K archives gives page-granular size noise; "
                         "treat B1/B2/B4/B5 sizes as smoke only.\n")
    outpath = a.out or f"lc4_results_{time.strftime('%Y%m%d-%H%M%S')}.txt"
    out = open(outpath, "w")
    line = lambda s: (print(s), out.write(s + "\n"))
    line(" ROUND 4  " + time.strftime("%Y-%m-%d %H:%M:%S"))
    for e in engines:
        line(f"   {e['label']:<9} {e['version']}")

    storage_exps = {"B1", "B2", "B4", "B5"} & set(exps)
    paths = {}
    tmp_dirs = []
    if storage_exps:
        td = tempfile.mkdtemp(dir=a.tmpdir)
        tmp_dirs.append(td)
        sys.stderr.write("generating storage corpora ...\n")
        paths["plain"], _ = gen_corpus(a.small, C8, td, a.seed)
        os.rename(paths["plain"], os.path.join(td, "plain.tsv"))
        paths["plain"] = os.path.join(td, "plain.tsv")
        if "B1" in exps:
            paths["mb"], _ = gen_corpus(a.small, C8, td, a.seed, mb=True)
        os.chmod(td, 0o755)
        for p in paths.values():
            os.chmod(p, 0o644)
        for e in engines:
            sh(e["cmd"], f"DROP DATABASE IF EXISTS {DB}; CREATE DATABASE {DB};")
        if "B1" in exps:
            b1(engines, a, out, paths)
        if "B2" in exps:
            b2(engines, a, out, paths)
        if "B4" in exps:
            b4(engines, a, out, a.tmpdir, a.seed)
        if "B5" in exps:
            b5(engines, a, out, paths)
    query_exps = {"B3", "B6", "B7", "B8", "B9"} & set(exps)
    if query_exps:
        sys.stderr.write("building query corpus ...\n")
        sp, bp = build_query_corpus(engines, a, out, a.tmpdir, a.seed)
        tmp_dirs.append(os.path.dirname(sp))
        if "B6" in exps:
            b6(engines, a, out)
        if "B7" in exps:
            b7(engines, a, out, a.tmpdir, a.seed, sp, bp)
        if "B8" in exps:
            b8(engines, a, out)
        if "B9" in exps:
            b9(engines, a, out)
        if "B3" in exps:
            b3(engines, a, out)
    for e in engines:
        sh(e["cmd"], f"DROP DATABASE IF EXISTS {DB};")
    import shutil
    for td in tmp_dirs:
        shutil.rmtree(td, ignore_errors=True)
    line(f"\nwritten to {outpath}")
    out.close()


if __name__ == "__main__":
    main()
