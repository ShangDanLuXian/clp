#!/usr/bin/env python3
"""Weighs the REAL side table -- the hourly-partitioned DDL from the design -- built once
per value encoding, so the ascii-vs-utf8mb4 question is answered on the structure we would
actually ship rather than on a bare table.

This matters because bench4's B1 measured unpartitioned tables. Partitioning turns one
B-tree into one per hour, and per-partition overhead is charged whatever the charset is, so
the comparison has to be made inside that structure to be worth anything.

Each variant is loaded with identical postings and reported as data_length (the clustered
PK -- in InnoDB the table IS its primary key) plus index_length (secondary indexes, always
zero here: the PK is the inverted index).

  python3 check_side_charset.py
  python3 check_side_charset.py --archives 500000 --hours 336
  python3 check_side_charset.py --engine mysql="/opt/mysql8/usr/bin/mysql \\
      --defaults-file=/etc/my8.cnf -u root"
"""
import argparse
import os
import shlex
import subprocess
import sys
import tempfile
import time

DB = "sidecharset"
T0 = 1704067200 * 10**9
HOUR = 3600 * 10**9

# (tag, value width in characters, postings per archive, distinct values) -- the c8 shape
# used by bench4, so these numbers line up with its B1 and B4.
COLS = [("h0", 12, 1, 1000), ("h1", 12, 1, 1000), ("h2", 12, 1, 1000),
        ("e0", 8, 3, 12), ("e1", 8, 3, 12),
        ("s0", 10, 8, 40), ("s1", 10, 8, 40),
        ("m0", 14, 20, 300)]
POSTINGS = sum(c[2] for c in COLS)

# label, value column type, table charset clause, which corpus
VARIANTS = [
    ("ascii", "VARCHAR(64)", "DEFAULT CHARSET = ascii", "ascii"),
    ("utf8mb4", "VARCHAR(64)", "DEFAULT CHARSET = utf8mb4", "ascii"),
    ("VARBINARY", "VARBINARY(255)", "", "ascii"),
    ("ascii", "VARCHAR(64)", "DEFAULT CHARSET = ascii", "20% CJK"),
    ("utf8mb4", "VARCHAR(64)", "DEFAULT CHARSET = utf8mb4", "20% CJK"),
    ("VARBINARY", "VARBINARY(255)", "", "20% CJK"),
]


def sh(e, sql, db=None, local_infile=False):
    cmd = shlex.split(e["cmd"])
    if local_infile:
        cmd.append("--local-infile=1")
    if db:
        cmd.append(db)
    try:
        p = subprocess.run(cmd + ["-e", sql], capture_output=True, text=True, timeout=7200)
    except OSError as ex:
        return 127, "", str(ex)
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
            sys.stderr.write(f"  skip {label}: {err.strip().splitlines()[-1][:70]}\n"
                             f"    sudo mysql -e \"GRANT ALL PRIVILEGES ON {DB}.* TO "
                             f"'{os.environ.get('USER', 'you')}'@'localhost';\"\n")
            continue
        seen.add(ident)
        e["version"] = ident.rsplit("|", 1)[-1]
        e["flavour"] = "mariadb" if "mariadb" in e["version"].lower() else "mysql"
        out.append(e)
    return out


def parts(hours):
    """The design's partitioning: a floor, one partition per hour, and a MAXVALUE catch-all."""
    p = [f"PARTITION p_floor VALUES LESS THAN ({T0})"]
    p += [f"PARTITION p_h{h:06d} VALUES LESS THAN ({T0 + (h + 1) * HOUR})" for h in range(hours)]
    p.append("PARTITION p_future VALUES LESS THAN MAXVALUE")
    return "PARTITION BY RANGE (begin_timestamp) (" + ",".join(p) + ")"


def val(tag, width, i, mb=False):
    """Exactly `width` characters; with mb the last two are CJK, so the same character count
    costs 4 more bytes. The tag stays intact so columns sharing a pool shape cannot collide."""
    s = (tag + str(i)).ljust(width, "x")[:width]
    return s[:-2] + "中文" if mb else s


def gen(archives, hours, path, mb):
    import random
    rng = random.Random(7)
    pools = {c[0]: [val(c[0], c[1], i, mb and i % 5 == 0) for i in range(c[3])] for c in COLS}
    span = max(1, hours * HOUR // archives)
    with open(path, "w") as f:
        for i in range(archives):
            ts = T0 + i * span
            for n, c in enumerate(COLS):
                for v in rng.sample(pools[c[0]], c[2]):
                    f.write(f"{n}\t{v}\t{ts}\t{i}\n")


def sizes(e, tbl):
    """information_schema serves CACHED statistics; without ANALYZE a freshly loaded table
    reads far too small (~100x observed). MySQL additionally caches the row itself."""
    sh(e, f"ANALYZE TABLE {tbl};", DB)
    fresh = "SET SESSION information_schema_stats_expiry=0;\n" if e["flavour"] == "mysql" else ""
    _, out, _ = sh(e, fresh + "SELECT COALESCE(data_length,0), COALESCE(index_length,0) "
                              f"FROM information_schema.tables "
                              f"WHERE table_schema='{DB}' AND table_name='{tbl}';", DB)
    nums = [int(x) for x in out.split() if x.isdigit()]
    return (nums[-2], nums[-1]) if len(nums) >= 2 else (0, 0)


def run(e, a, line, corpora, partitioned):
    tag = f"hourly-partitioned ({a.hours} partitions)" if partitioned else "unpartitioned"
    line("")
    line(f"  {tag}")
    line(f"    {'charset':<12}{'content':<10}{'data_MB':>10}{'index_MB':>10}{'B/arch':>9}"
         f"{'B/posting':>11}{'vs ascii':>10}  note")
    line("    " + "-" * 88)
    base = None
    for label, vtype, charset, content in VARIANTS:
        pdef = ", " + parts(a.hours) if partitioned else ""
        rc, _, err = sh(e, f"DROP TABLE IF EXISTS s; CREATE TABLE s ("
                           f"column_id TINYINT UNSIGNED NOT NULL, value {vtype} NOT NULL, "
                           f"begin_timestamp BIGINT NOT NULL, archive_id INT UNSIGNED NOT NULL, "
                           f"PRIMARY KEY (column_id, value, begin_timestamp, archive_id)"
                           f") ENGINE = InnoDB {charset}{pdef};", DB)
        if rc != 0:
            line(f"    {label:<12}{content:<10} ! {(err.strip().splitlines() or [''])[-1][:50]}")
            continue
        sh(e, f"LOAD DATA LOCAL INFILE '{corpora[content]}' INTO TABLE s "
              f"(column_id,value,begin_timestamp,archive_id);", DB, True)
        d, i = sizes(e, "s")
        tot = d + i
        if content == "ascii" and label == "ascii":
            base = tot
        ratio = f"{tot / base:.3f}x" if base else "-"
        # Generated values never contain '?' (0x3F); any that do were substituted by the
        # server because the charset could not represent them.
        _, out, _ = sh(e, "SELECT COUNT(*) FROM s WHERE value LIKE '%?%';", DB)
        bad = int(([x for x in out.split() if x.isdigit()] or [0])[-1])
        note = f"{100 * bad / (a.archives * POSTINGS):.0f}% CORRUPTED" if bad else ""
        line(f"    {label:<12}{content:<10}{d / 1048576:>10,.1f}{i / 1048576:>10,.1f}"
             f"{tot / a.archives:>9,.0f}{tot / a.archives / POSTINGS:>11.1f}{ratio:>10}  {note}")
        sh(e, "DROP TABLE s;", DB)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", action="append", default=[])
    ap.add_argument("--archives", type=int, default=200_000)
    ap.add_argument("--hours", type=int, default=168, help="hourly partitions (168 = 7 days)")
    ap.add_argument("--tmpdir", default=tempfile.gettempdir())
    ap.add_argument("--out", default="")
    a = ap.parse_args()

    engines = detect(a.engine or ["mariadb=mysql", "mariadb=mariadb",
                                  "mysql=/opt/mysql8/usr/bin/mysql "
                                  "--defaults-file=/etc/my8.cnf -u root"])
    if not engines:
        sys.exit(f"no usable server (needs GRANT ALL ON {DB}.*)")

    outpath = a.out or f"side_charset_{time.strftime('%Y%m%d-%H%M%S')}.txt"
    out = open(outpath, "w")

    def line(s):
        print(s)
        out.write(s + "\n")

    td = tempfile.mkdtemp(dir=a.tmpdir)
    sys.stderr.write(f"generating {a.archives * POSTINGS:,} postings ...\n")
    corpora = {}
    for content, mb in (("ascii", False), ("20% CJK", True)):
        p = os.path.join(td, f"{'mb' if mb else 'plain'}.tsv")
        gen(a.archives, a.hours, p, mb)
        os.chmod(p, 0o644)
        corpora[content] = p
    os.chmod(td, 0o755)

    line(f" SIDE TABLE CHARSET COMPARISON  {time.strftime('%Y-%m-%d %H:%M:%S')}")
    line(f"   {a.archives:,} archives x {POSTINGS} postings = "
         f"{a.archives * POSTINGS:,} rows, spread over {a.hours} hours")
    for e in engines:
        line("")
        line("=" * 96)
        line(f" {e['label']}  {e['version']}")
        line("=" * 96)
        run(e, a, line, corpora, True)
        run(e, a, line, corpora, False)
        sh(e, f"DROP DATABASE IF EXISTS {DB};")

    import shutil
    shutil.rmtree(td, ignore_errors=True)
    line(f"\nwritten to {outpath}")
    out.close()


if __name__ == "__main__":
    main()
