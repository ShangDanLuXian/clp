#!/usr/bin/env python3
"""Validates the three independent claims about the side table's value column.

These get conflated constantly, because two of them are quoted in bytes:

  A. DECLARED ceiling -- how wide a value each encoding lets you declare. InnoDB caps a
     whole index key at 3072 bytes and charges a string column
     `declared_length x charset_max_bytes_per_char`, so utf8mb4 costs 4x per declared
     character. Checked once at CREATE TABLE; allocates nothing.
  B. STORED size -- how many bytes each encoding actually costs on disk. Independent of
     A: records hold the bytes present, never the declared maximum.
  C. FIDELITY -- what each encoding does to bytes that are not valid UTF-8, pushed
     through LOAD DATA because that is how postings really arrive.

Usage:
    python3 bench_valuetype.py
    python3 bench_valuetype.py --archives 500000 \\
        --engine mariadb=mysql \\
        --engine mysql="/opt/mysql8/usr/bin/mysql --defaults-file=/etc/my8.cnf -u root"
"""
import argparse
import os
import shlex
import subprocess
import sys
import tempfile
import time

DB = "vtcheck"
KEY_LIMIT = 3072
# The PK columns other than the value; they are charged against the same 3072 budget.
# column_id TINYINT + begin_timestamp BIGINT + archive_id INT UNSIGNED
PK_OTHER = 1 + 8 + 4

# label, CREATE TABLE fragment with {n}, max bytes per declared unit
ENCODINGS = [
    ("VARCHAR ascii", "VARCHAR({n}) CHARACTER SET ascii", 1),
    ("VARCHAR utf8mb3", "VARCHAR({n}) CHARACTER SET utf8mb3", 3),
    ("VARCHAR utf8mb4", "VARCHAR({n}) CHARACTER SET utf8mb4", 4),
    ("VARBINARY", "VARBINARY({n})", 1),
]

# One archive's postings: (tag, value width in characters, postings/archive, distinct values).
# Mirrors the c8 shape used by bench4.py so numbers are directly comparable to its B1.
COLS = [("h0", 12, 1, 1000), ("h1", 12, 1, 1000), ("h2", 12, 1, 1000),
        ("e0", 8, 3, 12), ("e1", 8, 3, 12),
        ("s0", 10, 8, 40), ("s1", 10, 8, 40),
        ("m0", 14, 20, 300)]
POSTINGS = sum(c[2] for c in COLS)
DISTINCT = sum(c[3] for c in COLS)

# label, value column definition, which corpus to load into it
STORAGE = [
    ("VARCHAR ascii", "VARCHAR(64) CHARACTER SET ascii", "ascii"),
    ("VARCHAR utf8mb4", "VARCHAR(64) CHARACTER SET utf8mb4", "ascii"),
    ("VARBINARY", "VARBINARY(255)", "ascii"),
    ("VARCHAR ascii", "VARCHAR(64) CHARACTER SET ascii", "20% CJK"),
    ("VARCHAR utf8mb4", "VARCHAR(64) CHARACTER SET utf8mb4", "20% CJK"),
    ("VARBINARY", "VARBINARY(255)", "20% CJK"),
]

# name, bytes written to the TSV. Two of these are deliberately not valid UTF-8.
PROBES = [("plain ascii", b"host-a"),
          ("valid CJK", "中".encode()),
          ("truncated multibyte", b"\xe4\xb8"),
          ("lone 0xff", b"\xff")]


def sh(e, sql, db=None, local_infile=False):
    cmd = shlex.split(e["cmd"])
    if local_infile:
        cmd.append("--local-infile=1")
    if db:
        cmd.append(db)
    try:
        p = subprocess.run(cmd + ["-e", sql], capture_output=True, text=True, timeout=7200)
    except OSError as ex:                  # a default engine path that is simply not installed
        return 127, "", str(ex)
    except subprocess.TimeoutExpired:
        return 124, "", "timed out"
    return p.returncode, p.stdout, p.stderr


def detect(specs):
    """Resolves --engine specs to working clients, reporting why each rejected one failed.

    The default list names both client binaries, since MariaDB 11 ships only `mariadb` and
    older packages ship only `mysql`. Where both exist they usually reach the SAME server,
    so servers are de-duplicated by their socket/port -- otherwise every table would be
    built twice and the report would show one engine as two."""
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
        rc, _, err = sh(e, f"CREATE DATABASE IF NOT EXISTS {DB};")
        if rc != 0:
            sys.stderr.write(f"  skip {label}: cannot create {DB}. Grant it with:\n"
                             f"    GRANT ALL PRIVILEGES ON {DB}.* TO CURRENT_USER();\n")
            continue
        seen.add(ident)
        e["version"] = ident.rsplit("|", 1)[-1]
        e["flavour"] = "mariadb" if "mariadb" in e["version"].lower() else "mysql"
        out.append(e)
    return out


def tbl_bytes(e, tbl):
    """Total bytes for a table. information_schema serves CACHED statistics: without the
    ANALYZE it reports a stale figure (observed ~100x low on a freshly loaded table), and
    MySQL additionally caches the row for information_schema_stats_expiry seconds."""
    sh(e, f"ANALYZE TABLE {tbl};", DB)
    fresh = "SET SESSION information_schema_stats_expiry=0;\n" if e["flavour"] == "mysql" else ""
    _, out, _ = sh(e, fresh + "SELECT COALESCE(data_length,0) + COALESCE(index_length,0) "
                              f"FROM information_schema.tables "
                              f"WHERE table_schema='{DB}' AND table_name='{tbl}';", DB)
    nums = [int(x) for x in out.split() if x.isdigit()]
    return nums[-1] if nums else 0


def val(tag, width, i, mb=False):
    """A value of exactly `width` CHARACTERS. With mb the LAST two are CJK, so the same
    character count occupies 4 more bytes -- precisely the effect under test.

    The CJK goes at the end on purpose: overwriting the leading tag would make h0/h1/h2
    (which share a value pool shape) generate identical strings, and the collision would
    then look like an encoding fault in the distinct-value check below."""
    s = (tag + str(i)).ljust(width, "x")[:width]
    return s[:-2] + "中文" if mb else s


def create(e, coldef):
    """Returns (ok, error). Only ERROR 1071 counts as 'key too long'; anything else is a bug."""
    rc, _, err = sh(e, f"DROP TABLE IF EXISTS vt; CREATE TABLE vt ("
                       f"column_id TINYINT UNSIGNED NOT NULL, value {coldef} NOT NULL, "
                       f"begin_timestamp BIGINT NOT NULL, archive_id INT UNSIGNED NOT NULL, "
                       f"PRIMARY KEY (column_id, value, begin_timestamp, archive_id)"
                       f") ENGINE=InnoDB;", DB)
    return rc == 0, (err.strip().splitlines() or [""])[-1][:60]


def part_a(e, line):
    """Binary-searches the widest declaration each encoding accepts inside the real PK."""
    line(f"  {'encoding':<18}{'bytes/unit':>11}{'predicted':>11}{'measured':>10}  verdict")
    line("  " + "-" * 62)
    for label, tmpl, mbmax in ENCODINGS:
        predicted = (KEY_LIMIT - PK_OTHER) // mbmax
        ok, err = create(e, tmpl.format(n=1))
        if not ok:
            line(f"  {label:<18}{mbmax:>11}{predicted:>11}{'n/a':>10}  {err}")
            continue
        lo, hi = 1, 4001                   # lo always creates, hi never does
        while hi - lo > 1:
            mid = (lo + hi) // 2
            ok, _ = create(e, tmpl.format(n=mid))
            if ok:
                lo = mid
            else:
                hi = mid
        mark = "matches" if lo == predicted else f"MISMATCH (expected {predicted})"
        line(f"  {label:<18}{mbmax:>11}{predicted:>11}{lo:>10}  {mark}")
    sh(e, "DROP TABLE IF EXISTS vt;", DB)
    line("")
    line(f"  predicted = (key limit {KEY_LIMIT} - {PK_OTHER} for the other 3 PK "
         f"columns) / bytes-per-unit")


def gen(archives, path, mb):
    """Writes the posting corpus. Every archive contributes the same shape."""
    import random
    rng = random.Random(7)
    pools = {c[0]: [val(c[0], c[1], i, mb and i % 5 == 0) for i in range(c[3])] for c in COLS}
    with open(path, "w") as f:
        for i in range(archives):
            ts = 1704067200 * 10**9 + i * 10**9
            for n, c in enumerate(COLS):
                for v in rng.sample(pools[c[0]], c[2]):
                    f.write(f"{n}\t{v}\t{ts}\t{i}\n")


def part_b(e, line, archives, corpora):
    line(f"  {'value type':<18}{'content':<10}{'B/arch':>9}{'B/posting':>11}"
         f"{'distinct':>10}{'mangled':>10}{'load_s':>8}")
    line("  " + "-" * 78)
    for label, coldef, content in STORAGE:
        ok, err = create(e, coldef)
        if not ok:
            line(f"  {label:<18}{content:<10} ! {err}")
            continue
        t0 = time.time()
        rc, _, err = sh(e, f"LOAD DATA LOCAL INFILE '{corpora[content]}' INTO TABLE vt "
                           f"(column_id,value,begin_timestamp,archive_id);", DB, True)
        if rc != 0:
            line(f"  {label:<18}{content:<10} ! {(err.strip().splitlines() or [''])[-1][:50]}")
            continue
        el = time.time() - t0
        by = tbl_bytes(e, "vt")
        _, out, _ = sh(e, "SELECT COUNT(DISTINCT value) FROM vt;", DB)
        dis = int([x for x in out.split() if x.isdigit()][-1])
        # The generated values never contain '?' (0x3F), so any row holding one is a value
        # the server could not represent and substituted. This is the detector that matters:
        # a mangled row is still distinct and still the same size, so neither of the other
        # columns can reveal it -- ascii + CJK otherwise reads as a free win.
        _, out, _ = sh(e, "SELECT COUNT(*) FROM vt WHERE value LIKE '%?%';", DB)
        bad = int([x for x in out.split() if x.isdigit()][-1])
        flag = ""
        if dis != DISTINCT:
            flag += f"  <- collapsed {DISTINCT - dis} values"
        if bad:
            flag += f"  <- {100 * bad / (archives * POSTINGS):.0f}% OF POSTINGS CORRUPTED"
        line(f"  {label:<18}{content:<10}{round(by / archives):>9,}"
             f"{by / archives / POSTINGS:>11.1f}{dis:>10,}{bad:>10,}{el:>8.1f}{flag}")
    sh(e, "DROP TABLE IF EXISTS vt;", DB)


def part_c(e, line, tmpdir):
    line(f"  {'value type':<18}{'probe':<22}{'written':<16}{'stored':<16}verdict")
    line("  " + "-" * 84)
    for label, coldef, _ in STORAGE[:3]:
        sh(e, f"DROP TABLE IF EXISTS vf; CREATE TABLE vf (v {coldef.replace('(64)', '(64)')} "
              f"NOT NULL) ENGINE=InnoDB;", DB)
        for pname, raw in PROBES:
            path = os.path.join(tmpdir, "probe.tsv")
            with open(path, "wb") as f:
                f.write(raw + b"\n")
            os.chmod(path, 0o644)
            sh(e, "DELETE FROM vf;", DB)
            sh(e, f"SET SESSION sql_mode=''; LOAD DATA LOCAL INFILE '{path}' "
                  f"INTO TABLE vf (v);", DB, True)
            _, out, _ = sh(e, "SELECT IFNULL(HEX(v),'<none>') FROM vf LIMIT 1;", DB)
            got = (out.strip().splitlines() or ["<none>"])[-1]
            want = raw.hex().upper()
            line(f"  {label:<18}{pname:<22}{want:<16}{got:<16}"
                 f"{'exact' if got == want else 'MANGLED'}")
    sh(e, "DROP TABLE IF EXISTS vf;", DB)
    line("")
    line("  (sql_mode is cleared above; LOAD DATA converts rather than errors in BOTH modes,")
    line("   so there is no setting that turns a mangled value into a failed load)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", action="append", default=[])
    ap.add_argument("--archives", type=int, default=200_000,
                    help="archives for the storage test (below ~50K the sizes are page noise)")
    ap.add_argument("--tmpdir", default=tempfile.gettempdir())
    ap.add_argument("--out", default="")
    ap.add_argument("--skip-storage", action="store_true",
                    help="run only the key-ceiling and fidelity checks (no data loaded)")
    a = ap.parse_args()

    defaults = ["mariadb=mysql", "mariadb2=mariadb",
                "mysql=/opt/mysql8/usr/bin/mysql --defaults-file=/etc/my8.cnf -u root"]
    engines = detect(a.engine or defaults)
    if not engines:
        sys.exit(f"no usable server (needs GRANT ALL ON {DB}.* and a client on PATH)")

    outpath = a.out or f"valuetype_{time.strftime('%Y%m%d-%H%M%S')}.txt"
    out = open(outpath, "w")

    def line(s):
        print(s)
        out.write(s + "\n")

    line(f" VALUE COLUMN TYPE CHECK  {time.strftime('%Y-%m-%d %H:%M:%S')}")
    for e in engines:
        line(f"   {e['label']:<10} {e['version']}")

    td = tempfile.mkdtemp(dir=a.tmpdir)
    corpora = {}
    if not a.skip_storage:
        sys.stderr.write(f"generating {a.archives * POSTINGS:,} postings ...\n")
        for content, mb in (("ascii", False), ("20% CJK", True)):
            p = os.path.join(td, f"{'mb' if mb else 'plain'}.tsv")
            gen(a.archives, p, mb)
            os.chmod(p, 0o644)
            corpora[content] = p
    os.chmod(td, 0o755)

    for e in engines:
        line("")
        line("=" * 92)
        line(f" A  DECLARED ceiling: widest value each encoding allows in the PK  [{e['label']}]")
        line("=" * 92)
        part_a(e, line)
        if not a.skip_storage:
            line("")
            line("=" * 92)
            line(f" B  STORED size: what each encoding actually costs "
                 f"({a.archives:,} archives, {POSTINGS} postings/archive)  [{e['label']}]")
            line("=" * 92)
            part_b(e, line, a.archives, corpora)
        line("")
        line("=" * 92)
        line(f" C  FIDELITY: what each encoding does to non-UTF-8 bytes via LOAD DATA "
             f"[{e['label']}]")
        line("=" * 92)
        part_c(e, line, td)
        sh(e, f"DROP DATABASE IF EXISTS {DB};")

    import shutil
    shutil.rmtree(td, ignore_errors=True)
    line(f"\nwritten to {outpath}")
    out.close()


if __name__ == "__main__":
    main()
