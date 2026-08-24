#!/usr/bin/env python3
"""Where does dictionary-encoding the side table's value stop paying for itself?

`bench_dict.py` showed dictionary encoding wins at the c8 shape, but c8 mixes eight columns
with repetition rates from 60 to 15,000, so it cannot locate a break-even. This isolates the
two parameters that decide it, one synthetic column at a time.

THE MODEL. Per column, with W the mean value width, R the postings per distinct value, k the
id width, A the fixed bytes per posting row (other columns plus InnoDB per-row overhead), B
the fixed bytes per dictionary row, and f the page fill factor:

    plain = (A + W)/f
    dict  = (A + k)/f + (B + W)/(R*f)

Setting them equal, f cancels (same engine, same fill) and A cancels (same columns), leaving

    W* = (k*R + B) / (R - 1)

so the break-even width depends ONLY on the id width and the repetition rate. It can never
fall below k -- with unbounded repetition the dictionary is free, so the dictionary wins as
soon as a value is wider than its id -- and it diverges as R approaches 1, where every value
is stored once either way and the dictionary is pure overhead.

WHAT THE MODEL CANNOT PREDICT. Page compression already removes repeated value bytes, so it
competes for the same redundancy the dictionary targets. The effective width under
compression is whatever zlib achieves inside an 8 KB block, which is not a closed form. That
interaction is the reason this sweep exists.

Usage:
    python3 bench_dict_breakeven.py --engine mariadb=mysql
    python3 bench_dict_breakeven.py --rows 1500000 --widths 8,16,32,64 --reps 2,8,40,800
"""
import argparse
import os
import shlex
import subprocess
import sys
import tempfile

DB = "dictbe"
K_ID = 4          # INT value_id, matching bench_dict.py
B_DICT = 30       # fitted fixed bytes per dictionary row; only shifts W* at small R


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


def detect(spec):
    label, _, cmd = spec.partition("=")
    if not cmd:
        label, cmd = os.path.basename(shlex.split(spec)[0]), spec
    e = {"label": label, "cmd": cmd}
    rc, out, err = sh(e, "SELECT VERSION();")
    if rc != 0:
        sys.stderr.write(f"cannot reach {label}: {err.strip().splitlines()[-1][:70]}\n")
        return None
    e["version"] = out.strip().splitlines()[-1]
    e["flavour"] = "mariadb" if "mariadb" in e["version"].lower() else "mysql"
    return e


def size_mb(e, tbl):
    """information_schema serves CACHED statistics; without ANALYZE it reads stale."""
    sh(e, f"ANALYZE TABLE {tbl};", DB)
    pre = "SET SESSION information_schema_stats_expiry=0;\n" if e["flavour"] == "mysql" else ""
    _, out, _ = sh(e, pre + "SELECT ROUND((data_length+index_length)/1048576, 2) "
                            f"FROM information_schema.tables WHERE table_schema='{DB}' "
                            f"AND table_name='{tbl}';", DB)
    for ln in out.splitlines():
        t = ln.strip()
        if t and (t[0].isdigit()):
            return float(t)
    return 0.0


def value_of(v, width):
    """Zero-padded decimal, so the encoding is EXACTLY `width` bytes and stays injective.

    An earlier version built "v<n>" then truncated to width, which silently collapsed
    distinct values whenever width was too narrow to hold the index: at width 4 only ~1,000
    strings exist, so a cell asking for 600,000 distinct values gave the plain table far
    more repetition than requested and made the dictionary's own PRIMARY KEY drop rows as
    duplicates. Every W=4 cell in that run was meaningless. `fits` below now refuses such a
    cell instead of reporting a number for it."""
    return str(v).zfill(width)


def fits(distinct, width):
    return len(str(max(0, distinct - 1))) <= width


def generate(fp, fd, fm, rows, width, distinct):
    """One column, `rows` postings drawn round-robin over `distinct` values, written once
    with the value inline and once as a dictionary id. Round-robin rather than random so
    every value appears exactly R times and the repetition rate is exact, not sampled."""
    t0 = 1704067200
    with open(fp, "w") as p, open(fd, "w") as d:
        for i in range(rows):
            v = i % distinct
            ts = t0 + (i % 4032) * 3600
            p.write(f"0\t0\t{value_of(v, width)}\t{ts}\t{i}\n")
            d.write(f"0\t0\t{v}\t{ts}\t{i}\n")
    with open(fm, "w") as m:
        for v in range(distinct):
            m.write(f"0\t0\t{value_of(v, width)}\t{v}\n")


PLAIN = ("dataset_id SMALLINT UNSIGNED NOT NULL, column_id TINYINT UNSIGNED NOT NULL, "
         "value VARBINARY(255) NOT NULL, begin_timestamp BIGINT NOT NULL, "
         "archive_id INT UNSIGNED NOT NULL")
DICT = ("dataset_id SMALLINT UNSIGNED NOT NULL, column_id TINYINT UNSIGNED NOT NULL, "
        "value_id INT UNSIGNED NOT NULL, begin_timestamp BIGINT NOT NULL, "
        "archive_id INT UNSIGNED NOT NULL")
MAP = ("dataset_id SMALLINT UNSIGNED NOT NULL, column_id TINYINT UNSIGNED NOT NULL, "
       "value VARBINARY(255) NOT NULL, value_id INT UNSIGNED NOT NULL")
PK_P = "dataset_id, column_id, value, begin_timestamp, archive_id"
PK_D = "dataset_id, column_id, value_id, begin_timestamp, archive_id"
KBS8 = "ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8"


def load(e, name, cols, pk, tsv, extra=""):
    sh(e, f"DROP TABLE IF EXISTS {name};", DB)
    sh(e, f"CREATE TABLE {name} ({cols}, PRIMARY KEY ({pk})) ENGINE=InnoDB {extra};", DB)
    rc, _, err = sh(e, f"LOAD DATA LOCAL INFILE '{tsv}' INTO TABLE {name};", DB)
    if rc != 0:
        sys.stderr.write(f"  load failed {name}: {err.strip().splitlines()[-1][:70]}\n")
        return False
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", default="mariadb=mysql")
    ap.add_argument("--rows", type=int, default=1500000)
    ap.add_argument("--widths", default="8,16,32,64")
    ap.add_argument("--reps", default="2,8,40,800")
    a = ap.parse_args()
    e = detect(a.engine)
    if not e:
        return 1
    widths = [int(x) for x in a.widths.split(",")]
    reps = [int(x) for x in a.reps.split(",")]
    print(f"engine {e['label']} {e['version']}")
    print(f"{a.rows:,} postings per cell, one synthetic column, INT value_id\n")
    print(f"  model: W* = (k*R + B)/(R - 1) with k={K_ID}, B={B_DICT}\n")
    sh(e, f"CREATE DATABASE IF NOT EXISTS {DB};")

    print(f"  {'R':>6} {'W*pred':>7} | {'W':>4} {'plain':>8} {'dict':>8} {'delta':>8}"
          f" | {'plain+z':>8} {'dict+z':>8} {'delta':>8}")
    print("  " + "-" * 84)
    tmp = tempfile.mkdtemp(prefix="dbe_")
    try:
        for R in reps:
            distinct = max(1, a.rows // R)
            wstar = (K_ID * R + B_DICT) / (R - 1) if R > 1 else float("inf")
            for w in widths:
                if not fits(distinct, w):
                    print(f"  {R:>6} {wstar:>7.1f} | {w:>4}   skipped: {w} bytes cannot hold "
                          f"{distinct:,} distinct values injectively")
                    continue
                fp = os.path.join(tmp, "p.tsv")
                fd = os.path.join(tmp, "d.tsv")
                fm = os.path.join(tmp, "m.tsv")
                generate(fp, fd, fm, a.rows, w, distinct)
                for f in (fp, fd, fm):
                    os.chmod(f, 0o644)
                cells = []
                for extra in ("", KBS8):
                    mp = md = 0.0
                    if load(e, "t", PLAIN, PK_P, fp, extra):
                        mp = size_mb(e, "t")
                    if load(e, "t", DICT, PK_D, fd, extra):
                        md = size_mb(e, "t")
                        # the dictionary is part of the cost, in the same row format
                        if load(e, "m", MAP, "dataset_id, column_id, value", fm, extra):
                            md += size_mb(e, "m")
                        sh(e, "DROP TABLE IF EXISTS m;", DB)
                    sh(e, "DROP TABLE IF EXISTS t;", DB)
                    cells.append((mp, md))
                (up, ud), (cp, cd) = cells
                du = f"{(ud / up - 1) * 100:+.1f}%" if up else "--"
                dc = f"{(cd / cp - 1) * 100:+.1f}%" if cp else "--"
                ws = f"{wstar:.1f}" if wstar != float("inf") else "inf"
                print(f"  {R:>6} {ws:>7} | {w:>4} {up:>8.1f} {ud:>8.1f} {du:>8}"
                      f" | {cp:>8.1f} {cd:>8.1f} {dc:>8}")
            print()
    finally:
        for f in os.listdir(tmp):
            os.unlink(os.path.join(tmp, f))
        os.rmdir(tmp)
    print("  A negative delta means the dictionary is smaller. The model predicts the")
    print("  UNCOMPRESSED sign flip at W = W*; the compressed columns have no closed form,")
    print("  because page compression competes for the same redundancy.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
