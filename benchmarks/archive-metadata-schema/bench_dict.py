#!/usr/bin/env python3
"""Does dictionary-encoding the side table's value column pay for itself?

The side table stores one row per (column, value, archive) posting, and the value bytes are
repeated in every posting that carries that value. Within one dataset a column draws from a
small, stable set, so the same bytes are stored hundreds of times. The alternative is a
per-(dataset, column) dictionary mapping value -> value_id, with the side table holding the
fixed-width id:

    plain   PRIMARY KEY (dataset_id, column_id, value,    begin_timestamp, archive_id)
    dict    PRIMARY KEY (dataset_id, column_id, value_id, begin_timestamp, archive_id)
            + dict(dataset_id, column_id, value) -> value_id

Estimating this from the existing run is not possible: stored bytes per row is
(payload + per-row overhead) / page fill factor, and one measurement cannot separate the
overhead from the fill factor. So both schemas are built with identical logical data and
measured.

Three things are swept, because the answer depends on all of them:
  - VALUE WIDTH. The saving is bounded by what fraction of the row the value occupies, and
    that fraction is small at c8's ~12 B and large at the 64-100 B values real deployments
    carry (pod names, URLs, error strings).
  - COMPRESSION. The primary key sorts by value, so identical values are ADJACENT and a
    page holds few distinct ones. Page compression should already squeeze most of the
    redundancy that dictionary encoding targets, which would make the two largely
    substitutes rather than additive.
  - CARDINALITY. Fewer distinct values means more repetition to eliminate.

Usage:
    python3 bench_dict.py --engine mariadb=mysql
    python3 bench_dict.py --archives 200000 --widths 12,40,100
"""
import argparse
import os
import random
import shlex
import subprocess
import sys
import tempfile
import time

DB = "dicttest"
# (tag, postings per archive, distinct values) -- the c8 shape, widths swept separately.
COLS = [("h0", 1, 1000), ("h1", 1, 1000), ("h2", 1, 1000),
        ("e0", 3, 12), ("e1", 3, 12),
        ("s0", 8, 40), ("s1", 8, 40),
        ("m0", 20, 300)]
POSTINGS = sum(c[1] for c in COLS)
DISTINCT = sum(c[2] for c in COLS)


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
    """information_schema serves CACHED statistics; without ANALYZE it reports a stale
    figure, observed ~100x low on a freshly loaded table in an earlier round."""
    sh(e, f"ANALYZE TABLE {tbl};", DB)
    pre = "SET SESSION information_schema_stats_expiry=0;\n" if e["flavour"] == "mysql" else ""
    rc, out, _ = sh(e, pre + "SELECT ROUND((data_length+index_length)/1048576, 1) "
                             f"FROM information_schema.tables WHERE table_schema='{DB}' "
                             f"AND table_name='{tbl}';", DB)
    for ln in out.splitlines():
        t = ln.strip()
        if t and t[0].isdigit():
            return float(t)
    return 0.0


def pools(width):
    """Distinct value pool per column, every value padded to exactly `width` bytes so the
    sweep isolates value width from cardinality."""
    return {c[0]: [(c[0] + str(i)).ljust(width, "x")[:width] for i in range(c[2])]
            for c in COLS}


def generate(path_plain, path_dict, path_map, archives, width, seed=7):
    """One logical posting stream written twice: once with the value inline, once with the
    dictionary id. Identical rows, identical order, so only the encoding differs."""
    rng = random.Random(seed)
    pl = pools(width)
    ids, nxt = {}, 0
    for ci, c in enumerate(COLS):
        for v in pl[c[0]]:
            ids[(ci, v)] = nxt
            nxt += 1
    with open(path_map, "w") as f:
        for (ci, v), vid in ids.items():
            f.write(f"0\t{ci}\t{v}\t{vid}\n")
    t0 = 1704067200
    with open(path_plain, "w") as fp, open(path_dict, "w") as fd:
        for a in range(archives):
            ts = t0 + (a % 4032) * 3600          # spread over a plausible window
            for ci, c in enumerate(COLS):
                for v in rng.sample(pl[c[0]], c[1]):
                    fp.write(f"0\t{ci}\t{v}\t{ts}\t{a}\n")
                    fd.write(f"0\t{ci}\t{ids[(ci, v)]}\t{ts}\t{a}\n")
    return archives * POSTINGS


def build(e, name, cols, pk, tsv, extra=""):
    sh(e, f"DROP TABLE IF EXISTS {name};", DB)
    sh(e, f"CREATE TABLE {name} ({cols}, PRIMARY KEY ({pk})) ENGINE=InnoDB {extra};", DB)
    rc, _, err = sh(e, f"LOAD DATA LOCAL INFILE '{tsv}' INTO TABLE {name};", DB)
    if rc != 0:
        print(f"  load failed for {name}: {err.strip().splitlines()[-1][:80]}")
        return False
    return True


PLAIN_COLS = ("dataset_id SMALLINT UNSIGNED NOT NULL, column_id TINYINT UNSIGNED NOT NULL, "
              "value VARBINARY(255) NOT NULL, begin_timestamp BIGINT NOT NULL, "
              "archive_id INT UNSIGNED NOT NULL")
DICT_COLS = ("dataset_id SMALLINT UNSIGNED NOT NULL, column_id TINYINT UNSIGNED NOT NULL, "
             "value_id INT UNSIGNED NOT NULL, begin_timestamp BIGINT NOT NULL, "
             "archive_id INT UNSIGNED NOT NULL")
MAP_COLS = ("dataset_id SMALLINT UNSIGNED NOT NULL, column_id TINYINT UNSIGNED NOT NULL, "
            "value VARBINARY(255) NOT NULL, value_id INT UNSIGNED NOT NULL")
PLAIN_PK = "dataset_id, column_id, value, begin_timestamp, archive_id"
DICT_PK = "dataset_id, column_id, value_id, begin_timestamp, archive_id"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", default="mariadb=mysql")
    ap.add_argument("--archives", type=int, default=100000)
    ap.add_argument("--widths", default="12,40,100")
    ap.add_argument("--compress", action="store_true", default=True)
    a = ap.parse_args()
    e = detect(a.engine)
    if not e:
        return 1
    widths = [int(w) for w in a.widths.split(",")]
    print(f"engine {e['label']} {e['version']}")
    print(f"{a.archives:,} archives x {POSTINGS} postings = {a.archives * POSTINGS:,} rows, "
          f"{DISTINCT} distinct values per dataset\n")
    sh(e, f"CREATE DATABASE IF NOT EXISTS {DB};")

    hdr = (f"  {'width':>5} {'design':<22} {'MB':>9} {'B/row':>7} {'vs plain':>9}")
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    tmp = tempfile.mkdtemp(prefix="dict_")
    try:
        for w in widths:
            fp = os.path.join(tmp, f"p{w}.tsv")
            fd = os.path.join(tmp, f"d{w}.tsv")
            fm = os.path.join(tmp, f"m{w}.tsv")
            rows = generate(fp, fd, fm, a.archives, w)
            for f in (fp, fd, fm):
                os.chmod(f, 0o644)
            base = None
            variants = [("plain (value inline)", PLAIN_COLS, PLAIN_PK, fp, ""),
                        ("dict (INT value_id)", DICT_COLS, DICT_PK, fd, "")]
            if a.compress:
                variants += [
                    ("plain + COMPRESSED kbs8", PLAIN_COLS, PLAIN_PK, fp,
                     "ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8"),
                    ("dict + COMPRESSED kbs8", DICT_COLS, DICT_PK, fd,
                     "ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8")]
            for label, cols, pk, tsv, extra in variants:
                if not build(e, "t", cols, pk, tsv, extra):
                    continue
                mb = size_mb(e, "t")
                if "dict" in label:                        # the dictionary is part of the cost
                    build(e, "dmap", MAP_COLS, "dataset_id, column_id, value", fm)
                    mb += size_mb(e, "dmap")
                    sh(e, "DROP TABLE IF EXISTS dmap;", DB)
                if base is None:
                    base = mb
                delta = "--" if mb == base else f"{(mb / base - 1) * 100:+.1f}%"
                print(f"  {w:>5} {label:<22} {mb:>9.1f} {mb * 1048576 / rows:>7.1f} "
                      f"{delta:>9}")
                sh(e, "DROP TABLE IF EXISTS t;", DB)
            print()
    finally:
        for f in os.listdir(tmp):
            os.unlink(os.path.join(tmp, f))
        os.rmdir(tmp)
    print("  Rows are identical across designs; only the value encoding differs.")
    print("  The dict rows INCLUDE the dictionary table, so the comparison is net.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
