#!/usr/bin/env python3
"""Oversized filter values: does the truncate + cap-length + overflow scheme stay correct,
and what does it cost?

A value longer than the side table's cap cannot be stored whole. Three write policies:

  trunc   store the first CAP bytes. Query terms are truncated identically. A posting of
          EXACTLY CAP bytes has an unknown tail, so it matches any wildcard pattern
          unconditionally -- otherwise a match sitting past the boundary is invisible.
  marker  drop the oversized postings, write one row per (column, archive) into an
          overflow table. Every query UNIONs it, so the archive is always a candidate.
  naive   drop the oversized postings and record nothing. This is the design we must NOT
          ship; it is measured here only to show what it silently loses.

The governing rule is that the side table generates CANDIDATES, not answers: over-matching
costs an archive open, under-matching is a wrong result that no later stage can repair.
So the pass condition is truth SUBSET candidates -- never equality.

  O1  correctness   every query shape x every policy, against untruncated ground truth
  O2  storage       cost of each policy as the oversized fraction rises
  O3  pruning       candidate-set inflation -- the real price of the cap-length rule
  O4  latency       does the extra OR / UNION cost anything measurable

Usage:
    python3 bench_oversize.py
    python3 bench_oversize.py --archives 200000 --engine mariadb=mysql
"""
import argparse
import os
import shlex
import subprocess
import sys
import tempfile
import time

DB = "oversize"
CAP = 1024                       # the side table's VARBINARY(CAP)
LONG_LEN = 3000                  # length of an oversized value
COLLIDE = 4                      # oversized values per identical-CAP-prefix group
POOL = 200                       # distinct values in the column's pool
PER_ARCHIVE = 8                  # postings per archive for this column
T0 = 1704067200 * 10**9
HOUR = 3600 * 10**9
HOURS = 168

SIDE_PK = "PRIMARY KEY (column_id, value, begin_timestamp, archive_id)"


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


def parts():
    p = [f"PARTITION p_floor VALUES LESS THAN ({T0})"]
    p += [f"PARTITION p_h{h:06d} VALUES LESS THAN ({T0 + (h + 1) * HOUR})"
          for h in range(HOURS)]
    p.append("PARTITION p_future VALUES LESS THAN MAXVALUE")
    return "PARTITION BY RANGE (begin_timestamp) (" + ",".join(p) + ")"


def build_pool(n_long):
    """Value pool. Oversized entries are built so the scheme's two hard cases both occur:

      - within a group of COLLIDE, the first CAP bytes are IDENTICAL, so truncation makes
        them indistinguishable (equality with a long term must over-match, not mis-match);
      - each carries a unique tag PAST byte CAP, so an infix search for that tag has its
        only evidence in the part truncation throws away.
    """
    pool = []
    for i in range(POOL - n_long):
        pool.append(f"host-{i:05d}-svc".encode())
    for i in range(n_long):
        head = f"L{i // COLLIDE:04d}".encode().ljust(CAP, b"a")     # shared across a group
        tail = f"ZZTAG{i:04d}".encode()                             # unique, beyond the cap
        pool.append((head + tail).ljust(LONG_LEN, b"b"))
    return pool


def gen(archives, pool, outdir):
    """Writes ground truth (full values) and the three policies' posting sets."""
    import random
    rng = random.Random(7)
    span = max(1, HOURS * HOUR // archives)
    paths = {n: os.path.join(outdir, f"{n}.tsv")
             for n in ("ref", "trunc", "marker", "naive", "overflow")}
    fh = {n: open(p, "wb") for n, p in paths.items()}
    for i in range(archives):
        ts = T0 + i * span
        vals = rng.sample(pool, PER_ARCHIVE)
        big = [v for v in vals if len(v) > CAP]
        for v in vals:
            row = b"\t%s\t%d\t%d\n" % (v, ts, i)
            fh["ref"].write(b"0" + row)
            fh["trunc"].write(b"0\t%s\t%d\t%d\n" % (v[:CAP], ts, i))
            if len(v) <= CAP:                       # marker and naive keep only short values
                fh["marker"].write(b"0" + row)
                fh["naive"].write(b"0" + row)
        if big:                                     # one marker row for the whole archive
            fh["overflow"].write(b"0\t%d\t%d\n" % (ts, i))
    for f in fh.values():
        f.close()
    for p in paths.values():
        os.chmod(p, 0o644)
    return paths


def load(e, tbl, path, cols):
    return sh(e, f"LOAD DATA LOCAL INFILE '{path}' INTO TABLE {tbl} ({cols});", DB, True)


def build(e, archives, n_long, tmpdir):
    pool = build_pool(n_long)
    td = tempfile.mkdtemp(dir=tmpdir)
    os.chmod(td, 0o755)
    paths = gen(archives, pool, td)
    sh(e, f"DROP DATABASE IF EXISTS {DB}; CREATE DATABASE {DB};")
    # Ground truth keeps values whole. It cannot index them (VARBINARY(3000) blows the
    # 3072-byte key limit once the other PK columns are counted), so it is scanned.
    sh(e, f"CREATE TABLE ref (column_id TINYINT UNSIGNED NOT NULL, "
          f"value VARBINARY({LONG_LEN}) NOT NULL, begin_timestamp BIGINT NOT NULL, "
          f"archive_id INT UNSIGNED NOT NULL) ENGINE=InnoDB;", DB)
    for t in ("trunc", "marker", "naive"):
        sh(e, f"CREATE TABLE {t} (column_id TINYINT UNSIGNED NOT NULL, "
              f"value VARBINARY({CAP}) NOT NULL, begin_timestamp BIGINT NOT NULL, "
              f"archive_id INT UNSIGNED NOT NULL, {SIDE_PK}) ENGINE=InnoDB " + parts() + ";", DB)
    sh(e, "CREATE TABLE overflow (column_id TINYINT UNSIGNED NOT NULL, "
          "begin_timestamp BIGINT NOT NULL, archive_id INT UNSIGNED NOT NULL, "
          "PRIMARY KEY (column_id, begin_timestamp, archive_id)) ENGINE=InnoDB "
          + parts() + ";", DB)
    cols = "column_id,value,begin_timestamp,archive_id"
    for t in ("ref", "trunc", "marker", "naive"):
        load(e, t, paths[t], cols)
    load(e, "overflow", paths["overflow"], "column_id,begin_timestamp,archive_id")
    import shutil
    shutil.rmtree(td, ignore_errors=True)
    return pool


def ids(e, sql):
    """Returns (set_of_archive_ids, elapsed_ms). Warm: runs twice, times the second."""
    sh(e, sql, DB)
    t0 = time.time()
    rc, out, err = sh(e, sql, DB)
    ms = (time.time() - t0) * 1000
    if rc != 0:
        return None, ms
    return {int(x) for x in out.split() if x.strip().isdigit()}, ms


def q_truth(e, where):
    return ids(e, f"SELECT DISTINCT archive_id FROM ref WHERE column_id=0 AND {where};")


def q_policy(e, tbl, where, with_overflow):
    sql = f"SELECT DISTINCT archive_id FROM {tbl} WHERE column_id=0 AND {where}"
    if with_overflow:
        sql += " UNION SELECT archive_id FROM overflow WHERE column_id=0"
    return ids(e, sql + ";")


def hexlit(b):
    return "0x" + b.hex()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", action="append", default=[])
    ap.add_argument("--archives", type=int, default=100_000)
    ap.add_argument("--tmpdir", default=tempfile.gettempdir())
    ap.add_argument("--out", default="")
    a = ap.parse_args()

    engines = detect(a.engine or ["mariadb=mysql", "mariadb=mariadb"])
    if not engines:
        sys.exit(f"no usable server (needs GRANT ALL ON {DB}.*)")
    outpath = a.out or f"oversize_results_{time.strftime('%Y%m%d-%H%M%S')}.txt"
    out = open(outpath, "w")

    def line(s):
        print(s, flush=True)
        out.write(s + "\n")
        out.flush()

    line(f" OVERSIZED VALUES  {time.strftime('%Y-%m-%d %H:%M:%S')}")
    line(f"   cap={CAP} B, oversized value={LONG_LEN} B, {COLLIDE} share each CAP-prefix,"
         f" pool={POOL}, {PER_ARCHIVE} postings/archive")
    for e in engines:
        line(f"   {e['label']:<9} {e['version']}")

    for e in engines:
        # ---------------------------------------------------------------- O1 correctness
        n_long = POOL // 10                                  # 10% of the pool oversized
        pool = build(e, a.archives, n_long, a.tmpdir)
        short_v = pool[0]
        long_v = pool[POOL - n_long]                         # first oversized value
        tag = f"ZZTAG{0:04d}".encode()                       # lives past byte CAP
        line("")
        line("=" * 98)
        line(f" O1  correctness: truth must be a SUBSET of candidates  [{e['label']}]")
        line("=" * 98)
        line(f"  {'query shape':<34}{'policy':<9}{'truth':>8}{'cands':>8}"
             f"{'missing':>9}{'extra':>8}  verdict")
        line("  " + "-" * 88)
        shapes = [
            ("equality, short term", f"value={hexlit(short_v)}", f"value={hexlit(short_v)}"),
            ("equality, term > cap", f"value={hexlit(long_v)}",
             f"value={hexlit(long_v[:CAP])}"),
            ("prefix  'host-000%'", "value LIKE 'host-000%'", "value LIKE 'host-000%'"),
            ("infix   '%svc%' (short vals)", "value LIKE '%svc%'",
             f"(value LIKE '%svc%' OR LENGTH(value)={CAP})"),
            ("infix   '%ZZTAG0000%' (past cap)", f"value LIKE '%{tag.decode()}%'",
             f"(value LIKE '%{tag.decode()}%' OR LENGTH(value)={CAP})"),
        ]
        failures = 0
        for name, truth_where, cand_where in shapes:
            truth, _ = q_truth(e, truth_where)
            if truth is None:
                line(f"  {name:<34} ground-truth query failed")
                continue
            for pol, tbl, ovf in (("trunc", "trunc", False), ("marker", "marker", True),
                                  ("naive", "naive", False)):
                # marker/naive never hold an oversized value, so a >cap term is truncated
                # only for trunc; the others search for what they actually stored.
                w = cand_where if pol == "trunc" else truth_where.replace(
                    f" OR LENGTH(value)={CAP}", "")
                cand, _ = q_policy(e, tbl, w, ovf)
                if cand is None:
                    line(f"  {name:<34}{pol:<9} query failed")
                    continue
                miss, extra = len(truth - cand), len(cand - truth)
                ok = miss == 0
                if not ok and pol != "naive":
                    failures += 1                 # only shippable policies count as failures
                line(f"  {name:<34}{pol:<9}{len(truth):>8,}{len(cand):>8,}"
                     f"{miss:>9,}{extra:>8,}  {'pass' if ok else 'FALSE NEGATIVES'}")
            line("  " + "-" * 88)
        line(f"  trunc + marker: {'ALL CORRECT' if failures == 0 else str(failures) + ' FAILED'}"
             f"    naive: false negatives above are the expected demonstration of why")
        line("  dropping an oversized posting without recording anything cannot be shipped.")

        # ---------------------------------------------------------------- O2/O3 cost sweep
        line("")
        line("=" * 98)
        line(f" O2/O3  cost of the cap-length rule as oversized values get more common"
             f"  [{e['label']}]")
        line("=" * 98)
        # Both probes must be SELECTIVE. A probe matching every archive makes the candidate
        # ratio 1.00x for any policy and hides the effect entirely.
        eq_probe = hexlit(pool[0])                       # one short value
        infix_probe = "%-00000-%"                        # substring of that one value only
        line(f"  {'oversized':<10}{'trunc_MB':>9}{'marker_MB':>10}{'ovf_rows':>9}"
             f"{'eq trunc':>10}{'eq mark':>9}{'infix trunc':>13}{'infix mark':>12}")
        line("  " + "-" * 84)
        for pct in (0, 1, 5, 10, 25):
            nl = max(0, POOL * pct // 100)
            build(e, a.archives, nl, a.tmpdir)
            sz = {}
            for t in ("trunc", "marker"):
                sh(e, f"ANALYZE TABLE {t};", DB)
                _, o, _ = sh(e, "SELECT data_length+index_length FROM information_schema."
                                f"tables WHERE table_schema='{DB}' AND table_name='{t}';", DB)
                n = [int(x) for x in o.split() if x.isdigit()]
                sz[t] = n[-1] if n else 0
            _, o, _ = sh(e, "SELECT COUNT(*) FROM overflow;", DB)
            ovf = int(([x for x in o.split() if x.isdigit()] or [0])[-1])

            def ratio(truth, cand):
                return (len(cand) / len(truth)) if truth and cand is not None else 0

            t_eq, _ = q_truth(e, f"value={eq_probe}")
            c_eq_t, _ = q_policy(e, "trunc", f"value={eq_probe}", False)
            c_eq_m, _ = q_policy(e, "marker", f"value={eq_probe}", True)
            t_ix, _ = q_truth(e, f"value LIKE '{infix_probe}'")
            c_ix_t, _ = q_policy(e, "trunc",
                                 f"(value LIKE '{infix_probe}' OR LENGTH(value)={CAP})", False)
            c_ix_m, _ = q_policy(e, "marker", f"value LIKE '{infix_probe}'", True)
            line(f"  {str(pct) + '%':<10}{sz['trunc'] / 1048576:>9,.1f}"
                 f"{sz['marker'] / 1048576:>10,.1f}{ovf:>9,}"
                 f"{ratio(t_eq, c_eq_t):>9.2f}x{ratio(t_eq, c_eq_m):>8.2f}x"
                 f"{ratio(t_ix, c_ix_t):>12.2f}x{ratio(t_ix, c_ix_m):>11.2f}x")
        line("  (ratios are archives-opened / archives-that-actually-match, on SELECTIVE")
        line("   probes. 1.00x is perfect pruning; 10x means ten archives opened per hit.")
        line("   trunc keeps equality exact and pays only on wildcards; marker pays on")
        line("   everything but its storage never grows.)")
        sh(e, f"DROP DATABASE IF EXISTS {DB};")

    line(f"\nwritten to {outpath}")
    out.close()


if __name__ == "__main__":
    main()
