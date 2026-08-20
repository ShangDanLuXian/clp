#!/usr/bin/env python3
"""Oversized filter values: does the truncate + cap-length + overflow scheme stay correct,
and what does it cost?

A value longer than the side table's cap cannot be stored whole. Four write policies:

  trunc   store the first CAP bytes. Query terms are truncated identically. A posting of
          EXACTLY CAP bytes has an unknown tail, so it matches any wildcard pattern
          unconditionally -- otherwise a match sitting past the boundary is invisible.
  marker  drop the oversized postings, write one row per (column, archive) into an
          overflow table. Every query UNIONs it, so the archive is always a candidate.
  ltab    keep values <= CAP in the normal postings and store oversized values WHOLE in
          a separate long-value table, so every query shape evaluates against the full
          value -- no cap-length rule, no truncation semantics. Bounded by a per-archive
          cutoff: an archive with more than REJECT_K oversized values for the column gets
          one rejection marker instead of rows.
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
REJECT_K = 3                     # ltab: more oversized values than this in one archive -> reject
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
    """Writes ground truth (full values) and every policy's posting set.

    The fourth policy, `ltab`, is the long-value side table: values <= CAP go to the normal
    postings (the same file the marker policy uses), values above it go WHOLE into a
    separate small table -- UNLESS one archive has more than REJECT_K of them for the
    column, in which case the application gives up on that (archive, column) and writes a
    single rejection marker instead. That per-archive cutoff is what bounds the long-value
    table: heavy offenders are rejected, so what remains is rare by construction."""
    import random
    rng = random.Random(7)
    span = max(1, HOURS * HOUR // archives)
    paths = {n: os.path.join(outdir, f"{n}.tsv")
             for n in ("ref", "trunc", "marker", "naive", "overflow", "longv", "reject")}
    fh = {n: open(p, "wb") for n, p in paths.items()}
    for i in range(archives):
        ts = T0 + i * span
        vals = rng.sample(pool, PER_ARCHIVE)
        big = [v for v in vals if len(v) > CAP]
        for v in vals:
            row = b"\t%s\t%d\t%d\n" % (v, ts, i)
            fh["ref"].write(b"0" + row)
            fh["trunc"].write(b"0\t%s\t%d\t%d\n" % (v[:CAP], ts, i))
            if len(v) <= CAP:                       # marker/naive/ltab short postings
                fh["marker"].write(b"0" + row)
                fh["naive"].write(b"0" + row)
        if big:                                     # one marker row for the whole archive
            fh["overflow"].write(b"0\t%d\t%d\n" % (ts, i))
            if len(big) > REJECT_K:                 # ltab: too many -> reject the column
                fh["reject"].write(b"0\t%d\t%d\n" % (ts, i))
            else:                                   # ltab: rare -> store the values WHOLE
                for s, v in enumerate(big):
                    fh["longv"].write(b"0\t%s\t%d\t%d\t%d\n" % (v, ts, i, s))
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
    for t in ("overflow", "reject"):
        sh(e, f"CREATE TABLE {t} (column_id TINYINT UNSIGNED NOT NULL, "
              "begin_timestamp BIGINT NOT NULL, archive_id INT UNSIGNED NOT NULL, "
              "PRIMARY KEY (column_id, begin_timestamp, archive_id)) ENGINE=InnoDB "
              + parts() + ";", DB)
    # The long-value table stores oversized values WHOLE. The full value cannot live in an
    # index (3072-byte key limit), so the PK is positional and a PREFIX index serves
    # equality/prefix seeks -- InnoDB fetches the row and applies the full predicate, so
    # the prefix never leaks false candidates.
    sh(e, f"CREATE TABLE longv (column_id TINYINT UNSIGNED NOT NULL, "
          f"value VARBINARY({LONG_LEN}) NOT NULL, begin_timestamp BIGINT NOT NULL, "
          f"archive_id INT UNSIGNED NOT NULL, seq SMALLINT UNSIGNED NOT NULL, "
          f"PRIMARY KEY (column_id, begin_timestamp, archive_id, seq), "
          f"KEY ix_val (column_id, value({CAP}))) ENGINE=InnoDB " + parts() + ";", DB)
    cols = "column_id,value,begin_timestamp,archive_id"
    for t in ("ref", "trunc", "marker", "naive"):
        load(e, t, paths[t], cols)
    load(e, "longv", paths["longv"], cols + ",seq")
    for t in ("overflow", "reject"):
        load(e, t, paths[t], "column_id,begin_timestamp,archive_id")
    import shutil
    shutil.rmtree(td, ignore_errors=True)
    return pool


def _wait_bp(e, want):
    """Waits for an online buffer-pool resize to settle."""
    for _ in range(240):
        _, o, _ = sh(e, "SELECT @@innodb_buffer_pool_size;")
        n = [int(x) for x in o.split() if x.isdigit()]
        if n and n[-1] == want:
            _, o, _ = sh(e, "SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_resize_status';")
            if "completing" not in o.lower() and "resizing" not in o.lower():
                time.sleep(0.5)
                return True
        time.sleep(0.5)
    return False


def bp_cold(e):
    """Evicts the buffer pool so the next query reads storage: shrink the pool to one
    chunk, sweep the whole corpus through the shrunken pool, restore the size. The next
    query then runs against a production-sized but EMPTY pool -- true cold. On a server
    whose pool is already one chunk the shrink is skipped and the sweep alone must do the
    evicting (honest only when the corpus is larger than the pool). ref is swept LAST so
    the residue that survives belongs to the one table no timed query touches."""
    if "bp" not in e:
        _, o, _ = sh(e, "SELECT @@innodb_buffer_pool_size, @@innodb_buffer_pool_chunk_size;")
        n = [int(x) for x in o.split() if x.isdigit()]
        e["bp"] = (n[-2], n[-1]) if len(n) >= 2 else (0, 0)
        _, o, _ = sh(e, "SELECT @@innodb_flush_method;")
        e["flush"] = (o.split() or ["?"])[-1]
    size, chunk = e["bp"]
    shrunk = False
    if size > chunk:
        rc, _, _ = sh(e, f"SET GLOBAL innodb_buffer_pool_size={chunk};")
        shrunk = rc == 0 and _wait_bp(e, chunk)
    sh(e, "SELECT SUM(LENGTH(value)) FROM trunc; SELECT SUM(LENGTH(value)) FROM marker; "
          "SELECT SUM(LENGTH(value)) FROM naive; SELECT SUM(LENGTH(value)) FROM longv; "
          "SELECT COUNT(*) FROM overflow; SELECT COUNT(*) FROM reject; "
          "SELECT SUM(LENGTH(value)) FROM ref;", DB)
    if shrunk:
        sh(e, f"SET GLOBAL innodb_buffer_pool_size={size};")
        _wait_bp(e, size)
    return "evict" if shrunk else "sweep-only"


def ids(e, sql):
    """Returns (set_of_archive_ids, run1_ms, warm_ms).

    run1 is the FIRST issue of the statement after the corpus was built. It is not fully
    disk-cold -- the buffer pool still holds pages the load itself touched -- but it pays
    first-touch costs the warm run does not (optimizer, dictionary, pages the load wrote
    but the query path has not read). warm is the same statement re-issued immediately."""
    t0 = time.time()
    rc, out, err = sh(e, sql, DB)
    r1 = (time.time() - t0) * 1000
    if rc != 0:
        return None, r1, r1
    t0 = time.time()
    rc, out, err = sh(e, sql, DB)
    wm = (time.time() - t0) * 1000
    if rc != 0:
        return None, r1, wm
    return {int(x) for x in out.split() if x.strip().isdigit()}, r1, wm


def q_truth(e, where):
    r = ids(e, f"SELECT DISTINCT archive_id FROM ref WHERE column_id=0 AND {where};")
    return r[0], r[2]


def q_policy(e, tbl, where, with_overflow):
    sql = f"SELECT DISTINCT archive_id FROM {tbl} WHERE column_id=0 AND {where}"
    if with_overflow:
        sql += " UNION SELECT archive_id FROM overflow WHERE column_id=0"
    return ids(e, sql + ";")


def q_ltab(e, where_short, where_long):
    """The long-value-table policy's candidate query: short postings, plus the long-value
    table evaluated against the FULL value (this is what removes the cap-length rule),
    plus the rejection markers. Either branch may be None when the term's length proves it
    could never match that table -- the application-side skip."""
    br = []
    if where_short is not None:
        br.append(f"SELECT DISTINCT archive_id FROM marker WHERE column_id=0 AND {where_short}")
    if where_long is not None:
        br.append(f"SELECT DISTINCT archive_id FROM longv WHERE column_id=0 AND {where_long}")
    br.append("SELECT archive_id FROM reject WHERE column_id=0")
    return ids(e, " UNION ".join(br) + ";")


def hexlit(b):
    return "0x" + b.hex()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", action="append", default=[])
    ap.add_argument("--archives", type=int, default=100_000)
    ap.add_argument("--tmpdir", default=tempfile.gettempdir())
    ap.add_argument("--out", default="")
    ap.add_argument("--no-cold", action="store_true",
                    help="skip the buffer-pool eviction before each O1 measurement")
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
        cold_mode = None if a.no_cold else bp_cold(e)
        if cold_mode:
            line(f"  cold = first run after buffer-pool eviction (mode: {cold_mode}; "
                 f"flush_method={e.get('flush', '?')} -- O_DIRECT means cold reads truly")
            line("  hit storage; a buffered flush_method can still serve them from the OS "
                 "page cache)")
        col1 = "cold_ms" if cold_mode else "run1_ms"
        line(f"  {'query shape':<34}{'policy':<9}{'truth':>8}{'cands':>8}"
             f"{'missing':>9}{'extra':>8}{col1:>9}{'warm_ms':>9}  verdict")
        line("  " + "-" * 104)
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
                                  ("ltab", None, None), ("naive", "naive", False)):
                if cold_mode:
                    bp_cold(e)
                if pol == "ltab":
                    # A term/pattern whose length proves it cannot match one of the two
                    # tables skips that branch entirely -- the application-side rule.
                    if name.startswith("equality, short"):
                        cand, r1, wm = q_ltab(e, truth_where, None)
                    elif name.startswith("equality, term > cap"):
                        cand, r1, wm = q_ltab(e, None, truth_where)
                    else:
                        cand, r1, wm = q_ltab(e, truth_where, truth_where)
                else:
                    # marker/naive never hold an oversized value, so a >cap term is
                    # truncated only for trunc; the others search what they stored.
                    w = cand_where if pol == "trunc" else truth_where.replace(
                        f" OR LENGTH(value)={CAP}", "")
                    cand, r1, wm = q_policy(e, tbl, w, ovf)
                if cand is None:
                    line(f"  {name:<34}{pol:<9} query failed")
                    continue
                miss, extra = len(truth - cand), len(cand - truth)
                ok = miss == 0
                if not ok and pol != "naive":
                    failures += 1                 # only shippable policies count as failures
                line(f"  {name:<34}{pol:<9}{len(truth):>8,}{len(cand):>8,}"
                     f"{miss:>9,}{extra:>8,}{r1:>9,.1f}{wm:>9,.1f}"
                     f"  {'pass' if ok else 'FALSE NEGATIVES'}")
            line("  " + "-" * 104)
        line(f"  trunc + marker + ltab: "
             f"{'ALL CORRECT' if failures == 0 else str(failures) + ' FAILED'}"
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
        line(f"  {'oversized':<10}{'trunc_MB':>9}{'mark_MB':>8}{'ltab_MB':>8}"
             f"{'eq tr':>7}{'eq mk':>7}{'eq lt':>7}"
             f"{'ifx tr':>8}{'ifx mk':>8}{'ifx lt':>8}")
        line("  " + "-" * 84)
        lat = []
        # Dense at the low end: the admission budget (total <= ~1.5x of the byte budget)
        # keeps the reachable regime under a few percent oversized -- a column beyond that
        # is demoted by accounting long before. The last row is deliberately outside the
        # reachable regime: it characterizes the FAILURE SHAPE if the valve broke (latency
        # grows smoothly, correctness never breaks), and is not a forecast.
        for label, nl in (("0%", 0), ("0.5%", 1), ("1%", 2), ("2%", 4), ("5%", 10),
                          ("10%*", 20)):
            build(e, a.archives, nl, a.tmpdir)
            sz = {}
            for t in ("trunc", "marker", "longv", "reject"):
                sh(e, f"ANALYZE TABLE {t};", DB)
                _, o, _ = sh(e, "SELECT data_length+index_length FROM information_schema."
                                f"tables WHERE table_schema='{DB}' AND table_name='{t}';", DB)
                n = [int(x) for x in o.split() if x.isdigit()]
                sz[t] = n[-1] if n else 0
            # ltab total = the short postings plus the long-value table plus its markers.
            sz["ltab"] = sz["marker"] + sz["longv"] + sz["reject"]

            def ratio(truth, cand):
                return (len(cand) / len(truth)) if truth and cand is not None else 0

            t_eq, _ = q_truth(e, f"value={eq_probe}")
            c_eq_t, r1_eq_t, ms_eq_t = q_policy(e, "trunc", f"value={eq_probe}", False)
            c_eq_m, r1_eq_m, ms_eq_m = q_policy(e, "marker", f"value={eq_probe}", True)
            c_eq_l, r1_eq_l, ms_eq_l = q_ltab(e, f"value={eq_probe}", None)
            t_ix, _ = q_truth(e, f"value LIKE '{infix_probe}'")
            c_ix_t, r1_ix_t, ms_ix_t = q_policy(
                e, "trunc", f"(value LIKE '{infix_probe}' OR LENGTH(value)={CAP})", False)
            c_ix_m, r1_ix_m, ms_ix_m = q_policy(e, "marker",
                                                f"value LIKE '{infix_probe}'", True)
            c_ix_l, r1_ix_l, ms_ix_l = q_ltab(e, f"value LIKE '{infix_probe}'",
                                              f"value LIKE '{infix_probe}'")
            line(f"  {label:<10}{sz['trunc'] / 1048576:>9,.1f}"
                 f"{sz['marker'] / 1048576:>8,.1f}{sz['ltab'] / 1048576:>8,.1f}"
                 f"{ratio(t_eq, c_eq_t):>6.2f}x{ratio(t_eq, c_eq_m):>6.2f}x"
                 f"{ratio(t_eq, c_eq_l):>6.2f}x"
                 f"{ratio(t_ix, c_ix_t):>7.2f}x{ratio(t_ix, c_ix_m):>7.2f}x"
                 f"{ratio(t_ix, c_ix_l):>7.2f}x")
            lat.append((f"  {label:<10}"
                        f"{r1_eq_t:>8,.1f}{r1_eq_m:>8,.1f}{r1_eq_l:>8,.1f}"
                        f"{r1_ix_t:>9,.1f}{r1_ix_m:>9,.1f}{r1_ix_l:>9,.1f}",
                        f"  {label:<10}"
                        f"{ms_eq_t:>8,.1f}{ms_eq_m:>8,.1f}{ms_eq_l:>8,.1f}"
                        f"{ms_ix_t:>9,.1f}{ms_ix_m:>9,.1f}{ms_ix_l:>9,.1f}"))
        line("")
        line(f" O4  side-table query latency  [{e['label']}]")
        hdr = (f"  {'oversized':<10}{'eq tr':>8}{'eq mk':>8}{'eq lt':>8}"
               f"{'ifx tr':>9}{'ifx mk':>9}{'ifx lt':>9}")
        line("  run1 (first issue after build -- pays first-touch, not fully disk-cold):")
        line(hdr)
        line("  " + "-" * 62)
        for r1row, _ in lat:
            line(r1row)
        line("  warm (same statement re-issued immediately):")
        line(hdr)
        line("  " + "-" * 62)
        for _, wmrow in lat:
            line(wmrow)
        line("  (run1 here is NOT cold -- the sweep rebuilds the corpus, so the load")
        line("   itself warms the pool. True cold is measured in O1 via eviction.)")
        line("  (this is ONLY the SQL side of the query. The dominating cost of a bad")
        line("   policy is downstream: every extra candidate is an archive OPENED and")
        line("   searched for nothing -- which is what the ratio table above counts.)")
        line("")
        line("  (ratios are archives-opened / archives-that-actually-match, on SELECTIVE")
        line("   probes. 1.00x is perfect pruning. ltab = short postings + long values")
        line("   stored WHOLE in a side table + rejection markers for archives with more")
        line(f"   than {REJECT_K} oversized values in this column: its only over-match is")
        line("   those rejected archives. The starred row is outside the budget-reachable")
        line("   regime -- failure-shape reference only.)")
        sh(e, f"DROP DATABASE IF EXISTS {DB};")

    line(f"\nwritten to {outpath}")
    out.close()


if __name__ == "__main__":
    main()
