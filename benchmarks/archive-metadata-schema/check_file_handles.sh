#!/bin/bash
# Two questions the topology benchmark reported on but never actually settled, measured
# here on a throwaway instance so neither depends on the big run.
#
#   A. WHEN is a partition's tablespace file opened -- at server startup, at table open, or
#      on first touch? This decides whether Innodb_num_open_files describes the instance or
#      the query.
#   B. What does exceeding innodb_open_files COST? The topology run reported three
#      configurations pinned at the cap and treated that as a finding, without ever
#      demonstrating a penalty.
#
# Method notes, because three earlier attempts at (A) produced confident wrong answers:
#   - Do NOT infer "opened lazily" from a zero delta in Innodb_num_open_files. A zero delta
#     means the files were already open, which is exactly what is in question. This script
#     counts the server's open file descriptors by NAME in /proc/<pid>/fd instead.
#   - innodb_buffer_pool_dump_at_shutdown defaults ON, so a naive restart reloads the pool
#     and opens tablespaces as a side effect. Both flags are off below.
#   - FLUSH TABLES does not close InnoDB tablespace handles.
#   - A recursive CTE silently stops at max_recursive_iterations (default 1000), which
#     produced an entire timing table measured on an empty table. Raised, and the row count
#     is asserted before anything is timed.
#
# Usage: bash check_file_handles.sh [workdir]
set -u
D=${1:-/tmp/clp_fh}
S=$D/s.sock
NPART=200            # partitions in the timed table
ROWS=4000            # rows per partition
POOL=32M             # deliberately far smaller than the data, so the sweep does real I/O
rm -rf "$D"; mkdir -p "$D/data"

command -v mariadbd >/dev/null || { echo "mariadbd not found"; exit 1; }
mariadb-install-db --datadir="$D/data" --auth-root-authentication-method=socket \
                   --skip-test-db >/dev/null 2>&1 || { echo "install-db failed"; exit 1; }

start() {                                        # $1 = innodb_open_files
  mariadbd --user="$(id -un)" --datadir="$D/data" --socket="$S" --skip-networking \
           --innodb-file-per-table=1 --innodb-open-files="$1" --table-open-cache=1000 \
           --innodb-buffer-pool-size=$POOL \
           --innodb-buffer-pool-dump-at-shutdown=0 --innodb-buffer-pool-load-at-startup=0 \
           --pid-file="$D/pid" >>"$D/err" 2>&1 &
  for _ in $(seq 90); do [ -S "$S" ] && sleep 2 && return 0; sleep 1; done
  echo "server did not start"; tail -5 "$D/err"; exit 1
}
q() { mariadb --socket="$S" -N -B -e "$1" 2>&1; }
g() { q "SHOW GLOBAL STATUS LIKE '$1';" | awk '{print $2}'; }
fds() { ls -l /proc/"$(cat "$D/pid")"/fd 2>/dev/null | grep -c "a#P#p"; }
die() { echo "FAILED: $1"; q "SHUTDOWN;" >/dev/null 2>&1; exit 1; }

start 1000
p="PARTITION p0 VALUES LESS THAN (1)"
for h in $(seq 1 $NPART); do p="$p,PARTITION p$h VALUES LESS THAN ($((h + 1)))"; done
out=$(q "CREATE DATABASE t;
   CREATE TABLE t.a (ts INT NOT NULL, v INT NOT NULL, pad CHAR(120) NOT NULL DEFAULT 'x',
                     PRIMARY KEY (ts, v)) ENGINE=InnoDB PARTITION BY RANGE (ts) ($p);
   CREATE TABLE t.nums (n INT PRIMARY KEY);
   SET SESSION max_recursive_iterations = 10000000;
   INSERT INTO t.nums WITH RECURSIVE s AS (SELECT 1 n UNION ALL SELECT n+1 FROM s
                                           WHERE n < $ROWS) SELECT n FROM s;
   INSERT INTO t.a (ts, v) SELECT p.n - 1, c.n FROM t.nums p JOIN t.nums c
     WHERE p.n <= $NPART;")
[ -n "$out" ] && echo "build output: $out"
rows=$(q "SELECT COUNT(*) FROM t.a;")
[ "$rows" = "$((NPART * ROWS))" ] || die "expected $((NPART * ROWS)) rows, got '$rows'"
echo "built: $rows rows over $NPART populated partitions, $POOL pool"

echo
echo "== A. when is a partition's tablespace opened? =="
echo "   open_fds counts the server's descriptors on this table's partition files."
q "SHUTDOWN;" >/dev/null 2>&1; sleep 3; start 1000
row() { printf "   %-40s open_fds=%-5s gauge=%s\n" "$1" "$2" "$(g Innodb_num_open_files)"; }
b=$(fds); row "after restart, table untouched" "$b"
q "SELECT 1;" >/dev/null
row "after a query not touching the table" "$(fds)"
q "SELECT COUNT(*) FROM t.a WHERE ts=7;" >/dev/null
o=$(fds); row "after query pruned to ONE partition" "$o"
q "SELECT COUNT(*), SUM(v) FROM t.a;" >/dev/null
f=$(fds); row "after full sweep of all $NPART" "$f"
echo
if [ "$b" -ge $((NPART * 9 / 10)) ]; then
  echo "   AT STARTUP: the files were open before any query ran. Innodb_num_open_files is"
  echo "   therefore min(tablespaces holding data, innodb_open_files) -- a property of the"
  echo "   INSTANCE, not of a query's time window."
elif [ "$o" -le 5 ] && [ "$f" -ge $((NPART / 2)) ]; then
  echo "   ON TOUCH: only the partitions a query reads are opened, so the working set is"
  echo "   the query's time window."
else
  echo "   inconclusive: untouched=$b one-partition=$o full-sweep=$f of $NPART"
fi

echo
echo "== B. what does exceeding the cap cost? =="
echo "   identical data, identical $POOL pool, identical sweep of all $NPART partitions;"
echo "   only innodb_open_files changes, so any difference is handle churn."
run() {
  q "SHUTDOWN;" >/dev/null 2>&1; sleep 3; start "$1"
  local r0 t0 t1 ms best=9999999 i
  r0=$(g Innodb_data_reads)
  for i in 1 2 3 4 5; do
    t0=$(date +%s%N); q "SELECT COUNT(*), SUM(v) FROM t.a;" >/dev/null; t1=$(date +%s%N)
    ms=$(( (t1 - t0) / 1000000 )); [ $ms -lt $best ] && best=$ms
  done
  printf "   cap=%-6s gauge=%-6s best_of_5=%6s ms   data_reads=%s\n" \
         "$1" "$(g Innodb_num_open_files)" "$best" "$(( $(g Innodb_data_reads) - r0 ))"
}
run 1000
run 300
run 30
run 12
echo
echo "   Equal data_reads across rows confirms the pool behaves identically, so the wall"
echo "   time difference is evict/reopen and not I/O volume. Note this sweep is I/O bound,"
echo "   which UNDER-states churn as a fraction: on cached data it would weigh more."
q "SHUTDOWN;" >/dev/null 2>&1; sleep 2
