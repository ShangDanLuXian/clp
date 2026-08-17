#!/usr/bin/env bash
# Round 4 at full scale, in two phases, unattended.
#
#   Phase 1 (concurrent, both engines at once): storage experiments B1/B2/B4/B5.
#           Their results are BYTES, which concurrency cannot change, so running the two
#           engines in parallel is free. Needs ~8 cores; fine on 16.
#   Phase 2 (strictly sequential, one engine at a time, nothing else running): the query
#           experiments B3/B6/B7/B8/B9. Timings are only meaningful on an idle machine --
#           a concurrent bulk load evicts the buffer-pool working set a query benchmark
#           depends on, which is a cache effect, not a CPU one, so extra cores do not help.
#
# Usage:  ./bench4_full.sh [ARCHIVES]     (default 4838400 = lc3 parity)
#         nohup ./bench4_full.sh > bench4_full.log 2>&1 &     to survive logout
set -uo pipefail                      # NOT -e: a failed phase must not skip the rest
cd "$(dirname "$0")"

N="${1:-4838400}"
STAMP=$(date +%Y%m%d-%H%M%S)
MARIA="mysql"
MY8="/opt/mysql8/usr/bin/mysql --defaults-file=/etc/my8.cnf -u root"
say() { printf '\n[%s] === %s\n' "$(date +%H:%M:%S)" "$1"; }

# WSL allocates its VM a slice of the host (default: half the RAM, all logical CPUs), and
# .wslconfig can override both -- so measure rather than assume. Phase 1 runs the two engines
# concurrently only if this box can actually host both without serialising on CPU or swapping.
CORES=$(nproc)
RAM_GB=$(awk '/MemTotal/{printf "%d", $2/1048576}' /proc/meminfo)
DISK_GB=$(df -BG --output=avail . | tail -1 | tr -dc '0-9')
say "machine: ${CORES} cores, ${RAM_GB} GB RAM, ${DISK_GB} GB free disk"
CONCURRENT=1
[ "$CORES" -lt 8 ] && CONCURRENT=0 && \
    echo "  <8 cores: phase 1 will run engines SEQUENTIALLY (concurrent would just serialise)"
[ "$RAM_GB" -lt 12 ] && CONCURRENT=0 && \
    echo "  <12 GB RAM: phase 1 sequential (two 4 GB buffer pools + generators would swap)"
if [ "$DISK_GB" -lt 120 ]; then
    echo "  WARNING: <120 GB free; a full-scale run needs ~100 GB of tables plus staging."
    echo "           Re-run with a smaller archive count if this fails mid-load."
fi
[ "$CONCURRENT" -eq 1 ] && echo "  phase 1: engines run CONCURRENTLY"

have_mysql8=0
if $MY8 -e "SELECT 1;" >/dev/null 2>&1; then
    have_mysql8=1
else
    say "MySQL 8 not reachable -- running MariaDB only (MySQL can be re-run later)"
fi

say "clearing lc4 on all engines"
$MARIA -e "DROP DATABASE IF EXISTS lc4;" 2>/dev/null
[ "$have_mysql8" -eq 1 ] && $MY8 -e "DROP DATABASE IF EXISTS lc4;" 2>/dev/null
# Staging dirs orphaned by an interrupted run; matched by content so nothing else is hit.
for d in "${TMPDIR:-/tmp}"/tmp*/; do
    [ -f "${d}side.tsv" ] && rm -rf "$d" && echo "  removed orphaned staging dir"
done
df -h . | awk 'NR==2{print "  disk: "$4" free"}'

# ---------------------------------------------------------------- phase 1: storage
MODE=$([ "$CONCURRENT" -eq 1 ] && echo CONCURRENTLY || echo SEQUENTIALLY)
say "PHASE 1  storage experiments (B1 B2 B4 B5), engines run $MODE, N=$N"
STORE_EXPS="--exp B1 --exp B2 --exp B4 --exp B5 --skip-kbs4"
# shellcheck disable=SC2086
python3 bench4.py $STORE_EXPS --small "$N" --engine mariadb="$MARIA" \
    --out "lc4_storage_mariadb_${STAMP}.txt" > "p1_mariadb_${STAMP}.log" 2>&1 &
P1M=$!
if [ "$have_mysql8" -eq 1 ] && [ "$CONCURRENT" -eq 1 ]; then
    # Stagger the two engines so their heaviest load stretches do not coincide. Scale it
    # with the run: a fixed delay would dominate a small smoke run entirely.
    STAGGER=$(( N / 10000 ))
    [ "$STAGGER" -gt 600 ] && STAGGER=600
    [ "$STAGGER" -gt 0 ] && echo "  staggering MySQL start by ${STAGGER}s"
    sleep "$STAGGER"
    # shellcheck disable=SC2086
    python3 bench4.py $STORE_EXPS --small "$N" --engine mysql="$MY8" \
        --out "lc4_storage_mysql_${STAMP}.txt" > "p1_mysql_${STAMP}.log" 2>&1 &
    P1Y=$!
fi
wait $P1M; echo "  mariadb storage: exit $?"
if [ "$have_mysql8" -eq 1 ]; then
    if [ "$CONCURRENT" -eq 1 ]; then
        wait $P1Y; echo "  mysql storage: exit $?"
    else
        # shellcheck disable=SC2086
        python3 bench4.py $STORE_EXPS --small "$N" --engine mysql="$MY8" \
            --out "lc4_storage_mysql_${STAMP}.txt" > "p1_mysql_${STAMP}.log" 2>&1
        echo "  mysql storage: exit $?"
    fi
fi
say "PHASE 1 complete"

# ---------------------------------------------------------------- phase 2: queries
# Sequential by construction: each engine runs alone, with the other idle.
QUERY_EXPS="--exp B3 --exp B6 --exp B7 --exp B8 --exp B9"
say "PHASE 2a  query experiments on MariaDB (exclusive), N=$N"
# shellcheck disable=SC2086
python3 bench4.py $QUERY_EXPS --archives "$N" --engine mariadb="$MARIA" \
    --out "lc4_query_mariadb_${STAMP}.txt" 2>&1 | tail -40
echo "  exit ${PIPESTATUS[0]}"

if [ "$have_mysql8" -eq 1 ]; then
    say "PHASE 2b  query experiments on MySQL (exclusive), N=$N"
    # shellcheck disable=SC2086
    python3 bench4.py $QUERY_EXPS --archives "$N" --engine mysql="$MY8" \
        --out "lc4_query_mysql_${STAMP}.txt" 2>&1 | tail -40
    echo "  exit ${PIPESTATUS[0]}"
fi

say "ALL DONE"
ls -la lc4_*_"${STAMP}".txt 2>/dev/null
cat <<EOF

  storage results (concurrent run -- sizes are exact; load_s/ins/s are indicative):
    lc4_storage_mariadb_${STAMP}.txt
    lc4_storage_mysql_${STAMP}.txt
  query results (exclusive runs -- timings are clean):
    lc4_query_mariadb_${STAMP}.txt
    lc4_query_mysql_${STAMP}.txt
  phase-1 progress logs: p1_*_${STAMP}.log
EOF
