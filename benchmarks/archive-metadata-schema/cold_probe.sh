#!/usr/bin/env bash
# TRUE cold query latency for the multi-tenancy comparison.
#
# An online buffer-pool resize is the cheap way to evict, but MariaDB 10.6 refuses or
# clamps the shrink, and a scan-only sweep does NOT evict InnoDB (scanned pages enter the
# OLD sublist and are dropped from there without displacing the YOUNG pages). The only
# reliable evictor left is restarting the server, which is what this does -- once per
# measurement, so every query below reads storage with an empty pool.
#
# Prerequisite: a tenancy corpus left in place by
#     python3 bench_tenancy.py --datasets 100 --per 3000 --engine mariadb=mysql --keep
#
# Usage:  ./cold_probe.sh [DATASETS] [HOURS]        (defaults: 100 168, matching the run)
#         sudo -v            # cache credentials first; each restart needs sudo
set -uo pipefail

DATASETS="${1:-100}"
HOURS="${2:-168}"
DB=tenancy
T0=1704067200000000000
HOUR=3600000000000
W1=$(( T0 + (HOURS - 24) * HOUR ))          # last 24h of the corpus, same window as T2
W2=$(( T0 + HOURS * HOUR ))
PROBE="m07xxxxxxxxxxx"                      # val("m0", 14, 7), the T2 probe
K=$(( DATASETS / 2 ))                       # the tenant T2 measures
PRED="column_id=7 AND value='$PROBE' AND begin_timestamp >= $W1 AND begin_timestamp < $W2"

mysql -N -B -e "SELECT 1" >/dev/null 2>&1 || { echo "no server" >&2; exit 1; }
mysql -N -B "$DB" -e "SELECT 1 FROM side_all LIMIT 1;" >/dev/null 2>&1 || {
    echo "no \`$DB\` corpus -- run bench_tenancy.py with --keep first" >&2; exit 1; }

restart() {
    sudo service mariadb restart >/dev/null 2>&1 || sudo systemctl restart mariadb >/dev/null 2>&1
    for _ in $(seq 1 60); do mysql -N -B -e "SELECT 1" >/dev/null 2>&1 && return 0; sleep 2; done
    echo "server did not come back after restart" >&2; exit 1
}

# Times one statement twice: the first read after a restart (COLD -- empty pool, storage
# reads) and an immediate re-issue (WARM). %.0f ms via the shell's SECONDS is too coarse,
# so timing is taken around the client with date +%s%N.
probe() {  # $1 = label, $2 = sql
    restart
    local t0 t1 cold warm
    t0=$(date +%s%N); mysql -N -B "$DB" -e "$2" >/dev/null 2>&1; t1=$(date +%s%N)
    cold=$(( (t1 - t0) / 1000000 ))
    t0=$(date +%s%N); mysql -N -B "$DB" -e "$2" >/dev/null 2>&1; t1=$(date +%s%N)
    warm=$(( (t1 - t0) / 1000000 ))
    printf '  %-40s %10s %10s\n' "$1" "${cold}" "${warm}"
}

echo "server: $(mysql -N -B -e 'SELECT VERSION();')"
echo "pool:   $(mysql -N -B -e 'SELECT @@innodb_buffer_pool_size;') B"
echo "cold = first query after a full server restart (empty buffer pool)."
printf '\n  %-40s %10s %10s\n' "query" "cold_ms" "warm_ms"
echo "  ----------------------------------------------------------------"

probe "split:   one tenant, 24h" \
    "SELECT COUNT(DISTINCT archive_id) FROM side_d$(printf '%04d' "$K") WHERE $PRED;"
probe "unified: one tenant, 24h" \
    "SELECT COUNT(DISTINCT archive_id) FROM side_all WHERE dataset_id=$K AND $PRED;"

UNION=""
for j in $(seq 0 $((DATASETS - 1))); do
    [ -n "$UNION" ] && UNION="$UNION UNION ALL "
    UNION="${UNION}SELECT COUNT(DISTINCT archive_id) c FROM side_d$(printf '%04d' "$j") WHERE $PRED"
done
probe "split:   ALL tenants (UNION ALL)" "SELECT SUM(c) FROM ($UNION) u;"

INLIST=$(seq -s, 0 $((DATASETS - 1)))
probe "unified: ALL tenants (IN-list)" \
    "SELECT COUNT(DISTINCT dataset_id, archive_id) FROM side_all
     WHERE dataset_id IN ($INLIST) AND $PRED;"
probe "unified: dataset_id OMITTED (the trap)" \
    "SELECT COUNT(DISTINCT dataset_id, archive_id) FROM side_all WHERE $PRED;"

echo
echo "  Each row restarted the server first, so cold is a genuinely empty pool."
echo "  warm is the same statement re-issued -- the gap is the storage-read cost."
