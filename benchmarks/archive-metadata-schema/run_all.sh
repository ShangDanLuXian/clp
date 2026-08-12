#!/usr/bin/env bash
# One command: clean up, make sure the servers exist and are tuned, then run round 2.
#
#   ./run_all.sh                       clean + verify setup + run (100K then 1M archives)
#   ./run_all.sh --with-mysql          also install/configure MySQL 8 for the cross-engine run
#   ./run_all.sh --scale 100000        anything after the flags is passed to bench_lc2.py
#   ./run_all.sh --clean-only          just reclaim space and exit
#
# Run as your normal user, NOT as root: it sudo's only for the steps that need it, so the
# results file stays yours. Everything it does is idempotent and safe to re-run.
set -euo pipefail
cd "$(dirname "$0")"

WITH_MYSQL=0
CLEAN_ONLY=0
ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --with-mysql) WITH_MYSQL=1 ;;
        --clean-only) CLEAN_ONLY=1 ;;
        *) ARGS+=("$1") ;;
    esac
    shift
done

MY8_CNF=/etc/my8.cnf
MY8_CLIENT=/opt/mysql8/usr/bin/mysql
MY8_SERVER=/opt/mysql8/usr/sbin/mysqld
say() { printf '\n=== %s\n' "$1"; }

# ---------------------------------------------------------------- 1. base server
say "checking MariaDB"
CLIENT=$(command -v mysql || command -v mariadb || true)
if [ -z "$CLIENT" ] || ! "$CLIENT" -e "SELECT 1;" >/dev/null 2>&1; then
    echo "    not usable; running setup_db.sh"
    sudo ./setup_db.sh
    CLIENT=$(command -v mysql || command -v mariadb)
fi
echo "    $("$CLIENT" -N -B -e 'SELECT VERSION();')"

# ---------------------------------------------------------------- 2. tuning
# A 125M-row random-key load fills a small redo log faster than the checkpointer can reclaim
# it; InnoDB then throttles the writer and the load looks hung. Raise it once, here.
REDO_MB=$("$CLIENT" -N -B -e "SELECT ROUND(@@innodb_log_file_size/1048576);" 2>/dev/null || echo 0)
if [ "${REDO_MB:-0}" -lt 1024 ]; then
    say "raising MariaDB redo log ${REDO_MB}M -> 4G (restart required)"
    sudo mkdir -p /etc/mysql/conf.d
    printf '[mysqld]\ninnodb_log_file_size = 4G\n' \
        | sudo tee /etc/mysql/conf.d/98-bench-redo.cnf >/dev/null
    sudo systemctl restart mariadb 2>/dev/null \
        || sudo service mariadb restart 2>/dev/null \
        || { sudo pkill -f mariadbd || true; sleep 5;
             sudo bash -c '(mariadbd-safe --skip-syslog >/dev/null 2>&1 &)'; }
    for _ in $(seq 1 60); do "$CLIENT" -e "SELECT 1;" >/dev/null 2>&1 && break; sleep 2; done
    echo "    now $("$CLIENT" -N -B -e 'SELECT ROUND(@@innodb_log_file_size/1048576);')M"
fi

# ---------------------------------------------------------------- 3. optional MySQL
if [ "$WITH_MYSQL" -eq 1 ]; then
    say "checking MySQL 8"
    if [ ! -x "$MY8_SERVER" ]; then
        echo "    not installed; running setup_mysql8.sh"
        sudo ./setup_mysql8.sh
    fi
    # Applies to installs predating these settings; both are needed for a fair comparison.
    for opt in "skip-log-bin" "innodb_redo_log_capacity=4G"; do
        if ! sudo grep -qF "$opt" "$MY8_CNF" 2>/dev/null; then
            echo "    adding $opt to $MY8_CNF"
            sudo sed -i "/^\[mysqld\]/a $opt" "$MY8_CNF"
            NEED_MY8_RESTART=1
        fi
    done
    if [ "${NEED_MY8_RESTART:-0}" -eq 1 ] || ! "$MY8_CLIENT" --defaults-file="$MY8_CNF" \
            -u root -e "SELECT 1;" >/dev/null 2>&1; then
        echo "    restarting MySQL 8"
        sudo "$MY8_CLIENT" --defaults-file="$MY8_CNF" -u root -e "SHUTDOWN;" 2>/dev/null || true
        sleep 5
        sudo rm -f /run/mysqld/mysql8.sock.lock
        sudo bash -c "($MY8_SERVER --defaults-file=$MY8_CNF >/dev/null 2>&1 &)"
        for _ in $(seq 1 90); do
            "$MY8_CLIENT" --defaults-file="$MY8_CNF" -u root -e "SELECT 1;" >/dev/null 2>&1 \
                && break
            sleep 2
        done
    fi
    "$MY8_CLIENT" --defaults-file="$MY8_CNF" -u root -e "SELECT VERSION();" 2>/dev/null \
        | tail -1 | sed 's/^/    /' || echo "    WARNING: MySQL 8 did not come up"
fi

# ---------------------------------------------------------------- 4. clean slate
say "clearing previous state"
"$CLIENT" -e "DROP DATABASE IF EXISTS lcbench;" 2>/dev/null && echo "    dropped lcbench (mariadb)"
if [ -x "$MY8_CLIENT" ]; then
    "$MY8_CLIENT" --defaults-file="$MY8_CNF" -u root -e "DROP DATABASE IF EXISTS lcbench;" \
        2>/dev/null && echo "    dropped lcbench (mysql)"
fi
# Staging dirs orphaned by an aborted run. Matched by content so nothing else is touched.
FREED=0
for d in "${TMPDIR:-/tmp}"/tmp*/; do
    [ -d "$d" ] || continue
    if [ -f "$d/side_ts.tsv" ] || [ -f "$d/inline.tsv" ] || [ -f "$d/delim.tsv" ]; then
        rm -rf "$d" && FREED=$((FREED + 1))
    fi
done
[ "$FREED" -gt 0 ] && echo "    removed $FREED orphaned staging dir(s)"
echo "    disk now: $(df -h . | awk 'NR==2{print $4" free"}')"
[ "$CLEAN_ONLY" -eq 1 ] && { echo; echo "clean-only: done."; exit 0; }

# ---------------------------------------------------------------- 5. run
STAMP=$(date +%Y%m%d-%H%M%S)
OUT="lc2_results_${STAMP}.txt"
LOG="lc2_progress_${STAMP}.log"
say "running benchmark -> $OUT (progress in $LOG)"
echo "    the 1M-archive step loads 125M rows per side variant; expect a long run."
echo "    pass --scale 100000 for a fast pass."
ENGINES=(--engine "mariadb=$CLIENT")
[ "$WITH_MYSQL" -eq 1 ] && [ -x "$MY8_CLIENT" ] \
    && ENGINES+=(--engine "mysql=$MY8_CLIENT --defaults-file=$MY8_CNF -u root")
START=$(date +%s)
set +e
python3 bench_lc2.py "${ENGINES[@]}" --out "$OUT" "${ARGS[@]}" 2>&1 | tee "$LOG"
RC=${PIPESTATUS[0]}
set -e
say "finished in $(( ($(date +%s) - START) / 60 )) min (exit $RC)"
echo "    results:  $OUT"
echo "    progress: $LOG"
exit "$RC"
