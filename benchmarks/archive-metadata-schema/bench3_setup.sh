#!/usr/bin/env bash
# Round 3, script 1 of 3: clean previous state, make sure both servers are up and tuned,
# then BUILD the lc3 database (which persists for query runs).
#
#   ./bench3_setup.sh                          quick build (0.05 arch/s x 28d)
#   ./bench3_setup.sh --rate 0.5               full build (~3B side rows; hours + ~500GB)
#   ./bench3_setup.sh --design side_percol     build a subset of designs
#
# Run as your normal user; sudo is used only where needed.
set -euo pipefail
cd "$(dirname "$0")"

MY8_CNF=/etc/my8.cnf
MY8_CLIENT=/opt/mysql8/usr/bin/mysql
MY8_SERVER=/opt/mysql8/usr/sbin/mysqld
say() { printf '\n=== %s\n' "$1"; }

say "MariaDB"
CLIENT=$(command -v mysql || command -v mariadb || true)
if [ -z "$CLIENT" ] || ! "$CLIENT" -e "SELECT 1;" >/dev/null 2>&1; then
    sudo ./setup_db.sh
    CLIENT=$(command -v mysql || command -v mariadb)
fi
sudo "$CLIENT" -e "GRANT ALL PRIVILEGES ON lc3.* TO '$USER'@'localhost';
                   GRANT RELOAD ON *.* TO '$USER'@'localhost';" 2>/dev/null \
    || "$CLIENT" -e "SELECT 1;" >/dev/null
REDO_MB=$("$CLIENT" -N -B -e "SELECT ROUND(@@innodb_log_file_size/1048576);" 2>/dev/null || echo 0)
if [ "${REDO_MB:-0}" -lt 1024 ]; then
    say "raising MariaDB redo log ${REDO_MB}M -> 4G"
    printf '[mysqld]\ninnodb_log_file_size = 4G\n' \
        | sudo tee /etc/mysql/conf.d/98-bench-redo.cnf >/dev/null
    sudo systemctl restart mariadb 2>/dev/null || sudo service mariadb restart 2>/dev/null || {
        sudo pkill -f mariadbd || true
        while pgrep -f '[m]ariadbd' >/dev/null; do sleep 2; done
        sudo bash -c '(mariadbd-safe --skip-syslog >/dev/null 2>&1 &)'
    }
    for _ in $(seq 1 120); do "$CLIENT" -e "SELECT 1;" >/dev/null 2>&1 && break; sleep 3; done
fi
echo "    $("$CLIENT" -N -B -e 'SELECT VERSION();')"

say "MySQL 8"
if [ ! -x "$MY8_SERVER" ]; then
    echo "    not installed; running setup_mysql8.sh"
    sudo ./setup_mysql8.sh
fi
NEED_RESTART=0
for opt in "skip-log-bin" "innodb_redo_log_capacity=4G"; do
    if ! sudo grep -qF "$opt" "$MY8_CNF" 2>/dev/null; then
        sudo sed -i "/^\[mysqld\]/a $opt" "$MY8_CNF"
        NEED_RESTART=1
    fi
done
MY8="$MY8_CLIENT --defaults-file=$MY8_CNF -u root"
if [ "$NEED_RESTART" -eq 1 ] || ! $MY8 -e "SELECT 1;" >/dev/null 2>&1; then
    echo "    (re)starting MySQL 8"
    $MY8 -e "SHUTDOWN;" 2>/dev/null || true
    # Wait for the OLD process to actually release the datadir before starting a new one:
    # launching over a still-flushing shutdown is how a restart "fails" with a locked ibdata.
    for _ in $(seq 1 150); do
        pgrep -f "[m]ysqld --defaults-file=$MY8_CNF" >/dev/null || break
        sleep 2
    done
    sudo rm -f /run/mysqld/mysql8.sock.lock
    sudo bash -c "($MY8_SERVER --defaults-file=$MY8_CNF >/dev/null 2>&1 &)"
    for _ in $(seq 1 200); do $MY8 -e "SELECT 1;" >/dev/null 2>&1 && break; sleep 3; done
fi
if $MY8 -e "SELECT VERSION();" >/dev/null 2>&1; then
    echo "    $($MY8 -N -B -e 'SELECT VERSION();')"
    MY8_OK=1
else
    echo "    WARNING: MySQL 8 did not come up; last error-log lines:"
    sudo tail -8 /var/log/mysql8/error.log 2>/dev/null | sed 's/^/      /'
    echo "    continuing with MariaDB only"
    MY8_OK=0
fi

say "clearing previous state"
"$CLIENT" -e "DROP DATABASE IF EXISTS lc3;" 2>/dev/null && echo "    dropped lc3 (mariadb)"
[ "${MY8_OK:-0}" -eq 1 ] && $MY8 -e "DROP DATABASE IF EXISTS lc3;" 2>/dev/null \
    && echo "    dropped lc3 (mysql)"
for d in "${TMPDIR:-/tmp}"/tmp*/; do
    [ -f "${d}base.tsv" ] || [ -f "${d}side_ts.tsv" ] && rm -rf "$d" \
        && echo "    removed orphaned staging dir"
done
rm -f lc3_manifest.json
echo "    disk: $(df -h . | awk 'NR==2{print $4" free"}')"

say "building"
ENGINES=(--engine "mariadb=$CLIENT")
[ "${MY8_OK:-0}" -eq 1 ] && ENGINES+=(--engine "mysql=$MY8")
python3 bench3.py build "${ENGINES[@]}" "$@"
