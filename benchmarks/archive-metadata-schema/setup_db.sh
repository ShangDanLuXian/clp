#!/usr/bin/env bash
# Prepares a machine to run these benchmarks: installs a database server if none is present,
# applies the benchmark configuration, starts it (works with or without systemd, so it is
# WSL-safe), and grants the calling user everything the harnesses need.
#
# Usage:  sudo ./setup_db.sh
set -euo pipefail

[ "$(id -u)" -eq 0 ] || { echo "run with sudo" >&2; exit 1; }
TARGET_USER="${SUDO_USER:-root}"

echo "==> checking for a database server"
if ! command -v mariadbd >/dev/null 2>&1 && ! command -v mysqld >/dev/null 2>&1; then
    echo "    none found; installing mariadb-server"
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y mariadb-server
else
    echo "    already installed"
fi

# Half of RAM, capped at 4G. WSL hands the VM a fraction of host memory, so a hardcoded 4G
# can push the box into swap or OOM on a small allocation.
TOTAL_MB=$(( $(awk '/MemTotal/{print $2}' /proc/meminfo) / 1024 ))
BP_MB=$(( TOTAL_MB / 2 ))
[ "$BP_MB" -gt 4096 ] && BP_MB=4096
[ "$BP_MB" -lt 512 ] && BP_MB=512
echo "==> configuring (buffer pool ${BP_MB}M of ${TOTAL_MB}M total RAM)"
mkdir -p /etc/mysql/conf.d
cat > /etc/mysql/conf.d/99-bench.cnf <<EOF
[mysqld]
innodb_buffer_pool_size = ${BP_MB}M
innodb_flush_log_at_trx_commit = 2
local_infile = 1
[mysql]
local-infile = 1
EOF

echo "==> starting the server"
start_db() {
    if [ -d /run/systemd/system ] && command -v systemctl >/dev/null 2>&1; then
        systemctl restart mariadb 2>/dev/null && return 0
        systemctl restart mysql 2>/dev/null && return 0
    fi
    # WSL without systemd: the SysV wrapper still works.
    if command -v service >/dev/null 2>&1; then
        service mariadb restart 2>/dev/null && return 0
        service mysql restart 2>/dev/null && return 0
    fi
    # Last resort: launch the daemon directly.
    mkdir -p /run/mysqld && chown mysql:mysql /run/mysqld
    if command -v mariadbd-safe >/dev/null 2>&1; then
        (mariadbd-safe --skip-syslog >/dev/null 2>&1 &)
    else
        (mysqld_safe >/dev/null 2>&1 &)
    fi
    return 0
}
start_db

CLIENT=$(command -v mysql || command -v mariadb)
[ -n "$CLIENT" ] || { echo "no client binary found" >&2; exit 1; }
for _ in $(seq 1 40); do
    "$CLIENT" -e "SELECT 1;" >/dev/null 2>&1 && break
    sleep 2
done
"$CLIENT" -e "SELECT 1;" >/dev/null 2>&1 \
    || { echo "server did not come up; check the error log under /var/log/mysql/" >&2; exit 1; }

VERSION=$("$CLIENT" -N -B -e "SELECT VERSION();")
echo "    up: $VERSION"

# Socket authentication is spelled differently by the two engines.
case "${VERSION,,}" in
    *mariadb*) AUTH="IDENTIFIED VIA unix_socket" ;;
    *)         AUTH="IDENTIFIED WITH auth_socket" ;;
esac

echo "==> granting privileges to '$TARGET_USER'"
# bench   = the archives/Pack harness (schema.sql, e*.sql)
# lcbench = the low-cardinality encoding harness (bench_lc.py)
# RELOAD is global and is required by FLUSH STATUS, which is how rows-scanned is counted.
"$CLIENT" <<EOF
CREATE USER IF NOT EXISTS '$TARGET_USER'@'localhost' $AUTH;
GRANT ALL PRIVILEGES ON bench.* TO '$TARGET_USER'@'localhost';
GRANT ALL PRIVILEGES ON lcbench.* TO '$TARGET_USER'@'localhost';
GRANT RELOAD ON *.* TO '$TARGET_USER'@'localhost';
FLUSH PRIVILEGES;
EOF

echo "==> verifying as '$TARGET_USER'"
sudo -u "$TARGET_USER" "$CLIENT" -e \
    "CREATE DATABASE IF NOT EXISTS lcbench; USE lcbench;
     CREATE TABLE IF NOT EXISTS __probe (i INT); FLUSH STATUS; DROP TABLE __probe;" \
    || { echo "verification failed" >&2; exit 1; }

cat <<EOF

Ready. As $TARGET_USER (no sudo, no password):

  ./load.sh                 # archives/Pack harness: generate + load (~20 min, ~40 GB disk)
  python3 bench_lc.py       # low-cardinality encoding harness (self-contained)

To also benchmark MySQL against MariaDB, run ./setup_mysql8.sh next -- it installs MySQL 8
alongside on port 3307 without evicting MariaDB.
EOF
