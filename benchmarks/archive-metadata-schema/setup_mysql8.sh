#!/usr/bin/env bash
# Installs a MySQL 8 server ALONGSIDE an existing MariaDB, for cross-engine benchmarking.
#
# `apt install mysql-server` would remove MariaDB (the packages conflict), so this extracts
# MySQL's binaries into a private prefix instead and runs them from a separate datadir on a
# separate port. Nothing about the existing MariaDB install is touched.
#
# Usage:  sudo ./setup_mysql8.sh [PORT] [BUFFER_POOL]      (defaults: 3307 4G)
set -euo pipefail

PORT="${1:-3307}"
BUFFER_POOL="${2:-4G}"
PREFIX=/opt/mysql8
DATADIR=/var/lib/mysql8
CONF=/etc/my8.cnf
SOCKET=/run/mysqld/mysql8.sock

[ "$(id -u)" -eq 0 ] || { echo "run with sudo" >&2; exit 1; }

echo "==> downloading MySQL 8 packages (not installing them)"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
cd "$TMP"
apt-get download mysql-server-core-8.0 mysql-client-core-8.0 mysql-common

echo "==> installing shared-library dependencies only (skipping conflicting mysql-* packages)"
DEPS=$(for d in *.deb; do dpkg-deb -f "$d" Depends; done \
       | tr ',' '\n' | sed 's/(.*)//;s/|.*//' | tr -d ' ' \
       | grep -Ev '^(mysql|mariadb|debconf|adduser|passwd)' | sort -u | tr '\n' ' ')
# shellcheck disable=SC2086
apt-get install -y --no-install-recommends $DEPS

echo "==> extracting to $PREFIX"
mkdir -p "$PREFIX"
for d in *.deb; do dpkg -x "$d" "$PREFIX"; done
MYSQLD="$PREFIX/usr/sbin/mysqld"
"$MYSQLD" --version || { echo "mysqld will not run; see missing libs above" >&2; exit 1; }

echo "==> writing $CONF (port $PORT, buffer pool $BUFFER_POOL)"
cat > "$CONF" <<EOF
[mysqld]
user=mysql
basedir=$PREFIX/usr
datadir=$DATADIR
socket=$SOCKET
port=$PORT
mysqlx=0
log-error=/var/log/mysql8/error.log
innodb_buffer_pool_size=$BUFFER_POOL
innodb_flush_log_at_trx_commit=2
local_infile=1
secure_file_priv=
[mysql]
socket=$SOCKET
port=$PORT
local-infile=1
EOF

mkdir -p "$DATADIR" /var/log/mysql8 /run/mysqld
chown -R mysql:mysql "$DATADIR" /var/log/mysql8 /run/mysqld

if [ -d "$DATADIR/mysql" ]; then
    echo "==> $DATADIR already initialized, keeping it"
else
    echo "==> initializing datadir (root with no password, local-only instance)"
    "$MYSQLD" --defaults-file="$CONF" --initialize-insecure
fi

echo "==> starting"
"$MYSQLD" --defaults-file="$CONF" >/dev/null 2>&1 &
for _ in $(seq 1 40); do
    sleep 2
    if "$PREFIX/usr/bin/mysqladmin" --defaults-file="$CONF" -u root ping >/dev/null 2>&1; then
        break
    fi
done
"$PREFIX/usr/bin/mysqladmin" --defaults-file="$CONF" -u root ping \
    || { echo "did not start; check /var/log/mysql8/error.log" >&2; exit 1; }

"$PREFIX/usr/bin/mysql" --defaults-file="$CONF" -u root -e "SELECT VERSION();"
cat <<EOF

MySQL 8 is running on port $PORT, independent of MariaDB.

  benchmark both:  python3 bench_lc.py \\
                     --engine mariadb="mariadb" \\
                     --engine mysql="$PREFIX/usr/bin/mysql --defaults-file=$CONF -u root"
  stop:            $PREFIX/usr/bin/mysqladmin --defaults-file=$CONF -u root shutdown
  start again:     $MYSQLD --defaults-file=$CONF &

Give both engines the same innodb_buffer_pool_size or the comparison is meaningless;
MariaDB's is set in /etc/mysql/mariadb.conf.d/.
EOF
