#!/usr/bin/env bash
# Round 4: runs all follow-up experiments (or a subset: ./bench4.sh --exp B2 --exp B9).
set -euo pipefail
cd "$(dirname "$0")"
CLIENT=$(command -v mysql || command -v mariadb)
"$CLIENT" -e "DROP DATABASE IF EXISTS lc4;" 2>/dev/null || true
MY8="/opt/mysql8/usr/bin/mysql --defaults-file=/etc/my8.cnf -u root"
$MY8 -e "DROP DATABASE IF EXISTS lc4;" 2>/dev/null || true
sudo "$CLIENT" -e "GRANT ALL PRIVILEGES ON lc4.* TO '$USER'@'localhost';" 2>/dev/null || true
python3 bench4.py "$@"
