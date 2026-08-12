#!/usr/bin/env bash
# Round 3, script 2 of 3: run query/write/interference experiments against the EXISTING
# lc3 database built by bench3_setup.sh. Re-runnable; the database is not modified except
# by the write-path tests (fresh ids at the end of history, away from the query windows).
#
#   ./bench3_query.sh                             full matrix
#   ./bench3_query.sh --query Q3_sel_hot --window 7d
#   ./bench3_query.sh --design side_percol --skip-writes
#   ./bench3_query.sh --join-form exists          alternate 2-predicate SQL form
#
# For COLD numbers: restart the database server(s), then run this immediately -- the
# run1_ms column of the first pass is then a true cold measurement.
set -euo pipefail
cd "$(dirname "$0")"
[ -f lc3_manifest.json ] || { echo "no lc3_manifest.json -- run ./bench3_setup.sh first" >&2; exit 1; }

CLIENT=$(command -v mysql || command -v mariadb)
ENGINES=(--engine "mariadb=$CLIENT")
MY8="/opt/mysql8/usr/bin/mysql --defaults-file=/etc/my8.cnf -u root"
if /opt/mysql8/usr/bin/mysql --defaults-file=/etc/my8.cnf -u root -e "SELECT 1;" >/dev/null 2>&1; then
    ENGINES+=(--engine "mysql=$MY8")
fi
python3 bench3.py query "${ENGINES[@]}" "$@"
