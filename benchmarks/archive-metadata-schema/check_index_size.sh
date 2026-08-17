#!/usr/bin/env bash
# Standalone verification of the index-key size rule for the side table's value column.
# Uses its own database, generates its rows in SQL, and finishes in seconds -- it proves the
# rule holds on YOUR server and version, it is not a benchmark.
#
# Two claims, checked independently because they are constantly conflated:
#
#   A. DECLARED ceiling. InnoDB caps one index key at 3072 bytes and charges a string column
#      `declared_length x charset_max_bytes_per_char`. The value column shares that budget
#      with the other 3 PK columns (1 + 8 + 4 = 13 bytes), so the widest declaration that
#      creates is (3072 - 13) / bytes-per-char. Checked at CREATE TABLE; allocates nothing.
#   B. STORED size. The charset does NOT change bytes on disk -- records hold the bytes
#      present, never the declared maximum. Same rows into all three types must weigh the same.
#
# Usage:  ./check_index_size.sh                                  # client from PATH
#         ./check_index_size.sh "mysql" 200000                   # explicit client, more rows
#         ./check_index_size.sh "/opt/mysql8/usr/bin/mysql --defaults-file=/etc/my8.cnf -u root"
set -uo pipefail

CLIENT="${1:-$(command -v mysql || command -v mariadb)}"
ROWS="${2:-100000}"
DB="${IDXSIZE_DB:-idxsize}"
# column_id TINYINT(1) + begin_timestamp BIGINT(8) + archive_id INT(4)
PK_OTHER=13
LIMIT=3072
FAILED=0

q() { $CLIENT -N -B ${2:+"$2"} -e "$1" 2>&1; }
$CLIENT -e "SELECT 1" >/dev/null 2>&1 || { echo "no server via: $CLIENT" >&2; exit 1; }
# Report the server's own message: "access denied" and "unknown database" need different
# fixes, and swallowing the error leaves the reader guessing which one they hit.
if ! ERR=$($CLIENT -e "DROP DATABASE IF EXISTS $DB; CREATE DATABASE $DB;" 2>&1) || [ -n "$ERR" ]
then
    printf '%s\n' "$ERR" >&2
    echo "  grant it with:" >&2
    echo "    sudo mysql -e \"GRANT ALL PRIVILEGES ON $DB.* TO '$USER'@'localhost';\"" >&2
    exit 1
fi
trap '$CLIENT -e "DROP DATABASE IF EXISTS $DB;" 2>/dev/null' EXIT
echo "server: $(q 'SELECT VERSION();')   rows: $ROWS"

# --- A: does the declared ceiling land exactly where the arithmetic says? ------------------
mk() {  # $1 = value column definition -> prints "ok" or the error
  out=$($CLIENT "$DB" -e "DROP TABLE IF EXISTS k; CREATE TABLE k (
      column_id TINYINT UNSIGNED NOT NULL, value $1 NOT NULL,
      begin_timestamp BIGINT NOT NULL, archive_id INT UNSIGNED NOT NULL,
      PRIMARY KEY (column_id, value, begin_timestamp, archive_id)) ENGINE=InnoDB;" 2>&1)
  [ -z "$out" ] && echo ok || echo "$out" | grep -oE 'ERROR [0-9]+' | head -1
}
# The width placeholder is %N% rather than a bare letter: a bare N also matches the one
# inside VARBINARY, which silently rewrites the type name instead of the width.
SPECS=(
    "VARCHAR utf8mb4|VARCHAR(%N%) CHARACTER SET utf8mb4|4"
    "VARCHAR utf8mb3|VARCHAR(%N%) CHARACTER SET utf8mb3|3"
    "VARCHAR ascii|VARCHAR(%N%) CHARACTER SET ascii|1"
    "VARBINARY|VARBINARY(%N%)|1"
)
printf '\n=== A  declared ceiling: (%d - %d) / bytes-per-char ===\n' "$LIMIT" "$PK_OTHER"
printf '  %-18s %6s %8s %10s %11s   %s\n' encoding b/char max at-max over-max verdict
for s in "${SPECS[@]}"; do
    IFS='|' read -r label tmpl mbmax <<<"$s"
    max=$(( (LIMIT - PK_OTHER) / mbmax ))
    at=$(mk "${tmpl//%N%/$max}")
    over=$(mk "${tmpl//%N%/$((max + 1))}")
    if [ "$at" = "ok" ] && [ "$over" = "ERROR 1071" ]; then v="PASS"; else v="FAIL"; FAILED=1; fi
    printf '  %-18s %6d %8d %10s %11s   %s\n' "$label" "$mbmax" "$max" "$at" "$over" "$v"
done
$CLIENT "$DB" -e "DROP TABLE IF EXISTS k;" 2>/dev/null

# --- B: identical rows into each type must occupy identical bytes -------------------------
# Rows are built by cross-joining a 10-row digit table, so no file staging and no recursion
# limits (MySQL caps recursive CTEs at 1000 by default, MariaDB does not).
printf '\n=== B  stored size: same %s rows, three declarations ===\n' "$ROWS"
$CLIENT "$DB" -e "CREATE TABLE d (n INT);
  INSERT INTO d VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);"
BASE=""
printf '  %-34s %14s   %s\n' "value type" bytes verdict
for spec in "VARCHAR(64) CHARACTER SET ascii" "VARCHAR(64) CHARACTER SET utf8mb4" \
            "VARBINARY(255)"; do
    $CLIENT "$DB" -e "DROP TABLE IF EXISTS t; CREATE TABLE t (
        column_id TINYINT UNSIGNED NOT NULL, value $spec NOT NULL,
        begin_timestamp BIGINT NOT NULL, archive_id INT UNSIGNED NOT NULL,
        PRIMARY KEY (column_id, value, begin_timestamp, archive_id)) ENGINE=InnoDB;
      INSERT INTO t SELECT n % 8, CONCAT('host-', LPAD(n % 1000, 6, '0')), 1700000000 + n, n
        FROM (SELECT a.n + b.n*10 + c.n*100 + d.n*1000 + e.n*10000 AS n
              FROM d a, d b, d c, d d, d e) s WHERE n < $ROWS;"
    # information_schema serves CACHED stats; without ANALYZE a fresh
    # table reads far too small.
    $CLIENT "$DB" -e "ANALYZE TABLE t;" >/dev/null 2>&1
    by=$(q "SELECT COALESCE(data_length,0)+COALESCE(index_length,0)
            FROM information_schema.tables WHERE table_schema='$DB' AND table_name='t';" "$DB")
    if [ -z "$BASE" ]; then BASE=$by; v="baseline"
    elif [ "$by" = "$BASE" ]; then v="PASS  identical"
    else v="FAIL  differs by $((by - BASE)) bytes"; FAILED=1; fi
    printf '  %-34s %14s   %s\n' "$spec" "$by" "$v"
done

printf '\n%s\n' "$([ $FAILED -eq 0 ] && echo 'ALL CHECKS PASSED' || echo 'SOME CHECKS FAILED')"
exit $FAILED
