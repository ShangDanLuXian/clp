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
#   B. STORED size, decomposed into data_length (the clustered PK B-tree -- in InnoDB the
#      table IS its primary key) and index_length (all SECONDARY indexes). The charset does
#      NOT change bytes on disk -- records hold the bytes
#      present, never the declared maximum. Same rows into all three types must weigh the same.
#      A side table reports index_length = 0 because its PK already IS the inverted index.
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
printf '\n=== B  stored size, decomposed (%s rows in every structure) ===\n' "$ROWS"
$CLIENT "$DB" -e "CREATE TABLE d (n INT);
  INSERT INTO d VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);"
ROWSRC="(SELECT a.n + b.n*10 + c.n*100 + d.n*1000 + e.n*10000 AS n
         FROM d a, d b, d c, d d, d e) s WHERE n < $ROWS"

# data_length is the CLUSTERED index (the PK B-tree -- in InnoDB the table IS its primary
# key). index_length is every SECONDARY index added together. So index% reads "what share
# of this table is secondary indexes", NOT "what share is index": a side table shows 0.0%
# precisely because its PK already is the inverted index and it needs no secondary.
sizes() {  # $1 = table -> "data_bytes index_bytes pct"
    $CLIENT "$DB" -e "ANALYZE TABLE $1;" >/dev/null 2>&1
    q "SELECT CONCAT_WS(' ', COALESCE(data_length,0), COALESCE(index_length,0),
         COALESCE(ROUND(100*index_length/NULLIF(data_length+index_length,0),1),0))
       FROM information_schema.tables
       WHERE table_schema='$DB' AND table_name='$1';" "$DB"
}
row() {  # $1 = label, $2 = table, $3 = verdict
    read -r dl il pc <<<"$(sizes "$2")"
    awk -v l="$1" -v d="$dl" -v i="$il" -v p="$pc" -v r="$ROWS" -v v="$3" 'BEGIN{
        printf "  %-32s %9.2f %10.2f %8s%% %8.1f   %s\n", l, d/1048576, i/1048576, p, (d+i)/r, v}'
}
printf '  %-32s %9s %10s %9s %8s   %s\n' structure data_MB index_MB "index%" B/row verdict

# The side table: a clustered PK and nothing else. All three declarations must weigh the same.
BASELINE=""
for pair in "side table, VARCHAR ascii|VARCHAR(64) CHARACTER SET ascii" \
            "side table, VARCHAR utf8mb4|VARCHAR(64) CHARACTER SET utf8mb4" \
            "side table, VARBINARY|VARBINARY(255)"; do
    IFS='|' read -r slabel spec <<<"$pair"
    $CLIENT "$DB" -e "DROP TABLE IF EXISTS t; CREATE TABLE t (
        column_id TINYINT UNSIGNED NOT NULL, value $spec NOT NULL,
        begin_timestamp BIGINT NOT NULL, archive_id INT UNSIGNED NOT NULL,
        PRIMARY KEY (column_id, value, begin_timestamp, archive_id)) ENGINE=InnoDB;
      INSERT INTO t SELECT n % 8, CONCAT('host-', LPAD(n % 1000, 6, '0')), 1700000000 + n, n
        FROM $ROWSRC;"
    read -r dl il _ <<<"$(sizes t)"
    tot=$((dl + il))
    if [ -z "$BASELINE" ]; then BASELINE=$tot; v="baseline"
    elif [ "$tot" = "$BASELINE" ]; then v="PASS  same bytes"
    else v="FAIL  differs by $((tot - BASELINE))"; FAILED=1; fi
    row "$slabel" t "$v"
    $CLIENT "$DB" -e "DROP TABLE IF EXISTS t;"
done

# The inline alternative: an ordinary archives row, then one secondary index per filter
# column. data_length never moves -- the PK is unchanged -- while index_length climbs.
$CLIENT "$DB" -e "DROP TABLE IF EXISTS b; CREATE TABLE b (
    archive_id INT UNSIGNED NOT NULL, begin_timestamp BIGINT NOT NULL,
    value VARCHAR(64) NOT NULL, PRIMARY KEY (archive_id, begin_timestamp)) ENGINE=InnoDB;
  INSERT INTO b SELECT n, 1700000000 + n, CONCAT('host-', LPAD(n % 1000, 6, '0'))
    FROM $ROWSRC;"
row "archives row, PK only" b "no secondary index yet"
$CLIENT "$DB" -e "ALTER TABLE b ADD KEY ix_value (value, begin_timestamp);"
row "archives row + 1 secondary" b "cost of indexing 1 column inline"
$CLIENT "$DB" -e "ALTER TABLE b ADD KEY ix_ts (begin_timestamp);"
row "archives row + 2 secondaries" b "each column adds to index_MB"

printf '\n%s\n' "$([ $FAILED -eq 0 ] && echo 'ALL CHECKS PASSED' || echo 'SOME CHECKS FAILED')"
exit $FAILED
