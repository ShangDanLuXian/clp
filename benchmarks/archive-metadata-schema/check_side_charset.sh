#!/usr/bin/env bash
# Sanity check: does the side table's DEFAULT CHARSET change how big it is on disk?
#
# Builds the real DDL -- clustered PK, no secondaries, partitioned by hour -- once per
# encoding, loads identical rows into each, and prints the sizes. Seconds, not minutes.
#
# Usage:  ./check_side_charset.sh [CLIENT] [ROWS] [HOURS]      (defaults: auto 1000000 24)
set -uo pipefail

CLIENT="${1:-$(command -v mysql || command -v mariadb)}"
ROWS="${2:-1000000}"
HOURS="${3:-24}"
DB="${SIDECHARSET_DB:-sidecharset}"
T0=1704067200000000000                    # epoch for the partition range, in nanoseconds
HOUR=3600000000000

$CLIENT -e "SELECT 1" >/dev/null 2>&1 || { echo "no server via: $CLIENT" >&2; exit 1; }
if ! ERR=$($CLIENT -e "DROP DATABASE IF EXISTS $DB; CREATE DATABASE $DB;" 2>&1) || [ -n "$ERR" ]
then
    printf '%s\n' "$ERR" >&2
    echo "    sudo mysql -e \"GRANT ALL PRIVILEGES ON $DB.* TO '$USER'@'localhost';\"" >&2
    exit 1
fi
trap '$CLIENT -e "DROP DATABASE IF EXISTS $DB;" 2>/dev/null' EXIT

# The design's partitioning: a floor, one partition per hour, and a MAXVALUE catch-all.
PARTS="PARTITION p_floor VALUES LESS THAN ($T0)"
for ((h = 0; h < HOURS; h++)); do
    PARTS="$PARTS, PARTITION p_h$(printf '%06d' "$h") VALUES LESS THAN ($((T0 + (h + 1) * HOUR)))"
done
PARTS="PARTITION BY RANGE (begin_timestamp) ($PARTS, PARTITION p_future VALUES LESS THAN MAXVALUE)"

$CLIENT "$DB" -e "CREATE TABLE d (n INT);
  INSERT INTO d VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);"
echo "server: $($CLIENT -N -B -e 'SELECT VERSION();')"
echo "$ROWS postings over $HOURS hourly partitions, identical rows in every variant"
printf '\n  %-20s %-9s %9s %10s %11s   %s\n' \
    "DEFAULT CHARSET" content data_MB B/row corrupted verdict

# Two corpora. The CJK one appends two 3-byte characters to every value, written as a hex
# literal so the result does not depend on this file's encoding or the client's SET NAMES.
ASCII_VAL="CONCAT('host-', LPAD(n % 1000, 6, '0'))"
CJK_VAL="CONCAT('host-', LPAD(n % 1000, 6, '0'), CONVERT(0xE4B8ADE69687 USING utf8mb4))"

BASE=""
for pair in "ascii|VARCHAR(64)|DEFAULT CHARSET = ascii|ASCII|$ASCII_VAL" \
            "utf8mb4|VARCHAR(64)|DEFAULT CHARSET = utf8mb4|ASCII|$ASCII_VAL" \
            "binary (VARBINARY)|VARBINARY(255)||ASCII|$ASCII_VAL" \
            "ascii|VARCHAR(64)|DEFAULT CHARSET = ascii|+2 CJK|$CJK_VAL" \
            "utf8mb4|VARCHAR(64)|DEFAULT CHARSET = utf8mb4|+2 CJK|$CJK_VAL" \
            "binary (VARBINARY)|VARBINARY(255)||+2 CJK|$CJK_VAL"; do
    IFS='|' read -r label vtype charset content valexpr <<<"$pair"
    $CLIENT "$DB" -e "DROP TABLE IF EXISTS s; CREATE TABLE s (
        column_id       TINYINT UNSIGNED NOT NULL,
        value           $vtype NOT NULL,
        begin_timestamp BIGINT NOT NULL,
        archive_id      INT UNSIGNED NOT NULL,
        PRIMARY KEY (column_id, value, begin_timestamp, archive_id)
      ) ENGINE = InnoDB $charset $PARTS;"
    # Captured, not ignored: an ascii column REJECTS a multibyte value outright here
    # (ERROR 1366 under the default strict mode) instead of substituting '?' the way
    # LOAD DATA does. Reporting the aborted table's size would be meaningless.
    IERR=$($CLIENT "$DB" -e "INSERT INTO s
        SELECT n % 8, $valexpr, $T0 + n * ($HOURS * $HOUR DIV $ROWS), n
        FROM (SELECT a.n + b.n*10 + c.n*100 + d.n*1000 + e.n*10000 + f.n*100000 AS n
              FROM d a, d b, d c, d d, d e, d f) s WHERE n < $ROWS;" 2>&1 \
        | grep -oE 'ERROR [0-9]+' | head -1)
    if [ -n "$IERR" ]; then
        printf '  %-20s %-9s %9s %10s %11s   %s\n' \
            "$label" "$content" "--" "--" "--" "REJECTED the value ($IERR)"
        $CLIENT "$DB" -e "DROP TABLE s;"
        continue
    fi
    # information_schema serves CACHED stats; without ANALYZE a fresh table reads far too small.
    $CLIENT "$DB" -e "ANALYZE TABLE s;" >/dev/null 2>&1
    read -r dl il <<<"$($CLIENT -N -B "$DB" -e "SELECT COALESCE(data_length,0),
        COALESCE(index_length,0) FROM information_schema.tables
        WHERE table_schema='$DB' AND table_name='s';")"
    tot=$((dl + il))
    # Generated values never contain '?'; any that do were substituted by the server because
    # the column's charset could not represent the character. A smaller table that got that
    # way is not a saving -- it is data loss, which is why this column sits next to the size.
    bad=$($CLIENT -N -B "$DB" -e "SELECT COUNT(*) FROM s WHERE value LIKE '%?%';")
    if [ -z "$BASE" ]; then BASE=$tot; v="baseline"
    elif [ "${bad:-0}" -gt 0 ]; then v="MANGLED -- smaller only because it lost data"
    elif [ "$tot" = "$BASE" ]; then v="identical to ascii/ASCII"
    else pct=$(awk "BEGIN{printf \"%+.1f%%\", 100*($tot-$BASE)/$BASE}")
         v="$pct vs ascii/ASCII"
    fi
    awk -v l="$label" -v c="$content" -v d="$dl" -v t="$tot" -v r="$ROWS" -v b="${bad:-0}" \
        -v v="$v" 'BEGIN{printf "  %-20s %-9s %9.2f %10.1f %11d   %s\n", \
            l, c, d/1048576, t/r, b, v}'
    $CLIENT "$DB" -e "DROP TABLE s;"
done
echo
echo "  index_MB is omitted: it is 0 for every row by construction. The side table has no"
echo "  secondary index -- its primary key IS the inverted index, which InnoDB files under"
echo "  data_length, so 100% of the size shown is index."
