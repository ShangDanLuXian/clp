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
printf '\n  %-28s %10s %11s %10s   %s\n' "DEFAULT CHARSET" data_MB index_MB B/row verdict

BASE=""
for pair in "ascii|VARCHAR(64)|DEFAULT CHARSET = ascii" \
            "utf8mb4|VARCHAR(64)|DEFAULT CHARSET = utf8mb4" \
            "binary (VARBINARY)|VARBINARY(255)|"; do
    IFS='|' read -r label vtype charset <<<"$pair"
    $CLIENT "$DB" -e "DROP TABLE IF EXISTS s; CREATE TABLE s (
        column_id       TINYINT UNSIGNED NOT NULL,
        value           $vtype NOT NULL,
        begin_timestamp BIGINT NOT NULL,
        archive_id      INT UNSIGNED NOT NULL,
        PRIMARY KEY (column_id, value, begin_timestamp, archive_id)
      ) ENGINE = InnoDB $charset $PARTS;
      INSERT INTO s SELECT n % 8, CONCAT('host-', LPAD(n % 1000, 6, '0')),
          $T0 + n * ($HOURS * $HOUR DIV $ROWS), n
        FROM (SELECT a.n + b.n*10 + c.n*100 + d.n*1000 + e.n*10000 + f.n*100000 AS n
              FROM d a, d b, d c, d d, d e, d f) s WHERE n < $ROWS;"
    # information_schema serves CACHED stats; without ANALYZE a fresh table reads far too small.
    $CLIENT "$DB" -e "ANALYZE TABLE s;" >/dev/null 2>&1
    read -r dl il <<<"$($CLIENT -N -B "$DB" -e "SELECT COALESCE(data_length,0),
        COALESCE(index_length,0) FROM information_schema.tables
        WHERE table_schema='$DB' AND table_name='s';")"
    tot=$((dl + il))
    if [ -z "$BASE" ]; then BASE=$tot; v="baseline"
    elif [ "$tot" = "$BASE" ]; then v="identical to ascii"
    else pct=$(awk "BEGIN{printf \"%+.2f%%\", 100*($tot-$BASE)/$BASE}")
         v="differs by $((tot - BASE)) bytes ($pct)"
    fi
    awk -v l="$label" -v d="$dl" -v i="$il" -v t="$tot" -v r="$ROWS" -v v="$v" 'BEGIN{
        printf "  %-28s %10.2f %11.2f %10.1f   %s\n", l, d/1048576, i/1048576, t/r, v}'
    $CLIENT "$DB" -e "DROP TABLE s;"
done
echo
echo "  (index_MB is 0 by construction: the side table has no secondary index -- its"
echo "   primary key IS the inverted index, which InnoDB reports under data_length.)"
