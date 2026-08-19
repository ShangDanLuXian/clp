#!/usr/bin/env bash
# Run this BEFORE deleting anything -- it distinguishes space a DROP DATABASE will
# return from space that only a datadir wipe or a server restart can.
# Read-only: shows where benchmark disk went. Deletes nothing.
MARIA="${MARIA:-mysql}"
MY8="${MY8:-/opt/mysql8/usr/bin/mysql --defaults-file=/etc/my8.cnf -u root}"
echo "=== databases (reclaimable by DROP DATABASE) ==="
for e in "MariaDB|$MARIA" "MySQL8|$MY8"; do
  IFS='|' read -r n c <<<"$e"
  $c -e "SELECT 1" >/dev/null 2>&1 || { echo "  $n: not reachable"; continue; }
  $c -N -B -e "SELECT CONCAT('  $n  ',RPAD(table_schema,14),
      LPAD(ROUND(SUM(data_length+index_length)/1073741824,1),8),' GB  ',
      COUNT(*),' tables')
    FROM information_schema.tables
    WHERE table_schema NOT IN ('mysql','information_schema','performance_schema','sys')
    GROUP BY table_schema ORDER BY SUM(data_length+index_length) DESC;" 2>/dev/null
done
echo
echo "=== datadirs on disk (includes what DROP cannot reclaim) ==="
for d in /var/lib/mysql /var/lib/mysql8; do
  [ -d "$d" ] && sudo du -sh "$d" 2>/dev/null | sed 's/^/  /'
done
echo
echo "=== fixed-size / non-shrinking files ==="
for d in /var/lib/mysql /var/lib/mysql8; do
  [ -d "$d" ] || continue
  sudo ls -la "$d"/ibdata1 "$d"/ib_logfile* "$d"/ibtmp1 "$d"/undo_* 2>/dev/null \
    | awk -v d="$d" '{printf "  %-22s %10.1f MB  %s\n", d, $5/1048576, $9}'
done
echo
echo "=== staging TSVs left by interrupted runs ==="
for p in "${TMPDIR:-/tmp}"/tmp*/; do
  [ -d "$p" ] || continue
  if ls "$p"*.tsv >/dev/null 2>&1; then
    echo "  $p  $(du -sh "$p" 2>/dev/null | cut -f1)"
  fi
done
find . -maxdepth 1 -name '*.tsv' -size +100M -printf '  %p  %kk\n' 2>/dev/null
echo
echo "=== free space ==="
df -h . | awk 'NR==2{print "  "$4" available of "$2}'
