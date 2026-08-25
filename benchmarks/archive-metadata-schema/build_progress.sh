#!/bin/bash
# Snapshot of an in-flight build_all_configs.sh run. Safe to run at any time; it only reads.
#
#   ./build_progress.sh                  # one snapshot
#   watch -n 15 ./build_progress.sh      # live
#
# Three independent signals, because no single one is trustworthy on its own:
#   - PHASE, from each config's log: which step bench_topology.py is on (create/load/query).
#   - ROWS, from information_schema: InnoDB's row estimates, which are approximate but
#     directionally right and cheap, so they show how far a load has got.
#   - RATE, from Handler_write sampled twice: an exact server-wide count of rows written,
#     so it is the honest basis for an ETA even when the per-table estimates drift.
#     (Innodb_rows_inserted does NOT exist on MariaDB and silently yields an empty string,
#     which reads as a rate of zero -- Handler_write is the portable equivalent.)
set -u
ENGINE="mariadb=mysql"
USERS=20
DPU=5
ARCHIVES=12000
LOGDIR="${TMPDIR:-/tmp}/topo_builds"
SAMPLE=3

while [ $# -gt 0 ]; do
  case "$1" in
    --engine) ENGINE=$2; shift 2;;
    --users) USERS=$2; shift 2;;
    --datasets-per-user) DPU=$2; shift 2;;
    --archives) ARCHIVES=$2; shift 2;;
    --logdir) LOGDIR=$2; shift 2;;
    --sample) SAMPLE=$2; shift 2;;
    *) echo "unknown option: $1"; exit 2;;
  esac
done

CMD=${ENGINE#*=}
[ "$CMD" = "$ENGINE" ] && CMD=$ENGINE
q() { $CMD -N -B -e "$1" 2>/dev/null; }

# 45 postings per archive plus the archive row itself.
TARGET=$(( USERS * DPU * ARCHIVES * 46 ))

echo "build progress  $(date '+%Y-%m-%d %H:%M:%S')"
echo
printf "  %-14s %-10s %7s %14s %10s\n" config phase tables "est. rows" "of target"
for c in per_dataset per_user unified tiered; do
  # REGEXP, not LIKE: in LIKE, `_` is a single-character wildcard, so '%_d%' matches every
  # table whose name merely contains a "d" -- including every siDe_* table of every other
  # configuration, which inflated this column past 100%.
  case $c in
    per_dataset) pat="^(arch|side)_d[0-9]";;
    per_user)    pat="^(arch|side)_u[0-9]";;
    unified)     pat="^(arch|side)_all$";;
    tiered)      pat="^(arch|side)_t[0-9]";;
  esac
  read -r tabs rows <<<"$(q "SELECT COUNT(*), IFNULL(SUM(table_rows),0)
        FROM information_schema.tables
        WHERE table_schema='topo' AND table_name REGEXP '${pat}';")"
  tabs=${tabs:-0}; rows=${rows:-0}
  phase=$(grep -o "$c: [a-z]*" "$LOGDIR/$c.log" 2>/dev/null | tail -1 | cut -d' ' -f2)
  grep -q "written to" "$LOGDIR/$c.log" 2>/dev/null && phase=done
  pct=$(( TARGET > 0 ? rows * 100 / TARGET : 0 ))
  printf "  %-14s %-10s %7s %14s %9s%%\n" "$c" "${phase:-pending}" "$tabs" \
         "$(printf "%'d" "$rows")" "$pct"
done

r0=$(q "SHOW GLOBAL STATUS LIKE 'Handler_write';" | awk '{print $2}')
sleep "$SAMPLE"
r1=$(q "SHOW GLOBAL STATUS LIKE 'Handler_write';" | awk '{print $2}')
if [ -z "${r0:-}" ] || [ -z "${r1:-}" ]; then
  echo; echo "  cannot read Handler_write -- is the server reachable?"; exit 1
fi
rate=$(( (r1 - r0) / SAMPLE ))

total_rows=$(q "SELECT IFNULL(SUM(table_rows),0) FROM information_schema.tables
                WHERE table_schema='topo';")
total_rows=${total_rows:-0}
remaining=$(( TARGET * 4 - total_rows ))
echo
printf "  %-14s %s rows/s (%ss sample)\n" "insert rate" "$(printf "%'d" "$rate")" "$SAMPLE"
if [ "$rate" -gt 0 ] && [ "$remaining" -gt 0 ]; then
  printf "  %-14s ~%s M rows -> ETA ~%s min\n" "remaining" \
         $(( remaining / 1000000 )) $(( remaining / rate / 60 ))
else
  printf "  %-14s %s\n" "remaining" \
         "$([ "$remaining" -le 0 ] && echo "loads appear complete" || echo "no inserts moving")"
fi

read -r files bytes <<<"$(q "SELECT COUNT(*), IFNULL(SUM(file_size),0)
      FROM information_schema.INNODB_SYS_TABLESPACES WHERE name LIKE 'topo/%';")"
printf "  %-14s %s GB allocated in %s tablespace files\n" "on disk" \
       "$(awk -v b="${bytes:-0}" 'BEGIN{printf "%.1f", b/1073741824}')" \
       "$(printf "%'d" "${files:-0}")"
printf "  %-14s %s\n" "threads" \
       "$(q "SELECT COUNT(*) FROM information_schema.processlist WHERE command<>'Sleep';")"
