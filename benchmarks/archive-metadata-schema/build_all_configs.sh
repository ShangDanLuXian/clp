#!/bin/bash
# Build every topology configuration and LEAVE THEM IN PLACE for query_probe.py.
#
# Why this exists rather than one bench_topology.py invocation:
#
#   - The four configurations use disjoint table names inside the same `topo` schema
#     (arch_d*/side_d*, arch_u*/side_u*, arch_all/side_all, arch_t*/side_t*), so they can be
#     built CONCURRENTLY by separate processes without colliding. Most of a build's wall
#     time is single-threaded Python generating the INSERT stream, so overlapping one
#     config's generation with another's load is a real win on a multi-core box.
#   - --keep stops each config being dropped when its cycle ends, so all four survive
#     together and the probe can compare them without a rebuild.
#   - --skip-retention is REQUIRED here, not merely faster. The retention phase issues
#     ALTER TABLE ... DROP PARTITION, so leaving it on would hand the probe a dataset with
#     its expired partitions already removed.
#
# Usage:
#   ./build_all_configs.sh                                  # parallel, default scale
#   ./build_all_configs.sh --sequential                     # one at a time
#   ./build_all_configs.sh --users 4 --datasets-per-user 2 --archives 60 --hours 6
set -u
ENGINE="mariadb=mysql"
USERS=20
DPU=5
ARCHIVES=12000
HOURS=168
WORKERS=8
PARALLEL=1
LOGDIR="${TMPDIR:-/tmp}/topo_builds"

while [ $# -gt 0 ]; do
  case "$1" in
    --engine) ENGINE=$2; shift 2;;
    --users) USERS=$2; shift 2;;
    --datasets-per-user) DPU=$2; shift 2;;
    --archives) ARCHIVES=$2; shift 2;;
    --hours) HOURS=$2; shift 2;;
    --workers) WORKERS=$2; shift 2;;
    --sequential) PARALLEL=0; shift;;
    --logdir) LOGDIR=$2; shift 2;;
    *) echo "unknown option: $1"; exit 2;;
  esac
done

CONFIGS="per_dataset per_user unified tiered"
mkdir -p "$LOGDIR"
# An ARRAY, not a string: --engine's value may contain spaces (e.g. a socket path), and an
# unquoted string expansion would split it into separate arguments.
COMMON=(--engine "$ENGINE" --users "$USERS" --datasets-per-user "$DPU"
        --archives "$ARCHIVES" --hours "$HOURS" --workers "$WORKERS"
        --keep --skip-retention)

echo "building: $CONFIGS"
echo "scale   : $USERS users x $DPU datasets x $ARCHIVES archives, $HOURS h partitions"
echo "mode    : $([ $PARALLEL -eq 1 ] && echo "parallel, $WORKERS load workers each" \
                                     || echo "sequential")"
echo "logs    : $LOGDIR/<config>.log"
echo

start=$(date +%s)
rc=0
if [ $PARALLEL -eq 1 ]; then
  pids=""
  for c in $CONFIGS; do
    python3 bench_topology.py "${COMMON[@]}" --configs "$c" > "$LOGDIR/$c.log" 2>&1 &
    pids="$pids $!:$c"
    echo "  started $c (pid $!)"
  done
  for pc in $pids; do
    p=${pc%%:*}; c=${pc##*:}
    if wait "$p"; then echo "  done   $c"; else echo "  FAILED $c -- see $LOGDIR/$c.log"; rc=1; fi
  done
else
  for c in $CONFIGS; do
    echo "  building $c"
    if python3 bench_topology.py "${COMMON[@]}" --configs "$c" > "$LOGDIR/$c.log" 2>&1; then
      echo "  done   $c"
    else
      echo "  FAILED $c -- see $LOGDIR/$c.log"; rc=1
    fi
  done
fi
echo
echo "elapsed $(( $(date +%s) - start ))s"

# Report what actually survived, so a silent build failure is not discovered later by a
# probe that simply reports the config "absent".
python3 - "$ENGINE" <<'PY'
import shlex, subprocess, sys
cmd = shlex.split(sys.argv[1].partition("=")[2] or sys.argv[1])
q = ("SELECT table_name FROM information_schema.tables "
     "WHERE table_schema='topo' AND table_name LIKE 'side%';")
out = subprocess.run(cmd + ["-N", "-B", "-e", q], capture_output=True, text=True).stdout
names = out.split()
groups = {"per_dataset": "side_d", "per_user": "side_u",
          "unified": "side_all", "tiered": "side_t"}
print("\nside tables now in `topo`:")
for cfg, pfx in groups.items():
    n = sum(1 for x in names if x.startswith(pfx))
    print(f"  {cfg:<12} {n:>4} table(s)   {'OK' if n else 'MISSING'}")
PY
exit $rc
