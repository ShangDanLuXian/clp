# stall-200s

**Varied:** `stall --stall-secs 200`, with `clp-s` process sampling.

Same package, archives (8 × 40 MiB, soft 10.8 s, hard 80 s) and harness as `../20261021T012838Z`; see its `RESULTS.md` (sections 6 and 8) for the analysis and the clock note. Times are monotonic seconds from the request.

Job 24: SUCCEEDED (DB and query coordinator, 01:43:39.662 log time; the harness poller stopped before recording it). `end`: Succeeded, 659,677 emitted, **659,677 duplicates**, 0 protocol errors; `identical_multiset: true`.

All 8 first attempts hit the hard timeout, in two waves (tasks 0-3 at 80 s, tasks 4-7 at 160 s); each task started twice; no task was killed twice before the client resumed at 200 s. Processes: 4 zombies at the start (from job 22), then 4 children of the executors, +4 orphans after the first wave, 8 orphans after the second; after the resume all became zombies (12).

`stall.clp-s-processes.txt` samples every `clp-s` in the 4 `spider-worker` containers every 2 s (`pid`, state and parent PID, parent's name; `parent=spider-executio` with PPID 1 means an orphan of the execution manager, state `Z` a zombie). Sampler, run in the background for the duration of the phase:

```bash
#!/usr/bin/env bash
# Samples the clp-s processes in every spider-worker container every 2 s until killed.
# Usage: sample_clp_s.sh <compose project> <output file>
project="$1"
out="$2"
script='for d in /proc/[0-9]*; do if [ "$(cat $d/comm 2>/dev/null)" = clp-s ]; then
  pid=${d#/proc/}; st=$(awk "{print \$3, \$4}" $d/stat); pcomm=$(cat /proc/$(awk "{print \$4}" $d/stat)/comm 2>/dev/null);
  echo "pid=$pid state_ppid=\"$st\" parent=$pcomm"; fi; done'
: > "$out"
while true; do
    ts=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
    for c in $(docker ps -q --filter "label=com.docker.compose.project=$project" --filter "label=com.docker.compose.service=spider-worker"); do
        name=$(docker inspect --format '{{.Name}}' "$c" | sed "s|^/$project-||")
        docker exec "$c" sh -c "$script" 2>/dev/null | sed "s|^|$ts $name |" >> "$out"
    done
    echo "$ts ---" >> "$out"
    sleep 2
done
```
