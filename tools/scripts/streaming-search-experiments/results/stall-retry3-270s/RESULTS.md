# stall-retry3-270s

**Varied:** `query_task_max_retry: 3` (package restarted), `stall --stall-secs 270`; `clp-s` process sampling.

Same package, archives (8 × 40 MiB, soft 10.8 s, hard 80 s) and harness as `../20261021T012838Z`; see its `RESULTS.md` (sections 6 and 8) for the analysis and the clock note. Times are monotonic seconds from the request.

Job 28: RUNNING at 0.185 s, **SUCCEEDED at 273.747 s** (3.7 s after the resume) — the stall that failed `../stall-270s` with 1 retry. `end`: Succeeded, 659,677 emitted, 987,204 duplicates, 0 protocol errors; `identical_multiset: true`. Hard timeouts: tasks 0-3 twice, 4-7 once (12); tasks 0-3 started 3 times. Processes: peak 12 orphans + 4 children; zombies 4 → 16.

`stall.clp-s-processes.txt` samples every `clp-s` in the 4 `spider-worker` containers every 2 s (`pid`, state and parent PID, parent's name; `parent=spider-executio` with PPID 1 means an orphan of the execution manager, state `Z` a zombie). Sampler: see `../stall-200s/RESULTS.md`.
