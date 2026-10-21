# stall-retry3-125s

**Varied:** `query_task_max_retry: 3` in `query_coordinator` (package restarted), `stall` with the default 125 s; `clp-s` process sampling.

Same package, archives (8 × 40 MiB, soft 10.8 s, hard 80 s) and harness as `../20261021T012838Z`; see its `RESULTS.md` (sections 6 and 8) for the analysis and the clock note. Times are monotonic seconds from the request.

Job 27: RUNNING at 0.266 s, SUCCEEDED at 127.72 s (2.6 s after the resume). `end`: Succeeded, 659,677 emitted, 327,527 duplicates, 0 protocol errors; `identical_multiset: true`. Tasks 0-3 timed out once each (4); no extra retry was needed. Processes: 0 zombies after the restart, peak 4 orphans + 4 children, 4 zombies at the end.

`stall.clp-s-processes.txt` samples every `clp-s` in the 4 `spider-worker` containers every 2 s (`pid`, state and parent PID, parent's name; `parent=spider-executio` with PPID 1 means an orphan of the execution manager, state `Z` a zombie). Sampler: see `../stall-200s/RESULTS.md`.
