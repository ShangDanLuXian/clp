# stall-270s

**Varied:** `stall --stall-secs 270`, with `clp-s` process sampling.

Same package, archives (8 × 40 MiB, soft 10.8 s, hard 80 s) and harness as `../20261021T012838Z`; see its `RESULTS.md` (sections 6 and 8) for the analysis and the clock note. Times are monotonic seconds from the request.

Job 25: RUNNING at 0.22 s, **FAILED at 160.869 s** (query coordinator: `outcome=Failed { error_message: "hard timeout (80000 ms) exceeded" }`). `end`: **Failed**, 659,677 emitted, 328,083 duplicates, 0 protocol errors; stream closed cleanly; `identical_multiset: true` — the orphaned `clp-s` delivered every result after the resume although the job had failed.

Task 0 finished from the buffers in 0.43 s; tasks 1-4 timed out at 80 s; task 1's retry started at once on the freed slot and timed out at 160 s, failing the job. Hard timeouts: tasks 1-4 twice, 5-7 once (11). The 3 retries still queued kept running until their own timeouts and could not report (`Failed to report task outcome to storage. Dropping the report. err=CacheStale("cache stale: job no longer running")` × 3). Processes: peak 11 orphans + 4 children; zombies 12 → 23.

`stall.clp-s-processes.txt` samples every `clp-s` in the 4 `spider-worker` containers every 2 s (`pid`, state and parent PID, parent's name; `parent=spider-executio` with PPID 1 means an orphan of the execution manager, state `Z` a zombie). Sampler: see `../stall-200s/RESULTS.md`.
