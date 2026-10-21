# fault-4-kills

**Varied:** `fault --fault-kills 4 --fault-stall-secs 5`.

Same package, archives (8 × 40 MiB, soft 10.8 s, hard 80 s) and harness as `../20261021T012838Z`; see its `RESULTS.md` (sections 6 and 8) for the analysis and the clock note. Times are monotonic seconds from the request.

Job 26: RUNNING at 0.245 s, **FAILED at 16.889 s** (`in-task failure: …clp-s exited on error with status=signal: 9 (SIGKILL)`, with undecoded bytes in the message). `end`: Failed, 167,875 emitted, 129,865 duplicates, 0 protocol errors; `identical_multiset: false`, 491,802 missing, 0 extra — expected once the retries are exhausted, and reported as `Failed` rather than as a short success. Kills at 5.5, 10.9, 16.2, 21.6 s; the per-container counts (5-7) include the 23 zombies left by the earlier stalls (`kill -9` on a zombie succeeds), so they overstate the live `clp-s` killed (one per container).
