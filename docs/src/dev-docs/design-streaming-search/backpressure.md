# Backpressure vs. Spider's task timeouts

This is the detailed analysis behind [issue 1](issues.md). In one sentence: `clp-s` finishes its
own work in seconds, but when the consumer is slow every concurrent `clp-s` blocks in `send()`,
and the time spent blocked counts against Spider's per-task hard timeout, so the task is killed and
the job eventually fails.

## How backpressure works in this design

When a producer is faster than its consumer, the surplus has three possible fates: buffer it
without bound (memory grows until OOM), drop it (incomplete results), or slow the producer down.
Backpressure is the third: pressure propagates upstream until the source waits.

Data flows through six bounded stages, and each full stage blocks the one before it:

```text
clp-s process (worker)                         listener process (CLI / API server)
+---------------------------+                  +----------------------------------------+
| 1. scan archive, produce  |                  | 5. connection task                     |
|    results                |                  |    read_buf -> decode -> CAS           |
|          v                |                  |       -> results_sender.send().await   |
| 2. ::send() into kernel   | 3. TCP   4. kernel|          v                             |
|    send buffer            | window   receive  | 6. bounded mpsc channel (1024)         |
|    (blocking socket)      | -------> buffer ->|          v                             |
+---------------------------+                  | 7. consumer: ResultStream.next()       |
                                               |    -> stdout / HTTP body               |
                                               +----------------------------------------+
```

When stage 7 slows down:

| Stage | What happens                                                                                         | Where |
|-------|------------------------------------------------------------------------------------------------------|-------|
| 7     | `ResultStream.next()` is called slowly; results are not taken from the channel.                      | caller |
| 6     | The channel fills (1024 results). The connection task's `results_sender.send(result).await` suspends. | `search-result-listener/src/connection.rs`, capacity in `session.rs` |
| 5     | Suspended in `send`, the task never reaches its next `stream.read_buf`.                               | `connection.rs` |
| 4     | Nobody reads the socket; the kernel receive buffer fills (autotuned, hundreds of KB to a few MB).    | kernel |
| 3     | The receiver advertises a zero window; the sender stops sending and only probes.                      | kernel |
| 2     | `clp-s`'s kernel send buffer fills; the socket is blocking, so `::send()` does not return.            | `core/src/clp/networking/socket_utils.cpp`, `try_send` |
| 1     | `write()` does not return; the scan loop cannot advance. The process is alive, idle, producing nothing.| `core/src/clp_s/search/Output.cpp` |

The prototype report has a matching measurement: the `clp-s` killed in job 13 had 335,872 B queued
in its send buffer, which is stage 2's backlog.

Properties of this chain:

* Memory is bounded at every stage; the listener and the worker never grow however slow the
  consumer is.
* Nothing is dropped; every stage waits rather than discards.
* No explicit protocol is needed; `clp-s` does not even read its socket. The pressure travels
  entirely through TCP flow control and blocking calls.
* The cost: `clp-s`'s run time is no longer its own. If the consumer pauses for 10 s, `clp-s`
  sits in `send()` for 10 s.

That last property is the root of the conflict, because `clp-s` is not an ordinary process. It is
a Spider task, and Spider is timing it.

## Spider's time budget per task

From `query-coordinator/src/job_handle.rs`, `compute_query_task_execution_policy` (added by #2564):

| Timeout      | Formula                                   | Assumption                                     | On expiry                                    |
|--------------|-------------------------------------------|------------------------------------------------|----------------------------------------------|
| Soft         | 10 s + 20 ms x uncompressed MiB           | `clp-s` sustains 50 MiB/s                      | Spider **re-dispatches** a new attempt; the old one keeps running |
| Hard         | 60 s + 500 ms x uncompressed MiB          | worst case: a fully contended worker at 2 MiB/s| Spider **kills** the task; one failure counted |
| Retries      | `query_task_max_retry`, default **1**     |                                                | Second failure fails the task and the job     |

For the benchmark data (607 archives, 211 GB uncompressed, about 332 MiB each): soft 16.6 s, hard
about 226 s. For an archive under 1 MiB: soft 10 s, hard 60 s.

The implicit model is `task time = startup + archive size / throughput`. The two timeouts are an
optimistic and a pessimistic throughput. The code comment says the hard timeout "assumes the
worst-case throughput of a fully contended worker": it allows for CPU contention, never for
waiting on someone else.

## The term the formula does not have

```text
task time = startup + archive size / throughput + time blocked in send()
                                                  '-- a function of consumer speed and result volume,
                                                      unrelated to archive size, unknown to the coordinator
```

Two properties of the last term matter:

* **It is independent of the timeout's variable.** The timeout scales with the *input* (archive
  size); the blocked time scales with the *output* (result bytes) divided by the consumer's rate.
  A 50 MiB archive has an 85 s hard timeout. If it is a dense error log yielding 6 MB of results,
  and the client reads at 50 KB/s, sending alone takes 120 s. Killed.
* **It is shared across concurrent tasks.** A session has one result channel. With 8 tasks
  streaming, the consumer's 50 KB/s is split 8 ways, about 6 KB/s per task. The task above needs
  1,000 s.

## Timeline

Setting: an API client on a slow link reading 50 KB/s; the job runs 8 concurrent tasks.

```text
t=0      8 tasks start, scan, find results, begin send()
t~1 s    channel full (1024) -> receive buffers full -> zero window -> all 8 clp-s blocked in send()
         all 8 worker slots held; other jobs queue
t=11 s   soft timeout: Spider re-dispatches each of the 8 tasks
         -> needs 8 more slots. None free: they queue (prototype job 15 saw this)
         -> slots free (a larger cluster): the new attempts connect, block too; 16 slots now serve
            one slow client, and everything the new attempts send is a duplicate dropped by the CAS
t=85 s   hard timeout (50 MiB archive): Spider kills the first attempts
         -> the kernel still flushes the send buffer before FIN, so nothing is lost at the listener
         -> the cursor stops at k
t=86 s   retry starts (the second and last attempt)
         re-sends from 0: 0..k are dropped by the CAS without touching the channel, so catching up
         is fast (network-bound, not consumer-bound); then it blocks in send() again at k
t=171 s  second hard timeout. Killed. Retries exhausted -> task FAILED -> job FAILED
```

Across both attempts the task can advance about `2 x 85 s x (consumer rate / concurrency)`. Past
that, the job fails. The data, the cluster, and `clp-s` are all fine; the only cause is a slow
reader.

On the results-cache path, a task ends as soon as MongoDB has accepted its writes; when and how
slowly the consumer reads is irrelevant. So this is a failure mode that streaming introduces.

## A client that stops reading entirely

A client that stays connected but reads nothing (a suspended tab, a SIGSTOPped process, a debugger
break): every task with results blocks, every one hits its hard timeout, is killed, retried, killed
again; the job FAILs after about twice the hard timeout.

The API report marks "a stalled client makes the job FAIL" as refuted, without a reason. The
longest stalls in either report were 25 s (prototype job 15) and 5 s (API disconnect test), both
under the 60 s minimum hard timeout, so the refutation has no test behind it. Suggested experiment:
a small archive and a 90 s stall; check whether Spider kills the task.

"Stalled" is not "disconnected". After a disconnect, the API server's session enters discard mode
and `clp-s` unblocks immediately; the reports tested that path and it works. The problem is a
connected client that does not read.

## Why raising the timeouts is not a fix

The timeouts exist for liveness detection: a deadlocked `clp-s`, a listener host that vanished
without RST, a partitioned worker. These are found, and their slots released, by the hard timeout.

Backpressure makes "alive but waiting for the consumer" and "hung" look identical to Spider: a
task that runs a long time and does not finish. Raising the timeouts lets both through: a slow
client no longer fails the job, but a genuinely hung task also holds its slot for hours.

That is the essence of the conflict: **Spider's liveness signal is wall-clock time, and backpressure
makes wall-clock time a function of the consumer. As long as the pressure reaches the worker, both
cannot be right at once.**

## Second-order effects

* **Slot starvation.** Blocked tasks hold slots; soft-timeout re-dispatches want more. One slow
  client queues every other job (measured by the API report: one stalled stream held all 8 slots).
* **Misattribution.** Spider logs a task timeout; operators look at the worker and `clp-s`; the
  cause is the client.
* **Retries make it worse.** The default is one retry, and the retry changes nothing about the
  consumer, so the second attempt almost always fails the same way. Retrying is ineffective here.

## What the experiment showed (2026-10-21)

Setup: a local WSL2 machine (i7-13700K), this branch, 8 archives of 40 MiB (hard timeout 80 s,
soft 10.8 s), 4 general-pool worker slots, query `*Transmitted*` with 659,677 results, a client
that reads one result and then stops reading for the stall length. Raw data and write-ups:
`tools/scripts/streaming-search-experiments/results/`.

| Stall  | Retries | What happened                                                                                              |
|--------|---------|------------------------------------------------------------------------------------------------------------|
| 40 s   | 1       | Control. No timeout, SUCCEEDED at 43.5 s, 0 duplicates.                                                    |
| 125 s  | 1       | The 4 streaming tasks hit the hard timeout at 80 s. Retries queued behind the 4 unstarted tasks. SUCCEEDED 2.6 s after the resume; 328,619 duplicates dropped. |
| 200 s  | 1       | All 8 first attempts timed out, in two waves (80 s, 160 s). SUCCEEDED after the resume; 659,677 duplicates dropped. |
| 270 s  | 1       | **FAILED at 160.9 s.** One task finished from the buffers at once, which freed a slot; task 1's retry started at 80 s and timed out at 160 s. The client still received every result (from the orphans, see below) and then `end: Failed`. |
| 270 s  | 3       | SUCCEEDED 3.7 s after the resume; 12 hard timeouts; 987,204 duplicates dropped.                           |

So the analysis above holds, with two corrections:

1. **The failure point is later than "twice the hard timeout" when the job has more tasks than
   slots.** A failed task goes to the back of the queue, behind the tasks that have not started,
   so its retry starts one wave later: the job fails at about `(1 + waves) × hard timeout`,
   160 s when a slot was free and 240 s when none was. A job with fewer tasks than slots fails
   at 2 × hard timeout with one retry.
2. **Spider's hard timeout does not kill `clp-s`.** It frees the executor and fails the task
   instance, but the `clp-s` process is orphaned to the container's PID 1, keeps its socket,
   finishes streaming once the client resumes, and stays a zombie. This is
   [issue 20](issues.md). It means the retry and the orphan both stream the whole archive (one
   archive's results dropped as duplicates per timeout), the "stale attempt" case is the normal
   case, and a job that Spider reports as FAILED can still deliver a complete result set.

Two more measurements: the buffers between `clp-s` and the SSE client (socket buffers plus the
listener's channel) absorbed about one archive's results, 82,000 results or roughly 15 MB, before
backpressure reached `clp-s` (task 1 finished in 0.46 s while the client was already stalled);
and no soft-timeout re-dispatch appeared in any run, so nothing in the timeline above about the
soft timeout was observed.

The disconnect runs confirm the other half of the slot problem: closing the socket marked the job
CANCELLING within 0.14 s, but every task ran to the end and 659,319 results were discarded by the
API server. Cancellation frees nothing today.

## Options

The one requirement: a task's run time must again depend only on the archive and the cluster, and a
slow consumer must only hurt itself. That means cutting the backpressure chain somewhere.

| Option | Cut point         | Trade-off                                                                                 |
|--------|-------------------|-------------------------------------------------------------------------------------------|
| A      | listener memory   | Bounded per-session buffer; past the cap, cancel the job and tell the client it is too slow. Within the cap the consumer may be slow. |
| B      | listener disk     | A with spill to disk; the cap becomes disk-sized; temporary files to manage.               |
| C      | worker disk       | `clp-s` writes to a local file first; task time is archive-bound again, but the sender must outlive the task, which Spider's model does not express. |
| D      | shared storage    | Tasks write to S3 or a shared FS; the consumer pulls. Full decoupling, listener-crash recovery for free; an extra hop and a storage dependency. |
| E      | none              | Huge timeouts for network-output tasks. Gives up liveness detection. Not recommended.       |

Recommendation: **A**, with B for the API server if needed. It is the smallest change (listener
only), the semantics are clear (a slow client gets an explicit error instead of a mysterious FAILED
job), and workers are never affected by consumers. A's parameters, the per-session buffer size and
the no-progress timeout, must be aligned with the soft timeout so that normal consumer jitter never
triggers a cancel.

A depends on job cancellation actually stopping Spider tasks ([issue 5](issues.md)), so the
backpressure policy and the cancellation design have to be settled together.
