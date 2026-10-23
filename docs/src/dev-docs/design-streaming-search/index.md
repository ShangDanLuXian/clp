# Streaming search (experimental)

Streaming search delivers search results from Spider search tasks to a listener as they are found,
instead of writing them to the results cache first. This section records the design, the issues
found while reviewing it, and the experiments run against the prototype. Everything here is
experimental: the design is still being iterated and nothing in this section is a commitment.

## Where things are

* **Design doc** (external, 2026-10-06): the original design, with the per-task result counter, the
  v1 wire protocol, the listener library, and the `streaming-search.sh` entry point.
* **Prototype report** (external): the `clp-streaming-search` CLI prototype, its corrections to the
  design, and its benchmark against `sbin/search.sh`.
* **API server integration report** (external): the `POST /query/stream` prototype, its cancel-on-
  disconnect behaviour, and its time-to-first-result benchmark.
* **Code**: the prototype branches `query-coordinator/streaming-search` and
  `query-coordinator/streaming-search-api` from the `LinZhihao-723/clp` fork, merged into this
  branch. The new crates are `components/search-result-listener` (the listener library) and
  `components/clp-package-cli` (the `clp-streaming-search` binary).

::::{grid} 1 1 1 1
:gutter: 2

:::{grid-item-card}
:link: design-summary
Design summary
^^^
A condensed description of the design as prototyped, including the prototype's corrections.
:::

:::{grid-item-card}
:link: issues
Issues
^^^
Every issue found in the design review, ranked by severity, with evidence and proposed fixes.
:::

:::{grid-item-card}
:link: backpressure
Backpressure vs. Spider's task timeouts
^^^
A detailed analysis of the most serious issue: how a slow consumer fails a job.
:::

:::{grid-item-card}
:link: reproduction
Experiment reproduction
^^^
What was re-run from the prototype reports, and how the results compare.
:::

:::{grid-item-card}
:link: rust-reader
The Rust reader prototype and V3
^^^
What of the in-process Rust reader is on this branch, where it attaches, and what to settle.
:::

:::{grid-item-card}
:link: implementation-plan
Implementation plan
^^^
The two-week plan: the PR breakdown, schedule, known limitations and validation gates.
:::
::::

:::{toctree}
:hidden:

design-summary
issues
backpressure
reproduction
rust-reader
implementation-plan
:::
