# CLP archive-metadata schema migration — handoff

Written 2026-10-22. Supersedes `~/clp-devin/HANDOFF.md` (2026-09-11). This file lives only on the
`schema-migration-handoff` branch and must never be included in a PR.

---

## 1. Context

The work migrates CLP's archive metadata from per-dataset MySQL tables (`clp_<dataset>_archives`,
`clp_<dataset>_column_metadata`, ...) to shared tables keyed by `dataset_id`
(`clp_archives`, `clp_long_span_archives`, `clp_column_metadata`, `clp_datasets`).

People and remotes in `~/clp-devin/clp`:

| Remote | Repo | Owner |
|---|---|---|
| `fork` | `ShangDanLuXian/clp` | Xuhui Chen (the user; also signs as "Sever") |
| `origin` | `gibber9809/clp` | Devin Gibson, mentor and reviewer |
| (none) | `y-scope/clp` | upstream; `origin/main` and `fork/main` are synced copies of it |

PR flow so far: branch from `origin/schema-migration-beta`, push to `fork`, open the PR against
`gibber9809/clp:schema-migration-beta`. Devin reviews and squash-merges.

The next phase is the **first PR to upstream** (`y-scope/clp` main). See section 4.

---

## 2. Rules from the user (must follow)

- Never put any assistant or model identity into a commit, comment, doc, or PR description. No
  `Co-Authored-By`, no "generated with".
- Commits are authored as `Xuhui Chen <90581933+ShangDanLuXian@users.noreply.github.com>`.
  Short conventional-commit messages. 100-character lines.
- When explaining a change or review finding, first establish what the code currently does, then
  analyse what changed.
- Prefer minimal diffs. Don't restructure when a local change works. Match the surrounding file's
  conventions; don't add comments or docstrings to files that don't already have them. Ask before
  expanding a task's scope.
- New feature code: do NOT add comments/docstrings that explain the feature's behaviour unless
  asked. Only update existing comments/docstrings that the change makes stale.
- Coding workflow: delegate coding to a subagent, then review its output. The coding agent reads
  the coding style guide; the reviewer reads both the coding and the review guides:
  - Rust: https://github.com/LinZhihao-723/claude-instruction/tree/main/rust
    (`coding-style.md`, `pr-review.md`)
  - C++: https://github.com/LinZhihao-723/claude-instruction/tree/main/cpp
    (`pr-review.md` only; there is no C++ coding style guide, so follow the surrounding files)
- Builds run from the `xuhuichen@docker-desktop` container; CLP is started from WSL
  (`xuhuichen@xuhuiyscope`). Both see the same bind-mounted `~/clp-devin/clp`.
- Do not push unless asked. The user pushes.

---

## 3. Environment quirks

- **No network from the sandbox.** `github.com`, crates.io and npm are blocked, so `git fetch`,
  `git push`, `cargo` and `pnpm install` fail. Remote refs are only as fresh as the user's last
  fetch on the host.
- **`.git/config.lock` is a `/dev/null` character device.** Anything that writes `.git/config`
  fails half-way: `git checkout -b`, `git branch --set-upstream-to`, `git config`, `git push -u`.
  Create branches with `git branch --no-track <name> <base>` then `git checkout <name>`. Push with
  explicit refs (`git push fork <branch>`).
- **`pull.rebase=true` is set locally**, so a bare `git pull` rebases. Use
  `git merge <explicit-ref>` or `git pull --no-rebase <remote> <branch>`.
- **The git-lfs `post-checkout` hook exits non-zero** and aborts rebases. Use
  `git -c core.hooksPath=/dev/null rebase ...` if a rebase is ever needed.
- **Never run anything that writes into `build/` from the sandbox** (`task`, `cargo build` with the
  task-configured target dir, `b2`, `cmake`). The sandbox's glibc (2.43) is newer than the build
  container's; a sandbox-built artifact once contaminated `build/deps/cpp/boost-*` and broke the
  container build with `__isoc23_sscanf` link errors. Builds happen in `docker-desktop` only.
- **`docker-desktop` container**: no `docker compose` plugin (start CLP from WSL instead), and its
  `~/.docker/config.json` has `credsStore: desktop.exe`, which isn't on its PATH. Workaround used:
  `DOCKER_CONFIG=~/.docker-container task` with `~/.docker-container/config.json` = `{}`.
- **Mirrors**: the user is back in Canada, so no China mirrors are needed. The old local-only
  mirror commits live only on `schema-migration-integration`; don't cherry-pick them.
- `~/clp-devin/memory` is a dated snapshot, not live memory. Treat it as background.

---

## 4. Current task: the first upstream PR

### 4.1 Branch `schema-migration-upstream` (commit `49c3f827`, not pushed)

Cut from `origin/main` `02032152` (Devin's synced copy of upstream, 8 commits newer than
`fork/main`). One commit:

```
feat(clp-package)!: Store archive metadata in shared tables keyed by dataset ID.
```

49 files, +743/-719. Built by taking the migration's net content (beta + the WebUI branch) relative
to upstream `f3c5ea13`, filtering it, and applying it to `origin/main`. Contents:

- Shared-table DDL and init script (`clp_metadata_db_utils.py`, `initialize-clp-metadata-db.py`).
- Rust table-name helpers (`clp-rust-utils/src/clp_config/package/config.rs`), dataset helpers
  (`clp-rust-utils/src/dataset.rs`: `register_dataset`), `ArchiveId::as_bytes`
  (`clp-rust-utils/src/types.rs`).
- Spider compression path: commit task (`clp-tdl-package/.../commit.rs`: archives + long-span
  archives + dataset registration, binds `BINARY(16)` UUIDs), compression coordinator registers the
  dataset before submitting tasks (`compression-coordinator/src/{bin,coordination,job_handle,
  error}.rs`).
- api-server `get_timestamp_column_names` on the shared column-metadata table.
- clp-s indexer (`components/core/src/clp_s/indexer/*`).
- Package tools: archive manager, dataset manager, per-dataset creation-time archive GC with the
  `UnknownRetentionPeriod` placeholder, `archive_output.retention_period` removed from config.
- Dataset-name validation relaxed to <=255 chars (Python + WebUI).
- WebUI on the shared schema, including review fixes: query parameters for
  `/api/archive-metadata/sql`, `buildLegacyMultiDatasetDetailsSql` kept, QuerySpeed builders that
  return `{queryParams, sql}`.

Deliberately excluded (unchanged vs upstream):

- **Celery path** — `scheduler/compress/compression_scheduler.py`,
  `executor/compress/compression_task.py`, `scheduler/query/query_scheduler.py`. The user's
  decision: "We are deprecating python path, so leave it broken." Consequences: Celery compression
  fails (`existing_datasets.add` on a dict; old column names), and search through the Celery query
  scheduler fails (old table/column names; `archive_output.retention_period` AttributeError).
- **#18 file-split metadata C++** — `clp_s/{ArchiveWriter.hpp, JsonParser.cpp,
  TimestampDictionaryWriter.{cpp,hpp}, TimestampEntry.{cpp,hpp}, archive_constants.hpp}`. This is
  Devin's separate "PR2".
- Dataset create/list API + Datasets page, partition maintenance, files tables (later PRs).

Fixed during review of the assembled branch (all pre-existing bugs carried over from beta):

- `validate_dataset_name` takes one argument now, but `scripts/compress.py`,
  `compress_from_s3.py`, `decompress.py`, `search.py`, `dataset_manager.py` still passed two
  (TypeError). Fixed; the dropped `get_clp_connection_params_and_type` calls were redundant since
  each script loads credentials earlier via `validate_and_load_db_credentials_file`.
- `controller.py` (clp-text branch of WebUI settings) and `scripts/native/decompress.py` called
  `get_*_table_name(table_prefix, None)`. Fixed.
- Removed dead `ClientError::InvalidDatasetName`; replaced `config.archive_output.clone()` with a
  move; fixed a `datasets_table_name` docstring typo and a stale `:param clp_table_prefix:`.
- Removed four feature-narrating comments we had added (api-server NOTE, `LAST_INSERT_ID` NOTE,
  two `job_handle.rs` docstring paragraphs), per the user's comment rule.

**Not verified** (no compiler/linter available in the sandbox): `cargo clippy`, `cargo test`,
`tsc -b`, `eslint`, `ruff`. Only `py_compile` and line lengths were checked. Run from
`docker-desktop` before pushing, e.g. `task lint:check-rust`, `task lint:check-py`, and in
`components/webui`: `node_modules/.bin/tsc -b && node_modules/.bin/eslint . --max-warnings 0`.

### 4.2 Open question in progress

The user's last remark before handoff: **"The test seems have different template than the other
tests."** It was not resolved. Commit `49c3f827` adds no new test functions as far as checked, so
first ask which test is meant. Candidates to look at: tests in files the commit touches
(`clp-rust-utils/src/types.rs` tests are upstream's; `clp-rust-utils/src/dataset.rs` tests;
`clp-rust-utils/src/clp_config/package/config.rs` tests), and any Python/TS tests. Compare against
the Rust guide's testing rules (no `test_` prefix, `expect` not `unwrap`, `anyhow::Result`
returns, base-constant-plus-override inputs).

### 4.3 Still open for the upstream PR (not in `49c3f827`)

- clp-text: `core/src/clp/GlobalMySQLMetadataDB.cpp` (and `glt/` copy) writes old column names;
  WebUI clp-text Details queries `clp_files`, which the shared schema doesn't create;
  `core/tools/scripts/db/init-db.py` still creates the old schema.
- Helm chart still sets `archive_output.retention_period` (`tools/deployment/package-helm/
  values.yaml`, `templates/configmap.yaml`, `templates/garbage-collector-deployment.yaml`); Python
  silently ignores it.
- `ArchiveId`'s `sqlx::Type/Encode/Decode` still encode text, contradicting its (Devin-requested)
  docstring; `.bind(archive_id)` would write 36 chars into `BINARY(16)`. Devin to decide: switch to
  binary or remove.
- `query_tasks.archive_id` is `VARCHAR(255)`; QuerySpeed converts with `UNHEX(REPLACE(...))` and has
  a TODO. Depends on whether `query_tasks` survives the move to `query-coordinator`.
- `register_dataset` returns a soft-deleted dataset's ID without clearing `is_deleted`.
- DDL is `CREATE TABLE IF NOT EXISTS`; existing deployments must recreate their database. Say so in
  the PR.
- Per-dataset retention (#14) goes beyond "existing functionality", and nothing can set
  `retention_period_minutes` until the dataset API lands. Confirm with Devin.
- `docs/src/user-docs/guides-retention.md` table lost its column alignment.
- No integration tests for the new schema.

---

## 5. Branch map (local; SHAs as of 2026-10-22)

| Branch | Tip | State |
|---|---|---|
| `schema-migration-upstream` | `49c3f827` | first upstream PR, assembled, not pushed |
| `schema-migration-handoff` | this doc on top of `49c3f827` | never PR this |
| `webui-shared-schema` | `a00dcae5` | pushed; PR open into beta, last review comment addressed |
| `archive-partition-maintenance` | `13d3a0f5` | partition maintenance rebased on beta, deferred |
| `schema-migration-integration` | `0734bbb1` | local demo: dataset API, Datasets page, mirrors |
| `origin/schema-migration-beta` | `f493afbf` | Devin's beta; #25 was squash-merged |

Merged into beta: #14 GC, #15 archive manager, #20 CAST fix, #21 build/lockfile, #22 shared tables
for clp-s, #23 register dataset before submission, #25 upstream merge + `ArchiveId` adaptation.

Dead local branches (content landed or superseded): `update-archive-gc`, `update-archive-manager`,
`update-archive-manager-schema`, `fix-archive-creation-time-cast`, `register-dataset-at-submission`,
`fix-coordinator-dataset-guard`, `update-clp-s-indexer`, `update-webui-schema`,
`merge-upstream-into-beta`, `fix-rust-build-breakage`, `fix-clp-s-metadata-tables`,
`register-dataset-before-compression`, `add-archive-partitions`.

---

## 6. Settled decisions worth not re-deriving

- **Archive identity**: `clp_archives.id` is a BIGINT surrogate; the UUID is `uuid BINARY(16)` with
  `KEY archive_uuid (dataset_id, uuid)`. The text form is for S3 keys and on-disk directories only.
- **Retention**: per dataset (`clp_datasets.retention_period_minutes`, NULL = forever), keyed on
  `creation_time_millis`. GC and query scheduler mirror one cutoff expression; the base is
  `min(now, earliest running query job's creation time)`.
- **GC crash recovery**: `DeletionCandidatesBuffer` reloads its recovery file in `__init__`, so
  `execute_deletion` must run even when the expiry query returns no rows.
- **Partitioning**: RANGE on `timestamp_range_begin_millis`, uniform 24-hour windows, `p_bad_low` /
  `p_bad_high` quarantine, drop-on-empty only, DDL only in maintenance. One `p_future` partition is
  enough for baseline.
- **Datasets**: implicit creation during compression will be removed (Devin), in favour of an
  explicit dataset-creation API. Until then the coordinator registers the dataset before submitting
  tasks because the clp-s indexer resolves `dataset_id` during compression.
- **WebUI SQL**: never interpolate values; pass `queryParams` to `querySql`. A builder whose SQL
  shape depends on its inputs returns `{queryParams, sql}` together.

---

## 7. Project plan (for later)

1. Baseline to upstream: 1.1 #18 file-split C++ (independent); 1.2 the schema swap (this branch);
   1.3 partition maintenance; 1.4 retention scope; files tables later (Devin's PR5).
2. Dataset API: create/list exist on `schema-migration-integration`; needs a dataset-name character
   set (names are joined into storage paths, so `/` and `..` are unsafe), soft-delete semantics,
   stop creating datasets during compression, provision `default`, timestamp keys on the dataset.
3. In-database top-level filtering: postings tables designed in
   `fork/claude/top-level-indexing-analysis-4n0nul:benchmarks/archive-metadata-schema/`; blocked on
   measuring real filter-value widths (dictionary encoding) and archive span distribution.
4. Packed-filter top-level filtering: C++ framework on `fork/tlii_cpp`; value-aware packing design
   and simulator on `fork/simulate_repacking` (`REPACKING.md`); `clp_packs` table not built.
