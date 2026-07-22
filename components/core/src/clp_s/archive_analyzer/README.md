# archive-analyzer

A standalone, read-only diagnostic tool that inspects [CLP](https://github.com/y-scope/clp)
(clp-s) archives and prints statistics about them. It is meant to be built and run by you, on your
own machines, so you can review exactly what information it collects before sharing any of it.

## What it collects

For each archive path you give it (a single-file archive or an archive directory):

* The archive's total size, uncompressed size, compression ratio, format version, and the number
  of records and schemas it contains.
* A per-component size breakdown (dictionaries, encoded record tables, metadata, ...) with each
  component's percentage of the archive.
* Per-column statistics: each column's path, type, number of values, and number of distinct
  values. This pass decompresses every record table, so it can take a while for large archives;
  skip it with `--no-columns`.
* An MPT (merged parse tree) fingerprint: a canonical checksum of the archive's schema tree plus
  one-way per-node hashes, letting `generate_report.py` identify archives with identical MPTs and
  measure MPT similarity across archives - without exposing any key names.
* Log type and array dictionary fingerprints: the same checksum-plus-one-way-hash scheme applied
  to each dictionary's entries, measuring how much archives share log message templates - without
  exposing the templates themselves.

## What it does NOT do

* **No network access.** The recommended build disables CLP's optional libcurl support, so no
  networking library is linked into the binary at all. You can verify this with
  `ldd archive-analyzer`.
* **No writes.** The tool only reads the archive paths passed on the command line and prints to
  stdout. It never modifies archives and never creates files.
* **No automatic reporting.** Nothing leaves your machine. You review the output and decide what
  (if anything) to share with us.

## Auditing the source

This tool is a small addition on top of the open-source CLP codebase; no existing CLP code is
modified. The complete audit surface is this directory plus one `add_subdirectory` line, which you
can verify with:

```bash
git diff main...archive_analyzer -- components/core/src/clp_s
```

## Building

Requires Docker. From this directory:

```bash
./build.sh
```

This builds the binary from source in a clean `ubuntu:22.04` container (third-party dependencies
are downloaded pinned by checksum) and writes it to `out/archive-analyzer`, printing its SHA256.
The resulting binary runs on the machine that built it (or inside the same container image).

Alternatively, build natively with the standard CLP core setup: install the dependencies for your
platform from `components/core/tools/scripts/lib_install/`, then from the repository root:

```bash
task deps:core codegen:clp-s-generate-parsers
cmake -S components/core -B build/core \
    -C build/deps/cpp/cmake-settings/all-core.cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCLP_BUILD_CLP_S_ENABLE_CURL=OFF \
    -DCLP_BUILD_TESTING=OFF
cmake --build build/core --target archive-analyzer --parallel
```

## Usage

```bash
# Analyze one or more archives
archive-analyzer /path/to/archive1 /path/to/archive2

# Fast pass: skip the per-column statistics
archive-analyzer --no-columns /path/to/archive

# Machine-readable output
archive-analyzer --json /path/to/archive

# Version / provenance
archive-analyzer --version
```

Every report starts with the analyzer's version and the git description of the source it was
built from, so you (and we) always know which build produced a given report.

## Preparing a shareable report

The analyzer's own output includes column names, which may be sensitive. To produce a report that
is safe to share, run the analyzer with `--json` and pass the result through
`generate_report.py` (Python 3, standard library only):

```bash
archive-analyzer --json /path/to/archive > analysis.json
python3 generate_report.py analysis.json -o report.txt --mapping column_names.local.txt
```

`report.txt` contains **no column names**: columns appear only as anonymized IDs (`column_001`,
...) with their type and cardinality statistics, plus aggregated views (columns by type, and the
distribution of columns across cardinality ranges). Review it, then share it if you're
comfortable with its contents.

`column_names.local.txt` maps the anonymized IDs back to real column paths. It is for your own
reference (e.g. if we ask about a specific column on a call) and should **not** be shared.

