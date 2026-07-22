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
