# archive-analyzer

A standalone, read-only diagnostic tool that inspects [CLP](https://github.com/y-scope/clp)
(clp-s) archives and prints statistics about them. It ships as a container image built from this
source, so you can review exactly what it does before running it and exactly what it collected
before sharing any of it.

## Quick start

Load the image you received (requires Docker; no build tooling needed):

```bash
docker load < archive-analyzer-<version>.tar.gz
```

(If you prefer to build the image yourself from this source instead — e.g. for a security
review — run `./build.sh` from this directory; see "Auditing the source" below.)

Analyze local archives:

```bash
docker run --rm -v /data/archives:/archives -v "$PWD/out:/out" archive-analyzer \
    /archives/<archive-id> --output-dir /out
```

Analyze a random sample of 20 archives stored on S3 (credentials via the standard AWS
environment variables; only single-file archives are supported over S3):

```bash
docker run --rm -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
    -v "$PWD/out:/out" archive-analyzer \
    s3://my-bucket/archives/ --sample 20 --seed 1 --output-dir /out
```

Either way, three files land in `out/`:

| file | what it is | share? |
|---|---|---|
| `report.txt` | Shareable report - **no column names, no log content** | review, then yes |
| `analysis.json` | Full analysis (contains column names) | keep local |
| `column_names.local.txt` | Maps anonymized column IDs to real names | keep local |

Useful flags: `--no-columns` (skip the slow per-column pass), `--merge-estimate N` (include
merged-dictionary size estimates for packs of N archives), `--sample N --seed S` (reproducible
sampling).

## What it collects

For each archive (a local archive directory, a local single-file archive, or a single-file
archive on S3):

* Total size, uncompressed size, compression ratio, format version, and record/schema counts.
* A per-component size breakdown (dictionaries, encoded record tables, metadata, ...).
* Per-column statistics: type, number of values, number of distinct values. This pass
  decompresses every record table; skip it with `--no-columns`.
* An MPT (merged parse tree) fingerprint: a canonical checksum of the archive's schema tree plus
  one-way per-node hashes, letting the report identify archives with identical MPTs and measure
  MPT similarity across archives - without exposing any key names.
* Log type and array dictionary fingerprints (checksums, one-way per-entry hashes, and per-entry
  sizes): dictionary similarity across archives and entry-size tier histograms - without exposing
  the templates themselves.

## What it does NOT do

* **No network access beyond the archives you name.** The analyzer connects only to the archive
  locations passed on the command line (e.g. your S3 bucket, using your credentials). There is no
  telemetry and no other endpoint in the code - auditable below.
* **No writes to your data.** Archives are only read; outputs go to the directory you choose.
* **No automatic reporting.** Nothing leaves your machine. You review `report.txt` and decide
  what (if anything) to share with us.

## Auditing the source

The tool is a small addition on top of the open-source CLP codebase; no existing CLP code is
modified. The complete audit surface is this directory plus one `add_subdirectory` line:

```bash
git diff main...archive_analyzer -- components/core/src/clp_s
```

You don't have to trust the prebuilt image: `./build.sh` builds the same image from this source
in a clean `ubuntu:22.04` container, with third-party dependencies downloaded pinned by checksum
- so the image you run can be one you produced from source you read. (`./build.sh --binary`
exports just the binary instead, printing its SHA256. Maintainers use `./release.sh` to produce
the distributable tarball plus its SHA256.)

## Running without the container

Build natively with the standard CLP core setup (dependencies per
`components/core/tools/scripts/lib_install/`), then from the repository root:

```bash
task deps:core codegen:clp-s-generate-parsers
cmake -S components/core -B build/core \
    -C build/deps/cpp/cmake-settings/all-core.cmake \
    -DCMAKE_BUILD_TYPE=Release -DCLP_BUILD_TESTING=OFF
cmake --build build/core --target archive-analyzer --parallel
```

The underlying tools compose like this (the container's entrypoint, `analyze.py`, just wires
them together and adds S3 listing/sampling):

```bash
archive-analyzer --json [--auth s3] <archives...> > analysis.json
python3 generate_report.py analysis.json -o report.txt \
    --mapping column_names.local.txt [--merge-estimate N]
```

`archive-analyzer --version` prints the build's provenance (version + git description), which is
also stamped into every report.

## The shareable report

`report.txt` contains **no column names and no log content**: columns appear only as anonymized
IDs (`column_001`, ...) with type and cardinality statistics; dictionaries appear only as
checksums, counts, size tiers, and cross-archive similarity percentages. Review it, then share it
if you're comfortable with its contents. `column_names.local.txt` maps anonymized IDs back to
real column paths for your own reference (e.g. if we ask about a specific column on a call) and
should **not** be shared.
