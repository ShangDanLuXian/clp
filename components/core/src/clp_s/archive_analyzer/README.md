# archive-analyzer

A standalone, read-only diagnostic tool that inspects [CLP](https://github.com/y-scope/clp)
(clp-s) archives and prints statistics about them. It ships as a container image built from this
source, so you can review exactly what it does before running it and exactly what it collected
before sharing any of it.

## Quick start

Requires Docker only. Keep the image tarball you were given next to `run-analyzer.sh` — the
script loads it on first use — and run:

```bash
# All archives in a directory
./run-analyzer.sh /path/to/your/archives

# A random sample of 20 archives on S3 (AWS credentials from the environment or ~/.aws)
./run-analyzer.sh s3://my-bucket/archives/ --sample 20 --seed 1

# Faster pass, plus merged-dictionary estimates for packs of 16
./run-analyzer.sh /path/to/your/archives --no-columns --merge-estimate 16
```

The script mounts your archives read-only, runs the analyzer as your user, and writes the results
to `./analyzer-output` (override with `--output-dir`).

Prefer to build the image yourself before running anything? Use `build-from-source.sh` — it
clones this repository at a pinned version, builds the image, and tells you how to run it. See
"Auditing the source" below.

<details>
<summary>Running the container directly, without the scripts</summary>

```bash
docker run --rm -v /path/to/your/archives:/archives -v "$PWD/out:/out" archive-analyzer \
    /archives --output-dir /out
```

Paths are always as seen **inside** the container, so don't use a shell glob like `/archives/*`
— your shell would expand it against the host filesystem. Pass `/archives` (or
`/archives/<archive-id>` for a single archive) instead. For S3, add `-e AWS_ACCESS_KEY_ID -e
AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN` and pass the `s3://` location instead of a mount.
</details>

Either way, three files land in `out/`:

| file | what it is | share? |
|---|---|---|
| `report.txt` | Shareable report - **no column names, no log content** | review, then yes |
| `analysis.json` | Full analysis (contains column names) | keep local |
| `column_names.local.txt` | Maps anonymized column IDs to real names | keep local |

Useful flags:

| flag | effect |
|---|---|
| `--no-columns` | Skip the per-column statistics pass (much faster) |
| `--sample N --seed S` | Analyze a reproducible random sample of N archives |
| `--max-archives N` | Never consider more than N archives (default: 1024) |
| `--merge-estimate N` | Pack size for the merged-dictionary estimate (default: 128) |
| `--no-merge-estimate` | Omit the merged-dictionary estimate |
| `--sections LIST` | Only include some report sections (see below) |
| `--show-column-names` | Put real column names in the report instead of anonymized IDs |

`--max-archives` bounds the work (and, for S3, the download cost): listing stops once N objects
have been found, so a large bucket is never enumerated or fetched in full. Analyzing one archive
takes roughly a second locally, plus download time for remote archives — so the default of 1024
is on the order of 20 minutes. Combine with `--sample` for a smaller run.

`--sections` takes a comma-separated list of `failures`, `similarity`, `merge`, `summary`,
`components`, `dictionaries`, `columns` (default: all) — e.g. `--sections columns` for just the
column statistics, or `--sections similarity,merge` for just the cross-archive views.

`--show-column-names` produces a report for **your own** use: it's labelled as containing column
names and is not the version to share. Without it, columns appear as anonymized IDs and the real
names go only into the separate local mapping file.

## What it collects

For each archive (a local archive directory, a local single-file archive, or a single-file
archive on S3):

* Total size, uncompressed size, compression ratio, format version, and record/schema counts.
* A per-component size breakdown (dictionaries, encoded record tables, metadata, ...).
* Per-column statistics: type, number of values, number of distinct values, and cardinality
  (distinct/total), reported most-common-column first. This pass decompresses every record table;
  skip it with `--no-columns`.
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

You don't have to trust the prebuilt image. `./build-from-source.sh` clones this repository at a
pinned version, checks it out for you to read, and builds the image in a clean `ubuntu:22.04`
container with third-party dependencies pinned by checksum:

```bash
./build-from-source.sh                     # source is left in ./archive-analyzer-src
./run-analyzer.sh --image archive-analyzer:<commit>-selfbuilt /path/to/your/archives
```

Container images aren't bit-for-bit reproducible, so a self-built image's digest won't match the
prebuilt one even from identical source — the assurance is that you can run an image *you* built
from source you read, not that the two are byte-identical. Reports record which build produced
them, so a self-built image is identifiable in its own output.

(`./build.sh` builds from an existing checkout; `./build.sh --binary` exports just the binary
with its SHA256; maintainers use `./release.sh` to produce the distributable tarball.)

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
    --mapping column_names.local.txt [--merge-estimate N] [--sections LIST]
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
