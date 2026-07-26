#!/usr/bin/env python3
"""Driver for the archive analyzer: resolves inputs (local paths, URLs, or s3:// locations),
optionally samples a subset of archives, runs `archive-analyzer`, and generates the shareable
report - one command end to end.

Usage:
    # Local archives
    analyze.py /archives/* --output-dir out

    # A sample of 20 archives under an S3 prefix (credentials from the standard AWS env vars)
    analyze.py s3://bucket/prefix/ --sample 20 --output-dir out

Outputs written to the output directory:
    analysis.json            full analysis (stays with you; contains column names)
    report.txt               shareable report (no column names)
    column_names.local.txt   anonymized-column-ID mapping (keep local; do NOT share)

s3:// locations require the `boto3` package (preinstalled in the container image).
"""

import argparse
import os
import random
import subprocess
import sys
import urllib.parse
from typing import Callable, List, Optional, Tuple

def is_s3_uri(path: str) -> bool:
    return path.startswith("s3://")


def split_s3_uri(uri: str) -> Tuple[str, str]:
    """Splits an s3://bucket/key-or-prefix URI into (bucket, key_or_prefix)."""
    without_scheme = uri[len("s3://") :]
    bucket, _, key = without_scheme.partition("/")
    if not bucket:
        raise ValueError(f"Invalid S3 URI: {uri}")
    return bucket, key


def build_https_url(bucket: str, key: str, region: str) -> str:
    """Builds the virtual-hosted-style HTTPS URL for an S3 object, which the analyzer reads
    directly (signing requests itself when --auth s3 is used)."""
    quoted_key = urllib.parse.quote(key, safe="/")
    return f"https://{bucket}.s3.{region}.amazonaws.com/{quoted_key}"


def list_s3_objects(uri: str, limit: Optional[int] = None) -> List[str]:
    """Lists the objects under an s3:// URI (a single object or a prefix) and returns their
    HTTPS URLs. Listing stops once `limit` objects have been found, so a huge bucket is never
    enumerated (or fetched) in full."""
    try:
        import boto3
    except ImportError:
        print(
            "Error: s3:// inputs require the boto3 package (pip install boto3).",
            file=sys.stderr,
        )
        raise SystemExit(1)

    bucket, prefix = split_s3_uri(uri)
    client = boto3.client("s3")
    location = client.get_bucket_location(Bucket=bucket).get("LocationConstraint")
    region = location if location else "us-east-1"

    urls: List[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get("Contents", []):
            key = entry["Key"]
            if key.endswith("/") or 0 == entry.get("Size", 0):
                continue
            urls.append(build_https_url(bucket, key, region))
            if limit is not None and len(urls) >= limit:
                return urls
    return urls


# Files that make up a multi-file (directory) archive; a directory containing only these (and
# numbered segment files) is an archive itself rather than a directory of archives.
ARCHIVE_COMPONENT_FILES = {
    "header",
    "schema_tree",
    "schema_ids",
    "table_metadata",
    "var.dict",
    "log.dict",
    "array.dict",
}


def is_archive_directory(path: str) -> bool:
    """Returns whether `path` is a multi-file archive (as opposed to a directory containing
    archives), mirroring clp-s's own detection."""
    try:
        entries = list(os.scandir(path))
    except OSError:
        return False
    if not entries:
        return False
    for entry in entries:
        if entry.is_dir():
            return False
        if entry.name in ARCHIVE_COMPONENT_FILES or entry.name.isdigit():
            continue
        return False
    return True


def expand_local_input(path: str) -> List[str]:
    """Expands a local input into concrete archive paths: a directory containing archives expands
    to those archives; an archive (or a regular file) resolves to itself."""
    if not os.path.isdir(path):
        return [path]
    if is_archive_directory(path):
        return [path]
    return sorted(entry.path for entry in os.scandir(path))


def resolve_inputs(
    inputs: List[str],
    s3_lister: Callable[..., List[str]] = list_s3_objects,
    max_archives: Optional[int] = None,
) -> Tuple[List[str], bool, bool]:
    """Expands the given inputs into concrete archive paths/URLs, considering at most
    `max_archives` of them. Returns the paths, whether any input came from S3 (implying s3
    authentication), and whether the inputs were truncated by the cap."""
    resolved: List[str] = []
    any_s3 = False
    truncated = False
    missing: List[str] = []
    for raw_input in inputs:
        remaining: Optional[int] = None
        if max_archives is not None:
            remaining = max_archives - len(resolved)
            if remaining <= 0:
                truncated = True
                break

        if is_s3_uri(raw_input):
            any_s3 = True
            objects = s3_lister(raw_input, remaining)
            if not objects:
                print(f"Warning: no objects found under {raw_input}", file=sys.stderr)
            if remaining is not None and len(objects) >= remaining:
                truncated = True
            resolved.extend(objects[:remaining] if remaining is not None else objects)
        elif "://" in raw_input:
            resolved.append(raw_input)
        elif os.path.exists(raw_input):
            expanded = expand_local_input(raw_input)
            if remaining is not None and len(expanded) > remaining:
                expanded = expanded[:remaining]
                truncated = True
            resolved.extend(expanded)
        else:
            missing.append(raw_input)

    if missing:
        print(f"Error: {len(missing)} input path(s) do not exist:", file=sys.stderr)
        for path in missing[:5]:
            print(f"  {path}", file=sys.stderr)
        if len(missing) > 5:
            print(f"  ... and {len(missing) - 5} more", file=sys.stderr)
        print(
            "\nIf you're running in a container, pass paths as seen INSIDE the container and"
            "\nmount the archives, e.g.:"
            "\n  docker run --rm -v <host-archive-dir>:/archives -v \"$PWD/out:/out\" \\"
            "\n      archive-analyzer /archives --output-dir /out"
            "\nNote that shell globs (/archives/*) are expanded on the host, so pass the"
            "\ncontaining directory instead - it expands to its archives automatically.",
            file=sys.stderr,
        )
        raise SystemExit(1)
    return resolved, any_s3, truncated


def sample_archives(paths: List[str], sample_size: Optional[int], seed: Optional[int]) -> List[str]:
    """Returns a random sample of `sample_size` paths (all of them when the sample is unset or
    not smaller than the population), reproducible via `seed`."""
    if sample_size is None or sample_size >= len(paths):
        return paths
    return random.Random(seed).sample(paths, sample_size)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Analyzes clp-s archives (local, or sampled from S3) and generates a shareable"
            " report."
        )
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        help=(
            "Archive locations: local archive paths, HTTPS URLs of single-file archives, or"
            " s3://bucket/prefix locations (each object under the prefix is treated as a"
            " single-file archive)."
        ),
    )
    parser.add_argument(
        "--sample",
        type=int,
        metavar="N",
        help="Analyze a random sample of N archives instead of all of them.",
    )
    parser.add_argument("--seed", type=int, help="Random seed for reproducible sampling.")
    parser.add_argument(
        "--max-archives",
        type=int,
        default=1024,
        metavar="N",
        help=(
            "Never consider more than N archives (default: 1024). For s3:// inputs, listing"
            " stops at N objects, so a large bucket is neither enumerated nor downloaded in"
            " full. Analyzing an archive takes roughly a second locally, plus download time for"
            " remote archives."
        ),
    )
    parser.add_argument(
        "--auth",
        choices=["auto", "s3", "none"],
        default="auto",
        help=(
            "Authentication for network requests (default: auto - s3 when any s3:// input is"
            " given). s3 uses the AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (and optionally"
            " AWS_SESSION_TOKEN) environment variables."
        ),
    )
    parser.add_argument(
        "--no-columns",
        action="store_true",
        help="Skip the per-column statistics pass (much faster).",
    )
    parser.add_argument(
        "--merge-estimate",
        type=int,
        default=128,
        metavar="N",
        help="Pack size for the merged-dictionary estimate in the report (default: 128).",
    )
    parser.add_argument(
        "--no-merge-estimate",
        action="store_true",
        help="Omit the merged-dictionary estimate from the report.",
    )
    parser.add_argument(
        "--sections",
        metavar="LIST",
        help=(
            "Comma-separated list of report sections to include (failures, similarity, merge,"
            " summary, components, dictionaries, columns), or 'all' (default)."
        ),
    )
    parser.add_argument(
        "--show-column-names",
        action="store_true",
        help=(
            "Include real column names in the report instead of anonymized IDs. The report is"
            " then NOT safe to share."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory to write analysis.json, report.txt, and the column-name mapping to.",
    )
    args = parser.parse_args()

    if args.max_archives < 1:
        print("Error: --max-archives must be at least 1.", file=sys.stderr)
        return 1

    paths, any_s3, truncated = resolve_inputs(args.inputs, max_archives=args.max_archives)
    if not paths:
        print("Error: no archives to analyze.", file=sys.stderr)
        return 1
    if truncated:
        print(
            f"Note: stopped at {args.max_archives} archives (--max-archives); there may be more."
            + (
                "\nThe sample is drawn from those archives, not from every archive available."
                if args.sample is not None
                else ""
            ),
            file=sys.stderr,
        )
    paths = sample_archives(paths, args.sample, args.seed)
    print(f"Analyzing {len(paths)} archive(s)...", file=sys.stderr)

    auth = args.auth
    if "auto" == auth:
        auth = "s3" if any_s3 else "none"

    os.makedirs(args.output_dir, exist_ok=True)
    analysis_path = os.path.join(args.output_dir, "analysis.json")
    report_path = os.path.join(args.output_dir, "report.txt")
    mapping_path = os.path.join(args.output_dir, "column_names.local.txt")

    analyzer = os.environ.get("ARCHIVE_ANALYZER_BIN", "archive-analyzer")
    analyzer_cmd = [analyzer, "--json", "--auth", auth]
    if args.no_columns:
        analyzer_cmd.append("--no-columns")
    analyzer_cmd.extend(paths)
    with open(analysis_path, "w", encoding="utf-8") as analysis_file:
        analyzer_result = subprocess.run(analyzer_cmd, stdout=analysis_file, check=False)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    generate_report = os.environ.get(
        "GENERATE_REPORT_SCRIPT", os.path.join(script_dir, "generate_report.py")
    )
    report_cmd = [
        sys.executable,
        generate_report,
        analysis_path,
        "-o",
        report_path,
        "--mapping",
        mapping_path,
    ]
    if args.no_merge_estimate:
        report_cmd.append("--no-merge-estimate")
    else:
        report_cmd.extend(["--merge-estimate", str(args.merge_estimate)])
    if args.sections is not None:
        report_cmd.extend(["--sections", args.sections])
    if args.show_column_names:
        report_cmd.append("--show-column-names")
    report_result = subprocess.run(report_cmd, check=False)

    report_label = (
        "Report (INCLUDES column names):"
        if args.show_column_names
        else "Shareable report:            "
    )
    print(f"\nAnalysis (keep local):        {analysis_path}", file=sys.stderr)
    print(f"{report_label} {report_path}", file=sys.stderr)
    print(f"Column-name mapping (LOCAL):  {mapping_path}", file=sys.stderr)
    if 0 != analyzer_result.returncode:
        print(
            "Note: some archives failed to analyze; see messages above and the report's"
            " failure section.",
            file=sys.stderr,
        )
    return max(analyzer_result.returncode, report_result.returncode)


if __name__ == "__main__":
    sys.exit(main())
