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


def list_s3_objects(uri: str) -> List[str]:
    """Lists the objects under an s3:// URI (a single object or a prefix) and returns their
    HTTPS URLs."""
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

    urls = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get("Contents", []):
            key = entry["Key"]
            if key.endswith("/") or 0 == entry.get("Size", 0):
                continue
            urls.append(build_https_url(bucket, key, region))
    return urls


def resolve_inputs(
    inputs: List[str], s3_lister: Callable[[str], List[str]] = list_s3_objects
) -> Tuple[List[str], bool]:
    """Expands the given inputs into concrete archive paths/URLs. Returns the paths and whether
    any input came from S3 (implying s3 authentication)."""
    resolved: List[str] = []
    any_s3 = False
    for raw_input in inputs:
        if is_s3_uri(raw_input):
            any_s3 = True
            objects = s3_lister(raw_input)
            if not objects:
                print(f"Warning: no objects found under {raw_input}", file=sys.stderr)
            resolved.extend(objects)
        else:
            resolved.append(raw_input)
    return resolved, any_s3


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
        metavar="N",
        help="Include a merged-dictionary estimate for packs of N archives in the report.",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory to write analysis.json, report.txt, and the column-name mapping to.",
    )
    args = parser.parse_args()

    paths, any_s3 = resolve_inputs(args.inputs)
    if not paths:
        print("Error: no archives to analyze.", file=sys.stderr)
        return 1
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
    if args.merge_estimate is not None:
        report_cmd.extend(["--merge-estimate", str(args.merge_estimate)])
    report_result = subprocess.run(report_cmd, check=False)

    print(f"\nAnalysis (keep local):        {analysis_path}", file=sys.stderr)
    print(f"Shareable report:             {report_path}", file=sys.stderr)
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
