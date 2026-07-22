#!/usr/bin/env python3
"""Generates a shareable summary report from `archive-analyzer --json` output.

The generated report is designed to be safe to share: it contains NO column names or paths. Columns
appear only as anonymized IDs (`column_001`, ...) with their type and cardinality statistics, plus
aggregated views (e.g. how many columns fall into each cardinality range).

Optionally, a mapping from anonymized column IDs to real column paths can be written to a separate
file with `--mapping`. That file is for your own reference only and should NOT be shared.

Usage:
    archive-analyzer --json /path/to/archive > analysis.json
    python3 generate_report.py analysis.json -o report.txt --mapping column_names.local.txt

    # Review report.txt, then share it if you're comfortable with its contents.
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional, TextIO, Tuple

REPORT_GENERATOR_VERSION = "0.1.0"

# Upper bounds (exclusive, in percent) and labels of the cardinality ranges. A column's
# cardinality percentage is (num distinct values / num values) * 100.
CARDINALITY_RANGES: List[Tuple[float, str]] = [
    (1.0, "<1%"),
    (10.0, "1-10%"),
    (25.0, "10-25%"),
    (50.0, "25-50%"),
    (75.0, "50-75%"),
    (95.0, "75-95%"),
    (100.0, "95-<100%"),
    (float("inf"), "100%"),
]


def format_size(num_bytes: int) -> str:
    """Formats a byte count as a human-readable string (e.g. "1.2 MiB")."""
    size = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024.0:
            return f"{num_bytes} B" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TiB"


def get_cardinality_range_label(cardinality_percent: float) -> str:
    """Returns the label of the cardinality range that `cardinality_percent` falls into."""
    for upper_bound, label in CARDINALITY_RANGES:
        if cardinality_percent < upper_bound or upper_bound == float("inf"):
            return label
    return CARDINALITY_RANGES[-1][1]


def summarize_columns(columns: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Builds the sanitized (name-free) column summary for one archive's report."""
    sanitized_columns: List[Dict[str, Any]] = []
    columns_by_type: Dict[str, int] = {}
    histogram: Dict[str, int] = {label: 0 for _, label in CARDINALITY_RANGES}

    for idx, column in enumerate(sorted(columns, key=lambda c: c.get("path", ""))):
        num_values = int(column.get("num_values", 0))
        num_distinct = int(column.get("num_distinct_values", 0))
        column_type = str(column.get("type", "Unknown"))
        cardinality_percent: Optional[float] = (
            (100.0 * num_distinct / num_values) if num_values > 0 else None
        )

        sanitized_columns.append(
            {
                "id": f"column_{idx + 1:03d}",
                "type": column_type,
                "num_values": num_values,
                "num_distinct_values": num_distinct,
                "cardinality_percent": cardinality_percent,
            }
        )
        columns_by_type[column_type] = columns_by_type.get(column_type, 0) + 1
        if cardinality_percent is not None:
            histogram[get_cardinality_range_label(cardinality_percent)] += 1

    num_columns_with_values = sum(histogram.values())
    return {
        "num_columns": len(sanitized_columns),
        "columns_by_type": dict(sorted(columns_by_type.items())),
        "cardinality_histogram": [
            {
                "range": label,
                "num_columns": histogram[label],
                "percent_of_columns": (
                    100.0 * histogram[label] / num_columns_with_values
                    if num_columns_with_values > 0
                    else 0.0
                ),
            }
            for _, label in CARDINALITY_RANGES
        ],
        "columns": sanitized_columns,
    }


def build_column_name_mapping(columns: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    """Builds the (anonymized ID, real column path) mapping for one archive's report."""
    sorted_columns = sorted(columns, key=lambda c: c.get("path", ""))
    return [
        (f"column_{idx + 1:03d}", str(column.get("path", "")))
        for idx, column in enumerate(sorted_columns)
    ]


def summarize_report(report: Dict[str, Any]) -> Dict[str, Any]:
    """Builds the sanitized summary for one archive's report."""
    return {
        "archive": report.get("path", ""),
        "archive_format_version": report.get("archive_format_version", ""),
        "total_size": report.get("total_size", 0),
        "uncompressed_size": report.get("uncompressed_size", 0),
        "num_records": report.get("num_records", 0),
        "num_schemas": report.get("num_schemas", 0),
        "components": report.get("components", []),
        "column_summary": summarize_columns(report.get("columns", [])),
    }


def render_text(
    analyzer_version: str,
    summaries: List[Dict[str, Any]],
    failures: List[Dict[str, Any]],
    out: TextIO,
) -> None:
    """Renders the sanitized summaries as a human-readable text report."""
    out.write(f"# archive-analyzer report (analyzer {analyzer_version},")
    out.write(f" report generator {REPORT_GENERATOR_VERSION})\n")
    out.write("# This report contains no column names.\n\n")

    if failures:
        out.write(f"Archives that FAILED to analyze: {len(failures)}\n")
        for failure in failures:
            out.write(f"  {failure.get('path', '')}\n")
            out.write(f"    {failure.get('error', '')}\n")
        out.write("\n")

    for summary in summaries:
        out.write(f"Archive: {summary['archive']}\n")
        out.write(f"  Format version: {summary['archive_format_version']}\n")
        total_size = int(summary["total_size"])
        uncompressed_size = int(summary["uncompressed_size"])
        out.write(f"  Size: {format_size(total_size)} ({total_size} bytes)\n")
        if total_size > 0:
            ratio = uncompressed_size / total_size
            out.write(
                f"  Uncompressed size: {format_size(uncompressed_size)}"
                f" (compression ratio {ratio:.1f}x)\n"
            )
        out.write(
            f"  Records: {summary['num_records']} across {summary['num_schemas']} schemas\n"
        )

        out.write("\n  Components:\n")
        out.write(f"    {'name':<24} {'size':>12} {'%':>8}\n")
        for component in summary["components"]:
            out.write(
                f"    {component['name']:<24} {format_size(int(component['size'])):>12}"
                f" {component.get('percentage', 0.0):>7.1f}%\n"
            )

        column_summary = summary["column_summary"]
        if 0 == column_summary["num_columns"]:
            out.write("\n  Columns: (no column statistics collected)\n\n")
            continue

        out.write(f"\n  Columns: {column_summary['num_columns']} total, by type:\n")
        for column_type, count in column_summary["columns_by_type"].items():
            out.write(f"    {column_type:<24} {count:>8}\n")

        out.write("\n  Cardinality distribution (distinct values / values, % of columns):\n")
        for bucket in column_summary["cardinality_histogram"]:
            if 0 == bucket["num_columns"]:
                continue
            out.write(
                f"    {bucket['range']:<12} {bucket['num_columns']:>8} columns"
                f" {bucket['percent_of_columns']:>7.1f}%\n"
            )

        out.write("\n  Per-column statistics (anonymized):\n")
        out.write(
            f"    {'id':<12} {'type':<20} {'values':>12} {'distinct':>12} {'cardinality':>12}\n"
        )
        for column in column_summary["columns"]:
            cardinality = (
                f"{column['cardinality_percent']:.2f}%"
                if column["cardinality_percent"] is not None
                else "n/a"
            )
            out.write(
                f"    {column['id']:<12} {column['type']:<20} {column['num_values']:>12}"
                f" {column['num_distinct_values']:>12} {cardinality:>12}\n"
            )
        out.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generates a shareable summary report (no column names) from"
            " `archive-analyzer --json` output."
        )
    )
    parser.add_argument(
        "input",
        help="Path to the `archive-analyzer --json` output, or '-' to read from stdin.",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Path to write the report to (default: stdout).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Write the report as JSON instead of text.",
    )
    parser.add_argument(
        "--mapping",
        help=(
            "Path to write the anonymized-column-ID-to-column-path mapping to. This file is for"
            " your own reference and should NOT be shared."
        ),
    )
    args = parser.parse_args()

    try:
        if "-" == args.input:
            raw_input_text = sys.stdin.read()
        else:
            with open(args.input, encoding="utf-8") as input_file:
                raw_input_text = input_file.read()
    except OSError as e:
        print(f"Failed to read analysis input: {e}", file=sys.stderr)
        return 1

    stripped_input = raw_input_text.lstrip()
    if not stripped_input:
        print(
            "Analysis input is empty. The analyzer likely failed on every archive; check the"
            " messages it printed to stderr.",
            file=sys.stderr,
        )
        return 1
    if stripped_input.startswith("#") or stripped_input.startswith("Archive:"):
        print(
            "Analysis input looks like the analyzer's text output, which this script can't"
            " parse. Re-run the analyzer with --json, e.g.:"
            " `archive-analyzer --json <archive> > analysis.json`.",
            file=sys.stderr,
        )
        return 1

    try:
        analysis = json.loads(raw_input_text)
    except json.JSONDecodeError as e:
        print(f"Failed to parse analysis input as JSON: {e}", file=sys.stderr)
        return 1

    # Accept both the current wrapped format ({"analyzer_version", "reports": [...]}) and a bare
    # list of reports.
    if isinstance(analysis, list):
        analyzer_version = "unknown"
        reports = analysis
        failures = []
    else:
        analyzer_version = str(analysis.get("analyzer_version", "unknown"))
        reports = analysis.get("reports", [])
        failures = analysis.get("failures", [])

    if not reports and not failures:
        print("No archive reports found in the input.", file=sys.stderr)
        return 1
    if not reports:
        print(
            "Warning: every archive failed to analyze; the report will only list the failures.",
            file=sys.stderr,
        )

    summaries = [summarize_report(report) for report in reports]

    if args.mapping:
        with open(args.mapping, "w", encoding="utf-8") as mapping_file:
            mapping_file.write("# LOCAL ONLY - do NOT share this file.\n")
            mapping_file.write("# Maps anonymized column IDs in the report to column paths.\n")
            for report in reports:
                mapping_file.write(f"\nArchive: {report.get('path', '')}\n")
                for column_id, path in build_column_name_mapping(report.get("columns", [])):
                    mapping_file.write(f"  {column_id}  {path}\n")

    if args.json:
        output_document = {
            "analyzer_version": analyzer_version,
            "report_generator_version": REPORT_GENERATOR_VERSION,
            "contains_column_names": False,
            "failed_archives": failures,
            "reports": summaries,
        }
        rendered = json.dumps(output_document, indent=2) + "\n"
        if args.output:
            with open(args.output, "w", encoding="utf-8") as output_file:
                output_file.write(rendered)
        else:
            sys.stdout.write(rendered)
    elif args.output:
        with open(args.output, "w", encoding="utf-8") as output_file:
            render_text(analyzer_version, summaries, failures, output_file)
    else:
        render_text(analyzer_version, summaries, failures, sys.stdout)

    if args.output:
        print(f"Report written to {args.output}", file=sys.stderr)
    if args.mapping:
        print(
            f"Column-name mapping written to {args.mapping} - keep this file local.",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except BrokenPipeError:
        # The downstream consumer (e.g. `head`) closed the pipe; exit quietly.
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        sys.exit(1)
