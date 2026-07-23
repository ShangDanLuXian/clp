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


def coerce_to_dict(value: Any) -> Optional[Dict[str, Any]]:
    """Coerces a JSON entry to a dict, tolerating shapes emitted by older analyzer builds: an
    object flattened into a list of [key, value] pairs is converted; anything else non-dict yields
    None."""
    if isinstance(value, dict):
        return value
    if (
        isinstance(value, list)
        and value
        and all(
            isinstance(pair, list) and 2 == len(pair) and isinstance(pair[0], str)
            for pair in value
        )
    ):
        return {pair[0]: pair[1] for pair in value}
    return None


def normalize_entries(entries: Any) -> List[Dict[str, Any]]:
    """Normalizes a JSON array of objects (reports, failures, components, columns), tolerating
    artifacts emitted by older analyzer builds: entries wrapped in a single-element array are
    unwrapped, pair-list entries are converted to dicts, and stray artifacts (e.g. empty arrays)
    are dropped."""
    normalized: List[Dict[str, Any]] = []
    if not isinstance(entries, list):
        return normalized
    for entry in entries:
        as_dict = coerce_to_dict(entry)
        if as_dict is not None:
            normalized.append(as_dict)
        elif isinstance(entry, list):
            for inner in entry:
                inner_dict = coerce_to_dict(inner)
                if inner_dict is not None:
                    normalized.append(inner_dict)
    return normalized


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
    """Builds the sanitized summary for one archive's report. The MPT's per-node fingerprints are
    intentionally omitted, keeping only the aggregate checksum and node count."""
    mpt = coerce_to_dict(report.get("mpt")) or {}
    log_type_dict = coerce_to_dict(report.get("log_type_dict")) or {}
    array_dict = coerce_to_dict(report.get("array_dict")) or {}
    return {
        "archive": report.get("path", ""),
        "archive_format_version": report.get("archive_format_version", ""),
        "total_size": report.get("total_size", 0),
        "uncompressed_size": report.get("uncompressed_size", 0),
        "num_records": report.get("num_records", 0),
        "num_schemas": report.get("num_schemas", 0),
        "mpt": {
            "num_nodes": mpt.get("num_nodes", 0),
            "checksum": str(mpt.get("checksum", "")),
        },
        "log_type_dict": {
            "num_entries": log_type_dict.get("num_entries", 0),
            "checksum": str(log_type_dict.get("checksum", "")),
        },
        "array_dict": {
            "num_entries": array_dict.get("num_entries", 0),
            "checksum": str(array_dict.get("checksum", "")),
        },
        "components": normalize_entries(report.get("components", [])),
        "column_summary": summarize_columns(normalize_entries(report.get("columns", []))),
    }


def build_set_similarity(
    reports: List[Dict[str, Any]], json_key: str, fingerprints_key: str
) -> Optional[Dict[str, Any]]:
    """Builds a cross-archive similarity summary for one fingerprinted set (the MPT, the log type
    dictionary, ...): groups of archives whose sets are identical (same checksum), and the
    pairwise similarity of every archive pair (Jaccard overlap of their per-item fingerprints).
    Returns None when fewer than two archives carry the set's data."""
    entries = []
    for report in reports:
        fingerprinted_set = coerce_to_dict(report.get(json_key)) or {}
        checksum = str(fingerprinted_set.get("checksum", ""))
        if not checksum:
            continue
        raw_fingerprints = fingerprinted_set.get(fingerprints_key, [])
        fingerprints = (
            {str(f) for f in raw_fingerprints} if isinstance(raw_fingerprints, list) else set()
        )
        entries.append(
            {
                "archive": str(report.get("path", "")),
                "checksum": checksum,
                "fingerprints": fingerprints,
            }
        )
    if len(entries) < 2:
        return None

    groups: Dict[str, List[str]] = {}
    for entry in entries:
        groups.setdefault(entry["checksum"], []).append(entry["archive"])
    identical_groups = [
        {"checksum": checksum, "archives": archives}
        for checksum, archives in sorted(groups.items())
        if len(archives) > 1
    ]

    pairs = []
    for i in range(len(entries)):
        for j in range(i + 1, len(entries)):
            entry_a, entry_b = entries[i], entries[j]
            if entry_a["checksum"] == entry_b["checksum"]:
                similarity = 100.0
            elif entry_a["fingerprints"] and entry_b["fingerprints"]:
                intersection = len(entry_a["fingerprints"] & entry_b["fingerprints"])
                union = len(entry_a["fingerprints"] | entry_b["fingerprints"])
                similarity = 100.0 * intersection / union if union > 0 else 0.0
            else:
                # Fingerprints are unavailable (e.g. an older analyzer build); only checksum
                # equality can be evaluated for this pair.
                continue
            pairs.append(
                {
                    "archive_a": entry_a["archive"],
                    "archive_b": entry_b["archive"],
                    "similarity_percent": similarity,
                }
            )
    pairs.sort(key=lambda pair: -pair["similarity_percent"])

    return {
        "num_archives": len(entries),
        "identical_groups": identical_groups,
        "pairwise": pairs,
    }


def build_merged_set_stats(
    pack: List[Dict[str, Any]], json_key: str, fingerprints_key: str, component_name: Optional[str]
) -> Optional[Dict[str, Any]]:
    """Computes the exact merged-set statistics for one dictionary kind over one pack of
    archives: the total entry count, the exact union entry count (from the per-item
    fingerprints), and, when a component name is given, the summed on-disk component size and a
    projected merged size. The projection is byte-exact when per-entry sizes are available
    (scaled by the pack's measured compression factor), and falls back to entry-count scaling
    otherwise."""
    total_entries = 0
    union_entry_sizes: Dict[str, int] = {}
    have_all_sizes = True
    sum_component_size = 0
    sum_uncompressed_size = 0
    present = False
    for report in pack:
        fingerprinted_set = coerce_to_dict(report.get(json_key)) or {}
        fingerprints = fingerprinted_set.get(fingerprints_key, [])
        if not isinstance(fingerprints, list) or not fingerprinted_set.get("checksum", ""):
            continue
        present = True
        count_key = "num_nodes" if "mpt" == json_key else "num_entries"
        total_entries += int(fingerprinted_set.get(count_key, len(fingerprints)))
        entry_sizes = fingerprinted_set.get("entry_sizes")
        if isinstance(entry_sizes, list) and len(entry_sizes) == len(fingerprints):
            for fingerprint, size in zip(fingerprints, entry_sizes):
                union_entry_sizes.setdefault(str(fingerprint), int(size))
            sum_uncompressed_size += sum(int(size) for size in entry_sizes)
        else:
            have_all_sizes = False
            for fingerprint in fingerprints:
                union_entry_sizes.setdefault(str(fingerprint), 0)
        if component_name is not None:
            for component in normalize_entries(report.get("components", [])):
                if component.get("name") == component_name:
                    sum_component_size += int(component.get("size", 0))

    if not present or not union_entry_sizes:
        return None

    stats: Dict[str, Any] = {
        "total_entries": total_entries,
        "union_entries": len(union_entry_sizes),
        "dedup_factor": total_entries / len(union_entry_sizes),
    }
    if component_name is not None and sum_component_size > 0:
        stats["sum_component_size"] = sum_component_size
        if have_all_sizes and sum_uncompressed_size > 0:
            # Project the merged on-disk size by applying the pack's measured compression factor
            # to the union's uncompressed bytes.
            compression_factor = sum_component_size / sum_uncompressed_size
            stats["projected_merged_size"] = int(
                sum(union_entry_sizes.values()) * compression_factor
            )
            stats["projection_method"] = "entry-size-weighted"
        else:
            stats["projected_merged_size"] = int(
                sum_component_size * len(union_entry_sizes) / max(total_entries, 1)
            )
            stats["projection_method"] = "entry-count-scaled"
    return stats


# The dictionary kinds evaluated by the merged-dictionary estimate: (JSON key, fingerprints key,
# component name on disk).
MERGE_ESTIMATE_KINDS: List[Tuple[str, str, Optional[str]]] = [
    ("log_type_dict", "entry_fingerprints", "log.dict"),
    ("array_dict", "entry_fingerprints", "array.dict"),
    ("mpt", "node_fingerprints", None),
]


def build_merge_estimate(
    reports: List[Dict[str, Any]], pack_size: int
) -> Optional[Dict[str, Any]]:
    """Groups the archives into packs of `pack_size` (in input order; the last pack may be
    partial) and computes the exact merged-dictionary statistics for each pack, plus an overall
    row merging every archive. Returns None when no report carries fingerprint data."""
    packs = []
    for pack_start in range(0, len(reports), pack_size):
        pack = reports[pack_start : pack_start + pack_size]
        dictionaries = {}
        for json_key, fingerprints_key, component_name in MERGE_ESTIMATE_KINDS:
            stats = build_merged_set_stats(pack, json_key, fingerprints_key, component_name)
            if stats is not None:
                dictionaries[json_key] = stats
        if dictionaries:
            packs.append(
                {
                    "num_archives": len(pack),
                    "first_archive": str(pack[0].get("path", "")),
                    "last_archive": str(pack[-1].get("path", "")),
                    "dictionaries": dictionaries,
                }
            )
    if not packs:
        return None

    overall = {}
    for json_key, fingerprints_key, component_name in MERGE_ESTIMATE_KINDS:
        stats = build_merged_set_stats(reports, json_key, fingerprints_key, component_name)
        if stats is not None:
            overall[json_key] = stats
    return {"pack_size": pack_size, "packs": packs, "overall": overall}


def render_merged_set_stats(name: str, stats: Dict[str, Any], out: TextIO) -> None:
    """Renders one dictionary kind's merged-set statistics as a single line."""
    line = (
        f"    {name:<14} {stats['total_entries']} entries -> {stats['union_entries']} merged"
        f" ({stats['dedup_factor']:.1f}x dedup)"
    )
    if "projected_merged_size" in stats:
        line += (
            f"; on-disk {format_size(int(stats['sum_component_size']))}"
            f" -> ~{format_size(int(stats['projected_merged_size']))}"
            f" ({stats['projection_method']})"
        )
    out.write(line + "\n")


def render_merge_estimate(estimate: Dict[str, Any], out: TextIO) -> None:
    """Renders the merged-dictionary estimate."""
    out.write(f"Merged-dictionary estimate (packs of {estimate['pack_size']}, in input order):\n")
    for pack_idx, pack in enumerate(estimate["packs"]):
        out.write(
            f"  Pack {pack_idx + 1}: {pack['num_archives']} archives"
            f" ({pack['first_archive']} .. {pack['last_archive']})\n"
        )
        for name, stats in pack["dictionaries"].items():
            render_merged_set_stats(name, stats, out)
    if estimate["overall"]:
        out.write("  All archives merged into one:\n")
        for name, stats in estimate["overall"].items():
            render_merged_set_stats(name, stats, out)
    out.write("\n")


def render_similarity(title: str, similarity: Dict[str, Any], out: TextIO) -> None:
    """Renders one cross-archive similarity summary."""
    out.write(f"{title} similarity across archives:\n")
    if similarity["identical_groups"]:
        for group in similarity["identical_groups"]:
            out.write(
                f"  {len(group['archives'])} archives are identical"
                f" (checksum {group['checksum']}):\n"
            )
            for archive in group["archives"]:
                out.write(f"    {archive}\n")
    else:
        out.write("  No two archives are identical.\n")

    pairs = similarity["pairwise"]
    if pairs:
        max_rendered_pairs = 20
        out.write("\n  Pairwise similarity (fingerprint-set overlap):\n")
        for pair in pairs[:max_rendered_pairs]:
            out.write(
                f"    {pair['similarity_percent']:>6.1f}%  {pair['archive_a']}"
                f"  <->  {pair['archive_b']}\n"
            )
        if len(pairs) > max_rendered_pairs:
            values = [pair["similarity_percent"] for pair in pairs]
            out.write(
                f"    ... {len(pairs) - max_rendered_pairs} more pairs"
                f" (min {min(values):.1f}%, avg {sum(values) / len(values):.1f}%,"
                f" max {max(values):.1f}%)\n"
            )
    out.write("\n")


def render_text(
    analyzer_version: str,
    summaries: List[Dict[str, Any]],
    failures: List[Dict[str, Any]],
    similarities: List[Tuple[str, Optional[Dict[str, Any]]]],
    merge_estimate: Optional[Dict[str, Any]],
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

    for title, similarity in similarities:
        if similarity is not None:
            render_similarity(title, similarity, out)

    if merge_estimate is not None:
        render_merge_estimate(merge_estimate, out)

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
        mpt = summary.get("mpt") or {}
        if mpt.get("checksum"):
            out.write(f"  MPT: {mpt.get('num_nodes', 0)} nodes, checksum {mpt['checksum']}\n")
        log_type_dict = summary.get("log_type_dict") or {}
        if log_type_dict.get("checksum"):
            out.write(
                f"  Log types: {log_type_dict.get('num_entries', 0)} entries,"
                f" checksum {log_type_dict['checksum']}\n"
            )
        array_dict = summary.get("array_dict") or {}
        if array_dict.get("checksum"):
            out.write(
                f"  Array types: {array_dict.get('num_entries', 0)} entries,"
                f" checksum {array_dict['checksum']}\n"
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
        "--merge-estimate",
        type=int,
        metavar="N",
        help=(
            "Group archives into packs of N (in input order) and report the exact merged"
            " dictionary sizes per pack, computed from the per-entry fingerprints."
        ),
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
        reports = normalize_entries(analysis)
        failures = []
    else:
        analyzer_version = str(analysis.get("analyzer_version", "unknown"))
        reports = normalize_entries(analysis.get("reports", []))
        failures = normalize_entries(analysis.get("failures", []))

    if not reports and not failures:
        print("No archive reports found in the input.", file=sys.stderr)
        return 1
    if not reports:
        print(
            "Warning: every archive failed to analyze; the report will only list the failures.",
            file=sys.stderr,
        )

    summaries = [summarize_report(report) for report in reports]
    mpt_similarity = build_set_similarity(reports, "mpt", "node_fingerprints")
    log_type_similarity = build_set_similarity(reports, "log_type_dict", "entry_fingerprints")
    array_type_similarity = build_set_similarity(reports, "array_dict", "entry_fingerprints")
    similarities = [
        ("MPT (merged parse tree)", mpt_similarity),
        ("Log type", log_type_similarity),
        ("Array type", array_type_similarity),
    ]
    merge_estimate = None
    if args.merge_estimate is not None:
        if args.merge_estimate < 1:
            print("--merge-estimate must be at least 1.", file=sys.stderr)
            return 1
        merge_estimate = build_merge_estimate(reports, args.merge_estimate)
        if merge_estimate is None:
            print(
                "Warning: no fingerprint data found; the merged-dictionary estimate was"
                " skipped.",
                file=sys.stderr,
            )

    if args.mapping:
        with open(args.mapping, "w", encoding="utf-8") as mapping_file:
            mapping_file.write("# LOCAL ONLY - do NOT share this file.\n")
            mapping_file.write("# Maps anonymized column IDs in the report to column paths.\n")
            for report in reports:
                mapping_file.write(f"\nArchive: {report.get('path', '')}\n")
                for column_id, path in build_column_name_mapping(
                    normalize_entries(report.get("columns", []))
                ):
                    mapping_file.write(f"  {column_id}  {path}\n")

    if args.json:
        output_document = {
            "analyzer_version": analyzer_version,
            "report_generator_version": REPORT_GENERATOR_VERSION,
            "contains_column_names": False,
            "failed_archives": failures,
            "mpt_similarity": mpt_similarity,
            "log_type_similarity": log_type_similarity,
            "array_type_similarity": array_type_similarity,
            "merge_estimate": merge_estimate,
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
            render_text(
                analyzer_version, summaries, failures, similarities, merge_estimate, output_file
            )
    else:
        render_text(analyzer_version, summaries, failures, similarities, merge_estimate, sys.stdout)

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
