#!/usr/bin/env python3
"""Simulates packing archives into fixed-size packs and measures how much archive-level pruning
survives at pack granularity.

Premise: a database-level index over low-cardinality columns can prune archives that don't contain
a queried value. But when archives are stored in packs of K (e.g. merged dictionaries per 128
archives), the pruning granularity becomes the pack: a pack must be opened if ANY of its archives
matches. With value-agnostic (random or arrival-order) packing, even highly selective predicates
tend to touch nearly every pack. This tool quantifies that collapse and evaluates value-aware
packing strategies that recover the pruning.

Input: analysis.json produced by `archive-analyzer --json` with per-value fingerprints enabled
(`--value-fingerprints CAP`, on by default). Every archive analyzed with column statistics
contributes; each low-cardinality column's `value_fingerprints` say which values the archive
contains.

The simulated workload is one equality predicate per distinct value of each index column
(`col = v` for every v that appears anywhere), so reported averages weight each value equally.

Strategies:
    original   Archives in input order (typically arrival/time order).
    random     Random assignment, averaged over --trials shuffles.
    sorted     Archives sorted by their value-set signature (grouped by identical/similar sets),
               then cut into consecutive packs.
    greedy     Packs grown greedily: each pack starts from the first unassigned archive and
               repeatedly adds the archive whose value sets grow the pack's per-column unions
               the least (normalized per column), i.e. clustering that directly minimizes the
               number of packs each value lands in.

For every strategy the tool reports, per column and overall: the archive-level pruning (packing
independent), the pack-level pruning, the fraction of archive-level pruning retained, and the
fraction of all archives that must be read when reads happen at pack granularity. The `floor`
column is the best any packing could do: value v present in n_v archives needs at least
ceil(n_v / K) packs.

Usage:
    simulate_repacking.py analysis.json [-o report.txt]
    simulate_repacking.py analysis.json --pack-sizes 32,128,512 --columns s,c,id
"""

import argparse
import collections
import json
import math
import random
import sys
from typing import Dict, List, Optional, Sequence, Set, Tuple

METADATA_PREFIX = "[metadata] "


class ColumnData:
    """Per-column view over all archives: which archives contain the column, and which distinct
    values (as fingerprints) each of them contains."""

    def __init__(self, path: str) -> None:
        self.path = path
        # archive index -> set of value fingerprints (present only for archives that have the
        # column AND had its fingerprints recorded)
        self.values_by_archive: Dict[int, Set[str]] = {}
        # archives that have the column but whose fingerprints weren't recorded (cardinality
        # above the analyzer's cap); membership is unknown there
        self.unknown_archives: Set[int] = set()

    @property
    def num_present(self) -> int:
        return len(self.values_by_archive) + len(self.unknown_archives)

    def value_universe(self) -> Set[str]:
        universe: Set[str] = set()
        for values in self.values_by_archive.values():
            universe |= values
        return universe


def load_columns(reports: List[dict]) -> Dict[str, ColumnData]:
    columns: Dict[str, ColumnData] = {}
    for archive_idx, report in enumerate(reports):
        for column in report.get("columns", []):
            path = column.get("path", "")
            if path.startswith(METADATA_PREFIX):
                continue
            data = columns.setdefault(path, ColumnData(path))
            fingerprints = column.get("value_fingerprints")
            if fingerprints is None:
                data.unknown_archives.add(archive_idx)
            else:
                # The same path can appear once per node type (e.g. as both VarString and
                # ClpString); a query filters on the path, so the variants' values are unioned.
                data.values_by_archive.setdefault(archive_idx, set()).update(fingerprints)
    for data in columns.values():
        # An archive where any variant of the column exceeded the recording cap has incomplete
        # membership; treat the whole archive as unknown for this column.
        for archive_idx in data.unknown_archives:
            data.values_by_archive.pop(archive_idx, None)
    return columns


def value_archive_counts(column: ColumnData) -> "collections.Counter":
    """For each value in the column's universe, the number of archives (with known membership)
    containing it."""
    counts: "collections.Counter" = collections.Counter()
    for values in column.values_by_archive.values():
        counts.update(values)
    return counts


def archive_pruning(column: ColumnData, num_archives: int) -> float:
    """Average, over the column's value universe, of the fraction of archives an archive-level
    index prunes for `col = v`. Archives with unknown membership count as unprunable."""
    counts = value_archive_counts(column)
    if not counts:
        return 0.0
    unknown = len(column.unknown_archives)
    total = sum(1.0 - (count + unknown) / num_archives for count in counts.values())
    return total / len(counts)


def select_index_columns(
    columns: Dict[str, ColumnData],
    num_archives: int,
    requested: Optional[List[str]],
    max_columns: int,
    min_coverage: float,
) -> List[ColumnData]:
    if requested:
        selected = []
        for path in requested:
            if path not in columns:
                print(f"Error: column \"{path}\" not found in the analysis.", file=sys.stderr)
                print("Available columns:", file=sys.stderr)
                for available in sorted(columns):
                    print(f"  {available}", file=sys.stderr)
                raise SystemExit(1)
            column = columns[path]
            if column.unknown_archives:
                print(
                    f"Warning: column \"{path}\" has no recorded value fingerprints in"
                    f" {len(column.unknown_archives)} archive(s) (cardinality above the"
                    " analyzer's cap there); those archives are treated as matching every"
                    " value.",
                    file=sys.stderr,
                )
            selected.append(column)
        return selected

    candidates = []
    for column in columns.values():
        if column.unknown_archives:
            continue  # Membership unknown somewhere: unusable for an exact index simulation.
        if column.num_present < min_coverage * num_archives:
            continue
        if len(column.value_universe()) < 2:
            continue
        candidates.append((archive_pruning(column, num_archives), column))
    candidates.sort(key=lambda item: (-item[0], item[1].path))
    return [column for _, column in candidates[:max_columns]]


def pack_assignment_to_packs(order: Sequence[int], pack_size: int) -> List[List[int]]:
    return [list(order[start : start + pack_size]) for start in range(0, len(order), pack_size)]


def order_sorted_by_signature(
    num_archives: int, index_columns: List[ColumnData]
) -> List[int]:
    """Sorts archives by their per-column value-set signatures (columns in selection order, i.e.
    most-discriminating first), grouping archives with identical then similar sets."""

    def signature(archive_idx: int) -> Tuple:
        key = []
        for column in index_columns:
            values = column.values_by_archive.get(archive_idx)
            if values is None:
                # Unknown membership sorts after everything so must-open archives share packs.
                key.append((1,) if archive_idx in column.unknown_archives else (0,))
            else:
                key.append((0, tuple(sorted(values))))
        return tuple(key)

    return sorted(range(num_archives), key=signature)


def order_greedy_union(
    num_archives: int, index_columns: List[ColumnData], pack_size: int
) -> List[int]:
    """Builds packs greedily, always adding the unassigned archive whose values grow the current
    pack's per-column unions the least. Union growth is normalized by each column's universe size
    so no single column dominates. Ties fall back to input order, keeping temporal affinity."""
    universe_sizes = [max(1, len(column.value_universe())) for column in index_columns]
    unassigned = list(range(num_archives))
    order: List[int] = []
    while unassigned:
        seed = unassigned.pop(0)
        order.append(seed)
        unions: List[Set[str]] = [
            set(column.values_by_archive.get(seed, ())) for column in index_columns
        ]
        members = 1
        while members < pack_size and unassigned:
            best_idx = 0
            best_cost = None
            for position, candidate in enumerate(unassigned):
                cost = 0.0
                for column_idx, column in enumerate(index_columns):
                    values = column.values_by_archive.get(candidate)
                    if values is None:
                        continue
                    added = len(values - unions[column_idx])
                    cost += added / universe_sizes[column_idx]
                if best_cost is None or cost < best_cost:
                    best_cost = cost
                    best_idx = position
                    if 0.0 == cost:
                        break  # No growth at all; nothing can beat this.
            chosen = unassigned.pop(best_idx)
            order.append(chosen)
            for column_idx, column in enumerate(index_columns):
                values = column.values_by_archive.get(chosen)
                if values is not None:
                    unions[column_idx] |= values
            members += 1
    return order


class ColumnResult:
    def __init__(self) -> None:
        self.pack_pruning = 0.0
        self.read_fraction = 0.0
        self.floor_pruning = 0.0
        # One entry per value of the column's (sorted) universe:
        # (archive_selectivity, pack_pruning, floor_pruning)
        self.per_value: List[Tuple[float, float, float]] = []


def evaluate_packing(
    packs: List[List[int]],
    index_columns: List[ColumnData],
    num_archives: int,
    pack_size: int,
) -> Dict[str, ColumnResult]:
    """Evaluates one pack assignment: for each index column, averages over its value universe the
    pack-level pruning, the fraction of archives read at pack granularity, and the pruning floor
    of an optimal packing."""
    results: Dict[str, ColumnResult] = {}
    num_packs = len(packs)
    for column in index_columns:
        result = ColumnResult()
        universe = sorted(column.value_universe())
        if not universe or 0 == num_packs:
            results[column.path] = result
            continue
        # For each pack, the union of its archives' values (None = contains an unknown-membership
        # archive, so the pack matches everything).
        pack_unions: List[Optional[Set[str]]] = []
        for pack in packs:
            if any(archive_idx in column.unknown_archives for archive_idx in pack):
                pack_unions.append(None)
                continue
            union: Set[str] = set()
            for archive_idx in pack:
                union |= column.values_by_archive.get(archive_idx, set())
            pack_unions.append(union)

        counts = value_archive_counts(column)
        total_pack_pruning = 0.0
        total_read_fraction = 0.0
        total_floor_pruning = 0.0
        for value in universe:
            matched = [
                pack_idx
                for pack_idx, union in enumerate(pack_unions)
                if union is None or value in union
            ]
            value_pack_pruning = 1.0 - len(matched) / num_packs
            total_pack_pruning += value_pack_pruning
            archives_read = sum(len(packs[pack_idx]) for pack_idx in matched)
            total_read_fraction += archives_read / num_archives
            num_matching_archives = counts[value] + len(column.unknown_archives)
            floor_packs = math.ceil(num_matching_archives / pack_size)
            value_floor_pruning = 1.0 - floor_packs / num_packs
            total_floor_pruning += value_floor_pruning
            result.per_value.append(
                (num_matching_archives / num_archives, value_pack_pruning, value_floor_pruning)
            )
        count = len(universe)
        result.pack_pruning = total_pack_pruning / count
        result.read_fraction = total_read_fraction / count
        result.floor_pruning = total_floor_pruning / count
        results[column.path] = result
    return results


def average_results(per_trial: List[Dict[str, ColumnResult]]) -> Dict[str, ColumnResult]:
    averaged: Dict[str, ColumnResult] = {}
    for path in per_trial[0]:
        result = ColumnResult()
        result.pack_pruning = sum(trial[path].pack_pruning for trial in per_trial) / len(per_trial)
        result.read_fraction = sum(trial[path].read_fraction for trial in per_trial) / len(
            per_trial
        )
        result.floor_pruning = sum(trial[path].floor_pruning for trial in per_trial) / len(
            per_trial
        )
        # Per-value entries align across trials since every trial walks the same sorted universe.
        for entries in zip(*(trial[path].per_value for trial in per_trial)):
            result.per_value.append(
                (
                    entries[0][0],
                    sum(entry[1] for entry in entries) / len(entries),
                    sum(entry[2] for entry in entries) / len(entries),
                )
            )
        averaged[path] = result
    return averaged


def simulate(
    reports: List[dict],
    index_columns: List[ColumnData],
    pack_size: int,
    trials: int,
    seed: Optional[int],
) -> Dict[str, Dict[str, ColumnResult]]:
    """Runs every strategy for one pack size. Returns strategy -> column path -> result."""
    num_archives = len(reports)
    strategies: Dict[str, Dict[str, ColumnResult]] = {}

    original_order = list(range(num_archives))
    strategies["original"] = evaluate_packing(
        pack_assignment_to_packs(original_order, pack_size),
        index_columns,
        num_archives,
        pack_size,
    )

    rng = random.Random(seed)
    random_trials = []
    for _ in range(trials):
        shuffled = original_order[:]
        rng.shuffle(shuffled)
        random_trials.append(
            evaluate_packing(
                pack_assignment_to_packs(shuffled, pack_size),
                index_columns,
                num_archives,
                pack_size,
            )
        )
    strategies["random"] = average_results(random_trials)

    strategies["sorted"] = evaluate_packing(
        pack_assignment_to_packs(
            order_sorted_by_signature(num_archives, index_columns), pack_size
        ),
        index_columns,
        num_archives,
        pack_size,
    )

    strategies["greedy"] = evaluate_packing(
        pack_assignment_to_packs(
            order_greedy_union(num_archives, index_columns, pack_size), pack_size
        ),
        index_columns,
        num_archives,
        pack_size,
    )

    return strategies


STRATEGY_ORDER = ["random", "original", "sorted", "greedy"]

# Selectivity buckets for the per-value breakdown: (upper bound, label). A value's archive
# selectivity is the fraction of archives containing it; e.g. a value in 5% of archives (an
# archive-level index prunes 95%) falls in the second bucket.
SELECTIVITY_BUCKETS = [
    (0.01, "<=1% of archives"),
    (0.05, "1-5%"),
    (0.20, "5-20%"),
    (0.50, "20-50%"),
    (1.01, ">50%"),
]


def bucket_index(selectivity: float) -> int:
    for bucket_idx, (upper, _) in enumerate(SELECTIVITY_BUCKETS):
        if selectivity <= upper:
            return bucket_idx
    return len(SELECTIVITY_BUCKETS) - 1


def render_report(
    reports: List[dict],
    columns: Dict[str, ColumnData],
    index_columns: List[ColumnData],
    pack_sizes: List[int],
    all_results: Dict[int, Dict[str, Dict[str, ColumnResult]]],
    trials: int,
) -> str:
    num_archives = len(reports)
    lines: List[str] = []
    lines.append("Archive repacking simulation")
    lines.append("=" * 60)
    lines.append(f"Archives: {num_archives}")
    lines.append("")
    lines.append("Index columns (most discriminating first):")
    lines.append(
        f"  {'column':<40} {'coverage':>9} {'distinct':>9} {'union':>7} {'arch-prune':>11}"
    )
    archive_pruning_by_path = {}
    for column in index_columns:
        pruning = archive_pruning(column, num_archives)
        archive_pruning_by_path[column.path] = pruning
        avg_distinct = sum(
            len(values) for values in column.values_by_archive.values()
        ) / max(1, len(column.values_by_archive))
        lines.append(
            f"  {column.path:<40} {column.num_present / num_archives:>8.0%}"
            f" {avg_distinct:>9.1f} {len(column.value_universe()):>7}"
            f" {pruning:>10.1%}"
        )
    lines.append("")
    lines.append(
        "arch-prune: fraction of archives an archive-level index prunes, averaged over one"
    )
    lines.append(
        "equality query per distinct value. distinct: average per archive. union: across all"
    )
    lines.append("archives.")
    lines.append("")

    for pack_size in pack_sizes:
        strategies = all_results[pack_size]
        num_packs = math.ceil(num_archives / pack_size)
        lines.append("")
        lines.append(f"Pack size {pack_size} ({num_packs} packs)")
        lines.append("-" * 60)
        lines.append(
            "Pack-level pruning (fraction of packs skipped; higher is better; 'floor' is the"
        )
        lines.append(f"best any packing could reach; random averaged over {trials} trials):")
        header = f"  {'column':<38} {'arch':>7}"
        for strategy in STRATEGY_ORDER:
            header += f" {strategy:>9}"
        header += f" {'floor':>9}"
        lines.append(header)
        for column in index_columns:
            row = f"  {column.path:<38} {archive_pruning_by_path[column.path]:>7.1%}"
            for strategy in STRATEGY_ORDER:
                row += f" {strategies[strategy][column.path].pack_pruning:>9.1%}"
            row += f" {strategies['greedy'][column.path].floor_pruning:>9.1%}"
            lines.append(row)
        if index_columns:
            row = f"  {'ALL (mean)':<38}"
            mean_arch = sum(archive_pruning_by_path.values()) / len(index_columns)
            row += f" {mean_arch:>7.1%}"
            for strategy in STRATEGY_ORDER:
                mean = sum(
                    strategies[strategy][column.path].pack_pruning for column in index_columns
                ) / len(index_columns)
                row += f" {mean:>9.1%}"
            mean_floor = sum(
                strategies["greedy"][column.path].floor_pruning for column in index_columns
            ) / len(index_columns)
            row += f" {mean_floor:>9.1%}"
            lines.append(row)

        lines.append("")
        mean_archive_read = 1 - (
            sum(archive_pruning_by_path.values()) / max(1, len(index_columns))
        )
        lines.append("Fraction of all archives read when reads happen at pack granularity")
        lines.append(
            f"(lower is better; an archive-level index would read {mean_archive_read:.1%}):"
        )
        header = f"  {'column':<38}"
        for strategy in STRATEGY_ORDER:
            header += f" {strategy:>9}"
        lines.append(header)
        for column in index_columns:
            row = f"  {column.path:<38}"
            for strategy in STRATEGY_ORDER:
                row += f" {strategies[strategy][column.path].read_fraction:>9.1%}"
            lines.append(row)
        if index_columns:
            row = f"  {'ALL (mean)':<38}"
            for strategy in STRATEGY_ORDER:
                mean = sum(
                    strategies[strategy][column.path].read_fraction for column in index_columns
                ) / len(index_columns)
                row += f" {mean:>9.1%}"
            lines.append(row)

        lines.append("")
        lines.append(
            "Pack-level pruning by value selectivity (all index-column values pooled; e.g. a"
        )
        lines.append(
            "value in 5% of archives - where an archive-level index prunes 95% - is in the"
            " '1-5%' row):"
        )
        header = f"  {'values present in':<38} {'count':>7}"
        for strategy in STRATEGY_ORDER:
            header += f" {strategy:>9}"
        header += f" {'floor':>9}"
        lines.append(header)
        for bucket_idx, (_, label) in enumerate(SELECTIVITY_BUCKETS):
            bucket_count = 0
            strategy_totals = {strategy: 0.0 for strategy in STRATEGY_ORDER}
            floor_total = 0.0
            for column in index_columns:
                per_value_by_strategy = {
                    strategy: strategies[strategy][column.path].per_value
                    for strategy in STRATEGY_ORDER
                }
                for value_idx, entry in enumerate(per_value_by_strategy["greedy"]):
                    if bucket_index(entry[0]) != bucket_idx:
                        continue
                    bucket_count += 1
                    floor_total += entry[2]
                    for strategy in STRATEGY_ORDER:
                        strategy_totals[strategy] += per_value_by_strategy[strategy][value_idx][1]
            if 0 == bucket_count:
                continue
            row = f"  {label:<38} {bucket_count:>7}"
            for strategy in STRATEGY_ORDER:
                row += f" {strategy_totals[strategy] / bucket_count:>9.1%}"
            row += f" {floor_total / bucket_count:>9.1%}"
            lines.append(row)
    lines.append("")
    return "\n".join(lines)


def results_to_json(
    all_results: Dict[int, Dict[str, Dict[str, ColumnResult]]],
    index_columns: List[ColumnData],
    num_archives: int,
) -> dict:
    output: dict = {"num_archives": num_archives, "pack_sizes": {}}
    output["index_columns"] = {
        column.path: {
            "coverage": column.num_present / num_archives,
            "union_distinct": len(column.value_universe()),
            "archive_pruning": archive_pruning(column, num_archives),
        }
        for column in index_columns
    }
    for pack_size, strategies in all_results.items():
        output["pack_sizes"][str(pack_size)] = {
            strategy: {
                path: {
                    "pack_pruning": result.pack_pruning,
                    "read_fraction": result.read_fraction,
                    "floor_pruning": result.floor_pruning,
                }
                for path, result in per_column.items()
            }
            for strategy, per_column in strategies.items()
        }
    return output


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Simulates archive packing strategies and measures pack-level pruning for"
            " low-cardinality-column equality queries."
        )
    )
    parser.add_argument("analysis", help="analysis.json produced by archive-analyzer --json.")
    parser.add_argument(
        "--pack-sizes",
        default="128",
        metavar="LIST",
        help="Comma-separated pack sizes to simulate (default: 128).",
    )
    parser.add_argument(
        "--columns",
        metavar="LIST",
        help=(
            "Comma-separated column paths to index (default: auto-select the most discriminating"
            " low-cardinality columns)."
        ),
    )
    parser.add_argument(
        "--max-columns",
        type=int,
        default=8,
        metavar="N",
        help="Number of index columns to auto-select (default: 8).",
    )
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=0.8,
        metavar="F",
        help=(
            "Auto-select only columns present in at least this fraction of archives"
            " (default: 0.8)."
        ),
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=5,
        metavar="T",
        help="Number of random-packing trials to average (default: 5).",
    )
    parser.add_argument("--seed", type=int, default=0, help="Random seed (default: 0).")
    parser.add_argument("-o", "--output", help="Write the report here instead of stdout.")
    parser.add_argument("--json", metavar="FILE", help="Also write the raw results as JSON.")
    args = parser.parse_args()

    with open(args.analysis, "r", encoding="utf-8") as analysis_file:
        analysis = json.load(analysis_file)
    reports = [report for report in analysis.get("reports", []) if report.get("columns")]
    if not reports:
        print(
            "Error: the analysis contains no per-column statistics; re-run archive-analyzer"
            " without --no-columns (and with value fingerprints enabled).",
            file=sys.stderr,
        )
        return 1

    columns = load_columns(reports)
    if not any(column.values_by_archive for column in columns.values()):
        print(
            "Error: the analysis contains no per-value fingerprints; re-run archive-analyzer"
            " with --value-fingerprints (it is on by default in current versions).",
            file=sys.stderr,
        )
        return 1

    requested = args.columns.split(",") if args.columns else None
    index_columns = select_index_columns(
        columns, len(reports), requested, args.max_columns, args.min_coverage
    )
    if not index_columns:
        print("Error: no usable index columns found.", file=sys.stderr)
        return 1

    pack_sizes = [int(size) for size in args.pack_sizes.split(",")]
    all_results = {}
    for pack_size in pack_sizes:
        if pack_size < 1:
            print("Error: pack sizes must be at least 1.", file=sys.stderr)
            return 1
        all_results[pack_size] = simulate(
            reports, index_columns, pack_size, args.trials, args.seed
        )

    report = render_report(reports, columns, index_columns, pack_sizes, all_results, args.trials)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as output_file:
            output_file.write(report)
        print(f"Wrote {args.output}", file=sys.stderr)
    else:
        print(report)

    if args.json:
        with open(args.json, "w", encoding="utf-8") as json_file:
            json.dump(results_to_json(all_results, index_columns, len(reports)), json_file)
            json_file.write("\n")
        print(f"Wrote {args.json}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
