#ifndef CLP_S_ARCHIVE_ANALYZER_ARCHIVE_ANALYZER_HPP
#define CLP_S_ARCHIVE_ANALYZER_ARCHIVE_ANALYZER_HPP

#include <cstdint>
#include <string>
#include <vector>

#include <nlohmann/json_fwd.hpp>

#include <clp_s/archive_analyzer/MptFingerprint.hpp>
#include <clp_s/archive_analyzer/SetFingerprint.hpp>
#include <clp_s/SchemaTree.hpp>

namespace clp_s::archive_analyzer {
/**
 * The on-disk size of one component of an archive (e.g. a dictionary, the encoded record tables).
 */
struct ComponentStats {
    std::string name;
    uint64_t size{};
};

/**
 * Statistics for one column of an archive. A column is identified by its schema tree node, so the
 * same column is merged across all schemas (tables) that contain it.
 */
struct ColumnStats {
    std::string path;
    NodeType type{NodeType::Unknown};
    uint64_t num_values{};
    uint64_t num_distinct_values{};
};

/**
 * The full set of statistics collected from one archive.
 */
struct ArchiveStats {
    std::string path;
    std::string archive_format_version;
    uint64_t total_size{};
    uint64_t uncompressed_size{};
    uint64_t num_records{};
    uint64_t num_schemas{};
    MptFingerprint mpt;
    // Fingerprint of the log type dictionary's entries (the log message templates).
    SetFingerprint log_type_dict;
    // Fingerprint of the array dictionary's entries.
    SetFingerprint array_dict;
    std::vector<ComponentStats> components;
    // Empty when the cardinality pass is skipped.
    std::vector<ColumnStats> columns;
};

/**
 * @return The analyzer's version, including the git description of the source it was built from
 * when available.
 */
[[nodiscard]] auto get_analyzer_version() -> std::string;

/**
 * Analyzes an archive, collecting its total size, the size of each of its components, and
 * (optionally) per-column statistics.
 *
 * @param archive_path Path to a clp-s archive: either a single-file archive or an archive
 * directory.
 * @param collect_column_stats Whether to run the per-column statistics pass. The pass decompresses
 * every record table in the archive, so it can take a while for large archives.
 * @return The collected statistics.
 * @throws clp_s::TraceableException (or its derived classes) if the archive cannot be read.
 * @throws std::filesystem::filesystem_error if the archive's size cannot be determined.
 */
[[nodiscard]] auto analyze_archive(std::string const& archive_path, bool collect_column_stats)
        -> ArchiveStats;

/**
 * Prints an analysis as a human-readable report to stdout.
 *
 * @param stats
 */
auto print_stats_as_text(ArchiveStats const& stats) -> void;

/**
 * Converts an analysis into a JSON object.
 *
 * @param stats
 * @return The JSON representation of `stats`.
 */
[[nodiscard]] auto stats_to_json(ArchiveStats const& stats) -> nlohmann::json;
}  // namespace clp_s::archive_analyzer

#endif  // CLP_S_ARCHIVE_ANALYZER_ARCHIVE_ANALYZER_HPP
