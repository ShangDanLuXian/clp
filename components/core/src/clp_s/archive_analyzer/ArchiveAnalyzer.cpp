#include "ArchiveAnalyzer.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <fmt/format.h>
#include <msgpack.hpp>
#include <nlohmann/json.hpp>

#include <clp_s/archive_analyzer/MptFingerprint.hpp>
#include <clp_s/ArchiveReader.hpp>
#include <clp_s/archive_constants.hpp>
#include <clp_s/ColumnReader.hpp>
#include <clp_s/ErrorCode.hpp>
#include <clp_s/FileReader.hpp>
#include <clp_s/InputConfig.hpp>
#include <clp_s/SchemaReader.hpp>
#include <clp_s/SchemaTree.hpp>
#include <clp_s/SingleFileArchiveDefs.hpp>
#include <clp_s/TraceableException.hpp>
#include <clp_s/ZstdDecompressor.hpp>

namespace clp_s::archive_analyzer {
namespace {
class OperationFailed : public TraceableException {
public:
    // Constructors
    OperationFailed(ErrorCode error_code, char const* const filename, int line_number)
            : TraceableException(error_code, filename, line_number) {}
};

// The analyzer's version. Incremented whenever the analyzer's output or behaviour changes.
constexpr std::string_view cAnalyzerVersion{"0.1.0"};

// The name reported for the header and metadata section of a single-file archive.
constexpr std::string_view cHeaderAndMetadataComponentName{"header+metadata"};

constexpr size_t cDecompressorReadBufferCapacity{64 * 1024};

/**
 * @param type
 * @return A human-readable name for `type`.
 */
[[nodiscard]] auto node_type_to_string(NodeType type) -> std::string_view {
    switch (type) {
        case NodeType::Integer:
            return "Integer";
        case NodeType::Float:
            return "Float";
        case NodeType::ClpString:
            return "ClpString";
        case NodeType::VarString:
            return "VarString";
        case NodeType::Boolean:
            return "Boolean";
        case NodeType::Object:
            return "Object";
        case NodeType::UnstructuredArray:
            return "UnstructuredArray";
        case NodeType::NullValue:
            return "NullValue";
        case NodeType::DeprecatedDateString:
            return "DeprecatedDateString";
        case NodeType::StructuredArray:
            return "StructuredArray";
        case NodeType::Metadata:
            return "Metadata";
        case NodeType::DeltaInteger:
            return "DeltaInteger";
        case NodeType::FormattedFloat:
            return "FormattedFloat";
        case NodeType::DictionaryFloat:
            return "DictionaryFloat";
        case NodeType::Timestamp:
            return "Timestamp";
        default:
            return "Unknown";
    }
}

/**
 * @param size The size in bytes.
 * @return `size` formatted as a human-readable string (e.g. "1.2 MiB").
 */
[[nodiscard]] auto format_size(uint64_t size) -> std::string {
    constexpr double cUnitStep{1024.0};
    constexpr std::array<std::string_view, 5> cUnits{"B", "KiB", "MiB", "GiB", "TiB"};
    auto scaled_size{static_cast<double>(size)};
    size_t unit_idx{0};
    while (scaled_size >= cUnitStep && unit_idx < cUnits.size() - 1) {
        scaled_size /= cUnitStep;
        ++unit_idx;
    }
    if (0 == unit_idx) {
        return fmt::format("{} B", size);
    }
    return fmt::format("{:.1f} {}", scaled_size, cUnits[unit_idx]);
}

/**
 * Builds the dotted key path for a column from its schema tree node, e.g. "a.b.c". Columns under
 * the metadata subtree (e.g. the log-event-index column) are prefixed with "[metadata] ".
 *
 * @param schema_tree
 * @param node_id
 * @return The column's path.
 */
[[nodiscard]] auto build_column_path(SchemaTree const& schema_tree, int32_t node_id)
        -> std::string {
    std::vector<std::string_view> key_names;
    auto subtree_type{NodeType::Unknown};
    for (auto id{node_id}; constants::cRootNodeId != id;) {
        auto const& node{schema_tree.get_node(id)};
        if (false == node.get_key_name().empty()) {
            key_names.emplace_back(node.get_key_name());
        }
        subtree_type = node.get_type();
        id = node.get_parent_id();
    }

    std::string path;
    if (NodeType::Metadata == subtree_type) {
        path += "[metadata] ";
    }
    for (auto it{key_names.rbegin()}; it != key_names.rend(); ++it) {
        if (false == path.empty() && '.' != path.back() && ' ' != path.back()) {
            path += '.';
        }
        path += *it;
    }
    if (path.empty()) {
        path = "<root>";
    }
    return path;
}

/**
 * A `FilterClass` that visits every message of every table without marshalling any records, and
 * accumulates per-column statistics. `filter` always returns false, so a single call to
 * `SchemaReader::get_next_message` drives the collector over an entire table.
 */
class ColumnStatsCollector : public FilterClass {
public:
    // Methods implementing `FilterClass`
    auto init(SchemaReader* /* reader */, std::vector<BaseColumnReader*> const& column_readers)
            -> void override {
        m_current_columns = &column_readers;
    }

    auto filter(uint64_t cur_message) -> bool override {
        for (auto* column_reader : *m_current_columns) {
            auto& accumulator{m_column_accumulators[column_reader->get_id()]};
            if (NodeType::Unknown == accumulator.type) {
                accumulator.type = column_reader->get_type();
            }
            m_buffer.clear();
            column_reader->extract_string_value_into_buffer(cur_message, m_buffer);
            ++accumulator.num_values;
            accumulator.distinct_value_hashes.insert(
                    std::hash<std::string_view>{}(std::string_view{m_buffer})
            );
        }
        return false;
    }

    // Methods
    /**
     * @param schema_tree The schema tree of the archive the statistics were collected from.
     * @return The accumulated per-column statistics, ordered by column path.
     */
    [[nodiscard]] auto get_column_stats(SchemaTree const& schema_tree) const
            -> std::vector<ColumnStats> {
        std::vector<ColumnStats> columns;
        columns.reserve(m_column_accumulators.size());
        for (auto const& [column_id, accumulator] : m_column_accumulators) {
            columns.emplace_back(ColumnStats{
                    build_column_path(schema_tree, column_id),
                    accumulator.type,
                    accumulator.num_values,
                    accumulator.distinct_value_hashes.size()
            });
        }
        std::ranges::sort(columns, [](auto const& lhs, auto const& rhs) {
            return lhs.path < rhs.path;
        });
        return columns;
    }

private:
    // Types
    struct ColumnAccumulator {
        NodeType type{NodeType::Unknown};
        uint64_t num_values{};
        // Values are counted by their 64-bit hash, so reported distinct counts are exact up to
        // hash collisions.
        absl::flat_hash_set<uint64_t> distinct_value_hashes;
    };

    // Variables
    std::vector<BaseColumnReader*> const* m_current_columns{};
    std::string m_buffer;
    absl::flat_hash_map<int32_t, ColumnAccumulator> m_column_accumulators;
};

/**
 * Collects the components of an archive directory by taking the size of each file in the
 * directory.
 *
 * @param archive_dir
 * @return The components, ordered by descending size.
 */
[[nodiscard]] auto collect_directory_components(std::filesystem::path const& archive_dir)
        -> std::vector<ComponentStats> {
    std::vector<ComponentStats> components;
    for (auto const& entry : std::filesystem::directory_iterator{archive_dir}) {
        if (false == entry.is_regular_file()) {
            continue;
        }
        components.emplace_back(
                ComponentStats{entry.path().filename().string(), entry.file_size()}
        );
    }
    std::ranges::sort(components, [](auto const& lhs, auto const& rhs) {
        return lhs.size > rhs.size;
    });
    return components;
}

/**
 * Collects the components of a single-file archive by reading the archive's header and the
 * `ArchiveFileInfo` packet from its metadata section.
 *
 * @param archive_path
 * @param file_size The total size of the single-file archive.
 * @return The components, ordered by descending size.
 * @throws OperationFailed if the header or metadata section cannot be read.
 */
[[nodiscard]] auto
collect_single_file_components(std::string const& archive_path, uint64_t file_size)
        -> std::vector<ComponentStats> {
    FileReader reader;
    reader.open(archive_path);

    ArchiveHeader header{};
    if (ErrorCodeSuccess
        != reader.try_read_exact_length(reinterpret_cast<char*>(&header), sizeof(header)))
    {
        throw OperationFailed(ErrorCodeErrno, __FILENAME__, __LINE__);
    }
    if (0
        != std::memcmp(
                header.magic_number,
                cStructuredSFAMagicNumber.data(),
                cStructuredSFAMagicNumber.size()
        ))
    {
        throw OperationFailed(ErrorCodeMetadataCorrupted, __FILENAME__, __LINE__);
    }

    ZstdDecompressor decompressor;
    decompressor.open(reader, cDecompressorReadBufferCapacity);

    ArchiveFileInfoPacket file_info_packet;
    uint8_t num_packets{};
    if (ErrorCodeSuccess != decompressor.try_read_numeric_value(num_packets)) {
        throw OperationFailed(ErrorCodeMetadataCorrupted, __FILENAME__, __LINE__);
    }
    std::string packet_buffer;
    for (uint8_t packet_idx{0}; packet_idx < num_packets; ++packet_idx) {
        uint8_t packet_type{};
        uint32_t packet_size{};
        if (ErrorCodeSuccess != decompressor.try_read_numeric_value(packet_type)
            || ErrorCodeSuccess != decompressor.try_read_numeric_value(packet_size))
        {
            throw OperationFailed(ErrorCodeMetadataCorrupted, __FILENAME__, __LINE__);
        }
        if (ErrorCodeSuccess != decompressor.try_read_string(packet_size, packet_buffer)) {
            throw OperationFailed(ErrorCodeMetadataCorrupted, __FILENAME__, __LINE__);
        }
        if (static_cast<uint8_t>(ArchiveMetadataPacketType::ArchiveFileInfo) == packet_type) {
            auto const handle{msgpack::unpack(packet_buffer.data(), packet_buffer.size())};
            file_info_packet = handle.get().as<ArchiveFileInfoPacket>();
            break;
        }
    }
    decompressor.close();
    reader.close();

    if (file_info_packet.files.empty()) {
        throw OperationFailed(ErrorCodeMetadataCorrupted, __FILENAME__, __LINE__);
    }

    // Each file's offset is relative to the start of the files region, which begins immediately
    // after the header and the metadata section. A file's size is the distance to the next file's
    // offset; the last file extends to the end of the archive.
    auto const files_region_offset{sizeof(ArchiveHeader) + header.metadata_section_size};
    auto const files_region_size{file_size - files_region_offset};
    std::vector<ComponentStats> components;
    components.emplace_back(
            ComponentStats{std::string{cHeaderAndMetadataComponentName}, files_region_offset}
    );
    auto const& files{file_info_packet.files};
    for (size_t file_idx{0}; file_idx < files.size(); ++file_idx) {
        auto const end_offset{
                (file_idx + 1 < files.size()) ? files[file_idx + 1].o : files_region_size
        };
        auto name{files[file_idx].n};
        if (false == name.empty() && '/' == name.front()) {
            name.erase(0, 1);
        }
        components.emplace_back(ComponentStats{std::move(name), end_offset - files[file_idx].o});
    }
    std::ranges::sort(components, [](auto const& lhs, auto const& rhs) {
        return lhs.size > rhs.size;
    });
    return components;
}
}  // namespace

auto get_analyzer_version() -> std::string {
#ifdef ARCHIVE_ANALYZER_GIT_DESC
    return fmt::format("{} ({})", cAnalyzerVersion, ARCHIVE_ANALYZER_GIT_DESC);
#else
    return std::string{cAnalyzerVersion};
#endif
}

auto analyze_archive(std::string const& archive_path, bool collect_column_stats) -> ArchiveStats {
    ArchiveStats stats;
    stats.path = archive_path;

    std::filesystem::path const fs_path{archive_path};
    if (std::filesystem::is_directory(fs_path)) {
        stats.components = collect_directory_components(fs_path);
        for (auto const& component : stats.components) {
            stats.total_size += component.size;
        }
    } else {
        stats.total_size = std::filesystem::file_size(fs_path);
        stats.components = collect_single_file_components(archive_path, stats.total_size);
    }

    ArchiveReader archive_reader;
    archive_reader.open(
            Path{.source{InputSource::Filesystem}, .path{archive_path}},
            NetworkAuthOption{}
    );
    archive_reader.read_dictionaries_and_metadata();

    auto const& header{archive_reader.get_header()};
    auto const [major_version, minor_version, patch_version]{
            decompose_archive_version(header.version)
    };
    stats.archive_format_version
            = fmt::format("{}.{}.{}", major_version, minor_version, patch_version);
    stats.uncompressed_size = header.uncompressed_size;

    auto const& schema_ids{archive_reader.get_schema_ids()};
    stats.num_schemas = schema_ids.size();
    for (auto const schema_id : schema_ids) {
        stats.num_records += archive_reader.get_num_messages_for_schema(schema_id);
    }

    stats.mpt = compute_mpt_fingerprint(*archive_reader.get_schema_tree());

    auto const fingerprint_dictionary
            = [](LogTypeDictionaryReader const& dictionary) -> SetFingerprint {
        std::vector<std::string_view> values;
        values.reserve(dictionary.get_entries().size());
        for (auto const& entry : dictionary.get_entries()) {
            values.emplace_back(entry.get_value());
        }
        return compute_string_set_fingerprint(values);
    };
    stats.log_type_dict = fingerprint_dictionary(*archive_reader.get_log_type_dictionary());
    stats.array_dict = fingerprint_dictionary(*archive_reader.get_array_dictionary());

    if (collect_column_stats) {
        archive_reader.open_packed_streams();
        ColumnStatsCollector collector;
        std::string unused_message;
        for (auto const schema_id : schema_ids) {
            auto& schema_reader{archive_reader.read_schema_table(schema_id, false, false)};
            schema_reader.initialize_filter(collector);
            while (schema_reader.get_next_message(unused_message, collector)) {}
        }
        stats.columns = collector.get_column_stats(*archive_reader.get_schema_tree());
    }
    archive_reader.close();

    return stats;
}

auto print_stats_as_text(ArchiveStats const& stats) -> void {
    fmt::print("Archive: {}\n", stats.path);
    fmt::print("  Format version: {}\n", stats.archive_format_version);
    fmt::print("  Size: {} ({} bytes)\n", format_size(stats.total_size), stats.total_size);
    if (0 != stats.total_size) {
        fmt::print(
                "  Uncompressed size: {} (compression ratio {:.1f}x)\n",
                format_size(stats.uncompressed_size),
                static_cast<double>(stats.uncompressed_size)
                        / static_cast<double>(stats.total_size)
        );
    }
    fmt::print("  Records: {} across {} schemas\n", stats.num_records, stats.num_schemas);
    fmt::print("  MPT: {} nodes, checksum {}\n", stats.mpt.num_nodes, stats.mpt.checksum);
    fmt::print(
            "  Log types: {} entries, checksum {}\n",
            stats.log_type_dict.num_items,
            stats.log_type_dict.checksum
    );
    fmt::print(
            "  Array types: {} entries, checksum {}\n",
            stats.array_dict.num_items,
            stats.array_dict.checksum
    );

    fmt::print("\nComponents:\n");
    fmt::print("  {:<24} {:>12} {:>8}\n", "name", "size", "%");
    for (auto const& component : stats.components) {
        auto const percentage{
                0 == stats.total_size
                        ? 0.0
                        : 100.0 * static_cast<double>(component.size)
                                  / static_cast<double>(stats.total_size)
        };
        fmt::print(
                "  {:<24} {:>12} {:>7.1f}%\n",
                component.name,
                format_size(component.size),
                percentage
        );
    }

    if (false == stats.columns.empty()) {
        fmt::print("\nColumns:\n");
        fmt::print("  {:<48} {:<20} {:>12} {:>12}\n", "path", "type", "values", "distinct");
        for (auto const& column : stats.columns) {
            fmt::print(
                    "  {:<48} {:<20} {:>12} {:>12}\n",
                    column.path,
                    node_type_to_string(column.type),
                    column.num_values,
                    column.num_distinct_values
            );
        }
    }
    fmt::print("\n");
}

auto stats_to_json(ArchiveStats const& stats) -> nlohmann::json {
    // NOTE: The explicit `json::object`/`json::array` factories are used throughout instead of
    // brace initialization, since the latter can silently produce arrays with unintended shapes.
    auto components = nlohmann::json::array();
    for (auto const& component : stats.components) {
        components.push_back(nlohmann::json::object({
                {"name", component.name},
                {"size", component.size},
                {"percentage",
                 0 == stats.total_size
                         ? 0.0
                         : 100.0 * static_cast<double>(component.size)
                                   / static_cast<double>(stats.total_size)}
        }));
    }

    auto columns = nlohmann::json::array();
    for (auto const& column : stats.columns) {
        columns.push_back(nlohmann::json::object({
                {"path", column.path},
                {"type", node_type_to_string(column.type)},
                {"num_values", column.num_values},
                {"num_distinct_values", column.num_distinct_values}
        }));
    }

    // Fingerprints are serialized as hex strings since 64-bit values don't fit losslessly in
    // JSON numbers.
    auto const fingerprints_to_json = [](std::vector<uint64_t> const& fingerprints) {
        auto fingerprints_json = nlohmann::json::array();
        for (auto const fingerprint : fingerprints) {
            fingerprints_json.push_back(fmt::format("{:016x}", fingerprint));
        }
        return fingerprints_json;
    };

    auto mpt = nlohmann::json::object({
            {"num_nodes", stats.mpt.num_nodes},
            {"checksum", stats.mpt.checksum},
            {"node_fingerprints", fingerprints_to_json(stats.mpt.node_fingerprints)}
    });
    auto const set_fingerprint_to_json = [&](SetFingerprint const& set_fingerprint) {
        return nlohmann::json::object({
                {"num_entries", set_fingerprint.num_items},
                {"checksum", set_fingerprint.checksum},
                {"entry_fingerprints", fingerprints_to_json(set_fingerprint.fingerprints)}
        });
    };

    return nlohmann::json::object({
            {"path", stats.path},
            {"archive_format_version", stats.archive_format_version},
            {"total_size", stats.total_size},
            {"uncompressed_size", stats.uncompressed_size},
            {"num_records", stats.num_records},
            {"num_schemas", stats.num_schemas},
            {"mpt", std::move(mpt)},
            {"log_type_dict", set_fingerprint_to_json(stats.log_type_dict)},
            {"array_dict", set_fingerprint_to_json(stats.array_dict)},
            {"components", std::move(components)},
            {"columns", std::move(columns)}
    });
}
}  // namespace clp_s::archive_analyzer
