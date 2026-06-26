#include "PackedFilterIndexer.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <ios>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include "../ArchiveReader.hpp"
#include "../filter/BloomFilterIndexBuilder.hpp"
#include "../filter/IndexDefs.hpp"
#include "../filter/PackedFilterSpecification.hpp"
#include "../filter/PackedFilterWriter.hpp"
#include "../InputConfig.hpp"

namespace clp_s::indexer {
namespace {
// The Bloom filter index identity, matching `filter::register_indexes`.
constexpr filter::index_id_t cBloomFilterIndexId{filter::cOfficialOpenSourceIndexIdBegin};
constexpr filter::index_version_t cBloomFilterIndexVersion{filter::make_index_version(1, 0, 0)};

// Conservative estimates of a pack's non-blob serialized overhead, used to keep packs under the
// size bound. They intentionally over-estimate so that real packs end up at or below the bound.
constexpr size_t cPackBaseOverhead{256};
constexpr size_t cPerArchiveOverhead{32};

struct Archive {
    Path path;
    std::string id;
};

/**
 * Builds the Bloom filter blob for a single archive.
 * @param archive
 * @param archive_version
 * @param network_auth
 * @param blob Returned serialized Bloom filter for the archive.
 * @return Whether the blob was built successfully.
 */
[[nodiscard]] auto build_archive_blob(
        Archive const& archive,
        filter::archive_version_t archive_version,
        NetworkAuthOption const& network_auth,
        std::vector<char>& blob
) -> bool {
    ArchiveReader archive_reader;
    archive_reader.open(archive.path, network_auth);
    // The Bloom filter indexes only the variable dictionary, so read just that (as the search path
    // does) rather than `read_dictionaries_and_metadata`, which also reads per-table schema metadata
    // the index doesn't need.
    archive_reader.read_variable_dictionary();

    auto builder_result{filter::BloomFilterIndexBuilder::create(
            nlohmann::json::object(),
            filter::PackedFilterSpecification{1, archive_version}
    )};
    if (builder_result.has_error()) {
        SPDLOG_ERROR("Failed to create the Bloom filter builder for '{}'.", archive.path.path);
        archive_reader.close();
        return false;
    }
    auto& builder{builder_result.value()};
    auto const add_result{builder->add_archive(0, archive_reader)};
    archive_reader.close();
    if (add_result.has_error()) {
        SPDLOG_ERROR("Failed to index archive '{}'.", archive.path.path);
        return false;
    }

    auto const archive_blobs{builder->get_archive_blobs()};
    if (1 != archive_blobs.size()) {
        SPDLOG_ERROR("Bloom filter builder produced {} blobs for one archive.", archive_blobs.size());
        return false;
    }
    blob.assign(archive_blobs[0].begin(), archive_blobs[0].end());
    return true;
}

/**
 * Assembles a pack from a group of per-archive blobs and writes it to `output_dir`/`pack_index`.pack.
 * @return Whether the pack was written successfully.
 */
[[nodiscard]] auto write_pack(
        std::filesystem::path const& output_dir,
        size_t pack_index,
        std::vector<std::string> const& archive_ids,
        std::vector<std::vector<char>> const& blobs,
        filter::archive_version_t archive_version
) -> bool {
    std::vector<std::span<char const>> blob_spans;
    blob_spans.reserve(blobs.size());
    for (auto const& blob : blobs) {
        blob_spans.emplace_back(blob.data(), blob.size());
    }

    filter::PackedFilterWriter writer{archive_ids, archive_version};
    if (writer.add_index(cBloomFilterIndexId, cBloomFilterIndexVersion, blob_spans).has_error()) {
        SPDLOG_ERROR("Failed to add the Bloom filter index to pack {}.", pack_index);
        return false;
    }
    auto pack_result{writer.serialize()};
    if (pack_result.has_error()) {
        SPDLOG_ERROR("Failed to serialize pack {}.", pack_index);
        return false;
    }
    auto const& pack{pack_result.value()};

    auto const pack_path{output_dir / (std::to_string(pack_index) + ".pack")};
    std::ofstream pack_output{pack_path, std::ios::binary | std::ios::trunc};
    if (false == pack_output.is_open()) {
        SPDLOG_ERROR("Failed to open output file '{}'.", pack_path.string());
        return false;
    }
    pack_output.write(pack.data(), static_cast<std::streamsize>(pack.size()));
    if (false == pack_output.good()) {
        SPDLOG_ERROR("Failed to write pack to '{}'.", pack_path.string());
        return false;
    }
    SPDLOG_INFO(
            "Wrote pack {} with {} archive(s) ({} bytes) to '{}'.",
            pack_index,
            archive_ids.size(),
            pack.size(),
            pack_path.string()
    );
    return true;
}
}  // namespace

auto build_packed_filter(
        Path const& input_path,
        std::string const& output_dir,
        NetworkAuthOption const& network_auth,
        size_t max_pack_size
) -> bool {
    std::vector<Path> archive_paths;
    if (false == get_input_archives_for_path(input_path, archive_paths)) {
        SPDLOG_ERROR("Failed to enumerate archives under '{}'.", input_path.path);
        return false;
    }
    if (archive_paths.empty()) {
        SPDLOG_ERROR("No archives found under '{}'.", input_path.path);
        return false;
    }

    std::vector<Archive> archives;
    archives.reserve(archive_paths.size());
    for (auto& archive_path : archive_paths) {
        std::string archive_id;
        if (false == get_archive_id_from_path(archive_path, archive_id)) {
            SPDLOG_ERROR("Failed to determine the archive id for '{}'.", archive_path.path);
            return false;
        }
        archives.emplace_back(Archive{std::move(archive_path), std::move(archive_id)});
    }
    std::sort(archives.begin(), archives.end(), [](Archive const& lhs, Archive const& rhs) {
        return lhs.id < rhs.id;
    });

    try {
        // Every archive in a pack shares one archive version; read it from the first archive.
        filter::archive_version_t archive_version{};
        {
            ArchiveReader archive_reader;
            archive_reader.open(archives.front().path, network_auth);
            archive_version = archive_reader.get_header().version;
            archive_reader.close();
        }

        std::filesystem::create_directories(output_dir);
        std::filesystem::path const output_dir_path{output_dir};

        std::vector<std::string> group_archive_ids;
        std::vector<std::vector<char>> group_blobs;
        size_t group_size{cPackBaseOverhead};
        size_t pack_index{0};

        for (auto const& archive : archives) {
            std::vector<char> blob;
            if (false == build_archive_blob(archive, archive_version, network_auth, blob)) {
                return false;
            }

            auto const contribution{blob.size() + archive.id.size() + cPerArchiveOverhead};
            if (false == group_archive_ids.empty() && group_size + contribution > max_pack_size) {
                if (false
                    == write_pack(
                            output_dir_path,
                            pack_index,
                            group_archive_ids,
                            group_blobs,
                            archive_version
                    ))
                {
                    return false;
                }
                ++pack_index;
                group_archive_ids.clear();
                group_blobs.clear();
                group_size = cPackBaseOverhead;
            }

            if (group_archive_ids.empty() && contribution + cPackBaseOverhead > max_pack_size) {
                SPDLOG_WARN(
                        "Archive '{}' alone exceeds the pack size bound; writing it as its own pack.",
                        archive.path.path
                );
            }
            group_archive_ids.push_back(archive.id);
            group_blobs.push_back(std::move(blob));
            group_size += contribution;
        }

        if (false == group_archive_ids.empty()) {
            if (false
                == write_pack(
                        output_dir_path,
                        pack_index,
                        group_archive_ids,
                        group_blobs,
                        archive_version
                ))
            {
                return false;
            }
            ++pack_index;
        }

        SPDLOG_INFO(
                "Built {} pack(s) over {} archive(s) into '{}'.",
                pack_index,
                archives.size(),
                output_dir
        );
    } catch (std::exception const& e) {
        SPDLOG_ERROR("Failed to build Packed Filters: {}", e.what());
        return false;
    }

    return true;
}
}  // namespace clp_s::indexer
