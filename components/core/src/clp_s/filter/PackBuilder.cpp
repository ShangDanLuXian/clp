#include "PackBuilder.hpp"

#include <cstddef>
#include <exception>
#include <filesystem>
#include <fstream>
#include <ios>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/random_generator.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <clp_s/ArchiveReader.hpp>
#include <clp_s/filter/BloomFilterIndexBuilder.hpp>
#include <clp_s/filter/IndexDefs.hpp>
#include <clp_s/filter/PackedFilterSpecification.hpp>
#include <clp_s/filter/PackedFilterWriter.hpp>
#include <clp_s/InputConfig.hpp>

namespace clp_s::filter {
namespace {
// The Bloom filter index identity, matching `register_indexes`.
constexpr index_id_t cBloomFilterIndexId{cOfficialOpenSourceIndexIdBegin};
constexpr index_version_t cBloomFilterIndexVersion{make_index_version(1, 0, 0)};

/**
 * Builds the Bloom filter blob for a single archive.
 * @param archive_path
 * @param archive_version
 * @param network_auth
 * @param blob Returned serialized Bloom filter for the archive.
 * @return Whether the blob was built successfully.
 */
[[nodiscard]] auto build_archive_blob(
        Path const& archive_path,
        archive_version_t archive_version,
        NetworkAuthOption const& network_auth,
        std::vector<char>& blob
) -> bool {
    ArchiveReader archive_reader;
    archive_reader.open(archive_path, network_auth);
    // The Bloom filter indexes only the variable dictionary, so read just that (as the search path
    // does) rather than `read_dictionaries_and_metadata`, which also reads per-table schema metadata
    // the index doesn't need.
    archive_reader.read_variable_dictionary();

    auto builder_result{BloomFilterIndexBuilder::create(
            nlohmann::json::object(),
            PackedFilterSpecification{1, archive_version}
    )};
    if (builder_result.has_error()) {
        SPDLOG_ERROR("Failed to create the Bloom filter builder for '{}'.", archive_path.path);
        archive_reader.close();
        return false;
    }
    auto& builder{builder_result.value()};
    auto const add_result{builder->add_archive(0, archive_reader)};
    archive_reader.close();
    if (add_result.has_error()) {
        SPDLOG_ERROR("Failed to index archive '{}'.", archive_path.path);
        return false;
    }

    auto const archive_blobs{builder->get_archive_blobs()};
    if (1 != archive_blobs.size()) {
        SPDLOG_ERROR(
                "Bloom filter builder produced {} blobs for one archive.",
                archive_blobs.size()
        );
        return false;
    }
    blob.assign(archive_blobs[0].begin(), archive_blobs[0].end());
    return true;
}
}  // namespace

auto build_pack_from_archives(
        std::span<Path const> archive_paths,
        std::string const& output_dir,
        NetworkAuthOption const& network_auth
) -> bool {
    if (archive_paths.empty()) {
        SPDLOG_ERROR("Refusing to build a pack from an empty batch of archives.");
        return false;
    }

    try {
        // Every archive in a pack shares one archive version; read it from the first archive.
        archive_version_t archive_version{};
        {
            ArchiveReader archive_reader;
            archive_reader.open(archive_paths.front(), network_auth);
            archive_version = archive_reader.get_header().version;
            archive_reader.close();
        }

        std::vector<std::string> archive_ids;
        std::vector<std::vector<char>> blobs;
        archive_ids.reserve(archive_paths.size());
        blobs.reserve(archive_paths.size());
        for (auto const& archive_path : archive_paths) {
            std::string archive_id;
            if (false == get_archive_id_from_path(archive_path, archive_id)) {
                SPDLOG_ERROR("Failed to determine the archive id for '{}'.", archive_path.path);
                return false;
            }
            std::vector<char> blob;
            if (false == build_archive_blob(archive_path, archive_version, network_auth, blob)) {
                return false;
            }
            archive_ids.push_back(std::move(archive_id));
            blobs.push_back(std::move(blob));
        }

        std::vector<std::span<char const>> blob_spans;
        blob_spans.reserve(blobs.size());
        for (auto const& blob : blobs) {
            blob_spans.emplace_back(blob.data(), blob.size());
        }

        PackedFilterWriter writer{archive_ids, archive_version};
        if (writer.add_index(cBloomFilterIndexId, cBloomFilterIndexVersion, blob_spans).has_error())
        {
            SPDLOG_ERROR("Failed to add the Bloom filter index to the pack.");
            return false;
        }
        auto pack_result{writer.serialize()};
        if (pack_result.has_error()) {
            SPDLOG_ERROR("Failed to serialize the pack.");
            return false;
        }
        auto const& pack{pack_result.value()};

        boost::uuids::random_generator boost_uuid_generator;
        auto const pack_path{std::filesystem::path{output_dir} / (boost::uuids::to_string(boost_uuid_generator()) + ".pack")};
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
                "Wrote pack with {} archive(s) ({} bytes) to '{}'.",
                archive_ids.size(),
                pack.size(),
                pack_path.string()
        );
    } catch (std::exception const& e) {
        SPDLOG_ERROR("Failed to build pack: {}", e.what());
        return false;
    }

    return true;
}
}  // namespace clp_s::filter
