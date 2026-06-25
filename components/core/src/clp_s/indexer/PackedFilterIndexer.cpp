#include "PackedFilterIndexer.hpp"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <fstream>
#include <ios>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <ystdlib/error_handling/Result.hpp>

#include "../ArchiveReader.hpp"
#include "../filter/BloomFilterIndexBuilder.hpp"
#include "../filter/IndexBuilderSpecification.hpp"
#include "../filter/IndexDefs.hpp"
#include "../filter/IndexRegistry.hpp"
#include "../filter/IndexRunner.hpp"
#include "../InputConfig.hpp"
#include "../SingleFileArchiveDefs.hpp"

namespace clp_s::indexer {
namespace {
/**
 * Stub runner factory. This binary only builds Packed Filters; it never deserializes an index for
 * filtering, so the registered runner factory is never invoked.
 */
auto stub_runner_factory(
        filter::index_version_t /*index_version*/,
        std::vector<std::span<char const>> const& /*archive_blobs*/
) -> ystdlib::error_handling::Result<std::unique_ptr<filter::IndexRunner>> {
    return std::unique_ptr<filter::IndexRunner>{nullptr};
}

/**
 * Registers the indexes to build. Mirrors `filter::register_indexes`'s Bloom filter registration but
 * omits the read-side runner (see `stub_runner_factory`) so this build-only binary doesn't pull in
 * the filtering/AST machinery.
 */
[[nodiscard]] auto register_build_indexes(filter::IndexRegistry& registry)
        -> ystdlib::error_handling::Result<void> {
    std::vector<filter::IndexBuilderSpecification> builder_specs;
    builder_specs.emplace_back(
            filter::to_archive_section_bitmap(filter::ArchiveSection::Dictionaries),
            clp_s::cArchiveVersion,
            std::optional<filter::archive_version_t>{},
            filter::make_index_version(1, 0, 0),
            &filter::BloomFilterIndexBuilder::create
    );
    return registry.register_index(
            "bloom_filter",
            filter::cOfficialOpenSourceIndexIdBegin,
            &stub_runner_factory,
            std::move(builder_specs)
    );
}
}  // namespace

auto build_packed_filter(
        Path const& input_path,
        std::string const& output_path,
        NetworkAuthOption const& network_auth
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
    constexpr size_t cMaxArchivesPerPack{
            static_cast<size_t>(std::numeric_limits<uint16_t>::max()) + 1
    };
    if (archive_paths.size() > cMaxArchivesPerPack) {
        SPDLOG_ERROR(
                "Found {} archives, which exceeds the limit of {} per Packed Filter.",
                archive_paths.size(),
                cMaxArchivesPerPack
        );
        return false;
    }

    std::vector<std::string> archive_ids;
    archive_ids.reserve(archive_paths.size());
    for (auto const& archive_path : archive_paths) {
        std::string archive_id;
        if (false == get_archive_id_from_path(archive_path, archive_id)) {
            SPDLOG_ERROR("Failed to determine the archive id for '{}'.", archive_path.path);
            return false;
        }
        archive_ids.emplace_back(std::move(archive_id));
    }

    try {
        // Every archive in a pack shares one archive version; read it from the first archive.
        filter::archive_version_t archive_version{};
        {
            ArchiveReader archive_reader;
            archive_reader.open(archive_paths.front(), network_auth);
            archive_version = archive_reader.get_header().version;
            archive_reader.close();
        }

        filter::IndexRegistry registry;
        if (register_build_indexes(registry).has_error()) {
            SPDLOG_ERROR("Failed to register indexes.");
            return false;
        }

        std::vector<filter::PackedFilterIndexRequest> index_requests;
        index_requests.emplace_back(
                filter::PackedFilterIndexRequest{"bloom_filter", nlohmann::json::object()}
        );

        auto builder_result{
                registry.create_packed_filter_builder(archive_ids, archive_version, index_requests)
        };
        if (builder_result.has_error()) {
            SPDLOG_ERROR("Failed to create the Packed Filter builder.");
            return false;
        }
        auto& builder{builder_result.value()};

        for (size_t i{0}; i < archive_paths.size(); ++i) {
            ArchiveReader archive_reader;
            archive_reader.open(archive_paths[i], network_auth);
            archive_reader.read_dictionaries_and_metadata();
            auto const add_result{builder.add_archive(static_cast<uint16_t>(i), archive_reader)};
            archive_reader.close();
            if (add_result.has_error()) {
                SPDLOG_ERROR("Failed to index archive '{}'.", archive_paths[i].path);
                return false;
            }
        }

        auto pack_result{builder.serialize()};
        if (pack_result.has_error()) {
            SPDLOG_ERROR("Failed to serialize the Packed Filter.");
            return false;
        }
        auto const& pack{pack_result.value()};

        std::ofstream pack_output{output_path, std::ios::binary | std::ios::trunc};
        if (false == pack_output.is_open()) {
            SPDLOG_ERROR("Failed to open output file '{}'.", output_path);
            return false;
        }
        pack_output.write(pack.data(), static_cast<std::streamsize>(pack.size()));
        if (false == pack_output.good()) {
            SPDLOG_ERROR("Failed to write the Packed Filter to '{}'.", output_path);
            return false;
        }

        SPDLOG_INFO(
                "Built a Packed Filter over {} archive(s) ({} bytes) at '{}'.",
                archive_paths.size(),
                pack.size(),
                output_path
        );
    } catch (std::exception const& e) {
        SPDLOG_ERROR("Failed to build the Packed Filter: {}", e.what());
        return false;
    }

    return true;
}
}  // namespace clp_s::indexer
