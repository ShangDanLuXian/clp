#include <clp_s/filter/IndexRegistry.hpp>

#include <algorithm>
#include <cstddef>
#include <exception>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <msgpack.hpp>
#include <nlohmann/json.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <clp/ErrorCode.hpp>
#include <clp/ReaderInterface.hpp>
#include <clp_s/filter/ErrorCode.hpp>
#include <clp_s/filter/IndexBuilder.hpp>
#include <clp_s/filter/IndexBuilderSpecification.hpp>
#include <clp_s/filter/IndexDefs.hpp>
#include <clp_s/filter/IndexRunner.hpp>
#include <clp_s/filter/PackedFilterBuilder.hpp>
#include <clp_s/filter/PackedFilterDefs.hpp>
#include <clp_s/filter/PackedFilterReader.hpp>
#include <clp_s/filter/PackedFilterRunner.hpp>
#include <clp_s/filter/PackedFilterSpecification.hpp>

namespace clp_s::filter {
namespace {
/**
 * Reads a single msgpack `IndexBlobMetadata` object from `reader`, one byte at a time so the read
 * stops exactly at the object's end without consuming any of the index's blob bytes that follow it.
 * Leaves `reader` positioned at the start of the index's concatenated per-archive sub-blobs.
 * @param reader
 * @param metadata Returned metadata.
 * @return Whether the metadata was read and deserialized successfully.
 */
[[nodiscard]] auto
read_index_blob_metadata(clp::ReaderInterface& reader, IndexBlobMetadata& metadata) -> bool {
    msgpack::unpacker unpacker;
    msgpack::object_handle object_handle;
    while (false == unpacker.next(object_handle)) {
        unpacker.reserve_buffer(1);
        size_t num_bytes_read{0};
        if (clp::ErrorCode_Success != reader.try_read(unpacker.buffer(), 1, num_bytes_read)
            || 0 == num_bytes_read)
        {
            return false;
        }
        unpacker.buffer_consumed(num_bytes_read);
    }
    try {
        object_handle.get().convert(metadata);
    } catch (std::exception const&) {
        return false;
    }
    return true;
}
}  // namespace

auto IndexRegistry::register_index(
        std::string name,
        index_id_t index_id,
        IndexRunnerFactory runner_factory,
        std::vector<IndexBuilderSpecification> builder_specs
) -> ystdlib::error_handling::Result<void> {
    if (m_index_id_by_name.contains(name)) {
        return IndexErrorCode{IndexErrorCodeEnum::DuplicateIndexName};
    }
    if (m_indexes_by_id.contains(index_id)) {
        return IndexErrorCode{IndexErrorCodeEnum::DuplicateIndexId};
    }
    m_indexes_by_id.try_emplace(index_id, runner_factory, std::move(builder_specs));
    m_index_id_by_name.try_emplace(std::move(name), index_id);
    return ystdlib::error_handling::success();
}

auto IndexRegistry::list_supported_indexes(archive_version_t archive_version) const
        -> std::vector<SupportedIndex> {
    std::vector<SupportedIndex> supported_indexes;
    for (auto const& [name, index_id] : m_index_id_by_name) {
        auto const selected_result{select_builder_spec(name, archive_version)};
        if (selected_result.has_error()) {
            continue;
        }
        auto const* const spec{selected_result.value().spec};
        supported_indexes.push_back(SupportedIndex{
                name,
                index_id,
                spec->get_index_version(),
                spec->get_archive_section_bitmap()
        });
    }
    std::ranges::sort(supported_indexes, {}, &SupportedIndex::index_id);
    return supported_indexes;
}

auto IndexRegistry::create_writer(
        std::string_view name,
        nlohmann::json const& config,
        PackedFilterSpecification const& packed_filter_spec
) -> ystdlib::error_handling::Result<std::unique_ptr<IndexBuilder>> {
    auto const selected{YSTDLIB_ERROR_HANDLING_TRYX(
            select_builder_spec(name, packed_filter_spec.get_archive_version())
    )};
    return selected.spec->create_builder(config, packed_filter_spec);
}

auto IndexRegistry::create_packed_filter_builder(
        std::vector<std::string> archive_ids,
        archive_version_t archive_version,
        std::vector<PackedFilterIndexRequest> const& index_requests
) -> ystdlib::error_handling::Result<PackedFilterBuilder> {
    PackedFilterSpecification const packed_filter_spec{archive_ids.size(), archive_version};
    std::vector<PackedFilterBuilder::ActiveIndex> active_indexes;
    active_indexes.reserve(index_requests.size());
    for (auto const& request : index_requests) {
        auto const selected{
                YSTDLIB_ERROR_HANDLING_TRYX(select_builder_spec(request.name, archive_version))
        };
        auto builder{YSTDLIB_ERROR_HANDLING_TRYX(
                selected.spec->create_builder(request.config, packed_filter_spec)
        )};
        active_indexes.push_back(PackedFilterBuilder::ActiveIndex{
                selected.index_id,
                selected.spec->get_index_version(),
                std::move(builder)
        });
    }
    return PackedFilterBuilder{std::move(archive_ids), archive_version, std::move(active_indexes)};
}

auto IndexRegistry::create_reader(
        index_id_t index_id,
        index_version_t index_version,
        size_t num_archives,
        clp::ReaderInterface& reader
) -> ystdlib::error_handling::Result<std::unique_ptr<IndexRunner>> {
    auto const index_it{m_indexes_by_id.find(index_id)};
    if (m_indexes_by_id.cend() == index_it) {
        return IndexErrorCode{IndexErrorCodeEnum::UnknownIndexId};
    }
    return index_it->second.runner_factory(index_version, num_archives, reader);
}

auto IndexRegistry::create_packed_filter_runner(clp::ReaderInterface& reader)
        -> ystdlib::error_handling::Result<PackedFilterRunner> {
    auto const pack_reader{YSTDLIB_ERROR_HANDLING_TRYX(PackedFilterReader::create(reader))};
    auto const num_archives{pack_reader.get_num_archives()};
    auto archive_ids{pack_reader.get_archive_ids()};

    // `PackedFilterReader::create` left the reader at the first index's blob; track absolute blob
    // boundaries so we can forward-seek past indexes a runner doesn't (fully) consume.
    size_t region_offset{0};
    if (clp::ErrorCode_Success != reader.try_get_pos(region_offset)) {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::Truncated};
    }

    std::vector<PackedFilterRunner::ActiveRunner> active_runners;
    std::vector<index_id_t> skipped_index_ids;
    for (auto const& descriptor : pack_reader.get_index_descriptors()) {
        auto const region_end{region_offset + descriptor.blob_size};

        // Only registered indexes are loaded. For those, read the index's `IndexBlobMetadata` off
        // the stream (which advances the reader to the blobs), then let the runner read the blobs
        // straight from the reader.
        if (m_indexes_by_id.contains(descriptor.index_id)) {
            IndexBlobMetadata blob_metadata;
            if (false == read_index_blob_metadata(reader, blob_metadata)) {
                skipped_index_ids.push_back(descriptor.index_id);
            } else {
                auto runner_result{create_reader(
                        descriptor.index_id,
                        blob_metadata.impl_version,
                        num_archives,
                        reader
                )};
                if (runner_result.has_error()) {
                    skipped_index_ids.push_back(descriptor.index_id);
                } else {
                    active_runners.push_back(PackedFilterRunner::ActiveRunner{
                            descriptor.index_id,
                            std::move(runner_result.value())
                    });
                }
            }
        } else {
            skipped_index_ids.push_back(descriptor.index_id);
        }

        // Realign to the next index's blob regardless of how far we advanced.
        if (clp::ErrorCode_Success != reader.try_seek_from_begin(region_end)) {
            return PackedFilterErrorCode{PackedFilterErrorCodeEnum::Truncated};
        }
        region_offset = region_end;
    }

    return PackedFilterRunner{
            std::move(archive_ids),
            std::move(active_runners),
            std::move(skipped_index_ids)
    };
}

auto IndexRegistry::select_builder_spec(
        std::string_view name,
        archive_version_t archive_version
) const -> ystdlib::error_handling::Result<SelectedBuilderSpec> {
    auto const name_it{m_index_id_by_name.find(std::string{name})};
    if (m_index_id_by_name.cend() == name_it) {
        return IndexErrorCode{IndexErrorCodeEnum::UnknownIndexName};
    }

    auto const& registered_index{m_indexes_by_id.at(name_it->second)};
    auto const spec_it{std::ranges::find_if(
            registered_index.builder_specs,
            [archive_version](IndexBuilderSpecification const& spec) {
                return spec.supports_archive_version(archive_version);
            }
    )};
    if (registered_index.builder_specs.cend() == spec_it) {
        return IndexErrorCode{IndexErrorCodeEnum::UnsupportedArchiveVersion};
    }
    return SelectedBuilderSpec{name_it->second, &(*spec_it)};
}
}  // namespace clp_s::filter
