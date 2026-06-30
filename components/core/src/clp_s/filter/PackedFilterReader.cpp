#include <clp_s/filter/PackedFilterReader.hpp>

#include <cstddef>
#include <exception>
#include <utility>
#include <vector>

#include <msgpack.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <clp/ErrorCode.hpp>
#include <clp/ReaderInterface.hpp>
#include <clp_s/filter/ErrorCode.hpp>
#include <clp_s/filter/IndexDefs.hpp>
#include <clp_s/filter/PackedFilterDefs.hpp>

namespace clp_s::filter {
auto PackedFilterReader::create(clp::ReaderInterface& reader)
        -> ystdlib::error_handling::Result<PackedFilterReader> {
    PackedFilterHeader header{};
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    if (clp::ErrorCode_Success
        != reader.try_read_exact_length(reinterpret_cast<char*>(&header), sizeof(header)))
    {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::Truncated};
    }
    if (cPackedFilterMagicNumber != header.magic_number) {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::InvalidMagicNumber};
    }
    if (cPackedFilterFormatMajorVersion != header.format_major_version) {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::UnsupportedFormatVersion};
    }

    std::vector<char> metadata_buffer(header.metadata_section_size);
    if (header.metadata_section_size > 0
        && clp::ErrorCode_Success
                   != reader.try_read_exact_length(
                           metadata_buffer.data(),
                           header.metadata_section_size
                   ))
    {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::Truncated};
    }

    IndexMetadata metadata;
    try {
        auto object_handle{msgpack::unpack(metadata_buffer.data(), metadata_buffer.size())};
        object_handle.get().convert(metadata);
    } catch (std::exception const&) {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::CorruptMetadata};
    }
    if (metadata.archive_ids.size() != header.num_archives
        || metadata.index_ids.size() != header.num_indexes
        || metadata.index_sizes.size() != header.num_indexes
        || metadata.index_impl_versions.size() != header.num_indexes
        || metadata.index_blob_metadata_sizes.size() != header.num_indexes)
    {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::CorruptMetadata};
    }

    std::vector<IndexDescriptor> index_descriptors;
    index_descriptors.reserve(header.num_indexes);
    for (size_t index_idx{0}; index_idx < header.num_indexes; ++index_idx) {
        index_descriptors.push_back(IndexDescriptor{
                .index_id = metadata.index_ids[index_idx],
                .impl_version = metadata.index_impl_versions[index_idx],
                .blob_metadata_size = metadata.index_blob_metadata_sizes[index_idx],
                .region_size = metadata.index_sizes[index_idx]
        });
    }

    auto const archive_version{make_index_version(
            header.archive_major_version,
            header.archive_minor_version,
            header.archive_patch_version
    )};
    return PackedFilterReader{
            archive_version,
            std::move(metadata.archive_ids),
            std::move(index_descriptors)
    };
}
}  // namespace clp_s::filter
