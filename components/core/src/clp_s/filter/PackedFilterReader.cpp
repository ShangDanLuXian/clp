#include <clp_s/filter/PackedFilterReader.hpp>

#include <cstddef>
#include <cstring>
#include <exception>
#include <span>
#include <utility>
#include <vector>

#include <msgpack.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <clp_s/filter/ErrorCode.hpp>
#include <clp_s/filter/IndexDefs.hpp>
#include <clp_s/filter/PackedFilterDefs.hpp>

namespace clp_s::filter {
auto PackedFilterReader::create(std::span<char const> pack)
        -> ystdlib::error_handling::Result<PackedFilterReader> {
    if (pack.size() < sizeof(PackedFilterHeader)) {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::Truncated};
    }

    PackedFilterHeader header{};
    std::memcpy(&header, pack.data(), sizeof(PackedFilterHeader));
    if (cPackedFilterMagicNumber != header.magic_number) {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::InvalidMagicNumber};
    }
    if (cPackedFilterFormatMajorVersion != header.format_major_version) {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::UnsupportedFormatVersion};
    }

    size_t const metadata_offset{sizeof(PackedFilterHeader)};
    if (pack.size() - metadata_offset < header.metadata_section_size) {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::Truncated};
    }

    IndexMetadata metadata;
    try {
        auto const metadata_span{pack.subspan(metadata_offset, header.metadata_section_size)};
        auto object_handle{msgpack::unpack(metadata_span.data(), metadata_span.size())};
        object_handle.get().convert(metadata);
    } catch (std::exception const&) {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::CorruptMetadata};
    }
    if (metadata.archive_ids.size() != header.num_archives
        || metadata.index_ids.size() != header.num_indexes
        || metadata.index_sizes.size() != header.num_indexes)
    {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::CorruptMetadata};
    }

    std::vector<IndexBlobView> index_blobs;
    index_blobs.reserve(header.num_indexes);
    size_t blob_offset{metadata_offset + header.metadata_section_size};
    for (size_t index_idx{0}; index_idx < header.num_indexes; ++index_idx) {
        auto const index_size{metadata.index_sizes[index_idx]};
        if (pack.size() - blob_offset < index_size) {
            return PackedFilterErrorCode{PackedFilterErrorCodeEnum::Truncated};
        }
        auto const index_blob_span{pack.subspan(blob_offset, index_size)};
        blob_offset += index_size;

        IndexBlobMetadata blob_metadata;
        size_t archive_blobs_offset{0};
        try {
            auto object_handle{msgpack::unpack(
                    index_blob_span.data(),
                    index_blob_span.size(),
                    archive_blobs_offset
            )};
            object_handle.get().convert(blob_metadata);
        } catch (std::exception const&) {
            return PackedFilterErrorCode{PackedFilterErrorCodeEnum::CorruptMetadata};
        }
        if (blob_metadata.archive_index_sizes.size() != header.num_archives) {
            return PackedFilterErrorCode{PackedFilterErrorCodeEnum::CorruptMetadata};
        }

        std::vector<std::span<char const>> archive_blobs;
        archive_blobs.reserve(header.num_archives);
        size_t cursor{archive_blobs_offset};
        for (auto const archive_blob_size : blob_metadata.archive_index_sizes) {
            if (index_blob_span.size() - cursor < archive_blob_size) {
                return PackedFilterErrorCode{PackedFilterErrorCodeEnum::Truncated};
            }
            archive_blobs.push_back(index_blob_span.subspan(cursor, archive_blob_size));
            cursor += archive_blob_size;
        }

        index_blobs.push_back(IndexBlobView{
                .index_id = metadata.index_ids[index_idx],
                .impl_version = blob_metadata.impl_version,
                .archive_blobs = std::move(archive_blobs)
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
            std::move(index_blobs)
    };
}
}  // namespace clp_s::filter
