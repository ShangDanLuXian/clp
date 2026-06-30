#include <clp_s/filter/PackedFilterWriter.hpp>

#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <tuple>
#include <utility>
#include <vector>

#include <msgpack.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <clp_s/filter/ErrorCode.hpp>
#include <clp_s/filter/IndexDefs.hpp>
#include <clp_s/filter/PackedFilterDefs.hpp>

namespace clp_s::filter {
namespace {
/**
 * Appends the bytes viewed by `bytes` to `output`.
 */
auto append_bytes(std::span<char const> bytes, std::vector<char>& output) -> void {
    output.insert(output.cend(), bytes.begin(), bytes.end());
}
}  // namespace

auto PackedFilterWriter::add_index(
        index_id_t index_id,
        index_version_t impl_version,
        std::vector<std::span<char const>> const& archive_blobs
) -> ystdlib::error_handling::Result<void> {
    if (archive_blobs.size() != m_archive_ids.size()) {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::ArchiveCountMismatch};
    }

    IndexBlobMetadata blob_metadata;
    blob_metadata.impl_version = impl_version;
    blob_metadata.archive_index_sizes.reserve(archive_blobs.size());
    size_t total_blob_size{0};
    for (auto const& blob : archive_blobs) {
        if (blob.size() > std::numeric_limits<uint32_t>::max()) {
            return PackedFilterErrorCode{PackedFilterErrorCodeEnum::SerializedSizeOutOfRange};
        }
        blob_metadata.archive_index_sizes.push_back(static_cast<uint32_t>(blob.size()));
        total_blob_size += blob.size();
    }

    msgpack::sbuffer metadata_buffer;
    msgpack::pack(metadata_buffer, blob_metadata);

    std::vector<char> index_blob;
    index_blob.reserve(metadata_buffer.size() + total_blob_size);
    append_bytes(std::span<char const>{metadata_buffer.data(), metadata_buffer.size()}, index_blob);
    for (auto const& blob : archive_blobs) {
        append_bytes(blob, index_blob);
    }

    m_index_ids.push_back(index_id);
    m_index_blobs.push_back(std::move(index_blob));
    return ystdlib::error_handling::success();
}

auto PackedFilterWriter::serialize() const -> ystdlib::error_handling::Result<std::vector<char>> {
    if (m_archive_ids.size() > std::numeric_limits<uint32_t>::max()
        || m_index_ids.size() > std::numeric_limits<uint16_t>::max())
    {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::SerializedSizeOutOfRange};
    }

    IndexMetadata metadata;
    metadata.archive_ids = m_archive_ids;
    metadata.index_ids = m_index_ids;
    metadata.index_sizes.reserve(m_index_blobs.size());
    size_t total_index_blobs_size{0};
    for (auto const& index_blob : m_index_blobs) {
        if (index_blob.size() > std::numeric_limits<uint32_t>::max()) {
            return PackedFilterErrorCode{PackedFilterErrorCodeEnum::SerializedSizeOutOfRange};
        }
        metadata.index_sizes.push_back(static_cast<uint32_t>(index_blob.size()));
        total_index_blobs_size += index_blob.size();
    }

    msgpack::sbuffer metadata_buffer;
    msgpack::pack(metadata_buffer, metadata);
    if (metadata_buffer.size() > std::numeric_limits<uint32_t>::max()) {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::SerializedSizeOutOfRange};
    }

    auto const [archive_major, archive_minor, archive_patch]{
            decompose_index_version(m_archive_version)
    };
    PackedFilterHeader header{};
    header.magic_number = cPackedFilterMagicNumber;
    header.format_patch_version = cPackedFilterFormatPatchVersion;
    header.format_minor_version = cPackedFilterFormatMinorVersion;
    header.format_major_version = cPackedFilterFormatMajorVersion;
    header.archive_patch_version = archive_patch;
    header.archive_minor_version = archive_minor;
    header.archive_major_version = archive_major;
    header.num_archives = static_cast<uint32_t>(m_archive_ids.size());
    header.num_indexes = static_cast<uint16_t>(m_index_ids.size());
    header.archive_id_encoding_type = static_cast<uint16_t>(ArchiveIdEncodingType::UuidString);
    header.metadata_section_size = static_cast<uint32_t>(metadata_buffer.size());
    header.pack_size = sizeof(PackedFilterHeader) + metadata_buffer.size() + total_index_blobs_size;

    std::vector<char> output;
    output.reserve(sizeof(PackedFilterHeader) + metadata_buffer.size() + total_index_blobs_size);
    output.resize(sizeof(PackedFilterHeader));
    std::memcpy(output.data(), &header, sizeof(PackedFilterHeader));
    append_bytes(std::span<char const>{metadata_buffer.data(), metadata_buffer.size()}, output);
    for (auto const& index_blob : m_index_blobs) {
        append_bytes(std::span<char const>{index_blob}, output);
    }
    return output;
}
}  // namespace clp_s::filter
