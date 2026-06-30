#ifndef CLP_S_FILTER_PACKED_FILTER_DEFS_HPP
#define CLP_S_FILTER_PACKED_FILTER_DEFS_HPP

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include <msgpack.hpp>

#include <clp_s/filter/IndexDefs.hpp>

namespace clp_s::filter {
// Four-byte magic number identifying a Packed Filter pack object.
// NOTE: Placeholder value, subject to change.
constexpr std::array<uint8_t, 4> cPackedFilterMagicNumber{0x43, 0x4C, 0x50, 0x46};

// Current Packed Filter format version, as a semantic version.
constexpr uint8_t cPackedFilterFormatMajorVersion{1};
constexpr uint8_t cPackedFilterFormatMinorVersion{0};
constexpr uint16_t cPackedFilterFormatPatchVersion{0};

/**
 * Encoding of the fixed-width archive ID strings stored in a Packed Filter.
 */
enum class ArchiveIdEncodingType : uint16_t {
    // 36-character canonical UUID string (e.g. "550e8400-e29b-41d4-a716-446655440000").
    UuidString = 0,
};

/**
 * Fixed 64-byte header at the start of a Packed Filter pack object, serialized and deserialized as
 * raw bytes (mirroring `clp_s::ArchiveHeader`).
 *
 * The fields are ordered so that the struct's natural alignment introduces no padding, keeping it
 * exactly 64 bytes. `metadata_section_size` is the size of the metadata section that immediately
 * follows the header, so the first index blob begins at `sizeof(PackedFilterHeader) +
 * metadata_section_size`. `pack_size` is the total size of the whole pack object (header included),
 * letting a reader size its buffer from the header alone; it is zero in packs written before the
 * field existed.
 */
struct PackedFilterHeader {
    std::array<uint8_t, 4> magic_number{};
    uint16_t format_patch_version{};
    uint8_t format_minor_version{};
    uint8_t format_major_version{};
    uint16_t archive_patch_version{};
    uint8_t archive_minor_version{};
    uint8_t archive_major_version{};
    uint32_t num_archives{};
    uint16_t num_indexes{};
    uint16_t archive_id_encoding_type{};
    uint32_t metadata_section_size{};
    uint64_t pack_size{};
    std::array<uint64_t, 4> reserved_padding{};
};

static_assert(64 == sizeof(PackedFilterHeader));

/**
 * Metadata section of a Packed Filter, serialized with msgpack and located immediately after the
 * `PackedFilterHeader`. It records the mapping from local archive ID to archive ID, and, for each
 * index (indexed positionally), its total blob size, Index ID, implementation version, and the size
 * of its `IndexBlobMetadata`.
 *
 * Recording each index's `IndexBlobMetadata` size here lets a reader read that (variable-length)
 * metadata in a single sized read rather than parsing it incrementally from the stream.
 */
struct IndexMetadata {
    // The archive ID of each archive, indexed by local archive ID. The encoding is described by the
    // header's `archive_id_encoding_type`.
    std::vector<std::string> archive_ids;

    // The total serialized size, in bytes, of each index's region (its `IndexBlobMetadata` plus its
    // concatenated per-archive sub-blobs).
    std::vector<uint32_t> index_sizes;

    // The Index ID of each index.
    std::vector<uint16_t> index_ids;

    // The implementation version of the index that produced each index's blob.
    std::vector<uint32_t> index_impl_versions;

    // The serialized size, in bytes, of each index's `IndexBlobMetadata`.
    std::vector<uint32_t> index_blob_metadata_sizes;

    MSGPACK_DEFINE_MAP(
            archive_ids,
            index_sizes,
            index_ids,
            index_impl_versions,
            index_blob_metadata_sizes
    );
};

/**
 * Metadata for a single index's blob within a Packed Filter, serialized with msgpack and located at
 * the start of the index's region. Its serialized size is recorded in `IndexMetadata`. The
 * per-archive sub-blobs are concatenated immediately after it, sized by `archive_index_sizes`.
 */
struct IndexBlobMetadata {
    // The serialized size, in bytes, of each archive's sub-blob, indexed by local archive ID.
    std::vector<uint32_t> archive_index_sizes;

    MSGPACK_DEFINE_MAP(archive_index_sizes);
};
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_PACKED_FILTER_DEFS_HPP
