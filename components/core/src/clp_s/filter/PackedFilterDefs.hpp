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
constexpr uint8_t cPackedFilterFormatMajorVersion{0};
constexpr uint8_t cPackedFilterFormatMinorVersion{1};
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
 * `PackedFilterHeader`. It records the mapping from local archive ID to archive ID, and the size,
 * Index ID, and implementation version of each index (indexed positionally).
 *
 * Each index's blob, located after this section, is just its per-archive sub-blobs concatenated in
 * local-archive-ID order; the sub-blobs are self-delimiting, so no per-archive sizes are stored.
 */
struct IndexMetadata {
    // The archive ID of each archive, indexed by local archive ID. The encoding is described by the
    // header's `archive_id_encoding_type`.
    std::vector<std::string> archive_ids;

    // The serialized size, in bytes, of each index's blob.
    std::vector<uint32_t> index_sizes;

    // The Index ID of each index.
    std::vector<uint16_t> index_ids;

    // The implementation version of the index that produced each index's blob.
    std::vector<uint32_t> index_impl_versions;

    MSGPACK_DEFINE_MAP(archive_ids, index_sizes, index_ids, index_impl_versions);
};
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_PACKED_FILTER_DEFS_HPP
