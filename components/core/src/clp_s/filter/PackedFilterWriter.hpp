#ifndef CLP_S_FILTER_PACKED_FILTER_WRITER_HPP
#define CLP_S_FILTER_PACKED_FILTER_WRITER_HPP

#include <span>
#include <string>
#include <utility>
#include <vector>

#include <ystdlib/error_handling/Result.hpp>

#include <clp_s/filter/IndexDefs.hpp>

namespace clp_s::filter {
/**
 * Assembles a Packed Filter pack object in memory from the index blobs built over a set of
 * archives.
 *
 * The archives are fixed at construction, defining the local archive ID space and the
 * local-ID-to-archive-ID mapping. Each index's per-archive blobs are added with `add_index`, and
 * `serialize` produces the full pack: the 64-byte header, the msgpack-encoded `IndexMetadata`
 * section, and each index's blob (its raw per-archive blobs concatenated in local-archive-ID
 * order).
 */
class PackedFilterWriter {
public:
    // Constructors
    PackedFilterWriter(std::vector<std::string> archive_ids, archive_version_t archive_version)
            : m_archive_ids{std::move(archive_ids)},
              m_archive_version{archive_version} {}

    // Methods
    /**
     * Serializes an index's blob and appends it to the pack.
     * @param index_id
     * @param impl_version The implementation version of the index that produced the blobs.
     * @param archive_blobs The index's serialized data for each archive, indexed by local archive
     * ID.
     * @return A void result on success, or an error code indicating the failure:
     * - PackedFilterErrorCodeEnum::ArchiveCountMismatch if `archive_blobs` does not contain exactly
     *   one blob per archive.
     * - PackedFilterErrorCodeEnum::SerializedSizeOutOfRange if an archive blob's size exceeds the
     *   format's field width.
     */
    [[nodiscard]] auto add_index(
            index_id_t index_id,
            index_version_t impl_version,
            std::vector<std::span<char const>> const& archive_blobs
    ) -> ystdlib::error_handling::Result<void>;

    /**
     * Serializes the full pack object.
     * @return A result containing the serialized pack on success, or an error code indicating the
     * failure:
     * - PackedFilterErrorCodeEnum::SerializedSizeOutOfRange if a serialized size exceeds the
     *   format's field width.
     */
    [[nodiscard]] auto serialize() const -> ystdlib::error_handling::Result<std::vector<char>>;

private:
    std::vector<std::string> m_archive_ids;
    archive_version_t m_archive_version;
    std::vector<index_id_t> m_index_ids;
    std::vector<index_version_t> m_index_impl_versions;
    std::vector<std::vector<char>> m_index_blobs;
};
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_PACKED_FILTER_WRITER_HPP
