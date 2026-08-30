#ifndef CLP_S_FILTER_PACKED_FILTER_READER_HPP
#define CLP_S_FILTER_PACKED_FILTER_READER_HPP

#include <cstddef>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <ystdlib/error_handling/Result.hpp>

#include <clp_s/filter/IndexDefs.hpp>

namespace clp_s::filter {
/**
 * A parsed view of a single index's blob within a Packed Filter. The blob views point into the
 * buffer passed to `PackedFilterReader::create`, so they remain valid only while that buffer does.
 */
struct IndexBlobView {
    index_id_t index_id{};
    index_version_t impl_version{};

    // The index's serialized data for each archive, indexed by local archive ID.
    std::vector<std::span<char const>> archive_blobs;
};

/**
 * Parses a Packed Filter pack object held in memory and exposes its metadata and index blobs.
 *
 * The reader does not own the pack buffer; the buffer passed to `create` must outlive the reader,
 * since the index blob views point into it.
 */
class PackedFilterReader {
public:
    // Factory functions
    /**
     * Parses and validates a Packed Filter pack object.
     * @param pack The serialized pack object.
     * @return A result containing the reader on success, or an error code indicating the failure:
     * - PackedFilterErrorCodeEnum::Truncated if the pack is too small for the header, metadata, or
     *   an index blob.
     * - PackedFilterErrorCodeEnum::InvalidMagicNumber if the magic number does not match.
     * - PackedFilterErrorCodeEnum::UnsupportedFormatVersion if the format major version is not
     *   supported.
     * - PackedFilterErrorCodeEnum::CorruptMetadata if the metadata fails to deserialize or is
     *   internally inconsistent.
     */
    [[nodiscard]] static auto create(std::span<char const> pack)
            -> ystdlib::error_handling::Result<PackedFilterReader>;

    // Methods
    [[nodiscard]] auto get_num_archives() const -> size_t { return m_archive_ids.size(); }

    [[nodiscard]] auto get_archive_version() const -> archive_version_t {
        return m_archive_version;
    }

    /**
     * @return The archive ID of each archive, indexed by local archive ID.
     */
    [[nodiscard]] auto get_archive_ids() const -> std::vector<std::string> const& {
        return m_archive_ids;
    }

    /**
     * @return A parsed view of each index's blob.
     */
    [[nodiscard]] auto get_index_blobs() const -> std::vector<IndexBlobView> const& {
        return m_index_blobs;
    }

private:
    // Constructor
    PackedFilterReader(
            archive_version_t archive_version,
            std::vector<std::string> archive_ids,
            std::vector<IndexBlobView> index_blobs
    )
            : m_archive_version{archive_version},
              m_archive_ids{std::move(archive_ids)},
              m_index_blobs{std::move(index_blobs)} {}

    // Variables
    archive_version_t m_archive_version;
    std::vector<std::string> m_archive_ids;
    std::vector<IndexBlobView> m_index_blobs;
};
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_PACKED_FILTER_READER_HPP
