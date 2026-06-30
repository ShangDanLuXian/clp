#ifndef CLP_S_FILTER_PACKED_FILTER_READER_HPP
#define CLP_S_FILTER_PACKED_FILTER_READER_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <ystdlib/error_handling/Result.hpp>

#include <clp/ReaderInterface.hpp>
#include <clp_s/filter/IndexDefs.hpp>

namespace clp_s::filter {
/**
 * A description of one index in a Packed Filter, parsed from the metadata section. The index's blob
 * (its per-index `IndexBlobMetadata` followed by its concatenated per-archive sub-blobs) follows the
 * metadata section in `index_id`-positional order and is read separately, directly from the reader.
 */
struct IndexDescriptor {
    index_id_t index_id{};

    // The size, in bytes, of the index's blob (its `IndexBlobMetadata` plus concatenated sub-blobs).
    uint32_t blob_size{};
};

/**
 * Reads and validates the header and metadata section of a Packed Filter from a reader, leaving the
 * reader positioned at the start of the first index's blob.
 *
 * The reader is not retained: `create` consumes only the fixed header and the (small) metadata
 * section. The caller then drives the same reader through the index blobs that follow, each blob
 * beginning where the previous one ended, in the order given by `get_index_descriptors`. The bulk
 * blob data is never buffered here.
 */
class PackedFilterReader {
public:
    // Factory functions
    /**
     * Reads and validates a Packed Filter's header and metadata section.
     * @param reader Positioned at the start of the pack; left positioned at the first index's blob.
     * @return A result containing the reader on success, or an error code indicating the failure:
     * - PackedFilterErrorCodeEnum::Truncated if the header or metadata cannot be fully read.
     * - PackedFilterErrorCodeEnum::InvalidMagicNumber if the magic number does not match.
     * - PackedFilterErrorCodeEnum::UnsupportedFormatVersion if the format major version is not
     *   supported.
     * - PackedFilterErrorCodeEnum::CorruptMetadata if the metadata fails to deserialize or is
     *   internally inconsistent.
     */
    [[nodiscard]] static auto create(clp::ReaderInterface& reader)
            -> ystdlib::error_handling::Result<PackedFilterReader>;

    // Methods
    [[nodiscard]] auto get_num_archives() const -> std::size_t { return m_archive_ids.size(); }

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
     * @return A descriptor for each index, in the order its blob appears in the pack.
     */
    [[nodiscard]] auto get_index_descriptors() const -> std::vector<IndexDescriptor> const& {
        return m_index_descriptors;
    }

private:
    // Constructor
    PackedFilterReader(
            archive_version_t archive_version,
            std::vector<std::string> archive_ids,
            std::vector<IndexDescriptor> index_descriptors
    )
            : m_archive_version{archive_version},
              m_archive_ids{std::move(archive_ids)},
              m_index_descriptors{std::move(index_descriptors)} {}

    // Variables
    archive_version_t m_archive_version;
    std::vector<std::string> m_archive_ids;
    std::vector<IndexDescriptor> m_index_descriptors;
};
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_PACKED_FILTER_READER_HPP
