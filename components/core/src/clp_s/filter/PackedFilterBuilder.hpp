#ifndef CLP_S_FILTER_PACKED_FILTER_BUILDER_HPP
#define CLP_S_FILTER_PACKED_FILTER_BUILDER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <ystdlib/error_handling/Result.hpp>

#include <clp_s/filter/IndexBuilder.hpp>
#include <clp_s/filter/IndexDefs.hpp>
#include <clp_s/filter/PackedFilterWriter.hpp>

namespace clp_s {
class ArchiveReader;
}  // namespace clp_s

namespace clp_s::filter {
/**
 * Build-path orchestrator for a Packed Filter.
 *
 * Whereas `PackedFilterWriter` only serializes already-built index blobs, this class drives the
 * indexes themselves: it feeds each registered `IndexBuilder` one archive at a time (so only a
 * single archive needs to be loaded into memory at once), then collects every builder's per-archive
 * blobs and hands them to a `PackedFilterWriter` to produce the final pack.
 *
 * Instances are created by `IndexRegistry::create_packed_filter_builder`, which resolves each
 * requested index to the builder and framework metadata captured in `ActiveIndex`.
 */
class PackedFilterBuilder {
public:
    // Types
    /**
     * A resolved index to build: its framework metadata and the `IndexBuilder` producing its blobs.
     */
    struct ActiveIndex {
        index_id_t index_id{};
        index_version_t index_version{};
        std::unique_ptr<IndexBuilder> builder;
    };

    // Constructors
    PackedFilterBuilder(
            std::vector<std::string> archive_ids,
            archive_version_t archive_version,
            std::vector<ActiveIndex> active_indexes
    )
            : m_num_archives{archive_ids.size()},
              m_writer{std::move(archive_ids), archive_version},
              m_active_indexes{std::move(active_indexes)} {}

    // Methods
    /**
     * Feeds an archive to every index builder.
     * @param local_archive_id The Packed-Filter-local ID of the archive, in the range
     * [0, num_archives).
     * @param archive_reader A reader for the archive identified by `local_archive_id`.
     * @return A void result on success, or an error code indicating the failure:
     * - PackedFilterErrorCodeEnum::LocalArchiveIdOutOfRange if `local_archive_id` is not less than
     *   the number of archives.
     * - Forwards an index builder's `add_archive` return values on failure.
     */
    [[nodiscard]] auto
    add_archive(uint16_t local_archive_id, clp_s::ArchiveReader const& archive_reader)
            -> ystdlib::error_handling::Result<void>;

    /**
     * Collects each builder's per-archive blobs and serializes the full pack. Must be called at most
     * once, after every archive has been added.
     * @return A result containing the serialized pack on success, or an error code indicating the
     * failure:
     * - Forwards `PackedFilterWriter::add_index` and `PackedFilterWriter::serialize` return values
     *   on failure.
     */
    [[nodiscard]] auto serialize() -> ystdlib::error_handling::Result<std::vector<char>>;

private:
    // Variables
    size_t m_num_archives;
    PackedFilterWriter m_writer;
    std::vector<ActiveIndex> m_active_indexes;
};
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_PACKED_FILTER_BUILDER_HPP
