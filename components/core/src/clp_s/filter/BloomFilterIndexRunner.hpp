#ifndef CLP_S_FILTER_BLOOM_FILTER_INDEX_RUNNER_HPP
#define CLP_S_FILTER_BLOOM_FILTER_INDEX_RUNNER_HPP

#include <memory>
#include <span>
#include <utility>
#include <vector>

#include <ystdlib/error_handling/Result.hpp>

#include <clp_s/filter/FilterReader.hpp>
#include <clp_s/filter/IndexDefs.hpp>
#include <clp_s/filter/IndexRunner.hpp>

namespace clp_s::search::ast {
class Expression;
}  // namespace clp_s::search::ast

namespace clp_s::filter {
/**
 * An `IndexRunner` for the Bloom filter index. Holds one per-archive `FilterReader` deserialized
 * from the index's blobs, and prunes a candidate archive whenever the query is provably false for
 * that archive given its Bloom filter.
 *
 * Pruning is conservative: a Bloom filter can only prove that a value is absent, never that it is
 * present, so the runner clears an archive's candidate bit only when the query cannot be satisfied
 * without a value the archive's variable dictionary definitely does not contain.
 */
class BloomFilterIndexRunner : public IndexRunner {
public:
    // Factory functions
    /**
     * Factory matching `IndexRegistry::IndexRunnerFactory`.
     * @param index_version The serialized index version (currently unused; the blob layout is
     * self-describing).
     * @param archive_blobs One serialized Bloom filter per archive, indexed by local archive ID.
     * @return A result containing the created runner on success, or an error code indicating the
     * failure:
     * - Forwards `FilterReader::try_read`'s return values if a blob is malformed.
     */
    [[nodiscard]] static auto create(
            index_version_t index_version,
            std::vector<std::span<char const>> const& archive_blobs
    ) -> ystdlib::error_handling::Result<std::unique_ptr<IndexRunner>>;

    // Methods (IndexRunner)
    [[nodiscard]] auto filter(
            std::shared_ptr<clp_s::search::ast::Expression> const& query,
            CandidateArchiveBitmapView& candidate_archive_bitmap
    ) -> ystdlib::error_handling::Result<void> override;

private:
    // Constructors
    explicit BloomFilterIndexRunner(std::vector<FilterReader> filter_readers)
            : m_filter_readers{std::move(filter_readers)} {}

    // Variables
    // One Bloom filter reader per archive, indexed by local archive ID.
    std::vector<FilterReader> m_filter_readers;
};
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_BLOOM_FILTER_INDEX_RUNNER_HPP
