#ifndef CLP_S_FILTER_PACKED_FILTER_RUNNER_HPP
#define CLP_S_FILTER_PACKED_FILTER_RUNNER_HPP

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <ystdlib/error_handling/Result.hpp>

#include <clp_s/filter/IndexDefs.hpp>
#include <clp_s/filter/IndexRunner.hpp>

namespace clp_s::search::ast {
class Expression;
}  // namespace clp_s::search::ast

namespace clp_s::filter {
/**
 * Read-path orchestrator for a Packed Filter.
 *
 * Holds an `IndexRunner` for each index in a Packed Filter that the registry could load, and runs
 * them in sequence to narrow a set of candidate archives for a query: each runner clears the bits
 * of archives that cannot contain a match.
 *
 * Indexes whose Index ID is not registered, or whose runner fails to load, are skipped rather than
 * failing the whole Packed Filter; their Index IDs are reported by `get_skipped_index_ids` so the
 * caller can warn and continue.
 *
 * Each `IndexRunner` owns its deserialized data, so the pack's bytes need not be retained.
 */
class PackedFilterRunner {
public:
    // Types
    /**
     * A loaded index runner, paired with the Index ID it was loaded for.
     */
    struct ActiveRunner {
        index_id_t index_id{};
        std::unique_ptr<IndexRunner> runner;
    };

    // Constructors
    PackedFilterRunner(
            std::vector<std::string> archive_ids,
            std::vector<ActiveRunner> active_runners,
            std::vector<index_id_t> skipped_index_ids
    )
            : m_archive_ids{std::move(archive_ids)},
              m_active_runners{std::move(active_runners)},
              m_skipped_index_ids{std::move(skipped_index_ids)} {}

    // Methods
    /**
     * Narrows the set of candidate archives for a query by running each loaded index in sequence.
     * @param query The root of the query AST to filter against.
     * @param candidate_archive_bitmap The set of candidate archives, where bit i corresponds to the
     * local archive ID i. Returns the narrowed set of candidate archives.
     * @return A void result on success, or an error code indicating the failure:
     * - Forwards an index runner's `filter` return values on failure.
     */
    [[nodiscard]] auto filter(
            std::shared_ptr<clp_s::search::ast::Expression> const& query,
            CandidateArchiveBitmapView& candidate_archive_bitmap
    ) -> ystdlib::error_handling::Result<void>;

    /**
     * @return The number of archives the Packed Filter covers.
     */
    [[nodiscard]] auto get_num_archives() const -> size_t { return m_archive_ids.size(); }

    /**
     * @return The archive ID of each archive in the Packed Filter, indexed by local archive ID.
     */
    [[nodiscard]] auto get_archive_ids() const -> std::vector<std::string> const& {
        return m_archive_ids;
    }

    /**
     * @return The Index IDs of indexes in the Packed Filter that could not be loaded and were
     * skipped.
     */
    [[nodiscard]] auto get_skipped_index_ids() const -> std::vector<index_id_t> const& {
        return m_skipped_index_ids;
    }

private:
    // Variables
    std::vector<std::string> m_archive_ids;
    std::vector<ActiveRunner> m_active_runners;
    std::vector<index_id_t> m_skipped_index_ids;
};
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_PACKED_FILTER_RUNNER_HPP
