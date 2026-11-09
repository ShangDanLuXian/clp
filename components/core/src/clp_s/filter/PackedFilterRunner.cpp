#include <clp_s/filter/PackedFilterRunner.hpp>

#include <memory>

#include <ystdlib/error_handling/Result.hpp>

#include <clp_s/filter/IndexRunner.hpp>

namespace clp_s::search::ast {
class Expression;
}  // namespace clp_s::search::ast

namespace clp_s::filter {
auto PackedFilterRunner::filter(
        std::shared_ptr<clp_s::search::ast::Expression> const& query,
        CandidateArchiveBitmapView& candidate_archive_bitmap
) -> ystdlib::error_handling::Result<void> {
    for (auto const& active_runner : m_active_runners) {
        YSTDLIB_ERROR_HANDLING_TRYV(active_runner.runner->filter(query, candidate_archive_bitmap));
    }
    return ystdlib::error_handling::success();
}
}  // namespace clp_s::filter
