#ifndef CLP_S_FILTER_REGISTER_INDEXES_HPP
#define CLP_S_FILTER_REGISTER_INDEXES_HPP

#include <ystdlib/error_handling/Result.hpp>

namespace clp_s::filter {
class IndexRegistry;

/**
 * Registers every built-in (official open-source) index implementation with a registry. This is the
 * single entry point the framework calls to populate a registry before building or filtering.
 * @param registry
 * @return A void result on success, or an error code indicating the failure:
 * - Forwards `IndexRegistry::register_index`'s return values on failure.
 */
[[nodiscard]] auto register_indexes(IndexRegistry& registry)
        -> ystdlib::error_handling::Result<void>;
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_REGISTER_INDEXES_HPP
