#ifndef CLP_S_PACKED_FILTER_QUERY_HPP
#define CLP_S_PACKED_FILTER_QUERY_HPP

#include <string>

namespace clp_s::packed_filter {
/**
 * Loads every `.pack` file in `packs_dir`, filters each against `query_string` (a KQL query), and
 * reports how many archives were pruned (i.e. can be skipped) per pack and in total.
 * @param packs_dir Directory containing the `.pack` files to filter.
 * @param query_string The KQL query to filter against.
 * @return Whether every pack was processed successfully.
 */
[[nodiscard]] auto query_packs(std::string const& packs_dir, std::string const& query_string) -> bool;
}  // namespace clp_s::packed_filter

#endif  // CLP_S_PACKED_FILTER_QUERY_HPP
