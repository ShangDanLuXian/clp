#ifndef CLP_S_FILTER_PACKSEARCHER_HPP
#define CLP_S_FILTER_PACKSEARCHER_HPP

#include <string>

#include <clp_s/InputConfig.hpp>

namespace clp_s::filter {
/**
 * Loads a single `.pack` file, filters it against `kql_query`, and logs how many of the pack's
 * archives were pruned (i.e. can be skipped) versus how many must still be opened.
 *
 * The pack is read through `try_create_reader`, so both filesystem and network (e.g. S3) packs are
 * supported subject to `network_auth`. The KQL query is parsed and the index registry is built on
 * each call, which keeps the function self-contained and free of shared mutable state so it can be
 * invoked concurrently from many threads.
 *
 * @param kql_query The KQL query to filter against.
 * @param pack_path The pack to filter.
 * @param network_auth Authentication used when reading the pack over the network.
 * @return Whether the pack was processed successfully.
 */
[[nodiscard]] auto search_pack(
        std::string const& kql_query,
        Path const& pack_path,
        NetworkAuthOption const& network_auth
) -> bool;
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_PACKSEARCHER_HPP
