#ifndef CLP_S_INDEXER_PACKEDFILTERINDEXER_HPP
#define CLP_S_INDEXER_PACKEDFILTERINDEXER_HPP

#include <string>

#include "../InputConfig.hpp"

namespace clp_s::indexer {
/**
 * Builds a Bloom filter Packed Filter over every archive found under `input_path` and writes the
 * serialized pack to a local file.
 *
 * All archives under `input_path` are grouped into a single pack with one candidate bit per archive,
 * so that filtering can later prune (i.e. avoid opening) archives that cannot match a query.
 *
 * @param input_path A dataset path; every archive under it (filesystem or network/S3) is indexed.
 * @param output_path Local filesystem path the serialized pack is written to.
 * @param network_auth Authentication used when reading archives over the network.
 * @return Whether the Packed Filter was built and written successfully.
 */
[[nodiscard]] auto build_packed_filter(
        Path const& input_path,
        std::string const& output_path,
        NetworkAuthOption const& network_auth
) -> bool;
}  // namespace clp_s::indexer

#endif  // CLP_S_INDEXER_PACKEDFILTERINDEXER_HPP
