#ifndef CLP_S_PACKED_FILTER_BUILD_HPP
#define CLP_S_PACKED_FILTER_BUILD_HPP

#include <cstddef>
#include <string>

#include <clp_s/InputConfig.hpp>

namespace clp_s::packed_filter {
// Default upper bound on a single pack's serialized size (32 MiB).
constexpr size_t cDefaultMaxPackSize{32ULL * 1024 * 1024};

/**
 * Builds size-bounded Bloom filter Packed Filters over the archives found under `input_path`.
 *
 * Archives are processed in archive-id order and greedily grouped into packs: archives are added to
 * the current pack until adding the next one would exceed `max_pack_size`, at which point the
 * current pack is finalized and a new one is started. The packs are written to `output_dir` as
 * `0.pack`, `1.pack`, ... in build order.
 *
 * @param input_path A dataset path; every archive under it (filesystem or network/S3) is indexed.
 * @param output_dir Local directory the pack files are written to (created if it doesn't exist).
 * @param network_auth Authentication used when reading archives over the network.
 * @param max_pack_size Upper bound on each pack's serialized size, in bytes.
 * @return Whether every pack was built and written successfully.
 */
[[nodiscard]] auto build_packed_filter(
        Path const& input_path,
        std::string const& output_dir,
        NetworkAuthOption const& network_auth,
        size_t max_pack_size
) -> bool;
}  // namespace clp_s::packed_filter

#endif  // CLP_S_PACKED_FILTER_BUILD_HPP
