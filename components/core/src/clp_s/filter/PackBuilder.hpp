#ifndef CLP_S_FILTER_PACKBUILDER_HPP
#define CLP_S_FILTER_PACKBUILDER_HPP

#include <span>
#include <string>

#include <clp_s/InputConfig.hpp>

namespace clp_s::filter {
/**
 * Builds a single Bloom filter Packed Filter over the given batch of archives and writes it to
 * `output_dir`.
 *
 * A Bloom filter is built over each archive's variable dictionary, the per-archive filters are
 * assembled into one pack, and the pack is written to `output_dir`/`<first-archive-id>`.pack.
 * Naming the pack after its first archive's (unique) id lets several threads write packs into the
 * same directory without any coordination: the benchmark's work queue hands each archive to exactly
 * one batch, so no two batches share a first archive.
 *
 * @param archive_paths The archives to pack. Local archive IDs are assigned in the given order; the
 * batch must be non-empty.
 * @param output_dir Local directory the pack is written to. Must already exist.
 * @param network_auth Authentication used when reading archives over the network.
 * @return Whether the pack was built and written successfully.
 */
[[nodiscard]] auto build_pack_from_archives(
        std::span<Path const> archive_paths,
        std::string const& output_dir,
        NetworkAuthOption const& network_auth
) -> bool;
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_PACKBUILDER_HPP
