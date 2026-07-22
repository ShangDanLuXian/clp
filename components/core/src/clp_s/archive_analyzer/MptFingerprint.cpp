#include "MptFingerprint.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/format.h>

#include <clp_s/archive_constants.hpp>
#include <clp_s/SchemaTree.hpp>

namespace clp_s::archive_analyzer {
namespace {
// FNV-1a 64-bit parameters. FNV-1a is used instead of `std::hash` because fingerprints must be
// stable across platforms, builds, and runs.
constexpr uint64_t cFnv1a64OffsetBasis{14'695'981'039'346'656'037ULL};
constexpr uint64_t cFnv1a64Prime{1'099'511'628'211ULL};

// Separator between the fields of a node's canonical form. The ASCII unit separator is vanishingly
// unlikely to appear in key names.
constexpr char cCanonicalFormSeparator{'\x1f'};

/**
 * @param data
 * @return The FNV-1a 64-bit hash of `data`.
 */
[[nodiscard]] auto fnv1a64(std::string_view data) -> uint64_t {
    uint64_t hash{cFnv1a64OffsetBasis};
    for (char const c : data) {
        hash ^= static_cast<uint8_t>(c);
        hash *= cFnv1a64Prime;
    }
    return hash;
}
}  // namespace

auto compute_mpt_fingerprint(SchemaTree const& schema_tree) -> MptFingerprint {
    auto const& nodes{schema_tree.get_nodes()};
    MptFingerprint fingerprint;
    fingerprint.num_nodes = nodes.size();
    fingerprint.node_fingerprints.reserve(nodes.size());

    std::string canonical_form;
    std::vector<std::string_view> key_path;
    for (auto const& node : nodes) {
        // A node's canonical form is its type followed by its key path from the root, top-down.
        // The numeric type value is used since `NodeType` values are part of the archive format.
        canonical_form.clear();
        canonical_form += std::to_string(static_cast<int>(node.get_type()));
        key_path.clear();
        for (auto id{node.get_id()}; constants::cRootNodeId != id;) {
            auto const& ancestor{schema_tree.get_node(id)};
            key_path.emplace_back(ancestor.get_key_name());
            id = ancestor.get_parent_id();
        }
        for (auto it{key_path.rbegin()}; it != key_path.rend(); ++it) {
            canonical_form += cCanonicalFormSeparator;
            canonical_form += *it;
        }
        fingerprint.node_fingerprints.push_back(fnv1a64(canonical_form));
    }

    std::ranges::sort(fingerprint.node_fingerprints);
    auto const duplicates{std::ranges::unique(fingerprint.node_fingerprints)};
    fingerprint.node_fingerprints.erase(duplicates.begin(), duplicates.end());

    // Compute the order-independent checksum by hashing the sorted node fingerprints with an
    // explicit byte order.
    uint64_t checksum{cFnv1a64OffsetBasis};
    for (auto const node_fingerprint : fingerprint.node_fingerprints) {
        for (size_t byte_idx{0}; byte_idx < sizeof(node_fingerprint); ++byte_idx) {
            checksum ^= static_cast<uint8_t>(node_fingerprint >> (8 * byte_idx));
            checksum *= cFnv1a64Prime;
        }
    }
    fingerprint.checksum = fmt::format("{:016x}", checksum);
    return fingerprint;
}
}  // namespace clp_s::archive_analyzer
