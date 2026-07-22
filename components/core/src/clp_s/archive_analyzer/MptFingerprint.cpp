#include "MptFingerprint.hpp"

#include <string>
#include <string_view>
#include <vector>

#include <clp_s/archive_analyzer/SetFingerprint.hpp>
#include <clp_s/archive_constants.hpp>
#include <clp_s/SchemaTree.hpp>

namespace clp_s::archive_analyzer {
namespace {
// Separator between the fields of a node's canonical form. The ASCII unit separator is vanishingly
// unlikely to appear in key names.
constexpr char cCanonicalFormSeparator{'\x1f'};
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

    fingerprint.checksum = sort_and_checksum_fingerprints(fingerprint.node_fingerprints);
    return fingerprint;
}
}  // namespace clp_s::archive_analyzer
