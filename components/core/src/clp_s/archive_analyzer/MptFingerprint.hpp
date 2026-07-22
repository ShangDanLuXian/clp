#ifndef CLP_S_ARCHIVE_ANALYZER_MPT_FINGERPRINT_HPP
#define CLP_S_ARCHIVE_ANALYZER_MPT_FINGERPRINT_HPP

#include <cstdint>
#include <string>
#include <vector>

#include <clp_s/SchemaTree.hpp>

namespace clp_s::archive_analyzer {
/**
 * A canonical fingerprint of an archive's merged parse tree (MPT), i.e. its schema tree.
 *
 * The fingerprint is computed over a canonical form of the tree - each node is represented by its
 * type and its key path from the root - so it does not depend on node IDs or insertion order and
 * is comparable across archives. Two archives with the same checksum have identical MPTs (up to
 * hash collisions), and the overlap between two archives' node fingerprints measures how similar
 * their MPTs are. Since node fingerprints are one-way hashes, they can be compared across archives
 * without exposing any key names.
 */
struct MptFingerprint {
    // The number of nodes in the MPT.
    uint64_t num_nodes{};
    // Order-independent checksum of the whole MPT, as a 16-character hex string.
    std::string checksum;
    // The sorted, deduplicated fingerprint of each node's canonical form.
    std::vector<uint64_t> node_fingerprints;
};

/**
 * Computes the canonical MPT fingerprint of `schema_tree`.
 *
 * @param schema_tree
 * @return The fingerprint.
 */
[[nodiscard]] auto compute_mpt_fingerprint(SchemaTree const& schema_tree) -> MptFingerprint;
}  // namespace clp_s::archive_analyzer

#endif  // CLP_S_ARCHIVE_ANALYZER_MPT_FINGERPRINT_HPP
