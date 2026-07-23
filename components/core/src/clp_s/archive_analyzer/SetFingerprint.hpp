#ifndef CLP_S_ARCHIVE_ANALYZER_SET_FINGERPRINT_HPP
#define CLP_S_ARCHIVE_ANALYZER_SET_FINGERPRINT_HPP

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace clp_s::archive_analyzer {
/**
 * A canonical fingerprint of a set of strings (e.g. a dictionary's entries).
 *
 * The fingerprint does not depend on the order of the strings, so it is comparable across
 * archives: two archives with the same checksum have identical sets (up to hash collisions), and
 * the overlap between two archives' per-item fingerprints measures how similar their sets are.
 * Since the fingerprints are one-way hashes, they can be compared across archives without
 * exposing the strings themselves.
 */
struct SetFingerprint {
    // The number of items the fingerprint was computed over.
    uint64_t num_items{};
    // Order-independent checksum of the whole set, as a 16-character hex string.
    std::string checksum;
    // The sorted, deduplicated fingerprint of each item.
    std::vector<uint64_t> fingerprints;
    // The byte size of each distinct item, aligned with `fingerprints`. Lets consumers compute
    // size-weighted overlap (e.g. projecting the size of a merged dictionary) in addition to
    // count-based overlap.
    std::vector<uint64_t> fingerprint_sizes;
    // The zstd-compressed byte size of each distinct item compressed individually, aligned with
    // `fingerprints`. An upper bound on the item's cost in a jointly-compressed stream (joint
    // compression also exploits redundancy across items).
    std::vector<uint64_t> fingerprint_compressed_sizes;
};

/**
 * @param data
 * @return The FNV-1a 64-bit hash of `data`. FNV-1a is used instead of `std::hash` because
 * fingerprints must be stable across platforms, builds, and runs.
 */
[[nodiscard]] auto fnv1a64(std::string_view data) -> uint64_t;

/**
 * Computes the order-independent checksum of already-sorted fingerprints with an explicit byte
 * order.
 *
 * @param sorted_fingerprints
 * @return The checksum, as a 16-character hex string.
 */
[[nodiscard]] auto checksum_of_sorted_fingerprints(std::vector<uint64_t> const& sorted_fingerprints
) -> std::string;

/**
 * Sorts and deduplicates `fingerprints` in place, and computes their order-independent checksum
 * with an explicit byte order.
 *
 * @param fingerprints
 * @return The checksum, as a 16-character hex string.
 */
[[nodiscard]] auto sort_and_checksum_fingerprints(std::vector<uint64_t>& fingerprints)
        -> std::string;

/**
 * Computes the canonical fingerprint of a set of strings.
 *
 * @param values
 * @return The fingerprint.
 */
[[nodiscard]] auto compute_string_set_fingerprint(std::vector<std::string_view> const& values)
        -> SetFingerprint;
}  // namespace clp_s::archive_analyzer

#endif  // CLP_S_ARCHIVE_ANALYZER_SET_FINGERPRINT_HPP
