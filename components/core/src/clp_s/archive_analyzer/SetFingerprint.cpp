#include "SetFingerprint.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/format.h>

namespace clp_s::archive_analyzer {
namespace {
// FNV-1a 64-bit parameters.
constexpr uint64_t cFnv1a64OffsetBasis{14'695'981'039'346'656'037ULL};
constexpr uint64_t cFnv1a64Prime{1'099'511'628'211ULL};
}  // namespace

auto fnv1a64(std::string_view data) -> uint64_t {
    uint64_t hash{cFnv1a64OffsetBasis};
    for (char const c : data) {
        hash ^= static_cast<uint8_t>(c);
        hash *= cFnv1a64Prime;
    }
    return hash;
}

auto checksum_of_sorted_fingerprints(std::vector<uint64_t> const& sorted_fingerprints)
        -> std::string {
    uint64_t checksum{cFnv1a64OffsetBasis};
    for (auto const fingerprint : sorted_fingerprints) {
        for (size_t byte_idx{0}; byte_idx < sizeof(fingerprint); ++byte_idx) {
            checksum ^= static_cast<uint8_t>(fingerprint >> (8 * byte_idx));
            checksum *= cFnv1a64Prime;
        }
    }
    return fmt::format("{:016x}", checksum);
}

auto sort_and_checksum_fingerprints(std::vector<uint64_t>& fingerprints) -> std::string {
    std::ranges::sort(fingerprints);
    auto const duplicates{std::ranges::unique(fingerprints)};
    fingerprints.erase(duplicates.begin(), duplicates.end());
    return checksum_of_sorted_fingerprints(fingerprints);
}

auto compute_string_set_fingerprint(std::vector<std::string_view> const& values)
        -> SetFingerprint {
    SetFingerprint fingerprint;
    fingerprint.num_items = values.size();

    std::vector<std::pair<uint64_t, uint64_t>> hash_and_size_pairs;
    hash_and_size_pairs.reserve(values.size());
    for (auto const value : values) {
        hash_and_size_pairs.emplace_back(fnv1a64(value), value.size());
    }
    std::ranges::sort(hash_and_size_pairs);
    auto const duplicates{
            std::ranges::unique(
                    hash_and_size_pairs,
                    [](auto const& lhs, auto const& rhs) { return lhs.first == rhs.first; }
            )
    };
    hash_and_size_pairs.erase(duplicates.begin(), duplicates.end());

    fingerprint.fingerprints.reserve(hash_and_size_pairs.size());
    fingerprint.fingerprint_sizes.reserve(hash_and_size_pairs.size());
    for (auto const& [hash, size] : hash_and_size_pairs) {
        fingerprint.fingerprints.push_back(hash);
        fingerprint.fingerprint_sizes.push_back(size);
    }
    fingerprint.checksum = checksum_of_sorted_fingerprints(fingerprint.fingerprints);
    return fingerprint;
}
}  // namespace clp_s::archive_analyzer
