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

auto sort_and_checksum_fingerprints(std::vector<uint64_t>& fingerprints) -> std::string {
    std::ranges::sort(fingerprints);
    auto const duplicates{std::ranges::unique(fingerprints)};
    fingerprints.erase(duplicates.begin(), duplicates.end());

    uint64_t checksum{cFnv1a64OffsetBasis};
    for (auto const fingerprint : fingerprints) {
        for (size_t byte_idx{0}; byte_idx < sizeof(fingerprint); ++byte_idx) {
            checksum ^= static_cast<uint8_t>(fingerprint >> (8 * byte_idx));
            checksum *= cFnv1a64Prime;
        }
    }
    return fmt::format("{:016x}", checksum);
}

auto compute_string_set_fingerprint(std::vector<std::string_view> const& values)
        -> SetFingerprint {
    SetFingerprint fingerprint;
    fingerprint.num_items = values.size();
    fingerprint.fingerprints.reserve(values.size());
    for (auto const value : values) {
        fingerprint.fingerprints.push_back(fnv1a64(value));
    }
    fingerprint.checksum = sort_and_checksum_fingerprints(fingerprint.fingerprints);
    return fingerprint;
}
}  // namespace clp_s::archive_analyzer
