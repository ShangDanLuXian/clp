#include "SetFingerprint.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <fmt/format.h>
#include <zstd.h>

namespace clp_s::archive_analyzer {
namespace {
// FNV-1a 64-bit parameters.
constexpr uint64_t cFnv1a64OffsetBasis{14'695'981'039'346'656'037ULL};
constexpr uint64_t cFnv1a64Prime{1'099'511'628'211ULL};

// The zstd level used to measure each item's individually-compressed size. Matches clp-s's
// default archive compression level.
constexpr int cZstdCompressionLevel{3};

/**
 * @param value
 * @param compression_buffer A reusable scratch buffer.
 * @return The size of `value` after compressing it individually with zstd, or `value`'s
 * uncompressed size if compression fails.
 */
[[nodiscard]] auto
individually_compressed_size(std::string_view value, std::vector<char>& compression_buffer)
        -> uint64_t {
    auto const bound{ZSTD_compressBound(value.size())};
    if (compression_buffer.size() < bound) {
        compression_buffer.resize(bound);
    }
    auto const compressed_size{ZSTD_compress(
            compression_buffer.data(),
            compression_buffer.size(),
            value.data(),
            value.size(),
            cZstdCompressionLevel
    )};
    if (0 != ZSTD_isError(compressed_size)) {
        return value.size();
    }
    return compressed_size;
}
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

    std::vector<std::tuple<uint64_t, std::string_view>> hash_and_value_pairs;
    hash_and_value_pairs.reserve(values.size());
    for (auto const value : values) {
        hash_and_value_pairs.emplace_back(fnv1a64(value), value);
    }
    std::ranges::sort(hash_and_value_pairs, {}, [](auto const& pair) {
        return std::get<0>(pair);
    });
    auto const duplicates{
            std::ranges::unique(
                    hash_and_value_pairs,
                    [](auto const& lhs, auto const& rhs) {
                        return std::get<0>(lhs) == std::get<0>(rhs);
                    }
            )
    };
    hash_and_value_pairs.erase(duplicates.begin(), duplicates.end());

    fingerprint.fingerprints.reserve(hash_and_value_pairs.size());
    fingerprint.fingerprint_sizes.reserve(hash_and_value_pairs.size());
    fingerprint.fingerprint_compressed_sizes.reserve(hash_and_value_pairs.size());
    std::vector<char> compression_buffer;
    for (auto const& [hash, value] : hash_and_value_pairs) {
        fingerprint.fingerprints.push_back(hash);
        fingerprint.fingerprint_sizes.push_back(value.size());
        fingerprint.fingerprint_compressed_sizes.push_back(
                individually_compressed_size(value, compression_buffer)
        );
    }
    fingerprint.checksum = checksum_of_sorted_fingerprints(fingerprint.fingerprints);
    return fingerprint;
}
}  // namespace clp_s::archive_analyzer
