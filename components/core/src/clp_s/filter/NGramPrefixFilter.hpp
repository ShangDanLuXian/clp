#ifndef CLP_S_NGRAMPREFIXFILTER_HPP
#define CLP_S_NGRAMPREFIXFILTER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include "../../clp/ReaderInterface.hpp"
#include "FilterPolicy/BloomFilterPolicy.hpp"
#include "ProbabilisticFilter.hpp"

namespace clp_s {

/**
 * N-gram based probabilistic filter for exact and prefix wildcard matching.
 * 
 * Current implementation: Uniform memory distribution across n-grams for exact matching.
 * Future: Harmonic degradation for prefix wildcard queries (more bits to earlier n-grams).
 * 
 * Design:
 * - Keys are grouped by length into separate filters
 * - For keys with length >= n: decompose into n-grams, build filter on unique n-grams
 * - For keys with length < n: build filter on full keys (no n-gram decomposition)
 * - Exact match: AND of all n-gram checks
 */
class NGramPrefixFilter : public IProbabilisticFilter {
public:
    NGramPrefixFilter(double false_positive_rate);
    NGramPrefixFilter(absl::flat_hash_set<std::string> const& key_set, double false_positive_rate);
    NGramPrefixFilter() = default;

    // IProbabilisticFilter interface
    void add(std::string_view value) override;
    [[nodiscard]] auto possibly_contains(std::string_view value) const -> bool override;
    void write_to_file(class FileWriter& file_writer, class ZstdCompressor& compressor)
            const override;
    auto read_from_file(clp::ReaderInterface& reader, class ZstdDecompressor& decompressor)
            -> bool override;
    [[nodiscard]] auto is_empty() const -> bool override { 
        return m_length_filter_map.empty(); 
    }
    [[nodiscard]] auto get_type() const -> FilterType override { 
        return FilterType::NGramPrefix; 
    }
    [[nodiscard]] auto get_memory_usage() const -> size_t override;

    [[nodiscard]] auto clone() const -> std::unique_ptr<IProbabilisticFilter> override {
        auto copy = std::make_unique<NGramPrefixFilter>();
        copy->m_length_key_map = m_length_key_map;
        copy->m_length_n_gram_map = m_length_n_gram_map;
        copy->m_length_filter_map = m_length_filter_map;
        copy->m_n = m_n;
        return copy;
    }

    /**
     * @return The computed n-gram length
     */
    [[nodiscard]] auto get_n() const -> uint32_t { return m_n; }

private:
    static constexpr FilterType internal_filter_type = FilterType::BinaryFuse;
    static constexpr double m_target_collision_rate = 0.01;
    static constexpr uint32_t m_alphabet_size = 26;

    // Data structures
    absl::flat_hash_map<uint32_t, absl::flat_hash_set<std::string>> m_length_key_map;
    absl::flat_hash_map<uint32_t, absl::flat_hash_set<std::string>> m_length_n_gram_map;
    absl::flat_hash_map<uint32_t, ProbabilisticFilter> m_length_filter_map;

    uint32_t m_n{0};  // **IMPORTANT**: Initialize to 0

    void calculate_n();
    void extract_ngrams();
    void construct_filters(double false_positive_rate);

    [[nodiscard]] static auto compute_per_ngram_fpr(
            double target_false_positive_rate,
            uint32_t length,
            uint32_t num_entries,
            uint32_t ngram_count
    ) -> double;
};

}  // namespace clp_s

#endif  // CLP_S_NGRAMPREFIXFILTER_HPP