#ifndef CLP_S_PREFIXSUFFIXFILTER_HPP
#define CLP_S_PREFIXSUFFIXFILTER_HPP

#include <string_view>
#include <memory>
#include <vector>
#include <absl/container/flat_hash_set.h>

#include "ProbabilisticFilter.hpp"
#include "BloomFilter.hpp"

namespace clp_s {

class PrefixSuffixFilter : public IProbabilisticFilter {
public:
    // Constants for tuning space vs accuracy
    static constexpr size_t kMinLength = 3; // Don't index "a", "ab"
    static constexpr size_t kStride = 1;    // Index every Nth prefix. 1 = all, 2 = every other.

    /**
     * @param expected_num_elements The count of unique raw keys (e.g., dictionary size)
     * @param false_positive_rate Target FPR for the underlying Bloom filters
     * @param avg_key_length Estimated average length of keys (used to size the internal filters)
     */
    PrefixSuffixFilter(
        size_t expected_num_elements,
        double false_positive_rate,
        size_t avg_key_length = 32
    );

    PrefixSuffixFilter(
        absl::flat_hash_set<std::string> const& key_set,
        double false_positive_rate
    );

    PrefixSuffixFilter() = default;

    void add(std::string_view value) override;
    [[nodiscard]] auto possibly_contains(std::string_view value) const -> bool override;
    
    void write_to_file(class FileWriter& file_writer, class ZstdCompressor& compressor) const override;
    auto read_from_file(clp::ReaderInterface& reader, class ZstdDecompressor& decompressor) -> bool override;
    
    [[nodiscard]] auto is_empty() const -> bool override;
    [[nodiscard]] auto get_type() const -> FilterType override { return FilterType::PrefixSuffix; }
    [[nodiscard]] auto get_memory_usage() const -> size_t override;

    [[nodiscard]] auto clone() const -> std::unique_ptr<IProbabilisticFilter> override;

private:
    // Helpers
    void add_prefixes(std::string_view value, BloomFilter& filter);
    [[nodiscard]] auto check_prefix(std::string_view value, BloomFilter const& filter) const -> bool;

    // We wrap BloomFilter internally. 
    // We use unique_ptr to allow lazy initialization or polymorphic behavior if needed,
    // but direct objects would also work. unique_ptr fits your existing patterns.
    std::unique_ptr<BloomFilter> m_forward_filter;
    std::unique_ptr<BloomFilter> m_reverse_filter;
};

} // namespace clp_s

#endif // CLP_S_PREFIXSUFFIXFILTER_HPP