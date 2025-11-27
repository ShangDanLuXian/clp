#ifndef CLP_S_BLOOMFILTER_HPP
#define CLP_S_BLOOMFILTER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>
#include <absl/container/flat_hash_set.h>

#include "../../clp/ReaderInterface.hpp"
#include "FilterPolicy/BloomFilterPolicy.hpp"
#include "FilterPolicy/FilterPolicy.hpp"
#include "ProbabilisticFilter.hpp"

namespace clp_s {

class BloomFilter : public IProbabilisticFilter {
public:
    /**
     * Constructs a bloom filter with custom policy
     */
    BloomFilter(
            size_t expected_num_elements,
            double false_positive_rate,
            std::unique_ptr<IFilterPolicy> policy
    );

    /**
     * Constructs a bloom filter with default (optimal) policy
     */
    BloomFilter(size_t expected_num_elements, double false_positive_rate);

    BloomFilter(
            absl::flat_hash_set<std::string> const& key_set,
            double false_positive_rate
    );

    BloomFilter() = default;

    // IProbabilisticFilter interface
    void add(std::string_view value) override;
    [[nodiscard]] auto possibly_contains(std::string_view value) const -> bool override;
    void write_to_file(class FileWriter& file_writer, class ZstdCompressor& compressor)
            const override;
    auto read_from_file(clp::ReaderInterface& reader, class ZstdDecompressor& decompressor)
            -> bool override;
    [[nodiscard]] auto is_empty() const -> bool override { return m_bit_array.empty(); }
    [[nodiscard]] auto get_type() const -> FilterType override { return FilterType::Bloom; }
    [[nodiscard]] auto get_memory_usage() const -> size_t override { 
        return m_bit_array.size(); 
    }

    [[nodiscard]] auto get_bit_array_size() const -> size_t { return m_bit_array_size; }
    [[nodiscard]] auto get_num_hash_functions() const -> uint32_t { 
        return m_num_hash_functions; 
    }

    [[nodiscard]] auto clone() const -> std::unique_ptr<IProbabilisticFilter> override {
        auto copy = std::make_unique<BloomFilter>();
        copy->m_bit_array = m_bit_array;
        copy->m_bit_array_size = m_bit_array_size;
        copy->m_num_hash_functions = m_num_hash_functions;
        copy->m_policy = m_policy->clone();
        return copy;
    }

private:
    void generate_hash_values(std::string_view value, std::vector<size_t>& hash_values) const;
    void set_bit(size_t bit_index);
    [[nodiscard]] auto test_bit(size_t bit_index) const -> bool;

    std::vector<uint8_t> m_bit_array;
    size_t m_bit_array_size{0};
    uint32_t m_num_hash_functions{0};
    std::unique_ptr<IFilterPolicy> m_policy;
};

}  // namespace clp_s

#endif  // CLP_S_BLOOMFILTER_HPP