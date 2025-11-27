#ifndef CLP_S_BINARYFUSEFILTER_HPP
#define CLP_S_BINARYFUSEFILTER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>
#include <tuple>
#include <absl/container/flat_hash_set.h>

#include "../../clp/ReaderInterface.hpp"
#include "FilterPolicy/FilterPolicy.hpp" // Adjusted path based on your context
#include "ProbabilisticFilter.hpp"

namespace clp_s {

class BinaryFuseFilter : public IProbabilisticFilter {
public:
    BinaryFuseFilter(
            size_t expected_num_elements,
            double false_positive_rate,
            std::unique_ptr<IFilterPolicy> policy
    );

    BinaryFuseFilter(size_t expected_num_elements, double false_positive_rate);

    BinaryFuseFilter(
            absl::flat_hash_set<std::string> const& key_set,
            double false_positive_rate
    );

    BinaryFuseFilter() = default;

    void add(std::string_view value) override;
    [[nodiscard]] auto possibly_contains(std::string_view value) const -> bool override;
    void write_to_file(class FileWriter& file_writer, class ZstdCompressor& compressor)
            const override;
    auto read_from_file(clp::ReaderInterface& reader, class ZstdDecompressor& decompressor)
            -> bool override;
    
    [[nodiscard]] auto is_empty() const -> bool override { 
        return m_filter_u8.empty() && m_filter_u16.empty() && m_filter_u32.empty(); 
    }
    [[nodiscard]] auto get_type() const -> FilterType override { return FilterType::BinaryFuse; }
    [[nodiscard]] auto get_memory_usage() const -> size_t override;

    [[nodiscard]] auto clone() const -> std::unique_ptr<IProbabilisticFilter> override;

    [[nodiscard]] auto get_array_size() const -> size_t { return m_array_size; }
    [[nodiscard]] auto get_segment_length() const -> size_t { return m_segment_length; }
    [[nodiscard]] auto get_fingerprint_bits() const -> uint32_t { return m_fingerprint_bits; }
    [[nodiscard]] auto get_seed() const -> uint32_t { return m_seed; }

private:
    void construct_filter();
    auto try_construct() -> bool;
    
    [[nodiscard]] auto hash_key(std::string_view key, uint32_t seed) const -> uint64_t;
    
    [[nodiscard]] auto get_locations_and_fingerprint(std::string_view key) const
            -> std::tuple<size_t, size_t, size_t, uint32_t>;
    
    [[nodiscard]] auto get_filter_value(size_t pos) const -> uint32_t;
    void set_filter_value(size_t pos, uint32_t value);
    
    void init_filter_array();
    void write_packed_filter(ZstdCompressor& compressor) const;
    auto read_packed_filter(ZstdDecompressor& decompressor) -> bool;
    
    static auto calculate_expansion_factor(
            size_t n,
            size_t max_attempts,
            double target_confidence
    ) -> double;

    // We will use m_filter_u8 as a raw bit-buffer to save space.
    // m_filter_u16 and m_filter_u32 will remain unused but present to preserve interface.
    std::vector<uint8_t> m_filter_u8;   
    std::vector<uint16_t> m_filter_u16; 
    std::vector<uint32_t> m_filter_u32; 
    
    size_t m_array_size{0};          
    size_t m_segment_length{0};      
    uint32_t m_fingerprint_bits{8};  
    uint32_t m_fingerprint_mask{0};  
    uint32_t m_seed{0};              
    
    std::vector<std::string> m_keys_buffer;
    std::unique_ptr<IFilterPolicy> m_policy;
};

}  // namespace clp_s

#endif  // CLP_S_BINARYFUSEFILTER_HPP