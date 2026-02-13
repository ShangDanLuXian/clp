#ifndef CLP_S_BLOOM_FILTER_HPP
#define CLP_S_BLOOM_FILTER_HPP

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

#include "../FileWriter.hpp"
#include "../ErrorCode.hpp"
#include "../TraceableException.hpp"
#include "ProbabilisticFilter.hpp"

namespace clp {
class ReaderInterface;
}

namespace clp_s::filter {
class BloomFilter : public IProbabilisticFilter {
public:
    class OperationFailed : public TraceableException {
    public:
        OperationFailed(ErrorCode error_code, char const* const filename, int line_number)
                : TraceableException(error_code, filename, line_number) {}
    };

    BloomFilter(size_t expected_num_elements, double false_positive_rate);

    BloomFilter() = default;
    BloomFilter(BloomFilter const&) = default;
    BloomFilter(BloomFilter&&) noexcept = default;
    auto operator=(BloomFilter const&) -> BloomFilter& = default;
    auto operator=(BloomFilter&&) noexcept -> BloomFilter& = default;

    void add(std::string_view value) override;
    [[nodiscard]] auto possibly_contains(std::string_view value) const -> bool override;

    void write_to_file(FileWriter& writer) const override;
    auto read_from_file(clp::ReaderInterface& reader) -> bool override;

    [[nodiscard]] auto is_empty() const -> bool override { return m_bit_array.empty(); }
    [[nodiscard]] auto get_type() const -> FilterType override { return FilterType::BloomV1; }
    [[nodiscard]] auto get_memory_usage() const -> size_t override { return m_bit_array.size(); }
    [[nodiscard]] auto clone() const -> std::unique_ptr<IProbabilisticFilter> override {
        return std::make_unique<BloomFilter>(*this);
    }

private:
    [[nodiscard]] static auto compute_optimal_parameters(
            size_t expected_num_elements,
            double false_positive_rate
    ) -> std::pair<size_t, uint32_t>;

    void generate_hash_values(std::string_view value, std::vector<size_t>& hash_values) const;

    void set_bit(size_t bit_index);
    [[nodiscard]] auto test_bit(size_t bit_index) const -> bool;

    size_t m_bit_array_size{0};
    uint32_t m_num_hash_functions{0};
    std::vector<uint8_t> m_bit_array;
};
}  // namespace clp_s::filter

#endif  // CLP_S_BLOOM_FILTER_HPP
