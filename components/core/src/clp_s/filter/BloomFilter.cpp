#include "BloomFilter.hpp"

#include <algorithm>
#include <cmath>
#include <cstring>

#include "../clp/hash_utils.hpp"
#include "../clp/ErrorCode.hpp"
#include "../clp/ReaderInterface.hpp"

namespace clp_s::filter {
BloomFilter::BloomFilter(size_t expected_num_elements, double false_positive_rate) {
    auto [bit_array_size, num_hash_functions]
            = compute_optimal_parameters(expected_num_elements, false_positive_rate);

    m_bit_array_size = bit_array_size;
    m_num_hash_functions = num_hash_functions;

    size_t const num_bytes = (m_bit_array_size + 7) / 8;
    m_bit_array.resize(num_bytes, 0);
}

auto BloomFilter::compute_optimal_parameters(
        size_t expected_num_elements,
        double false_positive_rate
) -> std::pair<size_t, uint32_t> {
    if (expected_num_elements == 0 || false_positive_rate <= 0.0
        || false_positive_rate >= 1.0)
    {
        return {64, 1};
    }

    double const ln2 = std::log(2.0);
    double const ln2_squared = ln2 * ln2;
    auto const bit_array_size = static_cast<size_t>(
            -static_cast<double>(expected_num_elements) * std::log(false_positive_rate)
            / ln2_squared
    );

    auto const num_hash_functions = static_cast<uint32_t>(
            static_cast<double>(bit_array_size) / static_cast<double>(expected_num_elements)
            * ln2
    );

    uint32_t const capped_num_hash_functions
            = std::max(1u, std::min(20u, num_hash_functions));

    return {bit_array_size, capped_num_hash_functions};
}

void BloomFilter::add(std::string_view value) {
    if (m_bit_array.empty()) {
        return;
    }

    std::vector<size_t> hash_values;
    generate_hash_values(value, hash_values);

    for (auto const hash_value : hash_values) {
        set_bit(hash_value % m_bit_array_size);
    }
}

auto BloomFilter::possibly_contains(std::string_view value) const -> bool {
    if (m_bit_array.empty()) {
        return false;
    }

    std::vector<size_t> hash_values;
    generate_hash_values(value, hash_values);

    for (auto const hash_value : hash_values) {
        if (!test_bit(hash_value % m_bit_array_size)) {
            return false;
        }
    }

    return true;
}

void BloomFilter::generate_hash_values(
        std::string_view value,
        std::vector<size_t>& hash_values
) const {
    hash_values.clear();
    hash_values.reserve(m_num_hash_functions);

    std::vector<unsigned char> hash1;
    std::vector<unsigned char> hash2;

    auto const* value_bytes = reinterpret_cast<unsigned char const*>(value.data());
    std::span<unsigned char const> value_span(value_bytes, value.size());

    if (clp::ErrorCode_Success != clp::get_sha256_hash(value_span, hash1)) {
        hash1.resize(sizeof(size_t));
        size_t simple_hash = 0;
        for (char c : value) {
            simple_hash = simple_hash * 31 + static_cast<unsigned char>(c);
        }
        std::memcpy(hash1.data(), &simple_hash, sizeof(size_t));
    }

    std::string salted_value;
    salted_value.reserve(value.size() + 8);
    salted_value.append(value);
    salted_value.append("_bloom_");

    auto const* salted_bytes = reinterpret_cast<unsigned char const*>(salted_value.data());
    std::span<unsigned char const> salted_span(salted_bytes, salted_value.size());

    if (clp::ErrorCode_Success != clp::get_sha256_hash(salted_span, hash2)) {
        hash2.resize(sizeof(size_t));
        size_t simple_hash = 1;
        for (char c : salted_value) {
            simple_hash = simple_hash * 31 + static_cast<unsigned char>(c);
        }
        std::memcpy(hash2.data(), &simple_hash, sizeof(size_t));
    }

    size_t h1 = 0;
    size_t h2 = 0;
    size_t const bytes_to_use = std::min(sizeof(size_t), hash1.size());
    std::memcpy(&h1, hash1.data(), bytes_to_use);
    std::memcpy(&h2, hash2.data(), bytes_to_use);

    for (uint32_t i = 0; i < m_num_hash_functions; ++i) {
        size_t hash_value = h1 + i * h2;
        hash_values.push_back(hash_value);
    }
}

void BloomFilter::set_bit(size_t bit_index) {
    size_t const byte_index = bit_index / 8;
    size_t const bit_offset = bit_index % 8;
    m_bit_array[byte_index] |= static_cast<uint8_t>(1u << bit_offset);
}

auto BloomFilter::test_bit(size_t bit_index) const -> bool {
    size_t const byte_index = bit_index / 8;
    size_t const bit_offset = bit_index % 8;
    return (m_bit_array[byte_index] & static_cast<uint8_t>(1u << bit_offset)) != 0;
}

void BloomFilter::write_to_file(FileWriter& writer) const {
    writer.write_numeric_value<uint32_t>(m_num_hash_functions);
    writer.write_numeric_value<uint64_t>(static_cast<uint64_t>(m_bit_array_size));
    writer.write_numeric_value<uint64_t>(static_cast<uint64_t>(m_bit_array.size()));
    if (!m_bit_array.empty()) {
        writer.write(reinterpret_cast<char const*>(m_bit_array.data()), m_bit_array.size());
    }
}

auto BloomFilter::read_from_file(clp::ReaderInterface& reader) -> bool {
    if (clp::ErrorCode_Success != reader.try_read_numeric_value(m_num_hash_functions)) {
        return false;
    }

    uint64_t bit_array_size_u64 = 0;
    if (clp::ErrorCode_Success != reader.try_read_numeric_value(bit_array_size_u64)) {
        return false;
    }
    m_bit_array_size = static_cast<size_t>(bit_array_size_u64);

    uint64_t bit_array_bytes = 0;
    if (clp::ErrorCode_Success != reader.try_read_numeric_value(bit_array_bytes)) {
        return false;
    }

    m_bit_array.resize(static_cast<size_t>(bit_array_bytes));
    if (!m_bit_array.empty()) {
        if (clp::ErrorCode_Success
            != reader.try_read_exact_length(
                    reinterpret_cast<char*>(m_bit_array.data()),
                    static_cast<size_t>(bit_array_bytes)
            ))
        {
            return false;
        }
    }

    return true;
}
}  // namespace clp_s::filter
