#include "BloomFilter.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>
#include <absl/container/flat_hash_set.h>

#include "../../clp/ErrorCode.hpp"
#include "../../clp/hash_utils.hpp"
#include "../../clp/ReaderInterface.hpp"
#include "../ErrorCode.hpp"
#include "../FileWriter.hpp"
#include "../ZstdCompressor.hpp"
#include "../ZstdDecompressor.hpp"
#include "ProbabilisticFilter.hpp"

namespace clp_s {
// BloomFilter.cpp - Enforce minimum filter size

BloomFilter::BloomFilter(
    size_t expected_num_elements,
    double false_positive_rate,
    std::unique_ptr<IFilterPolicy> policy
) : m_policy(std::move(policy)) {
if (expected_num_elements == 0) {
    return;  // Empty filter
}

auto const params = m_policy->compute_parameters(false_positive_rate);

m_bit_array_size = static_cast<size_t>(
        std::ceil(params.bits_per_key * expected_num_elements)
);

// **CRITICAL FIX**: Ensure minimum 1 byte (8 bits) per filter
// This prevents 0-sized filters that cause segfaults
m_bit_array_size = std::max(m_bit_array_size, size_t{8});

m_num_hash_functions = params.num_hash_functions;

// Round up to nearest byte
size_t const num_bytes = (m_bit_array_size + 7) / 8;
m_bit_array.resize(num_bytes, 0);
}

BloomFilter::BloomFilter(size_t expected_num_elements, double false_positive_rate)
    : BloomFilter(
            expected_num_elements,
            false_positive_rate,
            std::make_unique<BloomFilterPolicy>()
    ) {}

BloomFilter::BloomFilter(
        absl::flat_hash_set<std::string> const& key_set,
        double false_positive_rate
) : BloomFilter(key_set.size(), false_positive_rate) {

    std::map<size_t, size_t> length_histogram;

    for (auto const& key : key_set) {
        add(key);
    }
}

void BloomFilter::add(std::string_view value) {
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
            return false;  // Definitely not in the set
        }
    }

    return true;  // Possibly in the set
}

void BloomFilter::generate_hash_values(std::string_view value, std::vector<size_t>& hash_values)
        const {
    hash_values.clear();
    hash_values.reserve(m_num_hash_functions);

    // Use double hashing technique to generate k hash functions from 2 hashes
    // h_i(x) = (h1(x) + i * h2(x)) mod m
    // This is a well-known technique that provides good hash independence

    std::vector<unsigned char> hash1;
    std::vector<unsigned char> hash2;

    // Generate first hash using SHA256 of the value
    auto const* value_bytes = reinterpret_cast<unsigned char const*>(value.data());
    std::span<unsigned char const> value_span(value_bytes, value.size());

    if (clp::ErrorCode_Success != clp::get_sha256_hash(value_span, hash1)) {
        // Fallback to simple hash if SHA256 fails
        hash1.resize(sizeof(size_t));
        size_t simple_hash = 0;
        for (char c : value) {
            simple_hash = simple_hash * 31 + static_cast<unsigned char>(c);
        }
        std::memcpy(hash1.data(), &simple_hash, sizeof(size_t));
    }

    // Generate second hash using SHA256 of (value + salt)
    std::string salted_value;
    salted_value.reserve(value.size() + 8);
    salted_value.append(value);
    salted_value.append("_bloom_");  // Salt to make hash different

    auto const* salted_bytes = reinterpret_cast<unsigned char const*>(salted_value.data());
    std::span<unsigned char const> salted_span(salted_bytes, salted_value.size());

    if (clp::ErrorCode_Success != clp::get_sha256_hash(salted_span, hash2)) {
        // Fallback to simple hash
        hash2.resize(sizeof(size_t));
        size_t simple_hash = 1;
        for (char c : salted_value) {
            simple_hash = simple_hash * 31 + static_cast<unsigned char>(c);
        }
        std::memcpy(hash2.data(), &simple_hash, sizeof(size_t));
    }

    // Convert hash bytes to size_t values
    size_t h1 = 0;
    size_t h2 = 0;

    // Use first 8 bytes of each hash
    size_t const bytes_to_use = std::min(sizeof(size_t), hash1.size());
    std::memcpy(&h1, hash1.data(), bytes_to_use);
    std::memcpy(&h2, hash2.data(), bytes_to_use);

    // Generate k hash values using double hashing
    for (uint32_t i = 0; i < m_num_hash_functions; ++i) {
        size_t hash_value = h1 + i * h2;
        hash_values.push_back(hash_value);
    }
}

void BloomFilter::set_bit(size_t bit_index) {
    size_t const byte_index = bit_index / 8;
    size_t const bit_offset = bit_index % 8;
    m_bit_array[byte_index] |= (1u << bit_offset);
}

auto BloomFilter::test_bit(size_t bit_index) const -> bool {
    size_t const byte_index = bit_index / 8;
    size_t const bit_offset = bit_index % 8;
    return (m_bit_array[byte_index] & (1u << bit_offset)) != 0;
}

void BloomFilter::write_to_file(FileWriter& file_writer, ZstdCompressor& compressor) const {
    // Write filter type
    compressor.write_numeric_value<uint8_t>(static_cast<uint8_t>(FilterType::Bloom));

    // Write header:
    // - uint32_t: number of hash functions
    // - uint64_t: bit array size in bits
    // - uint64_t: bit array size in bytes

    compressor.write_numeric_value<uint32_t>(m_num_hash_functions);
    compressor.write_numeric_value<uint64_t>(static_cast<uint64_t>(m_bit_array_size));
    compressor.write_numeric_value<uint64_t>(static_cast<uint64_t>(m_bit_array.size()));

    // Write bit array
    if (!m_bit_array.empty()) {
        compressor.write(reinterpret_cast<char const*>(m_bit_array.data()), m_bit_array.size());
    }
}

auto BloomFilter::read_from_file(clp::ReaderInterface& reader, ZstdDecompressor& decompressor)
        -> bool {
    // Read header
    if (ErrorCodeSuccess != decompressor.try_read_numeric_value(m_num_hash_functions)) {
        return false;
    }

    uint64_t bit_array_size_u64 = 0;
    if (ErrorCodeSuccess != decompressor.try_read_numeric_value(bit_array_size_u64)) {
        return false;
    }
    m_bit_array_size = static_cast<size_t>(bit_array_size_u64);

    uint64_t bit_array_bytes = 0;
    if (ErrorCodeSuccess != decompressor.try_read_numeric_value(bit_array_bytes)) {
        return false;
    }

    // Read bit array
    m_bit_array.resize(static_cast<size_t>(bit_array_bytes));
    if (!m_bit_array.empty()) {
        size_t num_bytes_read = 0;
        if (ErrorCodeSuccess
            != decompressor.try_read(
                    reinterpret_cast<char*>(m_bit_array.data()),
                    bit_array_bytes,
                    num_bytes_read
            ))
        {
            return false;
        }
        if (num_bytes_read != bit_array_bytes) {
            return false;
        }
    }

    return true;
}
}  // namespace clp_s
