#include "BinaryFuseFilter.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <deque>
#include <set>
#include <stdexcept>
#include <string_view>
#include <tuple>
#include <vector>
#include <spdlog/spdlog.h>

#include "../../clp/ErrorCode.hpp"
#include "../../clp/ReaderInterface.hpp"
#include "../ErrorCode.hpp"
#include "../FileWriter.hpp"
#include "../ZstdCompressor.hpp"
#include "../ZstdDecompressor.hpp"
#include "ProbabilisticFilter.hpp"
#include "FilterPolicy/BinaryFuseFilterPolicy.hpp"

namespace clp_s {

// Helper: Fast 64-bit mixer (WyHash style) to avoid SHA256 overhead
static inline auto mix(uint64_t A, uint64_t B) -> uint64_t {
    __uint128_t r = A;
    r *= B;
    return static_cast<uint64_t>(r) ^ static_cast<uint64_t>(r >> 64);
}

// Helper: Fast range reduction (x % n) using (x * n) >> 64
static inline auto fast_range(uint64_t hash, uint64_t range) -> size_t {
    return static_cast<size_t>((static_cast<__uint128_t>(hash) * range) >> 64);
}

auto BinaryFuseFilter::calculate_expansion_factor(
        size_t n,
        size_t max_attempts,
        double target_confidence
) -> double {
    // OPTIMIZATION: Adjusted constants for tighter packing.
    // Partitioned XOR filters typically fail below 1.23x.
    // We set a hard floor to ensure stability while minimizing space.
    
    double const e_critical = 1.23; // Theoretical limit for 3-way XOR
    
    // Add small safety margin based on N (smaller N needs more margin)
    double const margin = (n < 10000) ? 0.02 : 0.005;
    
    double expansion = e_critical + margin;
    expansion = std::min(2.0, expansion); // Cap upper bound
    
    return expansion;
}

BinaryFuseFilter::BinaryFuseFilter(
        size_t expected_num_elements,
        double false_positive_rate,
        std::unique_ptr<IFilterPolicy> policy
) : m_policy(std::move(policy)) {
    if (expected_num_elements == 0) return;

    // Enforce minimum size for mathematical stability
    size_t const n = std::max(expected_num_elements, size_t{32});

    auto const params = m_policy->compute_parameters(false_positive_rate);
    m_fingerprint_bits = params.num_hash_functions;
    if (m_fingerprint_bits > 32) m_fingerprint_bits = 32;
    m_fingerprint_mask = (1u << m_fingerprint_bits) - 1;
    
    // Fixed max attempts
    size_t const max_attempts = 100;
    
    double expansion_factor = calculate_expansion_factor(n, max_attempts, 0.99);
    
    m_segment_length = static_cast<size_t>(std::ceil((n * expansion_factor) / 3.0));
    m_array_size = 3 * m_segment_length;
    
    SPDLOG_INFO("BinaryFuseFilter: n={}, bits={}, array_size={}", n, m_fingerprint_bits, m_array_size);
    
    init_filter_array();
}

void BinaryFuseFilter::init_filter_array() {
    // SPACE OPTIMIZATION: 
    // Instead of using u16/u32 and wasting padding bits, we force everything 
    // into m_filter_u8 acting as a raw bit-stream.
    // e.g., 10-bit fingerprints will take exactly 10 bits, not 16.
    
    size_t const total_bits = m_array_size * m_fingerprint_bits;
    size_t const total_bytes = (total_bits + 7) / 8;
    
    m_filter_u8.resize(total_bytes, 0);
    
    // Ensure others are empty
    m_filter_u16.clear();
    m_filter_u32.clear();
}

auto BinaryFuseFilter::get_filter_value(size_t pos) const -> uint32_t {
    // Read directly from bit-packed u8 array
    size_t const bit_index = pos * m_fingerprint_bits;
    size_t const byte_index = bit_index / 8;
    size_t const bit_offset = bit_index % 8;

    uint64_t raw_val = 0;
    
    // Read enough bytes (up to 5 bytes for 32-bit fp)
    // We treat the u8 vector as a continuous memory block
    size_t const bytes_available = m_filter_u8.size() - byte_index;
    size_t const bytes_to_read = std::min(bytes_available, size_t{5});

    // Safely reconstruct the uint64 word
    for (size_t i = 0; i < bytes_to_read; ++i) {
        raw_val |= static_cast<uint64_t>(m_filter_u8[byte_index + i]) << (8 * i);
    }

    return (raw_val >> bit_offset) & m_fingerprint_mask;
}

void BinaryFuseFilter::set_filter_value(size_t pos, uint32_t value) {
    // Write directly to bit-packed u8 array
    size_t const bit_index = pos * m_fingerprint_bits;
    size_t const byte_index = bit_index / 8;
    size_t const bit_offset = bit_index % 8;

    uint64_t val_64 = static_cast<uint64_t>(value) & m_fingerprint_mask;
    
    // Modify up to 5 bytes
    for (size_t i = 0; i < 5; ++i) {
        size_t idx = byte_index + i;
        if (idx >= m_filter_u8.size()) break;
        
        // Calculate which bits of 'value' go into this byte
        // and which bits of the byte need to be masked out
        
        // This is a simplified bit-stream write:
        // 1. Read byte
        // 2. Clear relevant bits
        // 3. OR in new bits
        
        uint64_t shift = i * 8;
        uint64_t current_byte_bits = val_64;
        
        if (shift > bit_offset) {
            current_byte_bits = val_64 >> (shift - bit_offset);
        } else {
             current_byte_bits = val_64 << (bit_offset - shift);
        }

        // Mask logic requires care; using a standard robust bit-set approach:
        // However, since we construct linearly or via peeling, we can just 
        // Read-Modify-Write the specific bits.
        
        // Let's do it bit by bit for safety within the loop if complex masking is error-prone,
        // or use the window approach:
    }

    // Robust Implementation:
    // We load a 64-bit window, modify it, and write it back.
    if (byte_index + 8 <= m_filter_u8.size()) {
        uint64_t window = 0;
        std::memcpy(&window, &m_filter_u8[byte_index], 8); // Safe memcpy
        
        uint64_t mask = (static_cast<uint64_t>(1) << m_fingerprint_bits) - 1;
        window &= ~(mask << bit_offset); // Clear
        window |= (val_64 << bit_offset); // Set
        
        std::memcpy(&m_filter_u8[byte_index], &window, 8);
    } else {
        // Boundary case: modify byte by byte
        size_t bits_remaining = m_fingerprint_bits;
        size_t current_bit = bit_offset;
        size_t current_byte = byte_index;
        
        uint32_t temp_val = value;
        
        while (bits_remaining > 0 && current_byte < m_filter_u8.size()) {
            size_t bits_in_this_byte = std::min(8 - current_bit, bits_remaining);
            uint8_t mask = ((1 << bits_in_this_byte) - 1) << current_bit;
            
            uint8_t byte_val = (temp_val & ((1 << bits_in_this_byte) - 1)) << current_bit;
            
            m_filter_u8[current_byte] &= ~mask;
            m_filter_u8[current_byte] |= byte_val;
            
            temp_val >>= bits_in_this_byte;
            bits_remaining -= bits_in_this_byte;
            current_byte++;
            current_bit = 0;
        }
    }
}

BinaryFuseFilter::BinaryFuseFilter(
        size_t expected_num_elements,
        double false_positive_rate
) : BinaryFuseFilter(
        expected_num_elements,
        false_positive_rate,
        std::make_unique<BinaryFuseFilterPolicy>()
) {}

BinaryFuseFilter::BinaryFuseFilter(
        absl::flat_hash_set<std::string> const& key_set,
        double false_positive_rate
) : BinaryFuseFilter(key_set.size(), false_positive_rate) {
    m_keys_buffer.reserve(key_set.size());
    for (auto const& key : key_set) {
        m_keys_buffer.push_back(key);
    }
    construct_filter();
    // Clear temporary buffer immediately to free memory
    std::vector<std::string>().swap(m_keys_buffer);
}

void BinaryFuseFilter::add(std::string_view value) {
    throw std::logic_error("BinaryFuseFilter::add - Filter is static.");
}

auto BinaryFuseFilter::possibly_contains(std::string_view value) const -> bool {
    if (m_filter_u8.empty()) return false;
    
    auto const [pos0, pos1, pos2, fp] = get_locations_and_fingerprint(value);
    
    // We can rely on get_filter_value handling the bit-unpacking
    uint32_t const result = get_filter_value(pos0) ^ get_filter_value(pos1) ^ get_filter_value(pos2);
    return result == fp;
}

auto BinaryFuseFilter::get_memory_usage() const -> size_t {
    return m_filter_u8.size();
}

auto BinaryFuseFilter::clone() const -> std::unique_ptr<IProbabilisticFilter> {
    auto copy = std::make_unique<BinaryFuseFilter>();
    copy->m_filter_u8 = m_filter_u8;
    // u16/u32 are empty but copied for completeness
    copy->m_filter_u16 = m_filter_u16; 
    copy->m_filter_u32 = m_filter_u32;
    copy->m_array_size = m_array_size;
    copy->m_segment_length = m_segment_length;
    copy->m_fingerprint_bits = m_fingerprint_bits;
    copy->m_fingerprint_mask = m_fingerprint_mask;
    copy->m_seed = m_seed;
    if (m_policy) {
        copy->m_policy = m_policy->clone();
    }
    return copy;
}

void BinaryFuseFilter::construct_filter() {
    if (m_keys_buffer.empty()) return;
    
    size_t const n = m_keys_buffer.size();
    size_t const max_attempts = 500; // Increased limit
    
    // Try different seeds
    for (m_seed = 0; m_seed < max_attempts; ++m_seed) {
        if (try_construct()) {
            SPDLOG_INFO("BinaryFuseFilter: Construction succeeded with seed {}", m_seed);
            return;
        }
    }
    
    SPDLOG_ERROR("BinaryFuseFilter: Construction failed after {} attempts.", max_attempts);
    throw std::runtime_error("BinaryFuseFilter construction failed. Dataset may contain duplicates or is too small.");
}

auto BinaryFuseFilter::try_construct() -> bool {
    size_t const n = m_keys_buffer.size();
    
    // Reset bit-array
    std::fill(m_filter_u8.begin(), m_filter_u8.end(), 0);
    
    // Phase 1: Compute hashes
    struct HashData { size_t p0, p1, p2; uint32_t fp; };
    std::vector<HashData> hashes;
    hashes.reserve(n);
    
    for (auto const& key : m_keys_buffer) {
        auto [p0, p1, p2, fp] = get_locations_and_fingerprint(key);
        hashes.push_back({p0, p1, p2, fp});
    }
    
    // Phase 2: Build reverse index / XOR counts
    // Using simple vectors here for speed
    std::vector<uint8_t> counts(m_array_size, 0);
    std::vector<uint64_t> xor_keys(m_array_size, 0); // XOR of key indices
    
    for (size_t i = 0; i < n; ++i) {
        auto const& h = hashes[i];
        counts[h.p0]++; xor_keys[h.p0] ^= i;
        counts[h.p1]++; xor_keys[h.p1] ^= i;
        counts[h.p2]++; xor_keys[h.p2] ^= i;
    }
    
    // Phase 3: Peeling
    std::vector<size_t> q;
    q.reserve(m_array_size);
    for (size_t i = 0; i < m_array_size; ++i) {
        if (counts[i] == 1) q.push_back(i);
    }
    
    std::vector<std::pair<size_t, size_t>> stack;
    stack.reserve(n);
    
    size_t head = 0;
    while (head < q.size()) {
        size_t const pos = q[head++];
        if (counts[pos] == 1) {
            size_t const k = xor_keys[pos];
            stack.emplace_back(k, pos);
            
            auto const& h = hashes[k];
            // Update neighbors
            for (size_t p : {h.p0, h.p1, h.p2}) {
                xor_keys[p] ^= k;
                counts[p]--;
                if (counts[p] == 1) q.push_back(p);
            }
        }
    }
    
    if (stack.size() != n) return false;
    
    // Phase 4: Assignment
    for (auto it = stack.rbegin(); it != stack.rend(); ++it) {
        auto const [k, pos] = *it;
        auto const& h = hashes[k];
        
        uint32_t const xor_val = get_filter_value(h.p0) ^ get_filter_value(h.p1) ^ get_filter_value(h.p2);
        set_filter_value(pos, h.fp ^ xor_val);
    }
    
    return true;
}

auto BinaryFuseFilter::hash_key(std::string_view key, uint32_t seed) const -> uint64_t {
    // OPTIMIZATION: Zero-allocation hashing
    uint64_t h = static_cast<uint64_t>(seed) ^ 0x9E3779B97F4A7C15ULL;
    for (char c : key) {
        h ^= static_cast<uint8_t>(c);
        h = mix(h, 0xbf58476d1ce4e5b9ULL);
    }
    return mix(h, 0x94d049bb133111ebULL);
}

auto BinaryFuseFilter::get_locations_and_fingerprint(std::string_view key) const
        -> std::tuple<size_t, size_t, size_t, uint32_t> {
    
    uint64_t const h = hash_key(key, m_seed);
    
    uint32_t fp = static_cast<uint32_t>(h & m_fingerprint_mask);
    if (fp == 0) fp = 1; // Fix zero fingerprint bias
    
    uint64_t const h1 = (h >> 21) | (h << 43);
    uint64_t const h2 = (h >> 42) | (h << 22);
    
    // OPTIMIZATION: fast_range is much faster than modulo (%)
    size_t const pos0 = fast_range(h, m_segment_length);
    size_t const pos1 = fast_range(h1, m_segment_length) + m_segment_length;
    size_t const pos2 = fast_range(h2, m_segment_length) + 2 * m_segment_length;
    
    return {pos0, pos1, pos2, fp};
}

void BinaryFuseFilter::write_packed_filter(ZstdCompressor& compressor) const {
    // The data is ALREADY packed in m_filter_u8.
    // We just write it directly.
    
    uint64_t packed_size = m_filter_u8.size();
    compressor.write_numeric_value<uint64_t>(packed_size);
    
    if (packed_size > 0) {
        compressor.write(
            reinterpret_cast<char const*>(m_filter_u8.data()),
            packed_size
        );
    }
}

auto BinaryFuseFilter::read_packed_filter(ZstdDecompressor& decompressor) -> bool {
    // Read directly into m_filter_u8
    uint64_t packed_size = 0;
    if (ErrorCodeSuccess != decompressor.try_read_numeric_value(packed_size)) {
        return false;
    }
    
    m_filter_u8.resize(packed_size);
    m_filter_u16.clear();
    m_filter_u32.clear();
    
    if (packed_size > 0) {
        size_t num_bytes_read = 0;
        if (ErrorCodeSuccess != decompressor.try_read(
                reinterpret_cast<char*>(m_filter_u8.data()),
                packed_size,
                num_bytes_read
        )) {
            return false;
        }
        if (num_bytes_read != packed_size) return false;
    }
    
    return true;
}

void BinaryFuseFilter::write_to_file(
        FileWriter& file_writer,
        ZstdCompressor& compressor
) const {
    compressor.write_numeric_value<uint8_t>(static_cast<uint8_t>(FilterType::BinaryFuse));
    compressor.write_numeric_value<uint32_t>(m_fingerprint_bits);
    compressor.write_numeric_value<uint32_t>(m_seed);
    compressor.write_numeric_value<uint64_t>(static_cast<uint64_t>(m_array_size));
    compressor.write_numeric_value<uint64_t>(static_cast<uint64_t>(m_segment_length));
    
    write_packed_filter(compressor);
}

auto BinaryFuseFilter::read_from_file(
        clp::ReaderInterface& reader,
        ZstdDecompressor& decompressor
) -> bool {
    if (ErrorCodeSuccess != decompressor.try_read_numeric_value(m_fingerprint_bits)) return false;
    m_fingerprint_mask = (1u << m_fingerprint_bits) - 1;
    
    if (ErrorCodeSuccess != decompressor.try_read_numeric_value(m_seed)) return false;
    
    uint64_t array_size_u64 = 0;
    if (ErrorCodeSuccess != decompressor.try_read_numeric_value(array_size_u64)) return false;
    m_array_size = static_cast<size_t>(array_size_u64);
    
    uint64_t segment_length_u64 = 0;
    if (ErrorCodeSuccess != decompressor.try_read_numeric_value(segment_length_u64)) return false;
    m_segment_length = static_cast<size_t>(segment_length_u64);
    
    return read_packed_filter(decompressor);
}

}  // namespace clp_s