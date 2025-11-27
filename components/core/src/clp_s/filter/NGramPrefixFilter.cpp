#include "NGramPrefixFilter.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <spdlog/spdlog.h>

#include "../../clp/ErrorCode.hpp"
#include "../../clp/hash_utils.hpp"
#include "../../clp/ReaderInterface.hpp"
#include "../ErrorCode.hpp"
#include "../FileWriter.hpp"
#include "../ZstdCompressor.hpp"
#include "../ZstdDecompressor.hpp"
#include "ProbabilisticFilter.hpp"

namespace clp_s {
NGramPrefixFilter::NGramPrefixFilter(double false_positive_rate) {
}

NGramPrefixFilter::NGramPrefixFilter(absl::flat_hash_set<std::string> const &key_set, double false_positive_rate) {


    for (auto const &value : key_set) {
        add(value);
    }
    calculate_n();
    extract_ngrams();
    construct_filters(false_positive_rate);

}

void NGramPrefixFilter::add(std::string_view value) {
    m_length_key_map[value.length()].insert(std::string(value)); 
}

void NGramPrefixFilter::calculate_n() {
    // 1. Aggregate stats
    size_t total_length = 0;
    size_t key_count = 0;

    for (auto const& [length, set] : m_length_key_map) {
        total_length += length * set.size();
        key_count   += set.size();
    }

    if (key_count == 0) {
        m_n = 0;  // or 1, or some sentinel
        return;
    }

    double average_key_length
            = static_cast<double>(total_length) / static_cast<double>(key_count);

    // 2. Use your design knobs
    double T = m_target_collision_rate;
    double A = static_cast<double>(m_alphabet_size);

    // Sanity checks
    if (T <= 0.0 || T >= 1.0 || A <= 1.0) {
        m_n = 0;  // or fall back
        return;
    }

    // 3. Approximate n from T, K, A
    double denom = -std::log(1.0 - T);      // -ln(1 - T) > 0
    double A_pow_n = static_cast<double>(key_count) / denom;
    double n_real = std::log(A_pow_n) / std::log(A);

    int n_int = static_cast<int>(std::round(n_real));

    // Clamp between 1 and average key length (or some min length)
    n_int = std::max(1, n_int);
    n_int = std::min(n_int, static_cast<int>(std::floor(average_key_length)));

    m_n = n_int;
}

void NGramPrefixFilter::extract_ngrams() {
    if (m_n == 0) {
        return;
    }

    for (auto const& [length, key_set] : m_length_key_map) {
        if (length < m_n) {
            continue;
        }

        auto& ngram_set = m_length_n_gram_map[static_cast<uint32_t>(length)];

        for (auto const& key : key_set) {

            for (size_t pos = 0; pos + m_n <= key.size(); ++pos) {
                std::string ngram = key.substr(pos, m_n);
                ngram_set.insert(std::move(ngram));
            }
        }
    }
}

void NGramPrefixFilter::construct_filters(double false_positive_rate) {
    for (auto const& [length, key_set] : m_length_key_map) {
        if (length < m_n) {
            m_length_filter_map[length] = ProbabilisticFilter(
                internal_filter_type, key_set, false_positive_rate);
        } else {
            auto it = m_length_n_gram_map.find(length);
            if (it == m_length_n_gram_map.end() || it->second.empty()) {
                // Fallback to full-key filter
                m_length_filter_map[length] = ProbabilisticFilter(
                    internal_filter_type, key_set, false_positive_rate);
                continue;
            }
            
            auto const& ngram_set = it->second;
            
            // **CRITICAL**: Check if filter would be too small
            double bits_per_key = BloomFilterPolicy::compute_bits_per_key(false_positive_rate);
            double total_bits = bits_per_key * static_cast<double>(key_set.size());
            double bits_per_ngram = total_bits / static_cast<double>(ngram_set.size());
            
            // // Minimum viable: 4 bits per n-gram to avoid saturation
            // constexpr double min_bpn = 4.0;
            // if (bits_per_ngram < min_bpn) {
            //     SPDLOG_WARN("Length {}: Only {:.2f} bpn < {:.1f} threshold. "
            //                "Filter would be saturated. Using full-key filter.",
            //                length, bits_per_ngram, min_bpn);
            //     m_length_filter_map[length] = ProbabilisticFilter(
            //         internal_filter_type, key_set, false_positive_rate);
            //     continue;
            // }
            
            double per_ngram_fpr = compute_per_ngram_fpr(
                false_positive_rate, length,
                static_cast<uint32_t>(key_set.size()),
                static_cast<uint32_t>(ngram_set.size())
            );
            
            m_length_filter_map[length] = ProbabilisticFilter(
                internal_filter_type, ngram_set, per_ngram_fpr);
        }
    }
}

auto NGramPrefixFilter::compute_per_ngram_fpr(
        double target_false_positive_rate,
        uint32_t length,
        uint32_t num_entries,
        uint32_t ngram_count
) -> double {
    // Sanity checks
    if (target_false_positive_rate <= 0.0 || target_false_positive_rate >= 1.0) {
        return target_false_positive_rate;
    }
    if (ngram_count == 0 || num_entries == 0) {
        return target_false_positive_rate;
    }

    // Step 1: Compute bits per key for target exact-match FPR
    double bits_per_key = BloomFilterPolicy::compute_bits_per_key(target_false_positive_rate);
    
    // Step 2: Total memory budget for this length class (same as full-key Bloom)
    double total_bits = bits_per_key * static_cast<double>(num_entries);
    
    // Step 3: Distribute budget evenly among unique n-grams
    double bits_per_ngram = total_bits / static_cast<double>(ngram_count);
    
    // Step 4: Compute per-n-gram FPR from bits per n-gram
    auto [num_hash_functions, per_ngram_fpr] = 
        BloomFilterPolicy::compute_fpr_from_bits_per_key(bits_per_ngram);
    
    // Optional: Log for debugging
    SPDLOG_INFO("Length {}: {} keys -> {} n-grams, {:.2f} bpk -> {:.2f} bpn, "
                 "k={}, per-ngram FPR={:.6f}",
                 length, num_entries, ngram_count, bits_per_key, bits_per_ngram,
                 num_hash_functions, per_ngram_fpr);
    
    return per_ngram_fpr;
}

auto NGramPrefixFilter::get_memory_usage() const -> size_t {
    size_t total = 0;
    for (auto const& [length, filter] : m_length_filter_map) {
        total += filter.get_memory_usage();
    }
    return total;
}






auto NGramPrefixFilter::possibly_contains(std::string_view value) const -> bool {
    auto it = m_length_filter_map.find(value.length());
    if (it == m_length_filter_map.end()) {
        SPDLOG_WARN("No filter for length {}", value.length());
        return false;
    }

    SPDLOG_INFO("Query: '{}', length={}, n={}", value, value.length(), m_n);

    auto const& filter = it->second;

    if (value.length() < m_n) {
        return filter.possibly_contains(value);
    }

    // Count n-grams checked
    size_t ngrams_checked = 0;
    for (size_t pos = 0; pos + m_n <= value.size(); ++pos) {
        std::string_view ngram = value.substr(pos, m_n);
        ngrams_checked++;
        if (!filter.possibly_contains(ngram)) {
            SPDLOG_INFO("Rejected at n-gram {}: '{}'", ngrams_checked, ngram);
            return false;
        }
    }
    
    SPDLOG_INFO("Passed all {} n-gram checks!", ngrams_checked);
    return true;
}



void NGramPrefixFilter::write_to_file(FileWriter& file_writer,
    ZstdCompressor& compressor) const {
    // 1. Top-level filter type
    compressor.write_numeric_value<uint8_t>(
    static_cast<uint8_t>(FilterType::NGramPrefix));

    // 2. Global n-gram length
    compressor.write_numeric_value<uint32_t>(static_cast<uint32_t>(m_n));

    // 3. Number of per-length filters
    uint32_t num_lengths = static_cast<uint32_t>(m_length_filter_map.size());
    compressor.write_numeric_value<uint32_t>(num_lengths);

    // 4. For each length, write the length and the inner filter
    for (auto const& [length, filter] : m_length_filter_map) {
    // key length for this class
    compressor.write_numeric_value<uint32_t>(static_cast<uint32_t>(length));

    // inner filter (Bloom today) – writes its own type + header + bits
    filter.write_to_file(file_writer, compressor);
    }
}



auto NGramPrefixFilter::read_from_file(clp::ReaderInterface& reader,
    ZstdDecompressor& decompressor) -> bool {
    // NOTE: The FilterType::NGramPrefix byte should already have been read
    // by ProbabilisticFilter::create_from_file before this is called,
    // just like with BloomFilter. So we start AFTER the type byte.

    // 1. Read global n-gram length (m_n)
    uint32_t n_u32 = 0;
    if (ErrorCodeSuccess != decompressor.try_read_numeric_value(n_u32)) {
    return false;
    }
    m_n = static_cast<int>(n_u32);

    // 2. Read number of per-length filters
    uint32_t num_lengths = 0;
    if (ErrorCodeSuccess != decompressor.try_read_numeric_value(num_lengths)) {
    return false;
    }

    m_length_filter_map.clear();
    // (optional) reserve if your map supports it:
    // m_length_filter_map.reserve(num_lengths);

    // 3. For each length class, read length + inner filter
    for (uint32_t i = 0; i < num_lengths; ++i) {
    uint32_t length_u32 = 0;
    if (ErrorCodeSuccess != decompressor.try_read_numeric_value(length_u32)) {
    return false;
    }
    size_t length = static_cast<size_t>(length_u32);

    // Use ProbabilisticFilter::create_from_file to read the nested filter.
    // That function should:
    //   - read the inner filter type byte
    //   - construct the right impl (Bloom, etc.)
    //   - call impl->read_from_file(...)
    ProbabilisticFilter inner =
    ProbabilisticFilter::create_from_file(reader, decompressor);

    // You may want some sanity check here if inner.get_type() is not what you expect.

    m_length_filter_map.emplace(length, std::move(inner));
    }

    return true;
}

}  // namespace clp_s
