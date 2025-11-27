#include "PrefixSuffixFilter.hpp"

#include <algorithm>
#include <cmath>
#include <string>
#include <vector>

#include "../ErrorCode.hpp"
#include "../ZstdCompressor.hpp"
#include "../ZstdDecompressor.hpp"

namespace clp_s {

PrefixSuffixFilter::PrefixSuffixFilter(
    size_t expected_num_elements,
    double false_positive_rate,
    size_t avg_key_length
) {
    // 1. Estimate how many items we will actually insert.
    //    Unlike the n-gram failure, we calculate the total capacity needed upfront.
    //    Items = KeyCount * (AvgLength / Stride)
    
    size_t estimated_items_per_key = 0;
    if (avg_key_length > kMinLength) {
        estimated_items_per_key = (avg_key_length - kMinLength) / kStride + 1;
    } else {
        estimated_items_per_key = 1; 
    }

    // We assume forward and reverse have roughly same number of insertions
    size_t total_capacity = expected_num_elements * estimated_items_per_key;

    // 2. Initialize internal filters with the CORRECT capacity
    m_forward_filter = std::make_unique<BloomFilter>(total_capacity, false_positive_rate);
    m_reverse_filter = std::make_unique<BloomFilter>(total_capacity, false_positive_rate);
}

PrefixSuffixFilter::PrefixSuffixFilter(
    absl::flat_hash_set<std::string> const& key_set,
    double false_positive_rate
) {
    // Calculate exact needed capacity by iterating once
    size_t total_items = 0;
    for (const auto& key : key_set) {
        if (key.length() >= kMinLength) {
            total_items += (key.length() - kMinLength) / kStride + 1;
        }
    }
    // Fallback for very small keys
    if (total_items == 0) total_items = key_set.size();

    m_forward_filter = std::make_unique<BloomFilter>(total_items, false_positive_rate);
    m_reverse_filter = std::make_unique<BloomFilter>(total_items, false_positive_rate);

    for (const auto& key : key_set) {
        add(key);
    }
}

void PrefixSuffixFilter::add(std::string_view value) {
    if (value.empty()) return;

    // 1. Add to Forward Filter (Prefixes)
    add_prefixes(value, *m_forward_filter);

    // 2. Add to Reverse Filter (Suffixes)
    std::string reversed(value);
    std::reverse(reversed.begin(), reversed.end());
    add_prefixes(reversed, *m_reverse_filter);
}

void PrefixSuffixFilter::add_prefixes(std::string_view value, BloomFilter& filter) {
    // If the value is shorter than min length, we might want to insert the exact match
    // to ensure it is found.
    if (value.length() < kMinLength) {
        filter.add(value);
        return;
    }

    // Insert prefixes: "abcde" -> "abc", "abcd", "abcde" (if stride=1, min=3)
    for (size_t len = kMinLength; len <= value.length(); len += kStride) {
        filter.add(value.substr(0, len));
    }
    
    // Always insert the full string if the stride skipped it
    if ((value.length() - kMinLength) % kStride != 0) {
        filter.add(value);
    }
}

auto PrefixSuffixFilter::possibly_contains(std::string_view value) const -> bool {
    // The input `value` contains wildcards (e.g., "sys*err", "*fail", "warn*")
    // We need to parse it to decide which filter to check.
    
    if (is_empty()) return false;

    bool has_start_wildcard = (!value.empty() && value.front() == '*');
    bool has_end_wildcard = (!value.empty() && value.back() == '*');

    // 1. Case: *infix* (Wildcard at both ends) -> "contains substring"
    // Bloom filters cannot handle arbitrary substring search unless we used n-grams.
    // With Prefix/Suffix filters, we MUST return true (fallback to dictionary scan).
    if (has_start_wildcard && has_end_wildcard) {
        return true; 
    }

    // 2. Case: *suffix (Ends with string) -> Reverse check
    if (has_start_wildcard) {
        // Remove leading '*'
        std::string_view suffix = value.substr(1);
        std::string reversed_suffix(suffix);
        std::reverse(reversed_suffix.begin(), reversed_suffix.end());
        
        return check_prefix(reversed_suffix, *m_reverse_filter);
    }

    // 3. Case: prefix* (Starts with string) -> Forward check
    if (has_end_wildcard) {
        // Remove trailing '*'
        std::string_view prefix = value.substr(0, value.length() - 1);
        return check_prefix(prefix, *m_forward_filter);
    }

    // 4. Case: Exact match (No wildcards)
    // Check forward filter for exact string
    return check_prefix(value, *m_forward_filter);
    
    // Note: 'prefix*suffix' case is complex because the split point is unknown.
    // Standard approach: return true (fallback). 
    // Optimization: Check if 'prefix' is in Forward AND 'suffix' (reversed) is in Reverse.
    // If either is missing, the file definitely doesn't contain it.
}

auto PrefixSuffixFilter::check_prefix(std::string_view value, BloomFilter const& filter) const -> bool {
    // If the query is shorter than what we indexed (kMinLength), we can't rely on the filter 
    // unless we specifically handled short keys. 
    // Assuming we indexed short keys exactly in add_prefixes:
    return filter.possibly_contains(value);
}

void PrefixSuffixFilter::write_to_file(FileWriter& file_writer, ZstdCompressor& compressor) const {
    // Header
    compressor.write_numeric_value<uint8_t>(static_cast<uint8_t>(FilterType::PrefixSuffix));
    
    // Forward
    m_forward_filter->write_to_file(file_writer, compressor);
    // Reverse
    m_reverse_filter->write_to_file(file_writer, compressor);
}

auto PrefixSuffixFilter::read_from_file(clp::ReaderInterface& reader, ZstdDecompressor& decompressor) -> bool {
    // Note: Type byte is read by caller (ProbabilisticFilter::create_from_file)
    
    m_forward_filter = std::make_unique<BloomFilter>();
    // BloomFilter::read_from_file expects to read the header (num hashes, size), 
    // but NOT the type byte if it was wrapped.
    // However, looking at your BloomFilter::read_from_file, it DOES NOT read the type byte.
    // But BloomFilter::write_to_file DOES write the type byte.
    // This implies we need to consume the type byte for the internal filters 
    // because write_to_file wrote it.
    
    // Consume Type Byte for Forward Filter
    uint8_t type;
    if (decompressor.try_read_numeric_value(type) != ErrorCodeSuccess) return false;
    if (static_cast<FilterType>(type) != FilterType::Bloom) return false;
    
    if (!m_forward_filter->read_from_file(reader, decompressor)) return false;

    // Consume Type Byte for Reverse Filter
    m_reverse_filter = std::make_unique<BloomFilter>();
    if (decompressor.try_read_numeric_value(type) != ErrorCodeSuccess) return false;
    if (static_cast<FilterType>(type) != FilterType::Bloom) return false;

    if (!m_reverse_filter->read_from_file(reader, decompressor)) return false;

    return true;
}

auto PrefixSuffixFilter::is_empty() const -> bool {
    return (!m_forward_filter || m_forward_filter->is_empty());
}

auto PrefixSuffixFilter::get_memory_usage() const -> size_t {
    size_t total = 0;
    if (m_forward_filter) total += m_forward_filter->get_memory_usage();
    if (m_reverse_filter) total += m_reverse_filter->get_memory_usage();
    return total;
}

auto PrefixSuffixFilter::clone() const -> std::unique_ptr<IProbabilisticFilter> {
    auto copy = std::make_unique<PrefixSuffixFilter>();
    if (m_forward_filter) {
        // BloomFilter::clone returns unique_ptr<IProbabilisticFilter>, cast it back
        auto base_clone = m_forward_filter->clone();
        copy->m_forward_filter.reset(static_cast<BloomFilter*>(base_clone.release()));
    }
    if (m_reverse_filter) {
        auto base_clone = m_reverse_filter->clone();
        copy->m_reverse_filter.reset(static_cast<BloomFilter*>(base_clone.release()));
    }
    return copy;
}

} // namespace clp_s