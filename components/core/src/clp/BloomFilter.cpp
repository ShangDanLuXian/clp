#include "BloomFilter.hpp"

#include <fstream>
#include <functional>
#include "spdlog_with_specializations.hpp"


namespace clp {

BloomFilter::BloomFilter(size_t size, uint8_t num_hashes)
        : m_size(size),
          m_num_hashes(num_hashes),
          m_bits(size, false) {}

void BloomFilter::add(std::string const& item) {
    auto hashes = compute_hashes(item);
    for (auto hash : hashes) {
        m_bits[hash] = true;
    }
    ++m_num_items_added;
}

bool BloomFilter::might_contain(std::string const& item) const {
    auto hashes = compute_hashes(item);
    for (auto hash : hashes) {
        if (!m_bits[hash]) {
            return false;
        }
    }
    return true;
}

void BloomFilter::add_ngrams(std::string const& text, size_t n) {
    std::string normalized = normalize_string(text);
    
    if (normalized.length() < n) {
        // add(normalized);
        return;  // Too short for n-grams
    }
    
    // Extract and add all n-grams
    for (size_t i = 0; i + n <= normalized.length(); ++i) {
        add(normalized.substr(i, n));
    }
}

bool BloomFilter::might_contain_ngrams(std::string const& text, size_t n) const {
    std::string normalized = normalize_string(text);
    
    if (normalized.length() < n) {
        return true;
    }
    
    for (size_t i = 0; i + n <= normalized.length(); ++i) {
        std::string_view ngram_view(normalized.data() + i, n);  // Zero-copy view
        std::string ngram(ngram_view);  // Convert to string only for hashing
        
        if (!might_contain(ngram)) {
            return false;
        }
    }
    
    return true;
}

std::string BloomFilter::normalize_string(std::string const& str) {
    std::string result;
    result.reserve(str.length());
    
    for (char c : str) {
        if (std::isalnum(static_cast<unsigned char>(c)) || c == '_') {
            result += std::tolower(static_cast<unsigned char>(c));
        }
    }
    
    return result;
}

std::vector<size_t> BloomFilter::compute_hashes(std::string const& item) const {
    std::vector<size_t> hashes;
    hashes.reserve(m_num_hashes);
    
    std::hash<std::string> hasher;
    
    for (uint8_t i = 0; i < m_num_hashes; ++i) {
        std::string salted = item + std::to_string(i);
        size_t hash = hasher(salted) % m_size;
        hashes.push_back(hash);
    }
    
    return hashes;
}

bool BloomFilter::write_to_file(std::string const& filepath) const {
    std::ofstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        return false;
    }

    file.write(reinterpret_cast<char const*>(&m_size), sizeof(m_size));
    file.write(reinterpret_cast<char const*>(&m_num_hashes), sizeof(m_num_hashes));
    file.write(reinterpret_cast<char const*>(&m_num_items_added), sizeof(m_num_items_added));

    size_t num_bytes = (m_size + 7) / 8;
    const size_t CHUNK_SIZE = 1024 * 1024;
    
    for (size_t byte_idx = 0; byte_idx < num_bytes; byte_idx += CHUNK_SIZE) {
        size_t chunk_size = std::min(CHUNK_SIZE, num_bytes - byte_idx);
        std::vector<uint8_t> chunk(chunk_size, 0);
        
        for (size_t i = 0; i < chunk_size * 8 && (byte_idx * 8 + i) < m_size; ++i) {
            size_t bit_idx = byte_idx * 8 + i;
            if (m_bits[bit_idx]) {
                chunk[i / 8] |= (1 << (i % 8));
            }
        }
        
        file.write(reinterpret_cast<char const*>(chunk.data()), chunk_size);
    }
    
    return file.good();
}

bool BloomFilter::load_from_file(std::string const& filepath) {
    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        return false;
    }

    size_t size;
    uint8_t num_hashes;
    size_t num_items_added;
    
    file.read(reinterpret_cast<char*>(&size), sizeof(size));
    file.read(reinterpret_cast<char*>(&num_hashes), sizeof(num_hashes));
    file.read(reinterpret_cast<char*>(&num_items_added), sizeof(num_items_added));
    
    if (!file.good()) {
        return false;
    }

    m_size = size;
    m_num_hashes = num_hashes;
    m_num_items_added = num_items_added;
    m_bits.resize(m_size);

    size_t num_bytes = (m_size + 7) / 8;
    std::vector<uint8_t> bytes(num_bytes);
    
    file.read(reinterpret_cast<char*>(bytes.data()), num_bytes);
    
    if (!file.good()) {
        return false;
    }

    for (size_t i = 0; i < m_size; ++i) {
        m_bits[i] = (bytes[i / 8] & (1 << (i % 8))) != 0;
    }

    return true;
}

}  // namespace clp