#ifndef CLP_BLOOMFILTER_HPP
#define CLP_BLOOMFILTER_HPP

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string>
#include <vector>

namespace clp {

class BloomFilter {
public:
    static constexpr size_t DEFAULT_NGRAM_SIZE = 6;  // Like ClickHouse
    
    BloomFilter(size_t size = 100000, uint8_t num_hashes = 3);

    void add(std::string const& item);
    [[nodiscard]] bool might_contain(std::string const& item) const;
    
    /**
     * Add all n-grams from a string
     * @param text Text to extract n-grams from
     * @param n N-gram size (default 4, like ClickHouse)
     */
    void add_ngrams(std::string const& text, size_t n = DEFAULT_NGRAM_SIZE);
    
    /**
     * Check if all n-grams from text might be present
     * @param text Text to check (must be >= n chars after normalization)
     * @param n N-gram size (default 4)
     * @return true if all n-grams might be present, false if definitely not present
     */
    [[nodiscard]] bool might_contain_ngrams(std::string const& text, size_t n = DEFAULT_NGRAM_SIZE) const;

    bool write_to_file(std::string const& filepath) const;
    bool load_from_file(std::string const& filepath);

    [[nodiscard]] size_t get_num_items_added() const { return m_num_items_added; }
    [[nodiscard]] size_t get_size() const { return m_size; }

private:
    [[nodiscard]] std::vector<size_t> compute_hashes(std::string const& item) const;
    
    /**
     * Normalize string: lowercase alphanumeric only
     */
    static std::string normalize_string(std::string const& str);

    size_t m_size;
    uint8_t m_num_hashes;
    std::vector<bool> m_bits;
    size_t m_num_items_added{0};
};

}  // namespace clp

#endif  // CLP_BLOOMFILTER_HPP