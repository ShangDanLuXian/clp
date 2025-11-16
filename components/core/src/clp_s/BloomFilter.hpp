#ifndef CLP_S_BLOOMFILTER_HPP
#define CLP_S_BLOOMFILTER_HPP

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "../clp/ReaderInterface.hpp"

namespace clp_s {
/**
 * A space-efficient probabilistic data structure for testing set membership.
 *
 * This bloom filter uses multiple hash functions to achieve a target false positive rate.
 * It guarantees no false negatives (if an element was added, it will always return true),
 * but may have false positives (it may return true for elements not added).
 *
 * The implementation uses SHA256-based hashing with different seeds to generate
 * multiple independent hash functions.
 */
class BloomFilter {
public:
    /**
     * Constructs a bloom filter optimized for the expected number of elements and
     * target false positive rate.
     *
     * @param expected_num_elements Expected number of elements to be inserted
     * @param false_positive_rate Target false positive rate (e.g., 0.01 for 1%)
     */
    BloomFilter(size_t expected_num_elements, double false_positive_rate);

    /**
     * Default constructor creates an empty bloom filter
     */
    BloomFilter() = default;

    /**
     * Adds an element to the bloom filter
     * @param value The string value to add
     */
    void add(std::string_view value);

    /**
     * Tests whether an element might be in the set
     * @param value The string value to test
     * @return true if the element might be in the set (possible false positive)
     * @return false if the element is definitely not in the set (guaranteed)
     */
    [[nodiscard]] auto possibly_contains(std::string_view value) const -> bool;

    /**
     * Writes the bloom filter to a file
     * @param file_writer The file writer to use
     * @param compressor The compressor to use
     */
    void write_to_file(class FileWriter& file_writer, class ZstdCompressor& compressor) const;

    /**
     * Reads the bloom filter from a file
     * @param reader The reader interface to use
     * @param decompressor The decompressor to use
     * @return true on success, false on failure
     */
    auto read_from_file(clp::ReaderInterface& reader, class ZstdDecompressor& decompressor)
            -> bool;

    /**
     * @return The size of the bit array in bits
     */
    [[nodiscard]] auto get_bit_array_size() const -> size_t { return m_bit_array_size; }

    /**
     * @return The number of hash functions used
     */
    [[nodiscard]] auto get_num_hash_functions() const -> uint32_t { return m_num_hash_functions; }

    /**
     * @return Whether the bloom filter is empty (not initialized)
     */
    [[nodiscard]] auto is_empty() const -> bool { return m_bit_array.empty(); }

private:
    /**
     * Computes optimal bloom filter parameters
     * @param expected_num_elements Expected number of elements
     * @param false_positive_rate Target false positive rate
     * @return Pair of (bit_array_size, num_hash_functions)
     */
    static auto compute_optimal_parameters(size_t expected_num_elements, double false_positive_rate)
            -> std::pair<size_t, uint32_t>;

    /**
     * Generates hash values for a given string
     * @param value The string to hash
     * @param hash_values Output vector to store hash values
     */
    void generate_hash_values(std::string_view value, std::vector<size_t>& hash_values) const;

    /**
     * Sets a bit in the bit array
     * @param bit_index The index of the bit to set
     */
    void set_bit(size_t bit_index);

    /**
     * Tests a bit in the bit array
     * @param bit_index The index of the bit to test
     * @return true if the bit is set, false otherwise
     */
    [[nodiscard]] auto test_bit(size_t bit_index) const -> bool;

    // Bit array stored as bytes (8 bits per byte)
    std::vector<uint8_t> m_bit_array;

    // Size of the bit array in bits
    size_t m_bit_array_size{0};

    // Number of hash functions to use
    uint32_t m_num_hash_functions{0};
};
}  // namespace clp_s

#endif  // CLP_S_BLOOMFILTER_HPP
