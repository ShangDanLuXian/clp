#ifndef CLP_S_BLOOMFILTERPOLICY_HPP
#define CLP_S_BLOOMFILTERPOLICY_HPP

#include <algorithm>
#include <cmath>
#include <memory>
#include "FilterPolicy.hpp"

namespace clp_s {

/**
 * Policy for computing optimal Bloom filter parameters.
 * 
 * Uses the standard formulas:
 * - bits_per_key = -log₂(FPR) / ln(2) ≈ -1.44 × log₂(FPR)
 * - num_hash_functions = bits_per_key × ln(2) ≈ 0.693 × bits_per_key
 */
class BloomFilterPolicy : public IFilterPolicy {
public:
    BloomFilterPolicy() = default;
    
    [[nodiscard]] auto compute_parameters(double false_positive_rate) const
            -> FilterParameters override {
        auto const bits_per_key = compute_bits_per_key(false_positive_rate);
        auto const num_hash_functions = compute_num_hash_functions(bits_per_key);
        return {bits_per_key, num_hash_functions};
    }
    
    [[nodiscard]] auto clone() const -> std::unique_ptr<IFilterPolicy> override {
        return std::make_unique<BloomFilterPolicy>();
    }
    
    // /**
    //  * Computes bits per key for a given false positive rate
    //  * Formula: m/n = -log₂(FPR) / ln(2)
    //  */
    // [[nodiscard]] static auto compute_bits_per_key(double false_positive_rate) -> double {
    //     return -std::log2(false_positive_rate) / std::log(2.0);
    // }
    
    /**
     * Computes optimal number of hash functions for given bits per key
     * Formula: k = (m/n) × ln(2)
     */
    [[nodiscard]] static auto compute_num_hash_functions(double bits_per_key) -> uint32_t {
        auto const k = std::round(bits_per_key * std::log(2.0));
        return std::max(1U, static_cast<uint32_t>(k));
        
    }
    
    // /**
    // * Computes optimal number of hash functions and resulting FPR for given bits_per_key
    // * 
    // * Given a bits-per-key ratio, computes:
    // * 1. Optimal k = (m/n) × ln(2)
    // * 2. Resulting FPR ≈ (1/2)^k = 0.6185^(m/n)
    // * 
    // * @param bits_per_key Ratio of bit array size to number of elements (m/n)
    // * @return Pair of (optimal num_hash_functions, actual FPR)
    // */
    // [[nodiscard]] static auto compute_fpr_from_bits_per_key(double bits_per_key) 
    // -> std::pair<uint32_t, double> {
    // if (bits_per_key <= 0.0) {
    // return {1, 1.0};  // Degenerate case
    // }

    

    // // Optimal k = bpk × ln(2)
    // auto const num_hash_functions = compute_num_hash_functions(bits_per_key);

    // // FPR ≈ (1 - e^(-k/bpk))^k, but for optimal k this simplifies to (1/2)^k
    // double const exponent = -static_cast<double>(num_hash_functions) / bits_per_key;
    // double const base = 1.0 - std::exp(exponent);
    // double const fpr = std::pow(base, num_hash_functions);

    // return {num_hash_functions, fpr};
    // }

    // BloomFilterPolicy.hpp - Remove FPR clamping, just ensure valid math

[[nodiscard]] static auto compute_bits_per_key(double false_positive_rate) -> double {
    if (false_positive_rate <= 0.0) {
        return 100.0;  // Very low FPR requested
    }
    if (false_positive_rate >= 1.0) {
        return 0.1;  // Even 100% FPR filter needs some bits
    }
    return -std::log2(false_positive_rate) / std::log(2.0);
}

[[nodiscard]] static auto compute_fpr_from_bits_per_key(double bits_per_key) 
        -> std::pair<uint32_t, double> {
    if (bits_per_key <= 0.0) {
        return {1, 1.0};  // Degenerate case
    }

    auto const num_hash_functions = compute_num_hash_functions(bits_per_key);

    double const exponent = -static_cast<double>(num_hash_functions) / bits_per_key;
    double const base = 1.0 - std::exp(exponent);
    double const fpr = std::pow(base, num_hash_functions);

    return {num_hash_functions, fpr};
}

    /**
    * Computes FPR from absolute parameters by first computing bits_per_key
    * 
    * @param num_elements Number of elements to store
    * @param bit_array_size Total size of bit array in bits
    * @return Pair of (optimal num_hash_functions, actual FPR)
    */
    [[nodiscard]] static auto compute_fpr_from_size(
    size_t num_elements,
    size_t bit_array_size
    ) -> std::pair<uint32_t, double> {
    if (bit_array_size == 0 || num_elements == 0) {
    return {0, 0.0};
    }

    double const bits_per_key = static_cast<double>(bit_array_size) 
                            / static_cast<double>(num_elements);
    return compute_fpr_from_bits_per_key(bits_per_key);
    }
};

}  // namespace clp_s

#endif  // CLP_S_BLOOMFILTERPOLICY_HPP