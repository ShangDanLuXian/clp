#ifndef CLP_S_BINARYFUSEFILTERPOLICY_HPP
#define CLP_S_BINARYFUSEFILTERPOLICY_HPP

#include <algorithm>
#include <cmath>
#include <memory>
#include "FilterPolicy.hpp"

namespace clp_s {

/**
 * Policy for computing optimal Binary Fuse Filter parameters.
 * 
 * Binary fuse filters use a different approach than Bloom filters:
 * - Space: ~(fingerprint_bits × 1.25) bits per key
 * - FPR = 2^(-fingerprint_bits)
 * - Uses XOR-based storage with 3-way hashing
 */
class BinaryFuseFilterPolicy : public IFilterPolicy {
public:
    BinaryFuseFilterPolicy() = default;
    
    [[nodiscard]] auto compute_parameters(double false_positive_rate) const
            -> FilterParameters override {
        auto const fingerprint_bits = compute_fingerprint_bits(false_positive_rate);
        auto const bits_per_key = compute_bits_per_key(fingerprint_bits);
        
        // Binary fuse doesn't use traditional hash functions
        // We store fingerprint_bits as num_hash_functions for compatibility
        return {bits_per_key, fingerprint_bits};
    }
    
    [[nodiscard]] auto clone() const -> std::unique_ptr<IFilterPolicy> override {
        return std::make_unique<BinaryFuseFilterPolicy>();
    }
    
    /**
     * Computes fingerprint bits needed for a given false positive rate
     * Formula: fingerprint_bits = ceil(-log₂(FPR))
     */
    [[nodiscard]] static auto compute_fingerprint_bits(double false_positive_rate) -> uint32_t {
        if (false_positive_rate <= 0.0) {
            return 16;  // Very low FPR requested, use 16 bits
        }
        if (false_positive_rate >= 1.0) {
            return 1;  // Even 100% FPR filter needs some bits
        }
        
        // fingerprint_bits = -log₂(FPR)
        auto const fb = std::ceil(-std::log2(false_positive_rate));
        
        // Clamp to reasonable range [4, 32]
        return std::clamp(static_cast<uint32_t>(fb), 4u, 32u);
    }
    
    /**
     * Computes bits per key for given fingerprint bits
     * Formula: bits_per_key ≈ fingerprint_bits × 1.25
     * 
     * This accounts for the expansion factor needed by the binary fuse algorithm
     */
    [[nodiscard]] static auto compute_bits_per_key(uint32_t fingerprint_bits) -> double {
        // Binary fuse uses ~1.25x expansion factor
        return static_cast<double>(fingerprint_bits) * 1.25;
    }
    
    /**
     * Computes actual FPR from fingerprint bits
     * Formula: FPR = 2^(-fingerprint_bits)
     */
    [[nodiscard]] static auto compute_fpr_from_fingerprint_bits(uint32_t fingerprint_bits)
            -> double {
        return std::pow(2.0, -static_cast<double>(fingerprint_bits));
    }
    
    /**
     * Computes FPR from bits per key
     */
    [[nodiscard]] static auto compute_fpr_from_bits_per_key(double bits_per_key)
            -> std::pair<uint32_t, double> {
        if (bits_per_key <= 0.0) {
            return {4, 0.0625};  // Minimum practical: 4 bits
        }
        
        // Reverse formula: fingerprint_bits ≈ bits_per_key / 1.25
        auto const fingerprint_bits = static_cast<uint32_t>(
            std::round(bits_per_key / 1.25)
        );
        auto const clamped_bits = std::clamp(fingerprint_bits, 4u, 32u);
        auto const fpr = compute_fpr_from_fingerprint_bits(clamped_bits);
        
        return {clamped_bits, fpr};
    }
};

}  // namespace clp_s

#endif  // CLP_S_BINARYFUSEFILTERPOLICY_HPP