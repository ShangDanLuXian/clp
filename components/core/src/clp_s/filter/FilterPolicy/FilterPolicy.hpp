#ifndef CLP_S_FILTERPOLICY_HPP
#define CLP_S_FILTERPOLICY_HPP

#include <cstddef>
#include <cstdint>
#include <memory>

namespace clp_s {

/**
 * Parameters computed by a filter policy
 */
struct FilterParameters {
    double bits_per_key;         // Bits per element
    uint32_t num_hash_functions; // Number of hash functions (k)
};

/**
 * Abstract interface for filter policies that compute optimal parameters
 */
class IFilterPolicy {
public:
    virtual ~IFilterPolicy() = default;
    
    /**
     * Computes optimal filter parameters for a given false positive rate
     * @param false_positive_rate Target false positive rate
     * @return Filter parameters (bits per key and number of hash functions)
     */
    [[nodiscard]] virtual auto compute_parameters(double false_positive_rate) const
            -> FilterParameters = 0;
    
    /**
     * Creates a deep copy of this policy
     */
    [[nodiscard]] virtual auto clone() const -> std::unique_ptr<IFilterPolicy> = 0;
    
protected:
    IFilterPolicy() = default;
    IFilterPolicy(IFilterPolicy const&) = default;
    auto operator=(IFilterPolicy const&) -> IFilterPolicy& = default;
    IFilterPolicy(IFilterPolicy&&) = default;
    auto operator=(IFilterPolicy&&) -> IFilterPolicy& = default;
};

}  // namespace clp_s

#endif  // CLP_S_FILTERPOLICY_HPP