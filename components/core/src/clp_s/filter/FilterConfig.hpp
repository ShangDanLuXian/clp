#ifndef CLP_S_FILTER_CONFIG_HPP
#define CLP_S_FILTER_CONFIG_HPP

#include <cstdint>
#include <optional>
#include <string_view>

namespace clp_s {
/**
 * Supported filter types for variable dictionaries.
 */
enum class FilterType : uint8_t {
    None = 0,
    Bloom = 1,
};

/**
 * Default false-positive rate used when Bloom filtering is enabled without an explicit value.
 */
constexpr double kDefaultFilterFalsePositiveRate{0.01};

/**
 * Filter configuration for variable dictionary filtering.
 */
struct FilterConfig {
    /**
     * Selected filter type.
     */
    FilterType type{FilterType::None};

    /**
     * False-positive rate for Bloom filter mode.
     */
    double false_positive_rate{kDefaultFilterFalsePositiveRate};
};

/**
 * Parses a filter type string.
 * @param type_str Case-sensitive filter type string.
 * @return Parsed filter type for known strings ("none", "bloom"), or std::nullopt otherwise.
 */
[[nodiscard]] std::optional<FilterType> parse_filter_type(std::string_view type_str);

/**
 * Converts a filter type to a canonical string.
 * @param type
 * @return Canonical string representation for the filter type.
 */
[[nodiscard]] std::string_view filter_type_to_string(FilterType type);
}  // namespace clp_s

#endif  // CLP_S_FILTER_CONFIG_HPP
