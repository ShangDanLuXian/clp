#include "FilterConfig.hpp"

#include <algorithm>
#include <cctype>
#include <string>

namespace clp_s {
namespace {
std::string to_lower(std::string_view input) {
    std::string out(input);
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return out;
}
}  // namespace

auto parse_filter_type(std::string_view type_str) -> std::optional<FilterType> {
    auto lowered = to_lower(type_str);
    if (lowered == "none") {
        return FilterType::None;
    }
    if (lowered == "bloom_v1" || lowered == "bloom") {
        return FilterType::BloomV1;
    }
    return std::nullopt;
}

auto filter_type_to_string(FilterType type) -> std::string_view {
    switch (type) {
        case FilterType::None:
            return "none";
        case FilterType::BloomV1:
            return "bloom_v1";
    }
    return "unknown";
}
}  // namespace clp_s
