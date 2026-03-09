#ifndef CLP_S_FILTER_BUILDER_HPP
#define CLP_S_FILTER_BUILDER_HPP

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

#include "BloomFilter.hpp"
#include "FilterConfig.hpp"

namespace clp_s::filter {
/**
 * Builds and writes a normalized variable dictionary filter.
 */
class FilterBuilder {
public:
    /**
     * @param config Filter configuration.
     * @param num_elements Expected number of dictionary values.
     * @throws std::system_error if filter construction fails.
     */
    FilterBuilder(FilterConfig const& config, size_t num_elements);

    /**
     * Adds a value to the filter after normalization.
     * @param value
     */
    void add(std::string_view value);

    /**
     * @param filter_path Destination path for the serialized filter payload.
     * @return false when filter building is disabled (type == None); true when a filter is written.
     * @throws Any exception raised by file I/O.
     */
    [[nodiscard]] bool write(std::string const& filter_path) const;

private:
    FilterConfig m_config;
    std::optional<BloomFilter> m_filter{std::nullopt};
    bool m_enabled{false};
};
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_BUILDER_HPP
