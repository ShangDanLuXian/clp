#include "FilterBuilder.hpp"

#include <system_error>
#include <utility>

#include <clp/FileWriter.hpp>
#include <clp/string_utils/string_utils.hpp>

#include "FilterFile.hpp"

namespace clp_s::filter {
FilterBuilder::FilterBuilder(FilterConfig const& config, size_t num_elements) : m_config(config) {
    if (FilterType::Bloom == m_config.type) {
        auto bloom_filter_result = BloomFilter::create(num_elements, m_config.false_positive_rate);
        if (bloom_filter_result.has_error()) {
            throw std::system_error(
                    bloom_filter_result.error(),
                    "Failed to create BloomFilter from filter config"
            );
        }
        m_filter.emplace(std::move(bloom_filter_result.value()));
        m_enabled = true;
    }
}

void FilterBuilder::add(std::string_view value) {
    if (false == m_enabled || false == m_filter.has_value()) {
        return;
    }
    std::string lowered(value);
    clp::string_utils::to_lower(lowered);
    m_filter.value().add(lowered);
}

bool FilterBuilder::write(std::string const& filter_path) const {
    if (false == m_enabled || false == m_filter.has_value()) {
        return false;
    }
    clp::FileWriter writer;
    writer.open(filter_path, clp::FileWriter::OpenMode::CREATE_FOR_WRITING);
    write_filter_file(writer, m_config.type, m_filter.value());
    writer.close();
    return true;
}
}  // namespace clp_s::filter
