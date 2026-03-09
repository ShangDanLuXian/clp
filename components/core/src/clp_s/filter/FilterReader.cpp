#include "FilterReader.hpp"

#include <optional>
#include <string>
#include <utility>

#include <clp/string_utils/string_utils.hpp>

#include "FilterFile.hpp"

namespace clp_s::filter {
bool FilterReader::read_from_file(clp::ReaderInterface& reader) {
    auto parsed_filter_file = read_filter_file(reader);
    if (false == parsed_filter_file.has_value()) {
        return false;
    }

    m_type = parsed_filter_file->type;
    m_filter = std::move(parsed_filter_file->bloom_filter);
    return true;
}

bool FilterReader::possibly_contains(std::string_view value) const {
    if (FilterType::None == m_type) {
        return true;
    }
    if (FilterType::Bloom != m_type || false == m_filter.has_value()) {
        return true;
    }

    std::string lowered(value);
    clp::string_utils::to_lower(lowered);
    return m_filter.value().possibly_contains(lowered);
}
}  // namespace clp_s::filter
