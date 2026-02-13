#include "ProbabilisticFilter.hpp"

#include "BloomFilter.hpp"

namespace clp_s::filter {
ProbabilisticFilter::ProbabilisticFilter(ProbabilisticFilter const& other) {
    if (other.m_impl) {
        m_impl = other.m_impl->clone();
    }
}

auto ProbabilisticFilter::operator=(ProbabilisticFilter const& other)
        -> ProbabilisticFilter& {
    if (this != &other) {
        if (other.m_impl) {
            m_impl = other.m_impl->clone();
        } else {
            m_impl.reset();
        }
    }
    return *this;
}

auto ProbabilisticFilter::create(FilterConfig const& config, size_t expected_num_elements)
        -> ProbabilisticFilter {
    ProbabilisticFilter filter;
    switch (config.type) {
        case FilterType::BloomV1:
            filter.m_impl
                    = std::make_unique<BloomFilter>(expected_num_elements, config.false_positive_rate);
            break;
        case FilterType::None:
            break;
    }
    return filter;
}

auto ProbabilisticFilter::create_empty_for_type(FilterType type) -> ProbabilisticFilter {
    ProbabilisticFilter filter;
    switch (type) {
        case FilterType::BloomV1:
            filter.m_impl = std::make_unique<BloomFilter>();
            break;
        case FilterType::None:
            break;
    }
    return filter;
}

void ProbabilisticFilter::add(std::string_view value) {
    if (m_impl) {
        m_impl->add(value);
    }
}

auto ProbabilisticFilter::possibly_contains(std::string_view value) const -> bool {
    return m_impl ? m_impl->possibly_contains(value) : false;
}

void ProbabilisticFilter::write_to_file(clp_s::FileWriter& writer) const {
    if (m_impl) {
        m_impl->write_to_file(writer);
    }
}

auto ProbabilisticFilter::read_from_file(clp::ReaderInterface& reader) -> bool {
    if (m_impl) {
        return m_impl->read_from_file(reader);
    }
    return false;
}

auto ProbabilisticFilter::is_empty() const -> bool {
    return !m_impl || m_impl->is_empty();
}

auto ProbabilisticFilter::get_type() const -> FilterType {
    return m_impl ? m_impl->get_type() : FilterType::None;
}

auto ProbabilisticFilter::get_memory_usage() const -> size_t {
    return m_impl ? m_impl->get_memory_usage() : 0;
}
}  // namespace clp_s::filter
