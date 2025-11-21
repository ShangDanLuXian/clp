#include "ProbabilisticFilter.hpp"

#include "BloomFilter.hpp"
#include "../ErrorCode.hpp"
#include "../ZstdCompressor.hpp"
#include "../ZstdDecompressor.hpp"

namespace clp_s {

ProbabilisticFilter::ProbabilisticFilter(
        FilterType type,
        size_t expected_num_elements,
        double false_positive_rate
) {
    switch (type) {
        case FilterType::Bloom:
            m_impl = std::make_unique<BloomFilter>(expected_num_elements, false_positive_rate);
            break;
        case FilterType::None:
            throw std::logic_error("Invalid FilterType: unreachable code path");
    }
}

ProbabilisticFilter::ProbabilisticFilter()
        : m_impl{std::make_unique<BloomFilter>()} {}

ProbabilisticFilter::ProbabilisticFilter(ProbabilisticFilter const& other) {
    if (other.m_impl) {
        m_impl = other.m_impl->clone();
    }
}

auto ProbabilisticFilter::operator=(ProbabilisticFilter const& other) -> ProbabilisticFilter& {
    if (this != &other) {
        if (other.m_impl) {
            m_impl = other.m_impl->clone();
        } else {
            m_impl.reset();
        }
    }
    return *this;
}

void ProbabilisticFilter::add(std::string_view value) {
    if (m_impl) {
        m_impl->add(value);
    }
}

auto ProbabilisticFilter::possibly_contains(std::string_view value) const -> bool {
    return m_impl ? m_impl->possibly_contains(value) : false;
}

void ProbabilisticFilter::write_to_file(
        FileWriter& file_writer,
        ZstdCompressor& compressor
) const {
    if (m_impl) {
        m_impl->write_to_file(file_writer, compressor);
    }
}

auto ProbabilisticFilter::read_from_file(
        clp::ReaderInterface& reader,
        ZstdDecompressor& decompressor
) -> bool {
    // Read filter type from header
    uint8_t type_value = 0;
    if (ErrorCodeSuccess != decompressor.try_read_numeric_value(type_value)) {
        return false;
    }

    auto const type = static_cast<FilterType>(type_value);

    switch (type) {
        case FilterType::Bloom:
            m_impl = std::make_unique<BloomFilter>();
            break;
        default:
            return false;
    }

    return m_impl->read_from_file(reader, decompressor);
}

auto ProbabilisticFilter::is_empty() const -> bool {
    return !m_impl || m_impl->is_empty();
}

auto ProbabilisticFilter::get_type() const -> FilterType {
    return m_impl ? m_impl->get_type() : FilterType::Bloom;
}

auto ProbabilisticFilter::get_memory_usage() const -> size_t {
    return m_impl ? m_impl->get_memory_usage() : 0;
}

auto ProbabilisticFilter::create_from_file(
        clp::ReaderInterface& reader,
        ZstdDecompressor& decompressor
) -> ProbabilisticFilter {
    ProbabilisticFilter filter;
    filter.read_from_file(reader, decompressor);
    return filter;
}

}  // namespace clp_s