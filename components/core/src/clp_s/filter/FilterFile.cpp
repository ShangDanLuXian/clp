#include "FilterFile.hpp"

#include <cstring>
#include <utility>

#include <clp/ErrorCode.hpp>

namespace clp_s::filter {
void write_filter_file(clp::WriterInterface& writer, FilterType type, BloomFilter const& filter) {
    writer.write(kFilterFileMagic, sizeof(kFilterFileMagic));
    writer.write_numeric_value<uint8_t>(static_cast<uint8_t>(type));
    if (FilterType::None != type) {
        filter.write_to_file(writer);
    }
}

std::optional<ParsedFilterFile> read_filter_file(clp::ReaderInterface& reader) {
    char magic[sizeof(kFilterFileMagic)]{};
    if (clp::ErrorCode_Success != reader.try_read_exact_length(magic, sizeof(kFilterFileMagic))) {
        return std::nullopt;
    }
    if (0 != std::memcmp(magic, kFilterFileMagic, sizeof(kFilterFileMagic))) {
        return std::nullopt;
    }

    uint8_t type_value = 0;
    if (clp::ErrorCode_Success != reader.try_read_numeric_value(type_value)) {
        return std::nullopt;
    }
    auto const type = static_cast<FilterType>(type_value);
    if (FilterType::None == type) {
        return ParsedFilterFile{type, std::nullopt};
    }

    if (FilterType::Bloom != type) {
        return std::nullopt;
    }

    auto bloom_filter_result = BloomFilter::try_read_from_file(reader);
    if (bloom_filter_result.has_error()) {
        return std::nullopt;
    }

    return ParsedFilterFile{type, std::move(bloom_filter_result.value())};
}
}  // namespace clp_s::filter
