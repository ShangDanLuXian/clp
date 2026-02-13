#include "FilterFile.hpp"

#include <cstring>

#include "../clp/ReaderInterface.hpp"
#include "../clp/ErrorCode.hpp"

namespace clp_s::filter {
void write_filter_file(
        FileWriter& writer,
        FilterConfig const& config,
        ProbabilisticFilter const& filter,
        size_t num_elements
) {
    writer.write(kFilterFileMagic, sizeof(kFilterFileMagic));
    writer.write_numeric_value<uint32_t>(kFilterFileVersion);
    writer.write_numeric_value<uint8_t>(static_cast<uint8_t>(config.type));
    uint8_t flags = 0;
    if (config.normalize) {
        flags |= kFilterFlagNormalized;
    }
    writer.write_numeric_value<uint8_t>(flags);
    writer.write_numeric_value<uint16_t>(0);
    writer.write_numeric_value<double>(config.false_positive_rate);
    writer.write_numeric_value<uint64_t>(static_cast<uint64_t>(num_elements));

    if (FilterType::None != config.type) {
        filter.write_to_file(writer);
    }
}

bool read_filter_file(
        clp::ReaderInterface& reader,
        FilterConfig& out_config,
        ProbabilisticFilter& out_filter,
        size_t& out_num_elements
) {
    char magic[sizeof(kFilterFileMagic)]{};
    if (clp::ErrorCode_Success
        != reader.try_read_exact_length(magic, sizeof(kFilterFileMagic)))
    {
        return false;
    }
    if (0 != std::memcmp(magic, kFilterFileMagic, sizeof(kFilterFileMagic))) {
        return false;
    }

    uint32_t version = 0;
    if (clp::ErrorCode_Success != reader.try_read_numeric_value(version)) {
        return false;
    }
    if (version != kFilterFileVersion) {
        return false;
    }

    uint8_t type_value = 0;
    if (clp::ErrorCode_Success != reader.try_read_numeric_value(type_value)) {
        return false;
    }
    uint8_t flags = 0;
    if (clp::ErrorCode_Success != reader.try_read_numeric_value(flags)) {
        return false;
    }
    uint16_t reserved = 0;
    if (clp::ErrorCode_Success != reader.try_read_numeric_value(reserved)) {
        return false;
    }

    double fpr = 0.0;
    if (clp::ErrorCode_Success != reader.try_read_numeric_value(fpr)) {
        return false;
    }

    uint64_t num_elements = 0;
    if (clp::ErrorCode_Success != reader.try_read_numeric_value(num_elements)) {
        return false;
    }

    out_config.type = static_cast<FilterType>(type_value);
    out_config.false_positive_rate = fpr;
    out_config.normalize = (flags & kFilterFlagNormalized) != 0;
    out_num_elements = static_cast<size_t>(num_elements);

    if (FilterType::None == out_config.type) {
        return true;
    }

    out_filter = ProbabilisticFilter::create_empty_for_type(out_config.type);
    return out_filter.read_from_file(reader);
}
}  // namespace clp_s::filter
