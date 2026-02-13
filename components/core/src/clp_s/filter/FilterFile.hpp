#ifndef CLP_S_FILTER_FILE_HPP
#define CLP_S_FILTER_FILE_HPP

#include <cstdint>
#include <cstddef>

#include "../FileWriter.hpp"
#include "../TraceableException.hpp"
#include "FilterConfig.hpp"
#include "ProbabilisticFilter.hpp"

namespace clp {
class ReaderInterface;
}

namespace clp_s {
namespace filter {
constexpr char kFilterFileMagic[4] = {'C', 'L', 'P', 'F'};
constexpr uint32_t kFilterFileVersion = 1;

constexpr uint8_t kFilterFlagNormalized = 0x1;

struct FilterFileHeader {
    char magic[4];
    uint32_t version;
    uint8_t type;
    uint8_t flags;
    uint16_t reserved;
    double false_positive_rate;
    uint64_t num_elements;
};

void write_filter_file(
        FileWriter& writer,
        FilterConfig const& config,
        ProbabilisticFilter const& filter,
        size_t num_elements
);

bool read_filter_file(
        clp::ReaderInterface& reader,
        FilterConfig& out_config,
        ProbabilisticFilter& out_filter,
        size_t& out_num_elements
);
}  // namespace filter
}  // namespace clp_s

#endif  // CLP_S_FILTER_FILE_HPP
