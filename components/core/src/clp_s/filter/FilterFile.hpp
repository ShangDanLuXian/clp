#ifndef CLP_S_FILTER_FILE_HPP
#define CLP_S_FILTER_FILE_HPP

#include <cstdint>
#include <optional>

#include <clp/ReaderInterface.hpp>
#include <clp/WriterInterface.hpp>

#include "BloomFilter.hpp"
#include "FilterConfig.hpp"

namespace clp_s::filter {
/**
 * Filter file magic bytes: "CLPF".
 */
constexpr char kFilterFileMagic[4] = {'C', 'L', 'P', 'F'};

/**
 * Writes the filter file payload.
 * @param writer
 * @param type Filter type encoded in the payload.
 * @param filter Bloom filter payload when `type` is Bloom.
 */
void write_filter_file(clp::WriterInterface& writer, FilterType type, BloomFilter const& filter);

/**
 * Parsed filter file payload.
 */
struct ParsedFilterFile {
    /**
     * Parsed filter type.
     */
    FilterType type{FilterType::None};

    /**
     * Parsed Bloom filter payload when `type` is Bloom.
     */
    std::optional<BloomFilter> bloom_filter{std::nullopt};
};

/**
 * Reads a filter file payload.
 * @param reader
 * @return Parsed filter payload when valid; std::nullopt for corrupt/unsupported payload.
 */
[[nodiscard]] std::optional<ParsedFilterFile> read_filter_file(clp::ReaderInterface& reader);
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_FILE_HPP
