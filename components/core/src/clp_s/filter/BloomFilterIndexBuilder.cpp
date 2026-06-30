#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <clp/ErrorCode.hpp>
#include <clp/WriterInterface.hpp>
#include <clp_s/ArchiveReader.hpp>
#include <clp_s/DictionaryReader.hpp>
#include <clp_s/filter/BloomFilterIndexBuilder.hpp>
#include <clp_s/filter/ErrorCode.hpp>
#include <clp_s/filter/FilterBuilder.hpp>
#include <clp_s/filter/FilterOptions.hpp>
#include <clp_s/filter/IndexBuilder.hpp>
#include <clp_s/filter/PackedFilterSpecification.hpp>

namespace clp_s::filter {
namespace {
// Default target false-positive rate when "false_positive_rate" is omitted from the config.
constexpr double cDefaultFalsePositiveRate{0.001};

/**
 * A `clp::WriterInterface` that appends all written bytes into a growable in-memory buffer. Only
 * the append path is implemented, which is all `FilterBuilder::write` requires; seeking is
 * unsupported.
 */
class VectorWriter : public clp::WriterInterface {
public:
    auto write(char const* data, size_t data_length) -> void override {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        m_buffer.insert(m_buffer.end(), data, data + data_length);
    }

    auto flush() -> void override {}

    auto try_seek_from_begin(size_t /*pos*/) -> clp::ErrorCode override {
        return clp::ErrorCode_Unsupported;
    }

    auto try_seek_from_current(off_t /*offset*/) -> clp::ErrorCode override {
        return clp::ErrorCode_Unsupported;
    }

    auto try_get_pos(size_t& pos) const -> clp::ErrorCode override {
        pos = m_buffer.size();
        return clp::ErrorCode_Success;
    }

    [[nodiscard]] auto take_buffer() && -> std::vector<char> { return std::move(m_buffer); }

private:
    std::vector<char> m_buffer;
};

/**
 * Parses the optional "normalization" config field.
 * @param config
 * @return The parsed normalization strategy, or std::nullopt if the field holds an unknown value.
 */
[[nodiscard]] auto parse_normalization(nlohmann::json const& config)
        -> std::optional<FilterNormalization> {
    if (false == config.is_object() || false == config.contains("normalization")) {
        return FilterNormalization::None;
    }
    auto const& normalization_value{config.at("normalization")};
    if (false == normalization_value.is_string()) {
        return std::nullopt;
    }
    auto const normalization_string{normalization_value.get<std::string>()};
    if ("none" == normalization_string) {
        return FilterNormalization::None;
    }
    if ("lowercase" == normalization_string) {
        return FilterNormalization::Lowercase;
    }
    return std::nullopt;
}
}  // namespace

auto BloomFilterIndexBuilder::create(
        nlohmann::json const& config,
        PackedFilterSpecification const& packed_filter_spec
) -> ystdlib::error_handling::Result<std::unique_ptr<IndexBuilder>> {
    double false_positive_rate{cDefaultFalsePositiveRate};
    if (config.is_object() && config.contains("false_positive_rate")) {
        auto const& false_positive_rate_value{config.at("false_positive_rate")};
        if (false == false_positive_rate_value.is_number()) {
            return ErrorCode{ErrorCodeEnum::InvalidFalsePositiveRate};
        }
        false_positive_rate = false_positive_rate_value.get<double>();
    }

    auto const normalization{parse_normalization(config)};
    if (false == normalization.has_value()) {
        return ErrorCode{ErrorCodeEnum::UnsupportedFilterNormalization};
    }

    // Validate the false-positive rate (and normalization) up front using the authoritative logic.
    if (auto const validation_result{FilterBuilder::create(
                FilterType::Bloom,
                normalization.value(),
                0,
                false_positive_rate
        )};
        validation_result.has_error())
    {
        return validation_result.error();
    }

    auto builder{std::unique_ptr<BloomFilterIndexBuilder>(
            new BloomFilterIndexBuilder{false_positive_rate, normalization.value()}
    )};
    builder->m_archive_blobs.reserve(packed_filter_spec.get_num_archives());
    return std::unique_ptr<IndexBuilder>{std::move(builder)};
}

auto BloomFilterIndexBuilder::add_archive(
        uint16_t local_archive_id,
        clp_s::ArchiveReader const& archive_reader
) -> ystdlib::error_handling::Result<void> {
    if (local_archive_id != m_archive_blobs.size()) {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::LocalArchiveIdOutOfRange};
    }

    auto const variable_dictionary{archive_reader.get_variable_dictionary()};
    auto const& entries{variable_dictionary->get_entries()};
    auto filter_builder{YSTDLIB_ERROR_HANDLING_TRYX(
            FilterBuilder::create(
                    FilterType::Bloom,
                    m_normalization,
                    entries.size(),
                    m_false_positive_rate
            )
    )};
    for (auto const& entry : entries) {
        filter_builder.add(entry.get_value());
    }

    VectorWriter writer;
    filter_builder.write(writer);
    m_archive_blobs.push_back(std::move(writer).take_buffer());
    return ystdlib::error_handling::success();
}

auto BloomFilterIndexBuilder::get_archive_blobs() const -> std::vector<std::span<char const>> {
    std::vector<std::span<char const>> blobs;
    blobs.reserve(m_archive_blobs.size());
    for (auto const& blob : m_archive_blobs) {
        blobs.emplace_back(blob.data(), blob.size());
    }
    return blobs;
}
}  // namespace clp_s::filter
