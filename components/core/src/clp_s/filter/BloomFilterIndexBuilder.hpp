#ifndef CLP_S_FILTER_BLOOM_FILTER_INDEX_BUILDER_HPP
#define CLP_S_FILTER_BLOOM_FILTER_INDEX_BUILDER_HPP

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include <nlohmann/json_fwd.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <clp_s/filter/FilterOptions.hpp>
#include <clp_s/filter/IndexBuilder.hpp>
#include <clp_s/filter/PackedFilterSpecification.hpp>

namespace clp_s {
class ArchiveReader;
}  // namespace clp_s

namespace clp_s::filter {
/**
 * An `IndexBuilder` that builds a Bloom filter over each archive's variable dictionary values, so
 * that filtering can prune archives whose dictionary cannot contain a queried value.
 *
 * Each archive's filter is serialized into its own blob using the same layout as `FilterBuilder`, so
 * that `BloomFilterIndexRunner` can read it back with a `FilterReader`.
 *
 * Requires the framework to have loaded each archive's `ArchiveSection::Dictionaries` before calling
 * `add_archive`.
 */
class BloomFilterIndexBuilder : public IndexBuilder {
public:
    // Factory functions
    /**
     * Factory matching `IndexBuilderSpecification::Factory`.
     * @param config Bloom filter configuration:
     * - "false_positive_rate" (number, optional): target false-positive rate; defaults to
     *   `cDefaultFalsePositiveRate`.
     * - "normalization" (string, optional): "none" or "lowercase"; defaults to "none".
     * @param packed_filter_spec
     * @return A result containing the created builder on success, or an error code indicating the
     * failure:
     * - ErrorCodeEnum::InvalidFalsePositiveRate if "false_positive_rate" is not a number in the
     *   supported range.
     * - ErrorCodeEnum::UnsupportedFilterNormalization if "normalization" is not a known strategy.
     */
    [[nodiscard]] static auto create(
            nlohmann::json const& config,
            PackedFilterSpecification const& packed_filter_spec
    ) -> ystdlib::error_handling::Result<std::unique_ptr<IndexBuilder>>;

    // Methods (IndexBuilder)
    /**
     * Builds and serializes a Bloom filter over `archive_reader`'s variable dictionary values.
     * @param local_archive_id Must equal the number of archives already added, i.e. archives must be
     * added in order of ascending local archive ID.
     * @param archive_reader
     * @return A void result on success, or an error code indicating the failure:
     * - PackedFilterErrorCodeEnum::LocalArchiveIdOutOfRange if `local_archive_id` is not the next
     *   expected ID.
     * - Forwards `FilterBuilder::create`'s return values on failure.
     */
    [[nodiscard]] auto
    add_archive(uint16_t local_archive_id, clp_s::ArchiveReader const& archive_reader)
            -> ystdlib::error_handling::Result<void> override;

    [[nodiscard]] auto get_archive_blobs() const -> std::vector<std::span<char const>> override;

private:
    // Constructors
    BloomFilterIndexBuilder(double false_positive_rate, FilterNormalization normalization)
            : m_false_positive_rate{false_positive_rate},
              m_normalization{normalization} {}

    // Variables
    double m_false_positive_rate;
    FilterNormalization m_normalization;
    // One serialized Bloom filter per archive, indexed by local archive ID.
    std::vector<std::vector<char>> m_archive_blobs;
};
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_BLOOM_FILTER_INDEX_BUILDER_HPP
