#include <clp_s/filter/RegisterIndexes.hpp>

#include <optional>
#include <utility>
#include <vector>

#include <ystdlib/error_handling/Result.hpp>

#include <clp_s/filter/BloomFilterIndexBuilder.hpp>
#include <clp_s/filter/BloomFilterIndexRunner.hpp>
#include <clp_s/filter/IndexBuilderSpecification.hpp>
#include <clp_s/filter/IndexDefs.hpp>
#include <clp_s/filter/IndexRegistry.hpp>
#include <clp_s/SingleFileArchiveDefs.hpp>

namespace clp_s::filter {
namespace {
// The Bloom filter index is the first official open-source index.
constexpr index_id_t cBloomFilterIndexId{cOfficialOpenSourceIndexIdBegin};
constexpr index_version_t cBloomFilterIndexVersion{make_index_version(1, 0, 0)};

[[nodiscard]] auto register_bloom_filter_index(IndexRegistry& registry)
        -> ystdlib::error_handling::Result<void> {
    std::vector<IndexBuilderSpecification> builder_specs;
    builder_specs.emplace_back(
            to_archive_section_bitmap(ArchiveSection::Dictionaries),
            clp_s::cArchiveVersion,
            std::optional<archive_version_t>{},  // open upper bound: also supports newer archives
            cBloomFilterIndexVersion,
            &BloomFilterIndexBuilder::create
    );
    return registry.register_index(
            "bloom_filter",
            cBloomFilterIndexId,
            &BloomFilterIndexRunner::create,
            std::move(builder_specs)
    );
}
}  // namespace

auto register_indexes(IndexRegistry& registry) -> ystdlib::error_handling::Result<void> {
    YSTDLIB_ERROR_HANDLING_TRYV(register_bloom_filter_index(registry));
    return ystdlib::error_handling::success();
}
}  // namespace clp_s::filter
