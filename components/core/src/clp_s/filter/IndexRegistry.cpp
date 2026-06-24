#include <clp_s/filter/IndexRegistry.hpp>

#include <algorithm>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nlohmann/json_fwd.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <clp_s/filter/ErrorCode.hpp>
#include <clp_s/filter/IndexBuilder.hpp>
#include <clp_s/filter/IndexBuilderSpecification.hpp>
#include <clp_s/filter/IndexDefs.hpp>
#include <clp_s/filter/IndexRunner.hpp>
#include <clp_s/filter/PackedFilterSpecification.hpp>

namespace clp_s::filter {
auto IndexRegistry::register_index(
        std::string name,
        index_id_t index_id,
        IndexRunnerFactory runner_factory,
        std::vector<IndexBuilderSpecification> builder_specs
) -> ystdlib::error_handling::Result<void> {
    if (m_index_id_by_name.contains(name)) {
        return IndexErrorCode{IndexErrorCodeEnum::DuplicateIndexName};
    }
    if (m_indexes_by_id.contains(index_id)) {
        return IndexErrorCode{IndexErrorCodeEnum::DuplicateIndexId};
    }
    m_indexes_by_id.try_emplace(index_id, runner_factory, std::move(builder_specs));
    m_index_id_by_name.try_emplace(std::move(name), index_id);
    return ystdlib::error_handling::success();
}

auto IndexRegistry::create_writer(
        std::string_view name,
        nlohmann::json const& config,
        PackedFilterSpecification const& packed_filter_spec
) -> ystdlib::error_handling::Result<std::unique_ptr<IndexBuilder>> {
    auto const name_it{m_index_id_by_name.find(std::string{name})};
    if (m_index_id_by_name.cend() == name_it) {
        return IndexErrorCode{IndexErrorCodeEnum::UnknownIndexName};
    }

    auto const& registered_index{m_indexes_by_id.at(name_it->second)};
    auto const archive_version{packed_filter_spec.get_archive_version()};
    auto const spec_it{std::ranges::find_if(
            registered_index.builder_specs,
            [archive_version](IndexBuilderSpecification const& spec) {
                return spec.supports_archive_version(archive_version);
            }
    )};
    if (registered_index.builder_specs.cend() == spec_it) {
        return IndexErrorCode{IndexErrorCodeEnum::UnsupportedArchiveVersion};
    }
    return spec_it->create_builder(config, packed_filter_spec);
}

auto IndexRegistry::create_reader(
        index_id_t index_id,
        index_version_t index_version,
        std::vector<std::span<char const>> const& archive_blobs
) -> ystdlib::error_handling::Result<std::unique_ptr<IndexRunner>> {
    auto const index_it{m_indexes_by_id.find(index_id)};
    if (m_indexes_by_id.cend() == index_it) {
        return IndexErrorCode{IndexErrorCodeEnum::UnknownIndexId};
    }
    return index_it->second.runner_factory(index_version, archive_blobs);
}
}  // namespace clp_s::filter
