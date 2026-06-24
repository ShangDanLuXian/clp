#include <clp_s/filter/IndexRegistry.hpp>

#include <algorithm>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <clp_s/filter/ErrorCode.hpp>
#include <clp_s/filter/IndexBuilder.hpp>
#include <clp_s/filter/IndexBuilderSpecification.hpp>
#include <clp_s/filter/IndexDefs.hpp>
#include <clp_s/filter/IndexRunner.hpp>
#include <clp_s/filter/PackedFilterBuilder.hpp>
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

auto IndexRegistry::select_builder_spec(
        std::string_view name,
        archive_version_t archive_version
) const -> ystdlib::error_handling::Result<SelectedBuilderSpec> {
    auto const name_it{m_index_id_by_name.find(std::string{name})};
    if (m_index_id_by_name.cend() == name_it) {
        return IndexErrorCode{IndexErrorCodeEnum::UnknownIndexName};
    }

    auto const& registered_index{m_indexes_by_id.at(name_it->second)};
    auto const spec_it{std::ranges::find_if(
            registered_index.builder_specs,
            [archive_version](IndexBuilderSpecification const& spec) {
                return spec.supports_archive_version(archive_version);
            }
    )};
    if (registered_index.builder_specs.cend() == spec_it) {
        return IndexErrorCode{IndexErrorCodeEnum::UnsupportedArchiveVersion};
    }
    return SelectedBuilderSpec{name_it->second, &(*spec_it)};
}

auto IndexRegistry::create_writer(
        std::string_view name,
        nlohmann::json const& config,
        PackedFilterSpecification const& packed_filter_spec
) -> ystdlib::error_handling::Result<std::unique_ptr<IndexBuilder>> {
    auto const selected{YSTDLIB_ERROR_HANDLING_TRYX(
            select_builder_spec(name, packed_filter_spec.get_archive_version())
    )};
    return selected.spec->create_builder(config, packed_filter_spec);
}

auto IndexRegistry::create_packed_filter_builder(
        std::vector<std::string> archive_ids,
        archive_version_t archive_version,
        std::vector<PackedFilterIndexRequest> const& index_requests
) -> ystdlib::error_handling::Result<PackedFilterBuilder> {
    PackedFilterSpecification const packed_filter_spec{archive_ids.size(), archive_version};
    std::vector<PackedFilterBuilder::ActiveIndex> active_indexes;
    active_indexes.reserve(index_requests.size());
    for (auto const& request : index_requests) {
        auto const selected{
                YSTDLIB_ERROR_HANDLING_TRYX(select_builder_spec(request.name, archive_version))
        };
        auto builder{YSTDLIB_ERROR_HANDLING_TRYX(
                selected.spec->create_builder(request.config, packed_filter_spec)
        )};
        active_indexes.push_back(PackedFilterBuilder::ActiveIndex{
                selected.index_id,
                selected.spec->get_index_version(),
                std::move(builder)
        });
    }
    return PackedFilterBuilder{std::move(archive_ids), archive_version, std::move(active_indexes)};
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
