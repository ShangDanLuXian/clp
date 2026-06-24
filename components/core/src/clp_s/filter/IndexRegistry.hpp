#ifndef CLP_S_FILTER_INDEX_REGISTRY_HPP
#define CLP_S_FILTER_INDEX_REGISTRY_HPP

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <clp_s/filter/IndexBuilder.hpp>
#include <clp_s/filter/IndexBuilderSpecification.hpp>
#include <clp_s/filter/IndexDefs.hpp>
#include <clp_s/filter/IndexRunner.hpp>
#include <clp_s/filter/PackedFilterBuilder.hpp>
#include <clp_s/filter/PackedFilterSpecification.hpp>

namespace clp_s::filter {
/**
 * A request to build one index into a Packed Filter: the registered name of the index and the
 * implementation-defined configuration forwarded to its builder.
 */
struct PackedFilterIndexRequest {
    std::string name;
    nlohmann::json config;
};

/**
 * Registry of all known index implementations. Index implementations register themselves with a
 * unique name and Index ID, after which the registry creates `IndexBuilder`s (by name, at build
 * time) and `IndexRunner`s (by Index ID, at filtering time) on their behalf.
 */
class IndexRegistry {
public:
    // Types
    /**
     * A factory for creating an `IndexRunner` from an index's serialized blobs.
     * @param index_version The version of the serialized index, as read from the Packed Filter.
     * @param archive_blobs The index's serialized data for each archive, indexed by local archive
     * ID.
     * @return A result containing the created runner on success, or an error code indicating the
     * failure:
     * - Error codes are defined by the implementation.
     */
    using IndexRunnerFactory = auto (*)(
            index_version_t index_version,
            std::vector<std::span<char const>> const& archive_blobs
    ) -> ystdlib::error_handling::Result<std::unique_ptr<IndexRunner>>;

    // Constructors
    IndexRegistry() = default;

    // Methods
    /**
     * Registers an index implementation.
     * @param name The unique name of the index.
     * @param index_id The unique Index ID of the index.
     * @param runner_factory A factory for creating the index's `IndexRunner`.
     * @param builder_specs The index's `IndexBuilder` specifications, one per supported range of
     * archive versions.
     * @return A void result on success, or an error code indicating the failure:
     * - IndexErrorCodeEnum::DuplicateIndexName if an index is already registered with `name`.
     * - IndexErrorCodeEnum::DuplicateIndexId if an index is already registered with `index_id`.
     */
    [[nodiscard]] auto register_index(
            std::string name,
            index_id_t index_id,
            IndexRunnerFactory runner_factory,
            std::vector<IndexBuilderSpecification> builder_specs
    ) -> ystdlib::error_handling::Result<void>;

    /**
     * Creates an `IndexBuilder` for the index registered with the given name, selecting the builder
     * specification that supports the Packed Filter's archive version.
     * @param name The name of the index.
     * @param config Implementation-defined configuration forwarded to the builder's factory.
     * @param packed_filter_spec A description of the Packed Filter being built.
     * @return A result containing the created builder on success, or an error code indicating the
     * failure:
     * - IndexErrorCodeEnum::UnknownIndexName if no index is registered with `name`.
     * - IndexErrorCodeEnum::UnsupportedArchiveVersion if no registered builder supports the archive
     *   version.
     * - Forwards the builder factory's return values on failure.
     */
    [[nodiscard]] auto create_writer(
            std::string_view name,
            nlohmann::json const& config,
            PackedFilterSpecification const& packed_filter_spec
    ) -> ystdlib::error_handling::Result<std::unique_ptr<IndexBuilder>>;

    /**
     * Creates a `PackedFilterBuilder` that builds the requested indexes over the given archives.
     * Each request's builder is selected from its index's specifications based on the shared archive
     * version, exactly as in `create_writer`.
     * @param archive_ids The archive ID of each archive, indexed by local archive ID.
     * @param archive_version The shared archive version of every archive being indexed.
     * @param index_requests The indexes to build.
     * @return A result containing the created builder on success, or an error code indicating the
     * failure:
     * - IndexErrorCodeEnum::UnknownIndexName if a request names an unregistered index.
     * - IndexErrorCodeEnum::UnsupportedArchiveVersion if no registered builder supports the archive
     *   version.
     * - Forwards a builder factory's return values on failure.
     */
    [[nodiscard]] auto create_packed_filter_builder(
            std::vector<std::string> archive_ids,
            archive_version_t archive_version,
            std::vector<PackedFilterIndexRequest> const& index_requests
    ) -> ystdlib::error_handling::Result<PackedFilterBuilder>;

    /**
     * Creates an `IndexRunner` for the index registered with the given Index ID.
     * @param index_id The Index ID read from the Packed Filter.
     * @param index_version The index version read from the Packed Filter.
     * @param archive_blobs The index's serialized data for each archive, indexed by local archive
     * ID.
     * @return A result containing the created runner on success, or an error code indicating the
     * failure:
     * - IndexErrorCodeEnum::UnknownIndexId if no index is registered with `index_id`.
     * - Forwards the runner factory's return values on failure.
     */
    [[nodiscard]] auto create_reader(
            index_id_t index_id,
            index_version_t index_version,
            std::vector<std::span<char const>> const& archive_blobs
    ) -> ystdlib::error_handling::Result<std::unique_ptr<IndexRunner>>;

private:
    // Types
    struct RegisteredIndex {
        RegisteredIndex(
                IndexRunnerFactory runner_factory,
                std::vector<IndexBuilderSpecification> builder_specs
        )
                : runner_factory{runner_factory},
                  builder_specs{std::move(builder_specs)} {}

        IndexRunnerFactory runner_factory;
        std::vector<IndexBuilderSpecification> builder_specs;
    };

    struct SelectedBuilderSpec {
        index_id_t index_id{};
        IndexBuilderSpecification const* spec{};
    };

    // Methods
    /**
     * Resolves a registered index's name and a Packed Filter's archive version to the Index ID and
     * the builder specification that supports that archive version. Shared by `create_writer` and
     * `create_packed_filter_builder`.
     * @param name
     * @param archive_version
     * @return A result containing the selected specification on success, or an error code indicating
     * the failure:
     * - IndexErrorCodeEnum::UnknownIndexName if no index is registered with `name`.
     * - IndexErrorCodeEnum::UnsupportedArchiveVersion if no registered builder supports
     *   `archive_version`.
     */
    [[nodiscard]] auto
    select_builder_spec(std::string_view name, archive_version_t archive_version) const
            -> ystdlib::error_handling::Result<SelectedBuilderSpec>;

    // Variables
    std::unordered_map<index_id_t, RegisteredIndex> m_indexes_by_id;
    std::unordered_map<std::string, index_id_t> m_index_id_by_name;
};
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_INDEX_REGISTRY_HPP
