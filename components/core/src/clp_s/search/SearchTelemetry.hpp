#ifndef CLP_S_SEARCH_SEARCHTELEMETRY_HPP
#define CLP_S_SEARCH_SEARCHTELEMETRY_HPP

#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include <clp_s/Defs.hpp>
#include <clp_s/search/ast/Expression.hpp>

namespace clp_s::search {
struct SearchTelemetry {
    // Per-schema counters recorded for each schema that survives schema matching. These are emitted
    // as span events so they can be aggregated downstream while still being inspectable per schema.
    struct SchemaTelemetry {
        int32_t schema_id{};
        uint64_t candidate_records{};
        uint64_t matched_records{};
    };

    struct ColumnShapeMetrics {
        uint64_t pure_wildcard{};
        uint64_t some_wildcard{};
        uint64_t no_wildcard{};
    };

    struct PredicateTypeMetrics {
        uint64_t string{};
        uint64_t string_with_wildcard{};
        uint64_t integer{};
        uint64_t floating_point{};
        uint64_t null{};
        uint64_t exact_match{};
        uint64_t range{};
        uint64_t exists{};
    };

    ColumnShapeMetrics column_shape_metrics;
    PredicateTypeMetrics predicate_type_metrics;
    uint64_t num_predicates{};
    bool contains_or_clause{};
    std::optional<uint64_t> time_range_millis;

    uint64_t total_archive_records{};
    uint64_t candidate_records_after_schema_matching{};
    uint64_t records_matching_query{};
    std::vector<SchemaTelemetry> per_schema_metrics;

    bool terminated_after_time_range_matching{};
    bool terminated_after_schema_matching{};
    bool terminated_after_dictionary_search{};
    bool terminated_after_range_index_matching{};
    bool terminated_after_ert_scan{};
    std::string_view termination_stage{"record_scan"};
};

class SearchTelemetrySpan {
public:
    SearchTelemetrySpan();
    ~SearchTelemetrySpan();

    SearchTelemetrySpan(SearchTelemetrySpan const&) = delete;
    auto operator=(SearchTelemetrySpan const&) -> SearchTelemetrySpan& = delete;

    SearchTelemetrySpan(SearchTelemetrySpan&&) noexcept;
    auto operator=(SearchTelemetrySpan&&) noexcept -> SearchTelemetrySpan&;

    auto set_error(std::string_view message) -> void;
    auto set_telemetry(SearchTelemetry const& telemetry) -> void;

private:
    class Impl;
    std::unique_ptr<Impl> m_impl;
};

[[nodiscard]] auto collect_query_shape_metrics(
        std::shared_ptr<ast::Expression> const& expr,
        std::optional<epochtime_t> search_begin_ts,
        std::optional<epochtime_t> search_end_ts
) -> SearchTelemetry;
}  // namespace clp_s::search

#endif  // CLP_S_SEARCH_SEARCHTELEMETRY_HPP
