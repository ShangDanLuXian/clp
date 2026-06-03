#include "SearchTelemetry.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/nostd/string_view.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/span_metadata.h>
#include <opentelemetry/trace/tracer.h>

#include <clp_s/Defs.hpp>
#include <clp_s/search/ast/ColumnDescriptor.hpp>
#include <clp_s/search/ast/Expression.hpp>
#include <clp_s/search/ast/FilterExpr.hpp>
#include <clp_s/search/ast/FilterOperation.hpp>
#include <clp_s/search/ast/OrExpr.hpp>

using clp_s::search::ast::ColumnDescriptor;
using clp_s::search::ast::Expression;
using clp_s::search::ast::FilterExpr;
using clp_s::search::ast::FilterOperation;
using clp_s::search::ast::OrExpr;
using opentelemetry::trace::StatusCode;

namespace clp_s::search {
namespace {
constexpr char cTracerName[]{"clp_s.search"};
constexpr char cSearchArchiveSpanName[]{"clp_s.search.archive"};

/**
 * @param value
 * @return `value` clamped to the maximum `int64_t`, since OpenTelemetry span attributes are signed.
 */
[[nodiscard]] auto to_int64_attribute(uint64_t value) -> int64_t;

/**
 * @param column
 * @return Whether any descriptor in the column is a wildcard.
 */
[[nodiscard]] auto descriptor_has_wildcard(ColumnDescriptor const& column) -> bool;

/**
 * Increments the column-shape counter in `telemetry` corresponding to `column`'s wildcard usage.
 * @param telemetry
 * @param column
 */
auto add_column_shape(SearchTelemetry& telemetry, ColumnDescriptor const& column) -> void;

/**
 * Increments the predicate-type counters in `telemetry` for `filter`'s operation and operand type.
 * @param telemetry
 * @param filter
 */
auto add_predicate_type(SearchTelemetry& telemetry, FilterExpr const& filter) -> void;

/**
 * Recursively walks `expr`, accumulating query-shape metrics (column shapes, predicate types,
 * predicate count, and whether an OR clause is present) into `telemetry`.
 * @param telemetry
 * @param expr
 */
auto collect_query_shape_metrics(
        SearchTelemetry& telemetry,
        std::shared_ptr<Expression> const& expr
) -> void;

/**
 * Sets a `uint64_t`-valued attribute on `span`, clamping the value to the signed range expected by
 * OpenTelemetry span attributes.
 * @param span
 * @param key
 * @param value
 */
auto set_uint64_attribute(
        opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> const& span,
        opentelemetry::nostd::string_view key,
        uint64_t value
) -> void;

auto to_int64_attribute(uint64_t value) -> int64_t {
    constexpr auto cMaxInt64{static_cast<uint64_t>(std::numeric_limits<int64_t>::max())};
    return value > cMaxInt64 ? std::numeric_limits<int64_t>::max() : static_cast<int64_t>(value);
}

auto descriptor_has_wildcard(ColumnDescriptor const& column) -> bool {
    return std::ranges::any_of(
            column.descriptor_begin(),
            column.descriptor_end(),
            [](auto const& descriptor) { return descriptor.wildcard(); }
    );
}

auto add_column_shape(SearchTelemetry& telemetry, ColumnDescriptor const& column) -> void {
    if (column.is_pure_wildcard()) {
        ++telemetry.column_shape_metrics.pure_wildcard;
    } else if (descriptor_has_wildcard(column)) {
        ++telemetry.column_shape_metrics.some_wildcard;
    } else {
        ++telemetry.column_shape_metrics.no_wildcard;
    }
}

auto add_predicate_type(SearchTelemetry& telemetry, FilterExpr const& filter) -> void {
    auto const op{filter.get_operation()};
    switch (op) {
        case FilterOperation::EXISTS:
        case FilterOperation::NEXISTS:
            ++telemetry.predicate_type_metrics.exists;
            return;
        case FilterOperation::EQ:
        case FilterOperation::NEQ:
            ++telemetry.predicate_type_metrics.exact_match;
            break;
        case FilterOperation::LT:
        case FilterOperation::GT:
        case FilterOperation::LTE:
        case FilterOperation::GTE:
            ++telemetry.predicate_type_metrics.range;
            break;
    }

    auto const operand{filter.get_operand()};
    if (nullptr == operand) {
        return;
    }

    std::string string_value;
    int64_t int_value{};
    double float_value{};
    bool bool_value{};
    if (operand->as_clp_string(string_value, op) || operand->as_var_string(string_value, op)) {
        if (string_value.find('*') == std::string::npos) {
            ++telemetry.predicate_type_metrics.string;
        } else {
            ++telemetry.predicate_type_metrics.string_with_wildcard;
        }
    }
    if (operand->as_int(int_value, op) || operand->as_timestamp()) {
        ++telemetry.predicate_type_metrics.integer;
    }
    if (operand->as_float(float_value, op)) {
        ++telemetry.predicate_type_metrics.floating_point;
    }
    if (operand->as_null(op)) {
        ++telemetry.predicate_type_metrics.null;
    }
    static_cast<void>(operand->as_bool(bool_value, op));
}

auto collect_query_shape_metrics(
        SearchTelemetry& telemetry,
        std::shared_ptr<Expression> const& expr
) -> void {
    if (nullptr == expr) {
        return;
    }
    if (nullptr != std::dynamic_pointer_cast<OrExpr>(expr)) {
        telemetry.contains_or_clause = true;
    }
    if (auto const filter{std::dynamic_pointer_cast<FilterExpr>(expr)}; nullptr != filter) {
        ++telemetry.num_predicates;
        add_column_shape(telemetry, *filter->get_column());
        add_predicate_type(telemetry, *filter);
        return;
    }
    for (auto it{expr->op_begin()}; it != expr->op_end(); ++it) {
        if (auto const child{std::dynamic_pointer_cast<Expression>(*it)}; nullptr != child) {
            collect_query_shape_metrics(telemetry, child);
        }
    }
}

auto set_uint64_attribute(
        opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> const& span,
        opentelemetry::nostd::string_view key,
        uint64_t value
) -> void {
    span->SetAttribute(key, to_int64_attribute(value));
}
}  // namespace

class SearchTelemetrySpan::Impl {
public:
    Impl()
            : m_span{opentelemetry::trace::Provider::GetTracerProvider()
                             ->GetTracer(cTracerName)
                             ->StartSpan(cSearchArchiveSpanName)},
              m_scope{std::make_unique<opentelemetry::trace::Scope>(m_span)} {
        m_span->SetAttribute("clp.search.success", true);
    }

    ~Impl() { m_span->End(); }

    Impl(Impl const&) = delete;
    auto operator=(Impl const&) -> Impl& = delete;

    Impl(Impl&&) = delete;
    auto operator=(Impl&&) -> Impl& = delete;

    auto set_error(std::string_view message) -> void {
        m_span->SetAttribute("clp.search.success", false);
        m_span->SetAttribute("clp.search.error", opentelemetry::nostd::string_view{
                                                         message.data(),
                                                         message.size()
                                                 });
        m_span->SetStatus(StatusCode::kError, opentelemetry::nostd::string_view{
                                                      message.data(),
                                                      message.size()
                                              });
    }

    auto set_telemetry(SearchTelemetry const& telemetry) -> void {
        set_uint64_attribute(
                m_span,
                "clp.query_shape.column_types.pure_wildcard",
                telemetry.column_shape_metrics.pure_wildcard
        );
        set_uint64_attribute(
                m_span,
                "clp.query_shape.column_types.some_wildcard",
                telemetry.column_shape_metrics.some_wildcard
        );
        set_uint64_attribute(
                m_span,
                "clp.query_shape.column_types.no_wildcard",
                telemetry.column_shape_metrics.no_wildcard
        );
        set_uint64_attribute(
                m_span,
                "clp.query_shape.predicate_types.string",
                telemetry.predicate_type_metrics.string
        );
        set_uint64_attribute(
                m_span,
                "clp.query_shape.predicate_types.string_with_wildcard",
                telemetry.predicate_type_metrics.string_with_wildcard
        );
        set_uint64_attribute(
                m_span,
                "clp.query_shape.predicate_types.int",
                telemetry.predicate_type_metrics.integer
        );
        set_uint64_attribute(
                m_span,
                "clp.query_shape.predicate_types.float",
                telemetry.predicate_type_metrics.floating_point
        );
        set_uint64_attribute(
                m_span,
                "clp.query_shape.predicate_types.null",
                telemetry.predicate_type_metrics.null
        );
        set_uint64_attribute(
                m_span,
                "clp.query_shape.predicate_types.exact_match",
                telemetry.predicate_type_metrics.exact_match
        );
        set_uint64_attribute(
                m_span,
                "clp.query_shape.predicate_types.range",
                telemetry.predicate_type_metrics.range
        );
        set_uint64_attribute(
                m_span,
                "clp.query_shape.predicate_types.exists",
                telemetry.predicate_type_metrics.exists
        );
        set_uint64_attribute(
                m_span,
                "clp.query_shape.num_predicates",
                telemetry.num_predicates
        );
        m_span->SetAttribute(
                "clp.query_shape.contains_or_clause",
                telemetry.contains_or_clause
        );
        if (telemetry.time_range_millis.has_value()) {
            set_uint64_attribute(
                    m_span,
                    "clp.query_shape.time_range_millis",
                    *telemetry.time_range_millis
            );
        }

        set_uint64_attribute(
                m_span,
                "clp.search.total_archive_records",
                telemetry.total_archive_records
        );
        set_uint64_attribute(
                m_span,
                "clp.search.candidate_records_after_schema_matching",
                telemetry.candidate_records_after_schema_matching
        );
        set_uint64_attribute(
                m_span,
                "clp.search.records_matching_query",
                telemetry.records_matching_query
        );
        if (0 != telemetry.total_archive_records) {
            m_span->SetAttribute(
                    "clp.search.overall_selectivity",
                    static_cast<double>(telemetry.records_matching_query)
                            / static_cast<double>(telemetry.total_archive_records)
            );
            m_span->SetAttribute(
                    "clp.search.schema_matching_selectivity",
                    static_cast<double>(telemetry.candidate_records_after_schema_matching)
                            / static_cast<double>(telemetry.total_archive_records)
            );
        }
        m_span->SetAttribute(
                "clp.search.terminated_after_time_range_matching",
                telemetry.terminated_after_time_range_matching
        );
        m_span->SetAttribute(
                "clp.search.terminated_after_schema_matching",
                telemetry.terminated_after_schema_matching
        );
        m_span->SetAttribute(
                "clp.search.terminated_after_dictionary_search",
                telemetry.terminated_after_dictionary_search
        );
        m_span->SetAttribute(
                "clp.search.terminated_after_range_index_matching",
                telemetry.terminated_after_range_index_matching
        );
        m_span->SetAttribute(
                "clp.search.terminated_after_ert_scan",
                telemetry.terminated_after_ert_scan
        );
        m_span->SetAttribute(
                "clp.search.termination_stage",
                opentelemetry::nostd::string_view{
                        telemetry.termination_stage.data(),
                        telemetry.termination_stage.size()
                }
        );

        // Emit per-schema counters as span events so they can be aggregated downstream while
        // remaining inspectable for individual matched schemas.
        for (auto const& schema : telemetry.per_schema_metrics) {
            m_span->AddEvent(
                    "clp.search.schema_result",
                    {{"clp.search.schema_id", static_cast<int64_t>(schema.schema_id)},
                     {"clp.search.schema.candidate_records",
                      to_int64_attribute(schema.candidate_records)},
                     {"clp.search.schema.matched_records",
                      to_int64_attribute(schema.matched_records)}}
            );
        }
    }

private:
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> m_span;
    std::unique_ptr<opentelemetry::trace::Scope> m_scope;
};

SearchTelemetrySpan::SearchTelemetrySpan() : m_impl{std::make_unique<Impl>()} {}

SearchTelemetrySpan::~SearchTelemetrySpan() = default;

auto SearchTelemetrySpan::set_error(std::string_view message) -> void {
    m_impl->set_error(message);
}

auto SearchTelemetrySpan::set_telemetry(SearchTelemetry const& telemetry) -> void {
    m_impl->set_telemetry(telemetry);
}

auto collect_query_shape_metrics(
        std::shared_ptr<ast::Expression> const& expr,
        std::optional<epochtime_t> search_begin_ts,
        std::optional<epochtime_t> search_end_ts
) -> SearchTelemetry {
    SearchTelemetry telemetry;
    collect_query_shape_metrics(telemetry, expr);
    if (search_begin_ts.has_value() && search_end_ts.has_value()) {
        auto const time_range_millis{*search_end_ts - *search_begin_ts};
        if (0 <= time_range_millis) {
            telemetry.time_range_millis = static_cast<uint64_t>(time_range_millis);
        }
    }
    return telemetry;
}
}  // namespace clp_s::search
