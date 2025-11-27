#include "Output.hpp"

#include <cstdint>
#include <memory>
#include <vector>

#include <spdlog/spdlog.h>

#include "../../clp/type_utils.hpp"
#include "../SchemaTree.hpp"
#include "../Utils.hpp"
#include "ast/AndExpr.hpp"
#include "ast/ColumnDescriptor.hpp"
#include "ast/Expression.hpp"
#include "ast/FilterExpr.hpp"
#include "ast/FilterOperation.hpp"
#include "ast/Literal.hpp"
#include "ast/OrExpr.hpp"
#include "EvaluateTimestampIndex.hpp"
#include "clp_s/DictionaryReader.hpp"
#include "clp_s/archive_constants.hpp"
#include "clp/Stopwatch.hpp"

using clp_s::search::ast::AndExpr;
using clp_s::search::ast::ColumnDescriptor;
using clp_s::search::ast::DescriptorList;
using clp_s::search::ast::Expression;
using clp_s::search::ast::FilterExpr;
using clp_s::search::ast::FilterOperation;
using clp_s::search::ast::Literal;
using clp_s::search::ast::literal_type_bitmask_t;
using clp_s::search::ast::LiteralType;
using clp_s::search::ast::OpList;
using clp_s::search::ast::OrExpr;

#define eval(op, a, b) (((op) == FilterOperation::EQ) ? ((a) == (b)) : ((a) != (b)))

namespace clp_s::search {
bool Output::filter() {
    clp::Stopwatch schema_processing_stopwatch;
    std::vector<int32_t> matched_schemas;
    bool has_array = false;
    bool has_array_search = false;

    m_archive_reader->read_metadata();
    for (auto schema_id : m_archive_reader->get_schema_ids()) {
        if (m_match->schema_matched(schema_id)) {
            matched_schemas.push_back(schema_id);
            if (m_match->has_array(schema_id)) {
                has_array = true;
            }
            if (m_match->has_array_search(schema_id)) {
                has_array_search = true;
            }
        }
    }

    // Skip decompressing archive if it contains no
    // relevant schemas
    if (matched_schemas.empty()) {
        return true;
    }

    // Skip decompressing the rest of the archive if it won't match based on the timestamp range
    // index. This check happens a second time here because some ambiguous columns may now match the
    // timestamp column after column resolution.
    EvaluateTimestampIndex timestamp_index(m_archive_reader->get_timestamp_dictionary());
    if (EvaluatedValue::False == timestamp_index.run(m_expr)) {
        m_archive_reader->close();
        return true;
    }

    // skip if not in the filter
    if (m_use_filter && !filter_passed(m_ignore_case)) {
        return true;
    }

    m_archive_reader->read_variable_dictionary();
    m_archive_reader->read_log_type_dictionary();

    if (has_array) {
        if (has_array_search) {
            m_archive_reader->read_array_dictionary();
        } else {
            m_archive_reader->read_array_dictionary(true);
        }
    }

    m_query_runner.global_init();
    m_archive_reader->preload_schema_filters(matched_schemas);
    m_archive_reader->preload_schema_int_filters(matched_schemas);
    m_archive_reader->open_packed_streams();

    std::string message;
    auto const archive_id = m_archive_reader->get_archive_id();
    for (int32_t schema_id : matched_schemas) {
        schema_processing_stopwatch.start();
        if (EvaluatedValue::False == m_query_runner.schema_init(schema_id)) {
            continue;
        }

        // Check filter before loading ERT
        auto searched_var_ids = m_query_runner.get_searched_variable_ids();
        if (!m_archive_reader->schema_filter_check(schema_id, searched_var_ids)) {
            continue;
        }
        auto& reader = m_archive_reader->read_schema_table(
                schema_id,
                m_output_handler->should_output_metadata(),
                m_should_marshal_records
        );
        reader.initialize_filter(&m_query_runner);

        auto schema_expr = m_match->get_query_for_schema(schema_id);


        if (auto filter = std::dynamic_pointer_cast<ast::FilterExpr>(schema_expr)) {
            auto column_id = filter->get_column()->get_column_id();

            if (filter->get_column()->get_literal_type() == ast::IntegerT) {

                int64_t tmp_int;
                filter->get_operand()->as_int(tmp_int, filter->get_operation());


                if (!m_archive_reader->schema_int_filter_check(schema_id, column_id, tmp_int)) {

                    continue;
                }
            }
         }


        size_t messages_in_schema = 0;

        if (m_output_handler->should_output_metadata()) {
            epochtime_t timestamp{};
            int64_t log_event_idx{};
            while (reader.get_next_message_with_metadata(
                    message,
                    timestamp,
                    log_event_idx,
                    &m_query_runner
            ))
            {
                m_output_handler->write(message, timestamp, archive_id, log_event_idx);
                messages_in_schema++;
            }
        } else {
            while (reader.get_next_message(message, &m_query_runner)) {
                m_output_handler->write(message);
                messages_in_schema++;
            }
        }
        schema_processing_stopwatch.stop();
        m_query_runner.log_counts();
        SPDLOG_INFO(
            "[PERF] Schema processing - schema_id={}, messages_output={}, time={:.3f}ms",
            schema_id,
            messages_in_schema,
            schema_processing_stopwatch.get_time_taken_in_seconds() * 1000.0
    );
    schema_processing_stopwatch.reset();
        auto ecode = m_output_handler->flush();
        if (ErrorCode::ErrorCodeSuccess != ecode) {
            SPDLOG_ERROR(
                    "Failed to flush output handler, error={}.",
                    clp::enum_to_underlying_type(ecode)
            );
            return false;
        }
    }
    auto ecode = m_output_handler->finish();
    if (ErrorCode::ErrorCodeSuccess != ecode) {
        SPDLOG_ERROR(
                "Failed to flush output handler, error={}.",
                clp::enum_to_underlying_type(ecode)
        );
        return false;
    }
    return true;
}

void Output::extract_var_search_strings(
    std::shared_ptr<ast::Expression> const& expr,
    std::unordered_set<std::string>& search_strings
) {
if (nullptr == expr) {
    return;
}

// Recursively process nested expressions
if (expr->has_only_expression_operands()) {
    for (auto const& op : expr->get_op_list()) {
        extract_var_search_strings(std::static_pointer_cast<ast::Expression>(op), search_strings);
    }
    return;
}

// Check if this is a filter expression
auto filter = std::dynamic_pointer_cast<ast::FilterExpr>(expr);
if (nullptr == filter) {
    return;
}

// Skip EXISTS/NEXISTS operations
if (filter->get_operation() == ast::FilterOperation::EXISTS
    || filter->get_operation() == ast::FilterOperation::NEXISTS)
{
    return;
}

// Extract variable string literals
if (filter->get_column()->matches_type(ast::LiteralType::VarStringT)) {
    std::string query_string;
    filter->get_operand()->as_var_string(query_string, filter->get_operation());

    // Only extract non-wildcard strings (wildcards need full dictionary)
    if (false == ast::has_unescaped_wildcards(query_string)) {
        auto const unescaped_query_string{clp::string_utils::unescape_string(query_string)};
        search_strings.insert(unescaped_query_string);
    }
}
}

bool Output::filter_passed(bool ignore_case) {

    auto var_dict = m_archive_reader->get_variable_dictionary();

    // If filter is not loaded or disabled, we must load the dictionary
    if (!var_dict->load_filter(std::string(constants::cArchiveVarDictFile) + constants::cArchiveFilterFileSuffix)) {
        SPDLOG_INFO("[FILTER] Filter not available, dictionary load required");
        return true;
    }

    // Extract all variable string search terms from the query
    std::unordered_set<std::string> search_strings;
    extract_var_search_strings(m_expr, search_strings);

    // If no exact-match search strings found (e.g., all wildcards), we need the dictionary
    if (search_strings.empty()) {
        SPDLOG_DEBUG(
                "[FILTER] No exact-match search strings found (wildcards/complex query), "
                "dictionary load required"
        );
        return true;
    }

    // For case-insensitive searches, filter can't help (it's case-sensitive)
    if (ignore_case) {
        SPDLOG_DEBUG("[FILTER] Case-insensitive search, dictionary load required");
        return true;
    }

    // Check each search string against the filter
    size_t strings_checked = 0;
    size_t filter_passes = 0;
    size_t filter_rejects = 0;

    for (auto const& search_string : search_strings) {
        strings_checked++;
        if (var_dict->filter_might_contain(search_string)) {
            filter_passes++;
            SPDLOG_DEBUG("[FILTER] String '{}' might exist (filter pass)", search_string);
        } else {
            filter_rejects++;
            SPDLOG_DEBUG(
                    "[FILTER] String '{}' definitely doesn't exist (filter reject)",
                    search_string
            );
        }
    }

    SPDLOG_INFO(
            "[FILTER] Pre-check: {} search string(s), {} passed, {} rejected by filter",
            strings_checked,
            filter_passes,
            filter_rejects
    );

    // Only load dictionary if at least one string passed the filter
    if (filter_passes > 0) {
        SPDLOG_INFO("[FILTER] Dictionary load required ({} string(s) might exist)", filter_passes);
        return true;
    } else {
        SPDLOG_INFO(
                "[FILTER] Skipping dictionary load - all {} search string(s) rejected by filter",
                strings_checked
        );
        return false;
    }
}
}  // namespace clp_s::search
