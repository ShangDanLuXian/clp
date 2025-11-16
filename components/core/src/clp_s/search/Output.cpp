#include "Output.hpp"

#include <memory>
#include <vector>

#include <spdlog/spdlog.h>
#include <string_utils/string_utils.hpp>

#include "../../clp/Stopwatch.hpp"
#include "../../clp/type_utils.hpp"
#include "../archive_constants.hpp"
#include "../SchemaTree.hpp"
#include "../Utils.hpp"
#include "ast/AndExpr.hpp"
#include "ast/ColumnDescriptor.hpp"
#include "ast/Expression.hpp"
#include "ast/FilterExpr.hpp"
#include "ast/FilterOperation.hpp"
#include "ast/Literal.hpp"
#include "ast/OrExpr.hpp"
#include "ast/SearchUtils.hpp"
#include "EvaluateTimestampIndex.hpp"

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
    clp::Stopwatch total_stopwatch;
    clp::Stopwatch schema_processing_stopwatch;
    total_stopwatch.start();

    std::vector<int32_t> matched_schemas;
    bool has_array = false;
    bool has_array_search = false;

    SPDLOG_INFO("[SEARCH] Reading archive metadata");
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
        SPDLOG_INFO("[SEARCH] No schemas matched the query");

        total_stopwatch.stop();
        SPDLOG_INFO(
                "[PERF] Query execution complete (no matching schemas) - "
                "total_messages_output=0, total_time={:.3f}ms",
                total_stopwatch.get_time_taken_in_seconds() * 1000.0
        );
        return true;
    }
    SPDLOG_INFO("[SEARCH] Found {} matching schema(s)", matched_schemas.size());

    // Skip decompressing the rest of the archive if it won't match based on the timestamp range
    // index. This check happens a second time here because some ambiguous columns may now match the
    // timestamp column after column resolution.
    EvaluateTimestampIndex timestamp_index(m_archive_reader->get_timestamp_dictionary());
    if (EvaluatedValue::False == timestamp_index.run(m_expr)) {
        m_archive_reader->close();

        total_stopwatch.stop();
        SPDLOG_INFO(
                "[PERF] Query execution complete (timestamp index early return) - "
                "total_messages_output=0, total_time={:.3f}ms",
                total_stopwatch.get_time_taken_in_seconds() * 1000.0
        );
        return true;
    }

    SPDLOG_INFO("[SEARCH] Reading dictionaries");

    // Load bloom filter first (tiny and fast)
    auto var_dict = m_archive_reader->get_variable_dictionary();
    clp::Stopwatch bloom_stopwatch;
    bloom_stopwatch.start();
    if (var_dict->load_bloom_filter(clp_s::constants::cArchiveVarDictBloomFile)) {
        bloom_stopwatch.stop();
        SPDLOG_INFO(
                "[BLOOM] Loaded bloom filter - time={:.3f}ms, status=ENABLED",
                bloom_stopwatch.get_time_taken_in_seconds() * 1000.0
        );

        // Use bloom filter to determine if we need to load the dictionary
        if (!should_load_variable_dictionary(m_ignore_case)) {
            SPDLOG_INFO("[BLOOM] Skipping variable dictionary load - bloom filter optimization");
            // Dictionary won't be loaded, but we still need log type dictionary
            m_archive_reader->read_log_type_dictionary();
            // Set empty result and return early
            SPDLOG_INFO("No matching results possible based on bloom filter");

            total_stopwatch.stop();
            SPDLOG_INFO(
                    "[PERF] Query execution complete (bloom filter early return) - "
                    "total_messages_output=0, total_time={:.3f}ms",
                    total_stopwatch.get_time_taken_in_seconds() * 1000.0
            );
            return true;
        }
    } else {
        bloom_stopwatch.stop();
        SPDLOG_INFO("[BLOOM] Bloom filter not available - will load full dictionary");
    }

    // Load dictionaries (bloom filter check passed or not available)
    m_archive_reader->read_variable_dictionary();
    m_archive_reader->read_log_type_dictionary();

    if (has_array) {
        if (has_array_search) {
            SPDLOG_INFO("[SEARCH] Reading array dictionary (full)");
            m_archive_reader->read_array_dictionary();
        } else {
            SPDLOG_INFO("[SEARCH] Reading array dictionary (lazy)");
            m_archive_reader->read_array_dictionary(true);
        }
    }

    SPDLOG_INFO("[SEARCH] Initializing query runner");
    m_query_runner.global_init();
    SPDLOG_INFO("[SEARCH] Opening packed streams");
    m_archive_reader->open_packed_streams();

    std::string message;
    auto const archive_id = m_archive_reader->get_archive_id();
    size_t total_messages_processed = 0;
    for (int32_t schema_id : matched_schemas) {
        SPDLOG_INFO("[SEARCH] Processing schema {}", schema_id);
        schema_processing_stopwatch.start();

        if (EvaluatedValue::False == m_query_runner.schema_init(schema_id)) {
            SPDLOG_INFO("[SEARCH] Schema {} evaluated to false, skipping", schema_id);
            continue;
        }

        auto& reader = m_archive_reader->read_schema_table(
                schema_id,
                m_output_handler->should_output_metadata(),
                m_should_marshal_records
        );
        reader.initialize_filter(&m_query_runner);

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
        SPDLOG_INFO(
                "[PERF] Schema processing - schema_id={}, messages_output={}, time={:.3f}ms",
                schema_id,
                messages_in_schema,
                schema_processing_stopwatch.get_time_taken_in_seconds() * 1000.0
        );
        schema_processing_stopwatch.reset();

        total_messages_processed += messages_in_schema;

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

    total_stopwatch.stop();
    SPDLOG_INFO(
            "[PERF] Query execution complete - total_messages_output={}, total_time={:.3f}ms",
            total_messages_processed,
            total_stopwatch.get_time_taken_in_seconds() * 1000.0
    );

    return true;
}

void Output::extract_var_search_strings(
        std::shared_ptr<ast::Expression> const& expr,
        std::unordered_set<std::string>& search_strings
) const {
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

auto Output::should_load_variable_dictionary(bool ignore_case) const -> bool {
    auto var_dict = m_archive_reader->get_variable_dictionary();

    // If bloom filter is not loaded or disabled, we must load the dictionary
    if (!var_dict->has_bloom_filter()) {
        SPDLOG_DEBUG("[BLOOM] Bloom filter not available, dictionary load required");
        return true;
    }

    // Extract all variable string search terms from the query
    std::unordered_set<std::string> search_strings;
    extract_var_search_strings(m_expr, search_strings);

    // If no exact-match search strings found (e.g., all wildcards), we need the dictionary
    if (search_strings.empty()) {
        SPDLOG_DEBUG(
                "[BLOOM] No exact-match search strings found (wildcards/complex query), "
                "dictionary load required"
        );
        return true;
    }

    // For case-insensitive searches, bloom filter can't help (it's case-sensitive)
    if (ignore_case) {
        SPDLOG_DEBUG("[BLOOM] Case-insensitive search, dictionary load required");
        return true;
    }

    // Check each search string against the bloom filter
    size_t strings_checked = 0;
    size_t bloom_filter_passes = 0;
    size_t bloom_filter_rejects = 0;

    for (auto const& search_string : search_strings) {
        strings_checked++;
        if (var_dict->bloom_filter_might_contain(search_string)) {
            bloom_filter_passes++;
            SPDLOG_DEBUG("[BLOOM] String '{}' might exist (bloom filter pass)", search_string);
        } else {
            bloom_filter_rejects++;
            SPDLOG_DEBUG(
                    "[BLOOM] String '{}' definitely doesn't exist (bloom filter reject)",
                    search_string
            );
        }
    }

    SPDLOG_INFO(
            "[BLOOM] Pre-check: {} search string(s), {} passed, {} rejected by bloom filter",
            strings_checked,
            bloom_filter_passes,
            bloom_filter_rejects
    );

    // Only load dictionary if at least one string passed the bloom filter
    if (bloom_filter_passes > 0) {
        SPDLOG_INFO("[BLOOM] Dictionary load required ({} string(s) might exist)", bloom_filter_passes);
        return true;
    } else {
        SPDLOG_INFO(
                "[BLOOM] Skipping dictionary load - all {} search string(s) rejected by bloom filter",
                strings_checked
        );
        return false;
    }
}
}  // namespace clp_s::search
