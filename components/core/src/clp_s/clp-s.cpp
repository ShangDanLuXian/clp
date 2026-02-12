#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <mongocxx/instance.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

#include "../clp/CurlGlobalInstance.hpp"
#include "../clp/BufferReader.hpp"
#include "../clp/ir/constants.hpp"
#include "../clp/string_utils/string_utils.hpp"
#include "../clp/streaming_archive/ArchiveMetadata.hpp"
#include "../reducer/network_utils.hpp"
#include "CommandLineArguments.hpp"
#include "Defs.hpp"
#include "JsonConstructor.hpp"
#include "JsonParser.hpp"
#include "kv_ir_search.hpp"
#include "OutputHandlerImpl.hpp"
#include "filter/FilterFile.hpp"
#include "filter/ProbabilisticFilter.hpp"
#include "search/AddTimestampConditions.hpp"
#include "search/ast/AndExpr.hpp"
#include "search/ast/ConvertToExists.hpp"
#include "search/ast/EmptyExpr.hpp"
#include "search/ast/Expression.hpp"
#include "search/ast/FilterExpr.hpp"
#include "search/ast/NarrowTypes.hpp"
#include "search/ast/OrExpr.hpp"
#include "search/ast/OrOfAndForm.hpp"
#include "search/ast/SearchUtils.hpp"
#include "search/ast/SetTimestampLiteralPrecision.hpp"
#include "search/ast/TimestampLiteral.hpp"
#include "search/EvaluateRangeIndexFilters.hpp"
#include "search/EvaluateTimestampIndex.hpp"
#include "search/kql/kql.hpp"
#include "search/Output.hpp"
#include "search/OutputHandler.hpp"
#include "search/Projection.hpp"
#include "search/SchemaMatch.hpp"
#include "TimestampPattern.hpp"

using namespace clp_s::search;
using clp_s::cArchiveFormatDevelopmentVersionFlag;
using clp_s::cEpochTimeMax;
using clp_s::cEpochTimeMin;
using clp_s::CommandLineArguments;
using clp_s::KvIrSearchError;
using clp_s::KvIrSearchErrorEnum;

namespace {
/**
 * Compresses the input files specified by the command line arguments into an archive.
 * @param command_line_arguments
 * @return Whether compression was successful
 */
bool compress(CommandLineArguments const& command_line_arguments);

/**
 * Decompresses the archive specified by the given JsonConstructorOption.
 * @param json_constructor_option
 */
void decompress_archive(clp_s::JsonConstructorOption const& json_constructor_option);

/**
 * Searches the given archive.
 * @param command_line_arguments
 * @param archive_reader
 * @param expr A copy of the search AST which may be modified
 * @param reducer_socket_fd
 * @return Whether the search succeeded
 */
bool search_archive(
        CommandLineArguments const& command_line_arguments,
        std::shared_ptr<clp_s::ArchiveReader> const& archive_reader,
        std::shared_ptr<ast::Expression> expr,
        int reducer_socket_fd
);

struct FilterTermExtractionResult {
    bool supported{true};
    std::string reason;
    std::vector<std::string> terms;
};

struct FilterPackFooter {
    uint64_t body_offset{0};
    uint64_t index_offset{0};
    uint64_t index_size{0};
};

struct FilterPackIndexEntry {
    std::string archive_id;
    uint64_t offset{0};
    uint32_t size{0};
};

constexpr char kFilterPackMagic[4] = {'C', 'L', 'P', 'F'};
constexpr uint32_t kFilterPackVersion = 1;
constexpr char kFilterPackIndexMagic[4] = {'C', 'L', 'P', 'I'};
constexpr uint32_t kFilterPackIndexVersion = 1;
constexpr size_t kFilterPackFooterSize = 4 + sizeof(uint32_t) + sizeof(uint64_t) * 3;
constexpr size_t kFilterPackIndexHeaderSize = 4 + sizeof(uint32_t) * 2;

auto read_uint32_le(std::vector<char> const& data, size_t offset, uint32_t& value) -> bool;
auto read_uint64_le(std::vector<char> const& data, size_t offset, uint64_t& value) -> bool;
auto parse_filter_pack_footer(
        std::vector<char> const& data,
        FilterPackFooter& footer,
        std::string& error
) -> bool;
auto parse_filter_pack_index(
        std::vector<char> const& data,
        FilterPackFooter const& footer,
        std::vector<FilterPackIndexEntry>& entries,
        std::string& error
) -> bool;
auto read_file_bytes(std::string const& path, std::vector<char>& data, std::string& error) -> bool;
void collect_filter_terms(
        std::shared_ptr<ast::Expression> const& expr,
        bool inverted_context,
        FilterTermExtractionResult& result
);
auto run_filter_scan(CommandLineArguments const& command_line_arguments) -> int;

bool compress(CommandLineArguments const& command_line_arguments) {
    auto archives_dir = std::filesystem::path(command_line_arguments.get_archives_dir());

    // Create output directory in case it doesn't exist
    try {
        std::filesystem::create_directory(archives_dir.string());
    } catch (std::exception& e) {
        SPDLOG_ERROR(
                "Failed to create archives directory {} - {}",
                archives_dir.string(),
                e.what()
        );
        return false;
    }

    clp_s::JsonParserOption option{};
    option.input_paths = command_line_arguments.get_input_paths();
    option.network_auth = command_line_arguments.get_network_auth();
    option.archives_dir = archives_dir.string();
    option.target_encoded_size = command_line_arguments.get_target_encoded_size();
    option.max_document_size = command_line_arguments.get_max_document_size();
    option.min_table_size = command_line_arguments.get_minimum_table_size();
    option.compression_level = command_line_arguments.get_compression_level();
    option.timestamp_key = command_line_arguments.get_timestamp_key();
    option.print_archive_stats = command_line_arguments.print_archive_stats();
    option.retain_float_format = command_line_arguments.get_retain_float_format();
    option.single_file_archive = command_line_arguments.get_single_file_archive();
    option.structurize_arrays = command_line_arguments.get_structurize_arrays();
    option.record_log_order = command_line_arguments.get_record_log_order();
    option.filter_config = command_line_arguments.get_filter_config();
    option.filter_output_dir = command_line_arguments.get_var_filter_output_dir();

    clp_s::JsonParser parser(option);
    if (false == parser.ingest()) {
        SPDLOG_ERROR("Encountered error while parsing input.");
        return false;
    }
    std::ignore = parser.store();
    return true;
}

void decompress_archive(clp_s::JsonConstructorOption const& json_constructor_option) {
    clp_s::JsonConstructor constructor(json_constructor_option);
    constructor.store();
}

bool search_archive(
        CommandLineArguments const& command_line_arguments,
        std::shared_ptr<clp_s::ArchiveReader> const& archive_reader,
        std::shared_ptr<ast::Expression> expr,
        int reducer_socket_fd
) {
    auto const& query = command_line_arguments.get_query();

    auto timestamp_dict = archive_reader->get_timestamp_dictionary();
    AddTimestampConditions add_timestamp_conditions(
            timestamp_dict->get_authoritative_timestamp_tokenized_column(),
            command_line_arguments.get_search_begin_ts(),
            command_line_arguments.get_search_end_ts()
    );
    if (expr = add_timestamp_conditions.run(expr); std::dynamic_pointer_cast<ast::EmptyExpr>(expr))
    {
        SPDLOG_ERROR(
                "Query '{}' specified timestamp filters tge {} tle {}, but no authoritative "
                "timestamp column was found for this archive",
                query,
                command_line_arguments.get_search_begin_ts().value_or(cEpochTimeMin),
                command_line_arguments.get_search_end_ts().value_or(cEpochTimeMax)
        );
        return false;
    }

    ast::OrOfAndForm standardize_pass;
    if (expr = standardize_pass.run(expr); std::dynamic_pointer_cast<ast::EmptyExpr>(expr)) {
        SPDLOG_ERROR("Query '{}' is logically false", query);
        return false;
    }

    ast::NarrowTypes narrow_pass;
    if (expr = narrow_pass.run(expr); std::dynamic_pointer_cast<ast::EmptyExpr>(expr)) {
        SPDLOG_ERROR("Query '{}' is logically false", query);
        return false;
    }

    ast::ConvertToExists convert_pass;
    if (expr = convert_pass.run(expr); std::dynamic_pointer_cast<ast::EmptyExpr>(expr)) {
        SPDLOG_ERROR("Query '{}' is logically false", query);
        return false;
    }

    EvaluateRangeIndexFilters metadata_filter_pass{
            archive_reader->get_range_index(),
            false == command_line_arguments.get_ignore_case()
    };
    if (expr = metadata_filter_pass.run(expr); std::dynamic_pointer_cast<ast::EmptyExpr>(expr)) {
        SPDLOG_INFO("No matching metadata ranges for query '{}'", query);
        return true;
    }

    // skip decompressing the archive if we won't match based on
    // the timestamp index
    EvaluateTimestampIndex timestamp_index(timestamp_dict);
    if (clp_s::EvaluatedValue::False == timestamp_index.run(expr)) {
        SPDLOG_INFO("No matching timestamp ranges for query '{}'", query);
        return true;
    }

    ast::SetTimestampLiteralPrecision date_precision_pass{
            ast::TimestampLiteral::Precision::Milliseconds
    };
    expr = date_precision_pass.run(expr);

    // Narrow against schemas
    auto match_pass = std::make_shared<SchemaMatch>(
            archive_reader->get_schema_tree(),
            archive_reader->get_schema_map()
    );
    if (expr = match_pass->run(expr); std::dynamic_pointer_cast<ast::EmptyExpr>(expr)) {
        SPDLOG_INFO("No matching schemas for query '{}'", query);
        return true;
    }

    // Populate projection
    auto projection = std::make_shared<Projection>(
            command_line_arguments.get_projection_columns().empty()
                    ? ProjectionMode::ReturnAllColumns
                    : ProjectionMode::ReturnSelectedColumns
    );
    try {
        for (auto const& column : command_line_arguments.get_projection_columns()) {
            std::vector<std::string> descriptor_tokens;
            std::string descriptor_namespace;
            if (false
                == clp_s::search::ast::tokenize_column_descriptor(
                        column,
                        descriptor_tokens,
                        descriptor_namespace
                ))
            {
                SPDLOG_ERROR("Can not tokenize invalid column: \"{}\"", column);
                return false;
            }
            projection->add_column(
                    ast::ColumnDescriptor::create_from_escaped_tokens(
                            descriptor_tokens,
                            descriptor_namespace
                    )
            );
        }
    } catch (std::exception const& e) {
        SPDLOG_ERROR("{}", e.what());
        return false;
    }
    projection->resolve_columns(archive_reader->get_schema_tree());
    archive_reader->set_projection(projection);

    std::unique_ptr<OutputHandler> output_handler;
    try {
        switch (command_line_arguments.get_output_handler_type()) {
            case CommandLineArguments::OutputHandlerType::File:
                output_handler = std::make_unique<clp_s::FileOutputHandler>(
                        command_line_arguments.get_file_output_path(),
                        true
                );
                break;
            case CommandLineArguments::OutputHandlerType::Network:
                output_handler = std::make_unique<clp_s::NetworkOutputHandler>(
                        command_line_arguments.get_network_dest_host(),
                        command_line_arguments.get_network_dest_port()
                );
                break;
            case CommandLineArguments::OutputHandlerType::Reducer:
                if (command_line_arguments.do_count_results_aggregation()) {
                    output_handler = std::make_unique<clp_s::CountOutputHandler>(reducer_socket_fd);
                } else if (command_line_arguments.do_count_by_time_aggregation()) {
                    output_handler = std::make_unique<clp_s::CountByTimeOutputHandler>(
                            reducer_socket_fd,
                            command_line_arguments.get_count_by_time_bucket_size()
                    );
                } else {
                    SPDLOG_ERROR("Unhandled aggregation type.");
                    return false;
                }
                break;
            case CommandLineArguments::OutputHandlerType::ResultsCache:
                output_handler = std::make_unique<clp_s::ResultsCacheOutputHandler>(
                        command_line_arguments.get_mongodb_uri(),
                        command_line_arguments.get_mongodb_collection(),
                        command_line_arguments.get_batch_size(),
                        command_line_arguments.get_max_num_results()
                );
                break;
            case CommandLineArguments::OutputHandlerType::Stdout:
                output_handler = std::make_unique<clp_s::StandardOutputHandler>();
                break;
            default:
                SPDLOG_ERROR("Unhandled OutputHandlerType.");
                return false;
        }
    } catch (std::exception const& e) {
        SPDLOG_ERROR("Failed to create output handler - {}", e.what());
        return false;
    }

    // output result
    Output output(
            match_pass,
            expr,
            archive_reader,
            std::move(output_handler),
            command_line_arguments.get_ignore_case()
    );
    return output.filter();
}

auto read_uint32_le(std::vector<char> const& data, size_t offset, uint32_t& value) -> bool {
    if (offset + sizeof(uint32_t) > data.size()) {
        return false;
    }
    std::memcpy(&value, data.data() + offset, sizeof(uint32_t));
    return true;
}

auto read_uint64_le(std::vector<char> const& data, size_t offset, uint64_t& value) -> bool {
    if (offset + sizeof(uint64_t) > data.size()) {
        return false;
    }
    std::memcpy(&value, data.data() + offset, sizeof(uint64_t));
    return true;
}

auto parse_filter_pack_footer(
        std::vector<char> const& data,
        FilterPackFooter& footer,
        std::string& error
) -> bool {
    if (data.size() < kFilterPackFooterSize) {
        error = "pack is too small for footer";
        return false;
    }

    size_t footer_offset = data.size() - kFilterPackFooterSize;
    if (0 != std::memcmp(data.data() + footer_offset, kFilterPackMagic, sizeof(kFilterPackMagic)))
    {
        error = "invalid pack magic";
        return false;
    }

    uint32_t version = 0;
    if (!read_uint32_le(data, footer_offset + sizeof(kFilterPackMagic), version)) {
        error = "failed to read pack version";
        return false;
    }
    if (version != kFilterPackVersion) {
        error = "unsupported pack version";
        return false;
    }

    size_t offset = footer_offset + sizeof(kFilterPackMagic) + sizeof(uint32_t);
    if (!read_uint64_le(data, offset, footer.body_offset)) {
        error = "failed to read pack body offset";
        return false;
    }
    offset += sizeof(uint64_t);
    if (!read_uint64_le(data, offset, footer.index_offset)) {
        error = "failed to read pack index offset";
        return false;
    }
    offset += sizeof(uint64_t);
    if (!read_uint64_le(data, offset, footer.index_size)) {
        error = "failed to read pack index size";
        return false;
    }

    if (footer.index_offset + footer.index_size > data.size()) {
        error = "pack index offsets are out of range";
        return false;
    }

    return true;
}

auto parse_filter_pack_index(
        std::vector<char> const& data,
        FilterPackFooter const& footer,
        std::vector<FilterPackIndexEntry>& entries,
        std::string& error
) -> bool {
    if (footer.index_offset + footer.index_size > data.size()) {
        error = "pack index offsets are out of range";
        return false;
    }

    size_t offset = static_cast<size_t>(footer.index_offset);
    if (footer.index_size < kFilterPackIndexHeaderSize) {
        error = "pack index header is truncated";
        return false;
    }
    if (0 != std::memcmp(data.data() + offset, kFilterPackIndexMagic, sizeof(kFilterPackIndexMagic)))
    {
        error = "invalid pack index magic";
        return false;
    }

    uint32_t version = 0;
    if (!read_uint32_le(data, offset + sizeof(kFilterPackIndexMagic), version)) {
        error = "failed to read index version";
        return false;
    }
    if (version != kFilterPackIndexVersion) {
        error = "unsupported pack index version";
        return false;
    }

    uint32_t num_entries = 0;
    if (!read_uint32_le(
                data,
                offset + sizeof(kFilterPackIndexMagic) + sizeof(uint32_t),
                num_entries
        ))
    {
        error = "failed to read index entry count";
        return false;
    }

    offset += kFilterPackIndexHeaderSize;
    size_t index_end = static_cast<size_t>(footer.index_offset + footer.index_size);
    entries.clear();
    entries.reserve(num_entries);
    for (uint32_t i = 0; i < num_entries; ++i) {
        if (offset >= index_end) {
            error = "pack index truncated";
            return false;
        }

        uint8_t id_len = static_cast<uint8_t>(data[offset]);
        offset += sizeof(uint8_t);
        if (offset + id_len > index_end) {
            error = "pack index truncated";
            return false;
        }

        std::string archive_id(data.data() + offset, data.data() + offset + id_len);
        offset += id_len;

        uint64_t entry_offset = 0;
        uint32_t entry_size = 0;
        if (!read_uint64_le(data, offset, entry_offset)) {
            error = "pack index truncated";
            return false;
        }
        offset += sizeof(uint64_t);
        if (!read_uint32_le(data, offset, entry_size)) {
            error = "pack index truncated";
            return false;
        }
        offset += sizeof(uint32_t);

        entries.push_back(FilterPackIndexEntry{
                std::move(archive_id),
                entry_offset,
                entry_size
        });
    }

    return true;
}

auto read_file_bytes(std::string const& path, std::vector<char>& data, std::string& error) -> bool {
    std::ifstream stream(path, std::ios::binary | std::ios::ate);
    if (!stream) {
        error = "failed to open pack file";
        return false;
    }
    std::streamsize size = stream.tellg();
    if (size < 0) {
        error = "failed to stat pack file";
        return false;
    }
    stream.seekg(0, std::ios::beg);
    data.resize(static_cast<size_t>(size));
    if (size > 0 && !stream.read(data.data(), size)) {
        error = "failed to read pack file";
        return false;
    }
    return true;
}

void collect_filter_terms(
        std::shared_ptr<ast::Expression> const& expr,
        bool inverted_context,
        FilterTermExtractionResult& result
) {
    if (!result.supported || nullptr == expr) {
        return;
    }

    bool inverted = inverted_context ^ expr->is_inverted();
    if (inverted) {
        result.supported = false;
        result.reason = "inverted-expression";
        return;
    }

    if (std::dynamic_pointer_cast<ast::OrExpr>(expr)) {
        result.supported = false;
        result.reason = "or-expression";
        return;
    }

    if (auto and_expr = std::dynamic_pointer_cast<ast::AndExpr>(expr)) {
        for (auto const& op : and_expr->get_op_list()) {
            auto child = std::dynamic_pointer_cast<ast::Expression>(op);
            if (nullptr == child) {
                result.supported = false;
                result.reason = "non-expression-operand";
                return;
            }
            collect_filter_terms(child, inverted, result);
            if (!result.supported) {
                return;
            }
        }
        return;
    }

    auto filter = std::dynamic_pointer_cast<ast::FilterExpr>(expr);
    if (nullptr == filter) {
        result.supported = false;
        result.reason = "unsupported-expression";
        return;
    }

    if (filter->get_operation() != ast::FilterOperation::EQ) {
        return;
    }

    std::string value;
    auto literal = filter->get_operand();
    if (nullptr == literal || false == literal->as_var_string(value, filter->get_operation())) {
        return;
    }

    if (ast::has_unescaped_wildcards(value)) {
        return;
    }

    value = clp::string_utils::unescape_string(value);
    result.terms.emplace_back(std::move(value));
}

auto run_filter_scan(CommandLineArguments const& command_line_arguments) -> int {
    auto const& pack_path = command_line_arguments.get_filter_pack_path();
    auto const& archive_ids = command_line_arguments.get_filter_archive_ids();
    auto const& query = command_line_arguments.get_query();

    if (archive_ids.empty()) {
        nlohmann::json output;
        output["passed"] = nlohmann::json::array();
        output["total"] = 0;
        output["skipped"] = 0;
        std::cout << output.dump() << std::endl;
        return 0;
    }

    auto query_stream = std::istringstream(query);
    auto expr = kql::parse_kql_expression(query_stream);
    if (nullptr == expr) {
        SPDLOG_ERROR("Failed to parse query for filter scan.");
        return 1;
    }

    FilterTermExtractionResult term_result;
    collect_filter_terms(expr, false, term_result);

    std::vector<std::string> unique_terms;
    std::vector<std::string> unique_terms_lower;
    if (term_result.supported) {
        std::unordered_set<std::string> seen;
        for (auto const& term : term_result.terms) {
            if (seen.insert(term).second) {
                unique_terms.emplace_back(term);
            }
        }
        unique_terms_lower = unique_terms;
        for (auto& term : unique_terms_lower) {
            clp::string_utils::to_lower(term);
        }
    }

    if (!term_result.supported || unique_terms.empty()) {
        nlohmann::json output;
        output["supported"] = term_result.supported;
        if (!term_result.supported) {
            output["reason"] = term_result.reason;
        }
        output["passed"] = archive_ids;
        output["total"] = archive_ids.size();
        output["skipped"] = 0;
        std::cout << output.dump() << std::endl;
        return 0;
    }

    std::vector<char> pack_bytes;
    std::string error;
    if (!read_file_bytes(pack_path, pack_bytes, error)) {
        SPDLOG_ERROR("Failed to read filter pack {} - {}", pack_path, error);
        return 1;
    }

    FilterPackFooter footer;
    if (!parse_filter_pack_footer(pack_bytes, footer, error)) {
        SPDLOG_ERROR("Failed to parse filter pack footer {} - {}", pack_path, error);
        return 1;
    }

    std::vector<FilterPackIndexEntry> entries;
    if (!parse_filter_pack_index(pack_bytes, footer, entries, error)) {
        SPDLOG_ERROR("Failed to parse filter pack index {} - {}", pack_path, error);
        return 1;
    }

    std::unordered_map<std::string, FilterPackIndexEntry> entry_map;
    entry_map.reserve(entries.size());
    for (auto& entry : entries) {
        entry_map.emplace(entry.archive_id, std::move(entry));
    }

    std::vector<std::string> passed;
    passed.reserve(archive_ids.size());
    size_t skipped = 0;

    for (auto const& archive_id : archive_ids) {
        auto entry_it = entry_map.find(archive_id);
        if (entry_it == entry_map.end()) {
            passed.push_back(archive_id);
            continue;
        }

        auto const& entry = entry_it->second;
        uint64_t start = footer.body_offset + entry.offset;
        uint64_t end = start + entry.size;
        if (end > pack_bytes.size()) {
            passed.push_back(archive_id);
            continue;
        }

        clp::BufferReader reader(
                pack_bytes.data() + static_cast<size_t>(start),
                entry.size
        );
        clp_s::FilterConfig config;
        clp_s::filter::ProbabilisticFilter filter;
        size_t num_elements = 0;
        if (!clp_s::filter::read_filter_file(reader, config, filter, num_elements)) {
            passed.push_back(archive_id);
            continue;
        }

        bool matches = true;
        auto const& terms_to_check = config.normalize ? unique_terms_lower : unique_terms;
        for (auto const& term : terms_to_check) {
            if (!filter.possibly_contains(term)) {
                matches = false;
                break;
            }
        }

        if (matches) {
            passed.push_back(archive_id);
        } else {
            ++skipped;
        }
    }

    SPDLOG_INFO(
            "Filter scan pack={} total={} passed={} skipped={}",
            pack_path,
            archive_ids.size(),
            passed.size(),
            skipped
    );

    nlohmann::json output;
    output["supported"] = true;
    output["passed"] = passed;
    output["total"] = archive_ids.size();
    output["skipped"] = skipped;
    std::cout << output.dump() << std::endl;
    return 0;
}
}  // namespace

int main(int argc, char const* argv[]) {
    try {
        auto stderr_logger = spdlog::stderr_logger_st("stderr");
        spdlog::set_default_logger(stderr_logger);
        spdlog::set_pattern("%Y-%m-%dT%H:%M:%S.%e%z [%l] %v");
    } catch (std::exception& e) {
        // NOTE: We can't log an exception if the logger couldn't be constructed
        return 1;
    }

    clp_s::TimestampPattern::init();
    mongocxx::instance const mongocxx_instance{};
    clp::CurlGlobalInstance const curl_instance{};

    CommandLineArguments command_line_arguments("clp-s");
    auto parsing_result = command_line_arguments.parse_arguments(argc, argv);
    switch (parsing_result) {
        case CommandLineArguments::ParsingResult::Failure:
            return 1;
        case CommandLineArguments::ParsingResult::InfoCommand:
            return 0;
        case CommandLineArguments::ParsingResult::Success:
            // Continue processing
            break;
    }

    if (CommandLineArguments::Command::Compress == command_line_arguments.get_command()) {
        try {
            if (false == compress(command_line_arguments)) {
                return 1;
            }
        } catch (std::exception const& e) {
            SPDLOG_ERROR("Encountered error during compression - {}", e.what());
            return 1;
        }
    } else if (CommandLineArguments::Command::Extract == command_line_arguments.get_command()) {
        clp_s::JsonConstructorOption option{};
        option.output_dir = command_line_arguments.get_output_dir();
        option.ordered = command_line_arguments.get_ordered_decompression();
        option.target_ordered_chunk_size = command_line_arguments.get_target_ordered_chunk_size();
        option.print_ordered_chunk_stats = command_line_arguments.print_ordered_chunk_stats();
        option.network_auth = command_line_arguments.get_network_auth();
        if (false == command_line_arguments.get_mongodb_uri().empty()) {
            option.metadata_db
                    = {command_line_arguments.get_mongodb_uri(),
                       command_line_arguments.get_mongodb_collection()};
        }

        try {
            for (auto const& archive_path : command_line_arguments.get_input_paths()) {
                option.archive_path = archive_path;
                decompress_archive(option);
            }
        } catch (std::exception const& e) {
            SPDLOG_ERROR("Encountered error during decompression - {}", e.what());
            return 1;
        }
    } else if (CommandLineArguments::Command::FilterScan
               == command_line_arguments.get_command())
    {
        return run_filter_scan(command_line_arguments);
    } else {
        auto const& query = command_line_arguments.get_query();
        auto query_stream = std::istringstream(query);
        auto expr = kql::parse_kql_expression(query_stream);
        if (nullptr == expr) {
            return 1;
        }

        if (std::dynamic_pointer_cast<ast::EmptyExpr>(expr)) {
            SPDLOG_ERROR("Query '{}' is logically false", query);
            return 1;
        }

        int reducer_socket_fd{-1};
        if (command_line_arguments.get_output_handler_type()
            == CommandLineArguments::OutputHandlerType::Reducer)
        {
            reducer_socket_fd = reducer::connect_to_reducer(
                    command_line_arguments.get_reducer_host(),
                    command_line_arguments.get_reducer_port(),
                    command_line_arguments.get_job_id()
            );
            if (-1 == reducer_socket_fd) {
                SPDLOG_ERROR("Failed to connect to reducer");
                return 1;
            }
        }

        auto archive_reader = std::make_shared<clp_s::ArchiveReader>();
        for (auto const& input_path : command_line_arguments.get_input_paths()) {
            if (std::string::npos != input_path.path.find(clp::ir::cIrFileExtension)) {
                auto const result{clp_s::search_kv_ir_stream(
                        input_path,
                        command_line_arguments,
                        expr->copy(),
                        reducer_socket_fd
                )};
                if (false == result.has_error()) {
                    continue;
                }

                auto const error{result.error()};
                if (std::errc::result_out_of_range == error) {
                    // To support real-time search, we will allow incomplete IR streams.
                    // TODO: Use dedicated error code for this case once issue #904 is resolved.
                    SPDLOG_WARN("IR stream `{}` is truncated", input_path.path);
                    continue;
                }

                if (KvIrSearchError{KvIrSearchErrorEnum::ProjectionSupportNotImplemented} == error
                    || KvIrSearchError{KvIrSearchErrorEnum::UnsupportedOutputHandlerType} == error
                    || KvIrSearchError{KvIrSearchErrorEnum::CountSupportNotImplemented} == error)
                {
                    // These errors are treated as non-fatal because they result from unsupported
                    // features. However, this approach may cause archives with this extension to be
                    // skipped if the search uses advanced features that are not yet implemented. To
                    // mitigate this, we log a warning and proceed to search the input as an
                    // archive.
                    SPDLOG_WARN(
                            "Attempted to search an IR stream using unsupported features. Falling"
                            " back to searching the input as an archive."
                    );
                } else if (KvIrSearchError{KvIrSearchErrorEnum::DeserializerCreationFailure}
                           != error)
                {
                    // If the error is `DeserializerCreationFailure`, we may continue to treat the
                    // input as an archive and retry. Otherwise, it should be considered as a
                    // non-recoverable failure and return directly.
                    SPDLOG_ERROR(
                            "Failed to search '{}' as an IR stream, error_category={}, error={}",
                            input_path.path,
                            error.category().name(),
                            error.message()
                    );
                    return 1;
                }
            }

            try {
                archive_reader->open(input_path, command_line_arguments.get_network_auth());
            } catch (std::exception const& e) {
                SPDLOG_ERROR("Failed to open archive - {}", e.what());
                return 1;
            }
            if (false
                == search_archive(
                        command_line_arguments,
                        archive_reader,
                        expr->copy(),
                        reducer_socket_fd
                ))
            {
                return 1;
            }
            archive_reader->close();
        }
    }

    return 0;
}
