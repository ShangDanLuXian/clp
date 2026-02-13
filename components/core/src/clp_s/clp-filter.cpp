#include <cctype>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/program_options.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

#include "../clp/BufferReader.hpp"
#include "../clp/string_utils/string_utils.hpp"
#include "filter/FilterFile.hpp"
#include "filter/ProbabilisticFilter.hpp"
#include "search/ast/AndExpr.hpp"
#include "search/ast/Expression.hpp"
#include "search/ast/FilterExpr.hpp"
#include "search/ast/OrExpr.hpp"
#include "search/ast/SearchUtils.hpp"
#include "search/kql/kql.hpp"

using namespace clp_s::search;

namespace {
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

struct FilterPackBuildResult {
    size_t num_filters{0};
    uint64_t size{0};
    uint64_t index_offset{0};
    uint64_t index_size{0};
};

struct FilterPackInputEntry {
    std::string archive_id;
    std::filesystem::path filter_path;
};

constexpr char kFilterPackMagic[4] = {'C', 'L', 'P', 'F'};
constexpr uint32_t kFilterPackVersion = 1;
constexpr char kFilterPackIndexMagic[4] = {'C', 'L', 'P', 'I'};
constexpr uint32_t kFilterPackIndexVersion = 1;
constexpr size_t kFilterPackFooterSize = 4 + sizeof(uint32_t) + sizeof(uint64_t) * 3;
constexpr size_t kFilterPackIndexHeaderSize = 4 + sizeof(uint32_t) * 2;

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

void append_bytes(std::vector<char>& buffer, void const* data, size_t size) {
    auto const* byte_ptr = static_cast<char const*>(data);
    buffer.insert(buffer.end(), byte_ptr, byte_ptr + size);
}

void append_uint32_le(std::vector<char>& buffer, uint32_t value) {
    append_bytes(buffer, &value, sizeof(value));
}

void append_uint64_le(std::vector<char>& buffer, uint64_t value) {
    append_bytes(buffer, &value, sizeof(value));
}

auto encode_filter_pack_index(
        std::vector<FilterPackIndexEntry> const& entries,
        std::vector<char>& out_bytes,
        std::string& error
) -> bool {
    out_bytes.clear();
    out_bytes.reserve(
            kFilterPackIndexHeaderSize
            + entries.size()
                      * (sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t) + 32)
    );
    append_bytes(out_bytes, kFilterPackIndexMagic, sizeof(kFilterPackIndexMagic));
    append_uint32_le(out_bytes, kFilterPackIndexVersion);
    append_uint32_le(out_bytes, static_cast<uint32_t>(entries.size()));

    for (auto const& entry : entries) {
        if (entry.archive_id.size() > UINT8_MAX) {
            error = "archive_id is too long to encode";
            return false;
        }
        uint8_t id_len = static_cast<uint8_t>(entry.archive_id.size());
        out_bytes.push_back(static_cast<char>(id_len));
        append_bytes(out_bytes, entry.archive_id.data(), entry.archive_id.size());
        append_uint64_le(out_bytes, entry.offset);
        append_uint32_le(out_bytes, entry.size);
    }

    return true;
}

auto encode_filter_pack_footer(FilterPackFooter const& footer, std::vector<char>& out_bytes)
        -> void {
    out_bytes.clear();
    out_bytes.reserve(kFilterPackFooterSize);
    append_bytes(out_bytes, kFilterPackMagic, sizeof(kFilterPackMagic));
    append_uint32_le(out_bytes, kFilterPackVersion);
    append_uint64_le(out_bytes, footer.body_offset);
    append_uint64_le(out_bytes, footer.index_offset);
    append_uint64_le(out_bytes, footer.index_size);
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

auto emit_json(nlohmann::json const& output, std::string const& output_path, std::string& error)
        -> bool {
    if (output_path.empty()) {
        error = "output-json must be specified";
        return false;
    }

    std::ofstream out(output_path, std::ios::binary | std::ios::trunc);
    if (!out) {
        error = "failed to open json output file";
        return false;
    }

    out << output.dump();
    if (!out) {
        error = "failed to write json output file";
        return false;
    }
    return true;
}

auto read_pack_manifest(
        std::string const& manifest_path,
        std::vector<FilterPackInputEntry>& entries,
        std::string& error
) -> bool {
    std::ifstream manifest_file(manifest_path);
    if (!manifest_file) {
        error = "failed to open manifest file";
        return false;
    }

    std::string line;
    size_t line_no = 0;
    while (std::getline(manifest_file, line)) {
        ++line_no;
        if (line.empty() || line[0] == '#') {
            continue;
        }
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }

        auto tab_pos = line.find('\t');
        if (tab_pos == std::string::npos) {
            error = "invalid manifest line " + std::to_string(line_no);
            return false;
        }
        auto archive_id = line.substr(0, tab_pos);
        auto path_str = line.substr(tab_pos + 1);
        if (archive_id.empty() || path_str.empty()) {
            error = "invalid manifest line " + std::to_string(line_no);
            return false;
        }

        entries.push_back(FilterPackInputEntry{
                std::move(archive_id),
                std::filesystem::path(path_str)
        });
    }

    if (entries.empty()) {
        error = "manifest contains no entries";
        return false;
    }

    return true;
}

auto build_filter_pack_file(
        std::filesystem::path const& output_path,
        std::vector<FilterPackInputEntry> const& inputs,
        FilterPackBuildResult& result,
        std::string& error
) -> bool {
    if (inputs.empty()) {
        error = "no filters provided";
        return false;
    }

    auto parent_dir = output_path.parent_path();
    if (!parent_dir.empty()) {
        std::filesystem::create_directories(parent_dir);
    }
    std::ofstream out(output_path, std::ios::binary | std::ios::trunc);
    if (!out) {
        error = "failed to open output pack file";
        return false;
    }

    std::vector<FilterPackIndexEntry> index_entries;
    index_entries.reserve(inputs.size());

    for (auto const& input : inputs) {
        std::error_code fs_error;
        auto size = std::filesystem::file_size(input.filter_path, fs_error);
        if (fs_error) {
            error = "failed to stat filter file " + input.filter_path.string();
            return false;
        }
        if (size > std::numeric_limits<uint32_t>::max()) {
            error = "filter file is too large";
            return false;
        }

        auto pos = out.tellp();
        if (pos < 0) {
            error = "failed to determine pack offset";
            return false;
        }
        auto offset = static_cast<uint64_t>(pos);
        std::ifstream in(input.filter_path, std::ios::binary);
        if (!in) {
            error = "failed to open filter file " + input.filter_path.string();
            return false;
        }
        out << in.rdbuf();
        if (!out) {
            error = "failed to write pack data";
            return false;
        }

        index_entries.push_back(FilterPackIndexEntry{
                input.archive_id,
                offset,
                static_cast<uint32_t>(size)
        });
    }

    auto index_pos = out.tellp();
    if (index_pos < 0) {
        error = "failed to determine index offset";
        return false;
    }
    auto index_offset = static_cast<uint64_t>(index_pos);
    std::vector<char> index_bytes;
    if (!encode_filter_pack_index(index_entries, index_bytes, error)) {
        return false;
    }
    out.write(index_bytes.data(), static_cast<std::streamsize>(index_bytes.size()));
    if (!out) {
        error = "failed to write pack index";
        return false;
    }

    FilterPackFooter footer{
            .body_offset = 0,
            .index_offset = index_offset,
            .index_size = static_cast<uint64_t>(index_bytes.size())
    };
    std::vector<char> footer_bytes;
    encode_filter_pack_footer(footer, footer_bytes);
    out.write(footer_bytes.data(), static_cast<std::streamsize>(footer_bytes.size()));
    if (!out) {
        error = "failed to write pack footer";
        return false;
    }

    out.close();
    if (!out) {
        error = "failed to finalize pack file";
        return false;
    }

    auto pack_size = std::filesystem::file_size(output_path);
    result = FilterPackBuildResult{
            .num_filters = index_entries.size(),
            .size = pack_size,
            .index_offset = index_offset,
            .index_size = static_cast<uint64_t>(index_bytes.size())
    };
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

auto run_filter_scan(
        std::string const& pack_path,
        std::vector<std::string> const& archive_ids,
        std::string const& query,
        std::string const& output_json_path
) -> int {
    if (output_json_path.empty()) {
        SPDLOG_ERROR("output-json must be specified for filter scan.");
        return 1;
    }
    if (archive_ids.empty()) {
        nlohmann::json output;
        output["passed"] = nlohmann::json::array();
        output["total"] = 0;
        output["skipped"] = 0;
        std::string error;
        if (!emit_json(output, output_json_path, error)) {
            SPDLOG_ERROR("Failed to write filter scan output - {}", error);
            return 1;
        }
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
        std::string error;
        if (!emit_json(output, output_json_path, error)) {
            SPDLOG_ERROR("Failed to write filter scan output - {}", error);
            return 1;
        }
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
    std::string emit_error;
    if (!emit_json(output, output_json_path, emit_error)) {
        SPDLOG_ERROR("Failed to write filter scan output - {}", emit_error);
        return 1;
    }
    return 0;
}

auto run_filter_pack(
        std::string const& output_path,
        std::string const& manifest_path,
        std::string const& output_json_path
) -> int {
    if (output_json_path.empty()) {
        SPDLOG_ERROR("output-json must be specified for filter pack.");
        return 1;
    }
    std::vector<FilterPackInputEntry> inputs;
    std::string error;
    if (!read_pack_manifest(manifest_path, inputs, error)) {
        SPDLOG_ERROR("Failed to read pack manifest {} - {}", manifest_path, error);
        return 1;
    }

    FilterPackBuildResult result;
    if (!build_filter_pack_file(output_path, inputs, result, error)) {
        SPDLOG_ERROR("Failed to build filter pack {} - {}", output_path, error);
        return 1;
    }

    nlohmann::json output;
    output["num_filters"] = result.num_filters;
    output["size"] = result.size;
    output["index_offset"] = result.index_offset;
    output["index_size"] = result.index_size;

    if (!emit_json(output, output_json_path, error)) {
        SPDLOG_ERROR("Failed to write filter pack output - {}", error);
        return 1;
    }

    return 0;
}

auto split_archives(std::string const& csv) -> std::vector<std::string> {
    auto trim = [](std::string& value) {
        size_t start = 0;
        while (start < value.size() && std::isspace(static_cast<unsigned char>(value[start]))) {
            ++start;
        }
        size_t end = value.size();
        while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
            --end;
        }
        value = value.substr(start, end - start);
    };

    std::vector<std::string> results;
    std::stringstream csv_stream(csv);
    std::string token;
    while (std::getline(csv_stream, token, ',')) {
        trim(token);
        if (!token.empty()) {
            results.emplace_back(std::move(token));
        }
    }
    return results;
}
}  // namespace

int main(int argc, char const* argv[]) {
    try {
        auto stderr_logger = spdlog::stderr_logger_st("stderr");
        spdlog::set_default_logger(stderr_logger);
        spdlog::set_pattern("%Y-%m-%dT%H:%M:%S.%e%z [%l] %v");
    } catch (std::exception const&) {
        return 1;
    }

    try {
        namespace po = boost::program_options;

        auto print_usage = []() {
            std::cerr << "Usage: clp-filter <command> [options]\n"
                         "Commands:\n"
                         "  scan  Scan filter pack for query terms\n"
                         "  pack  Build a filter pack from a manifest\n"
                      << std::endl;
        };

        if (argc < 2) {
            print_usage();
            return 1;
        }

        std::string command = argv[1];
        if (command == "--help" || command == "-h") {
            print_usage();
            return 0;
        }

        if (command == "scan") {
            std::string pack_path;
            std::string archives_csv;
            std::string query;
            std::string output_json_path;

            po::options_description options("Filter scan options");
            // clang-format off
            options.add_options()
                    ("help,h", "Show help")
                    ("pack-path", po::value<std::string>(&pack_path)->value_name("PATH"),
                     "Path to filter pack file")
                    ("archives", po::value<std::string>(&archives_csv)->value_name("IDS"),
                     "Comma-separated archive IDs")
                    ("query,q", po::value<std::string>(&query),
                     "Query to extract filter terms from")
                    ("output-json", po::value<std::string>(&output_json_path)->value_name("PATH"),
                     "Write JSON output to file instead of stdout");
            // clang-format on

            po::positional_options_description positional;
            positional.add("pack-path", 1);
            positional.add("archives", 1);
            positional.add("query", 1);

            int sub_argc = argc - 1;
            char const** sub_argv = argv + 1;
            po::variables_map vm;
            po::store(
                    po::command_line_parser(sub_argc, sub_argv)
                            .options(options)
                            .positional(positional)
                            .run(),
                    vm
            );
            po::notify(vm);

            if (vm.count("help")) {
                std::cerr << "Usage: clp-filter scan --pack-path <PATH> --archives <ID1,ID2> "
                             "--query <Q> --output-json <PATH>"
                          << std::endl
                          << std::endl;
                std::cerr << options << std::endl;
                return 0;
            }

            if (pack_path.empty()) {
                throw std::invalid_argument("pack-path must be specified.");
            }
            if (archives_csv.empty()) {
                throw std::invalid_argument("archives must be specified.");
            }
            if (query.empty()) {
                throw std::invalid_argument("No query specified.");
            }
            if (output_json_path.empty()) {
                throw std::invalid_argument("output-json must be specified.");
            }

            auto archive_ids = split_archives(archives_csv);
            if (archive_ids.empty()) {
                throw std::invalid_argument("archives must include at least one id.");
            }

            return run_filter_scan(pack_path, archive_ids, query, output_json_path);
        }

        if (command == "pack") {
            std::string output_path;
            std::string manifest_path;
            std::string output_json_path;

            po::options_description options("Filter pack options");
            // clang-format off
            options.add_options()
                    ("help,h", "Show help")
                    ("output,o", po::value<std::string>(&output_path)->value_name("PATH"),
                     "Output filter pack path")
                    ("manifest", po::value<std::string>(&manifest_path)->value_name("PATH"),
                     "Manifest file with archive_id and filter path per line")
                    ("output-json", po::value<std::string>(&output_json_path)->value_name("PATH"),
                     "Write JSON output to file instead of stdout");
            // clang-format on

            po::positional_options_description positional;
            positional.add("output", 1);
            positional.add("manifest", 1);

            int sub_argc = argc - 1;
            char const** sub_argv = argv + 1;
            po::variables_map vm;
            po::store(
                    po::command_line_parser(sub_argc, sub_argv)
                            .options(options)
                            .positional(positional)
                            .run(),
                    vm
            );
            po::notify(vm);

            if (vm.count("help")) {
                std::cerr << "Usage: clp-filter pack --output <PATH> --manifest <PATH>"
                             " --output-json <PATH>"
                          << std::endl
                          << std::endl;
                std::cerr << options << std::endl;
                return 0;
            }

            if (output_path.empty()) {
                throw std::invalid_argument("output must be specified.");
            }
            if (manifest_path.empty()) {
                throw std::invalid_argument("manifest must be specified.");
            }
            if (output_json_path.empty()) {
                throw std::invalid_argument("output-json must be specified.");
            }

            return run_filter_pack(output_path, manifest_path, output_json_path);
        }

        print_usage();
        return 1;
    } catch (std::exception const& e) {
        SPDLOG_ERROR("{}", e.what());
        std::cerr << "Try --help for usage." << std::endl;
        return 1;
    }
}
