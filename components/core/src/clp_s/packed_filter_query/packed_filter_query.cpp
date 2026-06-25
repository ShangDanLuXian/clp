#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <boost/program_options.hpp>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

#include <clp_s/filter/IndexRegistry.hpp>
#include <clp_s/filter/IndexRunner.hpp>
#include <clp_s/filter/PackedFilterRunner.hpp>
#include <clp_s/filter/RegisterIndexes.hpp>
#include <clp_s/search/ast/Expression.hpp>
#include <clp_s/search/ast/SearchUtils.hpp>
#include <clp_s/search/kql/kql.hpp>

namespace po = boost::program_options;
namespace fs = std::filesystem;
namespace ast = clp_s::search::ast;
using clp_s::filter::CandidateArchiveBitmapView;
using clp_s::filter::IndexRegistry;
using clp_s::filter::register_indexes;

namespace {
[[nodiscard]] auto read_file(fs::path const& path, std::vector<char>& contents) -> bool {
    std::ifstream input{path, std::ios::binary};
    if (false == input.is_open()) {
        SPDLOG_ERROR("Failed to open '{}'.", path.string());
        return false;
    }
    contents.assign(std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{});
    return true;
}

/**
 * Collects the `.pack` files in `packs_dir`, ordered by their numeric stem (0.pack, 1.pack, ...).
 */
[[nodiscard]] auto collect_packs(fs::path const& packs_dir, std::vector<fs::path>& packs) -> bool {
    std::error_code error_code;
    if (false == fs::is_directory(packs_dir, error_code)) {
        SPDLOG_ERROR("'{}' is not a directory.", packs_dir.string());
        return false;
    }
    std::vector<std::pair<long long, fs::path>> indexed_packs;
    for (auto const& entry : fs::directory_iterator{packs_dir, error_code}) {
        if (false == entry.is_regular_file() || ".pack" != entry.path().extension()) {
            continue;
        }
        long long pack_index{0};
        try {
            pack_index = std::stoll(entry.path().stem().string());
        } catch (std::exception const&) {
            pack_index = 0;
        }
        indexed_packs.emplace_back(pack_index, entry.path());
    }
    std::sort(indexed_packs.begin(), indexed_packs.end(), [](auto const& lhs, auto const& rhs) {
        return lhs.first < rhs.first;
    });
    for (auto& [pack_index, path] : indexed_packs) {
        packs.push_back(std::move(path));
    }
    return true;
}
}  // namespace

int main(int argc, char const* argv[]) {
    try {
        auto logger = spdlog::stderr_logger_st("stderr");
        spdlog::set_default_logger(logger);
        spdlog::set_pattern("%Y-%m-%dT%H:%M:%S.%e%z [%l] %v");
    } catch (std::exception&) {
        return 1;
    }

    std::string packs_dir;
    std::string query_string;
    po::options_description options("Options");
    options.add_options()("help,h", "Print help")(
            "packs",
            po::value<std::string>(&packs_dir),
            "Directory containing the .pack files to filter"
    )("query", po::value<std::string>(&query_string), "KQL query to filter the packs against");
    po::positional_options_description positional_options;
    positional_options.add("packs", 1).add("query", 1);
    try {
        po::variables_map parsed_options;
        po::store(
                po::command_line_parser(argc, argv)
                        .options(options)
                        .positional(positional_options)
                        .run(),
                parsed_options
        );
        po::notify(parsed_options);
        if (0 != parsed_options.count("help")) {
            std::cerr << "Usage: packed-filter-query PACKS_DIR QUERY" << std::endl << options;
            return 0;
        }
        if (packs_dir.empty()) {
            SPDLOG_ERROR("No packs directory specified.");
            return 1;
        }
        if (query_string.empty()) {
            SPDLOG_ERROR("No query specified.");
            return 1;
        }
    } catch (std::exception& e) {
        SPDLOG_ERROR("{}", e.what());
        return 1;
    }

    auto query_stream = std::istringstream(query_string);
    auto query = ast::preprocess_query(clp_s::search::kql::parse_kql_expression(query_stream));
    if (nullptr == query) {
        SPDLOG_ERROR("Failed to parse query '{}'.", query_string);
        return 1;
    }

    IndexRegistry registry;
    if (register_indexes(registry).has_error()) {
        SPDLOG_ERROR("Failed to register indexes.");
        return 1;
    }

    std::vector<fs::path> packs;
    if (false == collect_packs(packs_dir, packs)) {
        return 1;
    }
    if (packs.empty()) {
        SPDLOG_ERROR("No .pack files found in '{}'.", packs_dir);
        return 1;
    }

    size_t total_archives{0};
    size_t total_pruned{0};
    for (auto const& pack_path : packs) {
        std::vector<char> pack;
        if (false == read_file(pack_path, pack)) {
            return 1;
        }

        auto runner_result{registry.create_packed_filter_runner(std::move(pack))};
        if (runner_result.has_error()) {
            SPDLOG_ERROR("Failed to load pack '{}'.", pack_path.string());
            return 1;
        }
        auto runner{std::move(runner_result.value())};

        auto const num_archives{runner.get_num_archives()};
        if (0 == num_archives) {
            SPDLOG_WARN("Pack '{}' covers no archives; skipping.", pack_path.string());
            continue;
        }

        // All archives start as candidates; bits past the end of the bitmap must be zero.
        std::vector<std::uint64_t> bitmap_words((num_archives + 63) / 64, ~std::uint64_t{0});
        if (auto const remainder{num_archives % 64}; 0 != remainder) {
            bitmap_words.back() = (std::uint64_t{1} << remainder) - 1;
        }
        auto bitmap_result{CandidateArchiveBitmapView::create(bitmap_words.data(), num_archives)};
        if (bitmap_result.has_error()) {
            SPDLOG_ERROR("Failed to create the candidate bitmap.");
            return 1;
        }
        auto bitmap{bitmap_result.value()};

        if (runner.filter(query, bitmap).has_error()) {
            SPDLOG_ERROR("Failed to filter pack '{}'.", pack_path.string());
            return 1;
        }

        size_t surviving{0};
        for (size_t i{0}; i < num_archives; ++i) {
            auto const bit{bitmap.test_bit(i)};
            if (bit.has_error()) {
                SPDLOG_ERROR("Failed to read the candidate bitmap.");
                return 1;
            }
            if (bit.value()) {
                ++surviving;
            }
        }
        auto const pruned{num_archives - surviving};
        total_archives += num_archives;
        total_pruned += pruned;
        SPDLOG_INFO(
                "{}: pruned {}/{} archives ({} left to open).",
                pack_path.filename().string(),
                pruned,
                num_archives,
                surviving
        );
    }

    SPDLOG_INFO(
            "Total: pruned {}/{} archives across {} pack(s).",
            total_pruned,
            total_archives,
            packs.size()
    );
    return 0;
}
