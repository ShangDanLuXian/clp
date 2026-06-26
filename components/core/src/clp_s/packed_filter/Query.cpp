#include "Query.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <spdlog/spdlog.h>

#include <clp_s/filter/IndexRegistry.hpp>
#include <clp_s/filter/IndexRunner.hpp>
#include <clp_s/filter/PackedFilterRunner.hpp>
#include <clp_s/filter/RegisterIndexes.hpp>
#include <clp_s/search/ast/Expression.hpp>
#include <clp_s/search/ast/SearchUtils.hpp>
#include <clp_s/search/kql/kql.hpp>

namespace fs = std::filesystem;
namespace ast = clp_s::search::ast;

namespace clp_s::packed_filter {
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

auto query_packs(std::string const& packs_dir, std::string const& query_string) -> bool {
    auto query_stream = std::istringstream(query_string);
    auto query = ast::preprocess_query(clp_s::search::kql::parse_kql_expression(query_stream));
    if (nullptr == query) {
        SPDLOG_ERROR("Failed to parse query '{}'.", query_string);
        return false;
    }

    filter::IndexRegistry registry;
    if (filter::register_indexes(registry).has_error()) {
        SPDLOG_ERROR("Failed to register indexes.");
        return false;
    }

    std::vector<fs::path> packs;
    if (false == collect_packs(packs_dir, packs)) {
        return false;
    }
    if (packs.empty()) {
        SPDLOG_ERROR("No .pack files found in '{}'.", packs_dir);
        return false;
    }

    size_t total_archives{0};
    size_t total_pruned{0};
    for (auto const& pack_path : packs) {
        std::vector<char> pack;
        if (false == read_file(pack_path, pack)) {
            return false;
        }

        auto runner_result{registry.create_packed_filter_runner(std::move(pack))};
        if (runner_result.has_error()) {
            SPDLOG_ERROR("Failed to load pack '{}'.", pack_path.string());
            return false;
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
        auto bitmap_result{
                filter::CandidateArchiveBitmapView::create(bitmap_words.data(), num_archives)
        };
        if (bitmap_result.has_error()) {
            SPDLOG_ERROR("Failed to create the candidate bitmap.");
            return false;
        }
        auto bitmap{bitmap_result.value()};

        if (runner.filter(query, bitmap).has_error()) {
            SPDLOG_ERROR("Failed to filter pack '{}'.", pack_path.string());
            return false;
        }

        size_t surviving{0};
        for (size_t i{0}; i < num_archives; ++i) {
            auto const bit{bitmap.test_bit(i)};
            if (bit.has_error()) {
                SPDLOG_ERROR("Failed to read the candidate bitmap.");
                return false;
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
    return true;
}
}  // namespace clp_s::packed_filter
