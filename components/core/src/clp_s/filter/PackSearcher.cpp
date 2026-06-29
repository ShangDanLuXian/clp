#include "PackSearcher.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <spdlog/spdlog.h>

#include <clp/ErrorCode.hpp>
#include <clp/ReaderInterface.hpp>
#include <clp_s/filter/IndexRegistry.hpp>
#include <clp_s/filter/IndexRunner.hpp>
#include <clp_s/filter/PackedFilterRunner.hpp>
#include <clp_s/filter/RegisterIndexes.hpp>
#include <clp_s/InputConfig.hpp>
#include <clp_s/search/ast/Expression.hpp>
#include <clp_s/search/ast/SearchUtils.hpp>
#include <clp_s/search/kql/kql.hpp>

namespace ast = clp_s::search::ast;

namespace clp_s::filter {
namespace {
// Chunk size used when streaming a pack into memory.
constexpr size_t cReadChunkSize{64ULL * 1024};

/**
 * Reads the entire contents of a (filesystem or network) pack into memory.
 *
 * When the pack's size is known up front (filesystem packs), the destination is sized once and the
 * whole pack is read in a single bulk read, avoiding the repeated reallocations and copies of a
 * growing vector. Network packs, whose size isn't known ahead of time, fall back to a chunked read
 * that grows the buffer geometrically.
 *
 * @param pack_path
 * @param network_auth
 * @param contents Returned pack bytes.
 * @return Whether the pack was read successfully.
 */
[[nodiscard]] auto read_pack(
        Path const& pack_path,
        NetworkAuthOption const& network_auth,
        std::vector<char>& contents
) -> bool {
    auto reader{try_create_reader(pack_path, network_auth)};
    if (nullptr == reader) {
        SPDLOG_ERROR("Failed to open pack '{}'.", pack_path.path);
        return false;
    }

    // Fast path: when the size is known up front, size the buffer once and read the whole pack in a
    // single bulk read — no reallocations and no intermediate chunk buffer.
    if (InputSource::Filesystem == pack_path.source) {
        std::error_code error_code;
        auto const pack_size{std::filesystem::file_size(pack_path.path, error_code)};
        if (false == static_cast<bool>(error_code)) {
            contents.resize(pack_size);
            if (pack_size > 0
                && clp::ErrorCode_Success
                           != reader->try_read_exact_length(contents.data(), pack_size))
            {
                SPDLOG_ERROR("Failed to read pack '{}'.", pack_path.path);
                return false;
            }
            return true;
        }
        // Couldn't stat the file; fall through to the chunked read below.
    }

    std::array<char, cReadChunkSize> buffer{};
    while (true) {
        size_t num_bytes_read{0};
        auto const error_code{reader->try_read(buffer.data(), buffer.size(), num_bytes_read)};
        if (num_bytes_read > 0) {
            contents.insert(contents.end(), buffer.data(), buffer.data() + num_bytes_read);
        }
        if (clp::ErrorCode_EndOfFile == error_code) {
            break;
        }
        if (clp::ErrorCode_Success != error_code) {
            SPDLOG_ERROR("Failed to read pack '{}'.", pack_path.path);
            return false;
        }
    }
    return true;
}
}  // namespace

auto search_pack(
        std::string const& kql_query,
        Path const& pack_path,
        NetworkAuthOption const& network_auth
) -> bool {
    auto query_stream{std::istringstream(kql_query)};
    auto query{ast::preprocess_query(clp_s::search::kql::parse_kql_expression(query_stream))};
    if (nullptr == query) {
        SPDLOG_ERROR("Failed to parse query '{}'.", kql_query);
        return false;
    }

    IndexRegistry registry;
    if (register_indexes(registry).has_error()) {
        SPDLOG_ERROR("Failed to register indexes.");
        return false;
    }

    std::vector<char> pack;
    if (false == read_pack(pack_path, network_auth, pack)) {
        return false;
    }

    auto runner_result{registry.create_packed_filter_runner(std::move(pack))};
    if (runner_result.has_error()) {
        SPDLOG_ERROR("Failed to load pack '{}'.", pack_path.path);
        return false;
    }
    auto runner{std::move(runner_result.value())};

    auto const num_archives{runner.get_num_archives()};
    if (0 == num_archives) {
        SPDLOG_WARN("Pack '{}' covers no archives; skipping.", pack_path.path);
        return true;
    }

    // All archives start as candidates; bits past the end of the bitmap must be zero.
    std::vector<std::uint64_t> bitmap_words((num_archives + 63) / 64, ~std::uint64_t{0});
    if (auto const remainder{num_archives % 64}; 0 != remainder) {
        bitmap_words.back() = (std::uint64_t{1} << remainder) - 1;
    }
    auto bitmap_result{CandidateArchiveBitmapView::create(bitmap_words.data(), num_archives)};
    if (bitmap_result.has_error()) {
        SPDLOG_ERROR("Failed to create the candidate bitmap.");
        return false;
    }
    auto bitmap{bitmap_result.value()};

    if (runner.filter(query, bitmap).has_error()) {
        SPDLOG_ERROR("Failed to filter pack '{}'.", pack_path.path);
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
    SPDLOG_INFO(
            "{}: pruned {}/{} archives ({} left to open).",
            pack_path.path,
            pruned,
            num_archives,
            surviving
    );
    return true;
}
}  // namespace clp_s::filter
