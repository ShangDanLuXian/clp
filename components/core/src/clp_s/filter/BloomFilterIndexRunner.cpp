#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <ystdlib/error_handling/Result.hpp>

#include <clp/ReaderInterface.hpp>
#include <clp_s/filter/BloomFilterIndexRunner.hpp>
#include <clp_s/filter/FilterReader.hpp>
#include <clp_s/filter/IndexDefs.hpp>
#include <clp_s/filter/IndexRunner.hpp>
#include <clp_s/search/ast/AndExpr.hpp>
#include <clp_s/search/ast/Expression.hpp>
#include <clp_s/search/ast/FilterExpr.hpp>
#include <clp_s/search/ast/FilterOperation.hpp>
#include <clp_s/search/ast/Literal.hpp>
#include <clp_s/search/ast/OrExpr.hpp>

namespace ast = clp_s::search::ast;

namespace clp_s::filter {
namespace {
/**
 * The state of a (sub)expression against a single archive's Bloom filter.
 *
 * A Bloom filter can only prove absence, so a leaf is never `Match`; `Match` arises only through
 * inversion (e.g. a negated leaf whose value is provably absent).
 */
enum class MatchState {
    Match,
    NoMatch,
    Unknown
};

[[nodiscard]] auto invert(MatchState state) -> MatchState {
    switch (state) {
        case MatchState::Match:
            return MatchState::NoMatch;
        case MatchState::NoMatch:
            return MatchState::Match;
        case MatchState::Unknown:
        default:
            return MatchState::Unknown;
    }
}

/**
 * Evaluates a leaf filter against an archive's Bloom filter.
 *
 * Only exact equality on a variable-style string can be pruned: if the queried value is definitely
 * absent from the archive's variable dictionary, the equality cannot hold. Every other predicate
 * (non-equality, non-string operands, wildcards, clp-strings) is `Unknown`.
 */
[[nodiscard]] auto evaluate_filter(ast::FilterExpr& filter, FilterReader const& filter_reader)
        -> MatchState {
    if (ast::FilterOperation::EQ != filter.get_operation()) {
        return MatchState::Unknown;
    }
    auto const operand{filter.get_operand()};
    if (nullptr == operand) {
        return MatchState::Unknown;
    }
    std::string value;
    if (false == operand->as_var_string(value, ast::FilterOperation::EQ)) {
        return MatchState::Unknown;
    }

    // `possibly_contains_query_string` treats a wildcard value as possibly present, and otherwise
    // unescapes and tests exact membership.
    if (filter_reader.possibly_contains_query_string(value)) {
        return MatchState::Unknown;
    }
    return MatchState::NoMatch;
}

/**
 * Recursively evaluates a query (sub)expression against an archive's Bloom filter, mirroring the
 * 3-valued logic of `EvaluateTimestampIndex`.
 */
[[nodiscard]] auto
evaluate(std::shared_ptr<ast::Expression> const& expression, FilterReader const& filter_reader)
        -> MatchState {
    if (nullptr == expression) {
        return MatchState::Unknown;
    }

    if (nullptr != std::dynamic_pointer_cast<ast::OrExpr>(expression)) {
        bool any_unknown{false};
        for (auto it{expression->op_begin()}; it != expression->op_end(); ++it) {
            auto const child{std::dynamic_pointer_cast<ast::Expression>(*it)};
            auto const child_state{evaluate(child, filter_reader)};
            if (MatchState::Match == child_state) {
                return expression->is_inverted() ? MatchState::NoMatch : MatchState::Match;
            }
            if (MatchState::Unknown == child_state) {
                any_unknown = true;
            }
        }
        if (any_unknown) {
            return MatchState::Unknown;
        }
        // Every child was NoMatch.
        return expression->is_inverted() ? MatchState::Match : MatchState::NoMatch;
    }

    if (nullptr != std::dynamic_pointer_cast<ast::AndExpr>(expression)) {
        bool any_unknown{false};
        for (auto it{expression->op_begin()}; it != expression->op_end(); ++it) {
            auto const child{std::dynamic_pointer_cast<ast::Expression>(*it)};
            auto const child_state{evaluate(child, filter_reader)};
            if (MatchState::NoMatch == child_state) {
                return expression->is_inverted() ? MatchState::Match : MatchState::NoMatch;
            }
            if (MatchState::Unknown == child_state) {
                any_unknown = true;
            }
        }
        if (any_unknown) {
            return MatchState::Unknown;
        }
        // Every child was Match.
        return expression->is_inverted() ? MatchState::NoMatch : MatchState::Match;
    }

    if (auto const filter{std::dynamic_pointer_cast<ast::FilterExpr>(expression)};
        nullptr != filter)
    {
        auto const state{evaluate_filter(*filter, filter_reader)};
        return filter->is_inverted() ? invert(state) : state;
    }

    // Unknown node kind (e.g. an empty expression): can't prune.
    return MatchState::Unknown;
}
}  // namespace

auto BloomFilterIndexRunner::create(
        index_version_t /*index_version*/,
        std::size_t num_archives,
        clp::ReaderInterface& reader
) -> ystdlib::error_handling::Result<std::unique_ptr<IndexRunner>> {
    std::vector<FilterReader> filter_readers;
    filter_readers.reserve(num_archives);
    for (std::size_t archive_idx{0}; archive_idx < num_archives; ++archive_idx) {
        filter_readers.push_back(YSTDLIB_ERROR_HANDLING_TRYX(FilterReader::try_read(reader)));
    }
    return std::unique_ptr<IndexRunner>{std::unique_ptr<BloomFilterIndexRunner>(
            new BloomFilterIndexRunner{std::move(filter_readers)}
    )};
}

auto BloomFilterIndexRunner::filter(
        std::shared_ptr<clp_s::search::ast::Expression> const& query,
        CandidateArchiveBitmapView& candidate_archive_bitmap
) -> ystdlib::error_handling::Result<void> {
    return candidate_archive_bitmap.filter_set_bits(
            [&](size_t local_archive_id) -> ystdlib::error_handling::Result<bool> {
                if (local_archive_id >= m_filter_readers.size()) {
                    // No filter loaded for this archive; keep it as a candidate.
                    return true;
                }
                auto const state{evaluate(query, m_filter_readers[local_archive_id])};
                // Keep the archive unless the query is provably false for it.
                return MatchState::NoMatch != state;
            }
    );
}
}  // namespace clp_s::filter
