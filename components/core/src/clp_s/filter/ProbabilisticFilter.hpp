#ifndef CLP_S_PROBABILISTIC_FILTER_HPP
#define CLP_S_PROBABILISTIC_FILTER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include "FilterConfig.hpp"

namespace clp_s {
class FileWriter;
}

namespace clp {
class ReaderInterface;
}

namespace clp_s::filter {
class IProbabilisticFilter {
public:
    virtual ~IProbabilisticFilter() = default;

    virtual void add(std::string_view value) = 0;
    [[nodiscard]] virtual auto possibly_contains(std::string_view value) const -> bool = 0;
    virtual void write_to_file(clp_s::FileWriter& writer) const = 0;
    virtual auto read_from_file(clp::ReaderInterface& reader) -> bool = 0;
    [[nodiscard]] virtual auto is_empty() const -> bool = 0;
    [[nodiscard]] virtual auto get_type() const -> FilterType = 0;
    [[nodiscard]] virtual auto get_memory_usage() const -> size_t = 0;
    [[nodiscard]] virtual auto clone() const -> std::unique_ptr<IProbabilisticFilter> = 0;

protected:
    IProbabilisticFilter() = default;
    IProbabilisticFilter(IProbabilisticFilter const&) = default;
    auto operator=(IProbabilisticFilter const&) -> IProbabilisticFilter& = default;
    IProbabilisticFilter(IProbabilisticFilter&&) = default;
    auto operator=(IProbabilisticFilter&&) -> IProbabilisticFilter& = default;
};

class ProbabilisticFilter {
public:
    ProbabilisticFilter() = default;

    ProbabilisticFilter(ProbabilisticFilter const& other);
    auto operator=(ProbabilisticFilter const& other) -> ProbabilisticFilter&;

    ProbabilisticFilter(ProbabilisticFilter&&) noexcept = default;
    auto operator=(ProbabilisticFilter&&) noexcept -> ProbabilisticFilter& = default;

    ~ProbabilisticFilter() = default;

    [[nodiscard]] static auto create(FilterConfig const& config, size_t expected_num_elements)
            -> ProbabilisticFilter;
    [[nodiscard]] static auto create_empty_for_type(FilterType type) -> ProbabilisticFilter;

    void add(std::string_view value);
    [[nodiscard]] auto possibly_contains(std::string_view value) const -> bool;
    void write_to_file(clp_s::FileWriter& writer) const;
    auto read_from_file(clp::ReaderInterface& reader) -> bool;
    [[nodiscard]] auto is_empty() const -> bool;
    [[nodiscard]] auto get_type() const -> FilterType;
    [[nodiscard]] auto get_memory_usage() const -> size_t;

private:
    std::unique_ptr<IProbabilisticFilter> m_impl;
};
}  // namespace clp_s::filter

#endif  // CLP_S_PROBABILISTIC_FILTER_HPP
