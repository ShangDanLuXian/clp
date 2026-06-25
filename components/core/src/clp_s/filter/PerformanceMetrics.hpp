#ifndef CLP_S_FILTER_PERFORMANCEMETRICS_HPP
#define CLP_S_FILTER_PERFORMANCEMETRICS_HPP

#include <chrono>
#include <cstddef>

namespace clp_s::filter {
/**
 * A class for measuring per-thread performance metrics during benchmarking. The timer starts
 * automatically on construction, items are counted via `mark_item_processed()`, and
 * `mark_finished()` freezes the end timestamp (idempotent).
 */
class PerformanceMetrics {
public:
    // Constructors
    PerformanceMetrics();

    // Methods
    auto mark_item_processed() -> void;
    auto mark_finished() -> void;

    [[nodiscard]] auto get_total_time() const -> std::chrono::nanoseconds;
    [[nodiscard]] auto get_num_items() const -> size_t;
    [[nodiscard]] auto get_average_item_time() const -> std::chrono::nanoseconds;

private:
    std::chrono::time_point<std::chrono::steady_clock> m_start_time;
    std::chrono::time_point<std::chrono::steady_clock> m_end_time;
    bool m_finished{false};
    size_t m_num_items{0};
};
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_PERFORMANCEMETRICS_HPP
