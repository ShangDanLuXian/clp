#include "PerformanceMetrics.hpp"

#include <chrono>
#include <cstddef>

namespace clp_s::filter {
PerformanceMetrics::PerformanceMetrics() : m_start_time{std::chrono::steady_clock::now()} {}

auto PerformanceMetrics::mark_item_processed() -> void {
    ++m_num_items;
}

auto PerformanceMetrics::mark_finished() -> void {
    if (false == m_finished) {
        m_end_time = std::chrono::steady_clock::now();
        m_finished = true;
    }
}

auto PerformanceMetrics::get_total_time() const -> std::chrono::nanoseconds {
    if (m_finished) {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(m_end_time - m_start_time);
    }
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - m_start_time
    );
}

auto PerformanceMetrics::get_num_items() const -> size_t {
    return m_num_items;
}

auto PerformanceMetrics::get_average_item_time() const -> std::chrono::nanoseconds {
    if (0 == m_num_items) {
        return std::chrono::nanoseconds{0};
    }
    return get_total_time() / m_num_items;
}
}  // namespace clp_s::filter
