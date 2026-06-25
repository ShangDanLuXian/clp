#include "AtomicWorkQueue.hpp"

#include <algorithm>
#include <atomic>
#include <optional>
#include <span>
#include <utility>

#include <clp_s/InputConfig.hpp>

namespace clp_s::filter {
AtomicWorkQueue::AtomicWorkQueue(std::vector<Path> paths) : m_paths{std::move(paths)} {}

auto AtomicWorkQueue::get_item() -> std::optional<Path*> {
    auto const idx{m_next_idx.fetch_add(1, std::memory_order_relaxed)};
    if (idx >= m_paths.size()) {
        return std::nullopt;
    }
    return &m_paths[idx];
}

auto AtomicWorkQueue::get_items(size_t num_items) -> std::optional<std::span<Path>> {
    auto const start{m_next_idx.fetch_add(num_items, std::memory_order_relaxed)};
    if (start >= m_paths.size()) {
        return std::nullopt;
    }
    auto const end{std::min(start + num_items, m_paths.size())};
    return std::span<Path>{m_paths.data() + start, end - start};
}
}  // namespace clp_s::filter
