#ifndef CLP_S_FILTER_ATOMICWORKQUEUE_HPP
#define CLP_S_FILTER_ATOMICWORKQUEUE_HPP

#include <atomic>
#include <cstddef>
#include <optional>
#include <span>
#include <vector>

#include <clp_s/InputConfig.hpp>

namespace clp_s::filter {
/**
 * A thread-safe work queue that distributes `Path` items across multiple consumer threads. The
 * queue is populated once at construction and then consumed via atomic index advancement — no
 * locks are needed on the hot path because the underlying vector is never mutated.
 *
 * Each call to `get_item()` or `get_items()` atomically claims one or more items, guaranteeing
 * that no two threads will claim the same item. When the queue is exhausted, subsequent calls
 * return `std::nullopt`.
 */
class AtomicWorkQueue {
public:
    // Constructors
    /**
     * Constructs the queue by taking ownership of the given paths.
     * @param paths The work items to distribute. Moved into internal storage.
     */
    explicit AtomicWorkQueue(std::vector<Path> paths);

    // Disable copy and move — the queue should not be relocated once threads are consuming.
    AtomicWorkQueue(AtomicWorkQueue const&) = delete;
    auto operator=(AtomicWorkQueue const&) -> AtomicWorkQueue& = delete;
    AtomicWorkQueue(AtomicWorkQueue&&) = delete;
    auto operator=(AtomicWorkQueue&&) -> AtomicWorkQueue& = delete;

    // Methods
    /**
     * Atomically claims a single item from the queue.
     * @return A pointer to the claimed item, or `std::nullopt` if the queue is exhausted. The
     * pointer remains valid as long as this queue exists.
     */
    [[nodiscard]] auto get_item() -> std::optional<Path*>;

    /**
     * Atomically claims up to `num_items` items from the queue.
     *
     * If fewer than `num_items` items remain, all remaining items are yielded. If the queue is
     * exhausted, returns `std::nullopt`.
     *
     * @param num_items Maximum number of items to claim.
     * @return A span over the claimed items, or `std::nullopt` if the queue is exhausted. The
     * span remains valid as long as this queue exists.
     */
    [[nodiscard]] auto get_items(size_t num_items) -> std::optional<std::span<Path>>;

private:
    // Data members
    std::vector<Path> m_paths;
    std::atomic<size_t> m_next_idx{0};
};
}  // namespace clp_s::filter

#endif  // CLP_S_FILTER_ATOMICWORKQUEUE_HPP
