#ifndef CLP_S_PROBABILISTICFILTER_HPP
#define CLP_S_PROBABILISTICFILTER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include "../clp/ReaderInterface.hpp"

namespace clp_s {

/**
 * Filter type enumeration
 */
enum class FilterType : uint8_t {
    None = 0,
    Bloom = 1,
};

/**
 * Abstract interface for probabilistic filters
 */
class IProbabilisticFilter {
public:
    virtual ~IProbabilisticFilter() = default;
    
    virtual void add(std::string_view value) = 0;
    [[nodiscard]] virtual auto possibly_contains(std::string_view value) const -> bool = 0;
    virtual void write_to_file(class FileWriter& file_writer, class ZstdCompressor& compressor)
            const = 0;
    virtual auto read_from_file(clp::ReaderInterface& reader, class ZstdDecompressor& decompressor)
            -> bool = 0;
    [[nodiscard]] virtual auto is_empty() const -> bool = 0;
    [[nodiscard]] virtual auto get_type() const -> FilterType = 0;
    [[nodiscard]] virtual auto get_memory_usage() const -> size_t = 0;
    
    /**
     * Create a deep copy of this filter
     */
    [[nodiscard]] virtual auto clone() const -> std::unique_ptr<IProbabilisticFilter> = 0;
    
protected:
    IProbabilisticFilter() = default;
    IProbabilisticFilter(IProbabilisticFilter const&) = delete;
    auto operator=(IProbabilisticFilter const&) -> IProbabilisticFilter& = delete;
    IProbabilisticFilter(IProbabilisticFilter&&) = default;
    auto operator=(IProbabilisticFilter&&) -> IProbabilisticFilter& = default;
};

/**
 * Concrete wrapper for probabilistic filters with value semantics.
 */
class ProbabilisticFilter {
public:
    /**
     * Constructs a filter of the specified type.
     */
    ProbabilisticFilter(
            FilterType type,
            size_t expected_num_elements,
            double false_positive_rate
    );

    /**
     * Default constructor creates an empty Bloom filter
     */
    ProbabilisticFilter();

    /**
     * Copy constructor (deep copy)
     */
    ProbabilisticFilter(ProbabilisticFilter const& other);

    /**
     * Copy assignment (deep copy)
     */
    auto operator=(ProbabilisticFilter const& other) -> ProbabilisticFilter&;

    /**
     * Move constructor
     */
    ProbabilisticFilter(ProbabilisticFilter&& other) noexcept = default;

    /**
     * Move assignment
     */
    auto operator=(ProbabilisticFilter&& other) noexcept -> ProbabilisticFilter& = default;

    ~ProbabilisticFilter() = default;

    void add(std::string_view value);
    [[nodiscard]] auto possibly_contains(std::string_view value) const -> bool;
    void write_to_file(class FileWriter& file_writer, class ZstdCompressor& compressor) const;
    auto read_from_file(clp::ReaderInterface& reader, class ZstdDecompressor& decompressor)
            -> bool;
    [[nodiscard]] auto is_empty() const -> bool;
    [[nodiscard]] auto get_type() const -> FilterType;
    [[nodiscard]] auto get_memory_usage() const -> size_t;

    [[nodiscard]] static auto create_from_file(
            clp::ReaderInterface& reader,
            class ZstdDecompressor& decompressor
    ) -> ProbabilisticFilter;

    [[nodiscard]] auto get_impl() -> IProbabilisticFilter& { return *m_impl; }
    [[nodiscard]] auto get_impl() const -> IProbabilisticFilter const& { return *m_impl; }

private:
    std::unique_ptr<IProbabilisticFilter> m_impl;
};

}  // namespace clp_s

#endif  // CLP_S_PROBABILISTICFILTER_HPP