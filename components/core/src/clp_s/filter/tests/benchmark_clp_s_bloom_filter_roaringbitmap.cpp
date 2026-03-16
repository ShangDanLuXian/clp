#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <numbers>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <roaring/roaring64map.hh>

#include <clp_s/filter/BloomFilter.hpp>
#include <clp_s/filter/HashAlgorithm.hpp>

namespace {
constexpr size_t cDefaultInsertions{100'000};
constexpr size_t cDefaultQueries{100'000};
constexpr double cDefaultFalsePositiveRate{0.001};
constexpr size_t cDefaultValueLength{30};
constexpr double cMinFalsePositiveRate{1e-6};
constexpr uint32_t cMinNumHashFunctions{1};
constexpr uint32_t cMaxNumHashFunctions{20};
constexpr uint64_t cPrimaryHashSeed{0};
constexpr uint64_t cSecondaryHashSeed{0x9e37'79b9'7f4a'7c15ULL};
constexpr size_t cNumBitsInByte{8};
constexpr size_t cSerializedHeaderSize{
        sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t)
};

struct BenchmarkConfig {
    size_t num_insertions{cDefaultInsertions};
    size_t num_queries{cDefaultQueries};
    double false_positive_rate{cDefaultFalsePositiveRate};
    size_t value_length{cDefaultValueLength};
};

struct FilterParameters {
    size_t bit_array_size{0};
    uint32_t num_hash_functions{0};
};

class RoaringBloomFilter {
public:
    RoaringBloomFilter(size_t bit_array_size, uint32_t num_hash_functions)
            : m_bit_array_size{bit_array_size},
              m_num_hash_functions{num_hash_functions} {}

    auto add(std::string_view value) -> void {
        uint64_t const h1{
                clp_s::filter::hash64(
                        clp_s::filter::HashAlgorithm::Xxh364,
                        value,
                        cPrimaryHashSeed
                )
        };
        uint64_t h2{
                clp_s::filter::hash64(
                        clp_s::filter::HashAlgorithm::Xxh364,
                        value,
                        cSecondaryHashSeed
                )
        };
        if (0 == h2) {
            h2 = 1;
        }

        for (uint32_t i = 0; i < m_num_hash_functions; ++i) {
            m_bit_indices.add(static_cast<uint64_t>((h1 + i * h2) % m_bit_array_size));
        }
    }

    [[nodiscard]] auto possibly_contains(std::string_view value) const -> bool {
        uint64_t const h1{
                clp_s::filter::hash64(
                        clp_s::filter::HashAlgorithm::Xxh364,
                        value,
                        cPrimaryHashSeed
                )
        };
        uint64_t h2{
                clp_s::filter::hash64(
                        clp_s::filter::HashAlgorithm::Xxh364,
                        value,
                        cSecondaryHashSeed
                )
        };
        if (0 == h2) {
            h2 = 1;
        }

        for (uint32_t i = 0; i < m_num_hash_functions; ++i) {
            if (false
                == m_bit_indices.contains(static_cast<uint64_t>((h1 + i * h2) % m_bit_array_size)))
            {
                return false;
            }
        }

        return true;
    }

    [[nodiscard]] auto serialized_bitmap_size_bytes() const -> size_t {
        auto optimized_bit_indices{m_bit_indices};
        optimized_bit_indices.runOptimize();
        return optimized_bit_indices.getSizeInBytes();
    }

private:
    size_t m_bit_array_size{0};
    uint32_t m_num_hash_functions{0};
    roaring::Roaring64Map m_bit_indices;
};

auto min_bytes_containing_bits(size_t num_bits) -> size_t {
    return (num_bits / cNumBitsInByte) + ((0 != (num_bits % cNumBitsInByte)) ? 1 : 0);
}

auto compute_optimal_parameters(size_t expected_num_elements, double false_positive_rate)
        -> FilterParameters {
    if (false_positive_rate < cMinFalsePositiveRate || false_positive_rate >= 1.0) {
        std::cerr << "false_positive_rate must be in [1e-6, 1)." << '\n';
        std::exit(1);
    }

    if (0 == expected_num_elements) {
        return FilterParameters{64, 1};
    }

    double const ln2{std::numbers::ln2_v<double>};
    double const ln2_squared{ln2 * ln2};
    double const ideal_bit_array_size{
            -static_cast<double>(expected_num_elements) * std::log(false_positive_rate)
            / ln2_squared
    };
    if (false == std::isfinite(ideal_bit_array_size)
        || ideal_bit_array_size > static_cast<double>(std::numeric_limits<size_t>::max()))
    {
        std::cerr << "Failed to compute Bloom filter parameters." << '\n';
        std::exit(1);
    }

    size_t const bit_array_size{
            std::max<size_t>(1, static_cast<size_t>(std::ceil(ideal_bit_array_size)))
    };
    auto const num_hash_functions = static_cast<uint32_t>(
            static_cast<double>(bit_array_size) / static_cast<double>(expected_num_elements) * ln2
    );
    return FilterParameters{
            bit_array_size,
            std::clamp(num_hash_functions, cMinNumHashFunctions, cMaxNumHashFunctions)
    };
}

auto make_value(uint64_t index, size_t value_length, char prefix) -> std::string {
    std::string value;
    value.reserve(value_length);
    value.push_back(prefix);
    value.append("-token-");
    value.append(std::to_string(index));

    while (value.length() < value_length) {
        value.push_back(static_cast<char>('a' + (index % 26)));
    }
    if (value.length() > value_length) {
        value.resize(value_length);
    }
    return value;
}

auto make_inserted_values(size_t num_values, size_t value_length) -> std::vector<std::string> {
    std::vector<std::string> values;
    values.reserve(num_values);
    for (size_t i = 0; i < num_values; ++i) {
        values.emplace_back(make_value((2 * i) + 1, value_length, 'i'));
    }
    return values;
}

auto make_absent_values(size_t num_values, size_t value_length) -> std::vector<std::string> {
    std::vector<std::string> values;
    values.reserve(num_values);
    for (size_t i = 0; i < num_values; ++i) {
        values.emplace_back(make_value(2 * i, value_length, 'q'));
    }
    return values;
}

template <typename Function>
auto time_call(Function&& function) -> std::chrono::nanoseconds {
    auto const begin_time{std::chrono::steady_clock::now()};
    function();
    auto const end_time{std::chrono::steady_clock::now()};
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - begin_time);
}

auto parse_args(int argc, char const* const* argv) -> BenchmarkConfig {
    BenchmarkConfig config;

    if (1 < argc) {
        config.num_insertions = static_cast<size_t>(std::strtoull(argv[1], nullptr, 10));
    }
    if (2 < argc) {
        config.num_queries = static_cast<size_t>(std::strtoull(argv[2], nullptr, 10));
    }
    if (3 < argc) {
        config.false_positive_rate = std::strtod(argv[3], nullptr);
    }
    if (4 < argc) {
        config.value_length = static_cast<size_t>(std::strtoull(argv[4], nullptr, 10));
    }

    return config;
}

auto print_duration(
        std::string_view label,
        std::chrono::nanoseconds duration,
        size_t operation_count
) -> void {
    double const duration_ms{std::chrono::duration<double, std::milli>(duration).count()};
    double const per_operation_ns{
            0 == operation_count
                    ? 0.0
                    : static_cast<double>(duration.count()) / static_cast<double>(operation_count)
    };
    std::cout << std::left << std::setw(34) << label << duration_ms << " ms"
              << "  (" << per_operation_ns << " ns/op)" << '\n';
}

auto print_size_comparison(
        std::string_view label,
        size_t baseline_size_bytes,
        size_t compared_size_bytes
) -> void {
    ptrdiff_t const delta_size_bytes{
            static_cast<ptrdiff_t>(compared_size_bytes)
            - static_cast<ptrdiff_t>(baseline_size_bytes)
    };
    double const size_ratio{
            0 == baseline_size_bytes
                    ? 0.0
                    : static_cast<double>(compared_size_bytes)
                              / static_cast<double>(baseline_size_bytes)
    };
    double const relative_change_pct{
            0 == baseline_size_bytes ? 0.0 : (size_ratio - 1.0) * 100.0
    };

    std::cout << std::left << std::setw(34) << label << compared_size_bytes << " bytes"
              << "  (delta=" << delta_size_bytes << " bytes"
              << ", change=" << relative_change_pct << "%)" << '\n';
}
}  // namespace

auto main(int argc, char const* const* argv) -> int {
    BenchmarkConfig const config{parse_args(argc, argv)};
    FilterParameters const filter_parameters{
            compute_optimal_parameters(config.num_insertions, config.false_positive_rate)
    };
    size_t const bloom_bitmap_size_bytes{min_bytes_containing_bits(filter_parameters.bit_array_size)};
    size_t const bloom_payload_size_bytes{cSerializedHeaderSize + bloom_bitmap_size_bytes};

    std::vector<std::string> const inserted_values{
            make_inserted_values(config.num_insertions, config.value_length)
    };
    std::vector<std::string> const absent_values{
            make_absent_values(config.num_queries, config.value_length)
    };

    auto bloom_filter_result{
            clp_s::filter::BloomFilter::create(config.num_insertions, config.false_positive_rate)
    };
    if (bloom_filter_result.has_error()) {
        std::cerr << "Failed to create BloomFilter benchmark instance." << '\n';
        return 1;
    }
    auto bloom_filter{std::move(bloom_filter_result.value())};
    RoaringBloomFilter roaring_filter{
            filter_parameters.bit_array_size,
            filter_parameters.num_hash_functions
    };

    auto const bloom_build_time{time_call([&bloom_filter, &inserted_values]() {
        for (auto const& value : inserted_values) {
            bloom_filter.add(value);
        }
    })};
    auto const roaring_build_time{time_call([&roaring_filter, &inserted_values]() {
        for (auto const& value : inserted_values) {
            roaring_filter.add(value);
        }
    })};

    size_t bloom_true_positives{};
    size_t roaring_true_positives{};
    auto const bloom_positive_query_time{
            time_call([&bloom_filter, &inserted_values, &bloom_true_positives]() {
                for (auto const& value : inserted_values) {
                    if (bloom_filter.possibly_contains(value)) {
                        ++bloom_true_positives;
                    }
                }
            })
    };
    auto const roaring_positive_query_time{
            time_call([&roaring_filter, &inserted_values, &roaring_true_positives]() {
                for (auto const& value : inserted_values) {
                    if (roaring_filter.possibly_contains(value)) {
                        ++roaring_true_positives;
                    }
                }
            })
    };

    size_t bloom_false_positives{};
    size_t roaring_false_positives{};
    auto const bloom_negative_query_time{
            time_call([&bloom_filter, &absent_values, &bloom_false_positives]() {
                for (auto const& value : absent_values) {
                    if (bloom_filter.possibly_contains(value)) {
                        ++bloom_false_positives;
                    }
                }
            })
    };
    auto const roaring_negative_query_time{
            time_call([&roaring_filter, &absent_values, &roaring_false_positives]() {
                for (auto const& value : absent_values) {
                    if (roaring_filter.possibly_contains(value)) {
                        ++roaring_false_positives;
                    }
                }
            })
    };

    size_t const roaring_bitmap_size_bytes{roaring_filter.serialized_bitmap_size_bytes()};
    size_t const roaring_payload_size_bytes{cSerializedHeaderSize + roaring_bitmap_size_bytes};
    double const bloom_false_positive_rate{
            0 == config.num_queries
                    ? 0.0
                    : static_cast<double>(bloom_false_positives)
                              / static_cast<double>(config.num_queries)
    };
    double const roaring_false_positive_rate{
            0 == config.num_queries
                    ? 0.0
                    : static_cast<double>(roaring_false_positives)
                              / static_cast<double>(config.num_queries)
    };

    if (bloom_true_positives != inserted_values.size()
        || roaring_true_positives != inserted_values.size())
    {
        std::cerr << "Unexpected false negatives detected during benchmark." << '\n';
    }

    std::cout << "BloomFilter vs RoaringBitmap-backed Bloom benchmark" << '\n';
    std::cout << "insertions=" << config.num_insertions << ", queries=" << config.num_queries
              << ", false_positive_rate=" << config.false_positive_rate
              << ", value_length=" << config.value_length << '\n';
    std::cout << "bit_array_size=" << filter_parameters.bit_array_size
              << ", num_hash_functions=" << filter_parameters.num_hash_functions << '\n';
    std::cout << "bloom_bitmap_size_bytes=" << bloom_bitmap_size_bytes
              << ", bloom_payload_size_bytes=" << bloom_payload_size_bytes << '\n';
    std::cout << "roaring_bitmap_size_bytes=" << roaring_bitmap_size_bytes
              << ", roaring_payload_size_bytes=" << roaring_payload_size_bytes << '\n';
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "roaring_to_bloom_payload_ratio="
              << static_cast<double>(roaring_payload_size_bytes)
                         / static_cast<double>(bloom_payload_size_bytes)
              << '\n';
    std::cout << "bloom_true_positives=" << bloom_true_positives
              << ", roaring_true_positives=" << roaring_true_positives << '\n';
    std::cout << "bloom_false_positive_rate=" << bloom_false_positive_rate
              << ", roaring_false_positive_rate=" << roaring_false_positive_rate
              << '\n';
    std::cout << '\n';

    print_size_comparison(
            "Roaring bitmap size vs Bloom",
            bloom_bitmap_size_bytes,
            roaring_bitmap_size_bytes
    );
    print_size_comparison(
            "Roaring payload size vs Bloom",
            bloom_payload_size_bytes,
            roaring_payload_size_bytes
    );
    print_duration("Bloom build", bloom_build_time, config.num_insertions);
    print_duration("Roaring build", roaring_build_time, config.num_insertions);
    print_duration("Bloom positive queries", bloom_positive_query_time, config.num_insertions);
    print_duration(
            "Roaring positive queries",
            roaring_positive_query_time,
            config.num_insertions
    );
    print_duration("Bloom negative queries", bloom_negative_query_time, config.num_queries);
    print_duration(
            "Roaring negative queries",
            roaring_negative_query_time,
            config.num_queries
    );

    return 0;
}
