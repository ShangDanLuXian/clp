#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>
#include <ystdlib/error_handling/Result.hpp>

#if CLP_BUILD_CLP_S_ENABLE_CURL
    #include <clp/CurlGlobalInstance.hpp>
#endif

#include <clp_s/FileWriter.hpp>

#include "AtomicWorkQueue.hpp"
#include "CommandLineArguments.hpp"
#include "PackBuilder.hpp"
#include "PackSearcher.hpp"
#include "PerformanceMetrics.hpp"

using clp_s::filter::CommandLineArguments;

namespace {
/**
 * Writes a performance report as CSV to the given file path. The report contains a single
 * aggregate row with metrics computed across all threads.
 * @param report_file_path Path to write the report to.
 * @param overall_duration Total wall-clock duration of the benchmark job.
 * @param num_threads Number of threads used for the job.
 * @param metrics Per-thread performance metrics.
 */
auto write_performance_report(
        std::string const& report_file_path,
        std::chrono::nanoseconds overall_duration,
        size_t num_threads,
        std::vector<clp_s::filter::PerformanceMetrics> const& metrics
) -> void;

/**
 * Builds one or more packs based on the configuration specified by the command line arguments.
 * @param command_line_arguments
 * @return A void result on success, or an error code indicating the failure.
 */
[[nodiscard]] auto build_pack(CommandLineArguments const& command_line_arguments)
        -> ystdlib::error_handling::Result<void>;

/**
 * Runs one or more packs based on the configuration specified by the command line arguments.
 * @param command_line_arguments
 * @return A void result on success, or an error code indicating the failure.
 */
[[nodiscard]] auto run_pack(CommandLineArguments const& command_line_arguments)
        -> ystdlib::error_handling::Result<void>;

/**
 * Thread function for the build-pack benchmarking path. Each thread claims batches of
 * archives from the work queue, builds a pack from each batch, and records performance
 * metrics.
 *
 * @param metrics Reference to the caller's metrics slot; overwritten with the thread's
 * local metrics upon completion.
 * @param queue Shared work queue for distributing input paths across threads.
 * @param command_line_arguments Configuration including output directory and
 * archives-per-pack count.
 */
auto build_pack_thread(
        clp_s::filter::PerformanceMetrics& metrics,
        clp_s::filter::AtomicWorkQueue& queue,
        CommandLineArguments const& command_line_arguments
) -> void;

/**
 * Thread function for the run-pack benchmarking path. Each thread claims pack paths
 * from the work queue, searches each pack, and records performance metrics.
 *
 * @param metrics Reference to the caller's metrics slot; overwritten with the thread's
 * local metrics upon completion.
 * @param queue Shared work queue for distributing input paths across threads.
 * @param command_line_arguments Configuration including KQL query and network auth.
 */
auto run_pack_thread(
        clp_s::filter::PerformanceMetrics& metrics,
        clp_s::filter::AtomicWorkQueue& queue,
        CommandLineArguments const& command_line_arguments
) -> void;

auto write_performance_report(
        std::string const& report_file_path,
        std::chrono::nanoseconds overall_duration,
        size_t num_threads,
        std::vector<clp_s::filter::PerformanceMetrics> const& metrics
) -> void {
    size_t total_items{0};
    std::chrono::nanoseconds total_item_time{0};
    for (auto const& m : metrics) {
        total_items += m.get_num_items();
        total_item_time += m.get_total_time();
    }

    std::chrono::nanoseconds avg_item_time{0};
    if (0 != total_items) {
        avg_item_time = total_item_time / total_items;
    }

    double const overall_duration_seconds{std::chrono::duration<double>(overall_duration).count()};
    double throughput{0};
    if (0 != num_threads && 0.0 != overall_duration_seconds) {
        throughput = static_cast<double>(total_items)
                     / (static_cast<double>(num_threads) * overall_duration_seconds);
    }

    clp_s::FileWriter writer;
    writer.open(report_file_path, clp_s::FileWriter::OpenMode::CreateForWriting);

    std::string header{"total_items,avg_item_time_ns,overall_duration_ns,num_threads,"
                       "throughput_items_per_thread_per_sec\n"};
    writer.write(header.data(), header.size());

    std::string row{fmt::format(
            "{},{},{},{},{}\n",
            total_items,
            avg_item_time.count(),
            overall_duration.count(),
            num_threads,
            throughput
    )};
    writer.write(row.data(), row.size());

    writer.close();
}

auto build_pack_thread(
        clp_s::filter::PerformanceMetrics& metrics,
        clp_s::filter::AtomicWorkQueue& queue,
        CommandLineArguments const& command_line_arguments
) -> void {
    clp_s::filter::PerformanceMetrics local_metrics;
    auto const num_archives_per_pack{command_line_arguments.get_num_archives_per_pack()};

    while (auto const batch = queue.get_items(num_archives_per_pack)) {
        if (false
            == clp_s::filter::build_pack_from_archives(
                    batch.value(),
                    command_line_arguments.get_output_dir(),
                    command_line_arguments.get_network_auth()
            ))
        {
            SPDLOG_ERROR("Failed to build a pack from a batch of archives.");
        }
        for (auto const& archive : batch.value()) {
            local_metrics.mark_item_processed();
        }
    }

    local_metrics.mark_finished();
    metrics = std::move(local_metrics);
}

auto run_pack_thread(
        clp_s::filter::PerformanceMetrics& metrics,
        clp_s::filter::AtomicWorkQueue& queue,
        [[maybe_unused]] CommandLineArguments const& command_line_arguments
) -> void {
    clp_s::filter::PerformanceMetrics local_metrics;

    while (auto const item = queue.get_item()) {
        if (false
            == clp_s::filter::search_pack(
                    command_line_arguments.get_kql_query(),
                    *item.value(),
                    command_line_arguments.get_network_auth()
            ))
        {
            SPDLOG_ERROR("Failed to search a pack.");
        }
        local_metrics.mark_item_processed();
    }

    local_metrics.mark_finished();
    metrics = std::move(local_metrics);
}

auto build_pack(CommandLineArguments const& command_line_arguments)
        -> ystdlib::error_handling::Result<void> {
    auto const num_threads{command_line_arguments.get_num_threads()};
    std::vector<clp_s::filter::PerformanceMetrics> metrics(num_threads);
    clp_s::filter::AtomicWorkQueue queue{command_line_arguments.get_input_paths()};
    auto const start_time{std::chrono::steady_clock::now()};

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (auto& metrics_slot : metrics) {
        threads.emplace_back(
                build_pack_thread,
                std::ref(metrics_slot),
                std::ref(queue),
                std::cref(command_line_arguments)
        );
    }
    for (auto& thread : threads) {
        thread.join();
    }

    auto const end_time{std::chrono::steady_clock::now()};
    auto const& report_file_path{command_line_arguments.get_report_file_path()};
    if (report_file_path.has_value()) {
        write_performance_report(
                report_file_path.value(),
                std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time),
                num_threads,
                metrics
        );
    }
    return ystdlib::error_handling::success();
}

auto run_pack(CommandLineArguments const& command_line_arguments)
        -> ystdlib::error_handling::Result<void> {
    auto const num_threads{command_line_arguments.get_num_threads()};
    std::vector<clp_s::filter::PerformanceMetrics> metrics(num_threads);
    clp_s::filter::AtomicWorkQueue queue{command_line_arguments.get_input_paths()};
    auto const start_time{std::chrono::steady_clock::now()};

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (auto& metric_slot : metrics) {
        threads.emplace_back(
                run_pack_thread,
                std::ref(metric_slot),
                std::ref(queue),
                std::cref(command_line_arguments)
        );
    }
    for (auto& thread : threads) {
        thread.join();
    }

    auto const end_time{std::chrono::steady_clock::now()};
    auto const& report_file_path{command_line_arguments.get_report_file_path()};
    if (report_file_path.has_value()) {
        write_performance_report(
                report_file_path.value(),
                std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time),
                num_threads,
                metrics
        );
    }
    return ystdlib::error_handling::success();
}
}  // namespace

auto main(int argc, char const* argv[]) -> int {
    try {
        auto stderr_logger = spdlog::stderr_logger_st("stderr");
        spdlog::set_default_logger(stderr_logger);
        spdlog::set_pattern("%Y-%m-%dT%H:%M:%S.%e%z [%l] %v");
    } catch (std::exception& e) {
        // NOTE: We can't log an exception if the logger couldn't be constructed
        return 1;
    }

#if CLP_BUILD_CLP_S_ENABLE_CURL
    clp::CurlGlobalInstance const curl_instance{};
#endif

    CommandLineArguments command_line_arguments("clp-f");
    auto const parsing_result = command_line_arguments.parse_arguments(argc, argv);
    switch (parsing_result) {
        case CommandLineArguments::ParsingResult::Failure:
            return 1;
        case CommandLineArguments::ParsingResult::InfoCommand:
            return 0;
        case CommandLineArguments::ParsingResult::Success:
            break;
    }

    switch (command_line_arguments.get_command()) {
        case CommandLineArguments::Command::BuildPack: {
            auto const result{build_pack(command_line_arguments)};
            if (result.has_error()) {
                SPDLOG_ERROR(
                        "Encountered error during pack building - {}",
                        result.error().message()
                );
                return 1;
            }
            break;
        }
        case CommandLineArguments::Command::RunPack: {
            auto const result{run_pack(command_line_arguments)};
            if (result.has_error()) {
                SPDLOG_ERROR(
                        "Encountered error during pack building - {}",
                        result.error().message()
                );
                return 1;
            }
            break;
        }
        default:
            SPDLOG_ERROR(
                    "Unrecognized command `{}`.",
                    static_cast<char>(command_line_arguments.get_command())
            );
            return 1;
    }
    return 0;
}
