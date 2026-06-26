#include <cstddef>
#include <exception>
#include <iostream>
#include <string>

#include <boost/program_options.hpp>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

#include <clp_s/InputConfig.hpp>

#include "Build.hpp"
#include "Query.hpp"

namespace po = boost::program_options;

namespace {
void print_usage() {
    std::cerr << "Usage: packed-filter <command> ...\n"
                 "  packed-filter build INPUT_PATH OUTPUT_DIR [--max-size BYTES]\n"
                 "  packed-filter query PACKS_DIR QUERY\n";
}

/**
 * Runs the `build` subcommand. `argc`/`argv` exclude the top-level program name but include the
 * subcommand name as `argv[0]` (which Boost treats as the program name and skips).
 */
[[nodiscard]] auto run_build(int argc, char const* argv[]) -> int {
    std::string input_path;
    std::string output_dir;
    size_t max_pack_size{clp_s::packed_filter::cDefaultMaxPackSize};
    po::options_description options("build options");
    // clang-format off
    options.add_options()
            ("help,h", "Print help")
            ("input", po::value<std::string>(&input_path),
             "Dataset path; every archive under it is packed")
            ("output", po::value<std::string>(&output_dir),
             "Directory the packs are written to")
            ("max-size", po::value<size_t>(&max_pack_size)->default_value(max_pack_size),
             "Upper bound on each pack's serialized size, in bytes");
    // clang-format on
    po::positional_options_description positional;
    positional.add("input", 1).add("output", 1);
    try {
        po::variables_map parsed_options;
        po::store(
                po::command_line_parser(argc, argv).options(options).positional(positional).run(),
                parsed_options
        );
        po::notify(parsed_options);
        if (0 != parsed_options.count("help")) {
            std::cerr << "Usage: packed-filter build INPUT_PATH OUTPUT_DIR [--max-size BYTES]\n"
                      << options;
            return 0;
        }
        if (input_path.empty()) {
            SPDLOG_ERROR("No input path specified.");
            return 1;
        }
        if (output_dir.empty()) {
            SPDLOG_ERROR("No output directory specified.");
            return 1;
        }
    } catch (std::exception& e) {
        SPDLOG_ERROR("{}", e.what());
        return 1;
    }

    auto const input{clp_s::get_path_object_for_raw_path(input_path)};
    return clp_s::packed_filter::build_packed_filter(
                   input,
                   output_dir,
                   clp_s::NetworkAuthOption{},
                   max_pack_size
           )
                   ? 0
                   : 1;
}

/**
 * Runs the `query` subcommand (see `run_build` for the `argc`/`argv` convention).
 */
[[nodiscard]] auto run_query(int argc, char const* argv[]) -> int {
    std::string packs_dir;
    std::string query_string;
    po::options_description options("query options");
    // clang-format off
    options.add_options()
            ("help,h", "Print help")
            ("packs", po::value<std::string>(&packs_dir),
             "Directory containing the .pack files to filter")
            ("query", po::value<std::string>(&query_string),
             "KQL query to filter the packs against");
    // clang-format on
    po::positional_options_description positional;
    positional.add("packs", 1).add("query", 1);
    try {
        po::variables_map parsed_options;
        po::store(
                po::command_line_parser(argc, argv).options(options).positional(positional).run(),
                parsed_options
        );
        po::notify(parsed_options);
        if (0 != parsed_options.count("help")) {
            std::cerr << "Usage: packed-filter query PACKS_DIR QUERY\n" << options;
            return 0;
        }
        if (packs_dir.empty()) {
            SPDLOG_ERROR("No packs directory specified.");
            return 1;
        }
        if (query_string.empty()) {
            SPDLOG_ERROR("No query specified.");
            return 1;
        }
    } catch (std::exception& e) {
        SPDLOG_ERROR("{}", e.what());
        return 1;
    }

    return clp_s::packed_filter::query_packs(packs_dir, query_string) ? 0 : 1;
}
}  // namespace

int main(int argc, char const* argv[]) {
    try {
        auto logger = spdlog::stderr_logger_st("stderr");
        spdlog::set_default_logger(logger);
        spdlog::set_pattern("%Y-%m-%dT%H:%M:%S.%e%z [%l] %v");
    } catch (std::exception&) {
        return 1;
    }

    if (argc < 2) {
        print_usage();
        return 1;
    }

    // Forward the remaining args to the subcommand; argv[1] (the subcommand name) becomes its
    // argv[0], which Boost skips as the program name.
    std::string const command{argv[1]};
    if ("build" == command) {
        return run_build(argc - 1, argv + 1);
    }
    if ("query" == command) {
        return run_query(argc - 1, argv + 1);
    }
    SPDLOG_ERROR("Unknown command '{}'.", command);
    print_usage();
    return 1;
}
