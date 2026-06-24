#include <exception>

#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>
#include <ystdlib/error_handling/Result.hpp>

#if CLP_BUILD_CLP_S_ENABLE_CURL
    #include <clp/CurlGlobalInstance.hpp>
#endif

#include "CommandLineArguments.hpp"

using clp_s::filter::CommandLineArguments;

namespace {
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

auto build_pack([[maybe_unused]] CommandLineArguments const& command_line_arguments)
        -> ystdlib::error_handling::Result<void> {
    SPDLOG_INFO("Build-pack stub.");
    return ystdlib::error_handling::success();
}

auto run_pack([[maybe_unused]] CommandLineArguments const& command_line_arguments)
        -> ystdlib::error_handling::Result<void> {
    SPDLOG_INFO("Run-pack stub.");
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
