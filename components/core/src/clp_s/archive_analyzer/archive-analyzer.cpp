#include <exception>
#include <iostream>
#include <string>
#include <string_view>

#include <nlohmann/json.hpp>

#include <clp_s/archive_analyzer/ArchiveAnalyzer.hpp>
#include <clp_s/archive_analyzer/CommandLineArguments.hpp>
#include <clp_s/ErrorCode.hpp>
#include <clp_s/TraceableException.hpp>

using clp_s::archive_analyzer::CommandLineArguments;

namespace {
/**
 * @param error_code
 * @return The name of `error_code`.
 */
[[nodiscard]] auto error_code_to_string(clp_s::ErrorCode error_code) -> std::string_view {
    switch (error_code) {
        case clp_s::ErrorCodeSuccess:
            return "Success";
        case clp_s::ErrorCodeBadParam:
            return "BadParam";
        case clp_s::ErrorCodeCorrupt:
            return "Corrupt";
        case clp_s::ErrorCodeErrno:
            return "Errno";
        case clp_s::ErrorCodeEndOfFile:
            return "EndOfFile";
        case clp_s::ErrorCodeFileExists:
            return "FileExists";
        case clp_s::ErrorCodeFileNotFound:
            return "FileNotFound";
        case clp_s::ErrorCodeNotInit:
            return "NotInit";
        case clp_s::ErrorCodeNotReady:
            return "NotReady";
        case clp_s::ErrorCodeOutOfBounds:
            return "OutOfBounds";
        case clp_s::ErrorCodeTooLong:
            return "TooLong";
        case clp_s::ErrorCodeTruncated:
            return "Truncated";
        case clp_s::ErrorCodeUnsupported:
            return "Unsupported";
        case clp_s::ErrorCodeNoAccess:
            return "NoAccess";
        case clp_s::ErrorCodeMetadataCorrupted:
            return "MetadataCorrupted";
        default:
            return "Unknown";
    }
}
}  // namespace

auto main(int argc, char const* argv[]) -> int {
    CommandLineArguments command_line_arguments{"archive-analyzer"};
    switch (command_line_arguments.parse_arguments(argc, argv)) {
        case CommandLineArguments::ParsingResult::Success:
            break;
        case CommandLineArguments::ParsingResult::InfoCommand:
            return 0;
        case CommandLineArguments::ParsingResult::Failure:
            return 1;
    }

    int return_code{0};
    auto const output_json{command_line_arguments.get_output_json()};
    auto failures{nlohmann::json::array()};

    // Records a failure on stderr and in the JSON output, without aborting the remaining
    // archives.
    auto record_failure
            = [&](std::string const& archive_path, std::string const& error_message) -> void {
        std::cerr << "Failed to analyze \"" << archive_path << "\": " << error_message << "\n";
        failures.emplace_back(
                nlohmann::json{{"path", archive_path}, {"error", error_message}}
        );
        return_code = 1;
    };

    // In JSON mode, reports are streamed as they complete rather than buffered: the enclosing
    // object is written incrementally, one report per line.
    if (output_json) {
        std::cout << "{\"analyzer_version\": "
                  << nlohmann::json(clp_s::archive_analyzer::get_analyzer_version()).dump()
                  << ", \"reports\": [";
    } else {
        std::cout << "# archive-analyzer " << clp_s::archive_analyzer::get_analyzer_version()
                  << "\n\n";
    }

    bool wrote_first_report{false};
    for (auto const& archive_path : command_line_arguments.get_archive_paths()) {
        try {
            auto const stats{clp_s::archive_analyzer::analyze_archive(
                    archive_path,
                    command_line_arguments.get_collect_column_stats()
            )};
            if (output_json) {
                if (wrote_first_report) {
                    std::cout << ",";
                }
                std::cout << "\n" << clp_s::archive_analyzer::stats_to_json(stats).dump()
                          << std::flush;
                wrote_first_report = true;
            } else {
                clp_s::archive_analyzer::print_stats_as_text(stats);
            }
        } catch (clp_s::TraceableException const& e) {
            std::string error_message{e.get_filename()};
            error_message += ":" + std::to_string(e.get_line_number()) + " ";
            error_message += error_code_to_string(e.get_error_code());
            error_message += " (error code " + std::to_string(e.get_error_code()) + ")";
            record_failure(archive_path, error_message);
            if (clp_s::ErrorCodeUnsupported == e.get_error_code()) {
                std::cerr << "Hint: this archive's format is not supported by this build of the"
                             " analyzer. It may have been created by a different clp-s version;"
                             " check whether this build's clp-s can read it (e.g. `clp-s x"
                             " <archive> <output-dir>`).\n";
            }
        } catch (std::exception const& e) {
            record_failure(archive_path, e.what());
        }
    }

    if (output_json) {
        std::cout << "\n], \"failures\": " << failures.dump() << "}\n";
    }
    return return_code;
}
