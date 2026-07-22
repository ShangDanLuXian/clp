#include <exception>
#include <iostream>
#include <string>

#include <nlohmann/json.hpp>

#include <clp_s/archive_analyzer/ArchiveAnalyzer.hpp>
#include <clp_s/archive_analyzer/CommandLineArguments.hpp>

using clp_s::archive_analyzer::CommandLineArguments;

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
    auto json_reports{nlohmann::json::array()};
    if (false == command_line_arguments.get_output_json()) {
        std::cout << "# archive-analyzer " << clp_s::archive_analyzer::get_analyzer_version()
                  << "\n\n";
    }
    for (auto const& archive_path : command_line_arguments.get_archive_paths()) {
        try {
            auto const stats{clp_s::archive_analyzer::analyze_archive(
                    archive_path,
                    command_line_arguments.get_collect_column_stats()
            )};
            if (command_line_arguments.get_output_json()) {
                json_reports.emplace_back(clp_s::archive_analyzer::stats_to_json(stats));
            } else {
                clp_s::archive_analyzer::print_stats_as_text(stats);
            }
        } catch (std::exception const& e) {
            std::cerr << "Failed to analyze \"" << archive_path << "\": " << e.what() << "\n";
            return_code = 1;
        }
    }

    if (command_line_arguments.get_output_json()) {
        constexpr int cJsonIndent{2};
        nlohmann::json const output{
                {"analyzer_version", clp_s::archive_analyzer::get_analyzer_version()},
                {"reports", std::move(json_reports)}
        };
        std::cout << output.dump(cJsonIndent) << "\n";
    }
    return return_code;
}
