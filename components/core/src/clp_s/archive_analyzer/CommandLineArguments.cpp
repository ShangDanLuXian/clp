#include "CommandLineArguments.hpp"

#include <exception>
#include <iostream>

#include <boost/program_options.hpp>

namespace po = boost::program_options;

namespace clp_s::archive_analyzer {
auto CommandLineArguments::parse_arguments(int argc, char const* argv[]) -> ParsingResult {
    po::options_description visible_options{"Options"};
    // clang-format off
    visible_options.add_options()
            ("help,h", "Print help and exit.")
            (
                    "no-columns",
                    po::bool_switch(),
                    "Skip the per-column statistics pass. The pass decompresses every record"
                    " table in the archive, so skipping it makes analysis much faster."
            )
            ("json", po::bool_switch(), "Print the analysis as JSON instead of text.");
    // clang-format on

    po::options_description hidden_options;
    hidden_options.add_options()(
            "archive-paths",
            po::value<std::vector<std::string>>(&m_archive_paths)->composing(),
            "Archive paths."
    );

    po::positional_options_description positional_options;
    positional_options.add("archive-paths", -1);

    po::options_description all_options;
    all_options.add(visible_options).add(hidden_options);

    try {
        po::variables_map parsed_options;
        po::store(
                po::command_line_parser(argc, argv)
                        .options(all_options)
                        .positional(positional_options)
                        .run(),
                parsed_options
        );
        po::notify(parsed_options);

        if (0 != parsed_options.count("help")) {
            std::cerr << "Usage: " << m_program_name << " [OPTIONS] ARCHIVE_PATH..." << "\n";
            std::cerr << "Analyzes clp-s archives (single-file archives or archive directories)"
                      << " and reports their size, a per-component size breakdown, and per-column"
                      << " statistics." << "\n\n";
            std::cerr << visible_options << "\n";
            return ParsingResult::InfoCommand;
        }

        if (m_archive_paths.empty()) {
            std::cerr << "Error: no archive paths given. Run with --help for usage." << "\n";
            return ParsingResult::Failure;
        }

        m_collect_column_stats = false == parsed_options["no-columns"].as<bool>();
        m_output_json = parsed_options["json"].as<bool>();
    } catch (std::exception const& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return ParsingResult::Failure;
    }
    return ParsingResult::Success;
}
}  // namespace clp_s::archive_analyzer
