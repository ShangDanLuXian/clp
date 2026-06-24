#include "CommandLineArguments.hpp"

#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

#include <boost/program_options.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include <clp_s/ErrorCode.hpp>
#include <clp_s/FileReader.hpp>
#include <clp_s/InputConfig.hpp>

namespace po = boost::program_options;

namespace clp_s::filter {
namespace {
// Authorization method constants
constexpr std::string_view cNoAuth{"none"};
constexpr std::string_view cS3Auth{"s3"};

/**
 * Read a list of newline-delimited paths from a file and put them into a vector passed by reference
 * @param input_path_list_file_path path to the file containing the list of paths
 * @param path_destination the vector that the paths are pushed into
 * @return Whether paths were read successfully.
 */
[[nodiscard]] auto read_paths_from_file(
        std::string const& input_path_list_file_path,
        std::vector<std::string>& path_destination
) -> bool;

/**
 * Validates and populates network authorization options.
 * @param auth_method
 * @param network_auth
 * @throws std::invalid_argument if the authorization option is invalid
 */
auto validate_network_auth(std::string_view auth_method, NetworkAuthOption& auth) -> void;

auto read_paths_from_file(
        std::string const& input_path_list_file_path,
        std::vector<std::string>& path_destination
) -> bool {
    FileReader reader;
    auto error_code = reader.try_open(input_path_list_file_path);
    if (ErrorCodeFileNotFound == error_code) {
        SPDLOG_ERROR(
                "Failed to open input path list file {} - file not found",
                input_path_list_file_path
        );
        return false;
    }
    if (ErrorCodeSuccess != error_code) {
        SPDLOG_ERROR("Error opening input path list file {}", input_path_list_file_path);
        return false;
    }

    std::string line;
    while (true) {
        error_code = reader.try_read_to_delimiter('\n', false, false, line);
        if (ErrorCodeSuccess != error_code) {
            break;
        }
        if (false == line.empty()) {
            path_destination.push_back(line);
        }
    }

    if (ErrorCodeEndOfFile != error_code) {
        return false;
    }
    return true;
}

auto validate_network_auth(std::string_view auth_method, NetworkAuthOption& auth) -> void {
    if (cS3Auth == auth_method) {
        auth.method = AuthMethod::S3PresignedUrlV4;
    } else if (cNoAuth != auth_method) {
        throw std::invalid_argument(fmt::format("Invalid authentication type \"{}\"", auth_method));
    }
}
}  // namespace

auto CommandLineArguments::parse_arguments(int argc, char const* argv[])
        -> CommandLineArguments::ParsingResult {
    if (1 == argc) {
        print_basic_usage();
        return ParsingResult::Failure;
    }

    po::options_description general_options("General options");
    general_options.add_options()("help,h", "Print help");

    char command_input{};
    po::options_description general_positional_options("General positional options");
    // clang-format off
    general_positional_options.add_options()(
            "command", po::value<char>(&command_input)
    )(
            "command-args", po::value<std::vector<std::string>>()
    );
    // clang-format on

    po::positional_options_description general_positional_options_description;
    general_positional_options_description.add("command", 1);
    general_positional_options_description.add("command-args", -1);

    po::options_description all_descriptions;
    all_descriptions.add(general_options);
    all_descriptions.add(general_positional_options);

    try {
        po::variables_map parsed_command_line_options;
        po::parsed_options const parsed
                = po::command_line_parser(argc, argv)
                          .options(all_descriptions)
                          .positional(general_positional_options_description)
                          .allow_unregistered()
                          .run();
        po::store(parsed, parsed_command_line_options);
        po::notify(parsed_command_line_options);

        if (0 == parsed_command_line_options.count("command")) {
            if (0 != parsed_command_line_options.count("help")) {
                if (argc > 2) {
                    SPDLOG_WARN("Ignoring all options besides --help.");
                }

                print_basic_usage();
                std::cerr << "COMMAND is one of:\n";
                std::cerr << "  b - build pack\n";
                std::cerr << "  r - run pack\n\n";
                std::cerr << "Try "
                          << " b --help OR"
                          << " r --help for command-specific details.\n";

                po::options_description visible_options;
                visible_options.add(general_options);
                std::cerr << visible_options << '\n' << std::flush;
                return ParsingResult::InfoCommand;
            }
            throw std::invalid_argument("Command unspecified");
        }

        switch (command_input) {
            case (char)Command::BuildPack:
            case (char)Command::RunPack:
                m_command = static_cast<Command>(command_input);
                break;
            default:
                throw std::invalid_argument(fmt::format("Unknown command {}.", command_input));
        }

        std::vector<std::string> unrecognized_options{
                po::collect_unrecognized(parsed.options, po::include_positional)
        };
        unrecognized_options.erase(unrecognized_options.begin());
        if (Command::BuildPack == m_command) {
            po::options_description build_pack_positional_options;
            std::vector<std::string> input_paths;
            // clang-format off
            build_pack_positional_options.add_options()(
                "packs-dir",
                po::value<std::string>(&m_output_dir)->value_name("DIR"),
                "output directory"
            )(
                "input-paths",
                po::value<std::vector<std::string>>(&input_paths)->value_name("PATHS"),
                "input paths"
            );
            // clang-format on

            std::string input_path_list_file_path;
            std::string auth{cNoAuth};
            po::options_description build_pack_options("Build options");
            // clang-format off
            build_pack_options.add_options()(
                "files-from,f",
                po::value<std::string>(&input_path_list_file_path)
                    ->value_name("FILE")
                    ->default_value(input_path_list_file_path),
                "Build pack for files specified in FILE"
            )(
                "auth",
                po::value<std::string>(&auth)
                    ->value_name("AUTH_METHOD")
                    ->default_value(auth),
                "Type of authentication required for network requests (s3 | none). Authentication"
                " with s3 requires the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment"
                " variables, and optionally the AWS_SESSION_TOKEN environment variable."
            );
            // clang-format on
            po::positional_options_description positional_options;
            positional_options.add("packs-dir", 1);
            positional_options.add("input-paths", -1);

            po::options_description all_build_options;
            all_build_options.add(build_pack_options);
            all_build_options.add(build_pack_positional_options);

            po::store(
                    po::command_line_parser(unrecognized_options)
                            .options(all_build_options)
                            .positional(positional_options)
                            .run(),
                    parsed_command_line_options
            );
            po::notify(parsed_command_line_options);

            if (parsed_command_line_options.count("help")) {
                print_build_pack_usage();

                po::options_description visible_options;
                visible_options.add(general_options);
                visible_options.add(build_pack_options);
                std::cerr << visible_options << "\n" << std::flush;
                return ParsingResult::InfoCommand;
            }

            if (m_output_dir.empty()) {
                throw std::invalid_argument("No output directory specified.");
            }

            if (false == input_path_list_file_path.empty()
                && false == read_paths_from_file(input_path_list_file_path, input_paths))
            {
                SPDLOG_ERROR("Failed to read paths from {}", input_path_list_file_path);
                return ParsingResult::Failure;
            }

            for (auto const& path : input_paths) {
                auto path_object{get_path_object_for_raw_path(path)};
                if (false == get_input_archives_for_path(path_object, m_input_paths)) {
                    throw std::invalid_argument(fmt::format("Invalid input path \"{}\".", path));
                }
            }

            if (m_input_paths.empty()) {
                throw std::invalid_argument("No input paths specified.");
            }

            validate_network_auth(auth, m_network_auth);
        } else /*Command::RunPack == m_command*/ {
            po::options_description run_pack_positional_options;
            std::vector<std::string> input_paths;
            // clang-format off
            run_pack_positional_options.add_options()(
                "kql-query",
                po::value<std::string>(&m_kql_query)->value_name("QUERY"),
                "kql query"
            )(
                "input-paths",
                po::value<std::vector<std::string>>(&input_paths)->value_name("PATHS"),
                "input paths"
            );
            // clang-format on

            std::string input_path_list_file_path;
            std::string auth{cNoAuth};
            po::options_description run_pack_options("Run options");
            // clang-format off
            run_pack_options.add_options()(
                "files-from,f",
                po::value<std::string>(&input_path_list_file_path)
                    ->value_name("FILE")
                    ->default_value(input_path_list_file_path),
                "Build pack for files specified in FILE"
            )(
                "auth",
                po::value<std::string>(&auth)
                    ->value_name("AUTH_METHOD")
                    ->default_value(auth),
                "Type of authentication required for network requests (s3 | none). Authentication"
                " with s3 requires the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment"
                " variables, and optionally the AWS_SESSION_TOKEN environment variable."
            );
            // clang-format on
            po::positional_options_description positional_options;
            positional_options.add("kql-query", 1);
            positional_options.add("input-paths", -1);

            po::options_description all_run_options;
            all_run_options.add(run_pack_options);
            all_run_options.add(run_pack_positional_options);

            po::store(
                    po::command_line_parser(unrecognized_options)
                            .options(all_run_options)
                            .positional(positional_options)
                            .run(),
                    parsed_command_line_options
            );
            po::notify(parsed_command_line_options);

            if (parsed_command_line_options.count("help")) {
                print_run_pack_usage();

                po::options_description visible_options;
                visible_options.add(general_options);
                visible_options.add(run_pack_options);
                std::cerr << visible_options << "\n" << std::flush;
                return ParsingResult::InfoCommand;
            }

            if (m_kql_query.empty()) {
                throw std::invalid_argument("No KQL query specified.");
            }

            if (false == input_path_list_file_path.empty()
                && false == read_paths_from_file(input_path_list_file_path, input_paths))
            {
                SPDLOG_ERROR("Failed to read paths from {}", input_path_list_file_path);
                return ParsingResult::Failure;
            }

            for (auto const& path : input_paths) {
                auto path_object{get_path_object_for_raw_path(path)};
                if (false == get_input_files_for_path(path_object, m_input_paths)) {
                    throw std::invalid_argument(fmt::format("Invalid input path \"{}\".", path));
                }
            }

            if (m_input_paths.empty()) {
                throw std::invalid_argument("No input paths specified.");
            }

            validate_network_auth(auth, m_network_auth);
        }
    } catch (std::exception& e) {
        SPDLOG_ERROR("{}", e.what());
        print_basic_usage();
        std::cerr << "Try " << m_program_name << " --help for detailed usage instructions\n"
                  << std::flush;
        return ParsingResult::Failure;
    }
    return ParsingResult::Success;
}

auto CommandLineArguments::print_basic_usage() const -> void {
    std::cerr << "Usage: " << m_program_name << " [OPTIONS] COMMAND [COMMAND ARGUMENTS]\n"
              << std::flush;
}

auto CommandLineArguments::print_build_pack_usage() const -> void {
    std::cerr << "Usage: " << m_program_name << " b [OPTIONS] PACKS_DIR [FILE/DIR...]\n"
              << std::flush;
}

auto CommandLineArguments::print_run_pack_usage() const -> void {
    std::cerr << "Usage: " << m_program_name << "[OPTIONS] KQL_QUERY [PACKS FILE/DIR...]\n"
              << std::flush;
}
}  // namespace clp_s::filter
