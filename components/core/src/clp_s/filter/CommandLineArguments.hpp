#ifndef CLP_S_FILTER_COMMANDLINEARGUMENTS_HPP
#define CLP_S_FILTER_COMMANDLINEARGUMENTS_HPP

#include <string>

#include <clp_s/InputConfig.hpp>

namespace clp_s::filter {
class CommandLineArguments {
public:
    // Types
    enum class ParsingResult {
        Success = 0,
        InfoCommand,
        Failure
    };

    enum class Command : char {
        BuildPack = 'b',
        RunPack = 'r'
    };

    // Constructors
    explicit CommandLineArguments(std::string_view program_name) : m_program_name{program_name} {}

    // Methods
    [[nodiscard]] auto parse_arguments(int argc, char const* argv[]) -> ParsingResult;

    [[nodiscard]] auto get_command() const -> Command { return m_command; }

    [[nodiscard]] auto get_output_dir() const -> std::string const& { return m_output_dir; }

    [[nodiscard]] auto get_input_paths() const -> std::vector<Path> const& { return m_input_paths; }

    [[nodiscard]] auto get_network_auth() const -> NetworkAuthOption const& {
        return m_network_auth;
    }

    [[nodiscard]] auto get_kql_query() const -> std::string const& { return m_kql_query; }

private:
    // Methods
    auto print_basic_usage() const -> void;
    auto print_build_pack_usage() const -> void;
    auto print_run_pack_usage() const -> void;

    // Data members
    std::string m_program_name;
    Command m_command{Command::BuildPack};

    // General arguments
    std::vector<Path> m_input_paths;
    NetworkAuthOption m_network_auth{};

    // Pack building arguments
    std::string m_output_dir;

    // Pack running arguments
    std::string m_kql_query;
};
}  // namespace clp_s::filter
#endif  // CLP_S_FILTER_COMMANDLINEARGUMENTS_HPP
