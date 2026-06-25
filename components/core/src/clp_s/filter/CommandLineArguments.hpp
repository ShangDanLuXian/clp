#ifndef CLP_S_FILTER_COMMANDLINEARGUMENTS_HPP
#define CLP_S_FILTER_COMMANDLINEARGUMENTS_HPP

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

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

    [[nodiscard]] auto get_num_threads() const -> size_t { return m_num_threads; }

    [[nodiscard]] auto get_report_file_path() const -> std::optional<std::string> const& {
        return m_performance_report_file_path;
    }

    [[nodiscard]] auto get_num_archives_per_pack() const -> size_t {
        return m_num_archives_per_pack;
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
    size_t m_num_threads{1};
    std::optional<std::string> m_performance_report_file_path;

    // Pack building arguments
    std::string m_output_dir;
    size_t m_num_archives_per_pack{16};

    // Pack running arguments
    std::string m_kql_query;
};
}  // namespace clp_s::filter
#endif  // CLP_S_FILTER_COMMANDLINEARGUMENTS_HPP
