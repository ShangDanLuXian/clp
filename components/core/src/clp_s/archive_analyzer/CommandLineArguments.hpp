#ifndef CLP_S_ARCHIVE_ANALYZER_COMMAND_LINE_ARGUMENTS_HPP
#define CLP_S_ARCHIVE_ANALYZER_COMMAND_LINE_ARGUMENTS_HPP

#include <string>
#include <string_view>
#include <vector>

namespace clp_s::archive_analyzer {
class CommandLineArguments {
public:
    // Types
    enum class ParsingResult {
        Success = 0,
        InfoCommand,
        Failure
    };

    // Constructors
    explicit CommandLineArguments(std::string_view program_name) : m_program_name{program_name} {}

    // Methods
    [[nodiscard]] auto parse_arguments(int argc, char const* argv[]) -> ParsingResult;

    [[nodiscard]] auto get_archive_paths() const -> std::vector<std::string> const& {
        return m_archive_paths;
    }

    [[nodiscard]] auto get_collect_column_stats() const -> bool {
        return m_collect_column_stats;
    }

    [[nodiscard]] auto get_output_json() const -> bool { return m_output_json; }

private:
    // Variables
    std::string m_program_name;
    std::vector<std::string> m_archive_paths;
    bool m_collect_column_stats{true};
    bool m_output_json{false};
};
}  // namespace clp_s::archive_analyzer

#endif  // CLP_S_ARCHIVE_ANALYZER_COMMAND_LINE_ARGUMENTS_HPP
