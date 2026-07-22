set(
        ARCHIVE_ANALYZER_SOURCES
        archive-analyzer.cpp
        ArchiveAnalyzer.cpp
        ArchiveAnalyzer.hpp
        CommandLineArguments.cpp
        CommandLineArguments.hpp
)

if(CLP_BUILD_EXECUTABLES)
        add_executable(
                archive-analyzer
                ${ARCHIVE_ANALYZER_SOURCES}
        )
        target_compile_features(archive-analyzer PRIVATE cxx_std_20)

        # Embed the git description of the source the binary was built from, so reports carry
        # their provenance.
        execute_process(
                COMMAND git describe --always --dirty
                WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
                OUTPUT_VARIABLE ARCHIVE_ANALYZER_GIT_DESC
                OUTPUT_STRIP_TRAILING_WHITESPACE
                ERROR_QUIET
        )
        if(ARCHIVE_ANALYZER_GIT_DESC)
                target_compile_definitions(
                        archive-analyzer
                        PRIVATE
                        ARCHIVE_ANALYZER_GIT_DESC="${ARCHIVE_ANALYZER_GIT_DESC}"
                )
        endif()
        target_include_directories(archive-analyzer PRIVATE ../../)
        target_link_libraries(
                archive-analyzer
                PRIVATE
                absl::flat_hash_map
                Boost::program_options
                clp_s::archive_reader
                clp_s::io
                fmt::fmt
                msgpack-cxx
                nlohmann_json::nlohmann_json
        )
        # Put the built executable at the root of the build directory
        set_target_properties(
                archive-analyzer
                PROPERTIES
                RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}"
        )
endif()
