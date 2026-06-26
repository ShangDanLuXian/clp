set(
        CLP_S_FILTER_SOURCES
        BitmapView.hpp
        BloomFilter.cpp
        BloomFilter.hpp
        BloomFilterIndexBuilder.cpp
        BloomFilterIndexBuilder.hpp
        BloomFilterIndexRunner.cpp
        BloomFilterIndexRunner.hpp
        ErrorCode.cpp
        ErrorCode.hpp
        FilterBuilder.cpp
        FilterBuilder.hpp
        FilterOptions.cpp
        FilterOptions.hpp
        FilterReader.cpp
        FilterReader.hpp
        HashAlgorithm.cpp
        HashAlgorithm.hpp
        IndexBuilder.hpp
        IndexBuilderSpecification.hpp
        IndexDefs.hpp
        IndexRegistry.cpp
        IndexRegistry.hpp
        IndexRunner.hpp
        PackBuilder.cpp
        PackBuilder.hpp
        PackSearcher.cpp
        PackSearcher.hpp
        PackedFilterBuilder.cpp
        PackedFilterBuilder.hpp
        PackedFilterDefs.hpp
        PackedFilterReader.cpp
        PackedFilterReader.hpp
        PackedFilterRunner.cpp
        PackedFilterRunner.hpp
        PackedFilterSpecification.hpp
        PackedFilterWriter.cpp
        PackedFilterWriter.hpp
        RegisterIndexes.cpp
        RegisterIndexes.hpp
        XxHash.cpp
        XxHash.hpp
)

if(CLP_BUILD_CLP_S_FILTER)
        add_library(
                clp_s_filter
                ${CLP_S_FILTER_SOURCES}
        )
        add_library(clp_s::filter ALIAS clp_s_filter)
        target_compile_features(clp_s_filter PRIVATE cxx_std_20)
        target_include_directories(clp_s_filter PUBLIC ../..)
        target_link_libraries(
                clp_s_filter
                PUBLIC
                clp_s::clp_dependencies
                msgpack-cxx
                nlohmann_json::nlohmann_json
                ystdlib::containers
                ystdlib::error_handling
                PRIVATE
                clp::string_utils
                clp_s::archive_reader
                clp_s::search::ast
                clp_s::search::kql
                xxHash::xxhash
        )
endif()

set(
        CLP_F_EXE_SOURCES
        AtomicWorkQueue.cpp
        AtomicWorkQueue.hpp
        CommandLineArguments.cpp
        CommandLineArguments.hpp
        PerformanceMetrics.cpp
        PerformanceMetrics.hpp
)


if(CLP_BUILD_EXECUTABLES)
        # Runtime dependencies build for the executable.
        add_library(clp_f_binary_runtime INTERFACE)
        target_link_libraries(
                clp_f_binary_runtime
                INTERFACE
                clp_s::archive_reader
                clp_s::filter
                clp_s::io
                clp_s::search::ast
                clp_s::search::kql
        )
        add_executable(
                clp-f
                clp-f.cpp
                ${CLP_F_EXE_SOURCES}
        )
        target_compile_definitions(
                clp-f
                PRIVATE
                CLP_BUILD_CLP_S_ENABLE_CURL=$<BOOL:${CLP_BUILD_CLP_S_ENABLE_CURL}>
        )
        target_compile_features(clp-f PRIVATE cxx_std_20)
        target_include_directories(clp-f PRIVATE ../../)
        target_link_libraries(
                clp-f
                PRIVATE
                Boost::program_options
                clp_f_binary_runtime
                fmt::fmt
                spdlog::spdlog
                ystdlib::error_handling
        )
        set_target_properties(
                clp-f
                PROPERTIES
                RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}"
        )

endif()
