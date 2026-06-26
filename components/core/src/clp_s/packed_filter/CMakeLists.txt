if(CLP_BUILD_EXECUTABLES AND CLP_BUILD_CLP_S_FILTER)
        add_executable(
                packed-filter
                Build.cpp
                Build.hpp
                packed_filter.cpp
                Query.cpp
                Query.hpp
        )
        target_compile_features(packed-filter PRIVATE cxx_std_20)
        target_include_directories(packed-filter PRIVATE ../../)
        target_link_libraries(packed-filter
                PRIVATE
                Boost::program_options
                clp_s::archive_reader
                clp_s::clp_dependencies
                clp_s::filter
                clp_s::io
                clp_s::search::ast
                clp_s::search::kql
                spdlog::spdlog
        )
        # Put the built executable at the root of the build directory
        set_target_properties(
                packed-filter
                PROPERTIES
                RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}"
        )
endif()
