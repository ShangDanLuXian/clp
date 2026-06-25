if(CLP_BUILD_EXECUTABLES AND CLP_BUILD_CLP_S_FILTER)
        add_executable(packed-filter-query packed_filter_query.cpp)
        target_compile_features(packed-filter-query PRIVATE cxx_std_20)
        target_include_directories(packed-filter-query PRIVATE ../../)
        target_link_libraries(packed-filter-query
                PRIVATE
                Boost::program_options
                clp_s::filter
                clp_s::search::ast
                clp_s::search::kql
                spdlog::spdlog
        )
        # Put the built executable at the root of the build directory
        set_target_properties(
                packed-filter-query
                PROPERTIES
                RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}"
        )
endif()
