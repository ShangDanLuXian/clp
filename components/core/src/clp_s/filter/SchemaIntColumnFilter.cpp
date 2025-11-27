#include "SchemaIntColumnFilter.hpp"
#include <spdlog/spdlog.h>

#include "../ErrorCode.hpp"          // For ErrorCodeSuccess
#include "../ZstdCompressor.hpp"
#include "../ZstdDecompressor.hpp"

namespace clp_s {

    SchemaIntColumnFilter::SchemaIntColumnFilter() = default;
    
    SchemaIntColumnFilter& SchemaIntColumnFilter::operator=(
            SchemaIntColumnFilter const& other
    ) {
        if (this != &other) {
            m_column_values_map = other.m_column_values_map;
            m_column_count_map = other.m_column_count_map;
            // m_threashold is const and same in all instances, so we don't touch it
        }
        return *this;
    }
    
    void SchemaIntColumnFilter::add_value(int column_id, ParsedMessage::variable_t& value) {
        m_column_values_map[column_id].insert(std::get<int64_t>(value));
        m_column_count_map[column_id] += 1;
    }
    
    void SchemaIntColumnFilter::write_to_file(ZstdCompressor& compressor) const {
        struct ColumnEntry {
            int column_id;
            absl::flat_hash_set<int64_t> const* values;
        };
    
        std::vector<ColumnEntry> selected_columns;
        selected_columns.reserve(m_column_values_map.size());
    
        for (auto const& [column_id, values] : m_column_values_map) {
            auto it_count = m_column_count_map.find(column_id);
            if (it_count == m_column_count_map.end()) {
                continue;
            }
    
            auto const total_count = it_count->second;
            if (total_count <= 0) {
                continue;
            }
    
            double const ratio =
                    static_cast<double>(values.size()) /
                    static_cast<double>(total_count);
    
            if (ratio <= m_threashold) {
                selected_columns.push_back(ColumnEntry{column_id, &values});
            }
        }
    
        compressor.write_numeric_value<uint32_t>(
                static_cast<uint32_t>(selected_columns.size())
        );
    
        for (auto const& entry : selected_columns) {
            int32_t const column_id = static_cast<int32_t>(entry.column_id);
            auto const& values = *entry.values;
    
            compressor.write_numeric_value<int32_t>(column_id);
    
            uint64_t const num_values = static_cast<uint64_t>(values.size());
            compressor.write_numeric_value<uint64_t>(num_values);
    
            for (auto const v : values) {
                compressor.write_numeric_value<int64_t>(v);
            }
        }
    }
    
    auto SchemaIntColumnFilter::read_from_file(
            ZstdDecompressor& decompressor
    ) -> bool {
        m_column_values_map.clear();
        m_column_count_map.clear();
    
        uint32_t num_columns = 0;
        if (ErrorCodeSuccess != decompressor.try_read_numeric_value(num_columns)) {
            return false;
        }    
        for (uint32_t i = 0; i < num_columns; ++i) {
            int32_t column_id = 0;
            if (ErrorCodeSuccess != decompressor.try_read_numeric_value(column_id)) {
                return false;
            }
    
            uint64_t num_values = 0;
            if (ErrorCodeSuccess != decompressor.try_read_numeric_value(num_values)) {
                return false;
            }
    
            auto& values_set = m_column_values_map[column_id];
            values_set.reserve(static_cast<size_t>(num_values));
    
            for (uint64_t j = 0; j < num_values; ++j) {
                int64_t value = 0;
                if (ErrorCodeSuccess != decompressor.try_read_numeric_value(value)) {
                    return false;
                }
                values_set.insert(value);
            }
    
            // Only needed at build time; after read it's not used for logic.
            m_column_count_map[column_id] = 0;

            // for (auto const & [id, values]: m_column_values_map) {
            //     for (auto const value: values) {
            //         spdlog::info("column id :{}, value: {}", column_id, value);
            //     }
                
            // }
        }
    
        return true;
    }
    
    bool SchemaIntColumnFilter::is_empty() const {
        return m_column_values_map.empty();
    }
    
    bool SchemaIntColumnFilter::contains(int column_id, int64_t value) const {
        auto it = m_column_values_map.find(column_id);
        if (it == m_column_values_map.end()) {
            return false;
        }
        auto const& values = it->second;
        return values.find(value) != values.end();
    }
    
    SchemaIntColumnFilter SchemaIntColumnFilter::clone() const {
        // Uses copy constructor
        return *this;
    }
    
    } // namespace clp_s
    