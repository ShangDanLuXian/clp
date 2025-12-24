#include "SchemaStringColumnFilter.hpp"
#include <spdlog/spdlog.h>

#include "../ErrorCode.hpp"
#include "../ZstdCompressor.hpp"
#include "../ZstdDecompressor.hpp"
#include "clp/Stopwatch.hpp"

namespace clp_s {

    SchemaStringColumnFilter::SchemaStringColumnFilter() = default;
    
    SchemaStringColumnFilter& SchemaStringColumnFilter::operator=(
            SchemaStringColumnFilter const& other
    ) {
        if (this != &other) {
            m_column_values_map = other.m_column_values_map;
            m_column_count_map = other.m_column_count_map;
            // m_threashold is const and same in all instances, so we don't touch it
        }
        return *this;
    }
    
    void SchemaStringColumnFilter::add_value(int column_id, ParsedMessage::variable_t& value) {
        m_column_values_map[column_id].insert(std::get<std::string>(value));
        m_column_count_map[column_id] += 1;
    }
    
    void SchemaStringColumnFilter::write_to_file(ZstdCompressor& compressor) const {
        struct ColumnEntry {
            int column_id;
            absl::flat_hash_set<std::string> const* values;
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
    
            for (auto const& v : values) {
                uint64_t const str_length = static_cast<uint64_t>(v.size());
                compressor.write_numeric_value<uint64_t>(str_length);
                compressor.write(v.data(), str_length);
            }
        }
    }
    
    auto SchemaStringColumnFilter::read_from_file(
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
                uint64_t str_length = 0;
                if (ErrorCodeSuccess != decompressor.try_read_numeric_value(str_length)) {
                    return false;
                }
                
                std::string value;
                value.resize(static_cast<size_t>(str_length));
                
                size_t num_bytes_read = 0;
                if (ErrorCodeSuccess != decompressor.try_read(
                        value.data(),
                        str_length,
                        num_bytes_read
                )) {
                    return false;
                }
                
                if (num_bytes_read != str_length) {
                    return false;
                }
                
                values_set.insert(std::move(value));
            }
    
            // Only needed at build time; after read it's not used for logic.
            m_column_count_map[column_id] = 0;
        }
    
        return true;
    }
    
    bool SchemaStringColumnFilter::is_empty() const {
        return m_column_values_map.empty();
    }
    
    bool SchemaStringColumnFilter::contains(int column_id, std::string const& value) const {
        auto it = m_column_values_map.find(column_id);
        if (it == m_column_values_map.end()) {
            return true;
        }
        auto const& values = it->second;
        return values.find(value) != values.end();
    }
    
    SchemaStringColumnFilter SchemaStringColumnFilter::clone() const {
        // Uses copy constructor
        return *this;
    }
    
} // namespace clp_s