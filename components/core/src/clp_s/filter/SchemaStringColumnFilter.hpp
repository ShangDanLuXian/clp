#ifndef CLP_S_SCHEMASTRINGCOLUMNFILTER_HPP
#define CLP_S_SCHEMASTRINGCOLUMNFILTER_HPP

#include <cstdint>
#include <string>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include "../ZstdCompressor.hpp"
#include "../ZstdDecompressor.hpp"
#include "clp_s/ParsedMessage.hpp"

namespace clp_s {

    class SchemaStringColumnFilter {
    public:
        SchemaStringColumnFilter();
        ~SchemaStringColumnFilter() = default;
    
        SchemaStringColumnFilter(SchemaStringColumnFilter const&) = default;
        SchemaStringColumnFilter& operator=(SchemaStringColumnFilter const& other);
    
        void add_value(int column_id, ParsedMessage::variable_t& value);
    
        void write_to_file(ZstdCompressor& compressor) const;
        auto read_from_file(ZstdDecompressor& decompressor) -> bool;
    
        [[nodiscard]] bool is_empty() const;
        [[nodiscard]] bool contains(int column_id, std::string const& value) const;
        [[nodiscard]] SchemaStringColumnFilter clone() const;
    
    private:
        absl::flat_hash_map<int, absl::flat_hash_set<std::string>> m_column_values_map;
        absl::flat_hash_map<int, int64_t> m_column_count_map;
        const double m_threashold = 1.0 / 100.0;
    };
    
} // namespace clp_s

#endif