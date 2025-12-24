#ifndef CLP_S_SCHEMAWRITER_HPP
#define CLP_S_SCHEMAWRITER_HPP

#include <unordered_set>
#include <vector>

#include "../clp/Defs.h"
#include "clp_s/filter/SchemaIntColumnFilter.hpp"
#include "clp_s/filter/SchemaStringColumnFilter.hpp"
#include "filter/ProbabilisticFilter.hpp"
#include "ColumnWriter.hpp"
#include "FileWriter.hpp"
#include "ParsedMessage.hpp"
#include "ZstdCompressor.hpp"

namespace clp_s {
class SchemaWriter {
public:
    // Constructor
    SchemaWriter() : m_num_messages(0), m_int_column_filter(), m_str_column_filter() {}

    // Destructor
    ~SchemaWriter();

    /**
     * Opens the schema writer.
     * @param path
     * @param compression_level
     */
    void open(std::string path, int compression_level);

    /**
     * Appends a column to the schema writer.
     * @param column_writer
     */
    void append_column(BaseColumnWriter* column_writer);

    /**
     * Appends a message to the schema writer.
     * @param message
     * @return The size of the message in bytes.
     */
    size_t append_message(ParsedMessage& message);

    /**
     * Stores the columns to disk.
     * @param compressor
     */
    void store(ZstdCompressor& compressor);

    uint64_t get_num_messages() const { return m_num_messages; }

    /**
     * @return the uncompressed in-memory size of the data that will be written to the compressor
     */
    size_t get_total_uncompressed_size() const { return m_total_uncompressed_size; }

    /**
     * Writes the filter for this schema to disk
     * @param filter_path Path to write the filter
     * @param compression_level Compression level for the filter
     * @return Size of the compressed filter file in bytes
     */
    [[nodiscard]] size_t write_filter(
            std::string const& filter_path,
            int compression_level
    );

    [[nodiscard]] size_t write_int_filter(
        std::string const& filter_path,
        int compression_level
);

[[nodiscard]] size_t write_str_filter(
    std::string const& filter_path,
    int compression_level
);

private:
    uint64_t m_num_messages;
    size_t m_total_uncompressed_size{};

    std::vector<BaseColumnWriter*> m_columns;
    std::vector<BaseColumnWriter*> m_unordered_columns;
    SchemaIntColumnFilter m_int_column_filter;
    SchemaStringColumnFilter m_str_column_filter;

    // Filter for this schema's variable dictionary IDs
    ProbabilisticFilter m_filter;
};
}  // namespace clp_s

#endif  // CLP_S_SCHEMAWRITER_HPP
