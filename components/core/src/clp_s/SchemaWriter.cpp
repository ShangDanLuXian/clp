#include "SchemaWriter.hpp"

#include <utility>
#include <spdlog/spdlog.h>
#include "clp_s/ColumnWriter.hpp"
#include "clp_s/filter/ProbabilisticFilter.hpp"

namespace clp_s {
void SchemaWriter::append_column(BaseColumnWriter* column_writer) {
    m_total_uncompressed_size += column_writer->get_total_header_size();
    m_columns.push_back(column_writer);
}

size_t SchemaWriter::append_message(ParsedMessage& message) {
    int count{};
    size_t total_size{};
    for (auto& i : message.get_content()) {
        total_size += m_columns[count]->add_value(i.second);
        if (dynamic_cast<Int64ColumnWriter*>(m_columns[count])) {

            m_int_column_filter.add_value(m_columns[count]->get_m_id(), i.second);
        }
        ++count;
    }

    for (auto& i : message.get_unordered_content()) {
        total_size += m_columns[count]->add_value(i);
        ++count;
    }

    m_num_messages++;
    m_total_uncompressed_size += total_size;
    return total_size;
}

void SchemaWriter::store(ZstdCompressor& compressor) {
    for (auto& writer : m_columns) {
        writer->store(compressor);
    }
}

SchemaWriter::~SchemaWriter() {
    for (auto i : m_columns) {
        delete i;
    }
}

size_t SchemaWriter::write_filter(
        std::string const& filter_path,
        int compression_level
) {
    // Collect all variable IDs from VariableStringColumnWriter columns
    std::unordered_set<clp::variable_dictionary_id_t> variable_ids;
    for (auto* column : m_columns) {
        auto* var_column = dynamic_cast<VariableStringColumnWriter*>(column);
        if (var_column != nullptr) {
            auto const& var_dict_ids = var_column->get_var_dict_ids();
            variable_ids.insert(var_dict_ids.begin(), var_dict_ids.end());
        }
    }
    for (auto* column : m_unordered_columns) {
        auto* var_column = dynamic_cast<VariableStringColumnWriter*>(column);
        if (var_column != nullptr) {
            auto const& var_dict_ids = var_column->get_var_dict_ids();
            variable_ids.insert(var_dict_ids.begin(), var_dict_ids.end());
        }
    }

    if (variable_ids.empty()) {
        return 0;
    }

    // Create filter with 7% false positive rate (same as var dict filter)
    m_filter = ProbabilisticFilter(FilterType::Bloom, variable_ids.size(), 0.07);

    // Add all variable IDs to the filter
    for (auto var_id : variable_ids) {
        m_filter.add(std::to_string(var_id));
    }

    // Write filter to disk
    FileWriter filter_file_writer;
    filter_file_writer.open(filter_path, FileWriter::OpenMode::CreateForWriting);

    ZstdCompressor filter_compressor;
    filter_compressor.open(filter_file_writer, compression_level);

    m_filter.write_to_file(filter_file_writer, filter_compressor);

    filter_compressor.close();
    size_t compressed_size = filter_file_writer.get_pos();
    filter_file_writer.close();

    return compressed_size;
}

size_t SchemaWriter::write_int_filter(
    std::string const& filter_path,
    int compression_level
) {

    // Write filter to disk
    FileWriter filter_file_writer;
    filter_file_writer.open(filter_path, FileWriter::OpenMode::CreateForWriting);

    ZstdCompressor filter_compressor;
    filter_compressor.open(filter_file_writer, compression_level);

    m_int_column_filter.write_to_file(filter_compressor);

    filter_compressor.close();
    size_t compressed_size = filter_file_writer.get_pos();
    filter_file_writer.close();

    return compressed_size;
}
}  // namespace clp_s
