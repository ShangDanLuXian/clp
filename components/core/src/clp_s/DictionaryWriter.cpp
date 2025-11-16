// Code from CLP

#include "DictionaryWriter.hpp"

#include <string>
#include <string_view>

#include <spdlog/spdlog.h>

#include "../clp/Defs.h"
#include "FileWriter.hpp"
#include "ZstdCompressor.hpp"

namespace clp_s {
void VariableDictionaryWriter::open_with_bloom_filter(
        std::string const& dictionary_path,
        int compression_level,
        clp::variable_dictionary_id_t max_id,
        size_t expected_num_entries
) {
    // Call base class open
    open(dictionary_path, compression_level, max_id);

    // Don't create bloom filter yet - we'll create it when closing with the actual entry count
    // This avoids sizing issues when the actual entries differ significantly from the estimate
    m_use_bloom_filter = true;
}

bool
VariableDictionaryWriter::add_entry(std::string_view value, clp::variable_dictionary_id_t& id) {
    bool new_entry = false;

    auto const ix = m_value_to_id.find(value);
    if (m_value_to_id.end() != ix) {
        id = ix->second;
    } else {
        // Entry doesn't exist so create it

        if (m_next_id > m_max_id) {
            SPDLOG_ERROR("VariableDictionaryWriter ran out of IDs.");
            throw OperationFailed(ErrorCodeOutOfBounds, __FILENAME__, __LINE__);
        }

        // Assign ID
        id = m_next_id;
        ++m_next_id;

        // Insert the ID obtained from the database into the dictionary
        auto entry = VariableDictionaryEntry(std::string{value}, id);
        m_value_to_id[value] = id;

        new_entry = true;

        m_data_size += entry.get_data_size();

        entry.write_to_file(m_dictionary_compressor);

        // Track ALL values for bloom filter - even if they're later removed from m_value_to_id
        // (e.g., invariant values stored in MPT). This prevents false negatives during search.
        if (m_use_bloom_filter) {
            m_bloom_filter_values.insert(std::string{value});
        }
    }
    return new_entry;
}

bool LogTypeDictionaryWriter::add_entry(
        LogTypeDictionaryEntry& logtype_entry,
        clp::logtype_dictionary_id_t& logtype_id
) {
    bool is_new_entry = false;

    std::string const& value = logtype_entry.get_value();
    auto const ix = m_value_to_id.find(value);
    if (m_value_to_id.end() != ix) {
        // Entry exists so get its ID
        logtype_id = ix->second;
    } else {
        // Assign ID
        logtype_id = m_next_id;
        ++m_next_id;
        logtype_entry.set_id(logtype_id);

        // Insert new entry into dictionary
        m_value_to_id[value] = logtype_id;

        is_new_entry = true;

        // TODO: This doesn't account for the segment index that's constantly updated
        m_data_size += logtype_entry.get_data_size();

        logtype_entry.write_to_file(m_dictionary_compressor);
    }
    return is_new_entry;
}

size_t VariableDictionaryWriter::write_bloom_filter(
        std::string const& bloom_filter_path,
        int compression_level
) {
    if (!m_use_bloom_filter) {
        return 0;
    }

    // Build bloom filter from ALL values seen, not just those in m_value_to_id
    // This prevents false negatives when values are removed from m_value_to_id
    // (e.g., invariant values stored in MPT per OSDI'24 paper section 5)
    size_t actual_entries = m_bloom_filter_values.size();

    // Create bloom filter with actual entry count (not an estimate)
    // Use 7% false positive rate for optimal I/O trade-off
    m_bloom_filter = BloomFilter(actual_entries, 0.07);

    SPDLOG_INFO(
            "[BLOOM] Creating bloom filter - actual_entries={}, target_fpr=7%",
            actual_entries
    );

    // Add all values from the complete set (includes invariant values)
    for (auto const& value : m_bloom_filter_values) {
        m_bloom_filter.add(value);
    }

    FileWriter bloom_file_writer;
    bloom_file_writer.open(bloom_filter_path, FileWriter::OpenMode::CreateForWriting);

    ZstdCompressor bloom_compressor;
    bloom_compressor.open(bloom_file_writer, compression_level);

    m_bloom_filter.write_to_file(bloom_file_writer, bloom_compressor);

    bloom_compressor.close();
    size_t compressed_size = bloom_file_writer.get_pos();
    bloom_file_writer.close();

    SPDLOG_INFO(
            "[BLOOM] Wrote bloom filter - actual_entries={}, compressed_size={}B ({:.2f}KB)",
            actual_entries,
            compressed_size,
            compressed_size / 1024.0
    );

    // Clear the tracking set to free memory - no longer needed after bloom filter is written
    m_bloom_filter_values.clear();

    return compressed_size;
}
}  // namespace clp_s
