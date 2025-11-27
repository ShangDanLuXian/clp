// Code from CLP

#ifndef CLP_S_DICTIONARYWRITER_HPP
#define CLP_S_DICTIONARYWRITER_HPP

#include <cstddef>
#include <string>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include "../clp/Defs.h"
#include "DictionaryEntry.hpp"
#include "clp_s/ZstdCompressor.hpp"
#include "clp_s/archive_constants.hpp"
#include "filter/ProbabilisticFilter.hpp"

namespace clp_s {
template <typename DictionaryIdType, typename EntryType>
class DictionaryWriter {
public:
    // Types
    class OperationFailed : public TraceableException {
    public:
        // Constructors
        OperationFailed(ErrorCode error_code, char const* const filename, int line_number)
                : TraceableException(error_code, filename, line_number) {}
    };

    using dictionary_id_t = DictionaryIdType;
    using Entry = EntryType;

    // Constructors
    DictionaryWriter() : m_is_open(false) {}

    ~DictionaryWriter() = default;

    // Methods
    /**
     * Opens dictionary for writing
     * @param dictionary_path
     * @param compression_level
     * @param max_id
     */
    void open(std::string const& dictionary_path, int compression_level, DictionaryIdType max_id);

        /**
     * Opens dictionary for writing with a filter
     * @param dictionary_path
     * @param compression_level
     * @param max_id
     * @param filter_path
     * @param filter_type
     */
     void open(std::string const& dictionary_path, int compression_level, DictionaryIdType max_id, FilterType filter_type);

    /**
     * Closes the dictionary
     * @return the compressed size of the dictionary in bytes
     */
    [[nodiscard]] size_t close();

    /**
     * Writes the dictionary's header and flushes unwritten content to disk
     */

    void write_header_and_flush_to_disk();

    /**
     * @return The size (in-memory) of the data contained in the dictionary
     */
    size_t get_data_size() const { return m_data_size; }

    /**
     * Writes the filter to disk
     * @return Size of the compressed filter file in bytes
     */
     [[nodiscard]] size_t write_filter();

protected:
    // Types
    using value_to_id_t = absl::flat_hash_map<std::string, DictionaryIdType>;

    // Variables
    bool m_is_open;

    // Variables related to on-disk storage
    FileWriter m_dictionary_file_writer;
    ZstdCompressor m_dictionary_compressor;

    value_to_id_t m_value_to_id;
    uint64_t m_next_id{};
    uint64_t m_max_id{};

    // Size (in-memory) of the data contained in the dictionary
    size_t m_data_size{};

    // Filter related to on-disck storage
    FileWriter m_filter_file_writer;
    ZstdCompressor m_filter_compressor;

    ProbabilisticFilter m_filter;
    FilterType m_filter_type = FilterType::None;
    // Track ALL values seen for filter, even if they're later removed from m_value_to_id
    // (e.g., invariant values that get stored in MPT instead of variable dictionary)
    absl::flat_hash_set<std::string> m_filter_values;
};

class VariableDictionaryWriter
        : public DictionaryWriter<clp::variable_dictionary_id_t, VariableDictionaryEntry> {
public:
    class OperationFailed : public TraceableException {
    public:
        // Constructors
        OperationFailed(ErrorCode error_code, char const* const filename, int line_number)
                : TraceableException(error_code, filename, line_number) {}
    };

    /**
     * Adds the given variable to the dictionary if it doesn't exist.
     * @param value
     * @param id ID of the variable matching the given entry
     */
    bool add_entry(std::string_view value, clp::variable_dictionary_id_t& id);

    bool add_int_for_filter(int value);
};

class LogTypeDictionaryWriter
        : public DictionaryWriter<clp::logtype_dictionary_id_t, LogTypeDictionaryEntry> {
public:
    class OperationFailed : public TraceableException {
    public:
        // Constructors
        OperationFailed(ErrorCode error_code, char const* const filename, int line_number)
                : TraceableException(error_code, filename, line_number) {}
    };

    /**
     * Adds the given entry to the dictionary if it doesn't exist
     * @param logtype_entry
     * @param logtype_id ID of the logtype matching the given entry
     */
    bool add_entry(LogTypeDictionaryEntry& logtype_entry, clp::logtype_dictionary_id_t& logtype_id);
};

template <typename DictionaryIdType, typename EntryType>
void DictionaryWriter<DictionaryIdType, EntryType>::open(
        std::string const& dictionary_path,
        int compression_level,
        DictionaryIdType max_id
) {
    if (m_is_open) {
        throw OperationFailed(ErrorCodeNotReady, __FILENAME__, __LINE__);
    }

    m_dictionary_file_writer.open(dictionary_path, FileWriter::OpenMode::CreateForWriting);
    // Write header
    m_dictionary_file_writer.write_numeric_value<uint64_t>(0);
    // Open compressor
    m_dictionary_compressor.open(m_dictionary_file_writer, compression_level);

    m_next_id = 0;
    m_max_id = max_id;

    m_data_size = 0;
    m_is_open = true;

}

template <typename DictionaryIdType, typename EntryType>
void DictionaryWriter<DictionaryIdType, EntryType>::open(
        std::string const& dictionary_path,
        int compression_level,
        DictionaryIdType max_id,
        FilterType filter_type
) {
    open(dictionary_path, compression_level, max_id);
    m_filter_type = filter_type;
    if (m_filter_type != FilterType::None) {
        m_filter_file_writer.open(dictionary_path + constants::cArchiveFilterFileSuffix, FileWriter::OpenMode::CreateForWriting);
        m_filter_compressor.open(m_filter_file_writer, compression_level);
    }
}

template <typename DictionaryIdType, typename EntryType>
size_t DictionaryWriter<DictionaryIdType, EntryType>::close() {
    if (false == m_is_open) {
        throw OperationFailed(ErrorCodeNotInit, __FILENAME__, __LINE__);
    }

    write_header_and_flush_to_disk();
    m_dictionary_compressor.close();
    size_t compressed_size = m_dictionary_file_writer.get_pos();
    m_dictionary_file_writer.close();

    if (m_filter_type != FilterType::None) {
        // TODO Make use of compressed bytes
        (void)write_filter();
    }

    m_value_to_id.clear();

    m_is_open = false;
    return compressed_size;
}

template <typename DictionaryIdType, typename EntryType>
void DictionaryWriter<DictionaryIdType, EntryType>::write_header_and_flush_to_disk() {
    if (false == m_is_open) {
        throw OperationFailed(ErrorCodeNotInit, __FILENAME__, __LINE__);
    }

    // Update header
    auto dictionary_file_writer_pos = m_dictionary_file_writer.get_pos();
    m_dictionary_file_writer.seek_from_begin(0);
    m_dictionary_file_writer.write_numeric_value<uint64_t>(m_value_to_id.size());
    m_dictionary_file_writer.seek_from_begin(dictionary_file_writer_pos);

    m_dictionary_compressor.flush();
    m_dictionary_file_writer.flush();
}

template <typename DictionaryIdType, typename EntryType>
size_t DictionaryWriter<DictionaryIdType, EntryType>::write_filter(
) {
if (m_filter_type == FilterType::None) {
    return 0;
}

m_filter = ProbabilisticFilter(m_filter_type, m_filter_values, 0.07);


m_filter.write_to_file(m_filter_file_writer, m_filter_compressor);

m_filter_compressor.close();
size_t compressed_size = m_filter_file_writer.get_pos();
m_filter_file_writer.close();

m_filter_values.clear();

return compressed_size;
}
}  // namespace clp_s

#endif  // CLP_S_DICTIONARYWRITER_HPP
