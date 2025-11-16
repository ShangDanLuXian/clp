// Code from CLP

#ifndef CLP_S_DICTIONARYREADER_HPP
#define CLP_S_DICTIONARYREADER_HPP

#include <iterator>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

#include <boost/algorithm/string/case_conv.hpp>
#include <spdlog/spdlog.h>
#include <string_utils/string_utils.hpp>

#include "../clp/Defs.h"
#include "ArchiveReaderAdaptor.hpp"
#include "BloomFilter.hpp"
#include "DictionaryEntry.hpp"

namespace clp_s {
template <typename DictionaryIdType, typename EntryType>
class DictionaryReader {
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
    DictionaryReader(ArchiveReaderAdaptor& adaptor) : m_is_open(false), m_adaptor(adaptor) {}

    // Methods
    /**
     * Opens dictionary for reading
     * @param dictionary_path
     */
    void open(std::string const& dictionary_path);

    /**
     * Closes the dictionary
     */
    void close();

    /**
     * Reads all entries from disk
     */
    void read_entries(bool lazy = false);

    /**
     * @return All dictionary entries
     */
    std::vector<EntryType> const& get_entries() const { return m_entries; }

    /**
     * @param id
     * @return The entry with the given ID
     */
    EntryType& get_entry(DictionaryIdType id);

    /**
     * @param id
     * @return Value of the entry with the specified ID
     */
    std::string const& get_value(DictionaryIdType id) const;

    /**
     * Gets the entries matching the given search string
     * @param search_string
     * @param ignore_case
     * @return a vector of matching entries, or an empty vector if no entry matches.
     */
    std::vector<EntryType const*>
    get_entry_matching_value(std::string_view search_string, bool ignore_case) const;

    /**
     * Gets the entries that match a given wildcard string
     * @param wildcard_string
     * @param ignore_case
     * @param entries Set in which to store found entries
     */
    void get_entries_matching_wildcard_string(
            std::string_view wildcard_string,
            bool ignore_case,
            std::unordered_set<EntryType const*>& entries
    ) const;

    /**
     * Loads the bloom filter from disk if available
     * @param bloom_filter_path Path to the bloom filter file
     * @return true if bloom filter was loaded successfully, false otherwise
     */
    bool load_bloom_filter(std::string const& bloom_filter_path);

    /**
     * @return Whether a bloom filter is loaded
     */
    [[nodiscard]] bool has_bloom_filter() const { return m_bloom_filter_loaded; }

    /**
     * Enable or disable the use of bloom filter for lookups
     * @param use_bloom_filter Whether to use the bloom filter
     */
    void set_use_bloom_filter(bool use_bloom_filter) { m_use_bloom_filter = use_bloom_filter; }

    /**
     * Check if a string possibly exists in the dictionary using the bloom filter.
     * This can be called before loading the dictionary entries.
     *
     * @param search_string The string to check
     * @return true if the string might exist (or bloom filter not loaded), false if definitely doesn't exist
     */
    [[nodiscard]] bool bloom_filter_might_contain(std::string_view search_string) const {
        if (!m_bloom_filter_loaded || !m_use_bloom_filter) {
            return true;  // If no bloom filter, assume it might contain
        }
        return m_bloom_filter.possibly_contains(search_string);
    }

protected:
    bool m_is_open;
    ArchiveReaderAdaptor& m_adaptor;
    std::string m_dictionary_path;
    ZstdDecompressor m_dictionary_decompressor;
    std::vector<EntryType> m_entries;
    BloomFilter m_bloom_filter;
    bool m_bloom_filter_loaded{false};
    bool m_use_bloom_filter{true};  // Default to true (enabled)
};

using VariableDictionaryReader
        = DictionaryReader<clp::variable_dictionary_id_t, VariableDictionaryEntry>;
using LogTypeDictionaryReader
        = DictionaryReader<clp::logtype_dictionary_id_t, LogTypeDictionaryEntry>;

template <typename DictionaryIdType, typename EntryType>
void DictionaryReader<DictionaryIdType, EntryType>::open(std::string const& dictionary_path) {
    if (m_is_open) {
        throw OperationFailed(ErrorCodeNotReady, __FILENAME__, __LINE__);
    }

    m_dictionary_path = dictionary_path;
    m_is_open = true;
}

template <typename DictionaryIdType, typename EntryType>
void DictionaryReader<DictionaryIdType, EntryType>::close() {
    if (false == m_is_open) {
        throw OperationFailed(ErrorCodeNotReady, __FILENAME__, __LINE__);
    }
    m_is_open = false;
}

template <typename DictionaryIdType, typename EntryType>
void DictionaryReader<DictionaryIdType, EntryType>::read_entries(bool lazy) {
    if (false == m_is_open) {
        throw OperationFailed(ErrorCodeNotInit, __FILENAME__, __LINE__);
    }

    constexpr size_t cDecompressorFileReadBufferCapacity = 64 * 1024;  // 64 KB
    auto dictionary_reader = m_adaptor.checkout_reader_for_section(m_dictionary_path);

    uint64_t num_dictionary_entries;
    dictionary_reader->read_numeric_value(num_dictionary_entries, false);
    m_dictionary_decompressor.open(*dictionary_reader, cDecompressorFileReadBufferCapacity);

    // Read dictionary entries
    m_entries.resize(num_dictionary_entries);
    for (size_t i = 0; i < num_dictionary_entries; ++i) {
        auto& entry = m_entries[i];
        entry.read_from_file(m_dictionary_decompressor, i, lazy);
    }

    m_dictionary_decompressor.close();
    m_adaptor.checkin_reader_for_section(m_dictionary_path);
}

template <typename DictionaryIdType, typename EntryType>
EntryType& DictionaryReader<DictionaryIdType, EntryType>::get_entry(DictionaryIdType id) {
    if (false == m_is_open) {
        throw OperationFailed(ErrorCodeNotInit, __FILENAME__, __LINE__);
    }
    if (id >= m_entries.size()) {
        throw OperationFailed(ErrorCodeBadParam, __FILENAME__, __LINE__);
    }

    return m_entries[id];
}

template <typename DictionaryIdType, typename EntryType>
std::string const&
DictionaryReader<DictionaryIdType, EntryType>::get_value(DictionaryIdType id) const {
    if (id >= m_entries.size()) {
        throw OperationFailed(ErrorCodeCorrupt, __FILENAME__, __LINE__);
    }
    return m_entries[id].get_value();
}

template <typename DictionaryIdType, typename EntryType>
std::vector<EntryType const*>
DictionaryReader<DictionaryIdType, EntryType>::get_entry_matching_value(
        std::string_view search_string,
        bool ignore_case
) const {
    // Check bloom filter for case-sensitive exact match (fast negative lookup)
    if (false == ignore_case && m_bloom_filter_loaded && m_use_bloom_filter) {
        if (!m_bloom_filter.possibly_contains(search_string)) {
            // Definitely not in the dictionary
            SPDLOG_DEBUG(
                    "[BLOOM] String '{}' not found in bloom filter, skipping dictionary lookup",
                    search_string
            );
            return {};
        }
        SPDLOG_DEBUG(
                "[BLOOM] String '{}' possibly in bloom filter, proceeding with dictionary lookup",
                search_string
        );
    }

    if (false == ignore_case) {
        // In case-sensitive match, there can be only one matched entry.
        if (auto const it = std::ranges::find_if(
                    m_entries,
                    [&](auto const& entry) { return entry.get_value() == search_string; }
            );
            m_entries.cend() != it)
        {
            return {&(*it)};
        }
        return {};
    }

    std::vector<EntryType const*> entries;
    std::string search_string_uppercase;
    std::ignore = boost::algorithm::to_upper_copy(
            std::back_inserter(search_string_uppercase),
            search_string
    );
    for (auto const& entry : m_entries) {
        if (boost::algorithm::to_upper_copy(entry.get_value()) == search_string_uppercase) {
            entries.push_back(&entry);
        }
    }
    return entries;
}

template <typename DictionaryIdType, typename EntryType>
void DictionaryReader<DictionaryIdType, EntryType>::get_entries_matching_wildcard_string(
        std::string_view wildcard_string,
        bool ignore_case,
        std::unordered_set<EntryType const*>& entries
) const {
    for (auto const& entry : m_entries) {
        if (clp::string_utils::wildcard_match_unsafe(
                    entry.get_value(),
                    wildcard_string,
                    !ignore_case
            ))
        {
            entries.insert(&entry);
        }
    }
}

template <typename DictionaryIdType, typename EntryType>
bool DictionaryReader<DictionaryIdType, EntryType>::load_bloom_filter(
        std::string const& bloom_filter_path
) {
    if (false == m_is_open) {
        throw OperationFailed(ErrorCodeNotInit, __FILENAME__, __LINE__);
    }

    try {
        constexpr size_t cDecompressorFileReadBufferCapacity = 64 * 1024;  // 64 KB
        auto bloom_reader = m_adaptor.checkout_reader_for_section(bloom_filter_path);

        ZstdDecompressor bloom_decompressor;
        bloom_decompressor.open(*bloom_reader, cDecompressorFileReadBufferCapacity);

        bool success = m_bloom_filter.read_from_file(*bloom_reader, bloom_decompressor);

        bloom_decompressor.close();
        m_adaptor.checkin_reader_for_section(bloom_filter_path);

        if (success) {
            m_bloom_filter_loaded = true;
        }

        return success;
    } catch (...) {
        // Bloom filter is optional, so if it fails to load, just return false
        m_bloom_filter_loaded = false;
        return false;
    }
}
}  // namespace clp_s

#endif  // CLP_S_DICTIONARYREADER_HPP
