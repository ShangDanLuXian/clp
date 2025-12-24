#ifndef CLP_S_ARCHIVEREADER_HPP
#define CLP_S_ARCHIVEREADER_HPP

#include <cstdint>
#include <map>
#include <set>
#include <span>
#include <string_view>
#include <utility>

#include "ArchiveReaderAdaptor.hpp"
#include "DictionaryReader.hpp"
#include "InputConfig.hpp"
#include "PackedStreamReader.hpp"
#include "ReaderUtils.hpp"
#include "SchemaReader.hpp"
#include "clp_s/filter/SchemaIntColumnFilter.hpp"
#include "clp_s/filter/SchemaStringColumnFilter.hpp"
#include "search/Projection.hpp"
#include "TimestampDictionaryReader.hpp"

namespace clp_s {
class ArchiveReader {
public:
    class OperationFailed : public TraceableException {
    public:
        // Constructors
        OperationFailed(ErrorCode error_code, char const* const filename, int line_number)
                : TraceableException(error_code, filename, line_number) {}
    };

    // Constructor
    ArchiveReader() : m_is_open(false) {}

    /**
     * Opens an archive for reading.
     * @param archive_path
     * @param network_auth
     */
    void open(Path const& archive_path, NetworkAuthOption const& network_auth);

    /**
     * Reads the dictionaries and metadata.
     */
    void read_dictionaries_and_metadata();

    /**
     * Opens packed streams for reading.
     */
    void open_packed_streams();

    /**
     * Reads the variable dictionary from the archive.
     * @param lazy
     * @return the variable dictionary reader
     */
    std::shared_ptr<VariableDictionaryReader> read_variable_dictionary(bool lazy = false) {
        m_var_dict->read_entries(lazy);
        return m_var_dict;
    }

    /**
     * Reads the log type dictionary from the archive.
     * @param lazy
     * @return the log type dictionary reader
     */
    std::shared_ptr<LogTypeDictionaryReader> read_log_type_dictionary(bool lazy = false) {
        m_log_dict->read_entries(lazy);
        return m_log_dict;
    }

    /**
     * Reads the array dictionary from the archive.
     * @param lazy
     * @return the array dictionary reader
     */
    std::shared_ptr<LogTypeDictionaryReader> read_array_dictionary(bool lazy = false) {
        m_array_dict->read_entries(lazy);
        return m_array_dict;
    }

    /**
     * Reads the metadata from the archive.
     */
    void read_metadata();

    /**
     * Reads a table from the archive.
     * @param schema_id
     * @param should_extract_timestamp
     * @param should_marshal_records
     * @return the schema reader
     */
    SchemaReader& read_schema_table(
            int32_t schema_id,
            bool should_extract_timestamp,
            bool should_marshal_records
    );


        /**
     * Sets whether to use schema filters for query optimization.
     * @param use_schema_filter Whether to enable schema filters
     */
     void set_use_schema_filter(bool use_schema_filter) {
        m_use_schema_filter = use_schema_filter;
    }

    /**
     * Preloads filters for the given schema IDs before packed streams are opened.
     * This must be called before open_packed_streams() to avoid reader checkout conflicts.
     * @param schema_ids The schema IDs to preload filters for
     */
    void preload_schema_filters(std::vector<int32_t> const& schema_ids);


    void preload_schema_int_filters(std::vector<int32_t> const& schema_ids);

    void preload_schema_str_filters(std::vector<int32_t> const& schema_ids);

    /**
     * Checks if any of the given variable IDs might be in the schema's filter.
     * Uses preloaded filters cached in memory.
     * @param schema_id The schema ID to check
     * @param var_ids The set of variable dictionary IDs to check
     * @return true if any variable might be in the schema (or if filter not available),
     *         false if definitely none of the variables are in the schema
     */
    bool schema_filter_check(
            int32_t schema_id,
            std::unordered_set<clp::variable_dictionary_id_t> const& var_ids
    );


    bool schema_int_filter_check(
        int32_t schema_id,
        int32_t column_id,
        int64_t value
    );

    bool schema_str_filter_check(
        int32_t schema_id,
        int32_t column_id,
        std::string value
    );

    /**
     * Loads all of the tables in the archive and returns SchemaReaders for them.
     * @return the schema readers for every table in the archive
     */
    std::vector<std::shared_ptr<SchemaReader>> read_all_tables();

    std::string_view get_archive_id() { return m_archive_id; }

    std::shared_ptr<VariableDictionaryReader> get_variable_dictionary() { return m_var_dict; }

    std::shared_ptr<LogTypeDictionaryReader> get_log_type_dictionary() { return m_log_dict; }

    std::shared_ptr<LogTypeDictionaryReader> get_array_dictionary() { return m_array_dict; }

    std::shared_ptr<TimestampDictionaryReader> get_timestamp_dictionary() {
        return m_archive_reader_adaptor->get_timestamp_dictionary();
    }

    std::shared_ptr<SchemaTree> get_schema_tree() { return m_schema_tree; }

    std::shared_ptr<ReaderUtils::SchemaMap> get_schema_map() { return m_schema_map; }

    auto get_range_index() const -> std::vector<RangeIndexEntry> const& {
        return m_archive_reader_adaptor->get_range_index();
    }

    /**
     * Writes decoded messages to a file.
     * @param writer
     */
    void store(FileWriter& writer);

    /**
     * Closes the archive.
     */
    void close();

    /**
     * @return The schema ids in the archive. It also defines the order that tables should be read
     * in to avoid seeking backwards.
     */
    [[nodiscard]] std::vector<int32_t> const& get_schema_ids() const { return m_schema_ids; }

    void set_projection(std::shared_ptr<search::Projection> projection) {
        m_projection = projection;
    }

    /**
     * @return true if this archive has log ordering information, and false otherwise.
     */
    bool has_log_order() { return m_log_event_idx_column_id >= 0; }

private:
    /**
     * Initializes a schema reader passed by reference to become a reader for a given schema.
     * @param reader
     * @param schema_id
     * @param should_extract_timestamp
     * @param should_marshal_records
     */
    void initialize_schema_reader(
            SchemaReader& reader,
            int32_t schema_id,
            bool should_extract_timestamp,
            bool should_marshal_records
    );

    /**
     * Appends a column to the schema reader.
     * @param reader
     * @param column_id
     * @return a pointer to the newly appended column reader or nullptr if no column reader was
     * created
     */
    BaseColumnReader* append_reader_column(SchemaReader& reader, int32_t column_id);

    /**
     * Appends columns for the entire schema of an unordered object.
     * @param reader
     * @param mst_subtree_root_node_id
     * @param schema_ids
     * @param should_marshal_records
     */
    void append_unordered_reader_columns(
            SchemaReader& reader,
            int32_t mst_subtree_root_node_id,
            std::span<int32_t> schema_ids,
            bool should_marshal_records
    );

    /**
     * Reads a table with given ID from the packed stream reader. If read_stream is called multiple
     * times in a row for the same stream_id a cached buffer is returned. This function allows the
     * caller to ask for the same buffer to be reused to read multiple different tables: this can
     * save memory allocations, but can only be used when tables are read one at a time.
     * @param stream_id
     * @param reuse_buffer when true the same buffer is reused across invocations, overwriting data
     * returned previous calls to read_stream
     * @return a buffer containing the decompressed stream identified by stream_id
     */
    std::shared_ptr<char[]> read_stream(size_t stream_id, bool reuse_buffer);

    bool m_is_open;
    std::string m_archive_id;
    std::shared_ptr<VariableDictionaryReader> m_var_dict;
    std::shared_ptr<LogTypeDictionaryReader> m_log_dict;
    std::shared_ptr<LogTypeDictionaryReader> m_array_dict;
    std::shared_ptr<ArchiveReaderAdaptor> m_archive_reader_adaptor;

    std::shared_ptr<SchemaTree> m_schema_tree;
    std::shared_ptr<ReaderUtils::SchemaMap> m_schema_map;
    std::vector<int32_t> m_schema_ids;
    std::map<int32_t, SchemaReader::SchemaMetadata> m_id_to_schema_metadata;
    std::shared_ptr<search::Projection> m_projection{
            std::make_shared<search::Projection>(search::ProjectionMode::ReturnAllColumns)
    };

    PackedStreamReader m_stream_reader;
    ZstdDecompressor m_table_metadata_decompressor;
    SchemaReader m_schema_reader;
    std::shared_ptr<char[]> m_stream_buffer{};
    size_t m_stream_buffer_size{0ULL};
    size_t m_cur_stream_id{0ULL};
    int32_t m_log_event_idx_column_id{-1};

    // Schema filter settings and cache
    bool m_use_schema_filter{true};
    std::map<int32_t, ProbabilisticFilter> m_schema_filters;
    std::map<int32_t, SchemaIntColumnFilter> m_schema_int_filters;
    std::map<int32_t, SchemaStringColumnFilter> m_schema_str_filters;
};
}  // namespace clp_s

#endif  // CLP_S_ARCHIVEREADER_HPP
