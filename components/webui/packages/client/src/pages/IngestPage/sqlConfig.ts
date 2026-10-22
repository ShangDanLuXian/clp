/**
 * Column names for the `clp_archives` table as written by the clp-text storage engine.
 */
enum CLP_ARCHIVES_TABLE_COLUMN_NAMES {
    BEGIN_TIMESTAMP = "begin_timestamp",
    END_TIMESTAMP = "end_timestamp",
    UNCOMPRESSED_SIZE = "uncompressed_size",
    SIZE = "size",
}

/**
 * Column names for the `clp_files` table.
 */
enum CLP_FILES_TABLE_COLUMN_NAMES {
    ORIG_FILE_ID = "orig_file_id",
    NUM_MESSAGES = "num_messages",
}

/**
 * Column names for the `clp_archives` table shared by every clp-s dataset.
 */
enum CLP_S_ARCHIVES_TABLE_COLUMN_NAMES {
    DATASET_ID = "dataset_id",
    NUM_COMPRESSED_BYTES = "num_compressed_bytes",
    NUM_UNCOMPRESSED_BYTES = "num_uncompressed_bytes",
    TIMESTAMP_RANGE_BEGIN_MILLIS = "timestamp_range_begin_millis",
    TIMESTAMP_RANGE_END_MILLIS = "timestamp_range_end_millis",
    UUID = "uuid",
}

/**
 * Column names for the `clp_datasets` table.
 */
enum CLP_DATASETS_TABLE_COLUMN_NAMES {
    ID = "id",
    IS_DELETED = "is_deleted",
    NAME = "name",
}

/**
 * Column names for the `clp_column_metadata` table shared by every clp-s dataset.
 */
enum CLP_S_COLUMN_METADATA_TABLE_COLUMN_NAMES {
    DATASET_ID = "dataset_id",
    NAME = "name",
    TYPE = "type",
}

export {
    CLP_ARCHIVES_TABLE_COLUMN_NAMES,
    CLP_DATASETS_TABLE_COLUMN_NAMES,
    CLP_FILES_TABLE_COLUMN_NAMES,
    CLP_S_ARCHIVES_TABLE_COLUMN_NAMES,
    CLP_S_COLUMN_METADATA_TABLE_COLUMN_NAMES,
};
