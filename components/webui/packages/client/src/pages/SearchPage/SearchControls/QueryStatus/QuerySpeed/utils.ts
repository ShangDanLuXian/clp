import {querySql} from "../../../../../api/sql";
import {settings} from "../../../../../settings";
import {
    CLP_DATASETS_TABLE_COLUMN_NAMES,
    CLP_S_ARCHIVES_TABLE_COLUMN_NAMES,
} from "../../../../IngestPage/sqlConfig";


interface SqlWithParams {
    sql: string;
    params: unknown[];
}

/**
 * Builds the archives subquery for the query speed SQL, exposing each archive's UUID as
 * `archive_uuid` so that it can be joined against `query_tasks`.
 *
 * @param datasetNames
 * @return The subquery and the values to bind to its placeholders.
 */
const buildQuerySpeedArchivesSubquery = (datasetNames: string[]): SqlWithParams => {
    const columns = `archives.${CLP_S_ARCHIVES_TABLE_COLUMN_NAMES.NUM_UNCOMPRESSED_BYTES}
            AS num_uncompressed_bytes,
        archives.${CLP_S_ARCHIVES_TABLE_COLUMN_NAMES.UUID} AS archive_uuid`;

    if (0 === datasetNames.length) {
        return {
            sql: `SELECT ${columns}
        FROM ${settings.SqlDbClpArchivesTableName} AS archives`,
            params: [],
        };
    }

    return {
        sql: `SELECT ${columns}
    FROM ${settings.SqlDbClpArchivesTableName} AS archives
    JOIN ${settings.SqlDbClpDatasetsTableName} AS datasets
        ON archives.${CLP_S_ARCHIVES_TABLE_COLUMN_NAMES.DATASET_ID} =
           datasets.${CLP_DATASETS_TABLE_COLUMN_NAMES.ID}
    WHERE datasets.${CLP_DATASETS_TABLE_COLUMN_NAMES.NAME} IN (?)`,
        params: [datasetNames],
    };
};

/**
 * Builds a SQL query string to retrieve the total uncompressed bytes and duration
 * for a specific query job across one or more datasets.
 *
 * NOTE: `query_tasks.archive_id` holds an archive's UUID in its canonical text form, so it's
 * converted to the archives table's binary representation to join on `uuid`.
 *
 * TODO: Drop the conversion once `query_tasks.archive_id` is switched to `BINARY(16)`.
 *
 * @param datasetNames
 * @param jobId
 * @return The query and the values to bind to its placeholders, in placeholder order.
 */
const buildQuerySpeedSql = (datasetNames: string[], jobId: string): SqlWithParams => {
    const archivesSubquery = buildQuerySpeedArchivesSubquery(datasetNames);

    return {
        sql: `WITH qt AS (
    SELECT job_id, UNHEX(REPLACE(archive_id, '-', '')) AS archive_uuid
    FROM query_tasks
    WHERE
        archive_id IS NOT NULL
        AND job_id = ?
),
totals AS (
    SELECT
        qt.job_id,
        SUM(ca.num_uncompressed_bytes) AS total_uncompressed_bytes
    FROM qt
    JOIN (${archivesSubquery.sql}) ca
    ON qt.archive_uuid = ca.archive_uuid
)
SELECT
    CAST(totals.total_uncompressed_bytes AS double) AS bytes,
    qj.duration AS duration
FROM query_jobs qj
JOIN totals
ON totals.job_id = qj.id`,
        params: [
            jobId,
            ...archivesSubquery.params,
        ],
    };
};

interface QuerySpeedResp {
    bytes: number | null;
    duration: number | null;
}

/**
 * Fetches the query speed data (bytes and duration) for a specific job ID
 * across the given datasets by executing a SQL query.
 *
 * @param datasetNames
 * @param jobId
 * @return
 */
const fetchQuerySpeed = async (datasetNames: string[], jobId: string): Promise<QuerySpeedResp> => {
    const {sql, params} = buildQuerySpeedSql(datasetNames, jobId);
    const resp = await querySql<QuerySpeedResp[]>(sql, params);
    const [data] = resp.data;
    if ("undefined" === typeof data) {
        throw new Error("Invalid query speed.");
    }

    return data;
};

export {fetchQuerySpeed};
