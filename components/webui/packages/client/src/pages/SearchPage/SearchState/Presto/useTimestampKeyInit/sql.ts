import {SqlTableSuffix} from "@webui/common/config";

import {querySql} from "../../../../../api/sql";
import {settings} from "../../../../../settings";
import {
    CLP_DATASETS_TABLE_COLUMN_NAMES,
    CLP_S_COLUMN_METADATA_TABLE_COLUMN_NAMES,
} from "../../../../IngestPage/sqlConfig";


/**
 * Matching the `NodeType::DeprecatedDateString` and `NodeType::Timestamp` values in
 * `clp/components/core/src/clp_s/SchemaTree.hpp`.
 */
const DEPRECATED_TIMESTAMP_TYPE = 8;
const TIMESTAMP_TYPE = 14;

interface TimestampColumnItem {
    [CLP_S_COLUMN_METADATA_TABLE_COLUMN_NAMES.NAME]: string;
}

/**
 * Builds SQL query to get timestamp columns for a specific dataset.
 *
 * @return
 */
const buildTimestampColumnsSql = (): string => `
    SELECT DISTINCT
        columns.${CLP_S_COLUMN_METADATA_TABLE_COLUMN_NAMES.NAME}
    FROM ${settings.SqlDbClpTablePrefix}${SqlTableSuffix.COLUMN_METADATA} AS columns
    JOIN ${settings.SqlDbClpDatasetsTableName} AS datasets
        ON columns.${CLP_S_COLUMN_METADATA_TABLE_COLUMN_NAMES.DATASET_ID} =
           datasets.${CLP_DATASETS_TABLE_COLUMN_NAMES.ID}
    WHERE datasets.${CLP_DATASETS_TABLE_COLUMN_NAMES.NAME} = ?
    AND columns.${CLP_S_COLUMN_METADATA_TABLE_COLUMN_NAMES.TYPE} IN
    (${TIMESTAMP_TYPE}, ${DEPRECATED_TIMESTAMP_TYPE})
    ORDER BY columns.${CLP_S_COLUMN_METADATA_TABLE_COLUMN_NAMES.NAME};
`;

/**
 * Fetches timestamp column names for a specific dataset.
 *
 * @param datasetName
 * @return
 */
const fetchTimestampColumns = async (datasetName: string): Promise<string[]> => {
    const sql = buildTimestampColumnsSql();
    const resp = await querySql<TimestampColumnItem[]>(sql, [datasetName]);
    return resp.data.map((column) => column.name);
};

export {fetchTimestampColumns};
