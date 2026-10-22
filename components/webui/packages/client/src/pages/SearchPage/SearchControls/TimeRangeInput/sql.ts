import {CLP_STORAGE_ENGINES} from "@webui/common/config";
import {Nullable} from "@webui/common/utility-types";
import dayjs, {Dayjs} from "dayjs";

import {querySql} from "../../../../api/sql";
import {SETTINGS_STORAGE_ENGINE} from "../../../../config";
import {settings} from "../../../../settings";
import {
    CLP_ARCHIVES_TABLE_COLUMN_NAMES,
    CLP_DATASETS_TABLE_COLUMN_NAMES,
    CLP_S_ARCHIVES_TABLE_COLUMN_NAMES,
} from "../../../IngestPage/sqlConfig";
import {DEFAULT_TIME_RANGE} from "./utils";


/**
 * Builds a SQL query string to retrieve the minimum and maximum timestamps from the CLP archives
 * table.
 *
 * @return
 */
const buildClpTimeRangeSql = () => `SELECT
    MIN(${CLP_ARCHIVES_TABLE_COLUMN_NAMES.BEGIN_TIMESTAMP})   AS begin_timestamp,
    MAX(${CLP_ARCHIVES_TABLE_COLUMN_NAMES.END_TIMESTAMP})     AS end_timestamp
FROM ${settings.SqlDbClpArchivesTableName}
`;

/**
 * Builds a SQL query string to retrieve the minimum and maximum timestamps across the given
 * CLP-s datasets' archives.
 *
 * @return
 */
const buildClpsTimeRangeSql = (): string => `SELECT
  MIN(archives.${CLP_S_ARCHIVES_TABLE_COLUMN_NAMES.TIMESTAMP_RANGE_BEGIN_MILLIS})
    AS begin_timestamp,
  MAX(archives.${CLP_S_ARCHIVES_TABLE_COLUMN_NAMES.TIMESTAMP_RANGE_END_MILLIS})
    AS end_timestamp
FROM ${settings.SqlDbClpArchivesTableName} AS archives
JOIN ${settings.SqlDbClpDatasetsTableName} AS datasets
  ON archives.${CLP_S_ARCHIVES_TABLE_COLUMN_NAMES.DATASET_ID} =
     datasets.${CLP_DATASETS_TABLE_COLUMN_NAMES.ID}
WHERE datasets.${CLP_DATASETS_TABLE_COLUMN_NAMES.NAME} IN (?)`;

/**
 * Fetches the earliest and latest log entry timestamps ("all time" range)
 * from the configured storage engine (CLP or CLPS).
 *
 * @param selectedDatasets
 * @return
 */
const fetchAllTimeRange = async (selectedDatasets: string[]): Promise<[Dayjs, Dayjs]> => {
    let sql: string;
    let queryParams: unknown[] | undefined;
    if (CLP_STORAGE_ENGINES.CLP === SETTINGS_STORAGE_ENGINE) {
        sql = buildClpTimeRangeSql();
    } else {
        if (0 === selectedDatasets.length) {
            console.error("Cannot fetch \"All Time\" time range. No selected datasets.");

            return DEFAULT_TIME_RANGE;
        }
        sql = buildClpsTimeRangeSql();
        queryParams = [selectedDatasets];
    }
    const resp = await querySql<
        {
            begin_timestamp: Nullable<number>;
            end_timestamp: Nullable<number>;
        }[]
    >(sql, queryParams);
    const [timestamps] = resp.data;
    if ("undefined" === typeof timestamps ||
              null === timestamps.begin_timestamp ||
              null === timestamps.end_timestamp
    ) {
        return DEFAULT_TIME_RANGE;
    }

    return [
        dayjs.utc(timestamps.begin_timestamp),
        dayjs.utc(timestamps.end_timestamp),
    ];
};

export {fetchAllTimeRange};
