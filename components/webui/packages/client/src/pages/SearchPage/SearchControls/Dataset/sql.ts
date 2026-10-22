import {querySql} from "../../../../api/sql";
import {settings} from "../../../../settings";
import {CLP_DATASETS_TABLE_COLUMN_NAMES} from "../../../IngestPage/sqlConfig";


/**
 * SQL query to get all dataset names.
 */
const GET_DATASETS_SQL = `
    SELECT
        ${CLP_DATASETS_TABLE_COLUMN_NAMES.NAME} AS name
    FROM ${settings.SqlDbClpDatasetsTableName}
    WHERE ${CLP_DATASETS_TABLE_COLUMN_NAMES.IS_DELETED} = FALSE
    ORDER BY ${CLP_DATASETS_TABLE_COLUMN_NAMES.NAME};
`;

interface DatasetItem {
    [CLP_DATASETS_TABLE_COLUMN_NAMES.NAME]: string;
}

/**
 * Fetches all dataset names from the datasets table.
 *
 * @return
 */
const fetchDatasetNames = async (): Promise<string[]> => {
    const resp = await querySql<DatasetItem[]>(GET_DATASETS_SQL);
    return resp.data.map((dataset) => dataset.name);
};

export {fetchDatasetNames};
