import axios from "axios";


/**
 * Query the SQL server with the queryString.
 *
 * @param queryString
 * @param queryParams Values to bind to the query's placeholders.
 * @return
 */
const querySql = async <T>(queryString: string, queryParams?: unknown[]) => {
    return axios.post<T>("/api/archive-metadata/sql", {
        queryParams,
        queryString,
    });
};

export {querySql};
