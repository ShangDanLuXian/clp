//! Reads of the column metadata table.

use crate::ColumnType;
use crate::DatasetId;
use crate::Error;
use crate::MetadataTables;

impl MetadataTables {
    /// Gets the names of a live dataset's columns that have any of `types`.
    ///
    /// # Returns
    ///
    /// The distinct matching column names, ordered by name, on success, or none if the dataset
    /// doesn't exist or is marked as deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryScalar::fetch_all`]'s return values on failure.
    pub async fn get_column_names_by_types(
        &self,
        conn: &mut sqlx::MySqlConnection,
        dataset_id: DatasetId,
        types: &[ColumnType],
    ) -> Result<Vec<String>, Error> {
        if types.is_empty() {
            return Ok(Vec::new());
        }

        // `sqlx` doesn't decode strings from `utf8mb4_bin` columns, which are reported as binary.
        // Grouping by the raw column keeps the names distinct and ordered byte-wise.
        let mut query_builder = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
            r"
SELECT CONVERT(`c`.`name` USING utf8mb4)
FROM `{column_metadata}` AS `c`
JOIN `{datasets}` AS `d` ON `d`.`id` = `c`.`dataset_id`
WHERE `d`.`is_deleted` = FALSE AND `c`.`dataset_id` = ",
            column_metadata = self.column_metadata,
            datasets = self.datasets,
        ));
        query_builder
            .push_bind(dataset_id)
            .push(" AND `c`.`type` IN (");
        let mut separated_types = query_builder.separated(", ");
        for column_type in types {
            // The unsigned value never equals a value of the signed `TINYINT` column that it
            // doesn't represent, including for `ColumnType::Unknown`.
            separated_types.push_bind(*column_type as u8);
        }
        query_builder.push(") GROUP BY `c`.`name` ORDER BY `c`.`name`");
        Ok(query_builder
            .build_query_scalar()
            .fetch_all(&mut *conn)
            .await?)
    }
}
