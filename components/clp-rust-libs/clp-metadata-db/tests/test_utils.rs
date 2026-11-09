use std::str::FromStr;

use clp_metadata_db::MetadataTables;
use clp_rust_utils::clp_config::package::config::Database;
use sqlx::Connection;
use sqlx::MySqlConnection;
use sqlx::mysql::MySqlConnectOptions;
use uuid::Uuid;

/// A uniquely named database on the test server, used by a single test.
pub struct TestDatabase {
    pub conn: MySqlConnection,
    pub database_config: Database,
    pub tables: MetadataTables,
    name: String,
    options: MySqlConnectOptions,
}

impl TestDatabase {
    /// Factory function.
    ///
    /// Creates a uniquely named database on the server at `CLP_METADATA_DB_TEST_URL` (e.g.
    /// `mysql://root:password@127.0.0.1:3306`) and connects to it. The metadata tables are named
    /// after the default [`Database`] config.
    ///
    /// # Returns
    ///
    /// The newly created test database.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// * `CLP_METADATA_DB_TEST_URL` isn't set or isn't a valid MySQL URL.
    /// * The database can't be created or connected to.
    pub async fn create() -> Self {
        const TEST_URL_ENV_VAR: &str = "CLP_METADATA_DB_TEST_URL";

        let server_url = std::env::var(TEST_URL_ENV_VAR)
            .expect("`CLP_METADATA_DB_TEST_URL` should be set to a MariaDB or MySQL server URL");
        let server_options = MySqlConnectOptions::from_str(&server_url)
            .expect("`CLP_METADATA_DB_TEST_URL` should be a valid MySQL URL");
        let name = format!("clp_metadata_db_test_{}", Uuid::new_v4().simple());

        let mut server_conn = MySqlConnection::connect_with(&server_options)
            .await
            .expect("failed to connect to the test server");
        sqlx::query(&format!("CREATE DATABASE `{name}`"))
            .execute(&mut server_conn)
            .await
            .expect("failed to create the test database");
        server_conn
            .close()
            .await
            .expect("failed to close the test server connection");

        let options = server_options.database(&name);
        let conn = MySqlConnection::connect_with(&options)
            .await
            .expect("failed to connect to the test database");
        let database_config = Database::default();
        let tables = MetadataTables::new(&database_config);
        Self {
            conn,
            database_config,
            tables,
            name,
            options,
        }
    }

    /// Opens another connection to the test database.
    ///
    /// # Returns
    ///
    /// The new connection.
    ///
    /// # Panics
    ///
    /// Panics if the test database can't be connected to.
    pub async fn connect(&self) -> MySqlConnection {
        MySqlConnection::connect_with(&self.options)
            .await
            .expect("failed to connect to the test database")
    }

    /// Drops the test database and closes the connection to it.
    ///
    /// # Panics
    ///
    /// Panics if the database can't be dropped or the connection can't be closed.
    pub async fn drop_database(mut self) {
        sqlx::query(&format!("DROP DATABASE `{}`", self.name))
            .execute(&mut self.conn)
            .await
            .expect("failed to drop the test database");
        self.conn
            .close()
            .await
            .expect("failed to close the test database connection");
    }
}
