//! The metadata database client and the connection and transaction handles it hands out.

use clp_rust_utils::clp_config::package::config::Database as DatabaseConfig;
use clp_rust_utils::clp_config::package::config::MetadataTables;
use sqlx::MySql;
use sqlx::MySqlConnection;
use sqlx::MySqlPool;
use sqlx::pool::PoolConnection;

use crate::error::Error;
use crate::schema::create_table_statements;

/// A client of the CLP metadata database. It holds the component's connection pool, and hands out
/// connection and transaction handles that implement the operation traits.
pub struct ClpDbClient {
    pool: MySqlPool,
    tables: MetadataTables,
}

impl ClpDbClient {
    /// Factory function.
    ///
    /// Wraps the component's existing pool. Doesn't create or check any table.
    ///
    /// # Returns
    ///
    /// A client that runs its operations on `pool`.
    #[must_use]
    pub fn new(pool: MySqlPool, config: &DatabaseConfig) -> Self {
        Self {
            pool,
            tables: config.metadata_tables(),
        }
    }

    /// Creates the shared metadata tables that don't exist yet. Only the table creator calls it.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    pub async fn create_tables(&self) -> Result<(), Error> {
        for statement in create_table_statements(&self.tables) {
            sqlx::query(&statement).execute(&self.pool).await?;
        }
        Ok(())
    }

    /// # Returns
    ///
    /// A connection from the pool on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::Pool::acquire`]'s return values on failure.
    pub async fn acquire(&self) -> Result<ClpDbConnection<'_>, Error> {
        Ok(ClpDbConnection {
            tables: &self.tables,
            connection: self.pool.acquire().await?,
        })
    }

    /// # Returns
    ///
    /// A newly started transaction on a connection from the pool on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::Pool::begin`]'s return values on failure.
    pub async fn begin(&self) -> Result<ClpDbTransaction<'_>, Error> {
        Ok(ClpDbTransaction {
            tables: &self.tables,
            transaction: self.pool.begin().await?,
        })
    }
}

/// A pooled connection, for reads. Each read runs on its own.
///
/// The write traits are implemented only for [`ClpDbTransaction`], so a write on a connection
/// doesn't compile:
///
/// ```compile_fail,E0599
/// use clp_metadata_db::ArchiveManagement;
/// use clp_metadata_db::ClpDbConnection;
///
/// async fn delete_archives(connection: &mut ClpDbConnection<'_>) {
///     let _ = connection.delete_archives(&[]).await;
/// }
/// ```
///
/// # Type Parameters
///
/// * `'client` - The lifetime of the client that handed out the connection.
pub struct ClpDbConnection<'client> {
    tables: &'client MetadataTables,
    connection: PoolConnection<MySql>,
}

impl Handle for ClpDbConnection<'_> {
    fn parts(&mut self) -> HandleParts<'_> {
        HandleParts {
            tables: self.tables,
            connection: &mut self.connection,
        }
    }
}

/// A transaction, whose operations aren't visible to others until [`Self::commit`].
///
/// The locks its operations take, and the guarantees that rely on them, last until it commits or
/// rolls back. Dropping the transaction rolls it back.
///
/// # Type Parameters
///
/// * `'client` - The lifetime of the client that started the transaction.
pub struct ClpDbTransaction<'client> {
    tables: &'client MetadataTables,
    transaction: sqlx::Transaction<'static, MySql>,
}

impl ClpDbTransaction<'_> {
    /// Commits the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
    pub async fn commit(self) -> Result<(), Error> {
        Ok(self.transaction.commit().await?)
    }

    /// Rolls the transaction back.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::Transaction::rollback`]'s return values on failure.
    pub async fn rollback(self) -> Result<(), Error> {
        Ok(self.transaction.rollback().await?)
    }

    /// # Returns
    ///
    /// The underlying `sqlx` transaction, for statements on tables outside this library (e.g. the
    /// job tables) that must commit or roll back together with the metadata operations. Take locks
    /// on rows outside this library before calling library operations in this transaction, so that
    /// every transaction locks job rows before dataset rows.
    pub const fn sqlx_transaction(&mut self) -> &mut sqlx::Transaction<'static, MySql> {
        &mut self.transaction
    }
}

impl Handle for ClpDbTransaction<'_> {
    fn parts(&mut self) -> HandleParts<'_> {
        HandleParts {
            tables: self.tables,
            connection: &mut self.transaction,
        }
    }
}

/// A handle's table names and connection, borrowed for one operation.
///
/// # Type Parameters
///
/// * `'handle` - The lifetime of the borrowed handle.
pub struct HandleParts<'handle> {
    pub tables: &'handle MetadataTables,
    pub connection: &'handle mut MySqlConnection,
}

/// A connection or transaction handle that the read-only operations run on.
///
/// The trait is public only so that the read-only operation traits can be implemented once for both
/// handles; it isn't reachable from outside this crate:
///
/// ```compile_fail,E0432
/// use clp_metadata_db::Handle;
/// ```
pub trait Handle: Send {
    /// # Returns
    ///
    /// The handle's table names and connection.
    fn parts(&mut self) -> HandleParts<'_>;
}
