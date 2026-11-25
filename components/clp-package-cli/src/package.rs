//! What every tool needs from the CLP package: its configuration, its database credentials, a
//! metadata database client, console logging and the exit code of a failed command.

use std::io::IsTerminal;
use std::path::Path;

use anyhow::Context;
use clp_metadata_db::ClpDbClient;
use clp_rust_utils::clp_config::package::config::Config;
use clp_rust_utils::clp_config::package::config::StorageEngine;
use clp_rust_utils::clp_config::package::credentials::Database as DatabaseCredentials;
use clp_rust_utils::database::mysql::create_clp_db_mysql_pool;
use clp_rust_utils::serde::yaml;
use secrecy::SecretString;

/// The exit code of a failed command. Mirror of the Python tools' `return -1`, which exits with
/// 255.
pub const FAILURE_EXIT_CODE: u8 = 255;

/// Loads the CLP package configuration of a clp-s package.
///
/// # Returns
///
/// The configuration on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if the package's storage engine isn't clp-s.
/// * Forwards [`yaml::from_path`]'s return values on failure.
pub fn load_config(path: &Path) -> anyhow::Result<Config> {
    let config: Config = yaml::from_path(path)
        .with_context(|| format!("failed to load the configuration file {}", path.display()))?;
    match config.package.storage_engine {
        StorageEngine::ClpS => Ok(config),
        StorageEngine::Clp => {
            anyhow::bail!("the package's storage engine is `clp`, but only `clp-s` is supported")
        }
    }
}

/// Reads the database credentials from `CLP_DB_USER` and `CLP_DB_PASS`, as the other package
/// components do.
///
/// # Returns
///
/// The credentials on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`std::env::var`]'s return values on failure.
pub fn database_credentials_from_env() -> anyhow::Result<DatabaseCredentials> {
    let user = std::env::var("CLP_DB_USER").context("failed to read `CLP_DB_USER`")?;
    let password = std::env::var("CLP_DB_PASS").context("failed to read `CLP_DB_PASS`")?;
    Ok(DatabaseCredentials {
        password: SecretString::from(password),
        user,
    })
}

/// Connects to the metadata database that `config` names.
///
/// # Returns
///
/// A client of the metadata database on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`create_clp_db_mysql_pool`]'s return values on failure.
pub async fn connect(
    config: &Config,
    credentials: &DatabaseCredentials,
) -> anyhow::Result<ClpDbClient> {
    // A command runs its operations one after another, so it never needs a second connection.
    const MAX_CONNECTIONS: u32 = 1;

    let pool = create_clp_db_mysql_pool(&config.database, credentials, MAX_CONNECTIONS)
        .await
        .context("failed to connect to the metadata database")?;
    Ok(ClpDbClient::new(pool, &config.database))
}

/// Sets up human-readable logging to stderr, at the debug level if `verbose` and the info level
/// otherwise. The output is colored only on a terminal.
pub fn set_up_logging(verbose: bool) {
    let max_level = if verbose {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .with_target(false)
        .with_max_level(max_level)
        .init();
}

#[cfg(test)]
mod tests {
    use clp_rust_utils::clp_config::package::config::StorageEngine;

    use super::load_config;

    #[test]
    fn load_config_accepts_only_a_clp_s_package() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("clp-config.yaml");

        std::fs::write(&path, "package:\n  storage_engine: \"clp-s\"\n")?;
        assert_eq!(
            load_config(&path)?.package.storage_engine,
            StorageEngine::ClpS
        );

        std::fs::write(&path, "package:\n  storage_engine: \"clp\"\n")?;
        let error = load_config(&path).expect_err("a clp package should be rejected");
        assert_eq!(
            error.to_string(),
            "the package's storage engine is `clp`, but only `clp-s` is supported"
        );
        Ok(())
    }
}
