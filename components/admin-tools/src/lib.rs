//! Administration tools for the archives and datasets of a CLP package that uses the clp-s storage
//! engine.

pub mod archive;
pub mod dataset;

use clp_rust_utils::clp_config::package::credentials;

/// The exit code of a failed run, the same as the Python admin tools' exit code of `-1`.
pub const FAILURE_EXIT_CODE: u8 = u8::MAX;

/// Reads the CLP DB credentials from the `CLP_DB_USER` and `CLP_DB_PASS` environment variables.
///
/// # Returns
///
/// The CLP DB credentials on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`std::env::var`]'s return values on failure.
pub fn read_db_credentials_from_env() -> Result<credentials::Database, std::env::VarError> {
    Ok(credentials::Database {
        password: secrecy::SecretString::new(
            std::env::var("CLP_DB_PASS")
                .inspect_err(|e| {
                    tracing::error!(
                        error = % e,
                        "Failed to read the database password from `CLP_DB_PASS`."
                    );
                })?
                .into_boxed_str(),
        ),
        user: std::env::var("CLP_DB_USER").inspect_err(|e| {
            tracing::error!(
                error = % e,
                "Failed to read the database user from `CLP_DB_USER`."
            );
        })?,
    })
}
