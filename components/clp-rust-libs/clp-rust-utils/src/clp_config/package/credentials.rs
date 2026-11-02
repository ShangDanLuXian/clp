use secrecy::SecretString;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Credentials {
    pub database: Database,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Database {
    pub password: SecretString,
    pub user: String,
}

impl Database {
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
    pub fn from_env() -> Result<Self, std::env::VarError> {
        Ok(Self {
            password: SecretString::new(
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
}
