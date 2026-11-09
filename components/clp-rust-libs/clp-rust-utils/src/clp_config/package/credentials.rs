use secrecy::SecretString;
use serde::Deserialize;

use crate::Error;

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
    /// * [`Error::EnvVar`] if either variable is unset or not valid Unicode.
    pub fn from_env() -> Result<Self, Error> {
        const CLP_DB_USER_ENV_VAR: &str = "CLP_DB_USER";
        const CLP_DB_PASS_ENV_VAR: &str = "CLP_DB_PASS";

        let read_env_var = |name: &'static str| {
            std::env::var(name).map_err(|source| Error::EnvVar { name, source })
        };
        let user = read_env_var(CLP_DB_USER_ENV_VAR)?;
        let password = read_env_var(CLP_DB_PASS_ENV_VAR)?;
        Ok(Self {
            password: SecretString::from(password),
            user,
        })
    }
}
