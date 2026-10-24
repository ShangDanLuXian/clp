use std::fmt::Display;
use std::fmt::Formatter;
use std::num::NonZeroU16;
use std::num::NonZeroU32;

use non_empty_string::NonEmptyString;
use num_enum::IntoPrimitive;
use num_enum::TryFromPrimitive;
use serde::Deserialize;
use serde::Serialize;
use strum::EnumString;
use utoipa::ToSchema;

use crate::dataset::DatasetName;

pub const QUERY_JOBS_TABLE_NAME: &str = "query_jobs";

pub type QueryJobId = i32;

/// Mirror of `job_orchestration.scheduler.job_config.AggregationConfig`. Must be kept in sync.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct AggregationConfig {
    pub job_id: Option<i64>,
    pub reducer_host: Option<String>,
    pub reducer_port: Option<u16>,
    pub do_count_aggregation: Option<bool>,
    /// Milliseconds
    pub count_by_time_bucket_size: Option<i64>,
}

/// The token that identifies a session to the query tasks that send their results to it.
///
/// A session token is a UUID. It serializes as a hyphenated string in every format, and
/// [`Display`] formats it the same way, since that's the form `clp-s` receives on its command line
/// (`--session-token`) and sends back in its handshake.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SessionToken {
    #[serde(with = "uuid::serde::hyphenated")]
    value: uuid::Uuid,
}

impl From<uuid::Uuid> for SessionToken {
    fn from(value: uuid::Uuid) -> Self {
        Self { value }
    }
}

impl Display for SessionToken {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(formatter)
    }
}

/// The network output of a query job: the listener that the job's query tasks send its results to,
/// and the listener's session that receives them.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NetworkOutput {
    /// The listener's host name or IP address, as the query tasks reach it.
    pub host: NonEmptyString,

    /// The listener's TCP port.
    pub port: NonZeroU16,

    /// The token of the session that receives the job's results.
    pub session_token: SessionToken,
}

/// Mirror of `job_orchestration.scheduler.job_config.SearchJobConfig`. Must be kept in sync, except
/// for `network_output`, which replaces the Python config's `network_address`.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct SearchJobConfig {
    pub datasets: Option<Vec<DatasetName>>,
    pub query_string: String,
    pub max_num_results: Option<NonZeroU32>,
    pub begin_timestamp: Option<i64>,
    pub end_timestamp: Option<i64>,
    pub ignore_case: bool,
    pub path_filter: Option<String>,

    /// The network output to send the job's results to, instead of the results cache.
    ///
    /// The Python config has `network_address` instead, which only the Python search path sets and
    /// no Rust code reads. A config that sets `network_address` still deserializes, without it.
    pub network_output: Option<NetworkOutput>,

    pub aggregation_config: Option<AggregationConfig>,
    pub write_to_file: bool,
}

/// Mirror of `job_orchestration.scheduler.constants.QueryJobStatus`. Must be kept in sync.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    EnumString,
    Eq,
    IntoPrimitive,
    PartialEq,
    Serialize,
    ToSchema,
    TryFromPrimitive,
    sqlx::Type,
)]
#[repr(i32)]
#[strum(ascii_case_insensitive)]
pub enum QueryJobStatus {
    Pending = 0,
    Running = 1,
    Succeeded = 2,
    Failed = 3,
    CancelRequested = 4,
    Cancelling = 5,
    Cancelled = 6,
    Killed = 7,
}

impl QueryJobStatus {
    /// # Returns
    ///
    /// Whether the status is terminal, i.e., whether the job has finished and its status will no
    /// longer change.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Killed
        )
    }
}

/// Mirror of `job_orchestration.scheduler.constants.QueryJobType`. Must be kept in sync.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    IntoPrimitive,
    PartialEq,
    Serialize,
    TryFromPrimitive,
    sqlx::Type,
)]
#[repr(i32)]
pub enum QueryJobType {
    SearchOrAggregation = 0,
    ExtractIr = 1,
    ExtractJson = 2,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use non_empty_string::NonEmptyString;

    use super::DatasetName;
    use super::NetworkOutput;
    use super::SearchJobConfig;
    use super::SessionToken;
    use crate::types::non_empty_string::ExpectedNonEmpty;

    const SESSION_TOKEN: &str = "6f1d3b52-8a4e-4c1b-9f6e-2d7a5c0b9e13";

    /// # Returns
    ///
    /// A network output with the session token [`SESSION_TOKEN`].
    fn network_output() -> NetworkOutput {
        NetworkOutput {
            host: NonEmptyString::from_static_str("10.0.0.7"),
            port: NonZeroU16::new(40_123).expect("40,123 is nonzero"),
            session_token: SessionToken::from(
                uuid::Uuid::parse_str(SESSION_TOKEN).expect("valid session token UUID"),
            ),
        }
    }

    #[test]
    fn search_job_config_round_trips_through_named_msgpack() {
        let without_network_output = SearchJobConfig {
            datasets: Some(vec![DatasetName::default()]),
            query_string: "*Transmitted*".to_owned(),
            ..SearchJobConfig::default()
        };
        let with_network_output = SearchJobConfig {
            network_output: Some(network_output()),
            ..without_network_output.clone()
        };
        for expected in [without_network_output, with_network_output] {
            let serialized =
                rmp_serde::to_vec_named(&expected).expect("search job config should serialize");
            let actual: SearchJobConfig =
                rmp_serde::from_slice(&serialized).expect("search job config should deserialize");
            assert_eq!(expected, actual);
        }
    }

    #[test]
    fn network_output_session_token_serializes_as_hyphenated_string() {
        let config = SearchJobConfig {
            query_string: "*".to_owned(),
            network_output: Some(network_output()),
            ..SearchJobConfig::default()
        };
        let serialized =
            rmp_serde::to_vec_named(&config).expect("search job config should serialize");
        let stored: serde_json::Value =
            rmp_serde::from_slice(&serialized).expect("msgpack should deserialize as a value");
        assert_eq!(
            stored["network_output"],
            serde_json::json!({
                "host": "10.0.0.7",
                "port": 40_123,
                "session_token": SESSION_TOKEN,
            })
        );

        let session_token = network_output().session_token;
        assert_eq!(session_token.to_string(), SESSION_TOKEN);
        assert_eq!(
            rmp_serde::to_vec(&session_token).expect("session token should serialize"),
            rmp_serde::to_vec(SESSION_TOKEN).expect("string should serialize")
        );
        assert_eq!(
            serde_json::to_value(session_token).expect("session token should serialize"),
            serde_json::json!(SESSION_TOKEN)
        );
    }

    #[test]
    fn network_output_rejects_invalid_session_tokens() {
        for session_token in ["", "not-a-uuid", "6f1d3b528a4e4c1b9f6e2d7a5c0b9e13"] {
            let persisted = serde_json::json!({
                "host": "10.0.0.7",
                "port": 40_123,
                "session_token": session_token,
            });
            let serialized = rmp_serde::to_vec_named(&persisted).expect("value should serialize");
            assert!(rmp_serde::from_slice::<NetworkOutput>(&serialized).is_err());
        }
    }

    #[test]
    fn search_job_config_with_network_address_deserializes_without_it() {
        let persisted = serde_json::json!({
            "query_string": "*",
            "network_address": ["localhost", 4_000],
        });
        let serialized = rmp_serde::to_vec_named(&persisted).expect("value should serialize");
        let actual: SearchJobConfig =
            rmp_serde::from_slice(&serialized).expect("search job config should deserialize");
        assert_eq!(
            actual,
            SearchJobConfig {
                query_string: "*".to_owned(),
                ..SearchJobConfig::default()
            }
        );
    }
}
