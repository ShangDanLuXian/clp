//! Error type for CLP Rust utilities.

use thiserror::Error;

use crate::job_config::QueryJobStatus;

#[derive(Error, Debug)]
pub enum Error {
    #[error("`rmp_serde::encode::Error`: {0}")]
    MsgpackEncode(#[from] rmp_serde::encode::Error),

    #[error("`rmp_serde::decode::Error`: {0}")]
    MsgpackDecode(#[from] rmp_serde::decode::Error),

    #[error("`std::io::Error`: {0}")]
    Io(#[from] std::io::Error),

    #[error("`zstd`: {0}")]
    Zstd(#[source] std::io::Error),

    #[error("`yaml_serde::Error`: {0}")]
    SerdeYaml(#[from] yaml_serde::Error),

    #[error("`sqlx::Error`: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("query job not found: {0}")]
    QueryJobNotFound(u64),

    #[error("query job already terminated: {0}")]
    QueryJobAlreadyTerminated(u64),

    #[error("invalid query job status: {0}")]
    InvalidQueryJobStatus(#[from] num_enum::TryFromPrimitiveError<QueryJobStatus>),

    #[error("unsupported S3 endpoint: {0}")]
    UnsupportedS3Endpoint(String),

    #[error(transparent)]
    TelemetryExporterBuildError(#[from] opentelemetry_otlp::ExporterBuildError),
}
