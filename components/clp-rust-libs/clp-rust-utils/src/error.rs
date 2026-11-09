//! Error type for CLP Rust utilities.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("`rmp_serde::encode::Error`: {0}")]
    MsgpackEncode(#[from] rmp_serde::encode::Error),

    #[error("`rmp_serde::decode::Error`: {0}")]
    MsgpackDecode(#[from] rmp_serde::decode::Error),

    #[error("`std::io::Error`: {0}")]
    Io(#[from] std::io::Error),

    #[error("failed to read the `{name}` environment variable: {source}")]
    EnvVar {
        name: &'static str,
        source: std::env::VarError,
    },

    #[error("`zstd`: {0}")]
    Zstd(#[source] std::io::Error),

    #[error("`yaml_serde::Error`: {0}")]
    SerdeYaml(#[from] yaml_serde::Error),

    #[error("`sqlx::Error`: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("unsupported S3 endpoint: {0}")]
    UnsupportedS3Endpoint(String),

    #[error("`aws_sdk_s3::Error`: {}", aws_sdk_s3::error::DisplayErrorContext(.0))]
    S3(Box<aws_sdk_s3::Error>),

    #[error("failed to delete S3 objects: {0:?}")]
    S3ObjectsNotDeleted(Vec<aws_sdk_s3::types::Error>),

    #[error(transparent)]
    TelemetryExporterBuildError(#[from] opentelemetry_otlp::ExporterBuildError),
}
