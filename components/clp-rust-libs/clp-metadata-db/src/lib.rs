//! The interface of the clp-s metadata tables that every dataset shares.
//!
//! A [`ClpDbClient`] wraps a component's connection pool and hands out [`ClpDbConnection`]s for
//! reads and [`ClpDbTransaction`]s for writes. Both implement the operation traits, grouped by
//! entity, which take no `sqlx` types, so consumers can be tested against mocks of the traits they
//! use.

mod client;
mod error;
mod operations;
mod schema;
mod stored;
mod types;

pub use client::ClpDbClient;
pub use client::ClpDbConnection;
pub use client::ClpDbTransaction;
pub use error::Error;
pub use operations::DatasetLookup;
pub use operations::DatasetManagement;
pub use types::Dataset;
pub use types::DatasetConfig;
pub use types::DatasetDescriptor;
