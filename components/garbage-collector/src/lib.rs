//! Garbage collectors of a CLP package that uses the clp-s storage engine.

mod archive;
mod error;

pub use archive::ArchiveGarbageCollector;
pub use error::Error;
