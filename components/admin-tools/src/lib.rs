//! Administration tools for the archives and datasets of a CLP package that uses the clp-s storage
//! engine.

pub mod archive;
pub mod dataset;

/// The exit code of a failed run, the same as the Python admin tools' exit code of `-1`.
pub const FAILURE_EXIT_CODE: u8 = u8::MAX;
