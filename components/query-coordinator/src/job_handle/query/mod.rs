//! Planning and execution of search queries.

mod archive_selector;
mod handle;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "wired up by the shared metadata tables switch")
)]
mod shared_tables_archive_selector;

pub use handle::QueryJobHandle;
