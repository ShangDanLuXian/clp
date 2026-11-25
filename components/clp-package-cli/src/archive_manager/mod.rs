//! The archive manager, which finds archives and deletes them from the metadata database and
//! storage.

mod commands;
mod selection;
mod storage;

pub use commands::SelectedArchive;
pub use commands::SelectedArchives;
pub use commands::delete_archives;
pub use commands::find_archives;
pub use commands::preview_deletion;
pub use selection::ArchiveReference;
pub use selection::ArchiveSelection;
pub use selection::SelectionError;
pub use storage::ArchiveStorage;
