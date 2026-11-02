//! A buffer of candidates to delete from the archive storage, backed by a recovery file.

use std::collections::BTreeSet;
use std::io::ErrorKind;
use std::io::Write;
use std::path::PathBuf;

/// Mirror of `job_orchestration.garbage_collector.utils.DeletionCandidatesBuffer`.
#[derive(Debug)]
pub struct DeletionCandidatesBuffer {
    recovery_file_path: PathBuf,
    candidates: BTreeSet<String>,
    candidates_to_persist: Vec<String>,
}

impl DeletionCandidatesBuffer {
    /// Factory function.
    ///
    /// Creates a buffer of the non-empty candidates persisted in the recovery file at
    /// `recovery_file_path`, if it exists.
    ///
    /// # Returns
    ///
    /// A newly created buffer on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`std::fs::read_to_string`]'s return values on failure, other than the recovery
    ///   file not existing.
    pub fn new(recovery_file_path: PathBuf) -> std::io::Result<Self> {
        let candidates = match std::fs::read_to_string(&recovery_file_path) {
            Ok(content) => content
                .lines()
                .map(str::trim)
                .filter(|candidate| !candidate.is_empty())
                .map(str::to_owned)
                .collect(),
            Err(e) if ErrorKind::NotFound == e.kind() => BTreeSet::new(),
            Err(e) => return Err(e),
        };
        Ok(Self {
            recovery_file_path,
            candidates,
            candidates_to_persist: Vec::new(),
        })
    }

    /// Adds `candidate` to the buffer, to be persisted by [`Self::persist_new_candidates`] if it
    /// isn't in the buffer yet.
    pub fn add_candidate(&mut self, candidate: String) {
        if self.candidates.insert(candidate.clone()) {
            self.candidates_to_persist.push(candidate);
        }
    }

    /// # Returns
    ///
    /// The candidates in the buffer.
    #[must_use]
    pub const fn candidates(&self) -> &BTreeSet<String> {
        &self.candidates
    }

    /// Appends the candidates added since the last call to the recovery file.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`std::fs::OpenOptions::open`]'s return values on failure.
    /// * Forwards [`std::io::Write::write_all`]'s return values on failure.
    pub fn persist_new_candidates(&mut self) -> std::io::Result<()> {
        if self.candidates_to_persist.is_empty() {
            return Ok(());
        }

        let mut recovery_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.recovery_file_path)?;
        recovery_file
            .write_all(format!("{}\n", self.candidates_to_persist.join("\n")).as_bytes())?;

        self.candidates_to_persist.clear();
        Ok(())
    }

    /// Removes every candidate from the buffer and deletes the recovery file.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`std::fs::remove_file`]'s return values on failure, other than the recovery file
    ///   not existing.
    pub fn clear(&mut self) -> std::io::Result<()> {
        self.candidates.clear();
        match std::fs::remove_file(&self.recovery_file_path) {
            Err(e) if ErrorKind::NotFound != e.kind() => Err(e),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::DeletionCandidatesBuffer;

    #[test]
    fn new_buffer_loads_non_empty_candidates_from_recovery_file() -> anyhow::Result<()> {
        let tmp_directory = tempfile::tempdir()?;
        let recovery_file_path = tmp_directory.path().join("recovery.tmp");

        let buffer = DeletionCandidatesBuffer::new(recovery_file_path.clone())?;
        assert_eq!(buffer.candidates(), &BTreeSet::new());

        std::fs::write(&recovery_file_path, "a/1\n\n  \n b/2 \r\na/1\nc/3")?;
        let buffer = DeletionCandidatesBuffer::new(recovery_file_path)?;
        assert_eq!(
            buffer.candidates(),
            &BTreeSet::from(["a/1".to_owned(), "b/2".to_owned(), "c/3".to_owned()])
        );
        Ok(())
    }

    #[test]
    fn new_buffer_rejects_recovery_path_that_is_not_a_file() -> anyhow::Result<()> {
        let tmp_directory = tempfile::tempdir()?;

        DeletionCandidatesBuffer::new(tmp_directory.path().to_path_buf())
            .expect_err("a directory shouldn't be accepted as the recovery file");
        Ok(())
    }

    #[test]
    fn persist_new_candidates_appends_only_unpersisted_candidates() -> anyhow::Result<()> {
        let tmp_directory = tempfile::tempdir()?;
        let recovery_file_path = tmp_directory.path().join("recovery.tmp");
        std::fs::write(&recovery_file_path, "a/1\n")?;

        let mut buffer = DeletionCandidatesBuffer::new(recovery_file_path.clone())?;
        buffer.add_candidate("a/1".to_owned());
        buffer.add_candidate("b/2".to_owned());
        buffer.add_candidate("b/2".to_owned());
        buffer.persist_new_candidates()?;
        assert_eq!(std::fs::read_to_string(&recovery_file_path)?, "a/1\nb/2\n");

        buffer.persist_new_candidates()?;
        buffer.add_candidate("c/3".to_owned());
        buffer.persist_new_candidates()?;
        assert_eq!(
            std::fs::read_to_string(&recovery_file_path)?,
            "a/1\nb/2\nc/3\n"
        );
        assert_eq!(
            buffer.candidates(),
            &BTreeSet::from(["a/1".to_owned(), "b/2".to_owned(), "c/3".to_owned()])
        );
        Ok(())
    }

    #[test]
    fn clear_removes_candidates_and_recovery_file() -> anyhow::Result<()> {
        let tmp_directory = tempfile::tempdir()?;
        let recovery_file_path = tmp_directory.path().join("recovery.tmp");

        let mut buffer = DeletionCandidatesBuffer::new(recovery_file_path.clone())?;
        buffer.add_candidate("a/1".to_owned());
        buffer.persist_new_candidates()?;
        buffer.clear()?;
        assert_eq!(buffer.candidates(), &BTreeSet::new());
        assert!(!recovery_file_path.exists());

        buffer.clear()?;
        Ok(())
    }
}
