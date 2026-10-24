//! Error types for the query result listener.

use clp_rust_utils::types::ArchiveId;

/// Violations of the wire protocol, each of which makes the listener close the connection without
/// acknowledging it.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProtocolError {
    /// Detected by checking a handshake against the task's earlier connections, which
    /// [`crate::protocol::ConnectionDecoder`] doesn't see.
    #[error(
        "task {task_index} streams archive {received}, but its earlier attempts streamed archive \
         {expected}"
    )]
    ArchiveIdMismatch {
        task_index: u64,
        expected: ArchiveId,
        received: ArchiveId,
    },

    #[error("bytes followed the trailer")]
    DataAfterTrailer,

    #[error("malformed message: {0}")]
    MalformedMessage(String),

    #[error(
        "the trailer counts {trailer_num_results} results, but the connection sent {num_results}"
    )]
    ResultCountMismatch {
        trailer_num_results: u64,
        num_results: u64,
    },

    #[error("expected result index {expected}, got {received}")]
    ResultIndexOutOfOrder { expected: u64, received: u64 },

    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u64),
}

#[cfg(test)]
mod tests {
    use clp_rust_utils::types::ArchiveId;

    use super::ProtocolError;

    #[test]
    fn archive_id_mismatch_names_the_task_and_both_archives() {
        const EXPECTED: &str = "018e90e5-8b2a-4a61-a2fc-cac799936caf";
        const RECEIVED: &str = "4b0f7f4e-3c2d-4a8e-9a51-0d6c1e2b7f90";
        let error = ProtocolError::ArchiveIdMismatch {
            task_index: 3,
            expected: EXPECTED.parse::<ArchiveId>().expect("valid UUID"),
            received: RECEIVED.parse::<ArchiveId>().expect("valid UUID"),
        };
        assert_eq!(
            error.to_string(),
            format!(
                "task 3 streams archive {RECEIVED}, but its earlier attempts streamed archive \
                 {EXPECTED}"
            )
        );
    }
}
