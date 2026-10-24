//! Receives the results that `clp-s` query tasks stream over TCP.
//!
//! # Wire protocol
//!
//! Each connection carries the results of one attempt of one query task, as a sequence of msgpack
//! arrays with no other framing:
//!
//! 1. The handshake, `[protocol_version, session_token, task_index, archive_id]`, once. The version
//!    is [`protocol::PROTOCOL_VERSION`]. The session token, a hyphenated UUID string, names the
//!    session that receives the results. The task index identifies the task within its query job,
//!    and the archive ID is the UUID string of the archive the task searches.
//! 2. One result, `[result_index, timestamp, message]`, per result. The index counts the
//!    connection's results from 0, the timestamp is in milliseconds since the Unix epoch, and the
//!    message is a UTF-8 string.
//! 3. The trailer, `[num_results]`, once, counting the results.
//!
//! Each value may use any msgpack encoding that holds it. After the trailer, `clp-s` shuts down its
//! side of the connection and waits for the listener's acknowledgement, `[num_results]` with the
//! count always encoded as a `uint 64`: the bytes `0x91 0xcf`, then the count in big-endian order.
//! `clp-s` fails the task unless the acknowledgement counts the results it sent, so Spider reruns
//! it. Every attempt opens its own connection, which starts again from result 0.
//!
//! [`protocol::ConnectionDecoder`] decodes the messages of a connection and checks their order, and
//! [`protocol::Acknowledgement`] encodes the reply.

mod error;
pub mod protocol;

pub use error::ProtocolError;
