//! The messages of the wire protocol, and the decoder that checks their order on a connection.
//!
//! See the [crate documentation](crate) for the protocol.

use bytes::Buf;
use bytes::BytesMut;
use clp_rust_utils::types::ArchiveId;
use rmp::decode::Bytes;
use rmp::decode::NumValueReadError;
use rmp::decode::ValueReadError;
use rmp::decode::bytes::BytesReadError;

use crate::ProtocolError;

/// The protocol version that every handshake must carry.
pub const PROTOCOL_VERSION: u64 = 1;

/// The first message of a connection, which identifies the task whose results follow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Handshake {
    /// The token of the session that receives the task's results.
    pub session_token: uuid::Uuid,

    /// The index of the task within its query job.
    pub task_index: u64,

    /// The ID of the archive that the task searches.
    pub archive_id: ArchiveId,
}

/// One result of a task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResultMessage {
    /// The number of results that the connection sent before this one.
    pub result_index: u64,

    /// The timestamp of the result's log event, in milliseconds since the Unix epoch.
    pub timestamp: i64,

    /// The result's log event.
    pub message: String,
}

/// The last message of a connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Trailer {
    /// The number of results that the connection sent.
    pub num_results: u64,
}

/// A message that a task sends on its connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Message {
    Handshake(Handshake),
    Result(ResultMessage),
    Trailer(Trailer),
}

/// The listener's reply to the trailer of a connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Acknowledgement {
    /// The number of results acknowledged, which `clp-s` checks against the number it sent.
    pub num_results: u64,
}

impl Acknowledgement {
    /// The length of every encoded acknowledgement, in bytes.
    pub const ENCODED_LEN: usize = 10;

    /// # Returns
    ///
    /// The msgpack array `[num_results]`, with the count always encoded as a `uint 64`.
    #[must_use]
    pub fn encode(self) -> [u8; Self::ENCODED_LEN] {
        let mut encoded = [0; Self::ENCODED_LEN];
        encoded[0] = FIXARRAY_OF_ONE_MARKER;
        encoded[1] = UINT64_MARKER;
        encoded[2..].copy_from_slice(&self.num_results.to_be_bytes());
        encoded
    }
}

/// Decodes the messages of one connection, and checks that they come in the protocol's order.
///
/// The order is one handshake, then results with indices from 0 in order, then one trailer that
/// counts them, and nothing after the trailer.
///
/// A message may be split across socket reads at any byte. The decoder decodes a message only once
/// the buffer holds all of it, so the caller can append each read to the buffer and decode again.
#[derive(Debug, Default)]
pub struct ConnectionDecoder {
    state: State,
}

impl ConnectionDecoder {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A decoder that expects the handshake of a new connection.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: State::AwaitingHandshake,
        }
    }

    /// Decodes the next message of the connection from the front of `buffer`, consuming its bytes.
    /// `buffer` may hold several messages, so the caller should decode until this returns `None`.
    ///
    /// Each check runs as soon as `buffer` holds the values it needs, so a message can be rejected
    /// before all of it arrives. On error, neither `buffer` nor the decoder changes.
    ///
    /// # Returns
    ///
    /// On success:
    ///
    /// * The decoded message.
    /// * `None`, leaving `buffer` untouched, if `buffer` doesn't hold all of the next message yet,
    ///   or if it's empty after the trailer.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ProtocolError::UnsupportedVersion`] if the handshake's version isn't
    ///   [`PROTOCOL_VERSION`]. The version is checked before the handshake's other fields.
    /// * [`ProtocolError::MalformedMessage`] if the bytes don't form the message expected next: the
    ///   handshake first, then a result or the trailer. That includes a value outside its field's
    ///   range, a session token that isn't a hyphenated UUID, an archive ID that isn't a UUID, and
    ///   a handshake string longer than 64 bytes.
    /// * [`ProtocolError::ResultIndexOutOfOrder`] if a result's index isn't the number of results
    ///   before it.
    /// * [`ProtocolError::ResultCountMismatch`] if the trailer doesn't count the results before it.
    /// * [`ProtocolError::DataAfterTrailer`] if `buffer` isn't empty after the trailer.
    pub fn decode(&mut self, buffer: &mut BytesMut) -> Result<Option<Message>, ProtocolError> {
        let mut reader = Bytes::new(buffer);
        let decoded = match self.state {
            State::AwaitingHandshake => decode_handshake(&mut reader).map(|handshake| {
                (
                    Message::Handshake(handshake),
                    State::AwaitingResultOrTrailer { num_results: 0 },
                )
            }),
            State::AwaitingResultOrTrailer { num_results } => {
                decode_result_or_trailer(&mut reader, num_results)
            }
            State::Finished => {
                return if buffer.is_empty() {
                    Ok(None)
                } else {
                    Err(ProtocolError::DataAfterTrailer)
                };
            }
        };
        match decoded {
            Ok((message, next_state)) => {
                let message_len = buffer.len() - reader.remaining_slice().len();
                buffer.advance(message_len);
                self.state = next_state;
                Ok(Some(message))
            }
            Err(DecodeError::Incomplete) => Ok(None),
            Err(DecodeError::Invalid(error)) => Err(error),
        }
    }

    /// # Returns
    ///
    /// Whether the decoder has decoded the connection's trailer.
    #[must_use]
    pub const fn is_finished(&self) -> bool {
        matches!(self.state, State::Finished)
    }
}

/// Where a connection is in the protocol.
#[derive(Clone, Copy, Debug, Default)]
enum State {
    /// Before the handshake.
    #[default]
    AwaitingHandshake,

    /// After the handshake and `num_results` results.
    AwaitingResultOrTrailer { num_results: u64 },

    /// After the trailer.
    Finished,
}

/// Why a message couldn't be decoded from the front of a buffer.
enum DecodeError {
    /// The buffer ends before the message does.
    Incomplete,

    /// The message violates the protocol.
    Invalid(ProtocolError),
}

impl From<ProtocolError> for DecodeError {
    fn from(error: ProtocolError) -> Self {
        Self::Invalid(error)
    }
}

const NUM_HANDSHAKE_FIELDS: u32 = 4;
const NUM_RESULT_FIELDS: u32 = 3;
const NUM_TRAILER_FIELDS: u32 = 1;

/// The longest string a handshake may carry, in bytes. Every textual form of a UUID fits, and the
/// limit keeps a peer from making the listener buffer a long handshake before its session token has
/// been checked.
const MAX_HANDSHAKE_STR_LEN: usize = 64;

/// The msgpack marker of an array of one element.
const FIXARRAY_OF_ONE_MARKER: u8 = 0x91;

/// The msgpack marker of a `uint 64`.
const UINT64_MARKER: u8 = 0xcf;

/// Decodes a handshake.
///
/// # Returns
///
/// The decoded handshake on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`DecodeError::Incomplete`] if `reader` ends before the handshake does.
/// * [`DecodeError::Invalid`] with [`ProtocolError::UnsupportedVersion`] if the version isn't
///   [`PROTOCOL_VERSION`].
/// * [`DecodeError::Invalid`] with [`ProtocolError::MalformedMessage`] if the bytes don't form a
///   valid handshake.
fn decode_handshake(reader: &mut Bytes<'_>) -> Result<Handshake, DecodeError> {
    let num_fields = read_array_len(reader, "the handshake")?;
    if num_fields == 0 {
        return Err(malformed("the handshake has no fields".to_owned()));
    }
    let protocol_version =
        rmp::decode::read_int(reader).map_err(|error| int_error(&error, "protocol_version"))?;
    if protocol_version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion(protocol_version).into());
    }
    if num_fields != NUM_HANDSHAKE_FIELDS {
        return Err(malformed(format!(
            "the handshake has {num_fields} fields instead of {NUM_HANDSHAKE_FIELDS}"
        )));
    }

    let session_token = read_str(reader, "session_token", MAX_HANDSHAKE_STR_LEN)?;
    let session_token = session_token
        .parse::<uuid::fmt::Hyphenated>()
        .map_err(|error| {
            malformed(format!(
                "`session_token` {session_token:?} isn't a hyphenated UUID: {error}"
            ))
        })?
        .into_uuid();
    let task_index =
        rmp::decode::read_int(reader).map_err(|error| int_error(&error, "task_index"))?;
    let archive_id = read_str(reader, "archive_id", MAX_HANDSHAKE_STR_LEN)?
        .parse::<ArchiveId>()
        .map_err(|error| malformed(error.to_string()))?;
    Ok(Handshake {
        session_token,
        task_index,
        archive_id,
    })
}

/// Decodes the message after `num_results` results, which the array's number of fields tells to
/// be a result or the trailer.
///
/// # Returns
///
/// The decoded message and the connection's state after it on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`DecodeError::Incomplete`] if `reader` ends before the message does.
/// * [`DecodeError::Invalid`] with [`ProtocolError::ResultIndexOutOfOrder`] if a result's index
///   isn't `num_results`.
/// * [`DecodeError::Invalid`] with [`ProtocolError::ResultCountMismatch`] if the trailer doesn't
///   count `num_results` results.
/// * [`DecodeError::Invalid`] with [`ProtocolError::MalformedMessage`] if the bytes don't form a
///   valid result or trailer.
fn decode_result_or_trailer(
    reader: &mut Bytes<'_>,
    num_results: u64,
) -> Result<(Message, State), DecodeError> {
    match read_array_len(reader, "the message")? {
        NUM_RESULT_FIELDS => {
            let result_index =
                rmp::decode::read_int(reader).map_err(|error| int_error(&error, "result_index"))?;
            if result_index != num_results {
                return Err(ProtocolError::ResultIndexOutOfOrder {
                    expected: num_results,
                    received: result_index,
                }
                .into());
            }
            let num_results = num_results.checked_add(1).ok_or_else(|| {
                malformed("the connection sends more results than a trailer can count".to_owned())
            })?;
            let timestamp =
                rmp::decode::read_int(reader).map_err(|error| int_error(&error, "timestamp"))?;
            let message = read_str(reader, "message", usize::MAX)?.to_owned();
            Ok((
                Message::Result(ResultMessage {
                    result_index,
                    timestamp,
                    message,
                }),
                State::AwaitingResultOrTrailer { num_results },
            ))
        }
        NUM_TRAILER_FIELDS => {
            let trailer_num_results =
                rmp::decode::read_int(reader).map_err(|error| int_error(&error, "num_results"))?;
            if trailer_num_results != num_results {
                return Err(ProtocolError::ResultCountMismatch {
                    trailer_num_results,
                    num_results,
                }
                .into());
            }
            Ok((Message::Trailer(Trailer { num_results }), State::Finished))
        }
        num_fields => Err(malformed(format!(
            "the message has {num_fields} fields, but a result has {NUM_RESULT_FIELDS} and the \
             trailer {NUM_TRAILER_FIELDS}"
        ))),
    }
}

/// Reads the header of the msgpack array that holds a message.
///
/// # Returns
///
/// The number of the array's elements on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`DecodeError::Incomplete`] if `reader` ends before the header does.
/// * [`DecodeError::Invalid`] if the next value isn't an array.
fn read_array_len(reader: &mut Bytes<'_>, message: &str) -> Result<u32, DecodeError> {
    rmp::decode::read_array_len(reader).map_err(|error| match error {
        ValueReadError::InvalidMarkerRead(_) | ValueReadError::InvalidDataRead(_) => {
            DecodeError::Incomplete
        }
        ValueReadError::TypeMismatch(marker) => malformed(format!(
            "expected {message} to be an array, got msgpack marker {marker:?}"
        )),
    })
}

/// Reads the msgpack string `field`, of at most `max_len` bytes. The length is checked before the
/// string's bytes are read, so a string that's too long is rejected before it arrives.
///
/// # Returns
///
/// The string, borrowed from the reader's buffer, on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`DecodeError::Incomplete`] if `reader` ends before the string does.
/// * [`DecodeError::Invalid`] if:
///   * The next value isn't a string.
///   * The string is longer than `max_len` bytes.
///   * The string isn't valid UTF-8.
fn read_str<'buffer>(
    reader: &mut Bytes<'buffer>,
    field: &str,
    max_len: usize,
) -> Result<&'buffer str, DecodeError> {
    let len = rmp::decode::read_str_len(reader).map_err(|error| match error {
        ValueReadError::InvalidMarkerRead(_) | ValueReadError::InvalidDataRead(_) => {
            DecodeError::Incomplete
        }
        ValueReadError::TypeMismatch(marker) => malformed(format!(
            "expected `{field}` to be a string, got msgpack marker {marker:?}"
        )),
    })?;
    let len = usize::try_from(len)
        .ok()
        .filter(|len| *len <= max_len)
        .ok_or_else(|| {
            malformed(format!(
                "`{field}` is {len} bytes long, more than the limit of {max_len}"
            ))
        })?;
    let Some((bytes, remaining)) = reader.remaining_slice().split_at_checked(len) else {
        return Err(DecodeError::Incomplete);
    };
    *reader = Bytes::new(remaining);
    std::str::from_utf8(bytes)
        .map_err(|error| malformed(format!("`{field}` isn't valid UTF-8: {error}")))
}

/// # Returns
///
/// The [`DecodeError`] for an error reading the integer `field`.
fn int_error(error: &NumValueReadError<BytesReadError>, field: &str) -> DecodeError {
    match error {
        NumValueReadError::InvalidMarkerRead(_) | NumValueReadError::InvalidDataRead(_) => {
            DecodeError::Incomplete
        }
        NumValueReadError::TypeMismatch(marker) => malformed(format!(
            "expected `{field}` to be an integer, got msgpack marker {marker:?}"
        )),
        NumValueReadError::OutOfRange => malformed(format!("`{field}` is out of range")),
    }
}

/// # Returns
///
/// The [`DecodeError`] for a malformed message, with `description` saying what's wrong.
const fn malformed(description: String) -> DecodeError {
    DecodeError::Invalid(ProtocolError::MalformedMessage(description))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SESSION_TOKEN: &str = "6f1d3b52-8a4e-4c1b-9f6e-2d7a5c0b9e13";
    const TASK_INDEX: u64 = 7;
    const ARCHIVE_ID: &str = "018e90e5-8b2a-4a61-a2fc-cac799936caf";
    const WRITE: &str = "writing to a `Vec` shouldn't fail";

    /// # Returns
    ///
    /// A handshake for task [`TASK_INDEX`] with the given strings, encoded the way `clp-s` encodes
    /// it.
    fn encode_handshake(session_token: &str, archive_id: &str) -> Vec<u8> {
        let mut bytes = Vec::new();
        rmp::encode::write_array_len(&mut bytes, 4).expect(WRITE);
        rmp::encode::write_uint(&mut bytes, PROTOCOL_VERSION).expect(WRITE);
        rmp::encode::write_str(&mut bytes, session_token).expect(WRITE);
        rmp::encode::write_uint(&mut bytes, TASK_INDEX).expect(WRITE);
        rmp::encode::write_str(&mut bytes, archive_id).expect(WRITE);
        bytes
    }

    /// # Returns
    ///
    /// A result, encoded the way `clp-s` encodes it.
    fn encode_result(result_index: u64, timestamp: i64, message: &str) -> Vec<u8> {
        let mut bytes = Vec::new();
        rmp::encode::write_array_len(&mut bytes, 3).expect(WRITE);
        rmp::encode::write_uint(&mut bytes, result_index).expect(WRITE);
        rmp::encode::write_sint(&mut bytes, timestamp).expect(WRITE);
        rmp::encode::write_str(&mut bytes, message).expect(WRITE);
        bytes
    }

    /// # Returns
    ///
    /// A trailer, encoded the way `clp-s` encodes it.
    fn encode_trailer(num_results: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        rmp::encode::write_array_len(&mut bytes, 1).expect(WRITE);
        rmp::encode::write_uint(&mut bytes, num_results).expect(WRITE);
        bytes
    }

    fn handshake_message() -> Message {
        Message::Handshake(Handshake {
            session_token: uuid::Uuid::parse_str(SESSION_TOKEN).expect("valid UUID"),
            task_index: TASK_INDEX,
            archive_id: ARCHIVE_ID.parse::<ArchiveId>().expect("valid UUID"),
        })
    }

    fn result_message(result_index: u64, timestamp: i64, message: &str) -> Message {
        Message::Result(ResultMessage {
            result_index,
            timestamp,
            message: message.to_owned(),
        })
    }

    /// Decodes every message that `buffer` holds in full.
    ///
    /// # Returns
    ///
    /// The decoded messages on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ConnectionDecoder::decode`]'s return values on failure.
    fn decode_all(
        decoder: &mut ConnectionDecoder,
        buffer: &mut BytesMut,
    ) -> Result<Vec<Message>, ProtocolError> {
        let mut messages = Vec::new();
        while let Some(message) = decoder.decode(buffer)? {
            messages.push(message);
        }
        Ok(messages)
    }

    /// # Returns
    ///
    /// The error from decoding `bytes` after the valid messages in `preceding`.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// * `preceding` doesn't hold only complete, valid messages.
    /// * Decoding `bytes` doesn't fail, or the failure changes the buffer or the decoder.
    fn decode_error(preceding: &[u8], bytes: &[u8]) -> ProtocolError {
        let mut decoder = ConnectionDecoder::new();
        let mut buffer = BytesMut::from(preceding);
        decode_all(&mut decoder, &mut buffer).expect("the preceding messages should be valid");
        assert!(
            buffer.is_empty(),
            "the preceding messages should be complete"
        );

        buffer.extend_from_slice(bytes);
        let error = decoder
            .decode(&mut buffer)
            .expect_err("decoding should fail");
        assert_eq!(
            &buffer[..],
            bytes,
            "a failed decode shouldn't consume bytes"
        );
        assert_eq!(decoder.decode(&mut buffer), Err(error.clone()));
        error
    }

    /// Asserts that decoding `bytes` after `preceding` reports a malformed message whose
    /// description contains `expected`.
    fn assert_malformed(preceding: &[u8], bytes: &[u8], expected: &str) {
        match decode_error(preceding, bytes) {
            ProtocolError::MalformedMessage(description) => assert!(
                description.contains(expected),
                "{description:?} should mention {expected:?}"
            ),
            error => panic!("expected a malformed message mentioning {expected:?}, got {error:?}"),
        }
    }

    #[test]
    fn decodes_a_connection_as_clp_s_encodes_it() {
        let mut bytes = vec![0x94, 0x01, 0xd9, 0x24];
        bytes.extend_from_slice(SESSION_TOKEN.as_bytes());
        bytes.extend_from_slice(&[0x07, 0xd9, 0x24]);
        bytes.extend_from_slice(ARCHIVE_ID.as_bytes());
        // msgpack-c packs a non-negative `int64_t` with the smallest unsigned encoding.
        bytes.extend_from_slice(&[0x93, 0x00, 0xcf]);
        bytes.extend_from_slice(&1_700_000_000_000_i64.to_be_bytes());
        bytes.push(0xa6);
        bytes.extend_from_slice(b"first\n");
        bytes.extend_from_slice(&[0x93, 0x01, 0xfb, 0xa7]);
        bytes.extend_from_slice(b"second\n");
        bytes.extend_from_slice(&[0x91, 0x02]);
        assert_eq!(
            bytes,
            [
                encode_handshake(SESSION_TOKEN, ARCHIVE_ID),
                encode_result(0, 1_700_000_000_000, "first\n"),
                encode_result(1, -5, "second\n"),
                encode_trailer(2),
            ]
            .concat()
        );

        let mut decoder = ConnectionDecoder::new();
        let mut buffer = BytesMut::from(&bytes[..]);
        assert_eq!(
            decode_all(&mut decoder, &mut buffer),
            Ok(vec![
                handshake_message(),
                result_message(0, 1_700_000_000_000, "first\n"),
                result_message(1, -5, "second\n"),
                Message::Trailer(Trailer { num_results: 2 }),
            ])
        );
        assert!(buffer.is_empty());
        assert!(decoder.is_finished());
    }

    #[test]
    fn decodes_a_connection_cut_at_every_byte_offset() {
        let long_message = "x".repeat(300);
        let encoded_messages = [
            encode_handshake(SESSION_TOKEN, ARCHIVE_ID),
            encode_result(0, 0, ""),
            encode_result(1, -1, "line\n"),
            encode_result(2, i64::MAX, &long_message),
            encode_trailer(3),
        ];
        let expected = [
            handshake_message(),
            result_message(0, 0, ""),
            result_message(1, -1, "line\n"),
            result_message(2, i64::MAX, &long_message),
            Message::Trailer(Trailer { num_results: 3 }),
        ];
        let bytes = encoded_messages.concat();
        let mut message_ends = Vec::new();
        for encoded in &encoded_messages {
            message_ends.push(message_ends.last().unwrap_or(&0) + encoded.len());
        }

        for cut in 0..=bytes.len() {
            let mut decoder = ConnectionDecoder::new();
            let mut buffer = BytesMut::from(&bytes[..cut]);
            let num_complete = message_ends.iter().filter(|end| **end <= cut).count();
            let first_messages =
                decode_all(&mut decoder, &mut buffer).expect("the prefix should be valid");
            assert_eq!(first_messages, expected[..num_complete], "cut at {cut}");
            let num_consumed = num_complete
                .checked_sub(1)
                .map_or(0, |last| message_ends[last]);
            assert_eq!(&buffer[..], &bytes[num_consumed..cut], "cut at {cut}");
            assert_eq!(decoder.is_finished(), num_complete == expected.len());

            buffer.extend_from_slice(&bytes[cut..]);
            let last_messages =
                decode_all(&mut decoder, &mut buffer).expect("the rest should be valid");
            assert_eq!(last_messages, expected[num_complete..], "cut at {cut}");
            assert!(buffer.is_empty());
            assert!(decoder.is_finished());
        }

        // Feeding one byte at a time resumes each message many times.
        let mut decoder = ConnectionDecoder::new();
        let mut buffer = BytesMut::new();
        let mut all_messages = Vec::new();
        for byte in &bytes {
            buffer.extend_from_slice(&[*byte]);
            all_messages
                .extend(decode_all(&mut decoder, &mut buffer).expect("bytes should be valid"));
        }
        assert_eq!(all_messages, expected);
    }

    #[test]
    fn accepts_any_msgpack_encoding_of_each_value() {
        // Every value below uses another encoding than the smallest one, which `clp-s` uses.
        let mut bytes = vec![0xdc, 0x00, 0x04];
        rmp::encode::write_i8(&mut bytes, 1).expect(WRITE);
        bytes.extend_from_slice(&[0xdb, 0x00, 0x00, 0x00, 0x24]);
        bytes.extend_from_slice(SESSION_TOKEN.as_bytes());
        rmp::encode::write_u64(&mut bytes, TASK_INDEX).expect(WRITE);
        bytes.extend_from_slice(&[0xda, 0x00, 0x24]);
        bytes.extend_from_slice(ARCHIVE_ID.as_bytes());

        bytes.extend_from_slice(&[0xdd, 0x00, 0x00, 0x00, 0x03]);
        rmp::encode::write_i64(&mut bytes, 0).expect(WRITE);
        rmp::encode::write_i8(&mut bytes, -1).expect(WRITE);
        bytes.extend_from_slice(&[0xd9, 0x02, b'a', b'b']);

        bytes.push(0x93);
        rmp::encode::write_u16(&mut bytes, 1).expect(WRITE);
        rmp::encode::write_i64(&mut bytes, i64::MIN).expect(WRITE);
        bytes.extend_from_slice(&[0xda, 0x00, 0x00]);

        bytes.push(0x93);
        rmp::encode::write_u32(&mut bytes, 2).expect(WRITE);
        rmp::encode::write_u64(&mut bytes, i64::MAX.cast_unsigned()).expect(WRITE);
        rmp::encode::write_str(&mut bytes, "c").expect(WRITE);

        bytes.push(0x93);
        rmp::encode::write_u8(&mut bytes, 3).expect(WRITE);
        rmp::encode::write_i16(&mut bytes, 300).expect(WRITE);
        rmp::encode::write_str(&mut bytes, "d").expect(WRITE);

        bytes.extend_from_slice(&[0xdc, 0x00, 0x01]);
        rmp::encode::write_i32(&mut bytes, 4).expect(WRITE);

        let mut decoder = ConnectionDecoder::new();
        let mut buffer = BytesMut::from(&bytes[..]);
        assert_eq!(
            decode_all(&mut decoder, &mut buffer),
            Ok(vec![
                handshake_message(),
                result_message(0, -1, "ab"),
                result_message(1, i64::MIN, ""),
                result_message(2, i64::MAX, "c"),
                result_message(3, 300, "d"),
                Message::Trailer(Trailer { num_results: 4 }),
            ])
        );
        assert!(decoder.is_finished());
    }

    #[test]
    fn rejects_unsupported_versions_before_the_other_fields() {
        for version in [0, 2, u64::MAX] {
            let mut bytes = vec![0x94];
            rmp::encode::write_uint(&mut bytes, version).expect(WRITE);
            assert_eq!(
                decode_error(&[], &bytes),
                ProtocolError::UnsupportedVersion(version)
            );
        }
        // Another version may have another number of fields.
        assert_eq!(
            decode_error(&[], &[0x95, 0x02]),
            ProtocolError::UnsupportedVersion(2)
        );
    }

    #[test]
    fn rejects_malformed_handshakes() {
        let simple_session_token = SESSION_TOKEN.replace('-', "");
        let cases: [(&[u8], &str); 10] = [
            (&[0xa1, b'x'], "to be an array"),
            (&[0xc1], "to be an array"),
            (&[0x90], "no fields"),
            (&[0x93, 0x01], "3 fields instead of 4"),
            (&[0x94, 0xc0], "`protocol_version` to be an integer"),
            (&[0x94, 0x01, 0x05], "`session_token` to be a string"),
            (
                &encode_handshake("not-a-uuid", ARCHIVE_ID),
                "isn't a hyphenated UUID",
            ),
            (
                &encode_handshake(&simple_session_token, ARCHIVE_ID),
                "isn't a hyphenated UUID",
            ),
            (
                &[0x94, 0x01, 0xdb, 0x00, 0x10, 0x00, 0x00],
                "more than the limit of 64",
            ),
            (
                &encode_handshake(SESSION_TOKEN, "not-a-uuid"),
                "invalid archive ID",
            ),
        ];
        for (bytes, expected) in cases {
            assert_malformed(&[], bytes, expected);
        }
    }

    #[test]
    fn rejects_malformed_results_and_trailers() {
        let handshake = encode_handshake(SESSION_TOKEN, ARCHIVE_ID);
        let cases: [(&[u8], &str); 8] = [
            (&handshake, "4 fields"),
            (&[0x00], "to be an array"),
            (&[0x90], "0 fields"),
            (&[0x92, 0x00, 0x00], "2 fields"),
            (&[0x93, 0x00, 0xa1, b'x'], "`timestamp` to be an integer"),
            (
                &[0x93, 0x00, 0x00, 0xc4, 0x01, b'x'],
                "`message` to be a string",
            ),
            (
                &[0x93, 0x00, 0x00, 0xa1, 0xff],
                "`message` isn't valid UTF-8",
            ),
            (&[0x91, 0xc2], "`num_results` to be an integer"),
        ];
        for (bytes, expected) in cases {
            assert_malformed(&handshake, bytes, expected);
        }
    }

    #[test]
    fn rejects_out_of_range_values() {
        let mut negative_task_index = encode_handshake(SESSION_TOKEN, ARCHIVE_ID);
        negative_task_index.truncate(2 + 2 + SESSION_TOKEN.len());
        negative_task_index.push(0xff);
        let mut timestamp_beyond_i64 = vec![0x93, 0x00];
        rmp::encode::write_u64(&mut timestamp_beyond_i64, u64::MAX).expect(WRITE);

        assert_malformed(&[], &[0x94, 0xff], "`protocol_version` is out of range");
        let mut min_i64_version = vec![0x94];
        rmp::encode::write_i64(&mut min_i64_version, i64::MIN).expect(WRITE);
        assert_malformed(&[], &min_i64_version, "`protocol_version` is out of range");
        assert_malformed(&[], &negative_task_index, "`task_index` is out of range");

        let handshake = encode_handshake(SESSION_TOKEN, ARCHIVE_ID);
        assert_malformed(&handshake, &[0x93, 0xff], "`result_index` is out of range");
        assert_malformed(
            &handshake,
            &timestamp_beyond_i64,
            "`timestamp` is out of range",
        );
        assert_malformed(
            &handshake,
            &[0x91, 0xd0, 0x80],
            "`num_results` is out of range",
        );
    }

    #[test]
    fn rejects_a_result_that_no_trailer_can_count() {
        let mut decoder = ConnectionDecoder {
            state: State::AwaitingResultOrTrailer {
                num_results: u64::MAX,
            },
        };
        let mut buffer = BytesMut::from(&encode_result(u64::MAX, 0, "")[..]);
        assert!(
            matches!(
                decoder.decode(&mut buffer),
                Err(ProtocolError::MalformedMessage(description))
                    if description.contains("more results than a trailer can count")
            ),
            "the result should be rejected"
        );
    }

    #[test]
    fn rejects_results_out_of_order() {
        let handshake = encode_handshake(SESSION_TOKEN, ARCHIVE_ID);
        let after_one_result = [handshake.clone(), encode_result(0, 0, "")].concat();
        // The index is checked before the rest of the result arrives.
        let cases = [
            (&handshake, vec![0x93, 0x01], 0, 1),
            (&after_one_result, encode_result(0, 0, ""), 1, 0),
            (&after_one_result, encode_result(2, 0, ""), 1, 2),
        ];
        for (preceding, bytes, expected, received) in cases {
            assert_eq!(
                decode_error(preceding, &bytes),
                ProtocolError::ResultIndexOutOfOrder { expected, received }
            );
        }
    }

    #[test]
    fn checks_the_trailer_against_the_results() {
        let handshake = encode_handshake(SESSION_TOKEN, ARCHIVE_ID);
        let after_two_results = [
            handshake.clone(),
            encode_result(0, 0, ""),
            encode_result(1, 0, ""),
        ]
        .concat();
        for trailer_num_results in [0, 1, 3, u64::MAX] {
            assert_eq!(
                decode_error(&after_two_results, &encode_trailer(trailer_num_results)),
                ProtocolError::ResultCountMismatch {
                    trailer_num_results,
                    num_results: 2,
                }
            );
        }

        let mut decoder = ConnectionDecoder::new();
        let mut buffer = BytesMut::from(&[handshake, encode_trailer(0)].concat()[..]);
        assert_eq!(
            decode_all(&mut decoder, &mut buffer),
            Ok(vec![
                handshake_message(),
                Message::Trailer(Trailer { num_results: 0 })
            ])
        );
        assert!(decoder.is_finished());
    }

    #[test]
    fn rejects_data_after_the_trailer() {
        let connection = [
            encode_handshake(SESSION_TOKEN, ARCHIVE_ID),
            encode_result(0, 0, ""),
            encode_trailer(1),
        ]
        .concat();
        for bytes in [
            vec![0x00],
            encode_trailer(1),
            encode_handshake(SESSION_TOKEN, ARCHIVE_ID),
        ] {
            assert_eq!(
                decode_error(&connection, &bytes),
                ProtocolError::DataAfterTrailer
            );
        }

        // Bytes in the same buffer as the trailer are rejected on the next decode.
        let mut decoder = ConnectionDecoder::new();
        let mut buffer = BytesMut::from(&[connection, vec![0x00]].concat()[..]);
        assert_eq!(
            decode_all(&mut decoder, &mut buffer),
            Err(ProtocolError::DataAfterTrailer)
        );
        assert!(decoder.is_finished());
        assert_eq!(&buffer[..], &[0x00]);
    }

    #[test]
    fn encodes_the_acknowledgement_in_ten_bytes() {
        assert_eq!(
            Acknowledgement { num_results: 3 }.encode(),
            [0x91, 0xcf, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03]
        );
        for num_results in [0, 1, 127, 128, 0x0102_0304_0506_0708, u64::MAX] {
            let encoded = Acknowledgement { num_results }.encode();
            assert_eq!(encoded[..2], [0x91, 0xcf]);
            assert_eq!(encoded[2..], num_results.to_be_bytes());

            let mut reader = Bytes::new(&encoded);
            assert_eq!(rmp::decode::read_array_len(&mut reader).ok(), Some(1));
            assert_eq!(
                rmp::decode::read_int::<u64, _>(&mut reader).ok(),
                Some(num_results)
            );
            assert_eq!(reader.remaining_slice(), &[] as &[u8]);
        }
    }
}
