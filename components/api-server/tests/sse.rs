use std::convert::Infallible;
use std::time::Duration;

use anyhow::Result;
use api_server::sse::SseEncoding;
use api_server::sse::SseResponse;
use axum::body::Body;
use axum::body::Bytes;
use axum::response::IntoResponse;
use axum::response::Sse;
use axum::response::sse::Event;
use axum::response::sse::KeepAlive;
use clp_rust_utils::types::ArchiveId;
use futures::stream;
use http_body_util::BodyExt;
use query_result_listener::encoding::ResultEncoder;
use query_result_listener::encoding::ResultEncoding;
use query_result_listener::test_utils::ARCHIVE_ID;
use serde::Serialize;
use tokio::time::advance;

/// The interval of [`KeepAlive::default`].
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// The smallest step of tokio's timer.
const TICK: Duration = Duration::from_millis(1);

/// The data of a query result's event.
#[derive(Serialize)]
struct StreamingQueryResult {
    archive_id: ArchiveId,
    timestamp_millisecs: i64,
    message: String,
}

/// The next frame of a response body, as far as polling the body once shows.
#[derive(Debug, Eq, PartialEq)]
enum NextFrame {
    /// No frame is ready.
    Pending,

    /// A frame holding data.
    Data(Bytes),

    /// An error, as its message.
    Error(String),

    /// The body has ended.
    End,
}

impl NextFrame {
    /// Polls `body` once for its next frame.
    ///
    /// # Returns
    ///
    /// The next frame, or [`NextFrame::Pending`] if none is ready.
    ///
    /// # Panics
    ///
    /// Panics if `body` yields a frame that doesn't hold data.
    fn poll(body: &mut Body) -> Self {
        use futures::FutureExt;

        match body.frame().now_or_never() {
            None => Self::Pending,
            Some(None) => Self::End,
            Some(Some(Ok(frame))) => {
                Self::Data(frame.into_data().expect("the frame should hold data"))
            }
            Some(Some(Err(error))) => Self::Error(error.to_string()),
        }
    }
}

/// # Returns
///
/// The event that [`Event::json_data`] encodes for the result made of `archive_id`,
/// `timestamp_millisecs` and `message`.
///
/// # Panics
///
/// Panics if serializing the result fails.
fn axum_result_event(archive_id: ArchiveId, timestamp_millisecs: i64, message: String) -> Event {
    Event::default()
        .json_data(StreamingQueryResult {
            archive_id,
            timestamp_millisecs,
            message,
        })
        .expect("serializing a result shouldn't fail")
}

/// # Returns
///
/// The body of an [`Sse`] response streaming `events`.
///
/// # Panics
///
/// Panics if the body can't be read.
async fn axum_sse_body(events: Vec<Event>) -> Bytes {
    Sse::new(stream::iter(events.into_iter().map(Ok::<_, Infallible>)))
        .into_response()
        .into_body()
        .collect()
        .await
        .expect("the body should be readable")
        .to_bytes()
}

/// Waits for the first keep-alive comment of an idle [`Sse`] response with [`KeepAlive::default`].
///
/// # Returns
///
/// The comment.
///
/// # Panics
///
/// Panics if the body ends, can't be read, or yields a frame that doesn't hold data.
async fn axum_keep_alive_comment() -> Bytes {
    Sse::new(stream::pending::<Result<Event, Infallible>>())
        .keep_alive(KeepAlive::default())
        .into_response()
        .into_body()
        .frame()
        .await
        .expect("the body shouldn't end")
        .expect("the body should be readable")
        .into_data()
        .expect("the frame should hold data")
}

#[tokio::test]
async fn sse_encoder_matches_axum_json_events() -> Result<()> {
    const TIMESTAMP_MILLISECS: i64 = -1;

    let archive_id = ARCHIVE_ID.parse::<ArchiveId>()?;
    let mut encoder = <SseEncoding as ResultEncoding>::Encoder::new(archive_id);
    let mut axum_events = Vec::new();
    for (message, event_message) in [
        ("{\"quoted\":\"value\"}\n", "{\"quoted\":\"value\"}"),
        ("x\n\n", "x\n"),
        ("x\r\n", "x\r"),
        ("inner\nnewline", "inner\nnewline"),
        ("", ""),
        ("\n", ""),
    ] {
        encoder.encode(TIMESTAMP_MILLISECS, message);
        axum_events.push(axum_result_event(
            archive_id,
            TIMESTAMP_MILLISECS,
            event_message.to_owned(),
        ));
    }

    assert_eq!(encoder.take_batch(), axum_sse_body(axum_events).await);
    Ok(())
}

#[tokio::test]
async fn sse_encoder_starts_the_next_batch_empty() -> Result<()> {
    let archive_id = ARCHIVE_ID.parse::<ArchiveId>()?;
    let mut encoder = <SseEncoding as ResultEncoding>::Encoder::new(archive_id);
    encoder.encode(1, "first\n");
    encoder.take_batch();
    encoder.encode(2, "second\n");

    assert_eq!(
        encoder.take_batch(),
        axum_sse_body(vec![axum_result_event(archive_id, 2, "second".to_owned())]).await
    );
    Ok(())
}

#[tokio::test]
async fn encode_event_matches_axum_named_json_events() -> Result<()> {
    use api_server::sse::encode_event;
    use serde_json::json;

    let error = json!({"message": "failed to wait for the \"query\" job\nto terminate"});

    assert_eq!(
        encode_event("error", &error)?,
        axum_sse_body(vec![Event::default().event("error").json_data(&error)?]).await
    );
    Ok(())
}

#[tokio::test]
async fn sse_response_has_the_headers_of_axum_sse_with_default_keep_alive() -> Result<()> {
    let response = SseResponse::new(stream::pending::<Result<Bytes, Infallible>>()).into_response();
    let axum_response = Sse::new(stream::pending::<Result<Event, Infallible>>())
        .keep_alive(KeepAlive::default())
        .into_response();

    assert_eq!(response.headers(), axum_response.headers());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn idle_sse_response_sends_axum_keep_alive_comment_every_interval() -> Result<()> {
    let keep_alive_comment = axum_keep_alive_comment().await;
    let mut body = SseResponse::new(stream::pending::<Result<Bytes, Infallible>>())
        .into_response()
        .into_body();

    for _ in 0..2 {
        advance(KEEP_ALIVE_INTERVAL.saturating_sub(TICK)).await;
        assert_eq!(NextFrame::poll(&mut body), NextFrame::Pending);
        advance(TICK).await;
        assert_eq!(
            NextFrame::poll(&mut body),
            NextFrame::Data(keep_alive_comment.clone())
        );
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn event_postpones_the_next_keep_alive_comment_by_a_full_interval() -> Result<()> {
    use futures::StreamExt;
    use tokio::time::sleep;

    const EVENT: &[u8] = b"data: 1\n\n";

    let keep_alive_comment = axum_keep_alive_comment().await;
    let event_delay = KEEP_ALIVE_INTERVAL * 3 / 5;
    let event_timer = sleep(event_delay);
    let events = stream::once(async move {
        event_timer.await;
        Ok::<_, Infallible>(Bytes::from_static(EVENT))
    })
    .chain(stream::pending());
    let mut body = SseResponse::new(events).into_response().into_body();

    advance(event_delay).await;
    assert_eq!(
        NextFrame::poll(&mut body),
        NextFrame::Data(Bytes::from_static(EVENT))
    );
    advance(KEEP_ALIVE_INTERVAL.saturating_sub(TICK)).await;
    assert_eq!(NextFrame::poll(&mut body), NextFrame::Pending);
    advance(TICK).await;
    assert_eq!(
        NextFrame::poll(&mut body),
        NextFrame::Data(keep_alive_comment)
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn sse_response_forwards_errors_and_ends_with_its_events() -> Result<()> {
    const ERROR: &str = "failed to stream the events";

    let mut body = SseResponse::new(stream::iter([Err::<Bytes, _>(ERROR)]))
        .into_response()
        .into_body();

    assert_eq!(
        NextFrame::poll(&mut body),
        NextFrame::Error(ERROR.to_owned())
    );
    assert_eq!(NextFrame::poll(&mut body), NextFrame::End);
    Ok(())
}
