//! Keep alive handling for response streaming.
//!
//! # The Problem
//! Under some deployment scenarios, we receive reports of cryptic error messages for certain long-running queries. For
//! example, the InfluxDB IOx CLI will report:
//!
//! ```text
//! Error querying:
//!   Tonic(
//!     Status {
//!       code: Internal, message: "h2 protocol error: error reading a body from connection: stream error received: unexpected internal error encountered",
//!       source: Some(
//!         hyper::Error(
//!           Body,
//!           Error { kind: Reset(StreamId(1), INTERNAL_ERROR, Remote) }
//!         )
//!       )
//!     }
//!   )
//! ```
//!
//! And [PyArrow] will report something like:
//!
//! ```text
//! pyarrow._flight.FlightInternalError:
//!   Flight returned internal error, with message:
//!     Received RST_STREAM with error code 2. gRPC client debug context:
//!       UNKNOWN:Error received from peer ipv6:%5B::1%5D:8888 {
//!         created_time:"2023-07-03T17:54:56.346363565+02:00",
//!         grpc_status:13,
//!         grpc_message:"Received RST_STREAM with error code 2"
//!       }.
//!       Client context: OK
//! ```
//!
//! `Received RST_STREAM with error code 2` is a good hint. According to [RFC 7540] (the HTTP/2 spec) the error code is
//! (see section 7):
//!
//! > INTERNAL_ERROR (0x2): The endpoint encountered an unexpected internal error.
//!
//! and `RST_STREAM` is (see section 6.4):
//!
//! > The `RST_STREAM` frame (type=0x3) allows for immediate termination of a stream. `RST_STREAM` is sent to request
//! > cancellation of a stream or to indicate that an error condition has occurred.
//!
//! The `grpc_status:13` confirms that -- according to [gRPC Status Codes] this means:
//!
//! > Internal errors. This means that some invariants expected by the underlying system have been broken. This error
//! > code is reserved for serious errors.
//!
//! The issue was replicated using [NGINX] and a hack in InfluxDB that makes streams really slow.
//!
//! The underlying issue is that some middleware or egress component -- e.g. [NGINX] -- terminates the response stream
//! because it thinks it is dead.
//!
//! # The Official Way
//! The [gPRC Keepalive] docs say:
//!
//! > HTTP/2 PING-based keepalives are a way to keep an HTTP/2 connection alive even when there is no data being
//! > transferred. This is done by periodically sending a PING frame to the other end of the connection.
//!
//! The `PING` mechanism is described by [RFC 7540] in section 6.7:
//!
//! > In addition to the frame header, `PING` frames MUST contain 8 octets of opaque data in the payload. ...
//! >
//! > Receivers of a `PING frame that does not include an ACK flag MUST send a `PING` frame with the ACK flag set in
//! > response, with an identical payload. ...
//!
//! So every "ping" has a "pong". However the same section also says:
//!
//! > `PING` frames are not associated with any individual stream. If a `PING` frame is received with a stream
//! > identifier field value other than `0x0`, the recipient MUST respond with a connection error (Section 5.4.1) of
//! > type `PROTOCOL_ERROR`.
//!
//! Now how should an egress proxy deal with this? Because streams may come from multiple upstream servers, they have
//! no way to establish a proper ping-pong end-to-end signaling path per stream. Hence in general it is not feasible to
//! use `PING` as a keep-alive mechanism, contrary to what the [gRPC] spec says. So what DO egress proxies do then?
//! Looking at various egress solutions:
//!
//! - <https://github.com/microsoft/reverse-proxy/issues/118#issuecomment-940191553>
//! - <https://kubernetes.github.io/ingress-nginx/examples/grpc/#notes-on-using-responserequest-streams>
//!
//! They all seem to agree that either you set really long timeouts and/or activity-based keep-alive, i.e. they require
//! SOMETHING to be send on that stream.
//!
//! # The Wanted Workaround
//! Since all `PING`-based signalling is broken, we fall back to activity-based keep-alive, i.e. we ensure that we
//! regularly send something in our stream.
//!
//! Our response stream follows the [Apache Flight] defintion. This means that we have a [gRPC] stream with
//! [`FlightData`] messages. Every of these messages has a [`MessageHeader`] describing its content. This is
//! [FlatBuffers] union with the following options:
//!
//! - `None`: This is the implicit default.
//! - `Schema`: Sent before any other data to describe the schema of the stream.
//! - `DictionaryBatch`: Encodes dictionary data. This is not used in practice at the moment because dictionaries are
//!   always hydrated.
//! - `RecordBatch`: Content of a `RecordBatch` w/o schema information.
//! - `Tensor`, `SparseTensor`: Irrelevant for us.
//!
//! Ideally we would send a `None` messages with some metdata. However most clients are too broken to accept this and
//! will trip over these messages. E.g. [PyArrow] -- which uses the C++ implementation -- will fail with:
//!
//! ```text
//! OSError: Header-type of flatbuffer-encoded Message is not RecordBatch.
//! ```
//!
//! # The Actual Workaround
//! So we send actual empty `RecordBatch`es instead. These are encoded as `RecordBatch` messages w/o a schema (see
//! section above). The schema is sent separately right at the start of the stream. The arrow-rs implementation does
//! that for us and also ensures that the schema is adjusted for dictionary hydration. So we just inspect the data
//! stream and wait for that schema (the upstream implementation will always send this without any blocking / wait
//! time / actual `RecordBatch` data).
//!
//!
//! [Apache Flight]: https://arrow.apache.org/docs/format/Flight.html
//! [FlatBuffers]: https://flatbuffers.dev/
//! [`FlightData`]: https://github.com/apache/arrow/blob/cd1ed18fd1e08912ea47b64edf55be9c046375c4/format/Flight.proto#L401-L429
//! [gRPC]: https://grpc.io/
//! [gPRC Keepalive]: https://grpc.io/docs/guides/keepalive/
//! [gRPC Status Codes]: https://grpc.github.io/grpc/core/md_doc_statuscodes.html
//! [`MessageHeader`]: https://github.com/apache/arrow/blob/cd1ed18fd1e08912ea47b64edf55be9c046375c4/format/Message.fbs#L124-L132
//! [NGINX]: https://nginx.org/
//! [PyArrow]: https://arrow.apache.org/docs/python/index.html
//! [RFC 7540]: https://httpwg.org/specs/rfc7540.html

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use arrow::{
    datatypes::{DataType, Schema, SchemaRef},
    ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
    record_batch::RecordBatch,
};
use arrow_flight::{error::FlightError, FlightData};
use futures::{stream::BoxStream, Stream, StreamExt};
use observability_deps::tracing::{info, warn};
use tokio::time::{Interval, MissedTickBehavior};

/// Keep alive underlying response stream by sending regular empty [`RecordBatch`]es.
pub struct KeepAliveStream {
    inner: BoxStream<'static, Result<FlightData, FlightError>>,
}

impl KeepAliveStream {
    /// Create new keep-alive wrapper from the underlying stream and the given interval.
    ///
    /// The interval is measured from the last message -- which can either be a "real" message or a keep-alive.
    pub fn new<S>(s: S, interval: Duration) -> Self
    where
        S: Stream<Item = Result<FlightData, FlightError>> + Send + 'static,
    {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let state = State {
            inner: s.boxed(),
            schema: None,
            ticker,
        };

        let inner = futures::stream::unfold(state, |mut state| async move {
            loop {
                tokio::select! {
                    _ = state.ticker.tick() => {
                        let Some(data) = build_empty_batch_msg(state.schema.as_ref()) else {
                            continue;
                        };
                        info!("stream keep-alive");
                        return Some((Ok(data), state));
                    }
                    res = state.inner.next() => {
                        // peek at content to detect schema transmission
                        if let Some(Ok(data)) = &res {
                            if let Some(schema) = decode_schema(data) {
                                if check_schema(&schema) {
                                    state.schema = Some(Arc::new(schema));
                                }
                            }
                        }

                        state.ticker.reset();
                        return res.map(|res| (res, state));
                    }
                }
            }
        })
        .boxed();

        Self { inner }
    }
}

impl Stream for KeepAliveStream {
    type Item = Result<FlightData, FlightError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

/// Inner state of [`KeepAliveStream`]
struct State {
    /// The underlying stream that is kept alive.
    inner: BoxStream<'static, Result<FlightData, FlightError>>,

    /// A [`Schema`] that was already received from the stream.
    ///
    /// We need this to produce sensible empty [`RecordBatch`]es and because [`RecordBatch`] messages can only come
    /// AFTER an encoded [`Schema`].
    schema: Option<SchemaRef>,

    /// Keep-alive ticker.
    ticker: Interval,
}

/// Decode [`Schema`] from response data stream.
fn decode_schema(data: &FlightData) -> Option<Schema> {
    let message = arrow::ipc::root_as_message(&data.data_header[..]).ok()?;

    if arrow::ipc::MessageHeader::Schema != message.header_type() {
        return None;
    }
    Schema::try_from(data).ok()
}

/// Check that the [`Schema`] that we've [decoded](decode_schema) is sensible.
///
/// Returns `true` if the [`Schema`] is OK. Will log a warning and return `false` if there is a problem.
fn check_schema(schema: &Schema) -> bool {
    schema.fields().iter().all(|field| match field.data_type() {
        DataType::Dictionary(_, _) => {
            warn!(
                field = field.name(),
                "arrow IPC schema still contains dictionary, should have been hydrated by now",
            );
            false
        }
        _ => true,
    })
}

/// Encode an empty [`RecordBatch`] as a message.
///
/// This must only be sent AFTER a [`Schema`] was transmitted.
fn build_empty_batch_msg(schema: Option<&SchemaRef>) -> Option<FlightData> {
    let Some(schema) = schema else {
        warn!(
            "cannot send keep-alive because no schema was transmitted yet",
        );
        return None;
    };

    let batch = RecordBatch::new_empty(Arc::clone(schema));
    let data_gen = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(true);
    let write_options = IpcWriteOptions::default();
    let batch_data = match data_gen.encoded_batch(&batch, &mut dictionary_tracker, &write_options) {
        Ok((dicts_data, batch_data)) => {
            assert!(dicts_data.is_empty());
            batch_data
        }
        Err(e) => {
            warn!(
                %e,
                "cannot encode empty batch",
            );
            return None;
        }
    };

    Some(batch_data.into())
}

#[cfg(test)]
pub mod test_util {
    use std::time::Duration;

    use futures::{stream::BoxStream, Stream, StreamExt};

    /// Ensure that there is a delay between steam responses.
    pub fn make_stream_slow<S>(s: S, delay: Duration) -> BoxStream<'static, S::Item>
    where
        S: Send + Stream + Unpin + 'static,
    {
        futures::stream::unfold(s, move |mut s| async move {
            tokio::time::sleep(delay).await;
            let res = s.next().await;
            res.map(|res| (res, s))
        })
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use arrow::{array::Int64Array, datatypes::Field};
    use arrow_flight::{decode::FlightRecordBatchStream, encode::FlightDataEncoderBuilder};
    use datafusion::assert_batches_eq;
    use futures::TryStreamExt;
    use test_helpers::maybe_start_logging;

    use super::{test_util::make_stream_slow, *};

    type BatchStream = BoxStream<'static, Result<RecordBatch, FlightError>>;
    type FlightStream = BoxStream<'static, Result<FlightData, FlightError>>;

    #[tokio::test]
    #[should_panic(expected = "stream timeout")]
    async fn test_timeout() {
        let s = make_test_stream(false);
        let s = FlightRecordBatchStream::new_from_flight_data(s);
        s.collect::<Vec<_>>().await;
    }

    #[tokio::test]
    async fn test_keep_alive() {
        maybe_start_logging();

        let s = make_test_stream(true);
        let s = FlightRecordBatchStream::new_from_flight_data(s);
        let batches: Vec<_> = s.try_collect().await.unwrap();
        assert_batches_eq!(
            vec!["+---+", "| f |", "+---+", "| 1 |", "| 2 |", "| 3 |", "| 4 |", "| 5 |", "+---+",],
            &batches
        );
    }

    /// Creates a stream like the query processing would do.
    fn make_query_result_stream() -> (BatchStream, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Int64, false)]));

        let batch_1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let batch_2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![4, 5]))],
        )
        .unwrap();

        let s = futures::stream::iter([batch_1, batch_2]).map(Ok).boxed();
        (s, schema)
    }

    /// Convert query result stream (= [`RecordBatch`]es) into a [`FlightData`] stream.
    ///
    /// This stream will -- as in prod -- send the [`Schema`] data even when there are no [`RecordBatch`]es yet.
    fn make_flight_data_stream(s: BatchStream, schema: SchemaRef) -> FlightStream {
        FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(s)
            .boxed()
    }

    fn panic_on_stream_timeout(s: FlightStream, timeout: Duration) -> FlightStream {
        futures::stream::unfold(s, move |mut s| async move {
            let res = tokio::time::timeout(timeout, s.next())
                .await
                .expect("stream timeout");
            res.map(|res| (res, s))
        })
        .boxed()
    }

    fn make_test_stream(keep_alive: bool) -> FlightStream {
        let (s, schema) = make_query_result_stream();
        let s = make_stream_slow(s, Duration::from_millis(500));
        let s = make_flight_data_stream(s, schema);
        let s = if keep_alive {
            KeepAliveStream::new(s, Duration::from_millis(100)).boxed()
        } else {
            s
        };
        let s = panic_on_stream_timeout(s, Duration::from_millis(250));
        s
    }
}
