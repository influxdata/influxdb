use std::sync::Arc;

use data_types::{NamespaceId, PartitionKey, Sequence, TableId};
use dml::{DmlMeta, DmlOperation, DmlWrite};
use generated_types::influxdata::iox::ingester::v1::{
    self as proto, write_service_server::WriteService,
};
use mutable_batch::writer;
use mutable_batch_pb::decode::decode_database_batch;
use observability_deps::tracing::*;
use thiserror::Error;
use tonic::{Code, Request, Response};

use crate::{
    dml_sink::{DmlError, DmlSink},
    ingest_state::{IngestState, IngestStateError},
    timestamp_oracle::TimestampOracle,
    TRANSITION_SHARD_INDEX,
};

/// A list of error states when handling an RPC write request.
///
/// Note that this isn't strictly necessary as the [`WriteService`] trait
/// expects a [`tonic::Status`] error value, but by defining the errors here they
/// serve as documentation of the potential error states (which are then
/// converted into [`tonic::Status`] for the handler).
#[derive(Debug, Error)]
enum RpcError {
    /// The RPC write request did not contain a write payload.
    #[error("rpc write request does not contain a payload")]
    NoPayload,

    /// The write payload contains no tables.
    #[error("rpc write request does not contain any table data")]
    NoTables,

    /// The serialised write payload could not be read.
    #[error(transparent)]
    Decode(mutable_batch_pb::decode::Error),

    /// The ingester's [`IngestState`] returns [`IngestStateError`] instances if
    /// set by a subsystem. See [`IngestState`] for documentation.
    #[error(transparent)]
    SystemState(IngestStateError),
}

impl From<RpcError> for tonic::Status {
    fn from(e: RpcError) -> Self {
        let code = match e {
            RpcError::Decode(_) | RpcError::NoPayload | RpcError::NoTables => Code::InvalidArgument,
            RpcError::SystemState(IngestStateError::PersistSaturated) => Code::ResourceExhausted,
            RpcError::SystemState(IngestStateError::GracefulStop) => Code::FailedPrecondition,
        };

        Self::new(code, e.to_string())
    }
}

/// Convert a [`DmlError`] returned by the configured [`DmlSink`] to a
/// [`tonic::Status`].
impl From<DmlError> for tonic::Status {
    fn from(e: DmlError) -> Self {
        match e {
            DmlError::Buffer(e) => map_write_error(e),
            DmlError::Wal(_) => Self::internal(e.to_string()),
        }
    }
}

/// Map a [`mutable_batch::Error`] to a [`tonic::Status`].
///
/// This method takes care to enumerate all possible error states, so that new
/// error additions cause a compilation failure, and therefore require the new
/// error to be explicitly mapped to a gRPC status code.
fn map_write_error(e: mutable_batch::Error) -> tonic::Status {
    use tonic::Status;
    match e {
        mutable_batch::Error::ColumnError { .. }
        | mutable_batch::Error::ArrowError { .. }
        | mutable_batch::Error::InternalSchema { .. }
        | mutable_batch::Error::ColumnNotFound { .. }
        | mutable_batch::Error::WriterError {
            source: writer::Error::KeyNotFound { .. } | writer::Error::InsufficientValues { .. },
        } => Status::internal(e.to_string()),
        mutable_batch::Error::WriterError {
            source: writer::Error::TypeMismatch { .. },
        } => {
            // While a schema type conflict is ultimately a user error, if it
            // reaches the ingester it should have already passed through schema
            // validation in the router, and as such it is an internal system
            // failure.
            Status::internal(e.to_string())
        }
    }
}

/// A gRPC [`WriteService`] handler.
///
/// This handler accepts writes from an upstream, and applies them to the
/// provided [`DmlSink`].
#[derive(Debug)]
pub(crate) struct RpcWrite<T> {
    sink: T,
    timestamp: Arc<TimestampOracle>,
    ingest_state: Arc<IngestState>,
}

impl<T> RpcWrite<T> {
    /// Instantiate a new [`RpcWrite`] that pushes [`DmlOperation`] instances
    /// into `sink`.
    pub(crate) fn new(
        sink: T,
        timestamp: Arc<TimestampOracle>,
        ingest_state: Arc<IngestState>,
    ) -> Self {
        Self {
            sink,
            timestamp,
            ingest_state,
        }
    }
}

#[tonic::async_trait]
impl<T> WriteService for RpcWrite<T>
where
    T: DmlSink + 'static,
{
    /// Handle an RPC write request.
    async fn write(
        &self,
        request: Request<proto::WriteRequest>,
    ) -> Result<Response<proto::WriteResponse>, tonic::Status> {
        // Drop writes if the persistence is saturated or the ingester is
        // shutting down.
        //
        // Stopping writes when persist is saturated gives the ingester a chance
        // to reduce the backlog of persistence tasks, which in turn reduces the
        // memory usage of the ingester. If ingest was to continue unabated, an
        // OOM would be inevitable.
        //
        // If you're seeing overload/persist saturation error responses in RPC
        // requests, you need to either:
        //
        //   * Increase the persist queue depth if there is a decent headroom of
        //     unused RAM allocated to the ingester.
        //   * Increase the RAM allocation, and increase the persist queue depth
        //     proportionally.
        //   * Deploy more ingesters to reduce the request load on any single
        //     ingester.
        //
        self.ingest_state.read().map_err(RpcError::SystemState)?;

        // Extract the remote address for debugging.
        let remote_addr = request
            .remote_addr()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "<unknown>".to_string());

        // Extract the write payload
        let payload = request.into_inner().payload.ok_or(RpcError::NoPayload)?;

        let batches = decode_database_batch(&payload).map_err(RpcError::Decode)?;
        let num_tables = batches.len();
        let namespace_id = NamespaceId::new(payload.database_id);
        let partition_key = PartitionKey::from(payload.partition_key);

        // Never attempt to create a DmlWrite with no tables - doing so causes a
        // panic.
        if num_tables == 0 {
            return Err(RpcError::NoTables)?;
        }

        trace!(
            remote_addr,
            num_tables,
            %namespace_id,
            %partition_key,
            "received rpc write"
        );

        // Reconstruct the DML operation
        let op = DmlWrite::new(
            namespace_id,
            batches
                .into_iter()
                .map(|(k, v)| (TableId::new(k), v))
                .collect(),
            partition_key,
            DmlMeta::sequenced(
                Sequence {
                    shard_index: TRANSITION_SHARD_INDEX, // TODO: remove this from DmlMeta
                    sequence_number: self.timestamp.next(),
                },
                iox_time::Time::MAX, // TODO: remove this from DmlMeta
                // The tracing context should be propagated over the RPC boundary.
                //
                // See https://github.com/influxdata/influxdb_iox/issues/6177
                None,
                42, // TODO: remove this from DmlMeta
            ),
        );

        // Apply the DML op to the in-memory buffer.
        match self.sink.apply(DmlOperation::Write(op)).await {
            Ok(()) => {}
            Err(e) => {
                error!(error=%e, "failed to apply DML op");
                return Err(e.into())?;
            }
        }

        Ok(Response::new(proto::WriteResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use generated_types::influxdata::pbdata::v1::{
        column::{SemanticType, Values},
        Column, DatabaseBatch, TableBatch,
    };

    use super::*;
    use crate::dml_sink::mock_sink::MockDmlSink;

    const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);
    const PARTITION_KEY: &str = "bananas";
    const PERSIST_QUEUE_DEPTH: usize = 42;

    macro_rules! test_rpc_write {
        (
            $name:ident,
            request = $request:expr,        // Proto WriteRequest request the server receives
            sink_ret = $sink_ret:expr,      // The mock return value from the DmlSink, if called
            want_err = $want_err:literal,   // The expectation of an error from the handler
            want_calls = $($want_calls:tt)+ //
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_rpc_write_ $name>]() {
                    let mock = Arc::new(
                        MockDmlSink::default().with_apply_return(vec![$sink_ret]),
                    );
                    let timestamp = Arc::new(TimestampOracle::new(0));

                    let ingest_state = Arc::new(IngestState::default());

                    let handler = RpcWrite::new(Arc::clone(&mock), timestamp, ingest_state);

                    let ret = handler
                        .write(Request::new($request))
                        .await;

                    assert_eq!(ret.is_err(), $want_err, "wanted handler error {} got {:?}", $want_err, ret);
                    assert_matches!(mock.get_calls().as_slice(), $($want_calls)+);
                }
            }
        };
    }

    test_rpc_write!(
        apply_ok,
        request = proto::WriteRequest {
        payload: Some(DatabaseBatch {
                database_id: NAMESPACE_ID.get(),
                partition_key: PARTITION_KEY.to_string(),
                table_batches: vec![TableBatch {
                    table_id: 42,
                    columns: vec![Column {
                        column_name: "time".to_string(),
                        semantic_type: SemanticType::Time.into(),
                        values: Some(Values {
                            i64_values: vec![4242],
                            f64_values: vec![],
                            u64_values: vec![],
                            string_values: vec![],
                            bool_values: vec![],
                            bytes_values: vec![],
                            packed_string_values: None,
                            interned_string_values: None,
                        }),
                        null_mask: vec![0],
                    }],
                    row_count: 1,
                }],
            }),
        },
        sink_ret = Ok(()),
        want_err = false,
        want_calls = [DmlOperation::Write(w)] => {
            // Assert the various DmlWrite properties match the expected values
            assert_eq!(w.namespace_id(), NAMESPACE_ID);
            assert_eq!(w.table_count(), 1);
            assert_eq!(*w.partition_key(), PartitionKey::from(PARTITION_KEY));
            assert_eq!(w.meta().sequence().unwrap().sequence_number.get(), 1);
        }
    );

    test_rpc_write!(
        no_payload,
        request = proto::WriteRequest { payload: None },
        sink_ret = Ok(()),
        want_err = true,
        want_calls = []
    );

    test_rpc_write!(
        no_tables,
        request = proto::WriteRequest {
            payload: Some(DatabaseBatch {
                database_id: NAMESPACE_ID.get(),
                partition_key: PARTITION_KEY.to_string(),
                table_batches: vec![],
            }),
        },
        sink_ret = Ok(()),
        want_err = true,
        want_calls = []
    );

    test_rpc_write!(
        batch_error,
        request = proto::WriteRequest {
            payload: Some(DatabaseBatch {
                database_id: NAMESPACE_ID.get(),
                partition_key: PARTITION_KEY.to_string(),
                table_batches: vec![TableBatch {
                    table_id: 42,
                    columns: vec![Column {
                        column_name: "time".to_string(),
                        semantic_type: SemanticType::Time.into(),
                        values: Some(Values {
                            i64_values: vec![4242],
                            f64_values: vec![],
                            u64_values: vec![4242], // Two types for one column
                            string_values: vec![],
                            bool_values: vec![],
                            bytes_values: vec![],
                            packed_string_values: None,
                            interned_string_values: None,
                        }),
                        null_mask: vec![0],
                    }],
                    row_count: 1,
                }],
            }),
        },
        sink_ret = Ok(()),
        want_err = true,
        want_calls = []
    );

    /// A property test asserting that writes that succeed earlier writes have
    /// greater timestamps assigned.
    #[tokio::test]
    async fn test_rpc_write_ordered_timestamps() {
        let mock = Arc::new(MockDmlSink::default().with_apply_return(vec![Ok(()), Ok(())]));
        let timestamp = Arc::new(TimestampOracle::new(0));

        let ingest_state = Arc::new(IngestState::default());

        let handler = RpcWrite::new(Arc::clone(&mock), timestamp, ingest_state);

        let req = proto::WriteRequest {
            payload: Some(DatabaseBatch {
                database_id: NAMESPACE_ID.get(),
                partition_key: PARTITION_KEY.to_string(),
                table_batches: vec![TableBatch {
                    table_id: 42,
                    columns: vec![Column {
                        column_name: "time".to_string(),
                        semantic_type: SemanticType::Time.into(),
                        values: Some(Values {
                            i64_values: vec![4242],
                            f64_values: vec![],
                            u64_values: vec![],
                            string_values: vec![],
                            bool_values: vec![],
                            bytes_values: vec![],
                            packed_string_values: None,
                            interned_string_values: None,
                        }),
                        null_mask: vec![0],
                    }],
                    row_count: 1,
                }],
            }),
        };

        handler
            .write(Request::new(req.clone()))
            .await
            .expect("write should succeed");

        handler
            .write(Request::new(req))
            .await
            .expect("write should succeed");

        assert_matches!(
            *mock.get_calls(),
            [DmlOperation::Write(ref w1), DmlOperation::Write(ref w2)] => {
                let w1 = w1.meta().sequence().unwrap().sequence_number.get();
                let w2 = w2.meta().sequence().unwrap().sequence_number.get();
                assert!(w1 < w2);
            }
        );
    }

    /// Validate that the persist system being marked as saturated prevents the
    /// ingester from accepting new writes.
    #[tokio::test]
    async fn test_rpc_write_persist_saturation() {
        let mock = Arc::new(MockDmlSink::default().with_apply_return(vec![Ok(()), Ok(())]));
        let timestamp = Arc::new(TimestampOracle::new(0));

        let ingest_state = Arc::new(IngestState::default());

        let handler = RpcWrite::new(Arc::clone(&mock), timestamp, Arc::clone(&ingest_state));

        let req = proto::WriteRequest {
            payload: Some(DatabaseBatch {
                database_id: NAMESPACE_ID.get(),
                partition_key: PARTITION_KEY.to_string(),
                table_batches: vec![TableBatch {
                    table_id: 42,
                    columns: vec![Column {
                        column_name: "time".to_string(),
                        semantic_type: SemanticType::Time.into(),
                        values: Some(Values {
                            i64_values: vec![4242],
                            f64_values: vec![],
                            u64_values: vec![],
                            string_values: vec![],
                            bool_values: vec![],
                            bytes_values: vec![],
                            packed_string_values: None,
                            interned_string_values: None,
                        }),
                        null_mask: vec![0],
                    }],
                    row_count: 1,
                }],
            }),
        };

        handler
            .write(Request::new(req.clone()))
            .await
            .expect("write should succeed");

        ingest_state.set(IngestStateError::PersistSaturated);

        let err = handler
            .write(Request::new(req))
            .await
            .expect_err("write should fail");

        // Validate the error code returned to the user.
        assert_eq!(err.code(), Code::ResourceExhausted);

        // One write should have been passed through to the DML sinks.
        assert_matches!(*mock.get_calls(), [DmlOperation::Write(_)]);
    }

    /// Validate that the ingester being marked as stopping prevents the
    /// ingester from accepting new writes.
    #[tokio::test]
    async fn test_rpc_write_shutdown() {
        let mock = Arc::new(MockDmlSink::default().with_apply_return(vec![Ok(()), Ok(())]));
        let timestamp = Arc::new(TimestampOracle::new(0));

        let ingest_state = Arc::new(IngestState::default());

        let handler = RpcWrite::new(Arc::clone(&mock), timestamp, Arc::clone(&ingest_state));

        let req = proto::WriteRequest {
            payload: Some(DatabaseBatch {
                database_id: NAMESPACE_ID.get(),
                partition_key: PARTITION_KEY.to_string(),
                table_batches: vec![TableBatch {
                    table_id: 42,
                    columns: vec![Column {
                        column_name: "time".to_string(),
                        semantic_type: SemanticType::Time.into(),
                        values: Some(Values {
                            i64_values: vec![4242],
                            f64_values: vec![],
                            u64_values: vec![],
                            string_values: vec![],
                            bool_values: vec![],
                            bytes_values: vec![],
                            packed_string_values: None,
                            interned_string_values: None,
                        }),
                        null_mask: vec![0],
                    }],
                    row_count: 1,
                }],
            }),
        };

        handler
            .write(Request::new(req.clone()))
            .await
            .expect("write should succeed");

        ingest_state.set(IngestStateError::GracefulStop);

        let err = handler
            .write(Request::new(req))
            .await
            .expect_err("write should fail");

        // Validate the error code returned to the user.
        assert_eq!(err.code(), Code::FailedPrecondition);

        // One write should have been passed through to the DML sinks.
        assert_matches!(*mock.get_calls(), [DmlOperation::Write(_)]);
    }
}
