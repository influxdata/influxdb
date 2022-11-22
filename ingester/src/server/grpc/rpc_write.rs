use data_types::{NamespaceId, PartitionKey, TableId};
use dml::{DmlMeta, DmlOperation, DmlWrite};
use generated_types::influxdata::iox::ingester::v1::{
    self as proto, write_service_server::WriteService,
};
use mutable_batch::writer;
use mutable_batch_pb::decode::decode_database_batch;
use observability_deps::tracing::*;
use thiserror::Error;
use tonic::{Request, Response};

use crate::{
    data::DmlApplyAction,
    dml_sink::{DmlError, DmlSink},
};

// A list of error states when handling an RPC write request.
//
// Note that this isn't strictly necessary as the [`WriteService`] trait
// expects a [`tonic::Status`] error value, but by defining the errors here they
// serve as documentation of the potential error states (which are then
// converted into [`tonic::Status`] for the handler).
#[derive(Debug, Error)]
enum RpcError {
    #[error("rpc write request does not contain a payload")]
    NoPayload,

    #[error("rpc write request does not contain any table data")]
    NoTables,

    #[error(transparent)]
    Decode(mutable_batch_pb::decode::Error),

    #[error(transparent)]
    Apply(DmlError),
}

impl From<RpcError> for tonic::Status {
    fn from(e: RpcError) -> Self {
        use crate::data::Error;

        match e {
            RpcError::Decode(_) | RpcError::NoPayload | RpcError::NoTables => {
                Self::invalid_argument(e.to_string())
            }
            RpcError::Apply(DmlError::Data(Error::BufferWrite { source })) => {
                map_write_error(source)
            }
            RpcError::Apply(DmlError::Data(Error::ShardNotFound { .. })) => {
                // This is not a reachable error state in the gRPC write model,
                // and is enumerated here purely because of error conflation
                // (one big error type instead of small, composable errors).
                unreachable!()
            }
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
pub struct RpcWrite<T> {
    sink: T,
}

impl<T> RpcWrite<T> {
    /// Instantiate a new [`RpcWrite`] that pushes [`DmlOperation`] instances
    /// into `sink`.
    #[allow(dead_code)]
    pub fn new(sink: T) -> Self {
        Self { sink }
    }
}

#[tonic::async_trait]
impl<T> WriteService for RpcWrite<T>
where
    T: DmlSink + 'static,
{
    async fn write(
        &self,
        request: Request<proto::WriteRequest>,
    ) -> Result<Response<proto::WriteResponse>, tonic::Status> {
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
            // The tracing context should be propagated over the RPC boundary.
            //
            // See https://github.com/influxdata/influxdb_iox/issues/6177
            DmlMeta::unsequenced(None),
        );

        // Apply the DML op to the in-memory buffer.
        match self.sink.apply(DmlOperation::Write(op)).await {
            Ok(DmlApplyAction::Applied(_)) => {
                // Discard the lifecycle manager's "should_pause" hint.
            }
            Ok(DmlApplyAction::Skipped) => {
                // Assert that the write was not skipped due to having a non-monotonic
                // sequence number. In this gRPC write model, there are no sequence
                // numbers!
                unreachable!("rpc write saw skipped op apply call")
            }
            Err(e) => {
                error!(error=%e, "failed to apply DML op");
                return Err(RpcError::Apply(e.into()))?;
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
                    let handler = RpcWrite::new(Arc::clone(&mock));

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
        apply_ok_pause_true,
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
        sink_ret = Ok(DmlApplyAction::Applied(true)),
        want_err = false,
        want_calls = [DmlOperation::Write(w)] => {
            // Assert the various DmlWrite properties match the expected values
            assert_eq!(w.namespace_id(), NAMESPACE_ID);
            assert_eq!(w.table_count(), 1);
            assert_eq!(*w.partition_key(), PartitionKey::from(PARTITION_KEY));
        }
    );

    test_rpc_write!(
        apply_ok_pause_false,
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
        sink_ret = Ok(DmlApplyAction::Applied(false)),
        want_err = false,
        want_calls = [DmlOperation::Write(w)] => {
            // Assert the various DmlWrite properties match the expected values
            assert_eq!(w.namespace_id(), NAMESPACE_ID);
            assert_eq!(w.table_count(), 1);
            assert_eq!(*w.partition_key(), PartitionKey::from(PARTITION_KEY));
        }
    );

    test_rpc_write!(
        no_payload,
        request = proto::WriteRequest { payload: None },
        sink_ret = Ok(DmlApplyAction::Applied(false)),
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
        sink_ret = Ok(DmlApplyAction::Applied(false)),
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
        sink_ret = Ok(DmlApplyAction::Applied(false)),
        want_err = true,
        want_calls = []
    );

    #[tokio::test]
    #[should_panic(expected = "unreachable")]
    async fn test_rpc_write_apply_skipped() {
        let mock =
            Arc::new(MockDmlSink::default().with_apply_return(vec![Ok(DmlApplyAction::Skipped)]));
        let handler = RpcWrite::new(Arc::clone(&mock));

        let _ = handler
            .write(Request::new(proto::WriteRequest {
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
            }))
            .await;
    }
}
