use std::pin::Pin;

use arrow_flight::{
    encode::FlightDataEncoderBuilder, error::FlightError,
    flight_service_server::FlightService as Flight, Action, ActionType, Criteria, Empty,
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, IpcMessage,
    PutResult, SchemaResult, Ticket,
};
use data_types::{NamespaceId, PartitionId, TableId};
use flatbuffers::FlatBufferBuilder;
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use generated_types::influxdata::iox::ingester::v1::{self as proto, PartitionStatus};
use metric::U64Counter;
use observability_deps::tracing::*;
use prost::Message;
use thiserror::Error;
use tokio::sync::{Semaphore, TryAcquireError};
use tonic::{Request, Response, Streaming};
use trace::{ctx::SpanContext, span::SpanExt};

use uuid::Uuid;

use crate::query::{response::QueryResponse, QueryError, QueryExec};

/// Error states for the query RPC handler.
///
/// Note that this DOES NOT include any query-time error states - those are
/// mapped directly from the [`QueryError`] itself.
///
/// Note that this isn't strictly necessary as the [`FlightService`] trait
/// expects a [`tonic::Status`] error value, but by defining the errors here
/// they serve as documentation of the potential error states (which are then
/// converted into [`tonic::Status`] for the handler).
#[derive(Debug, Error)]
enum Error {
    /// The payload within the Flight ticket cannot be deserialised into a
    /// [`proto::IngesterQueryRequest`].
    #[error("invalid flight ticket: {0}")]
    InvalidTicket(#[from] prost::DecodeError),

    /// The number of simultaneous queries being executed has been reached.
    #[error("simultaneous query limit exceeded")]
    RequestLimit,
}

/// Map a query-execution error into a [`tonic::Status`].
impl From<QueryError> for tonic::Status {
    fn from(e: QueryError) -> Self {
        use tonic::Code;

        let code = match e {
            QueryError::TableNotFound(_, _) | QueryError::NamespaceNotFound(_) => Code::NotFound,
        };

        Self::new(code, e.to_string())
    }
}

/// Map a gRPC handler error to a [`tonic::Status`].
impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        use tonic::Code;

        let code = match e {
            Error::InvalidTicket(_) => {
                debug!(error=%e, "invalid flight query ticket");
                Code::InvalidArgument
            }
            Error::RequestLimit => {
                warn!("simultaneous query limit exceeded");
                Code::ResourceExhausted
            }
        };

        Self::new(code, e.to_string())
    }
}

/// Concrete implementation of the gRPC Arrow Flight Service API
#[derive(Debug)]
pub(crate) struct FlightService<Q> {
    query_handler: Q,

    /// A request limiter to restrict the number of simultaneous requests this
    /// ingester services.
    ///
    /// This allows the ingester to drop a portion of requests when experiencing
    /// an unusual flood of requests
    request_sem: Semaphore,

    /// Number of queries rejected due to lack of available `request_sem`
    /// permit.
    query_request_limit_rejected: U64Counter,

    ingester_uuid: Uuid,
}

impl<Q> FlightService<Q> {
    pub(super) fn new(
        query_handler: Q,
        max_simultaneous_requests: usize,
        metrics: &metric::Registry,
    ) -> Self {
        let query_request_limit_rejected = metrics
            .register_metric::<U64Counter>(
                "query_request_limit_rejected",
                "number of query requests rejected due to exceeding parallel request limit",
            )
            .recorder(&[]);

        Self {
            query_handler,
            request_sem: Semaphore::new(max_simultaneous_requests),
            query_request_limit_rejected,
            ingester_uuid: Uuid::new_v4(),
        }
    }
}

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + 'static>>;

#[tonic::async_trait]
impl<Q> Flight for FlightService<Q>
where
    Q: QueryExec<Response = QueryResponse> + 'static,
{
    type HandshakeStream = TonicStream<HandshakeResponse>;
    type ListFlightsStream = TonicStream<FlightInfo>;
    type DoGetStream = TonicStream<FlightData>;
    type DoPutStream = TonicStream<PutResult>;
    type DoActionStream = TonicStream<arrow_flight::Result>;
    type ListActionsStream = TonicStream<ActionType>;
    type DoExchangeStream = TonicStream<FlightData>;

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, tonic::Status> {
        let span_ctx: Option<SpanContext> = request.extensions().get().cloned();

        // Acquire and hold a permit for the duration of this request, or return
        // an error if the existing requests have already exhausted the
        // allocation.
        //
        // Our goal is to limit the number of concurrently executing queries as
        // a rough way of ensuring we don't explode memory by trying to do too
        // much at the same time.
        let _permit = match self.request_sem.try_acquire() {
            Ok(p) => p,
            Err(TryAcquireError::NoPermits) => {
                warn!("simultaneous request limit exceeded - dropping query request");
                self.query_request_limit_rejected.inc(1);
                return Err(Error::RequestLimit)?;
            }
            Err(e) => panic!("request limiter error: {e}"),
        };

        let ticket = request.into_inner();
        let request = proto::IngesterQueryRequest::decode(&*ticket.ticket).map_err(Error::from)?;

        // Extract the namespace/table identifiers
        let namespace_id = NamespaceId::new(request.namespace_id);
        let table_id = TableId::new(request.table_id);

        // Predicate pushdown is part of the API, but not implemented.
        if let Some(p) = request.predicate {
            warn!(predicate=?p, "ignoring query predicate (unsupported)");
        }

        let response = self
            .query_handler
            .query_exec(
                namespace_id,
                table_id,
                request.columns,
                span_ctx.child_span("ingester query"),
            )
            .await?;

        let output = encode_response(response, self.ingester_uuid).map_err(tonic::Status::from);

        Ok(Response::new(Box::pin(output) as Self::DoGetStream))
    }

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, tonic::Status> {
        let request = request.into_inner().message().await?.unwrap();
        let response = HandshakeResponse {
            protocol_version: request.protocol_version,
            payload: request.payload,
        };
        let output = futures::stream::iter(std::iter::once(Ok(response)));
        Ok(Response::new(Box::pin(output) as Self::HandshakeStream))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }
}

/// Encode the partition information as a None flight data with metadata
fn encode_partition(
    // Partition ID.
    partition_id: PartitionId,
    // Partition persistence status.
    status: PartitionStatus,
    // Count of persisted Parquet files
    completed_persistence_count: u64,
    ingester_uuid: Uuid,
) -> std::result::Result<FlightData, FlightError> {
    let mut bytes = bytes::BytesMut::new();
    let app_metadata = proto::IngesterQueryResponseMetadata {
        partition_id: partition_id.get(),
        status: Some(proto::PartitionStatus {
            parquet_max_sequence_number: status.parquet_max_sequence_number,
        }),
        ingester_uuid: ingester_uuid.to_string(),
        completed_persistence_count,
    };
    prost::Message::encode(&app_metadata, &mut bytes)
        .map_err(|e| FlightError::from_external_error(Box::new(e)))?;

    Ok(FlightData::new(
        None,
        IpcMessage(build_none_flight_msg().into()),
        bytes.to_vec(),
        vec![],
    ))
}

fn build_none_flight_msg() -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();

    let mut message = arrow::ipc::MessageBuilder::new(&mut fbb);
    message.add_version(arrow::ipc::MetadataVersion::V5);
    message.add_header_type(arrow::ipc::MessageHeader::NONE);
    message.add_bodyLength(0);

    let data = message.finish();
    fbb.finish(data, None);

    fbb.finished_data().to_vec()
}

/// Converts a QueryResponse into a stream of Arrow Flight [`FlightData`] response frames.
fn encode_response(
    response: QueryResponse,
    ingester_uuid: Uuid,
) -> BoxStream<'static, std::result::Result<FlightData, FlightError>> {
    response
        .into_partition_stream()
        .flat_map(move |partition| {
            let partition_id = partition.id();
            let completed_persistence_count = partition.completed_persistence_count();
            let head = futures::stream::once(async move {
                encode_partition(
                    partition_id,
                    PartitionStatus {
                        parquet_max_sequence_number: None,
                    },
                    completed_persistence_count,
                    ingester_uuid,
                )
            });

            match partition.into_record_batch_stream() {
                Some(stream) => {
                    let stream = stream.map_err(|e| FlightError::ExternalError(Box::new(e)));

                    let tail = FlightDataEncoderBuilder::new().build(stream);

                    head.chain(tail).boxed()
                }
                None => head.boxed(),
            }
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tonic::Code;

    use crate::query::mock_query_exec::MockQueryExec;

    use super::*;

    #[tokio::test]
    async fn limits_concurrent_queries() {
        let mut flight =
            FlightService::new(MockQueryExec::default(), 100, &metric::Registry::default());

        let req = tonic::Request::new(Ticket {
            ticket: Bytes::new(),
        });
        match flight.do_get(req).await {
            Ok(_) => panic!("expected error because of invalid ticket"),
            Err(s) => {
                assert_eq!(s.code(), Code::NotFound); // Mock response value
            }
        }

        flight.request_sem = Semaphore::new(0);

        let req = tonic::Request::new(Ticket {
            ticket: Bytes::new(),
        });
        match flight.do_get(req).await {
            Ok(_) => panic!("expected error because of request limit"),
            Err(s) => {
                assert_eq!(s.code(), Code::ResourceExhausted);
            }
        }
    }
}
