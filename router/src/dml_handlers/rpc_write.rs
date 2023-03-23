mod balancer;
mod circuit_breaker;
mod circuit_breaking_client;
pub mod client;
pub mod lazy_connector;
mod upstream_snapshot;

use crate::dml_handlers::rpc_write::client::WriteClient;

use self::{
    balancer::Balancer,
    circuit_breaker::CircuitBreaker,
    circuit_breaking_client::{CircuitBreakerState, CircuitBreakingClient},
    upstream_snapshot::UpstreamSnapshot,
};

use super::{DmlHandler, Partitioned};
use async_trait::async_trait;
use data_types::{DeletePredicate, NamespaceId, NamespaceName, TableId};
use dml::{DmlMeta, DmlWrite};
use generated_types::influxdata::iox::ingester::v1::WriteRequest;
use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use mutable_batch_pb::encode::encode_write;
use observability_deps::tracing::*;
use std::{fmt::Debug, sync::Arc, time::Duration};
use thiserror::Error;
use trace::ctx::SpanContext;

/// The bound on RPC request duration.
///
/// This includes the time taken to send the request, and wait for the response.
pub const RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Errors experienced when submitting an RPC write request to an Ingester.
#[derive(Debug, Error)]
pub enum RpcWriteError {
    /// The upstream ingester returned an error response.
    #[error("upstream ingester error: {0}")]
    Upstream(#[from] tonic::Status),

    /// The RPC call timed out after [`RPC_TIMEOUT`] length of time.
    #[error("timeout writing to upstream ingester")]
    Timeout(tokio::time::error::Elapsed),

    /// There are no healthy ingesters to route a write to.
    #[error("no healthy upstream ingesters available")]
    NoUpstreams,

    /// The upstream connection is not established.
    #[error("upstream {0} is not connected")]
    UpstreamNotConnected(String),

    /// A delete request was rejected (not supported).
    #[error("deletes are not supported")]
    DeletesUnsupported,
}

/// An [`RpcWrite`] handler submits a write directly to an Ingester via the
/// [gRPC write service].
///
/// Requests are sent to an arbitrary downstream Ingester, and request load is
/// distributed approximately uniformly across all downstream Ingesters. There
/// is no effort made to enforce or attempt data locality.
///
/// # Deletes
///
/// This handler drops delete requests, logging the attempt and returning an
/// error to the client.
///
/// [gRPC write service]: client::WriteClient
#[derive(Debug)]
pub struct RpcWrite<T, C = CircuitBreaker> {
    endpoints: Balancer<T, C>,
}

impl<T> RpcWrite<T> {
    /// Initialise a new [`RpcWrite`] that sends requests to an arbitrary
    /// downstream Ingester, using a round-robin strategy.
    pub fn new<N>(endpoints: impl IntoIterator<Item = (T, N)>, metrics: &metric::Registry) -> Self
    where
        T: Send + Sync + Debug + 'static,
        N: Into<Arc<str>>,
    {
        Self {
            endpoints: Balancer::new(
                endpoints
                    .into_iter()
                    .map(|(client, name)| CircuitBreakingClient::new(client, name.into())),
                Some(metrics),
            ),
        }
    }
}

#[async_trait]
impl<T, C> DmlHandler for RpcWrite<T, C>
where
    T: WriteClient + 'static,
    C: CircuitBreakerState + 'static,
{
    type WriteInput = Partitioned<HashMap<TableId, (String, MutableBatch)>>;
    type WriteOutput = Vec<DmlMeta>;

    type WriteError = RpcWriteError;
    type DeleteError = RpcWriteError;

    async fn write(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_id: NamespaceId,
        writes: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, RpcWriteError> {
        // Extract the partition key & DML writes.
        let (partition_key, writes) = writes.into_parts();

        // Drop the table names from the value tuple.
        let writes = writes
            .into_iter()
            .map(|(id, (_name, data))| (id, data))
            .collect();

        // Build the DmlWrite
        let op = DmlWrite::new(
            namespace_id,
            writes,
            partition_key.clone(),
            DmlMeta::unsequenced(span_ctx.clone()),
        );

        // Serialise this write into the wire format.
        let req = WriteRequest {
            payload: Some(encode_write(namespace_id.get(), &op)),
        };

        // Perform the gRPC write to an ingester.
        //
        // This call is bounded to at most RPC_TIMEOUT duration of time.
        write_loop(
            self.endpoints
                .endpoints()
                .ok_or(RpcWriteError::NoUpstreams)?,
            req,
        )
        .await?;

        debug!(
            %partition_key,
            table_count=op.table_count(),
            %namespace,
            %namespace_id,
            approx_size=%op.size(),
            "dispatched write to ingester"
        );

        Ok(vec![op.meta().clone()])
    }

    async fn delete(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_id: NamespaceId,
        table_name: &str,
        _predicate: &DeletePredicate,
        _span_ctx: Option<SpanContext>,
    ) -> Result<(), RpcWriteError> {
        warn!(
            %namespace,
            %namespace_id,
            %table_name,
            "dropping delete request"
        );

        Err(RpcWriteError::DeletesUnsupported)
    }
}

/// Perform an RPC write with `req` against one of the upstream ingesters in
/// `endpoints`.
///
/// This write attempt is bounded in time to at most [`RPC_TIMEOUT`].
///
/// If at least one upstream request has failed (returning an error), the most
/// recent error is returned.
async fn write_loop<T>(
    mut endpoints: UpstreamSnapshot<'_, T>,
    req: WriteRequest,
) -> Result<(), RpcWriteError>
where
    T: WriteClient,
{
    // The last error returned from an upstream write request attempt.
    let mut last_err = None;

    tokio::time::timeout(RPC_TIMEOUT, async {
        // Infinitely cycle through the snapshot, trying each node in turn until the
        // request succeeds or this async call times out.
        let mut delay = Duration::from_millis(50);
        loop {
            match endpoints
                .next()
                .ok_or(RpcWriteError::NoUpstreams)?
                .write(req.clone())
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    warn!(error=%e, "failed ingester rpc write");
                    last_err = Some(e);
                }
            };
            tokio::time::sleep(delay).await;
            delay = delay.saturating_mul(2);
        }
    })
    .await
    .map_err(|e| match last_err {
        // This error is an internal implementation detail - the meaningful
        // error for the user is "there's no healthy upstreams".
        Some(RpcWriteError::UpstreamNotConnected(_)) => RpcWriteError::NoUpstreams,
        // Any other error is returned as-is.
        Some(v) => v,
        // If the entire write attempt fails during the first RPC write
        // request, then the per-request timeout is greater than the write
        // attempt timeout, and therefore only one upstream is ever tried.
        //
        // Log a warning so the logs show the timeout, but also include
        // helpful hint for the user to adjust the configuration.
        None => {
            warn!(
                "failed ingester rpc write - rpc write request timed out during \
                 the first rpc attempt; consider decreasing rpc request timeout \
                 below {RPC_TIMEOUT:?}"
            );
            RpcWriteError::Timeout(e)
        }
    })?
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, iter, sync::Arc};

    use assert_matches::assert_matches;
    use data_types::PartitionKey;

    use crate::dml_handlers::rpc_write::circuit_breaking_client::mock::MockCircuitBreaker;

    use super::{client::mock::MockWriteClient, *};

    // Parse `lp` into a table-keyed MutableBatch map.
    pub(crate) fn lp_to_writes(lp: &str) -> HashMap<TableId, (String, MutableBatch)> {
        let (writes, _) = mutable_batch_lp::lines_to_batches_stats(lp, 42)
            .expect("failed to build test writes from LP");

        writes
            .into_iter()
            .enumerate()
            .map(|(i, (name, data))| (TableId::new(i as _), (name, data)))
            .collect()
    }

    const NAMESPACE_NAME: &str = "bananas";
    const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);

    #[tokio::test]
    async fn test_write() {
        let batches = lp_to_writes(
            "\
                bananas,tag1=A,tag2=B val=42i 1\n\
                platanos,tag1=A,tag2=B value=42i 2\n\
                another,tag1=A,tag2=B value=42i 3\n\
                bananas,tag1=A,tag2=B val=42i 2\n\
                table,tag1=A,tag2=B val=42i 1\n\
            ",
        );

        // Wrap the table batches in a partition key
        let input = Partitioned::new(PartitionKey::from("2022-01-01"), batches.clone());

        // Init the write handler with a mock client to capture the rpc calls.
        let client = Arc::new(MockWriteClient::default());
        let handler = RpcWrite::new(
            [(Arc::clone(&client), "mock client")],
            &metric::Registry::default(),
        );

        // Drive the RPC writer
        let got = handler
            .write(
                &NamespaceName::new(NAMESPACE_NAME).unwrap(),
                NAMESPACE_ID,
                input,
                None,
            )
            .await;
        assert_matches!(got, Ok(_));

        // Inspect the resulting RPC call
        let call = {
            let mut calls = client.calls();
            assert_eq!(calls.len(), 1);
            calls.pop().unwrap()
        };

        let payload = assert_matches!(call.payload, Some(p) => p);
        assert_eq!(payload.database_id, NAMESPACE_ID.get());
        assert_eq!(payload.partition_key, "2022-01-01");
        assert_eq!(payload.table_batches.len(), 4);

        let got_tables = payload
            .table_batches
            .into_iter()
            .map(|t| t.table_id)
            .collect::<HashSet<_>>();

        let want_tables = batches
            .into_iter()
            .map(|(id, (_name, _data))| id.get())
            .collect::<HashSet<_>>();

        assert_eq!(got_tables, want_tables);
    }

    /// Ensure all candidates returned by the balancer are tried, aborting after
    /// the first successful request.
    #[tokio::test]
    async fn test_write_tries_all_candidates() {
        let batches = lp_to_writes("bananas,tag1=A,tag2=B val=42i 1");

        // Wrap the table batches in a partition key
        let input = Partitioned::new(PartitionKey::from("2022-01-01"), batches.clone());

        // Init the write handler with a mock client to capture the rpc calls.
        let client1 = Arc::new(MockWriteClient::default().with_ret(Box::new(iter::once(Err(
            RpcWriteError::Upstream(tonic::Status::internal("")),
        )))));
        let client2 = Arc::new(MockWriteClient::default());
        let client3 = Arc::new(MockWriteClient::default());
        let handler = RpcWrite::new(
            [
                (Arc::clone(&client1), "client1"),
                (Arc::clone(&client2), "client2"),
                (Arc::clone(&client3), "client3"),
            ],
            &metric::Registry::default(),
        );

        // Drive the RPC writer
        let got = handler
            .write(
                &NamespaceName::new(NAMESPACE_NAME).unwrap(),
                NAMESPACE_ID,
                input,
                None,
            )
            .await;
        assert_matches!(got, Ok(_));

        // Ensure client 2 observed a write.
        let call = {
            let mut calls = client2.calls();
            assert_eq!(calls.len(), 1);
            calls.pop().unwrap()
        };

        // Ensure client 3 was not called.
        assert!(client3.calls().is_empty());

        let payload = assert_matches!(call.payload, Some(p) => p);
        assert_eq!(payload.database_id, NAMESPACE_ID.get());
        assert_eq!(payload.partition_key, "2022-01-01");
        assert_eq!(payload.table_batches.len(), 1);

        let got_tables = payload
            .table_batches
            .into_iter()
            .map(|t| t.table_id)
            .collect::<HashSet<_>>();

        let want_tables = batches
            .into_iter()
            .map(|(id, (_name, _data))| id.get())
            .collect::<HashSet<_>>();

        assert_eq!(got_tables, want_tables);
    }

    /// Ensure all candidates are tried more than once should they first fail.
    #[tokio::test]
    async fn test_write_retries() {
        let batches = lp_to_writes("bananas,tag1=A,tag2=B val=42i 1");

        // Wrap the table batches in a partition key
        let input = Partitioned::new(PartitionKey::from("2022-01-01"), batches.clone());

        // The first client in line fails the first request, but will succeed
        // the second try.
        let client1 = Arc::new(
            MockWriteClient::default().with_ret(Box::new(
                [
                    Err(RpcWriteError::Upstream(tonic::Status::internal(""))),
                    Ok(()),
                ]
                .into_iter(),
            )),
        );
        // This client always errors.
        let client2 = Arc::new(
            MockWriteClient::default().with_ret(Box::new(iter::repeat_with(|| {
                Err(RpcWriteError::Upstream(tonic::Status::internal("")))
            }))),
        );

        let handler = RpcWrite::new(
            [
                (Arc::clone(&client1), "client1"),
                (Arc::clone(&client2), "client2"),
            ],
            &metric::Registry::default(),
        );

        // Drive the RPC writer
        let got = handler
            .write(
                &NamespaceName::new(NAMESPACE_NAME).unwrap(),
                NAMESPACE_ID,
                input,
                None,
            )
            .await;
        assert_matches!(got, Ok(_));

        // Ensure client 1 observed both calls.
        let call = {
            let mut calls = client1.calls();
            assert_eq!(calls.len(), 2);
            calls.pop().unwrap()
        };

        let payload = assert_matches!(call.payload, Some(p) => p);
        assert_eq!(payload.database_id, NAMESPACE_ID.get());
        assert_eq!(payload.partition_key, "2022-01-01");
        assert_eq!(payload.table_batches.len(), 1);

        let got_tables = payload
            .table_batches
            .into_iter()
            .map(|t| t.table_id)
            .collect::<HashSet<_>>();

        let want_tables = batches
            .into_iter()
            .map(|(id, (_name, _data))| id.get())
            .collect::<HashSet<_>>();

        assert_eq!(got_tables, want_tables);
    }

    async fn make_request<T, C>(
        endpoints: impl IntoIterator<Item = CircuitBreakingClient<T, C>> + Send,
    ) -> Result<Vec<DmlMeta>, RpcWriteError>
    where
        T: WriteClient + 'static,
        C: CircuitBreakerState + 'static,
    {
        let handler = RpcWrite {
            endpoints: Balancer::new(endpoints, None),
        };

        // Generate some write input
        let input = Partitioned::new(
            PartitionKey::from("2022-01-01"),
            lp_to_writes("bananas,tag1=A,tag2=B val=42i 1"),
        );

        // Use tokio's "auto-advance" time feature to avoid waiting for the
        // actual timeout duration.
        tokio::time::pause();

        // Drive the RPC writer
        handler
            .write(
                &NamespaceName::new(NAMESPACE_NAME).unwrap(),
                NAMESPACE_ID,
                input,
                None,
            )
            .await
    }

    /// Assert the error response for a write request when there are no healthy
    /// upstreams.
    #[tokio::test]
    async fn test_write_no_healthy_upstreams() {
        let client_1 = Arc::new(MockWriteClient::default());
        let circuit_1 = Arc::new(MockCircuitBreaker::default());

        // Mark the client circuit breaker as unhealthy
        circuit_1.set_healthy(false);

        let got = make_request([
            CircuitBreakingClient::new(client_1, "client_1").with_circuit_breaker(circuit_1)
        ])
        .await;

        assert_matches!(got, Err(RpcWriteError::NoUpstreams));
    }

    /// Assert the error response when the only upstream continuously returns an
    /// error.
    #[tokio::test]
    async fn test_write_upstream_error() {
        let client_1 = Arc::new(
            MockWriteClient::default().with_ret(Box::new(iter::repeat_with(|| {
                Err(RpcWriteError::Upstream(tonic::Status::internal("bananas")))
            }))),
        );
        let circuit_1 = Arc::new(MockCircuitBreaker::default());
        circuit_1.set_healthy(true);

        let got = make_request([
            CircuitBreakingClient::new(client_1, "client_1").with_circuit_breaker(circuit_1)
        ])
        .await;

        assert_matches!(got, Err(RpcWriteError::Upstream(s)) => {
            assert_eq!(s.code(), tonic::Code::Internal);
            assert_eq!(s.message(), "bananas");
        });
    }

    /// Assert that an [`RpcWriteError::UpstreamNotConnected`] error is mapped
    /// to a user-friendly [`RpcWriteError::NoUpstreams`] for consistency.
    #[tokio::test]
    async fn test_write_map_upstream_not_connected_error() {
        let client_1 = Arc::new(
            MockWriteClient::default().with_ret(Box::new(iter::repeat_with(|| {
                Err(RpcWriteError::UpstreamNotConnected("bananas".to_string()))
            }))),
        );
        let circuit_1 = Arc::new(MockCircuitBreaker::default());
        circuit_1.set_healthy(true);

        let got = make_request([
            CircuitBreakingClient::new(client_1, "client_1").with_circuit_breaker(circuit_1)
        ])
        .await;

        assert_matches!(got, Err(RpcWriteError::NoUpstreams));
    }
}
