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
use data_types::{NamespaceId, NamespaceName, TableId};
use dml::{DmlMeta, DmlWrite};
use generated_types::influxdata::iox::ingester::v1::WriteRequest;
use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use mutable_batch_pb::encode::encode_write;
use observability_deps::tracing::*;
use std::{fmt::Debug, num::NonZeroUsize, sync::Arc, time::Duration};
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

    /// The write request was not attempted, because not enough upstream
    /// ingesters needed to satisfy the configured replication factor are
    /// healthy.
    #[error("not enough upstreams to satisfy write replication")]
    NotEnoughReplicas,

    /// A replicated write was attempted but not enough upstream ingesters
    /// acknowledged the write to satisfy the desired replication factor.
    #[error(
        "not enough upstreams accepted replicated write; \
        want {want_n_copies}, but only received {acks} acks"
    )]
    PartialWrite {
        /// The total number of copies of data needed for this write to be
        /// considered sucessful.
        want_n_copies: usize,
        /// The number of successful upstream ingester writes.
        acks: usize,
    },
}

/// An [`RpcWrite`] handler submits a write directly to an Ingester via the
/// [gRPC write service].
///
/// Requests are sent to an arbitrary downstream Ingester, and request load is
/// distributed approximately uniformly across all downstream Ingesters. There
/// is no effort made to enforce or attempt data locality.
///
/// # Replication
///
/// If replication is configured, the total number of upstream ingesters
/// that acknowledge the write must be `replica_copies + 1` for the write to
/// be considered successful.
///
/// A write that is accepted by some upstream ingesters, but not enough to
/// satisfy the desired replication factor will be considered a partial failure,
/// and a [`RpcWriteError::PartialWrite`] error is returned.
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

    /// The total number of distinct upstream ingesters to write a single
    /// request to.
    ///
    /// Invariant: after an upstream has ACKed a write, it MUST NOT be sent the
    /// same write again. This minimises duplication within the same upstream,
    /// for which no additional durability is realised.
    ///
    /// NOTE: it is NOT possible to eliminate duplication of write requests
    /// being sent to the same upstream, as an upstream (or a middle-man proxy)
    /// may NACK a write, having already buffered the data. When this request is
    /// retried, the data will be duplicated.
    n_copies: usize,
}

impl<T> RpcWrite<T> {
    /// Initialise a new [`RpcWrite`] that sends requests to an arbitrary
    /// downstream Ingester, using a round-robin strategy.
    ///
    /// If [`Some`], `replica_copies` specifies the number of additional
    /// upstream ingesters that must receive and acknowledge the write for it to
    /// be considered successful.
    ///
    /// # Panics
    ///
    /// It's invalid to configure `replica_copies` such that more ACKs are
    /// needed than the number of `endpoints`; doing so will cause a panic.
    pub fn new<N>(
        endpoints: impl IntoIterator<Item = (T, N)>,
        replica_copies: Option<NonZeroUsize>,
        metrics: &metric::Registry,
    ) -> Self
    where
        T: Send + Sync + Debug + 'static,
        N: Into<Arc<str>>,
    {
        let endpoints = Balancer::new(
            endpoints
                .into_iter()
                .map(|(client, name)| CircuitBreakingClient::new(client, name.into())),
            Some(metrics),
        );

        // Map the "replication factor" into the total number of distinct data
        // copies necessary to consider a write a success.
        let n_copies = replica_copies.map(NonZeroUsize::get).unwrap_or(1);

        debug!(n_copies, "write replication factor");

        // Assert this configuration is not impossible to satisfy.
        assert!(
            n_copies <= endpoints.len(),
            "cannot configure more write copies ({n_copies}) than ingester \
            endpoints ({count})",
            count = endpoints.len(),
        );

        Self {
            endpoints,
            n_copies,
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

        // Obtain a snapshot of currently-healthy upstreams (and potentially
        // some that need probing)
        let mut snap = self
            .endpoints
            .endpoints()
            .ok_or(RpcWriteError::NoUpstreams)?;

        // Validate the required number of writes is possible given the current
        // number of healthy endpoints.
        if snap.len() < self.n_copies {
            return Err(RpcWriteError::NotEnoughReplicas);
        }

        // Write the desired number of copies of `req`.
        for i in 0..self.n_copies {
            // Perform the gRPC write to an ingester.
            //
            // This call is bounded to at most RPC_TIMEOUT duration of time.
            write_loop(&mut snap, &req).await.map_err(|e| {
                // In all cases, if at least one write succeeded, then this
                // becomes a partial write error.
                if i > 0 {
                    return RpcWriteError::PartialWrite {
                        want_n_copies: self.n_copies,
                        acks: i,
                    };
                }

                // This error was for the first request - there have been no
                // ACKs received.
                match e {
                    // This error is an internal implementation detail - the
                    // meaningful error for the user is "there's no healthy
                    // upstreams".
                    RpcWriteError::UpstreamNotConnected(_) => RpcWriteError::NoUpstreams,
                    // The number of upstreams no longer satisfies the desired
                    // replication factor.
                    RpcWriteError::NoUpstreams => RpcWriteError::NotEnoughReplicas,
                    // All other errors pass through.
                    v => v,
                }
            })?;
            // Remove the upstream that was successfully wrote to from the
            // candidates
            snap.remove_last_unstable();
        }

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
}

/// Perform an RPC write with `req` against one of the upstream ingesters in
/// `endpoints`.
///
/// This write attempt is bounded in time to at most [`RPC_TIMEOUT`].
///
/// If at least one upstream request has failed (returning an error), the most
/// recent error is returned.
async fn write_loop<T>(
    endpoints: &mut UpstreamSnapshot<'_, T>,
    req: &WriteRequest,
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
    use rand::seq::SliceRandom;

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

    /// A helper function to perform an arbitrary write against `endpoints`,
    /// with the given number of desired distinct data copies.
    async fn make_request<T, C>(
        endpoints: impl IntoIterator<Item = CircuitBreakingClient<T, C>> + Send,
        n_copies: usize,
    ) -> Result<Vec<DmlMeta>, RpcWriteError>
    where
        T: WriteClient + 'static,
        C: CircuitBreakerState + 'static,
    {
        let handler = RpcWrite {
            endpoints: Balancer::new(endpoints, None),
            n_copies,
        };

        assert!(
            n_copies <= handler.endpoints.len(),
            "cannot configure more write copies ({n_copies}) than ingester \
            endpoints ({count})",
            count = handler.endpoints.len(),
        );

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
            None,
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
        let client1 = Arc::new(MockWriteClient::default().with_ret(iter::once(Err(
            RpcWriteError::Upstream(tonic::Status::internal("")),
        ))));
        let client2 = Arc::new(MockWriteClient::default());
        let client3 = Arc::new(MockWriteClient::default());
        let handler = RpcWrite::new(
            [
                (Arc::clone(&client1), "client1"),
                (Arc::clone(&client2), "client2"),
                (Arc::clone(&client3), "client3"),
            ],
            None,
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
        let client1 = Arc::new(MockWriteClient::default().with_ret([
            Err(RpcWriteError::Upstream(tonic::Status::internal(""))),
            Ok(()),
        ]));
        // This client always errors.
        let client2 = Arc::new(MockWriteClient::default().with_ret(iter::repeat_with(|| {
            Err(RpcWriteError::Upstream(tonic::Status::internal("")))
        })));

        let handler = RpcWrite::new(
            [
                (Arc::clone(&client1), "client1"),
                (Arc::clone(&client2), "client2"),
            ],
            None,
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

    /// Assert the error response for a write request when there are no healthy
    /// upstreams.
    #[tokio::test]
    async fn test_write_no_healthy_upstreams() {
        let client_1 = Arc::new(MockWriteClient::default());
        let circuit_1 = Arc::new(MockCircuitBreaker::default());

        // Mark the client circuit breaker as unhealthy
        circuit_1.set_healthy(false);

        let got = make_request(
            [CircuitBreakingClient::new(client_1, "client_1").with_circuit_breaker(circuit_1)],
            1,
        )
        .await;

        assert_matches!(got, Err(RpcWriteError::NoUpstreams));
    }

    /// Assert the error response when the only upstream continuously returns an
    /// error.
    #[tokio::test]
    async fn test_write_upstream_error() {
        let client_1 = Arc::new(MockWriteClient::default().with_ret(iter::repeat_with(|| {
            Err(RpcWriteError::Upstream(tonic::Status::internal("bananas")))
        })));
        let circuit_1 = Arc::new(MockCircuitBreaker::default());
        circuit_1.set_healthy(true);

        let got = make_request(
            [CircuitBreakingClient::new(client_1, "client_1").with_circuit_breaker(circuit_1)],
            1,
        )
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
        let client_1 = Arc::new(MockWriteClient::default().with_ret(iter::repeat_with(|| {
            Err(RpcWriteError::UpstreamNotConnected("bananas".to_string()))
        })));
        let circuit_1 = Arc::new(MockCircuitBreaker::default());
        circuit_1.set_healthy(true);

        let got = make_request(
            [CircuitBreakingClient::new(client_1, "client_1").with_circuit_breaker(circuit_1)],
            1,
        )
        .await;

        assert_matches!(got, Err(RpcWriteError::NoUpstreams));
    }

    /// Assert that an error is returned without any RPC request being made when
    /// the number of healthy upstreams is less than the desired replication
    /// factor.
    #[tokio::test]
    async fn test_write_not_enough_upstreams_for_replication() {
        // Initialise two upstreams, 1 healthy, 1 not.
        let client_1 = Arc::new(MockWriteClient::default().with_ret(iter::repeat_with(|| {
            Err(RpcWriteError::UpstreamNotConnected("bananas".to_string()))
        })));
        let circuit_1 = Arc::new(MockCircuitBreaker::default());
        circuit_1.set_healthy(true);

        let client_2 = Arc::new(MockWriteClient::default().with_ret(iter::repeat_with(|| {
            Err(RpcWriteError::UpstreamNotConnected("bananas".to_string()))
        })));
        let circuit_2 = Arc::new(MockCircuitBreaker::default());
        circuit_2.set_healthy(false);

        let got = make_request(
            [
                CircuitBreakingClient::new(client_1, "client_1").with_circuit_breaker(circuit_1),
                CircuitBreakingClient::new(client_2, "client_2").with_circuit_breaker(circuit_2),
            ],
            2, // 2 copies required
        )
        .await;

        assert_matches!(got, Err(RpcWriteError::NotEnoughReplicas));
    }

    /// Assert distinct upstreams are wrote to.
    #[tokio::test]
    async fn test_write_replication_distinct_hosts() {
        // Initialise two upstreams.
        let client_1 = Arc::new(MockWriteClient::default().with_ret(iter::once(Ok(()))));
        let circuit_1 = Arc::new(MockCircuitBreaker::default());
        circuit_1.set_healthy(true);

        let client_2 = Arc::new(MockWriteClient::default().with_ret(iter::once(Ok(()))));
        let circuit_2 = Arc::new(MockCircuitBreaker::default());
        circuit_2.set_healthy(true);

        let got = make_request(
            [
                CircuitBreakingClient::new(Arc::clone(&client_1), "client_1")
                    .with_circuit_breaker(circuit_1),
                CircuitBreakingClient::new(Arc::clone(&client_2), "client_2")
                    .with_circuit_breaker(circuit_2),
            ],
            2, // 2 copies required
        )
        .await;

        assert_matches!(got, Ok(_));

        // Assert each client received one (matching) write each
        let calls_1 = client_1.calls();
        assert_eq!(calls_1.len(), 1);
        assert_eq!(calls_1, client_2.calls());
    }

    /// Assert that once a request has been sent to an upstream, it is never
    /// retried again.
    #[tokio::test]
    async fn test_write_replication_distinct_hosts_partial_write() {
        // Initialise two upstreams, 1 willing to ACK a write, and the other
        // always throwing an error.
        let client_1 = Arc::new(MockWriteClient::default().with_ret(iter::once(Ok(()))));
        let circuit_1 = Arc::new(MockCircuitBreaker::default());
        circuit_1.set_healthy(true);

        let client_2 = Arc::new(MockWriteClient::default().with_ret(iter::repeat_with(|| {
            Err(RpcWriteError::Upstream(tonic::Status::internal("bananas")))
        })));
        let circuit_2 = Arc::new(MockCircuitBreaker::default());
        circuit_2.set_healthy(true);

        let mut clients = vec![
            CircuitBreakingClient::new(Arc::clone(&client_1), "client_1")
                .with_circuit_breaker(circuit_1),
            CircuitBreakingClient::new(Arc::clone(&client_2), "client_2")
                .with_circuit_breaker(circuit_2),
        ];

        // The order should never affect the outcome.
        clients.shuffle(&mut rand::thread_rng());

        let got = make_request(
            clients, 2, // 2 copies required
        )
        .await;

        assert_matches!(
            got,
            Err(RpcWriteError::PartialWrite {
                want_n_copies: 2,
                acks: 1
            })
        );

        // Assert the healthy upstream was only ever called once.
        assert_eq!(client_1.calls().len(), 1);
    }

    /// Replication writes must tolerate a transient error from an upstream.
    #[tokio::test]
    async fn test_write_replication_tolerates_temporary_error() {
        // Initialise two upstreams, 1 willing to ACK a write, and the other
        // always throwing an error.
        let client_1 = Arc::new(MockWriteClient::default().with_ret(iter::once(Ok(()))));
        let circuit_1 = Arc::new(MockCircuitBreaker::default());
        circuit_1.set_healthy(true);

        let client_2 = Arc::new(MockWriteClient::default().with_ret([
            Err(RpcWriteError::Upstream(tonic::Status::internal("bananas"))),
            Ok(()),
        ]));
        let circuit_2 = Arc::new(MockCircuitBreaker::default());
        circuit_2.set_healthy(true);

        let got = make_request(
            [
                CircuitBreakingClient::new(Arc::clone(&client_1), "client_1")
                    .with_circuit_breaker(circuit_1),
                CircuitBreakingClient::new(Arc::clone(&client_2), "client_2")
                    .with_circuit_breaker(circuit_2),
            ],
            2, // 2 copies required
        )
        .await;

        assert_matches!(got, Ok(_));

        // Assert the happy upstream was only ever called once.
        let calls_1 = client_1.calls();
        assert_eq!(calls_1.len(), 1);
        // The unhappy upstream was retried
        let calls_2 = client_2.calls();
        assert_eq!(calls_2.len(), 2);
        // The two upstreams got the same request.
        assert_eq!(calls_1[0], calls_2[0]);
        // And the second request to the unhappy upstream matched the first.
        assert_eq!(calls_2[0], calls_2[1]);
    }

    /// Replication writes find the desired number of replicas by trying all
    /// upstreams.
    #[tokio::test]
    async fn test_write_replication_tolerates_bad_upstream() {
        // Initialise three upstreams, 1 willing to ACK a write immediately, the
        // second will error twice, and the third always errors.
        let client_1 = Arc::new(MockWriteClient::default().with_ret(iter::once(Ok(()))));
        let circuit_1 = Arc::new(MockCircuitBreaker::default());
        circuit_1.set_healthy(true);

        // This client sometimes errors (2 times)
        let client_2 = Arc::new(MockWriteClient::default().with_ret([
            Err(RpcWriteError::Upstream(tonic::Status::internal("bananas"))),
            Err(RpcWriteError::Upstream(tonic::Status::internal("bananas"))),
            Ok(()),
        ]));
        let circuit_2 = Arc::new(MockCircuitBreaker::default());
        circuit_2.set_healthy(true);

        // This client always errors
        let client_3 = Arc::new(MockWriteClient::default().with_ret(iter::repeat_with(|| {
            Err(RpcWriteError::UpstreamNotConnected("bananas".to_string()))
        })));
        let circuit_3 = Arc::new(MockCircuitBreaker::default());
        circuit_3.set_healthy(true);

        let mut clients = vec![
            CircuitBreakingClient::new(Arc::clone(&client_1), "client_1")
                .with_circuit_breaker(circuit_1),
            CircuitBreakingClient::new(Arc::clone(&client_2), "client_2")
                .with_circuit_breaker(circuit_2),
            CircuitBreakingClient::new(Arc::clone(&client_3), "client_3")
                .with_circuit_breaker(circuit_3),
        ];

        // The order should never affect the outcome.
        clients.shuffle(&mut rand::thread_rng());

        let got = make_request(
            clients, 2, // 2 copies required
        )
        .await;

        assert_matches!(got, Ok(_));

        // Assert the happy upstream was only ever called once.
        let calls_1 = client_1.calls();
        assert_eq!(calls_1.len(), 1);

        // The sometimes error upstream was retried until it succeeded.
        let calls_2 = client_2.calls();
        assert_eq!(calls_2.len(), 3);

        // All requests were equal.
        assert!(calls_1
            .iter()
            .chain(calls_2.iter())
            .chain(client_3.calls().iter())
            .all(|v| *v == calls_1[0]));
    }
}
