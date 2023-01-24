use std::{borrow::Cow, cell::RefCell, cmp::max, fmt::Debug, sync::Arc, time::Duration};

use futures::Future;
use metric::U64Gauge;
use observability_deps::tracing::warn;
use tokio::task::JoinHandle;

use super::{
    circuit_breaker::CircuitBreaker,
    circuit_breaking_client::{CircuitBreakerState, CircuitBreakingClient},
};

thread_local! {
    /// A per-thread counter incremented once per call to
    /// [`Balancer::endpoints()`].
    static COUNTER: RefCell<usize> = RefCell::new(0);
}

/// How often to re-evaluate the health of the [`Balancer`] endpoints for
/// metrics / logging.
const METRIC_EVAL_INTERVAL: Duration = Duration::from_secs(3);

/// A set of health-checked gRPC endpoints, with an approximate round-robin
/// distribution of load over healthy nodes.
///
/// # Health Checking
///
/// The health evaluation of a node is delegated to the
/// [`CircuitBreakingClient`].
///
/// # Request Distribution
///
/// Requests are distributed uniformly across all shards **per thread**. Given
/// enough requests (where `N` is significantly larger than the number of
/// threads) an approximately uniform distribution is achieved.
#[derive(Debug)]
pub(super) struct Balancer<T, C = CircuitBreaker> {
    endpoints: Arc<[CircuitBreakingClient<T, C>]>,

    /// An optional metric exporter task that evaluates the state of this
    /// [`Balancer`] every [`METRIC_EVAL_INTERVAL`].
    metric_task: Option<JoinHandle<()>>,
}

impl<T, C> Balancer<T, C>
where
    T: Send + Sync + Debug + 'static,
    C: CircuitBreakerState + 'static,
{
    /// Construct a new [`Balancer`] distributing work over the healthy
    /// `endpoints`.
    pub(super) fn new(
        endpoints: impl IntoIterator<Item = CircuitBreakingClient<T, C>>,
        metrics: Option<&metric::Registry>,
    ) -> Self {
        let endpoints = endpoints.into_iter().collect();
        Self {
            metric_task: metrics.map(|m| tokio::spawn(metric_task(m, Arc::clone(&endpoints)))),
            endpoints,
        }
    }

    /// Return an (infinite) iterator of healthy [`CircuitBreakingClient`].
    ///
    /// A snapshot of healthy nodes is taken at call time and the health state
    /// is evaluated at this point and the result is returned to the caller as
    /// an infinite / cycling iterator. A node that becomes unavailable after
    /// the snapshot was taken will continue to be returned by the iterator.
    pub(super) fn endpoints(&self) -> impl Iterator<Item = &'_ CircuitBreakingClient<T, C>> {
        // Grab and increment the current counter.
        let counter = COUNTER.with(|cell| {
            let mut cell = cell.borrow_mut();
            let new_value = cell.wrapping_add(1);
            *cell = new_value;
            new_value
        });

        // Take a snapshot containing only healthy nodes.
        //
        // This ensures unhealthy nodes are not continuously (and unnecessarily)
        // polled/probed in the iter cycle below. The low frequency and impact
        // of a node becoming unavailable during a single request easily
        // outweighs the trade-off of the constant health evaluation overhead.
        let snapshot = self
            .endpoints
            .iter()
            .filter(|e| e.is_usable())
            .collect::<Vec<_>>();

        // Reduce it to the range of [0, N) where N is the number of healthy
        // clients in this balancer, ensuring not to calculate the remainder of
        // a division by 0.
        let idx = counter % max(snapshot.len(), 1);

        snapshot.into_iter().cycle().skip(idx)
    }
}

/// Initialise the health metric exported by the RPC balancer, and return the
/// health evaluation future that updates it.
fn metric_task<T, C>(
    metrics: &metric::Registry,
    endpoints: Arc<[CircuitBreakingClient<T, C>]>,
) -> impl Future<Output = ()> + Send
where
    T: Send + Sync + 'static,
    C: CircuitBreakerState + 'static,
{
    let metric = metrics.register_metric::<U64Gauge>(
        "rpc_balancer_endpoints_healthy",
        "1 when the upstream is healthy, 0 otherwise",
    );

    metric_loop(metric, endpoints)
}

async fn metric_loop<T, C>(
    metric: metric::Metric<U64Gauge>,
    endpoints: Arc<[CircuitBreakingClient<T, C>]>,
) where
    T: Send + Sync + 'static,
    C: CircuitBreakerState + 'static,
{
    // Map the endpoints into an endpoint and a metric.
    let endpoints = endpoints
        .iter()
        .map(|c| {
            let name = Cow::from(c.endpoint_name().to_string());
            let metric = metric.recorder([("endpoint", name)]);
            (c, metric)
        })
        .collect::<Vec<_>>();

    // Periodically re-evaluate the health state of the balancer's endpoints.
    let mut tick = tokio::time::interval(METRIC_EVAL_INTERVAL);

    // And track the healthy / unhealthy endpoint names for logging context.
    let mut healthy = vec![];
    let mut unhealthy = vec![];
    loop {
        healthy.clear();
        unhealthy.clear();
        tick.tick().await;

        for (client, metric) in &endpoints {
            let value = match client.is_usable() {
                true => {
                    healthy.push(client.endpoint_name());
                    1
                }
                false => {
                    unhealthy.push(client.endpoint_name());
                    0
                }
            };
            metric.set(value);
        }

        // Emit a log entry if at least one endpoint is unavailable.
        if !unhealthy.is_empty() {
            warn!(
                healthy = %healthy.join(","),
                unhealthy = %unhealthy.join(","),
                "upstream rpc endpoint(s) are unavailable"
            );
        }
    }
}

impl<T, C> Drop for Balancer<T, C> {
    fn drop(&mut self) {
        if let Some(t) = self.metric_task.take() {
            t.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use generated_types::influxdata::iox::ingester::v1::WriteRequest;
    use metric::{Attributes, Metric};
    use test_helpers::timeout::FutureTimeout;

    use crate::dml_handlers::rpc_write::{
        circuit_breaking_client::mock::MockCircuitBreaker,
        client::{mock::MockWriteClient, WriteClient},
    };

    use super::*;

    /// No healthy nodes yields an empty iterator.
    #[tokio::test]
    async fn test_balancer_empty_iter() {
        const BALANCER_CALLS: usize = 10;

        // Initialise 3 RPC clients and configure their mock circuit breakers;
        // two returns a unhealthy state, one is healthy.
        let circuit_err_1 = Arc::new(MockCircuitBreaker::default());
        circuit_err_1.set_usable(false);
        let client_err_1 =
            CircuitBreakingClient::new(Arc::new(MockWriteClient::default()), "bananas")
                .with_circuit_breaker(Arc::clone(&circuit_err_1));

        let circuit_err_2 = Arc::new(MockCircuitBreaker::default());
        circuit_err_2.set_usable(false);
        let client_err_2 =
            CircuitBreakingClient::new(Arc::new(MockWriteClient::default()), "bananas")
                .with_circuit_breaker(Arc::clone(&circuit_err_2));

        assert_eq!(circuit_err_1.ok_count(), 0);
        assert_eq!(circuit_err_2.ok_count(), 0);

        let balancer = Balancer::new([client_err_1, client_err_2], None);
        let mut endpoints = balancer.endpoints();

        assert_matches!(endpoints.next(), None);
    }

    /// A test that ensures only healthy clients are returned by the balancer,
    /// and that they are polled exactly once per request.
    #[tokio::test]
    async fn test_balancer_yield_healthy_polled_once() {
        const BALANCER_CALLS: usize = 10;

        // Initialise 3 RPC clients and configure their mock circuit breakers;
        // two returns a unhealthy state, one is healthy.
        let circuit_err_1 = Arc::new(MockCircuitBreaker::default());
        circuit_err_1.set_usable(false);
        let client_err_1 =
            CircuitBreakingClient::new(Arc::new(MockWriteClient::default()), "bananas")
                .with_circuit_breaker(Arc::clone(&circuit_err_1));

        let circuit_err_2 = Arc::new(MockCircuitBreaker::default());
        circuit_err_2.set_usable(false);
        let client_err_2 =
            CircuitBreakingClient::new(Arc::new(MockWriteClient::default()), "bananas")
                .with_circuit_breaker(Arc::clone(&circuit_err_2));

        let circuit_ok = Arc::new(MockCircuitBreaker::default());
        circuit_ok.set_usable(true);
        let client_ok = CircuitBreakingClient::new(Arc::new(MockWriteClient::default()), "bananas")
            .with_circuit_breaker(Arc::clone(&circuit_ok));

        assert_eq!(circuit_ok.ok_count(), 0);
        assert_eq!(circuit_err_1.ok_count(), 0);
        assert_eq!(circuit_err_2.ok_count(), 0);

        let balancer = Balancer::new([client_err_1, client_ok, client_err_2], None);
        let mut endpoints = balancer.endpoints();

        // Only the health client should be yielded, and it should cycle
        // indefinitely.
        for i in 1..=BALANCER_CALLS {
            endpoints
                .next()
                .expect("should yield healthy client")
                .write(WriteRequest::default())
                .await
                .expect("should succeed");

            assert_eq!(circuit_ok.ok_count(), i);
            assert_eq!(circuit_ok.err_count(), 0);
        }

        // There health of the endpoints should not be constantly re-evaluated
        // by a single request (reducing overhead / hot spinning - in the
        // probing phase this would serialise clients).
        assert_eq!(circuit_ok.is_usable_count(), 1);
        assert_eq!(circuit_err_1.is_usable_count(), 1);
        assert_eq!(circuit_err_1.is_usable_count(), 1);

        // The other clients should not have been invoked.
        assert_eq!(circuit_err_1.ok_count(), 0);
        assert_eq!(circuit_err_1.err_count(), 0);

        assert_eq!(circuit_err_2.ok_count(), 0);
        assert_eq!(circuit_err_2.err_count(), 0);
    }

    /// An unhealthy node that recovers is yielded to the caller.
    #[tokio::test]
    async fn test_balancer_upstream_recovery() {
        const BALANCER_CALLS: usize = 10;

        // Initialise 3 RPC clients and configure their mock circuit breakers;
        // two returns a unhealthy state, one is healthy.
        let circuit = Arc::new(MockCircuitBreaker::default());
        circuit.set_usable(false);
        let client = CircuitBreakingClient::new(Arc::new(MockWriteClient::default()), "broken")
            .with_circuit_breaker(Arc::clone(&circuit));

        assert_eq!(circuit.ok_count(), 0);

        let balancer = Balancer::new([client], None);

        let mut endpoints = balancer.endpoints();
        assert_matches!(endpoints.next(), None);
        assert_eq!(circuit.is_usable_count(), 1);

        circuit.set_usable(true);

        let mut endpoints = balancer.endpoints();
        assert_matches!(endpoints.next(), Some(_));
        assert_eq!(circuit.is_usable_count(), 2);

        // The now-healthy client is constantly yielded.
        const N: usize = 3;
        for _ in 0..N {
            endpoints
                .next()
                .expect("should yield healthy client")
                .write(WriteRequest::default())
                .await
                .expect("should succeed");
        }
        assert_eq!(circuit.ok_count(), N);
    }

    // Ensure the balancer round-robins across all healthy clients.
    //
    // Note this is a property test that asserts the even distribution of the
    // client calls, not the order themselves.
    #[tokio::test]
    async fn test_round_robin() {
        const N: usize = 100;
        #[allow(clippy::assertions_on_constants)]
        {
            assert!(N % 2 == 0, "test iterations must be even");
        }

        // Initialise 3 RPC clients and configure their mock circuit breakers;
        // two returns a healthy state, one is unhealthy.
        let circuit_err = Arc::new(MockCircuitBreaker::default());
        circuit_err.set_usable(false);
        let client_err =
            CircuitBreakingClient::new(Arc::new(MockWriteClient::default()), "bananas")
                .with_circuit_breaker(Arc::clone(&circuit_err));

        let circuit_ok_1 = Arc::new(MockCircuitBreaker::default());
        circuit_ok_1.set_usable(true);
        let client_ok_1 =
            CircuitBreakingClient::new(Arc::new(MockWriteClient::default()), "bananas")
                .with_circuit_breaker(Arc::clone(&circuit_ok_1));

        let circuit_ok_2 = Arc::new(MockCircuitBreaker::default());
        circuit_ok_2.set_usable(true);
        let client_ok_2 =
            CircuitBreakingClient::new(Arc::new(MockWriteClient::default()), "bananas")
                .with_circuit_breaker(Arc::clone(&circuit_ok_2));

        let balancer = Balancer::new([client_err, client_ok_1, client_ok_2], None);

        for _ in 0..N {
            balancer
                .endpoints()
                .next()
                .expect("should yield healthy client")
                .write(WriteRequest::default())
                .await
                .expect("should succeed");
        }

        assert_eq!(circuit_err.ok_count(), 0);
        assert_eq!(circuit_ok_1.ok_count(), N / 2);
        assert_eq!(circuit_ok_2.ok_count(), N / 2);

        assert_eq!(circuit_err.err_count(), 0);
        assert_eq!(circuit_ok_1.err_count(), 0);
        assert_eq!(circuit_ok_2.err_count(), 0);
    }

    // Ensure the metric task exports the correct "healthy" values.
    #[tokio::test]
    async fn test_metric_exporter() {
        // Initialise 3 RPC clients and configure their mock circuit breakers;
        // two returns a healthy state, one is unhealthy.
        let circuit_err = Arc::new(MockCircuitBreaker::default());
        circuit_err.set_usable(false);
        let client_err =
            CircuitBreakingClient::new(Arc::new(MockWriteClient::default()), "bad-client")
                .with_circuit_breaker(Arc::clone(&circuit_err));

        let circuit_ok_1 = Arc::new(MockCircuitBreaker::default());
        circuit_ok_1.set_usable(true);
        let client_ok_1 =
            CircuitBreakingClient::new(Arc::new(MockWriteClient::default()), "ok-client-1")
                .with_circuit_breaker(Arc::clone(&circuit_ok_1));

        let circuit_ok_2 = Arc::new(MockCircuitBreaker::default());
        circuit_ok_2.set_usable(true);
        let client_ok_2 =
            CircuitBreakingClient::new(Arc::new(MockWriteClient::default()), "ok-client-2")
                .with_circuit_breaker(Arc::clone(&circuit_ok_2));

        let balancer = Balancer::new([client_err, client_ok_1, client_ok_2], None);

        let metrics = metric::Registry::default();
        let worker = tokio::spawn(metric_task(&metrics, Arc::clone(&balancer.endpoints)));

        // Wait for the first state to converge to the expected value, or time
        // out if it's never observed.

        fn get_health_state(metrics: &metric::Registry, client: &'static str) -> Option<u64> {
            Some(
                metrics
                    .get_instrument::<Metric<U64Gauge>>("rpc_balancer_endpoints_healthy")
                    .expect("failed to read metric")
                    .get_observer(&Attributes::from(&[("endpoint", client)]))?
                    .fetch(),
            )
        }

        async {
            loop {
                tokio::time::sleep(Duration::from_millis(50)).await;
                if !matches!(get_health_state(&metrics, "ok-client-1"), Some(1)) {
                    continue;
                }
                if !matches!(get_health_state(&metrics, "ok-client-2"), Some(1)) {
                    continue;
                }
                if !matches!(get_health_state(&metrics, "bad-client"), Some(0)) {
                    continue;
                }

                break;
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        // The eval above observed the correct metric state.

        worker.abort();
    }
}
