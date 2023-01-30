use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use generated_types::ingester::IngesterQueryRequest;
use iox_time::{Time, TimeProvider};
use metric::{Metric, Registry, U64Gauge};
use observability_deps::tracing::{info, warn};
use parking_lot::Mutex;
use pin_project::{pin_project, pinned_drop};
use rand::rngs::mock::StepRng;
use trace::ctx::SpanContext;

use crate::ingester::flight_client::{Error as FlightClientError, IngesterFlightClient, QueryData};

/// Wrapper around a [`Future`] that signals if the future was cancelled or not.
#[pin_project(PinnedDrop)]
struct TrackedFuture<F> {
    #[pin]
    inner: F,
    done: bool,
    /// If false, the future was dropped before resolving to `ready`
    not_cancelled: Arc<AtomicBool>,
}

impl<F> TrackedFuture<F> {
    /// Create new tracked future.
    ///
    /// # Panic
    /// The `not_cancelled` MUST be set to `true`.
    fn new(f: F, not_cancelled: Arc<AtomicBool>) -> Self {
        assert!(not_cancelled.load(Ordering::SeqCst));

        Self {
            inner: f,
            done: false,
            not_cancelled,
        }
    }
}

impl<F> Future for TrackedFuture<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        assert!(!*this.done);
        let res = this.inner.poll(cx);
        *this.done = res.is_ready();
        res
    }
}

#[pinned_drop]
impl<F> PinnedDrop for TrackedFuture<F> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();

        if !*this.done {
            this.not_cancelled.store(false, Ordering::SeqCst);
        }
    }
}

/// Metrics for a specific circuit.
#[derive(Debug, Clone)]
struct CircuitMetrics {
    open: U64Gauge,
    closed: U64Gauge,
    half_open: U64Gauge,
}

impl CircuitMetrics {
    fn new(metric_registry: &Registry, ingester_addr: &str) -> Self {
        let circuit_state: Metric<U64Gauge> = metric_registry.register_metric(
            "ingester_circuit_state",
            "state of the given ingestger connection",
        );

        Self {
            open: circuit_state.recorder([
                ("ingester", Cow::Owned(ingester_addr.to_owned())),
                ("state", Cow::from("open")),
            ]),
            closed: circuit_state.recorder([
                ("ingester", Cow::Owned(ingester_addr.to_owned())),
                ("state", Cow::from("closed")),
            ]),
            half_open: circuit_state.recorder([
                ("ingester", Cow::Owned(ingester_addr.to_owned())),
                ("state", Cow::from("half_open")),
            ]),
        }
    }

    /// Set state to [open](Circuit::Open).
    fn set_open(&self) {
        self.open.set(1);
        self.closed.set(0);
        self.half_open.set(0);
    }

    /// Set state to [closed](Circuit::Closed).
    fn set_closed(&self) {
        self.open.set(0);
        self.closed.set(1);
        self.half_open.set(0);
    }

    /// Set state to [half open](Circuit::HalfOpen).
    fn set_half_open(&self) {
        self.open.set(0);
        self.closed.set(0);
        self.half_open.set(1);
    }
}

/// Current circuit state of a specific connection.
///
/// # State Machine
///
/// ```text
///                 o------(ok)----<request>---(err)--------------------o
///                 |                  |                                |
///                 V                  |                                V
///           +------------+    +-------------+                     +--------+
///           |            |    |             |             o-(no)->|        |
///           |            |    |             |             |       |        |
/// <start>-->|   Closed   |    |  Half Open  |<--(yes)--<waited?>--|  Open  |
///           |            |    |             |                     |        |
///           |            |    |             |                     |        |
///           +------------+    +-------------+                     +--------+
///            ^     |                                                  ^
///            |     |                                                  |
///            |  <request>--(err)---<too many errors?>--(yes)----------o
///            |     |                 |
///            |    (ok)              (no)
///            |     |                 |
///            | [reset err counter]   |
///            |     |                 |
///            o-----o-----------------o
/// ```
///
/// # Generation Counter
/// The circuit state carries a generation counter so because we can have multiple concurrent requests for the same
/// connection and a response should only be able to change a circuit state if the the state hasn't changed while the
/// request was running. Otherwise the reasoning about the state machine will get pretty nasty.
#[derive(Debug)]
enum Circuit {
    /// Circuit is closed, connection is used.
    Closed {
        /// Metrics.
        metrics: CircuitMetrics,

        /// Number of errors on this connection.
        error_count: u64,

        /// Change counter.
        gen: u64,
    },

    /// Circuit is open, no connection will be used.
    Open {
        /// How long this open state will last.
        until: Time,

        /// Backoff state to generate the next `until` value if the trial during [half open](Self::HalfOpen) fails.
        backoff: Option<Backoff>,

        /// Metrics.
        metrics: CircuitMetrics,

        /// Change counter.
        gen: u64,
    },

    /// Circuit is half-open, we will try if the connection is usable again.
    HalfOpen {
        /// Backoff state in case the trial fails and we need to go back into the [open](Self::Open) state.
        backoff: Option<Backoff>,

        /// Signal that is set to `true` if we already have a test request running.
        has_test_running: Arc<AtomicBool>,

        /// Metrics.
        metrics: CircuitMetrics,

        /// Change counter.
        gen: u64,
    },
}

/// Wrapper around [`IngesterFlightClient`] that implements the [Circuit Breaker Design Pattern].
///
/// [Circuit Breaker Design Pattern]: https://en.wikipedia.org/wiki/Circuit_breaker_design_pattern
#[derive(Debug)]
pub struct CircuitBreakerFlightClient {
    /// The underlying client.
    inner: Arc<dyn IngesterFlightClient>,

    /// After how many consecutive errors shall we open a circuit?
    open_circuit_after_n_errors: u64,

    /// Time provider.
    time_provider: Arc<dyn TimeProvider>,

    /// Metric registry.
    metric_registry: Arc<Registry>,

    /// Backoff config.
    backoff_config: BackoffConfig,

    /// Current circuit states, keyed by ingester address
    circuits: Mutex<HashMap<Arc<str>, Circuit>>,

    /// Overwrite for the backoff RNG, used for testing.
    rng_overwrite: Option<StepRng>,
}

impl CircuitBreakerFlightClient {
    /// Create new circuit breaker wrapper.
    ///
    /// Use `open_circuit_after_n_errors` to determine after how many consecutive errors we shall open a circuit.
    pub fn new(
        inner: Arc<dyn IngesterFlightClient>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        open_circuit_after_n_errors: u64,
        backoff_config: BackoffConfig,
    ) -> Self {
        Self {
            inner,
            open_circuit_after_n_errors,
            time_provider,
            metric_registry,
            backoff_config,
            circuits: Default::default(),
            rng_overwrite: None,
        }
    }
}

#[async_trait]
impl IngesterFlightClient for CircuitBreakerFlightClient {
    async fn invalidate_connection(&self, ingester_address: Arc<str>) {
        self.inner.invalidate_connection(ingester_address).await;
    }

    async fn query(
        &self,
        ingester_addr: Arc<str>,
        request: IngesterQueryRequest,
        span_context: Option<SpanContext>,
    ) -> Result<Box<dyn QueryData>, FlightClientError> {
        let (test_signal, start_gen) = {
            let mut circuits = self.circuits.lock();

            match circuits.entry(Arc::clone(&ingester_addr)) {
                Entry::Vacant(v) => {
                    let metrics = CircuitMetrics::new(&self.metric_registry, &ingester_addr);
                    metrics.set_closed();
                    let gen = 0;
                    v.insert(Circuit::Closed {
                        metrics,
                        error_count: 0,
                        gen,
                    });
                    (None, gen)
                }
                Entry::Occupied(mut o) => {
                    let o = o.get_mut();

                    // Open => HalfOpen transition
                    if let Circuit::Open {
                        until,
                        backoff,
                        metrics,
                        gen,
                    } = o
                    {
                        let now = self.time_provider.now();
                        if *until <= now {
                            let backoff = backoff.take().expect("not moved");
                            metrics.set_half_open();
                            *o = Circuit::HalfOpen {
                                backoff: Some(backoff),
                                has_test_running: Arc::new(AtomicBool::new(false)),
                                metrics: metrics.clone(),
                                gen: *gen + 1,
                            }
                        }
                    }

                    let (open, test_signal, gen) = match o {
                        Circuit::Open { gen, .. } => {
                            warn!(
                                ingester_address = ingester_addr.as_ref(),
                                "Circuit open, not contacting ingester",
                            );
                            (true, None, *gen)
                        }
                        Circuit::HalfOpen {
                            has_test_running,
                            gen,
                            ..
                        } => {
                            let this_is_this_test = has_test_running
                                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                                .is_ok();

                            if this_is_this_test {
                                info!(
                                    ingester_address = ingester_addr.as_ref(),
                                    "Circuit half-open and this is a test request",
                                );
                            } else {
                                info!(
                                    ingester_address = ingester_addr.as_ref(),
                                    "Circuit half-open but a test request is already running, not contacting ingester",
                                );
                            }

                            (
                                !this_is_this_test,
                                this_is_this_test.then(|| Arc::clone(has_test_running)),
                                *gen,
                            )
                        }
                        Circuit::Closed { gen, .. } => (false, None, *gen),
                    };

                    if open {
                        return Err(FlightClientError::CircuitBroken {
                            ingester_address: ingester_addr.to_string(),
                        });
                    }

                    (test_signal, gen)
                }
            }
        };

        let fut = self
            .inner
            .query(Arc::clone(&ingester_addr), request, span_context);
        let not_cancelled = test_signal.unwrap_or_else(|| Arc::new(AtomicBool::new(true)));
        let fut = TrackedFuture::new(fut, not_cancelled);
        let res = fut.await;

        let is_error = if let Err(e) = &res {
            e.is_upstream_error()
        } else {
            false
        };

        if is_error {
            let mut circuits = self.circuits.lock();

            let now = self.time_provider.now();

            match circuits.entry(Arc::clone(&ingester_addr)) {
                Entry::Vacant(_) => unreachable!("should have been inserted already"),
                Entry::Occupied(mut o) => {
                    let o = o.get_mut();
                    let maybe_backoff_and_metrics = match o {
                        Circuit::Open { gen, .. } => {
                            assert_ne!(
                                start_gen, *gen,
                                "could not have started in an open circuit state"
                            );
                            None
                        }
                        Circuit::HalfOpen {
                            backoff,
                            metrics,
                            gen,
                            ..
                        } => {
                            assert_eq!(
                                start_gen, *gen,
                                "there's only a single concurrent request for half-open circuits"
                            );
                            Some((backoff.take().expect("not moved"), metrics.clone()))
                        }
                        Circuit::Closed {
                            metrics,
                            error_count,
                            gen,
                        } => {
                            if *gen == start_gen {
                                *error_count += 1;
                                (*error_count >= self.open_circuit_after_n_errors).then(|| {
                                    warn!(
                                        ingester_address = ingester_addr.as_ref(),
                                        "Error contacting ingester, circuit opened"
                                    );

                                    (
                                        Backoff::new_with_rng(
                                            &self.backoff_config,
                                            self.rng_overwrite
                                                .as_ref()
                                                .map(|rng| Box::new(rng.clone()) as _),
                                        ),
                                        metrics.clone(),
                                    )
                                })
                            } else {
                                None
                            }
                        }
                    };

                    if let Some((mut backoff, metrics)) = maybe_backoff_and_metrics {
                        let until = now + backoff.next().expect("never end backoff");
                        metrics.set_open();
                        *o = Circuit::Open {
                            until,
                            backoff: Some(backoff),
                            metrics,
                            gen: start_gen + 1,
                        };
                    }
                }
            }
        } else {
            let mut circuits = self.circuits.lock();

            match circuits.entry(Arc::clone(&ingester_addr)) {
                Entry::Vacant(_) => unreachable!("should have been inserted already"),
                Entry::Occupied(mut o) => {
                    let o = o.get_mut();
                    match o {
                        Circuit::Open { gen, .. } => {
                            // We likely started in an "closed" state but this very request here got delayed and in the
                            // meantime there were so many errors that we've opened the circuit. Keep it open.
                            assert_ne!(
                                start_gen, *gen,
                                "cannot have started a request for an open circuit"
                            );
                        }
                        Circuit::HalfOpen { metrics, gen, .. } => {
                            assert_eq!(
                                start_gen, *gen,
                                "there's only a single concurrent request for half-open circuits"
                            );
                            info!(ingester_address = ingester_addr.as_ref(), "Circuit closed",);

                            metrics.set_closed();
                            *o = Circuit::Closed {
                                metrics: metrics.clone(),
                                error_count: 0,
                                gen: start_gen + 1,
                            };
                        }
                        Circuit::Closed {
                            error_count, gen, ..
                        } => {
                            if start_gen == *gen {
                                *error_count = 0;
                            }
                        }
                    }
                }
            }
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow_flight::decode::DecodedPayload;
    use assert_matches::assert_matches;
    use data_types::{NamespaceId, TableId};
    use generated_types::google::FieldViolation;
    use influxdb_iox_client::flight::generated_types::IngesterQueryResponseMetadata;
    use iox_time::MockProvider;
    use metric::Attributes;
    use test_helpers::maybe_start_logging;
    use tokio::{spawn, sync::Barrier};

    use super::*;

    #[test]
    #[should_panic(expected = "mocked actions left")]
    fn test_meta_mock_client_checks_remaining_actions() {
        let _client = MockClient::from([MockAction::default()]);
    }

    #[test]
    #[should_panic(expected = "foo")]
    fn test_meta_mock_client_drop_does_not_double_panic() {
        let _client = MockClient::from([MockAction::default()]);
        panic!("foo");
    }

    #[tokio::test]
    #[should_panic(expected = "no mocked actions left")]
    async fn test_meta_mock_client_no_actions_left() {
        let client = MockClient::from([]);
        client.query(ingester_address(), request(), None).await.ok();
    }

    #[tokio::test]
    async fn test_happy_path() {
        maybe_start_logging();

        let TestSetup {
            client,
            metric_registry,
            ..
        } = TestSetup::from([MockAction::default()]);

        client.assert_query_ok().await;
        assert_eq!(
            Metrics {
                open: 0,
                closed: 1,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );
    }

    #[tokio::test]
    async fn test_cut_after_n_errors() {
        maybe_start_logging();

        let TestSetup {
            client,
            metric_registry,
            ..
        } = TestSetup::from([
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
        ]);

        client.assert_query_err_flight().await;
        assert_eq!(
            Metrics {
                open: 0,
                closed: 1,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );

        client.assert_query_err_flight().await;
        assert_eq!(
            Metrics {
                open: 1,
                closed: 0,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );

        client.assert_query_err_circuit().await;
        assert_eq!(
            Metrics {
                open: 1,
                closed: 0,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );
    }

    #[tokio::test]
    async fn test_ok_resets_error_counter() {
        maybe_start_logging();

        let TestSetup { client, .. } = TestSetup::from([
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
            MockAction::default(),
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
        ]);

        client.assert_query_err_flight().await;
        client.assert_query_ok().await;
        client.assert_query_err_flight().await;
        client.assert_query_err_flight().await;
        client.assert_query_err_circuit().await;
    }

    #[tokio::test]
    async fn test_not_found_does_not_count_as_error() {
        maybe_start_logging();

        let TestSetup { client, .. } = TestSetup::from([
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
            MockAction {
                err: Some(err_grpc_not_found()),
                ..Default::default()
            },
            MockAction::default(),
        ]);

        client.assert_query_err_flight().await;
        client.assert_query_err_flight().await;
        client.assert_query_ok().await;
    }

    #[tokio::test]
    async fn test_creating_request_error_does_not_count_as_error() {
        maybe_start_logging();

        let TestSetup { client, .. } = TestSetup::from([
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
            MockAction {
                err: Some(err_creating_request()),
                ..Default::default()
            },
            MockAction::default(),
        ]);

        client.assert_query_err_flight().await;
        client.assert_query_err_creating_request().await;
        client.assert_query_ok().await;
    }

    #[tokio::test]
    async fn test_recovery() {
        maybe_start_logging();

        let TestSetup {
            client,
            metric_registry,
            time_provider,
            ..
        } = TestSetup::from([
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
            MockAction::default(),
            MockAction::default(),
        ]);

        client.assert_query_err_flight().await;
        client.assert_query_err_flight().await;
        client.assert_query_err_circuit().await;
        client.assert_query_err_circuit().await;

        assert_eq!(
            Metrics {
                open: 1,
                closed: 0,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );

        time_provider.inc(Duration::from_secs(1));

        client.assert_query_ok().await;
        assert_eq!(
            Metrics {
                open: 0,
                closed: 1,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );

        client.assert_query_ok().await;
    }

    #[tokio::test]
    async fn test_fail_during_recovery() {
        maybe_start_logging();

        let TestSetup {
            client,
            metric_registry,
            time_provider,
            ..
        } = TestSetup::from([
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
            MockAction::default(),
        ]);

        client.assert_query_err_flight().await;
        client.assert_query_err_flight().await;
        client.assert_query_err_circuit().await;

        assert_eq!(
            Metrics {
                open: 1,
                closed: 0,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );

        time_provider.inc(Duration::from_secs(1));

        client.assert_query_err_flight().await;
        assert_eq!(
            Metrics {
                open: 1,
                closed: 0,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );

        client.assert_query_err_circuit().await;

        // backoff is longer now (base is 2)
        time_provider.inc(Duration::from_secs(1));
        client.assert_query_err_circuit().await;
        time_provider.inc(Duration::from_secs(1));

        client.assert_query_ok().await;
        assert_eq!(
            Metrics {
                open: 0,
                closed: 1,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );
    }

    #[tokio::test]
    async fn test_only_one_concurrent_request_during_recovery() {
        maybe_start_logging();

        let barrier = barrier();
        let TestSetup {
            client,
            metric_registry,
            time_provider,
            ..
        } = TestSetup::from([
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
            MockAction {
                wait: Some(Arc::clone(&barrier)),
                ..Default::default()
            },
            MockAction::default(),
        ]);

        client.assert_query_err_flight().await;
        client.assert_query_err_flight().await;
        client.assert_query_err_circuit().await;
        client.assert_query_err_circuit().await;

        assert_eq!(
            Metrics {
                open: 1,
                closed: 0,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );

        time_provider.inc(Duration::from_secs(1));

        let client_captured = Arc::clone(&client);
        let mut fut = spawn(async move {
            client_captured.assert_query_ok().await;
        });
        fut.assert_pending().await;

        client.assert_query_err_circuit().await;

        assert_eq!(
            Metrics {
                open: 0,
                closed: 0,
                half_open: 1,
            },
            Metrics::from(&metric_registry),
        );

        barrier.wait().await;
        fut.await.unwrap();

        assert_eq!(
            Metrics {
                open: 0,
                closed: 1,
                half_open: 0,
            },
            Metrics::from(&metric_registry),
        );

        client.assert_query_ok().await;
    }

    // this test may seem a bit weird / unnecessary, but I was wondering if this could happen during the implementation
    #[tokio::test]
    async fn test_ok_finishes_during_open_state() {
        maybe_start_logging();

        let barrier = barrier();
        let TestSetup {
            client,
            metric_registry,
            ..
        } = TestSetup::from([
            MockAction {
                wait: Some(Arc::clone(&barrier)),
                ..Default::default()
            },
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
        ]);

        let client_captured = Arc::clone(&client);
        let mut fut = spawn(async move {
            client_captured.assert_query_ok().await;
        });
        fut.assert_pending().await;

        client.assert_query_err_flight().await;
        client.assert_query_err_flight().await;
        client.assert_query_err_circuit().await;

        barrier.wait().await;
        fut.await.unwrap();

        // keep it open because while the request was running the circuit opened
        assert_eq!(
            Metrics {
                open: 1,
                closed: 0,
                half_open: 0,
            },
            Metrics::from(&metric_registry),
        );
        client.assert_query_err_circuit().await;
    }

    #[tokio::test]
    async fn test_cancel_recovery() {
        maybe_start_logging();

        let barrier = barrier();
        let TestSetup {
            client,
            metric_registry,
            time_provider,
            ..
        } = TestSetup::from([
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
            MockAction {
                err: Some(err_grpc_internal()),
                ..Default::default()
            },
            MockAction {
                wait: Some(Arc::clone(&barrier)),
                ..Default::default()
            },
            MockAction::default(),
        ]);

        client.assert_query_err_flight().await;
        client.assert_query_err_flight().await;
        client.assert_query_err_circuit().await;

        assert_eq!(
            Metrics {
                open: 1,
                closed: 0,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );

        time_provider.inc(Duration::from_secs(1));

        let client_captured = Arc::clone(&client);
        let mut fut = spawn(async move {
            client_captured.assert_query_ok().await;
        });
        fut.assert_pending().await;

        client.assert_query_err_circuit().await;

        assert_eq!(
            Metrics {
                open: 0,
                closed: 0,
                half_open: 1,
            },
            Metrics::from(&metric_registry),
        );

        fut.abort();

        // wait for tokio to actually cancel the background task
        tokio::time::timeout(Duration::from_secs(5), async move {
            loop {
                if Arc::strong_count(&barrier) == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(
            Metrics {
                open: 0,
                closed: 0,
                half_open: 1,
            },
            Metrics::from(&metric_registry),
        );

        client.assert_query_ok().await;

        assert_eq!(
            Metrics {
                open: 0,
                closed: 1,
                half_open: 0,
            },
            Metrics::from(&metric_registry),
        );
    }

    #[derive(Debug, Default)]
    struct MockAction {
        err: Option<FlightClientError>,
        wait: Option<Arc<Barrier>>,
    }

    #[derive(Debug)]
    struct MockClient {
        actions: Mutex<Vec<MockAction>>,
    }

    impl<const N: usize> From<[MockAction; N]> for MockClient {
        fn from(actions: [MockAction; N]) -> Self {
            Self {
                actions: Mutex::new(actions.into()),
            }
        }
    }

    impl Drop for MockClient {
        fn drop(&mut self) {
            // avoid abort due to double-panic
            if !std::thread::panicking() {
                assert!(self.actions.lock().is_empty(), "mocked actions left");
            }
        }
    }

    #[async_trait]
    impl IngesterFlightClient for MockClient {
        async fn invalidate_connection(&self, _ingester_address: Arc<str>) {
            // no cache
        }

        async fn query(
            &self,
            _ingester_addr: Arc<str>,
            _request: IngesterQueryRequest,
            _span_context: Option<SpanContext>,
        ) -> Result<Box<dyn QueryData>, FlightClientError> {
            let action = {
                let mut actions = self.actions.lock();
                assert!(!actions.is_empty(), "no mocked actions left");
                actions.remove(0)
            };

            if let Some(barrier) = action.wait {
                barrier.wait().await;
            }

            if let Some(e) = action.err {
                return Err(e);
            }

            Ok(Box::new(MockQueryData))
        }
    }

    #[derive(Debug)]
    struct MockQueryData;

    #[async_trait]
    impl QueryData for MockQueryData {
        async fn next_message(
            &mut self,
        ) -> Result<
            Option<(DecodedPayload, IngesterQueryResponseMetadata)>,
            influxdb_iox_client::flight::Error,
        > {
            Ok(None)
        }
    }

    const TEST_INGESTER: &str = "http://my-ingester";

    fn ingester_address() -> Arc<str> {
        Arc::from(TEST_INGESTER)
    }

    fn request() -> IngesterQueryRequest {
        IngesterQueryRequest {
            namespace_id: NamespaceId::new(0),
            table_id: TableId::new(0),
            columns: vec![],
            predicate: None,
        }
    }

    fn barrier() -> Arc<Barrier> {
        Arc::new(Barrier::new(2))
    }

    struct TestSetup {
        client: Arc<CircuitBreakerFlightClient>,
        time_provider: Arc<MockProvider>,
        metric_registry: Arc<Registry>,
    }

    impl<const N: usize> From<[MockAction; N]> for TestSetup {
        fn from(actions: [MockAction; N]) -> Self {
            let mock_client = MockClient::from(actions);
            let time_provider = Arc::new(MockProvider::new(Time::MIN));
            let metric_registry = Arc::new(Registry::new());

            let mut client = CircuitBreakerFlightClient::new(
                Arc::new(mock_client),
                Arc::clone(&time_provider) as _,
                Arc::clone(&metric_registry),
                2,
                BackoffConfig::default(),
            );

            // set up "RNG" that always generates the maximum, so we can test things easier
            client.rng_overwrite = Some(StepRng::new(u64::MAX, 0));
            client.backoff_config = BackoffConfig {
                init_backoff: Duration::from_secs(1),
                max_backoff: Duration::MAX,
                base: 2.,
                deadline: None,
            };

            Self {
                client: Arc::new(client),
                time_provider,
                metric_registry,
            }
        }
    }

    #[async_trait]
    trait FlightClientExt {
        async fn assert_query_ok(&self);
        async fn assert_query_err_flight(&self);
        async fn assert_query_err_creating_request(&self);
        async fn assert_query_err_circuit(&self);
    }

    #[async_trait]
    impl<T> FlightClientExt for T
    where
        T: IngesterFlightClient,
    {
        async fn assert_query_ok(&self) {
            self.query(ingester_address(), request(), None)
                .await
                .unwrap();
        }

        async fn assert_query_err_flight(&self) {
            let e = self
                .query(ingester_address(), request(), None)
                .await
                .unwrap_err();
            assert_matches!(e, FlightClientError::Flight { .. });
        }

        async fn assert_query_err_creating_request(&self) {
            let e = self
                .query(ingester_address(), request(), None)
                .await
                .unwrap_err();
            assert_matches!(e, FlightClientError::CreatingRequest { .. });
        }

        async fn assert_query_err_circuit(&self) {
            let e = self
                .query(ingester_address(), request(), None)
                .await
                .unwrap_err();
            assert_matches!(e, FlightClientError::CircuitBroken { .. });
        }
    }

    fn err_grpc_internal() -> FlightClientError {
        FlightClientError::Flight {
            source: tonic::Status::internal("test error").into(),
        }
    }

    fn err_grpc_not_found() -> FlightClientError {
        FlightClientError::Flight {
            source: tonic::Status::not_found("test error").into(),
        }
    }

    fn err_creating_request() -> FlightClientError {
        FlightClientError::CreatingRequest {
            source: FieldViolation::required("foo"),
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Metrics {
        open: u64,
        closed: u64,
        half_open: u64,
    }

    impl From<&Arc<Registry>> for Metrics {
        fn from(registry: &Arc<Registry>) -> Self {
            let instrument = registry
                .get_instrument::<Metric<U64Gauge>>("ingester_circuit_state")
                .expect("failed to read metric");

            let open = instrument
                .get_observer(&Attributes::from(&[
                    ("state", "open"),
                    ("ingester", TEST_INGESTER),
                ]))
                .expect("failed to get observer")
                .fetch();

            let closed = instrument
                .get_observer(&Attributes::from(&[
                    ("state", "closed"),
                    ("ingester", TEST_INGESTER),
                ]))
                .expect("failed to get observer")
                .fetch();

            let half_open = instrument
                .get_observer(&Attributes::from(&[
                    ("state", "half_open"),
                    ("ingester", TEST_INGESTER),
                ]))
                .expect("failed to get observer")
                .fetch();

            Self {
                open,
                closed,
                half_open,
            }
        }
    }

    #[async_trait]
    trait AssertFutureExt {
        async fn assert_pending(&mut self);
    }

    #[async_trait]
    impl<F> AssertFutureExt for F
    where
        F: Future + Send + Unpin,
    {
        async fn assert_pending(&mut self) {
            tokio::select! {
                biased;
                _ = self => {
                    panic!("not pending")
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    }
}
