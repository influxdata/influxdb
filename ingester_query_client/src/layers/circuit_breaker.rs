//! Circuit breaker.

use std::{
    borrow::Cow,
    future::Future,
    ops::DerefMut,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use iox_time::{Time, TimeProvider};
use metric::{Metric, Registry, U64Gauge};
use observability_deps::tracing::{info, warn};
use pin_project::{pin_project, pinned_drop};
use rand::rngs::mock::StepRng;
use snafu::Snafu;

use crate::{
    error::DynError,
    error_classifier::{is_upstream_error, ErrorClassifier},
    layer::{Layer, QueryResponse},
};

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("ingester circuit broken / open: {addr}"))]
    CircuitBroken { addr: Arc<str> },
}

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
    fn new(metric_registry: &Registry, addr: &str) -> Self {
        let circuit_state: Metric<U64Gauge> = metric_registry.register_metric(
            "ingester_circuit_state",
            "state of the given ingestger connection",
        );

        Self {
            open: circuit_state.recorder([
                ("ingester", Cow::Owned(addr.to_owned())),
                ("state", Cow::from("open")),
            ]),
            closed: circuit_state.recorder([
                ("ingester", Cow::Owned(addr.to_owned())),
                ("state", Cow::from("closed")),
            ]),
            half_open: circuit_state.recorder([
                ("ingester", Cow::Owned(addr.to_owned())),
                ("state", Cow::from("half_open")),
            ]),
        }
    }

    /// Set state to [open](CircuitState::Open).
    fn set_open(&self) {
        self.open.set(1);
        self.closed.set(0);
        self.half_open.set(0);
    }

    /// Set state to [closed](CircuitState::Closed).
    fn set_closed(&self) {
        self.open.set(0);
        self.closed.set(1);
        self.half_open.set(0);
    }

    /// Set state to [half open](CircuitState::HalfOpen).
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
enum CircuitState {
    /// Circuit is closed, connection is used.
    Closed {
        /// Number of errors on this connection.
        error_count: u64,
    },

    /// Circuit is open, no connection will be used.
    Open {
        /// How long this open state will last.
        until: Time,

        /// Backoff state to generate the next `until` value if the trial during [half open](Self::HalfOpen) fails.
        backoff: Option<Backoff>,
    },

    /// Circuit is half-open, we will try if the connection is usable again.
    HalfOpen {
        /// Backoff state in case the trial fails and we need to go back into the [open](Self::Open) state.
        backoff: Option<Backoff>,

        /// Signal that is set to `true` if we already have a test request running.
        has_test_running: Arc<AtomicBool>,
    },
}

#[derive(Debug)]
struct Circuit {
    /// Current state.
    state: CircuitState,

    /// Change counter.
    gen: u64,
}

/// Wrapper around [`Layer`] that implements the [Circuit Breaker Design Pattern].
///
///
/// [Circuit Breaker Design Pattern]: https://en.wikipedia.org/wiki/Circuit_breaker_design_pattern
#[derive(Debug)]
pub struct CircuitBreakerLayer<L>
where
    L: Layer,
{
    /// The underlying client.
    inner: Arc<L>,

    /// Ingester addr.
    addr: Arc<str>,

    /// After how many consecutive errors shall we open a circuit?
    open_circuit_after_n_errors: u64,

    /// Time provider.
    time_provider: Arc<dyn TimeProvider>,

    /// Backoff config.
    backoff_config: BackoffConfig,

    /// Metrics.
    metrics: CircuitMetrics,

    /// Detect if a given error should trigger the circuit breaker.
    should_break: ErrorClassifier,

    /// Current circuit states, keyed by ingester address
    circuit: Mutex<Circuit>,

    /// Overwrite for the backoff RNG, used for testing.
    rng_overwrite: Option<StepRng>,
}

impl<L> CircuitBreakerLayer<L>
where
    L: Layer,
{
    /// Create new circuit breaker wrapper.
    ///
    /// Use `open_circuit_after_n_errors` to determine after how many consecutive errors we shall open a circuit.
    pub fn new(
        inner: L,
        addr: Arc<str>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &Registry,
        open_circuit_after_n_errors: u64,
        backoff_config: BackoffConfig,
    ) -> Self {
        Self::new_with_classifier(
            inner,
            addr,
            time_provider,
            metric_registry,
            open_circuit_after_n_errors,
            backoff_config,
            ErrorClassifier::new(is_upstream_error),
        )
    }

    fn new_with_classifier(
        inner: L,
        addr: Arc<str>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &Registry,
        open_circuit_after_n_errors: u64,
        backoff_config: BackoffConfig,
        should_break: ErrorClassifier,
    ) -> Self {
        let metrics = CircuitMetrics::new(metric_registry, &addr);
        metrics.set_closed();
        Self {
            inner: Arc::new(inner),
            addr,
            open_circuit_after_n_errors,
            time_provider,
            backoff_config,
            metrics,
            should_break,
            circuit: Mutex::new(Circuit {
                state: CircuitState::Closed { error_count: 0 },
                gen: 0,
            }),
            rng_overwrite: None,
        }
    }
}

#[async_trait]
impl<L> Layer for CircuitBreakerLayer<L>
where
    L: Layer,
{
    type Request = L::Request;
    type ResponseMetadata = L::ResponseMetadata;
    type ResponsePayload = L::ResponsePayload;

    async fn query(
        &self,
        request: Self::Request,
    ) -> Result<QueryResponse<Self::ResponseMetadata, Self::ResponsePayload>, DynError> {
        let (test_signal, start_gen) = {
            let mut circuit = self.circuit.lock().expect("not poisoned");

            // Open => HalfOpen transition
            if let CircuitState::Open { until, backoff } = &mut circuit.state {
                let now = self.time_provider.now();
                if *until <= now {
                    let backoff = backoff.take().expect("not moved");
                    self.metrics.set_half_open();
                    circuit.state = CircuitState::HalfOpen {
                        backoff: Some(backoff),
                        has_test_running: Arc::new(AtomicBool::new(false)),
                    };
                    circuit.gen += 1;
                }
            }

            let (open, test_signal) = match &circuit.state {
                CircuitState::Open { .. } => {
                    warn!(
                        addr = self.addr.as_ref(),
                        "Circuit open, not contacting ingester",
                    );
                    (true, None)
                }
                CircuitState::HalfOpen {
                    has_test_running, ..
                } => {
                    let this_is_this_test = has_test_running
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok();

                    if this_is_this_test {
                        info!(
                            addr = self.addr.as_ref(),
                            "Circuit half-open and this is a test request",
                        );
                    } else {
                        info!(
                            addr = self.addr.as_ref(),
                            "Circuit half-open but a test request is already running, not contacting ingester",
                        );
                    }

                    (
                        !this_is_this_test,
                        this_is_this_test.then(|| Arc::clone(has_test_running)),
                    )
                }
                CircuitState::Closed { .. } => (false, None),
            };

            if open {
                return Err(DynError::new(Error::CircuitBroken {
                    addr: Arc::clone(&self.addr),
                }));
            }

            (test_signal, circuit.gen)
        };

        let fut = self.inner.query(request);
        let not_cancelled = test_signal.unwrap_or_else(|| Arc::new(AtomicBool::new(true)));
        let fut = TrackedFuture::new(fut, not_cancelled);
        let res = fut.await;

        let is_error = if let Err(e) = &res {
            self.should_break.matches(e)
        } else {
            false
        };

        let mut circuit = self.circuit.lock().expect("not poisoned");
        let circuit = circuit.deref_mut(); // needed so we can later borrow state and gen seperately
        if is_error {
            let maybe_backoff = match &mut circuit.state {
                CircuitState::Open { .. } => {
                    assert_ne!(
                        start_gen, circuit.gen,
                        "could not have started in an open circuit state"
                    );
                    None
                }
                CircuitState::HalfOpen { backoff, .. } => {
                    if circuit.gen == start_gen {
                        Some(backoff.take().expect("not moved"))
                    } else {
                        // this was not the test request but an old one
                        None
                    }
                }
                CircuitState::Closed { error_count } => {
                    if circuit.gen == start_gen {
                        *error_count += 1;
                        (*error_count >= self.open_circuit_after_n_errors).then(|| {
                            warn!(
                                addr = self.addr.as_ref(),
                                "Error contacting ingester, circuit opened"
                            );

                            Backoff::new_with_rng(
                                &self.backoff_config,
                                self.rng_overwrite
                                    .as_ref()
                                    .map(|rng| Box::new(rng.clone()) as _),
                            )
                        })
                    } else {
                        None
                    }
                }
            };

            if let Some(mut backoff) = maybe_backoff {
                let until = self.time_provider.now() + backoff.next().expect("never end backoff");
                self.metrics.set_open();
                circuit.state = CircuitState::Open {
                    until,
                    backoff: Some(backoff),
                };
                circuit.gen += 1;
            }
        } else {
            match &mut circuit.state {
                CircuitState::Open { .. } => {
                    // We likely started in an "closed" state but this very request here got delayed and in the
                    // meantime there were so many errors that we've opened the circuit. Keep it open.
                    assert_ne!(
                        start_gen, circuit.gen,
                        "cannot have started a request for an open circuit"
                    );
                }
                CircuitState::HalfOpen { .. } => {
                    if start_gen == circuit.gen {
                        info!(addr = self.addr.as_ref(), "Circuit closed",);

                        self.metrics.set_closed();
                        circuit.state = CircuitState::Closed { error_count: 0 };
                        circuit.gen += 1;
                    }
                }
                CircuitState::Closed { error_count, .. } => {
                    if start_gen == circuit.gen {
                        *error_count = 0;
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

    use iox_time::MockProvider;
    use metric::Attributes;
    use test_helpers::maybe_start_logging;
    use tokio::{spawn, sync::Barrier};

    use crate::{
        error::ErrorChainExt,
        error_classifier::{test_error_classifier, TestError},
        layers::testing::{TestLayer, TestResponse},
    };

    use super::*;

    #[tokio::test]
    async fn test_metric_initially_closed() {
        maybe_start_logging();

        let TestSetup {
            metric_registry, ..
        } = TestSetup::from([]);
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
    async fn test_happy_path() {
        maybe_start_logging();

        let TestSetup {
            l, metric_registry, ..
        } = TestSetup::from([TestResponse::ok(())]);

        l.assert_query_ok().await;
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
            l, metric_registry, ..
        } = TestSetup::from([
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::err(DynError::new(TestError::RETRY)),
        ]);

        l.assert_query_err_upstream(TestError::RETRY).await;
        assert_eq!(
            Metrics {
                open: 0,
                closed: 1,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );

        l.assert_query_err_upstream(TestError::RETRY).await;
        assert_eq!(
            Metrics {
                open: 1,
                closed: 0,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );

        l.assert_query_err_circuit().await;
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

        let TestSetup { l, .. } = TestSetup::from([
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::ok(()),
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::err(DynError::new(TestError::RETRY)),
        ]);

        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_ok().await;
        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_circuit().await;
    }

    #[tokio::test]
    async fn test_error_classifier_used() {
        maybe_start_logging();

        let TestSetup { l, .. } = TestSetup::from([
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::err(DynError::new(TestError::NO_RETRY)),
            TestResponse::ok(()),
        ]);

        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_upstream(TestError::NO_RETRY).await;
        l.assert_query_ok().await;
    }

    #[tokio::test]
    async fn test_recovery() {
        maybe_start_logging();

        let TestSetup {
            l,
            metric_registry,
            time_provider,
            ..
        } = TestSetup::from([
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::ok(()),
            TestResponse::ok(()),
        ]);

        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_circuit().await;
        l.assert_query_err_circuit().await;

        assert_eq!(
            Metrics {
                open: 1,
                closed: 0,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );

        time_provider.inc(Duration::from_secs(1));

        l.assert_query_ok().await;
        assert_eq!(
            Metrics {
                open: 0,
                closed: 1,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );

        l.assert_query_ok().await;
    }

    #[tokio::test]
    async fn test_fail_during_recovery() {
        maybe_start_logging();

        let TestSetup {
            l,
            metric_registry,
            time_provider,
            ..
        } = TestSetup::from([
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::ok(()),
        ]);

        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_circuit().await;

        assert_eq!(
            Metrics {
                open: 1,
                closed: 0,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );

        time_provider.inc(Duration::from_secs(1));

        l.assert_query_err_upstream(TestError::RETRY).await;
        assert_eq!(
            Metrics {
                open: 1,
                closed: 0,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );

        l.assert_query_err_circuit().await;

        // backoff is longer now (base is 2)
        time_provider.inc(Duration::from_secs(1));
        l.assert_query_err_circuit().await;
        time_provider.inc(Duration::from_secs(1));

        l.assert_query_ok().await;
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

        let barrier_pre = barrier();
        let barrier_post = barrier();
        let TestSetup {
            l,
            metric_registry,
            time_provider,
            ..
        } = TestSetup::from([
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::ok(())
                .with_initial_barrier(Arc::clone(&barrier_pre))
                .with_initial_barrier(Arc::clone(&barrier_post)),
            TestResponse::ok(()),
        ]);

        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_circuit().await;
        l.assert_query_err_circuit().await;

        assert_eq!(
            Metrics {
                open: 1,
                closed: 0,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );

        time_provider.inc(Duration::from_secs(1));

        let l_captured = Arc::clone(&l);
        let fut = spawn(async move {
            l_captured.assert_query_ok().await;
        });
        barrier_pre.wait().await;

        l.assert_query_err_circuit().await;

        assert_eq!(
            Metrics {
                open: 0,
                closed: 0,
                half_open: 1,
            },
            Metrics::from(&metric_registry),
        );

        barrier_post.wait().await;
        fut.await.unwrap();

        assert_eq!(
            Metrics {
                open: 0,
                closed: 1,
                half_open: 0,
            },
            Metrics::from(&metric_registry),
        );

        l.assert_query_ok().await;
    }

    // this test may seem a bit weird / unnecessary, but I was wondering if this could happen during the implementation
    #[tokio::test]
    async fn test_ok_finishes_during_open_state() {
        maybe_start_logging();

        let barrier_pre = barrier();
        let barrier_post = barrier();
        let TestSetup {
            l, metric_registry, ..
        } = TestSetup::from([
            TestResponse::ok(())
                .with_initial_barrier(Arc::clone(&barrier_pre))
                .with_initial_barrier(Arc::clone(&barrier_post)),
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::err(DynError::new(TestError::RETRY)),
        ]);

        let l_captured = Arc::clone(&l);
        let fut = spawn(async move {
            l_captured.assert_query_ok().await;
        });
        barrier_pre.wait().await;

        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_circuit().await;

        barrier_post.wait().await;
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
        l.assert_query_err_circuit().await;
    }

    #[tokio::test]
    async fn test_cancel_recovery() {
        maybe_start_logging();

        let barrier_pre = barrier();
        let barrier_post = barrier();
        let TestSetup {
            l,
            metric_registry,
            time_provider,
            ..
        } = TestSetup::from([
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::ok(())
                .with_initial_barrier(Arc::clone(&barrier_pre))
                .with_initial_barrier(Arc::clone(&barrier_post)),
            TestResponse::ok(()),
        ]);

        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_circuit().await;

        assert_eq!(
            Metrics {
                open: 1,
                closed: 0,
                half_open: 0
            },
            Metrics::from(&metric_registry),
        );

        time_provider.inc(Duration::from_secs(1));

        let l_captured = Arc::clone(&l);
        let fut = spawn(async move {
            l_captured.assert_query_ok().await;
        });
        barrier_pre.wait().await;

        l.assert_query_err_circuit().await;

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
                if Arc::strong_count(&barrier_post) == 1 {
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

        l.assert_query_ok().await;

        assert_eq!(
            Metrics {
                open: 0,
                closed: 1,
                half_open: 0,
            },
            Metrics::from(&metric_registry),
        );
    }

    #[tokio::test]
    async fn test_late_failure_after_recovery() {
        maybe_start_logging();

        let barrier_pre = barrier();
        let barrier_post = barrier();
        let TestSetup {
            l, time_provider, ..
        } = TestSetup::from([
            TestResponse::err(DynError::new(TestError::RETRY))
                .with_initial_barrier(Arc::clone(&barrier_pre))
                .with_initial_barrier(Arc::clone(&barrier_post)),
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::ok(()),
            TestResponse::ok(()),
        ]);

        // set up request that will fail later
        let l_captured = Arc::clone(&l);
        let fut = spawn(async move {
            l_captured.assert_query_err_upstream(TestError::RETRY).await;
        });
        barrier_pre.wait().await;

        // break circuit
        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_circuit().await;

        // recover circuit
        time_provider.inc(Duration::from_secs(1));
        l.assert_query_ok().await;

        barrier_post.wait().await;
        fut.await.unwrap();

        // circuit not broken, because it was too late
        l.assert_query_ok().await;
    }

    #[tokio::test]
    async fn test_late_failure_after_half_open() {
        maybe_start_logging();

        let barrier1_pre = barrier();
        let barrier1_post = barrier();
        let barrier2_pre = barrier();
        let barrier2_post = barrier();
        let TestSetup {
            l, time_provider, ..
        } = TestSetup::from([
            TestResponse::err(DynError::new(TestError::RETRY))
                .with_initial_barrier(Arc::clone(&barrier1_pre))
                .with_initial_barrier(Arc::clone(&barrier1_post)),
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::ok(())
                .with_initial_barrier(Arc::clone(&barrier2_pre))
                .with_initial_barrier(Arc::clone(&barrier2_post)),
        ]);

        // set up request that will fail later
        let l_captured = Arc::clone(&l);
        let fut1 = spawn(async move {
            l_captured.assert_query_err_upstream(TestError::RETRY).await;
        });
        barrier1_pre.wait().await;

        // break circuit
        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_circuit().await;

        // half-open
        time_provider.inc(Duration::from_secs(1));
        let l_captured = Arc::clone(&l);
        let fut2 = spawn(async move {
            l_captured.assert_query_ok().await;
        });
        barrier2_pre.wait().await;

        barrier1_post.wait().await;
        fut1.await.unwrap();

        barrier2_post.wait().await;
        fut2.await.unwrap();
    }

    #[tokio::test]
    async fn test_late_ok_after_half_open() {
        maybe_start_logging();

        let barrier1_pre = barrier();
        let barrier1_post = barrier();
        let barrier2_pre = barrier();
        let barrier2_post = barrier();
        let TestSetup {
            l, time_provider, ..
        } = TestSetup::from([
            TestResponse::ok(())
                .with_initial_barrier(Arc::clone(&barrier1_pre))
                .with_initial_barrier(Arc::clone(&barrier1_post)),
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::err(DynError::new(TestError::RETRY)),
            TestResponse::ok(())
                .with_initial_barrier(Arc::clone(&barrier2_pre))
                .with_initial_barrier(Arc::clone(&barrier2_post)),
        ]);

        // set up request that will fail later
        let l_captured = Arc::clone(&l);
        let fut1 = spawn(async move {
            l_captured.assert_query_ok().await;
        });
        barrier1_pre.wait().await;

        // break circuit
        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_upstream(TestError::RETRY).await;
        l.assert_query_err_circuit().await;

        // half-open
        time_provider.inc(Duration::from_secs(1));
        let l_captured = Arc::clone(&l);
        let fut2 = spawn(async move {
            l_captured.assert_query_ok().await;
        });
        barrier2_pre.wait().await;

        barrier1_post.wait().await;
        fut1.await.unwrap();

        barrier2_post.wait().await;
        fut2.await.unwrap();
    }

    const TEST_INGESTER: &str = "http://my-ingester";

    fn barrier() -> Arc<Barrier> {
        Arc::new(Barrier::new(2))
    }

    struct TestSetup {
        l: Arc<CircuitBreakerLayer<TestLayer<(), (), ()>>>,
        time_provider: Arc<MockProvider>,
        metric_registry: Arc<Registry>,
    }

    impl<const N: usize> From<[TestResponse<(), ()>; N]> for TestSetup {
        fn from(responses: [TestResponse<(), ()>; N]) -> Self {
            let l = TestLayer::<(), (), ()>::default();
            for resp in responses {
                l.mock_response(resp);
            }
            let time_provider = Arc::new(MockProvider::new(Time::MIN));
            let metric_registry = Arc::new(Registry::new());

            let mut l = CircuitBreakerLayer::new_with_classifier(
                l,
                Arc::from(TEST_INGESTER),
                Arc::clone(&time_provider) as _,
                &metric_registry,
                2,
                BackoffConfig::default(),
                test_error_classifier(),
            );

            // set up "RNG" that always generates the maximum, so we can test things easier
            l.rng_overwrite = Some(StepRng::new(u64::MAX, 0));
            l.backoff_config = BackoffConfig {
                init_backoff: Duration::from_secs(1),
                max_backoff: Duration::MAX,
                base: 2.,
                deadline: None,
            };

            Self {
                l: Arc::new(l),
                time_provider,
                metric_registry,
            }
        }
    }

    #[async_trait]
    trait TestLayerExt {
        async fn assert_query_ok(&self);
        async fn assert_query_err_upstream(&self, e: TestError);
        async fn assert_query_err_circuit(&self);
    }

    #[async_trait]
    impl<L> TestLayerExt for L
    where
        L: Layer<Request = (), ResponseMetadata = (), ResponsePayload = ()>,
    {
        async fn assert_query_ok(&self) {
            self.query(()).await.unwrap();
        }

        async fn assert_query_err_upstream(&self, e: TestError) {
            let e_actual = self.query(()).await.unwrap_err();
            assert!(
                e_actual.error_chain().any(|e_actual| e_actual
                    .downcast_ref::<TestError>()
                    .map(|e_actual| e_actual == &e)
                    .unwrap_or_default()),
                "Error does not match.\n\nActual:\n{e_actual}\n\nExpected:\n{e}",
            );
        }

        async fn assert_query_err_circuit(&self) {
            let e = self.query(()).await.unwrap_err();
            assert!(
                e.error_chain().any(|e| e
                    .downcast_ref::<Error>()
                    .map(|e| matches!(e, Error::CircuitBroken { .. }))
                    .unwrap_or_default()),
                "Error is NOT a circuit breaker:\n{e}",
            );
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
}
