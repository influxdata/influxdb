use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use crossbeam_utils::CachePadded;
use observability_deps::tracing::*;
use parking_lot::Mutex;
use tokio::{
    task::JoinHandle,
    time::{Instant, MissedTickBehavior},
};

/// The limit on the ratio of the number of error requests to the number of
/// successful requests within the configured error window to be considered
/// healthy. If updating this value, remember to update the documentation
/// in the CLI flag for the configurable error window.
const MAX_ERROR_RATIO: f32 = 0.8;
/// The length of time during which up to the configured number of probes
/// are allowed when in an unhealthy state.
const PROBE_INTERVAL: Duration = Duration::from_secs(1);

/// A low-overhead, error ratio gated [`CircuitBreaker`].
///
/// # Usage
///
/// Callers using this [`CircuitBreaker`] are expected to determine whether to
/// take some action (for example, whether to send a request to an upstream)
/// based off of the modelled health state of the upstream (read by calling
/// [`CircuitBreaker::is_healthy()`]). If the upstream is "healthy", a request
/// can be sent.
///
/// If [`CircuitBreaker::is_healthy()`] returns `false`, the caller optionally
/// may choose to send a request functioning as a health probe to the upstream
/// to detect recovery (with an expectation this will likely fail). The
/// [`CircuitBreaker`] selects callers to send a probe request, indicated by a
/// `true` return value from [`CircuitBreaker::should_probe()`]. At least
/// the configured number of probes must be sent to drive a [`CircuitBreaker`] to a healthy
/// state, so callers must call [`CircuitBreaker::should_probe()`] periodically.
///
/// When requests made to the ingester return, [`CircuitBreaker::observe()`] is
/// called with the request's success or error state to be considered part of
/// the circuit breaker calculations.
///
/// In pseudocode:
///
/// ```ignore
/// if ingester.circuit_breaker.is_healthy() || ingester.circuit_breaker.should_probe() {
///     let result = make_request(&ingester);
///     ingester.circuit_breaker.observe(&result);
///     result
/// }
/// ```
///
/// # Implementation
///
/// The circuit breaker is considered unhealthy when 80% ([`MAX_ERROR_RATIO`])
/// of requests within the configured error window fail. The breaker
/// becomes healthy again when the error rate falls below 80%
/// ([`MAX_ERROR_RATIO`]) for the, at most, configured number of probe requests
/// allowed through within 1 second ([`PROBE_INTERVAL`]).
///
/// The circuit breaker initialises in the healthy state.
///
/// ## States
///
/// A circuit breaker has 3 states, described below with implementation specific
/// notes:
///
///   * Closed: The "healthy" state - all requests are allowed. The circuit
///     breaker is considered healthy when the ratio of the number of error
///     requests to the number of successful requests in the current window is
///     less than [`MAX_ERROR_RATIO`]. If the ratio of errors exceeds
///     [`MAX_ERROR_RATIO`] within a single error window,
///     the circuit breaker is then considered to be in the "open/unhealthy"
///     state.
///
///   * Open: The "unhealthy" state - requests are refused because
///     [`is_healthy`][`CircuitBreaker::is_healthy`] and
///     [`should_probe`][`CircuitBreaker::should_probe`] will both return
///     `false`. This is a short-lived state-- after a [`PROBE_INTERVAL`] delay,
///     the circuit breaker transitions to half-open and begins allowing some
///     probing requests of the remote to proceed.
///
///   * Half-open: A transition state between "open/unhealthy" and
///     "closed/healthy"; a majority of traffic is refused, but up to
///     the configured number of probes number of requests are allowed to proceed per
///     [`PROBE_INTERVAL`]. Once the probes are sent, the error ratio is
///     evaluated, and the system returns to either open or closed as
///     appropriate.
///
/// ## Error Ratio / Opening (becoming unhealthy)
///
/// Successful requests and errors are recorded when passed to
/// [`CircuitBreaker::observe()`]. These counters are reset at intervals of
/// the configured error window, meaning that the ratio of errors must exceed
/// [`MAX_ERROR_RATIO`] within a single window to open the circuit breaker to
/// start being considered unhealthy.
///
/// A floor of at least [`MAX_ERROR_RATIO`] * `configured num_probes` must be observed per
/// error window before the circuit breaker opens / becomes unhealthy.
///
/// Error ratios are measured on every call to [`CircuitBreaker::is_healthy`],
/// which should be done before determining whether to perform each request.
/// [`CircuitBreaker::is_healthy`] will begin returning `false` and be
/// considered in the open/unhealthy case the instant that the
/// [`MAX_ERROR_RATIO`] is exceeded.
///
/// This continuous evaluation and discrete time windows ensure timely opening
/// of the breaker even if there have been large numbers of successful requests
/// previously.
///
/// ## Probing / Closing (becoming healthy)
///
/// Once a circuit breaker transitions to "open/unhealthy", up to the configured number of probe
/// requests are allowed per 1s [`PROBE_INTERVAL`], as determined by calling
/// [`CircuitBreaker::should_probe`] before sending a request. This is referred
/// to as "probing", allowing the client to discover the state of the
/// (potentially unavailable) remote while bounding the number of requests that
/// may fail as a result.
///
/// Whilst in the probing state, the result of each allowed probing request is
/// recorded - once at least the configured number of probe requests have been completed and the
/// ratio of errors drops below [`MAX_ERROR_RATIO`], the circuit breaker
/// transitions to "closed/healthy".
///
/// If the configured number of probe requests have been completed and the ratio of errors to
/// successes continues to be above [`MAX_ERROR_RATIO`], transition back to
/// "open/unhealthy" to wait another [`PROBE_INTERVAL`] delay before
/// transitioning back to the probing state and allowing another set of probe
/// requests to proceed.
///
/// If there are not enough requests made to exceed the configured number of probes within a
/// period of [`PROBE_INTERVAL`], all requests are probes and are allowed
/// through.
#[derive(Debug)]
pub struct CircuitBreaker {
    /// Counters tracking the number of [`Ok`] and [`Err`] observed in the
    /// current error window.
    ///
    /// When the total number of requests ([`RequestCounterValue::total()`]) is
    /// less than the configured number of probes, the circuit is in the "probing" regime. When
    /// the number of requests is greater than this amount, the circuit
    /// open/closed state depends on the current error ratio.
    requests: Arc<RequestCounter>,

    /// The slow-path probing state, tracking how many probes have been and
    /// should be allowed and resetting the [`PROBE_INTERVAL`].
    probes: Mutex<ProbeState>,

    /// A task to reset the request count at the configured error window.
    reset_task: JoinHandle<()>,

    /// A string description of the endpoint this [`CircuitBreaker`] models.
    ///
    /// Used for logging context only.
    endpoint: Arc<str>,

    num_probes: u64,
}

#[derive(Debug, Default)]
struct ProbeState {
    /// The instant at which this set of probes started to be sent.
    ///
    /// Up to the configured probe limit SHOULD be sent in the time range between this
    /// timestamp plus [`PROBE_INTERVAL`].
    probe_window_started_at: Option<Instant>,
    /// The number of probes sent so far in this [`PROBE_INTERVAL`].
    probes_started: u64,
}

impl CircuitBreaker {
    pub(crate) fn new(
        endpoint: impl Into<Arc<str>>,
        error_window: Duration,
        num_probes: u64,
    ) -> Self {
        let requests = Arc::new(RequestCounter::default());
        let s = Self {
            requests: Arc::clone(&requests),
            probes: Mutex::new(ProbeState::default()),
            reset_task: tokio::spawn(async move {
                let mut ticker = tokio::time::interval(error_window);
                ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    reset_closed_state_counters(&requests, num_probes);
                }
            }),
            endpoint: endpoint.into(),
            num_probes,
        };
        s.set_healthy();
        s
    }

    /// Force-set the state of the circuit breaker to "closed" / healthy.
    pub(crate) fn set_healthy(&self) {
        self.requests.set(self.num_probes as u32, 0);
    }

    /// Observe a request result, recording the success/error.
    ///
    /// # Blocking
    ///
    /// This method never blocks (wait free population oblivious), and is very
    /// cheap to call.
    pub(crate) fn observe<V, E>(&self, r: &Result<V, E>) {
        self.requests.observe(r);
    }

    /// Returns `true` iff the circuit breaker is fully "closed" and considered
    /// healthy.
    ///
    /// # Blocking
    ///
    /// This method never blocks (wait free population oblivious), and is very
    /// cheap to call.
    #[inline]
    pub(crate) fn is_healthy(&self) -> bool {
        let counts = self.requests.read();

        // If the counts have previously transitioned to being in the probing
        // state, the circuit breaker can't be healthy, and we don't need to
        // check the error ratio.
        if is_probing(counts, self.num_probes) {
            return false;
        }

        // If we're not in the probing state, we need to check the current error
        // ratio to determine whether the circuit breaker is healthy or not.
        is_healthy(counts)
    }

    /// Return `true` if the caller should be allowed to begin a request to the
    /// potentially- unavailable endpoint that can also be used as a probe of
    /// the endpoint. Always returns `false` if `self` is in the
    /// "closed/healthy" state; callers should check `is_healthy` first and only
    /// call this if `is_healthy` returns `false`.
    ///
    /// This method will return `true` at most `num_probes` per
    /// [`PROBE_INTERVAL`] discrete duration.
    ///
    /// # Blocking
    ///
    /// This method MAY block and serialise concurrent callers.
    ///
    /// # Concurrent Requests
    ///
    /// Concurrent requests that were started before (or after) `self` switches
    /// to the probing regime without observing [`Self::should_probe()`] as
    /// `true` count as probes when evaluating the state of the circuit. These
    /// requests are in addition to the configured number of probes guaranteed to observe
    /// `true` when calling this method.
    pub(crate) fn should_probe(&self) -> bool {
        // Enable this code path only when probing needs to start, or has
        // already started.
        if self.is_healthy() {
            return false;
        }

        let mut guard = self.probes.lock();
        let now = Instant::now();

        // Reset the probe count once per PROBE_INTERVAL.
        match guard.probe_window_started_at {
            // It is time to begin probing again.
            Some(p) if now.duration_since(p) > PROBE_INTERVAL => {
                warn!(endpoint=%self.endpoint, "remote unavailable, probing");

                // It should be impossible to have allowed more than NUM_PROBES
                // requests through since the last time `guard.probes_started`
                // has been reset because of the `return false` in the next
                // match arm that prevents the increase of
                // `guard.probes_started` if it has reached `NUM_PROBES`.
                assert!(guard.probes_started <= self.num_probes);
                // Record the start of a probing interval.
                guard.probe_window_started_at = Some(now);
                // Reset the number of probes allowed.
                guard.probes_started = 0;

                // Reset the request success/error counters.
                //
                // The probes populate the request counters, which in turn drive
                // the health check.  In order to exit the probing state, at
                // least NUM_PROBES number of probes are needed, and the ratio
                // of probe errors must not exceed MAX_ERROR_RATIO.
                self.requests.set(0, 0);
            }
            Some(_p) => {
                // Probing is ongoing.
                //
                // If there have already been the configured number of probes,
                // do not allow more.
                if guard.probes_started >= self.num_probes {
                    debug!(
                        endpoint=%self.endpoint,
                        "probes exhausted"
                    );
                    return false;
                }
            }
            None => {
                // First time this circuit breaker has entered the probing
                // state; no start of a probe interval to check.
                guard.probe_window_started_at = Some(now);
                // It should be impossible to have started probes if we've never
                // been in the probing state before.
                assert_eq!(guard.probes_started, 0);
                self.requests.set(0, 0);
            }
        }

        // Record this probe was started.
        guard.probes_started += 1;

        debug!(
            nth_probe = guard.probes_started,
            max_probes = self.num_probes,
            endpoint=%self.endpoint,
            "sending probe"
        );

        true
    }
}

impl Drop for CircuitBreaker {
    fn drop(&mut self) {
        self.reset_task.abort()
    }
}

// Returns `true` if the circuit is currently in the "probe" state.
#[inline]
fn is_probing(counts: RequestCounterValue, num_probes: u64) -> bool {
    // When there are less than `NUM_PROBES` completed requests, the circuit is
    // not closed/healthy, as some previous call to `should_probe` has observed
    // an error rate to put the circuit into the probing state, which resets the
    // request counts to start at 0.
    counts.total() < num_probes
}

/// Return `true` if the current ratio of errors is below MAX_ERROR_RATIO,
/// irrespective of the current circuit state.
#[inline]
fn is_healthy(counts: RequestCounterValue) -> bool {
    // Ensure there is never a division by 0 by adding 1 to both counters,
    // maintaining the same ratio between them.
    let pcnt = (counts.errors() + 1) as f32 / (counts.successes() + 1) as f32;
    let ratio = pcnt < MAX_ERROR_RATIO;

    trace!(
        errors = counts.errors(),
        successes = counts.successes(),
        total = counts.total(),
        healthy = ratio,
        "evaluate circuit breaker health"
    );

    ratio
}

/// Resets the absolute request counter values if the current circuit state is
/// "closed" (healthy, not probing) at the time of the call, such that the there
/// must be `num_probes` * [`MAX_ERROR_RATIO`] number of failed requests to open the
/// circuit (mark as unhealthy).
///
/// Retains the closed/healthy state of the circuit. This is NOT an atomic
/// operation.
fn reset_closed_state_counters(counters: &RequestCounter, num_probes: u64) {
    let counts = counters.read();
    if !is_healthy(counts) || is_probing(counts, num_probes) {
        return;
    }
    counters.set(num_probes as u32, 0);
}

/// A store of two `u32` that can be read atomically; one tracking successful
/// requests and one for errors.
///
/// The success count is stored in the 32 least-significant bits, with the error
/// count stored in the 32 most-significant bits.
#[derive(Default)]
struct RequestCounter(CachePadded<AtomicU64>);

impl Debug for RequestCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RequestCounter").field(&self.read()).finish()
    }
}

impl RequestCounter {
    /// Return an atomic snapshot of the counter values.
    fn read(&self) -> RequestCounterValue {
        RequestCounterValue(self.0.load(Ordering::Relaxed))
    }

    /// Set the counter values.
    fn set(&self, successes: u32, errors: u32) {
        let value = ((errors as u64) << u32::BITS) | successes as u64;
        self.0.store(value, Ordering::Relaxed);
    }

    /// Increment the success/error count based on the given [`Result`].
    fn observe<T, E>(&self, r: &Result<T, E>) {
        let value = match r {
            Ok(_) => 1,
            Err(_) => 1 << u32::BITS,
        };

        self.0.fetch_add(value, Ordering::Relaxed);
    }
}

/// A consistent snapshot of the success/error counter values.
#[derive(Clone, Copy, PartialEq, Eq)]
struct RequestCounterValue(u64);

impl Debug for RequestCounterValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestCounterValue")
            .field("successes", &self.successes())
            .field("errors", &self.errors())
            .field("total", &self.total())
            .finish()
    }
}

impl RequestCounterValue {
    fn successes(&self) -> u32 {
        self.0 as u32
    }

    fn errors(&self) -> u32 {
        (self.0 >> u32::BITS) as u32
    }

    fn total(&self) -> u64 {
        self.successes() as u64 + self.errors() as u64
    }
}

#[cfg(test)]
mod tests {
    use rand::random;

    use super::*;

    /// Assert that calling [`reset_closed_state_counters`] does nothing.
    #[track_caller]
    fn assert_reset_is_nop(counters: &RequestCounter, num_probes: u64) {
        let v = counters.read();
        reset_closed_state_counters(counters, num_probes);
        assert_eq!(v, counters.read());
    }

    /// Helper to calculate the number of errors needed to mark the circuit
    /// breaker as unhealthy.
    fn errors_to_unhealthy(counters: &RequestCounter) -> usize {
        (counters.read().total() as f32 * MAX_ERROR_RATIO).ceil() as usize
    }

    /// Return a new [`CircuitBreaker`] with the reset ticker disabled.
    fn new_no_reset() -> CircuitBreaker {
        let c = CircuitBreaker::new("bananas", Duration::from_secs(5), 10);
        c.reset_task.abort();
        c
    }

    /// Assert that a newly initialised circuit breaker starts in the fully
    /// closed state, is driven open, and allows NUM_PROBES per PROBE_INTERVAL
    /// when probing.
    #[tokio::test]
    async fn test_init_closed_drive_open_probe_recovery() {
        let c = new_no_reset();

        // The circuit breaker starts in a healthy state.
        assert!(c.is_healthy());
        assert!(!c.should_probe());
        assert_reset_is_nop(&c.requests, c.num_probes);

        // Observe enough errors to become unhealthy
        let n = errors_to_unhealthy(&c.requests);
        for _ in 0..n {
            assert!(c.is_healthy());
            assert!(!c.should_probe());
            c.requests.observe::<(), ()>(&Err(()));
        }

        // It should then transition to the probe state, and allow the
        // configured amount of probe requests.
        for _ in 0..c.num_probes {
            assert!(!c.is_healthy());
            assert!(c.should_probe());
            // Counter resets should not be allowed when the circuit is not
            // healthy.
            assert_reset_is_nop(&c.requests, c.num_probes);
        }

        // And once NUM_PROBES is reached, stop allowing more probes.
        //
        // It should remain unhealthy during this time.
        assert!(!c.should_probe());
        assert!(!c.is_healthy());
        assert_reset_is_nop(&c.requests, c.num_probes);

        // Pretend it is time to probe again.
        c.probes.lock().probe_window_started_at = Some(
            Instant::now()
                .checked_sub(PROBE_INTERVAL + Duration::from_nanos(1))
                .expect("instant cannot roll back far enough - test issue, not code issue"),
        );

        for _ in 0..(c.num_probes - 1) {
            // Recording a successful probe request should not mark the circuit
            // as healthy until the NUM_PROBES has been observed.
            assert!(c.should_probe());
            assert!(!c.is_healthy());
            c.requests.observe::<(), ()>(&Ok(()));
            assert_reset_is_nop(&c.requests, c.num_probes);
        }

        // Upon reaching NUM_PROBES of entirely successful requests, the circuit
        // becomes healthy.
        assert!(!c.is_healthy());
        assert!(c.should_probe());
        c.requests.observe::<(), ()>(&Ok(()));
        assert!(c.is_healthy());
        assert!(!c.should_probe());
    }

    /// A circuit breaker is initialised in the healthy state, driven to
    /// open/unhealthy in response to sufficient error observations, and does
    /// not recover due to insufficient successful probes.
    #[tokio::test]
    async fn test_probe_insufficient_success() {
        let c = new_no_reset();

        assert!(c.is_healthy());
        assert!(!c.should_probe());

        // Observe enough errors to become unhealthy
        let n = errors_to_unhealthy(&c.requests);
        for _ in 0..n {
            assert!(c.is_healthy());
            c.requests.observe::<(), ()>(&Err(()));
        }

        // The circuit breaker is now in an unhealthy/probe state and should
        // allow the configured amount of probe requests to drive to a
        // closed/healthy state.
        //
        // Observing half of them as failing should end probing until the next
        // probe period.
        let n_failed = c.num_probes / 2;
        for _ in 0..(n_failed) {
            assert!(!c.is_healthy());
            assert!(c.should_probe());
            assert_reset_is_nop(&c.requests, c.num_probes);
            c.requests.observe::<(), ()>(&Ok(()));
        }
        for _ in 0..(c.num_probes - n_failed) {
            assert!(!c.is_healthy());
            assert!(c.should_probe());
            assert_reset_is_nop(&c.requests, c.num_probes);
            c.requests.observe::<(), ()>(&Err(()));
        }
        assert_eq!(c.requests.read().total(), c.num_probes);

        // The probes did not drive the circuit breaker to closed/healthy.
        assert!(!c.is_healthy());
        // And no more probes should be allowed.
        assert!(!c.should_probe());
        // The request counters should not reset when unhealthy.
        assert_reset_is_nop(&c.requests, c.num_probes);

        // Pretend it is time to probe again.
        c.probes.lock().probe_window_started_at = Some(
            Instant::now()
                .checked_sub(PROBE_INTERVAL + Duration::from_nanos(1))
                .expect("instant cannot roll back far enough - test issue, not code issue"),
        );

        // Do the probe requests, all succeeding.
        for _ in 0..c.num_probes {
            assert!(!c.is_healthy());
            assert!(c.should_probe());
            assert_reset_is_nop(&c.requests, c.num_probes);
            c.requests.observe::<(), ()>(&Ok(()));
        }

        // Upon reaching NUM_PROBES of entirely successful requests, the circuit
        // closes and is healthy again.
        assert!(c.is_healthy());
    }

    /// The circuit is marked unhealthy if the error rate exceeds
    /// MAX_ERROR_RATIO within a single error window (approximately).
    ///
    /// This test ensures the counter reset logic prevents errors from different
    /// error window periods from changing the circuit to open/unhealthy.
    #[tokio::test]
    async fn test_periodic_counter_reset() {
        let c = CircuitBreaker::new("bananas", Duration::from_secs(5), 10);

        // Assert the circuit breaker as healthy.
        assert!(c.is_healthy());
        assert_reset_is_nop(&c.requests, c.num_probes);

        // Calculate how many errors are needed to mark the circuit breaker as
        // unhealthy.
        let n = errors_to_unhealthy(&c.requests);

        // Ensure N-1 does not mark the circuit unhealthy.
        for _ in 0..(n - 1) {
            assert!(c.is_healthy());
            c.requests.observe::<(), ()>(&Err(()));
        }

        assert!(c.is_healthy());

        // Reset the counters for the new error observation window
        let v = c.requests.read();
        reset_closed_state_counters(&c.requests, c.num_probes);
        assert_ne!(v, c.requests.read());

        assert!(c.is_healthy());

        // Ensure N-1 still does not mark the circuit unhealthy.
        for _ in 0..(n - 1) {
            assert!(c.is_healthy());
            c.requests.observe::<(), ()>(&Err(()));
        }

        // But the final error observation in this window does.
        c.requests.observe::<(), ()>(&Err(()));
        assert!(!c.is_healthy());
    }

    /// A multi-threaded fuzz test that ensures no matter what sequence of
    /// events occur, the circuit can always be driven healthy by probing
    /// successfully.
    #[tokio::test]
    async fn test_recovery_fuzz() {
        const OBSERVATIONS_PER_THREAD: usize = 100_000;
        const N_THREADS: usize = 20;

        let c = Arc::new(new_no_reset());

        assert!(c.is_healthy());
        assert!(!c.should_probe());

        let handles = (0..N_THREADS)
            .map(|_| {
                std::thread::spawn({
                    let c = Arc::clone(&c);
                    move || {
                        for _ in 0..OBSERVATIONS_PER_THREAD {
                            if c.is_healthy() || c.should_probe() {
                                if random::<bool>() {
                                    c.observe::<(), ()>(&Ok(()))
                                } else {
                                    c.observe::<(), ()>(&Err(()))
                                }
                            }
                            // Every now and again (with a 1 in 50 probability),
                            // the error window advances and the counters are
                            // reset.
                            if (random::<usize>() % 50) == 0 {
                                reset_closed_state_counters(&c.requests, c.num_probes);
                            }
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        // Wait for the threads to complete driving the circuit breaker.
        for h in handles {
            h.join().unwrap();
        }

        // Pretend time advanced, ensuring the circuit breaker will start
        // probing if needed.
        c.probes.lock().probe_window_started_at = Some(
            Instant::now()
                .checked_sub(PROBE_INTERVAL + Duration::from_nanos(1))
                .expect("instant cannot roll back far enough - test issue, not code issue"),
        );

        // Drive successful probes if needed.
        for _ in 0..c.num_probes {
            if c.should_probe() {
                c.requests.observe::<(), ()>(&Ok(()));
            }
        }

        // Invariant: The circuit breaker must be healthy, either because it was
        // healthy at the end of the fuzz threads, or because it was
        // successfully probed and driven healthy afterwards.
        assert!(c.is_healthy(), "{c:?}");
    }

    /// Assert that when configured for low write volumes, a single successful
    /// request is sufficient to drive the state to "healthy".
    #[tokio::test]
    async fn test_low_volume_configuration() {
        let c = CircuitBreaker::new("bananas", Duration::from_secs(5), 1);
        c.reset_task.abort();

        // Assert the circuit breaker as healthy.
        assert!(c.is_healthy());
        assert_reset_is_nop(&c.requests, c.num_probes);

        // Calculate how many errors are needed to mark the circuit breaker as
        // unhealthy.
        let n = errors_to_unhealthy(&c.requests);

        // Drive the circuit to unhealthy.
        for _ in 0..n {
            assert!(c.is_healthy());
            c.requests.observe::<(), ()>(&Err(()));
        }
        assert!(!c.is_healthy());

        // Observe a single successful request
        assert!(c.should_probe());
        c.requests.observe::<(), ()>(&Ok(()));

        // And the circuit should now be healthy.
        assert!(c.is_healthy());
    }
}
