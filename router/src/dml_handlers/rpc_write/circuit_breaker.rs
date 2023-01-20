#![allow(unused)]
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

/// The error ratio that must be reached within [`ERROR_WINDOW`] to open the
/// [`CircuitBreaker`].
const MAX_ERROR_RATIO: f32 = 0.8;
/// The (discrete) slices of time in which the error ration must exceed
/// [`MAX_ERROR_RATIO`] to cause the [`CircuitBreaker`] to open.
const ERROR_WINDOW: Duration = Duration::from_secs(5);
/// The number of probe requests to send when half-open / probing.
const NUM_PROBES: u64 = 10;
/// The frequency at which up to [`NUM_PROBES`] are sent when half-open /
/// probing.
const PROBE_INTERVAL: Duration = Duration::from_secs(1);

/// A low-overhead, error ratio gated [`CircuitBreaker`].
///
/// The circuit breaker opens when 80% of requests within a 5 second window
/// fail. The breaker then closes when the error rate falls below 80% for 10
/// probes within 1 second.
///
/// The circuit breaker initialises in the "closed"/healthy state.
///
/// # States
///
/// A circuit breaker has 3 states, described below with implementation specific
/// notes:
///
///   * Closed: The "healthy" state - all requests are allowed. If the ratio of
///     errors exceeds [`MAX_ERROR_RATIO`] within a single [`ERROR_WINDOW`]
///     duration of time, the circuit breaker transitions to "open".
///
///   * Open: The "unhealthy" state - requests are refused. After
///     [`PROBE_INTERVAL`] delay, the circuit breaker transitions to half-open
///     and begins probing the remote.
///
///   * Half-open: A transition state; a majority of traffic is refused, but up
///     to [`NUM_PROBES`] number of requests are allowed per [`PROBE_INTERVAL`].
///     Once the probes are sent, the error ratio is evaluated, and the system
///     returns to either open or closed as appropriate.
///
/// # Error Ratio / Opening
///
/// Successful requests and errors are recorded when passed to
/// [`CircuitBreaker::observe()`]. These counters are reset at intervals of
/// [`ERROR_WINDOW`], meaning at the ratio of errors must exceed
/// [`MAX_ERROR_RATIO`] within a single window to open the circuit breaker.
///
/// Error ratios are measured continuously, instantly transitioning to the
/// open/unhealthy state once [`MAX_ERROR_RATIO`] is exceeded.
///
/// This continuous evaluation and discrete time windows ensure timely opening
/// of the breaker even if there have been large numbers of successful requests
/// previously.
///
/// # Probing / Closing
///
/// Once a circuit breaker transitions to "open/unhealthy", up to [`NUM_PROBES`]
/// are allowed per [`PROBE_INTERVAL`] - this is referred to as "probing",
/// allowing the client to discover the state of the (potentially unavailable)
/// remote while bounding the number of requests that may fail as a result.
///
/// Whilst in the probing state, the result of each request is recorded - once
/// at least [`NUM_PROBES`] requests have been completed, and the ratio of
/// errors drops below [`MAX_ERROR_RATIO`], the circuit breaker breaker
/// transitions to "closed/healthy".
///
/// If there are not enough requests made to exceed [`NUM_PROBES`], all requests
/// are probes.
#[derive(Debug)]
pub(crate) struct CircuitBreaker {
    /// Counters tracking the number of [`Ok`] and [`Err`] observed in the
    /// current [`ERROR_WINDOW`].
    ///
    /// When the total number of requests ([`RequestCounterValue::total()`]) is
    /// less than [`NUM_PROBES`], the circuit is in the "probing" regime. When
    /// the number of requests is greater than this amount, the circuit
    /// open/closed state depends on the current error ratio.
    requests: Arc<RequestCounter>,

    /// The slow-path probing state, taken when the circuit is open, tracking
    /// how many probes need to take place and resetting the [`PROBE_INTERVAL`].
    probes: Mutex<ProbeState>,

    /// A task to reset the request count at intervals of [`ERROR_WINDOW`].
    reset_task: JoinHandle<()>,
}

#[derive(Debug, Default)]
struct ProbeState {
    last_probe: Option<Instant>,
    probes_started: u64,
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        let requests = Arc::new(RequestCounter::default());
        let s = Self {
            requests: Arc::clone(&requests),
            probes: Mutex::new(ProbeState::default()),
            reset_task: tokio::spawn(async move {
                let mut ticker = tokio::time::interval(ERROR_WINDOW);
                ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    reset_closed_state_counters(&requests);
                }
            }),
        };
        s.set_healthy();
        s
    }
}

impl CircuitBreaker {
    /// Force-set the state of the circuit breaker to "closed" / healthy.
    pub(crate) fn set_healthy(&self) {
        self.requests.set(NUM_PROBES as u32, 0);
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

        // A lower-bound on the number of observations required to usefully
        // compute an error ratio.
        //
        // When the circuit breaker is first initialised, it starts in a probing
        // state and drives at most NUM_PROBES number of probes.
        if is_probing(counts) {
            return false;
        }

        is_healthy(counts)
    }

    /// Return `true` if the caller should begin a probe request to the
    /// unhealthy, potentially unavailable endpoint. Always returns `false` if
    /// `self` is in the "closed/healthy" state.
    ///
    /// This method will return `true` at most [`NUM_PROBES`] per
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
    /// requests are in addition to the [`NUM_PROBES`] guaranteed to observe
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
        match guard.last_probe {
            Some(p) if now.duration_since(p) > PROBE_INTERVAL => {
                debug!("remote unavailable, probing");

                // It is time to begin probing again.
                assert!(guard.probes_started <= NUM_PROBES);
                guard.last_probe = Some(now);
                guard.probes_started = 0;

                // Reset the request success/error counters.
                //
                // The probes populate the request counters, which in turn drive
                // the health check.  In order to open the circuit breaker, at
                // least NUM_PROBES number of probes are needed, and the ratio
                // of probe errors must not exceed MAX_ERROR_RATIO.
                self.requests.set(0, 0);
            }
            Some(_p) => {
                // Probing is ongoing.
                //
                // If there have already been the configured number of probes,
                // do not send more.
                if guard.probes_started >= NUM_PROBES {
                    debug!("remote unavailable, probes exhausted");
                    return false;
                }
            }
            None => {
                // Drive this circuit breaker to closed from the initial state.
                guard.last_probe = Some(now);
                assert_eq!(guard.probes_started, 0);
                self.requests.set(0, 0);
            }
        }

        // Record this probe was started.
        guard.probes_started += 1;

        debug!(
            nth_probe = guard.probes_started,
            max_probes = NUM_PROBES,
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
fn is_probing(counts: RequestCounterValue) -> bool {
    // When there are less than NUM_PROBES, the circuit is not closed, as it is
    // either:
    //
    //  * Driving to closed during initialisation via probing
    //  * Driving to closed after being opened via probing
    counts.total() < NUM_PROBES
}

/// Return `true` if the current ratio of errors is below MAX_ERROR_RATIO,
/// irrespective of the current circuit state.
#[inline]
fn is_healthy(counts: RequestCounterValue) -> bool {
    let pcnt = counts.errors() as f32 / counts.successes() as f32;
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
/// must be NUM_PROBES * MAX_ERROR_RATIO number of failed requests to open the
/// circuit.
///
/// Retains the closed state of the circuit. This is NOT an atomic operation.
fn reset_closed_state_counters(counters: &RequestCounter) {
    let counts = counters.read();
    if !is_healthy(counts) || is_probing(counts) {
        return;
    }
    counters.set(NUM_PROBES as u32, 0);
}

/// An store of two `u32` that can be read atomically; one tracking successful
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
    use super::*;

    /// Assert that calling [`reset_closed_state_counters`] does nothing.
    #[track_caller]
    fn assert_reset_is_nop(counters: &RequestCounter) {
        let v = counters.read();
        reset_closed_state_counters(counters);
        assert_eq!(v, counters.read());
    }

    /// Helper to calculate the number of errors needed to close the circuit
    /// breaker.
    fn errors_to_close(counters: &RequestCounter) -> usize {
        (counters.read().total() as f32 * MAX_ERROR_RATIO).ceil() as usize
    }

    /// Return a new [`CircuitBreaker`] with the reset ticker disabled.
    fn new_no_reset() -> CircuitBreaker {
        let c = CircuitBreaker::default();
        c.reset_task.abort();
        c
    }

    /// Assert that a newly initialised circuit breaker starts in the fully
    /// closed state, is driven open, and allows NUM_PROBES per PROBE_INTERVAL
    /// when probing.
    #[tokio::test]
    async fn test_init_closed_drive_open_probe_recovery() {
        let c = new_no_reset();

        // The circuit breaker starts in an healthy state.
        assert!(c.is_healthy());
        assert!(!c.should_probe());
        assert_reset_is_nop(&c.requests);

        // Observe enough errors to open
        let n = errors_to_close(&c.requests);
        for _ in 0..n {
            assert!(c.is_healthy());
            assert!(!c.should_probe());
            c.requests.observe::<(), ()>(&Err(()));
        }

        // It should then transition to the probe state, and allow the
        // configured amount of probe requests.
        for _ in 0..NUM_PROBES {
            assert!(!c.is_healthy());
            assert!(c.should_probe());
            // Counter resets should not be allowed when the circuit is not
            // healthy.
            assert_reset_is_nop(&c.requests);
        }

        // And once NUM_PROBES is reached, stop allowing more probes.
        //
        // It should remain unhealthy during this time.
        assert!(!c.should_probe());
        assert!(!c.is_healthy());
        assert_reset_is_nop(&c.requests);

        // Pretend it is time to probe again.
        c.probes.lock().last_probe = Some(
            Instant::now()
                .checked_sub(PROBE_INTERVAL + Duration::from_nanos(1))
                .expect("instant cannot roll back far enough - test issue, not code issue"),
        );

        for _ in 0..(NUM_PROBES - 1) {
            // Recording a successful probe request should not close the circuit
            // until the NUM_PROBES has been observed.
            assert!(c.should_probe());
            assert!(!c.is_healthy());
            c.requests.observe::<(), ()>(&Ok(()));
            assert_reset_is_nop(&c.requests);
        }

        // Upon reaching NUM_PROBES of entirely successful requests, the circuit
        // closes.
        assert!(!c.is_healthy());
        assert!(c.should_probe());
        c.requests.observe::<(), ()>(&Ok(()));
        assert!(c.is_healthy());
        assert!(!c.should_probe());
    }

    /// A circuit breaker is initialised in the healthy state, driven open in
    /// response to sufficient error observations, and does not recover due to
    /// insufficient successful probes.
    #[tokio::test]
    async fn test_probe_insufficient_success() {
        let c = new_no_reset();

        assert!(c.is_healthy());
        assert!(!c.should_probe());

        // Observe enough errors to open
        let n = errors_to_close(&c.requests);
        for _ in 0..n {
            assert!(c.is_healthy());
            c.requests.observe::<(), ()>(&Err(()));
        }

        // The circuit breaker is now in an unhealthy/probe state and should
        // allow the configured amount of probe requests to drive to a closed
        // state.
        //
        // Observing half of them as failing should end probing until the next
        // probe period.
        let n_failed = NUM_PROBES / 2;
        for _ in 0..(n_failed) {
            assert!(!c.is_healthy());
            assert!(c.should_probe());
            assert_reset_is_nop(&c.requests);
            c.requests.observe::<(), ()>(&Ok(()));
        }
        for _ in 0..(NUM_PROBES - n_failed) {
            assert!(!c.is_healthy());
            assert!(c.should_probe());
            assert_reset_is_nop(&c.requests);
            c.requests.observe::<(), ()>(&Err(()));
        }
        assert_eq!(c.requests.read().total(), NUM_PROBES);

        // The probes did not drive the circuit breaker closed.
        assert!(!c.is_healthy());
        // And no more probes should be allowed.
        assert!(!c.should_probe());
        // The request counters should not reset when unhealthy.
        assert_reset_is_nop(&c.requests);

        // Pretend it is time to probe again.
        c.probes.lock().last_probe = Some(
            Instant::now()
                .checked_sub(PROBE_INTERVAL + Duration::from_nanos(1))
                .expect("instant cannot roll back far enough - test issue, not code issue"),
        );

        // Do the probe requests, all succeeding.
        for _ in 0..NUM_PROBES {
            assert!(!c.is_healthy());
            assert!(c.should_probe());
            assert_reset_is_nop(&c.requests);
            c.requests.observe::<(), ()>(&Ok(()));
        }

        // Upon reaching NUM_PROBES of entirely successful requests, the circuit
        // closes and is healthy again.
        assert!(c.is_healthy());
    }

    /// The circuit closes if the error rate exceeds MAX_ERROR_RATIO within a
    /// single ERROR_WINDOW (approximately).
    ///
    /// This test ensures the counter reset logic prevents errors from discrete
    /// ERROR_WINDOW periods from opening the circuit.
    #[tokio::test]
    async fn test_periodic_counter_reset() {
        let c = CircuitBreaker::default();

        // Assert the circuit breaker as healthy.
        assert!(c.is_healthy());
        assert_reset_is_nop(&c.requests);

        // Calculate how many errors are needed to close the circuit breaker.
        let n = errors_to_close(&c.requests);

        // Ensure N-1 does not close the circuit.
        for _ in 0..(n - 1) {
            assert!(c.is_healthy());
            c.requests.observe::<(), ()>(&Err(()));
        }

        assert!(c.is_healthy());

        // Reset the counters for the new observation window
        let v = c.requests.read();
        reset_closed_state_counters(&c.requests);
        assert_ne!(v, c.requests.read());

        assert!(c.is_healthy());

        // Ensure N-1 still does not close the circuit.
        for _ in 0..(n - 1) {
            assert!(c.is_healthy());
            c.requests.observe::<(), ()>(&Err(()));
        }

        // But the final error observation in this window does.
        c.requests.observe::<(), ()>(&Err(()));
        assert!(!c.is_healthy());
    }
}
