//! Backoff functionality.
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro
)]
use observability_deps::tracing::warn;
use rand::prelude::*;
use snafu::Snafu;
use std::ops::ControlFlow;
use std::time::Duration;

/// Exponential backoff with jitter
///
/// See <https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/>
#[derive(Debug, Clone, PartialEq)]
#[allow(missing_copy_implementations)]
pub struct BackoffConfig {
    /// Initial backoff.
    pub init_backoff: Duration,

    /// Maximum backoff.
    pub max_backoff: Duration,

    /// Multiplier for each backoff round.
    pub base: f64,

    /// Timeout until we try to retry.
    pub deadline: Option<Duration>,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            init_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(500),
            base: 3.,
            deadline: None,
        }
    }
}

/// Error after giving up retrying.
#[derive(Debug, Snafu, PartialEq, Eq)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum BackoffError<E>
where
    E: std::error::Error + 'static,
{
    #[snafu(display("Retry did not exceed within {deadline:?}: {source}"))]
    DeadlineExceeded { deadline: Duration, source: E },
}

/// Backoff result.
pub type BackoffResult<T, E> = Result<T, BackoffError<E>>;

/// [`Backoff`] can be created from a [`BackoffConfig`]
///
/// Consecutive calls to [`Backoff::next`] will return the next backoff interval
///
pub struct Backoff {
    init_backoff: f64,
    next_backoff_secs: f64,
    max_backoff_secs: f64,
    base: f64,
    total: f64,
    deadline: Option<f64>,
    rng: Option<Box<dyn RngCore + Sync + Send>>,
}

impl std::fmt::Debug for Backoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backoff")
            .field("init_backoff", &self.init_backoff)
            .field("next_backoff_secs", &self.next_backoff_secs)
            .field("max_backoff_secs", &self.max_backoff_secs)
            .field("base", &self.base)
            .field("total", &self.total)
            .field("deadline", &self.deadline)
            .finish()
    }
}

impl Backoff {
    /// Create a new [`Backoff`] from the provided [`BackoffConfig`].
    ///
    /// # Pancis
    /// Panics if [`BackoffConfig::base`] is not finite or < 1.0.
    pub fn new(config: &BackoffConfig) -> Self {
        Self::new_with_rng(config, None)
    }

    /// Creates a new `Backoff` with the optional `rng`.
    ///
    /// Used [`rand::thread_rng()`] if no rng provided.
    ///
    /// See [`new`](Self::new) for panic handling.
    pub fn new_with_rng(
        config: &BackoffConfig,
        rng: Option<Box<dyn RngCore + Sync + Send>>,
    ) -> Self {
        assert!(
            config.base.is_finite(),
            "Backoff base ({}) must be finite.",
            config.base,
        );
        assert!(
            config.base >= 1.0,
            "Backoff base ({}) must be greater or equal than 1.",
            config.base,
        );

        let max_backoff = config.max_backoff.as_secs_f64();
        let init_backoff = config.init_backoff.as_secs_f64().min(max_backoff);
        Self {
            init_backoff,
            next_backoff_secs: init_backoff,
            max_backoff_secs: max_backoff,
            base: config.base,
            total: 0.0,
            deadline: config.deadline.map(|d| d.as_secs_f64()),
            rng,
        }
    }

    /// Fade this backoff over to a different backoff config.
    pub fn fade_to(&mut self, config: &BackoffConfig) {
        // Note: `new` won't have the same RNG, but this doesn't matter
        let new = Self::new(config);

        *self = Self {
            init_backoff: new.init_backoff,
            next_backoff_secs: self.next_backoff_secs,
            max_backoff_secs: new.max_backoff_secs,
            base: new.base,
            total: self.total,
            deadline: new.deadline,
            rng: self.rng.take(),
        };
    }

    /// Perform an async operation that retries with a backoff
    pub async fn retry_with_backoff<F, F1, B, E>(
        &mut self,
        task_name: &str,
        mut do_stuff: F,
    ) -> BackoffResult<B, E>
    where
        F: (FnMut() -> F1) + Send,
        F1: std::future::Future<Output = ControlFlow<B, E>> + Send,
        E: std::error::Error + Send + 'static,
    {
        loop {
            // first execute `F` and then use it, so we can avoid `F: Sync`.
            let do_stuff = do_stuff();

            let e = match do_stuff.await {
                ControlFlow::Break(r) => break Ok(r),
                ControlFlow::Continue(e) => e,
            };

            let backoff = match self.next() {
                Some(backoff) => backoff,
                None => {
                    return Err(BackoffError::DeadlineExceeded {
                        deadline: Duration::from_secs_f64(self.deadline.expect("deadline")),
                        source: e,
                    });
                }
            };

            warn!(
                error=%e,
                task_name,
                backoff_secs = backoff.as_secs(),
                "request encountered non-fatal error - backing off",
            );
            tokio::time::sleep(backoff).await;
        }
    }

    /// Retry all errors.
    pub async fn retry_all_errors<F, F1, B, E>(
        &mut self,
        task_name: &str,
        mut do_stuff: F,
    ) -> BackoffResult<B, E>
    where
        F: (FnMut() -> F1) + Send,
        F1: std::future::Future<Output = Result<B, E>> + Send,
        E: std::error::Error + Send + 'static,
    {
        self.retry_with_backoff(task_name, move || {
            // first execute `F` and then use it, so we can avoid `F: Sync`.
            let do_stuff = do_stuff();

            async {
                match do_stuff.await {
                    Ok(b) => ControlFlow::Break(b),
                    Err(e) => ControlFlow::Continue(e),
                }
            }
        })
        .await
    }
}

impl Iterator for Backoff {
    type Item = Duration;

    /// Returns the next backoff duration to wait for, if any
    fn next(&mut self) -> Option<Self::Item> {
        let range = self.init_backoff..=(self.next_backoff_secs * self.base);

        let rand_backoff = match self.rng.as_mut() {
            Some(rng) => rng.gen_range(range),
            None => thread_rng().gen_range(range),
        };

        let next_backoff = self.max_backoff_secs.min(rand_backoff);
        self.total += next_backoff;
        let res = std::mem::replace(&mut self.next_backoff_secs, next_backoff);
        if let Some(deadline) = self.deadline {
            if self.total >= deadline {
                return None;
            }
        }
        duration_try_from_secs_f64(res)
    }
}

const MAX_F64_SECS: f64 = 1_000_000.0;

/// Try to get `Duration` from `f64` secs.
///
/// This is required till <https://github.com/rust-lang/rust/issues/83400> is resolved.
fn duration_try_from_secs_f64(secs: f64) -> Option<Duration> {
    (secs.is_finite() && (0.0..=MAX_F64_SECS).contains(&secs))
        .then(|| Duration::from_secs_f64(secs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::mock::StepRng;

    #[test]
    fn test_backoff() {
        let init_backoff_secs = 1.;
        let max_backoff_secs = 500.;
        let base = 3.;

        let config = BackoffConfig {
            init_backoff: Duration::from_secs_f64(init_backoff_secs),
            max_backoff: Duration::from_secs_f64(max_backoff_secs),
            deadline: None,
            base,
        };

        let assert_fuzzy_eq = |a: f64, b: f64| assert!((b - a).abs() < 0.0001, "{a} != {b}");

        // Create a static rng that takes the minimum of the range
        let rng = Box::new(StepRng::new(0, 0));
        let mut backoff = Backoff::new_with_rng(&config, Some(rng));

        for _ in 0..20 {
            assert_eq!(backoff.next().unwrap().as_secs_f64(), init_backoff_secs);
        }

        // Create a static rng that takes the maximum of the range
        let rng = Box::new(StepRng::new(u64::MAX, 0));
        let mut backoff = Backoff::new_with_rng(&config, Some(rng));

        for i in 0..20 {
            let value = (base.powi(i) * init_backoff_secs).min(max_backoff_secs);
            assert_fuzzy_eq(backoff.next().unwrap().as_secs_f64(), value);
        }

        // Create a static rng that takes the mid point of the range
        let rng = Box::new(StepRng::new(u64::MAX / 2, 0));
        let mut backoff = Backoff::new_with_rng(&config, Some(rng));

        let mut value = init_backoff_secs;
        for _ in 0..20 {
            assert_fuzzy_eq(backoff.next().unwrap().as_secs_f64(), value);
            value =
                (init_backoff_secs + (value * base - init_backoff_secs) / 2.).min(max_backoff_secs);
        }

        // deadline
        let rng = Box::new(StepRng::new(u64::MAX, 0));
        let deadline = Duration::from_secs_f64(init_backoff_secs);
        let mut backoff = Backoff::new_with_rng(
            &BackoffConfig {
                deadline: Some(deadline),
                ..config
            },
            Some(rng),
        );
        assert_eq!(backoff.next(), None);
    }

    #[test]
    fn test_overflow() {
        let rng = Box::new(StepRng::new(u64::MAX, 0));
        let cfg = BackoffConfig {
            init_backoff: Duration::MAX,
            max_backoff: Duration::MAX,
            ..Default::default()
        };
        let mut backoff = Backoff::new_with_rng(&cfg, Some(rng));
        assert_eq!(backoff.next(), None);
    }

    #[test]
    fn test_duration_try_from_f64() {
        for d in [-0.1, f64::INFINITY, f64::NAN, MAX_F64_SECS + 0.1] {
            assert!(duration_try_from_secs_f64(d).is_none());
        }

        for d in [0.0, MAX_F64_SECS] {
            assert!(duration_try_from_secs_f64(d).is_some());
        }
    }

    #[test]
    fn test_max_backoff_smaller_init() {
        let rng = Box::new(StepRng::new(u64::MAX, 0));
        let cfg = BackoffConfig {
            init_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(1),
            ..Default::default()
        };
        let mut backoff = Backoff::new_with_rng(&cfg, Some(rng));
        assert_eq!(backoff.next(), Some(Duration::from_secs(1)));
        assert_eq!(backoff.next(), Some(Duration::from_secs(1)));
    }

    #[test]
    #[should_panic(expected = "Backoff base (inf) must be finite.")]
    fn test_panic_inf_base() {
        let cfg = BackoffConfig {
            base: f64::INFINITY,
            ..Default::default()
        };
        Backoff::new(&cfg);
    }

    #[test]
    #[should_panic(expected = "Backoff base (NaN) must be finite.")]
    fn test_panic_nan_base() {
        let cfg = BackoffConfig {
            base: f64::NAN,
            ..Default::default()
        };
        Backoff::new(&cfg);
    }

    #[test]
    #[should_panic(expected = "Backoff base (0) must be greater or equal than 1.")]
    fn test_panic_zero_base() {
        let cfg = BackoffConfig {
            base: 0.0,
            ..Default::default()
        };
        Backoff::new(&cfg);
    }

    #[test]
    fn test_constant_backoff() {
        let rng = Box::new(StepRng::new(u64::MAX, 0));
        let cfg = BackoffConfig {
            init_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(1),
            base: 1.0,
            ..Default::default()
        };
        let mut backoff = Backoff::new_with_rng(&cfg, Some(rng));
        assert_eq!(backoff.next(), Some(Duration::from_secs(1)));
        assert_eq!(backoff.next(), Some(Duration::from_secs(1)));
    }
}
