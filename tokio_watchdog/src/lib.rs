//! Monitors if the tokio runtime still looks healthy.
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

use observability_deps::tracing::warn;

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use std::time::{Duration, Instant};

use metric::{DurationHistogram, Registry, U64Counter};
use tokio::{
    runtime::Handle,
    sync::mpsc::{channel, error::TryRecvError},
};

/// Tokio watchdog config.
#[allow(missing_debug_implementations)]
pub struct WatchdogConfig<'a> {
    handle: &'a Handle,
    metric_registry: &'a Registry,
    runtime_name: &'static str,
    tick_duration: Duration,
    warn_threshold: Duration,
    new_thread_hook: Option<Box<dyn FnOnce() + Send>>,
}

impl<'a> WatchdogConfig<'a> {
    /// Create new config for given runtime handle and metric registry.
    #[must_use]
    pub fn new(handle: &'a Handle, metric_registry: &'a Registry) -> Self {
        Self {
            handle,
            metric_registry,
            runtime_name: "tokio",
            tick_duration: Duration::from_millis(100),
            warn_threshold: Duration::from_millis(100),
            new_thread_hook: None,
        }
    }

    /// Set runtime name.
    #[must_use]
    pub fn with_runtime_name(self, name: &'static str) -> Self {
        Self {
            runtime_name: name,
            ..self
        }
    }

    /// Set tick duration.
    ///
    /// The tick duration determines how often the alive check will be performed.
    #[must_use]
    pub fn with_tick_duration(self, d: Duration) -> Self {
        Self {
            tick_duration: d,
            ..self
        }
    }

    /// Set warn duration.
    ///
    /// Determines how long the watchdog waits after each check before it detects a hang.
    #[must_use]
    pub fn with_warn_duration(self, d: Duration) -> Self {
        Self {
            warn_threshold: d,
            ..self
        }
    }

    /// Sets a hook that is called when the watchdog thread is created.
    ///
    /// The hook is called from the new thread.
    #[must_use]
    pub fn with_new_thread_hook<F>(self, f: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        Self {
            new_thread_hook: Some(Box::new(f)),
            ..self
        }
    }

    /// Install watchdog.
    ///
    /// # Panic
    /// Panics if the sum of [tick duration](Self::with_tick_duration) and [warn duration](Self::with_warn_duration) is zero.
    pub fn install(self) {
        let Self {
            handle,
            metric_registry,
            runtime_name,
            tick_duration,
            warn_threshold,
            new_thread_hook,
        } = self;

        assert!(
            !(tick_duration + warn_threshold).is_zero(),
            "sum of tick and warn duration must be non-zero"
        );

        let (tx_request, mut rx_request) = channel::<Instant>(1);
        let (tx_response, mut rx_response) = channel::<Duration>(1);

        let metric_latency = metric_registry
            .register_metric::<DurationHistogram>(
                "tokio_watchdog_response_time",
                "Response time of the tokio watchdog task",
            )
            .recorder(&[("runtime", runtime_name)]);
        let metric_hang = metric_registry
            .register_metric::<U64Counter>(
                "tokio_watchdog_hangs",
                "Number of hangs detected by the tokio watchdog",
            )
            .recorder(&[("runtime", runtime_name)]);

        handle.spawn(async move {
            loop {
                let Some(start) = rx_request.recv().await else {
                    return;
                };

                if tx_response.try_send(start.elapsed()).is_err() {
                    return;
                }
            }
        });

        std::thread::Builder::new()
            .name(format!("tokio watchdog {runtime_name}"))
            .spawn(move || {
                if let Some(hook) = new_thread_hook {
                    hook();
                }

                loop {
                    std::thread::sleep(tick_duration);

                    if tx_request.try_send(Instant::now()).is_err() {
                        return;
                    }

                    std::thread::sleep(warn_threshold);

                    let d = match rx_response.try_recv() {
                        Ok(d) => d,
                        Err(TryRecvError::Empty) => {
                            warn!(runtime = runtime_name, "tokio starts hanging",);
                            metric_hang.inc(1);

                            let Some(d) = rx_response.blocking_recv() else {
                                return;
                            };
                            warn!(
                                runtime = runtime_name,
                                hang_secs = d.as_secs_f64(),
                                "tokio stops hanging",
                            );
                            d
                        }
                        Err(TryRecvError::Disconnected) => {
                            return;
                        }
                    };

                    metric_latency.record(d);
                }
            })
            .expect("start watchdog thread");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use test_helpers::tracing::TracingCapture;

    use super::*;

    #[tokio::test]
    #[should_panic(expected = "sum of tick and warn duration must be non-zero")]
    async fn test_panic_zero_duration() {
        let registry = Registry::default();
        WatchdogConfig::new(&Handle::current(), &registry)
            .with_tick_duration(Duration::ZERO)
            .with_warn_duration(Duration::ZERO)
            .install();
    }

    #[tokio::test]
    async fn test() {
        let capture = Arc::new(TracingCapture::new());
        let registry = Registry::default();
        let tick_duration = Duration::from_millis(100);
        let warn_threshold = Duration::from_millis(200);

        let capture2 = Arc::clone(&capture);
        WatchdogConfig::new(&Handle::current(), &registry)
            .with_tick_duration(tick_duration)
            .with_warn_duration(warn_threshold)
            .with_new_thread_hook(move || {
                capture2.register_in_current_thread();
            })
            .install();

        std::thread::sleep(warn_threshold * 2);
        tokio::time::sleep(tick_duration * 2).await;

        let logs = capture.to_string();
        assert!(logs.contains("tokio starts hanging"));
        assert!(logs.contains("tokio stops hanging"));
    }
}
