//! Custom panic hook that sends the panic information to a tracing
//! span

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro
)]

use std::{fmt, panic, sync::Arc};

use metric::U64Counter;
use observability_deps::tracing::{error, warn};
use panic::PanicInfo;

type PanicFunctionPtr = Arc<Box<dyn Fn(&PanicInfo<'_>) + Sync + Send + 'static>>;

/// RAII guard that installs a custom panic hook to send panic
/// information to tracing.
///
/// Upon construction registers a custom panic
/// hook which sends the panic to tracing first, before calling any
/// prior panic hook.
///
/// Upon drop, restores the pre-existing panic hook
#[derive(Default)]
pub struct SendPanicsToTracing {
    /// The previously installed panic hook -- Note it is wrapped in an
    /// `Option` so we can `.take` it during the call to `drop()`;
    old_panic_hook: Option<PanicFunctionPtr>,
}

impl SendPanicsToTracing {
    pub fn new() -> Self {
        let current_panic_hook: PanicFunctionPtr = Arc::new(panic::take_hook());
        let old_panic_hook = Some(Arc::clone(&current_panic_hook));
        panic::set_hook(Box::new(move |info| {
            tracing_panic_hook(&current_panic_hook, info)
        }));

        Self { old_panic_hook }
    }

    /// Configure this panic handler to emit a panic count metric.
    ///
    /// The metric is named `thread_panic_count_total` and is incremented each
    /// time the panic handler is invoked.
    pub fn with_metrics(self, metrics: &metric::Registry) -> Self {
        let panic_count = metrics
            .register_metric::<U64Counter>("thread_panic_count", "number of thread panics observed")
            .recorder(&[]);

        let old_hook = Arc::clone(self.old_panic_hook.as_ref().expect("no hook set"));
        panic::set_hook(Box::new(move |info| {
            panic_count.inc(1);
            tracing_panic_hook(&old_hook, info)
        }));

        self
    }
}

// can't derive because the function pointer doesn't implement Debug
impl fmt::Debug for SendPanicsToTracing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendPanicsToTracing").finish()
    }
}

impl Drop for SendPanicsToTracing {
    fn drop(&mut self) {
        if std::thread::panicking() {
            warn!("Can't reset old panic hook as we are currently panicking");
            return;
        }

        if let Some(old_panic_hook) = self.old_panic_hook.take() {
            // since `old_panic_hook` is an `Arc` - at this point it
            // should have two references -- the captured closure as
            // well as `self`.

            // Temporarily install a dummy hook that does nothing. We
            // need to release the ref count in the closure of the
            // panic handler.
            panic::set_hook(Box::new(|_| {
                println!("This panic hook should 'never' be called");
            }));

            if let Ok(old_panic_hook) = Arc::try_unwrap(old_panic_hook) {
                panic::set_hook(Box::new(old_panic_hook))
            } else {
                // Should not happen -- but could if the panic handler
                // was still running while this code is being executed
                warn!("Can't reset old panic hook, old hook still has more than one reference");
            }
        } else {
            // This is a "shouldn't happen" type error
            warn!("Can't reset old panic hook, old hook was None...");
        }
    }
}

fn tracing_panic_hook(other_hook: &PanicFunctionPtr, panic_info: &PanicInfo<'_>) {
    // Attempt to replicate the standard format:
    error!(panic_info=%panic_info, "Thread panic");

    // Call into the previous panic function (typically the standard
    // panic function)
    other_hook(panic_info)
}

/// Ensure panics are fatal events by exiting the process with an exit code of
/// 1 after calling the existing panic handler, if any.
pub fn make_panics_fatal() {
    let existing = panic::take_hook();

    panic::set_hook(Box::new(move |info| {
        // Call the existing panic hook.
        existing(info);
        // Exit the process.
        //
        // NOTE: execution may not reach this point if another hook
        // kills the process first.
        std::process::exit(1);
    }));
}

#[cfg(test)]
mod tests {
    use metric::{Attributes, Metric};

    use super::*;

    fn assert_count(metrics: &metric::Registry, count: u64) {
        let got = metrics
            .get_instrument::<Metric<U64Counter>>("thread_panic_count")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[]))
            .expect("failed to get observer")
            .fetch();
        assert_eq!(got, count);
    }

    #[test]
    fn test_panic_counter() {
        let metrics = metric::Registry::default();
        let _guard = SendPanicsToTracing::new().with_metrics(&metrics);

        assert_count(&metrics, 0);

        std::thread::spawn(|| {
            panic!("it's bananas");
        })
        .join()
        .expect_err("wat");

        assert_count(&metrics, 1);
    }
}
