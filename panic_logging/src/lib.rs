//! Custom panic hook that sends the panic information to a tracing
//! span

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use std::{fmt, panic, sync::Arc};

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
}

// recommended by clippy
impl Default for SendPanicsToTracing {
    fn default() -> Self {
        Self::new()
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
    // thread 'main' panicked at 'foo', src/libstd/panicking.rs:106:9
    let log_panic = |s: &str| {
        let location_string = match panic_info.location() {
            Some(location) => format!(
                "{}:{}:{}",
                location.file(),
                location.line(),
                location.column()
            ),
            None => "<NO LOCATION>".into(),
        };

        let thread_name: String = match std::thread::current().name() {
            Some(name) => name.into(),
            None => "unnamed".into(),
        };
        error!(
            "thread '{}' panicked at '{}', {}",
            thread_name, s, location_string
        );
    };

    if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
        log_panic(s);
    } else if let Some(s) = panic_info.payload().downcast_ref::<&String>() {
        log_panic(s);
    } else {
        log_panic("UNKNOWN");
    }

    // Call into the previous panic function (typically the standard
    // panic function)
    other_hook(panic_info)
}
