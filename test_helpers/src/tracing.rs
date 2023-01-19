//! Utilities for testing tracing
use std::{fmt, sync::Arc};

use observability_deps::tracing::{
    self,
    field::Field,
    span::{Attributes, Id, Record},
    subscriber::{DefaultGuard, Subscriber},
    Event,
};
use parking_lot::Mutex;

/// This struct captures tracing `Event`s as strings, and can be used
/// to verify that messages are making it to logs correctly
///
/// Upon creation it registers itself as the global default span
/// subscriber, and upon drop it sets a NoOp in its place.
#[derive(Debug)]
pub struct TracingCapture {
    /// The raw logs are captured as a list of strings
    logs: Arc<Mutex<Vec<String>>>,
    #[allow(dead_code)]
    guard: DefaultGuard,
}

impl TracingCapture {
    /// Create a new TracingCapture object and register it as a subscriber
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let logs = Arc::new(Mutex::new(Vec::new()));

        // Register a subscriber to actually capture the log messages
        let my_subscriber = TracingCaptureSubscriber {
            logs: Arc::clone(&logs),
        };

        // install the subscriber (is uninstalled when the guard is dropped)
        let guard = tracing::subscriber::set_default(my_subscriber);

        Self { logs, guard }
    }
}

impl fmt::Display for TracingCapture {
    /// Retrieves the contents of all captured traces as a string
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let logs = self.logs.lock();
        write!(f, "{}", logs.join("\n"))
    }
}

/// Captures span events to verify
struct TracingCaptureSubscriber {
    logs: Arc<Mutex<Vec<String>>>,
}

impl Subscriber for TracingCaptureSubscriber {
    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut v = StringVisitor {
            string: String::new(),
        };
        v.record_kv("level", &event.metadata().level().to_string());
        event.record(&mut v);
        let mut logs = self.logs.lock();
        logs.push(v.string);
    }

    fn enter(&self, _span: &Id) {}
    fn exit(&self, _span: &Id) {}
}

struct StringVisitor {
    string: String,
}

impl StringVisitor {
    fn record_kv(&mut self, key: &str, value: &str) {
        use std::fmt::Write;
        write!(self.string, "{} = {}; ", key, value).unwrap();
    }
}

impl tracing::field::Visit for StringVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record_kv(field.name(), &format!("{:?}", value))
    }
}
