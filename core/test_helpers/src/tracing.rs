//! Utilities for testing tracing
use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    sync::Arc,
};

use parking_lot::Mutex;
use serde::Serialize;
use tracing::{
    self, Event,
    field::Field,
    span::{Attributes, Id, Record},
    subscriber::{DefaultGuard, Subscriber},
};

/// A log value.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum LogValue {
    Bool(bool),
    Float(f64),
    SignedInt(i128),
    String(String),
    UnsignedInt(u128),
}

impl std::fmt::Display for LogValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bool(b) => write!(f, "{b}"),
            Self::Float(x) => write!(f, "{x:?}"),
            Self::SignedInt(i) => write!(f, "{i}"),
            Self::String(s) => write!(f, "{s}"),
            Self::UnsignedInt(u) => write!(f, "{u}"),
        }
    }
}

/// A single log line.
///
/// This is represented as a key-value pairing.
pub type LogLine = Vec<(String, LogValue)>;

type SharedLogLines = Arc<Mutex<Vec<LogLine>>>;

/// Builder/config for [`TracingCapture`].
#[derive(Debug, Clone)]
pub struct TracingCaptureConfig {
    add_level: bool,
    add_target: bool,
    target_filter: HashSet<&'static str>,
}

impl Default for TracingCaptureConfig {
    fn default() -> Self {
        Self {
            add_level: true,
            add_target: false,
            target_filter: HashSet::default(),
        }
    }
}

impl TracingCaptureConfig {
    #[must_use]
    pub fn build(self) -> TracingCapture {
        // See <https://github.com/tokio-rs/tracing/issues/2874>.
        let _dont_drop_me = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::new());

        let logs = Arc::new(Mutex::new(Vec::new()));

        let capture = TracingCapture {
            lines: logs,
            config: Arc::new(self),
            guards: Default::default(),
            _dont_drop_me,
        };

        capture.register_in_current_thread();

        capture
    }

    /// Include target in logs for easier filtering.
    #[must_use]
    pub fn add_target(self) -> Self {
        Self {
            add_target: true,
            ..self
        }
    }

    /// Filter for specific target.
    ///
    /// If a target filter was already set, then this is understood as an "OR".
    #[must_use]
    pub fn filter_target(mut self, target: &'static str) -> Self {
        self.target_filter.insert(target);
        self
    }
}

/// This struct captures tracing `Event`s as strings, and can be used
/// to verify that messages are making it to logs correctly
///
/// Upon creation it registers itself as the global default span
/// subscriber, and upon drop it sets a NoOp in its place.
#[derive(Debug)]
pub struct TracingCapture {
    /// The raw logs are captured.
    lines: SharedLogLines,

    /// Config.
    config: Arc<TracingCaptureConfig>,

    /// Registered default handlers.
    guards: Mutex<Vec<DefaultGuard>>,

    /// See <https://github.com/tokio-rs/tracing/issues/2874>.
    _dont_drop_me: tracing::Dispatch,
}

impl TracingCapture {
    /// Create a new TracingCapture object and register it as a subscriber
    #[expect(clippy::new_without_default)]
    #[must_use]
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Get builder.
    pub fn builder() -> TracingCaptureConfig {
        TracingCaptureConfig::default()
    }

    /// Registers capture in current thread.
    pub fn register_in_current_thread(&self) {
        // Register a subscriber to actually capture the log messages
        let my_subscriber = TracingCaptureSubscriber {
            lines: Arc::clone(&self.lines),
            config: Arc::clone(&self.config),
        };

        // install the subscriber (is uninstalled when the guard is dropped)
        let guard = tracing::subscriber::set_default(my_subscriber);

        self.guards.lock().push(guard);
    }

    /// Logged lines.
    pub fn lines(&self) -> Vec<LogLine> {
        self.lines.lock().clone()
    }

    /// Logged lines as map for easier assertion.
    pub fn lines_as_maps(&self) -> Vec<BTreeMap<String, LogValue>> {
        self.lines()
            .into_iter()
            .map(|line| line.into_iter().collect())
            .collect()
    }
}

impl fmt::Display for TracingCapture {
    /// Retrieves the contents of all captured traces as a string
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, line) in self.lines().into_iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }

            for (k, v) in line {
                write!(f, "{k} = {v}; ")?;
            }
        }

        Ok(())
    }
}

/// Captures span events to verify
struct TracingCaptureSubscriber {
    lines: SharedLogLines,
    config: Arc<TracingCaptureConfig>,
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
        let md = event.metadata();
        let target = md.target();
        if !self.config.target_filter.is_empty() && !self.config.target_filter.contains(target) {
            return;
        }

        let mut v = LineVisitor::default();
        if self.config.add_level {
            v.record_string("level", md.level().to_string());
        }
        if self.config.add_target {
            v.record_string("target", target.to_owned());
        }
        event.record(&mut v);

        let mut lines = self.lines.lock();
        lines.push(v.line);
    }

    fn enter(&self, _span: &Id) {}
    fn exit(&self, _span: &Id) {}
}

#[derive(Debug, Default)]
struct LineVisitor {
    line: LogLine,
}

impl LineVisitor {
    fn record_string(&mut self, key: &str, value: String) {
        self.line.push((key.to_owned(), LogValue::String(value)));
    }
}

impl tracing::field::Visit for LineVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record_string(field.name(), format!("{value:?}"))
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.line
            .push((field.name().to_owned(), LogValue::Bool(value)));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.line
            .push((field.name().to_owned(), LogValue::Float(value)));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_i128(field, value as i128);
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        self.line
            .push((field.name().to_owned(), LogValue::SignedInt(value)));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_u128(field, value as u128);
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        self.line
            .push((field.name().to_owned(), LogValue::UnsignedInt(value)));
    }
}
