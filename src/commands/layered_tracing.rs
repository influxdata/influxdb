use observability_deps::{
    tracing::{
        event::Event,
        span::{Attributes, Id, Record},
        subscriber::Subscriber,
    },
    tracing_subscriber::layer::{Context, Layer, Layered},
};

/// A FilteredLayer wraps a tracing subscriber Layer and passes events only
/// if the provided EnvFilter accepts the event.
///
/// Tracing subscriber's layering makes it easy to split event filtering from event recording
/// and collection, but the filtering decisions apply to all layers, effectively producing
/// an intersection of all filtering decisions.
///
/// A FilteredLayer on the other hand, allows to restrict the verbosity of one event sink
/// without throwing away events available to other layers.
pub struct FilteredLayer<S, F, L>
where
    S: Subscriber,
    F: Layer<S>,
    L: Layer<S>,
{
    inner: Layered<F, L, S>,
}

impl<S, F, L> FilteredLayer<S, F, L>
where
    S: Subscriber,
    F: Layer<S>,
    L: Layer<S>,
{
    // TODO remove when we'll use this
    #[allow(dead_code)]
    pub fn new(filter: F, delegate: L) -> Self {
        Self {
            inner: delegate.and_then(filter),
        }
    }
}

impl<S, F, L> Layer<S> for FilteredLayer<S, F, L>
where
    S: Subscriber,
    F: Layer<S>,
    L: Layer<S>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if self.inner.enabled(event.metadata(), ctx.clone()) {
            self.inner.on_event(event, ctx);
        }
    }
    fn new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        self.inner.new_span(attrs, id, ctx)
    }
    fn on_record(&self, span: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        self.inner.on_record(span, values, ctx)
    }
    fn on_follows_from(&self, span: &Id, follows: &Id, ctx: Context<'_, S>) {
        self.inner.on_follows_from(span, follows, ctx)
    }
    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        self.inner.on_enter(id, ctx)
    }
    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        self.inner.on_exit(id, ctx)
    }
    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        self.inner.on_close(id, ctx)
    }
    fn on_id_change(&self, old: &Id, new: &Id, ctx: Context<'_, S>) {
        self.inner.on_id_change(old, new, ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use observability_deps::{
        tracing::{self, debug, error, info},
        tracing_subscriber::{self, fmt, layer::SubscriberExt, EnvFilter},
    };
    use std::sync::{Arc, Mutex};
    use synchronized_writer::SynchronizedWriter;

    // capture_two_streams is a test helper that sets up two independent tracing subscribers, each with
    // a different filtering level and returns a tuple of the emitted lines for the respective streams.
    //
    // The first stream uses a textual format, the second stream uses a json format (just to make it clear they are
    // indeed completely different exporters).
    fn capture_two_streams(
        filter1: EnvFilter,
        filter2: EnvFilter,
        workload: impl FnOnce(),
    ) -> (Vec<String>, Vec<String>) {
        let writer1 = Arc::new(Mutex::new(Vec::new()));
        let writer1_captured = Arc::clone(&writer1);
        let writer2 = Arc::new(Mutex::new(Vec::new()));
        let writer2_captured = Arc::clone(&writer2);

        let layer1 = fmt::layer()
            .with_target(false)
            .with_ansi(false)
            .without_time()
            .with_writer(move || SynchronizedWriter::new(Arc::clone(&writer1_captured)));

        let layer2 = fmt::layer()
            .json()
            .with_target(false)
            .with_ansi(false)
            .without_time()
            .with_writer(move || SynchronizedWriter::new(Arc::clone(&writer2_captured)));

        let subscriber = tracing_subscriber::Registry::default()
            .with(FilteredLayer::new(filter1, layer1))
            .with(FilteredLayer::new(filter2, layer2));

        tracing::subscriber::with_default(subscriber, workload);

        let lines = |writer: Arc<Mutex<Vec<_>>>| {
            std::str::from_utf8(&writer.lock().unwrap())
                .unwrap()
                .to_string()
                .trim_end()
                .split('\n')
                .into_iter()
                .map(String::from)
                .collect()
        };

        (lines(writer1), lines(writer2))
    }

    #[test]
    fn test_independent() {
        let workload = || {
            error!("foo");
            info!("bar");
            debug!("baz");
        };

        let (lines1, lines2) =
            capture_two_streams(EnvFilter::new("info"), EnvFilter::new("debug"), workload);
        assert_eq!(
            lines1,
            vec!["ERROR foo".to_string(), " INFO bar".to_string()],
        );
        assert_eq!(
            lines2,
            vec![
                r#"{"timestamp":"","level":"ERROR","fields":{"message":"foo"}}"#.to_string(),
                r#"{"timestamp":"","level":"INFO","fields":{"message":"bar"}}"#.to_string(),
                r#"{"timestamp":"","level":"DEBUG","fields":{"message":"baz"}}"#.to_string()
            ],
        );

        let (lines1, lines2) =
            capture_two_streams(EnvFilter::new("debug"), EnvFilter::new("info"), workload);
        assert_eq!(
            lines1,
            vec![
                "ERROR foo".to_string(),
                " INFO bar".to_string(),
                "DEBUG baz".to_string(),
            ],
        );
        assert_eq!(
            lines2,
            vec![
                r#"{"timestamp":"","level":"ERROR","fields":{"message":"foo"}}"#.to_string(),
                r#"{"timestamp":"","level":"INFO","fields":{"message":"bar"}}"#.to_string(),
            ],
        );
    }
}
