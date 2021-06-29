use observability_deps::tracing::Metadata;
use observability_deps::tracing_subscriber::EnvFilter;
use observability_deps::{
    tracing::{
        event::Event,
        span::{Attributes, Id, Record},
        subscriber::Subscriber,
    },
    tracing_subscriber::layer::{Context, Layer},
};
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::sync::Arc;

/// A FilteredLayer wraps a tracing subscriber Layer and passes events only
/// if the provided EnvFilter accepts the event.
///
/// Tracing subscriber's layering makes it easy to split event filtering from event recording
/// and collection, but the filtering decisions apply to all layers, effectively producing
/// an intersection of all filtering decisions.
///
/// A FilteredLayer on the other hand, allows to restrict the verbosity of one event sink
/// without throwing away events available to other layers.
#[derive(Debug)]
pub struct FilteredLayer<S, L>
where
    S: Subscriber,
    L: Layer<S>,
{
    inner: L,
    _phantom_data: PhantomData<S>,
}

impl<S, L> FilteredLayer<S, L>
where
    S: Subscriber,
    L: Layer<S>,
{
    pub fn new(inner: L) -> Self {
        Self {
            inner,
            _phantom_data: Default::default(),
        }
    }
}

impl<S, L> Layer<S> for FilteredLayer<S, L>
where
    S: Subscriber,
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

/// A union filter is a filtering Layer that returns "enabled: true"
/// for events for which at least one of the inner filters returns "enabled: true".
///
/// This is the opposite of what the main subscriber's layered chain does, where
/// if one filter says nope, the event is filtered out.
///
/// Since there is a blanked `impl<L: Layer> Layer for Option<L>` that returns
/// "enabled: true" when the option is None, it would be very confusing if an
/// user accidentally passed such a none to the filter vector in the union filter.
/// However it's quite tempting to pass an optional layer around hoping it "does the right thing",
/// since it does indeed work in other places a layer is passed to a subscriber builder
/// (moreover, due to the static typing builder pattern used there, there is no way of
/// conditionally adding a filter other than passing an optional filter). Thus, it would be
/// (and it happened in practice) quite confusing for this API to technically accept optional
/// layers and silently do the wrong thing with them.
/// For this reason the [`UnionFilter::new`] method accepts a vector of [`Option`][option]s.
/// It's not perfect, since a user could pass an `Option<Option<L>>` but hopefully
///
/// This filter is intended to be used together with the [`FilteredLayer`] layer
/// which will filter unwanted events for each of the.
///
/// This [`UnionFilter`] and the [`FilteredLayer`] are likely to share filters.
/// Unfortunately the [`EnvFilter`][envfilter] doesn't implement [`Clone`].
/// See [`CloneableEnvFilter`] for a workaround.
///
/// [envfilter]: observability_deps::tracing_subscriber::EnvFilter
/// [option]: std::option::Option
pub struct UnionFilter<S>
where
    S: Subscriber,
{
    inner: Vec<Box<dyn Layer<S> + Send + Sync + 'static>>,
    _phantom_data: PhantomData<S>,
}

impl<S> UnionFilter<S>
where
    S: Subscriber,
{
    pub fn new(inner: Vec<Option<Box<dyn Layer<S> + Send + Sync + 'static>>>) -> Self {
        let inner = inner.into_iter().flatten().collect();
        Self {
            inner,
            _phantom_data: Default::default(),
        }
    }
}

impl<S> Layer<S> for UnionFilter<S>
where
    S: Subscriber,
{
    /// Return the disjunction of all the enabled flags of all the inner filters which are not None.
    ///
    /// A None filter doesn't really mean "I want this event", nor it means "I want to filter this event out";
    /// it just means "please ignore me, I'm not really a filter".
    ///
    /// Yet, there is a blanked implementation of `Option<L>` for all `L: Layer`, and its implementation
    /// if the `enabled()` method returns true. This works because the normal subscriber layer chain
    /// performs a conjunction of each filter decision.
    ///
    /// However, the [`UnionFilter`] here is the opposite, we want to enable processing of one event
    /// as long as one of the event filters registers interest in it.
    fn enabled(&self, metadata: &Metadata<'_>, ctx: Context<'_, S>) -> bool {
        self.inner.iter().any(|i| i.enabled(metadata, ctx.clone()))
    }
}

impl<S> std::fmt::Debug for UnionFilter<S>
where
    S: Subscriber,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("UnionFilter(...)")
    }
}

#[derive(Clone, Debug)]
pub struct CloneableEnvFilter(Arc<EnvFilter>);

impl CloneableEnvFilter {
    pub fn new(inner: EnvFilter) -> Self {
        Self(Arc::new(inner))
    }
}

impl<S> Layer<S> for CloneableEnvFilter
where
    S: Subscriber,
{
    fn enabled(&self, metadata: &Metadata<'_>, ctx: Context<'_, S>) -> bool {
        self.0.enabled(metadata, ctx)
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
            .with(FilteredLayer::new(filter1.and_then(layer1)))
            .with(FilteredLayer::new(filter2.and_then(layer2)));

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
