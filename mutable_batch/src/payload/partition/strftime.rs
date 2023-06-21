use std::fmt::Write;

use chrono::{format::StrftimeItems, TimeZone, Utc};

use crate::PartitionKeyError;

use super::encode_key_part;

/// The number of nanoseconds in 1 day, definitely recited from memory.
const DAY_NANOSECONDS: i64 = 86_400_000_000_000;

/// The default YMD formatter spec.
const YMD_SPEC: &str = "%Y-%m-%d";

/// A FIFO ring buffer, holding `N` lazily initialised slots.
///
/// This is optimised for low values of `N` (where N*T covers a few cache lines)
/// as it performs an O(n) linear search.
#[derive(Debug)]
struct RingBuffer<const N: usize, T> {
    buf: [Option<T>; N],

    /// Index into to the last wrote value.
    last_idx: usize,
}

impl<const N: usize, T> Default for RingBuffer<N, T>
where
    T: Default,
{
    fn default() -> Self {
        Self {
            buf: [(); N].map(|_| Default::default()), // default init for non-const type
            last_idx: N - 1,
        }
    }
}

impl<const N: usize, T> RingBuffer<N, T>
where
    T: Default,
{
    /// Return a mutable reference to the next slot to be overwrote. This method
    /// initialises the slot if it has not been previously used.
    ///
    /// This is like an "insert" operation, but allows the caller to re-use the
    /// contents of the slot to minimise allocations.
    ///
    /// This is an O(1) operation.
    fn next_slot(&mut self) -> &mut T {
        // Advance the next slot pointer
        self.last_idx += 1;
        self.last_idx %= N;

        let v = self.buf[self.last_idx].get_or_insert_with(Default::default);

        v
    }

    /// Drop the last buffer entry.
    ///
    /// This may cause spurious cache misses due to the short-circuiting search
    /// observing an empty element, potentially before non-empty elements.
    fn drop_last(&mut self) {
        self.buf[self.last_idx] = None;
    }

    /// Find the first initialised slot that causes `F` to evaluate to true,
    /// returning the slot contents.
    ///
    /// This is a O(n) linear search operation, which for small N can be as
    /// fast, or faster, than a hashmap lookup by key.
    fn find<F>(&self, f: F) -> Option<&'_ T>
    where
        F: Fn(&T) -> bool,
    {
        for v in &self.buf {
            let v = v.as_ref()?;
            if f(v) {
                return Some(v);
            }
        }
        None
    }
}

/// A strftime-like formatter of epoch timestamps with nanosecond granularity.
///
/// # Deferred Errors
///
/// If the provided stftime formatter is invalid, an
/// [`PartitionKeyError::InvalidStrftime`] error is raised during the formatting
/// call to [`StrftimeFormatter::render()`] and not during initialisation. This
/// is a limitation of the underlying library.
///
/// # Caching
///
/// It is very common for batches of writes to contain multiple measurements
/// taken at the same timestamp; for example, a periodic scraper of metric
/// values will assign a single timestamp for the entire batch of observations.
///
/// To leverage this reuse of timestamps, this type retains a cache of the 5
/// most recently observed distinct timestamps to avoid recomputing the same
/// formatted string for each repeat occurrence.
///
/// In the best case, this reduces N row formats down to a single format
/// operation, and in the worst case, it changes the memory overhead from "rows"
/// to "rows + 5" which amortises nicely as batch sizes increase. If more than 5
/// timestamps are observed, the existing buffer allocations are reused when
/// computing the replacement values.
///
/// # `YYYY-MM-DD` Reduction Specialisation
///
/// The default (and therefore most common) formatting spec is "%Y-%m-%d", as
/// this is the IOx default partitioning template. The vast majority of writes
/// will utilise this format spec.
///
/// Because this spec is so common, a special case optimisation is utilised for
/// it: for any given timestamp, first normalise the value by reducing the
/// precision such that the timestamp is rounded down to the nearest whole day
/// before further processing.
///
/// This removes all the sub-day variance (hours, minutes, seconds, etc) from
/// the value, without changing the formatter output (it still produces the same
/// string). This in turn causes any timestamp from the same day to be a cache
/// hit with any prior value for the same day, regardless of "time" portion of
/// the timestamp.
///
/// Combined with the above cache, this raises the cache hit rate to ~100% for
/// write batches that span less than 6 days, effectively amortising the cost of
/// timestamp formatting to O(1) for these very common batches.
#[derive(Debug)]
pub(super) struct StrftimeFormatter<'a> {
    /// The strftime formatter definition.
    ///
    /// NOTE: the value below is UNVALIDATED - if the input strftime format
    /// contains invalid formatter directives, then the error is deferred until
    /// formatting a timestamp.
    format: StrftimeItems<'a>,

    /// As an optimisation, when this formatter is using the default YYYY-MM-DD
    /// partitioning template, timestamps are normalised to per-day granularity,
    /// preventing variances in the timestamp of less-than 1 day from causing a
    /// miss in the cached "values".
    ///
    /// This optimisation massively increases the reuse of cached, pre-formatted
    /// strings.
    is_ymd_format: bool,

    /// A set of 5 most recently added timestamps, and the formatted string they
    /// map to.
    values: RingBuffer<5, (i64, String)>,

    /// The last observed timestamp.
    ///
    /// This value changes each time a timestamp is returned to the user, either
    /// from the cache of pre-generated strings, or by generating a new one and
    /// MUST always track the last timestamp given to
    /// [`StrftimeFormatter::render()`].
    last_ts: Option<i64>,
}

impl<'a> StrftimeFormatter<'a> {
    /// Initialise a new [`StrftimeFormatter`] with the given stftime-like
    /// format string.
    ///
    /// The exact formatter specification is [documented here].
    ///
    /// If the formatter contains an invalid spec, an error is raised when
    /// formatting.
    ///
    /// [documented here]:
    ///     https://docs.rs/chrono/latest/chrono/format/strftime/index.html
    pub(super) fn new(format: &'a str) -> Self {
        let mut is_default_format = false;
        if format == YMD_SPEC {
            is_default_format = true;
        }

        Self {
            format: StrftimeItems::new(format),
            is_ymd_format: is_default_format,
            values: RingBuffer::default(),
            last_ts: None,
        }
    }

    /// Format `timestamp` to the format spec provided during initialisation,
    /// writing the result to `out`.
    pub(super) fn render<W>(&mut self, timestamp: i64, mut out: W) -> Result<(), PartitionKeyError>
    where
        W: std::fmt::Write,
    {
        // Optionally apply the default format reduction optimisation.
        let timestamp = self.maybe_reduce(timestamp);

        // Retain this timestamp as the last observed timestamp.
        self.last_ts = Some(timestamp);

        // Check if this timestamp has already been rendered.
        if let Some(v) = self.values.find(|(t, _v)| *t == timestamp) {
            // It has! Re-use the existing formatted string.
            out.write_str(&v.1)?;
            return Ok(());
        }

        // Obtain a mutable reference to the next item to be replaced, re-using
        // the string buffer within it to avoid allocating (or initialising it
        // if it was not yet initialised).
        let buf = self.values.next_slot();

        // Reset the slot value
        buf.0 = timestamp;
        buf.1.clear();

        // Format the timestamp value into the slot buffer.
        if write!(
            buf.1,
            "{}",
            Utc.timestamp_nanos(timestamp)
                .format_with_items(self.format.clone()) // Cheap clone of refs
        )
        .is_err()
        {
            // The string buffer may be empty, or contain partially rendered
            // output before the error was raised.
            //
            // Remove this entry from the cache to prevent there being a mapping
            // of `timestamp -> <empty or incomplete output>`.
            self.values.drop_last();
            return Err(PartitionKeyError::InvalidStrftime);
        };

        // Encode any reserved characters in this new string.
        buf.1 = encode_key_part(&buf.1).to_string();

        // Render this new value to the caller's buffer
        out.write_str(&buf.1)?;

        Ok(())
    }

    /// Reduce the precision of the timestamp iff using the default "%Y-%m-%d"
    /// formatter string, returning a value rounded to the nearest whole day.
    ///
    /// If the formatter is not this special-case value, `timestamp` is returned
    /// unchanged.
    fn maybe_reduce(&self, timestamp: i64) -> i64 {
        if !self.is_ymd_format {
            return timestamp;
        }
        // Don't map timestamps less than the value we would subtract.
        if timestamp < DAY_NANOSECONDS {
            return timestamp;
        }
        timestamp - (timestamp % DAY_NANOSECONDS)
    }

    /// Returns true if the output of rendering `timestamp` will match the last
    /// rendered timestamp, after optionally applying the precision reduction
    /// optimisation.
    pub(crate) fn equals_last(&self, timestamp: i64) -> bool {
        // Optionally apply the default format reduction optimisation.
        let timestamp = self.maybe_reduce(timestamp);

        self.last_ts.map(|v| v == timestamp).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use data_types::partition_template::{TablePartitionTemplateOverride, TemplatePart};
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn test_default_formatter() {
        let template = TablePartitionTemplateOverride::default();
        let expect = template.parts().collect::<Vec<_>>();

        // If this assert fails (and it probably shouldn't!) then you may want
        // to consider changing the special case optimisation above.
        assert_matches!(expect.as_slice(), &[TemplatePart::TimeFormat(YMD_SPEC)]);
    }

    #[test]
    fn test_never_empty() {
        let mut fmt = StrftimeFormatter::new("");

        let mut buf = String::new();
        fmt.render(42, &mut buf).expect("should render string");
        assert!(!buf.is_empty());
        assert_eq!(buf, "^");
    }

    #[test]
    fn test_incomplete_formatter() {
        let mut fmt = StrftimeFormatter::new("%");

        let mut buf = String::new();
        let got = fmt.render(42, &mut buf);
        assert_matches!(got, Err(PartitionKeyError::InvalidStrftime));
    }

    #[test]
    fn test_incomplete_formatter_removes_bad_mapping() {
        let mut fmt = StrftimeFormatter::new("%s");

        let mut buf = String::new();
        fmt.render(42, &mut buf).unwrap();

        assert_matches!(
            fmt.values.buf.as_slice(),
            [Some((42, _)), None, None, None, None]
        );

        // This obviously isn't possible through normal usage, but to trigger
        // the "failed to render" code path, reach in and tweak the formatter to
        // cause it to fail.
        fmt.format = StrftimeItems::new("%");

        // Trigger the "cannot format" code path
        fmt.render(4242, &mut buf).expect_err("invalid formatter");

        // And ensure the ring buffer was left in a clean state
        assert_matches!(
            fmt.values.buf.as_slice(),
            [Some((42, _)), None, None, None, None]
        );
    }

    #[test]
    fn test_uses_ring_buffer() {
        let mut fmt = StrftimeFormatter::new("%H");
        let mut buf = String::new();

        fmt.render(42, &mut buf).expect("should render string");
        fmt.render(42, &mut buf).expect("should render string");
        fmt.render(42, &mut buf).expect("should render string");
        fmt.render(12345, &mut buf).expect("should render string");
        fmt.render(42, &mut buf).expect("should render string");

        // Assert the above repetitive values were deduped in the cache.
        assert_matches!(
            fmt.values.buf.as_slice(),
            [Some((42, _)), Some((12345, _)), None, None, None]
        );
        assert_eq!(fmt.values.last_idx, 1);
    }

    const FORMATTER_SPEC_PARTS: &[&str] = &[
        "%Y", "%m", "%d", "%H", "%m", "%.9f", "%r", "%+", "%t", "%n", "%A", "%c",
    ];

    prop_compose! {
        /// Yield an arbitrary formatter spec selected from
        /// [`FORMATTER_SPEC_PARTS`] delimited by a random character.
        fn arbitrary_formatter_spec()(
            delimiter in any::<char>(),
            v in proptest::collection::vec(
                proptest::sample::select(FORMATTER_SPEC_PARTS).prop_map(ToString::to_string),
                (0, 10) // Set size range
            )) -> String {
            v.join(&delimiter.to_string())
        }
    }

    fn default_formatter_spec() -> impl Strategy<Value = String> {
        Just(YMD_SPEC.to_string())
    }

    proptest! {
        /// The [`StrftimeFormatter`] is a glorified wrapper around chrono's
        /// formatter, therefore this test asserts the following property:
        ///
        ///     For any timestamp and formatter, the output of this type must
        ///     match the output of chrono's formatter, after key encoding.
        ///
        /// Validating this asserts correctness of the wrapper itself, assuming
        /// chrono's formatter produces correct output. Note the encoding is
        /// tested in the actual partitioner module.
        #[test]
        fn prop_differential_validation(
            timestamps in prop::collection::vec(any::<i64>(), 1..100),
            format in prop_oneof![arbitrary_formatter_spec(), default_formatter_spec(), any::<String>()],
        ) {
            let mut fmt = StrftimeFormatter::new(&format);
            let items = StrftimeItems::new(&format);

            for ts in timestamps {
                // Generate the control string.
                let mut control = String::new();
                let _ = write!(
                    control,
                    "{}",
                    Utc.timestamp_nanos(ts)
                        .format_with_items(items.clone())
                );
                let control = encode_key_part(&control);

                // Generate the test string.
                let mut test = String::new();
                if fmt.render(ts, &mut test).is_err() {
                    // Any error results in the key not being used, so any
                    // differences are inconsequential.
                    continue;
                }

                assert_eq!(control, test);
            }
        }
    }
}
