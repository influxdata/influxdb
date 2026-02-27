// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use parking_lot as _;
#[cfg(test)]
use regex as _;

use workspace_hack as _;

use humantime::format_rfc3339_micros;
use std::borrow::Cow;
use std::{io::Write, time::SystemTime};
use tracing::{
    self, Id, Level, Subscriber,
    field::{Field, Visit},
    subscriber::Interest,
};
use tracing_subscriber::{Layer, fmt::MakeWriter, layer::Context, registry::LookupSpan};
use unicode_segmentation::UnicodeSegmentation;

const MAX_FIELD_LENGTH: usize = 8_000; // roughly 8KB, which is half of the total line length allowed

/// Implements a `tracing_subscriber::Layer` which generates
/// [logfmt] formatted log entries, suitable for log ingestion
///
/// At time of writing, I could find no good existing crate
///
/// <https://github.com/mcountryman/logfmt_logger> from @mcountryman
/// looked very small and did not (obviously) work with the tracing subscriber
///
/// [logfmt]: https://brandur.org/logfmt
#[derive(Debug)]
pub struct LogFmtLayer<W>
where
    W: for<'writer> MakeWriter<'writer>,
{
    writer: W,
    display_target: bool,
}

impl<W> LogFmtLayer<W>
where
    W: for<'writer> MakeWriter<'writer>,
{
    /// Create a new logfmt Layer to pass into tracing_subscriber
    ///
    /// Note this layer simply formats and writes to the specified writer. It
    /// does not do any filtering for levels itself. Filtering can be done
    /// using a EnvFilter
    ///
    /// For example:
    /// ```
    ///  use logfmt::LogFmtLayer;
    ///  use tracing_subscriber::{EnvFilter, prelude::*, self};
    ///
    ///  // setup debug logging level
    ///  unsafe {
    ///      std::env::set_var("RUST_LOG", "debug");
    ///  }
    ///
    ///  // setup formatter to write to stderr
    ///  let formatter =
    ///    LogFmtLayer::new(std::io::stderr);
    ///
    ///  tracing_subscriber::registry()
    ///    .with(EnvFilter::from_default_env())
    ///    .with(formatter)
    ///    .init();
    /// ```
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            display_target: true,
        }
    }

    /// Control whether target and location attributes are displayed (on by default).
    ///
    /// Note: this API mimics that of other fmt layers in tracing-subscriber crate.
    pub fn with_target(self, display_target: bool) -> Self {
        Self {
            display_target,
            ..self
        }
    }
}

impl<S, W> Layer<S> for LogFmtLayer<W>
where
    W: for<'writer> MakeWriter<'writer> + 'static,
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn register_callsite(
        &self,
        _metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        Interest::always()
    }

    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let writer = self.writer.make_writer();
        let metadata = ctx.metadata(id).expect("span should have metadata");
        let mut p = FieldPrinter::new(writer, metadata.level(), self.display_target);
        p.write_span_name(metadata.name());
        attrs.record(&mut p);
        p.write_span_id(id);
        p.write_timestamp();
    }

    fn max_level_hint(&self) -> Option<tracing::metadata::LevelFilter> {
        None
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let writer = self.writer.make_writer();
        let mut p = FieldPrinter::new(writer, event.metadata().level(), self.display_target);
        // record fields
        event.record(&mut p);
        if let Some(span) = ctx.lookup_current() {
            p.write_span_id(&span.id())
        }
        // record source information
        p.write_source_info(event);
        p.write_timestamp();
    }
}

/// This thing is responsible for actually printing log information to
/// a writer
struct FieldPrinter<W: Write> {
    writer: W,
    display_target: bool,
}

impl<W: Write> FieldPrinter<W> {
    fn new(mut writer: W, level: &Level, display_target: bool) -> Self {
        let level_str = match *level {
            Level::TRACE => "trace",
            Level::DEBUG => "debug",
            Level::INFO => "info",
            Level::WARN => "warn",
            Level::ERROR => "error",
        };

        write!(writer, r#"level={level_str}"#).ok();

        Self {
            writer,
            display_target,
        }
    }

    fn write_span_name(&mut self, value: &str) {
        write!(
            self.writer,
            " span_name=\"{}\"",
            quote_escape_truncate(value, MAX_FIELD_LENGTH)
        )
        .ok();
    }

    fn write_source_info(&mut self, event: &tracing::Event<'_>) {
        if !self.display_target {
            return;
        }

        let metadata = event.metadata();
        write!(
            self.writer,
            " target=\"{}\"",
            quote_escape_truncate(metadata.target(), MAX_FIELD_LENGTH)
        )
        .ok();

        if let Some(module_path) = metadata.module_path()
            && metadata.target() != module_path
        {
            write!(self.writer, " module_path=\"{module_path}\"").ok();
        }
        if let (Some(file), Some(line)) = (metadata.file(), metadata.line()) {
            write!(self.writer, " location=\"{file}:{line}\"").ok();
        }
    }

    fn write_span_id(&mut self, id: &Id) {
        write!(self.writer, " span={}", id.into_u64()).ok();
    }

    fn write_timestamp(&mut self) {
        let system_time = SystemTime::now();
        let rfc3339_ts = format_rfc3339_micros(system_time);
        let ns_since_epoch = system_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("System time should have been after the epoch")
            .as_nanos();

        write!(self.writer, " ts={rfc3339_ts}").ok();
        write!(self.writer, " time={ns_since_epoch:?}").ok();
    }
}

impl<W: Write> Drop for FieldPrinter<W> {
    fn drop(&mut self) {
        // finish the log line
        writeln!(self.writer).ok();
    }
}

impl<W: Write> Visit for FieldPrinter<W> {
    fn record_i64(&mut self, field: &Field, value: i64) {
        write!(
            self.writer,
            " {}={}",
            translate_field_name(field.name()),
            value
        )
        .ok();
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        write!(
            self.writer,
            " {}={}",
            translate_field_name(field.name()),
            value
        )
        .ok();
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        write!(
            self.writer,
            " {}={}",
            translate_field_name(field.name()),
            value
        )
        .ok();
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        write!(
            self.writer,
            " {}={}",
            translate_field_name(field.name()),
            quote_escape_truncate(value, MAX_FIELD_LENGTH)
        )
        .ok();
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        let field_name = translate_field_name(field.name());

        let debug_formatted = format!("{value:?}");
        write!(
            self.writer,
            " {}={:?}",
            field_name,
            quote_escape_truncate(&debug_formatted, MAX_FIELD_LENGTH)
        )
        .ok();

        let display_formatted = format!("{value}");
        write!(
            self.writer,
            " {}.display={}",
            field_name,
            quote_escape_truncate(&display_formatted, MAX_FIELD_LENGTH)
        )
        .ok();
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // Note this appears to be invoked via `debug!` and `info! macros
        let formatted_value = format!("{value:?}");
        write!(
            self.writer,
            " {}={}",
            translate_field_name(field.name()),
            quote_escape_truncate(&formatted_value, MAX_FIELD_LENGTH)
        )
        .ok();
    }
}

/// return true if the string value already starts/ends with quotes and is
/// already properly escaped (all spaces escaped)
fn needs_quotes_and_escaping(value: &str) -> bool {
    // mismatches beginning  / end quotes
    if value.starts_with('"') != value.ends_with('"') {
        return true;
    }

    // ignore beginning/ending quotes, if any
    let pre_quoted = value.len() >= 2 && value.starts_with('"') && value.ends_with('"');

    let value = if pre_quoted {
        &value[1..value.len() - 1]
    } else {
        value
    };

    // unescaped quotes
    let c0 = value.chars();
    let c1 = value.chars().skip(1);
    if c0.zip(c1).any(|(c0, c1)| c0 != '\\' && c1 == '"') {
        return true;
    }

    // Quote any strings that contain a literal '=' which the logfmt parser
    // interprets as a key/value separator.
    if value.chars().any(|c| c == '=') && !pre_quoted {
        return true;
    }

    if value.bytes().any(|b| b <= b' ') && !pre_quoted {
        return true;
    }

    false
}

struct GraphemeIndex<'a> {
    index: usize,
    grapheme: &'a str,
}

/// escape any characters in name as needed, otherwise return string as is
fn quote_escape_truncate(value: &'_ str, truncate_to: usize) -> Cow<'_, str> {
    let value = if needs_quotes_and_escaping(value) {
        Cow::Owned(format!("{value:?}"))
    } else {
        Cow::Borrowed(value)
    };

    if value.len() <= truncate_to {
        return value;
    }

    let quoted = value.len() > 1 && value.starts_with('"') && value.ends_with('"');
    let suffix_len = '…'.len_utf8() + if quoted { '"'.len_utf8() } else { 0 };
    let mut truncate_at = truncate_to - suffix_len;

    // Get initial set of valid grapheme indices
    let valid_graphemes: Vec<GraphemeIndex<'_>> =
        UnicodeSegmentation::grapheme_indices(value.as_ref(), true)
            .map(|x| GraphemeIndex {
                index: x.0,
                grapheme: x.1,
            })
            .collect();

    // Find the highest index of valid unicode grapheme
    // that allows us to truncate at the requested limit (or below if necessary).
    for idx in (0..valid_graphemes.len() - 1).rev() {
        let current_grapheme_index = valid_graphemes[idx].index;
        if current_grapheme_index <= truncate_at {
            truncate_at = current_grapheme_index;
            break;
        }
    }

    // Check for escape sequences and make sure we
    // do not break half way through an escape.
    let value_vec = value.as_bytes();
    let mut escapes = 0;
    while value_vec[truncate_at - (escapes + 1)] == b'\\' {
        escapes += 1;
    }
    truncate_at -= escapes % 2;

    // Truncate to a valid string and push
    // a … to signify truncation, and `"` if
    // needed to stay logfmt valid.
    let mut new_string: String = valid_graphemes
        .iter()
        .map(|x| x.grapheme)
        .take(truncate_at)
        .collect();

    new_string.push('…');
    if quoted {
        new_string.push('"');
    }

    Cow::from(new_string)
}

// Translate the field name from tracing into the logfmt style
fn translate_field_name(name: &str) -> &str {
    if name == "message" { "msg" } else { name }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn quote_and_escape_len0() {
        assert_eq!(quote_escape_truncate("", MAX_FIELD_LENGTH), "");
    }

    #[test]
    fn quote_and_escape_len1() {
        assert_eq!(quote_escape_truncate("f", MAX_FIELD_LENGTH), "f");
    }

    #[test]
    fn quote_and_escape_len2() {
        assert_eq!(quote_escape_truncate("fo", MAX_FIELD_LENGTH), "fo");
    }

    #[test]
    fn quote_and_escape_len3() {
        assert_eq!(quote_escape_truncate("foo", MAX_FIELD_LENGTH), "foo");
    }

    #[test]
    fn quote_and_escape_len3_1quote_start() {
        assert_eq!(
            quote_escape_truncate("\"foo", MAX_FIELD_LENGTH),
            "\"\\\"foo\""
        );
    }

    #[test]
    fn quote_and_escape_len3_1quote_end() {
        assert_eq!(
            quote_escape_truncate("foo\"", MAX_FIELD_LENGTH),
            "\"foo\\\"\""
        );
    }

    #[test]
    fn quote_and_escape_len3_2quote() {
        assert_eq!(
            quote_escape_truncate("\"foo\"", MAX_FIELD_LENGTH),
            "\"foo\""
        );
    }

    #[test]
    fn quote_and_escape_space() {
        assert_eq!(
            quote_escape_truncate("foo bar", MAX_FIELD_LENGTH),
            "\"foo bar\""
        );
    }

    #[test]
    fn quote_and_escape_space_prequoted() {
        assert_eq!(
            quote_escape_truncate("\"foo bar\"", MAX_FIELD_LENGTH),
            "\"foo bar\""
        );
    }

    #[test]
    fn quote_and_escape_space_prequoted_but_not_escaped() {
        assert_eq!(
            quote_escape_truncate("\"foo \"bar\"", MAX_FIELD_LENGTH),
            "\"\\\"foo \\\"bar\\\"\""
        );
    }

    #[test]
    fn quote_and_escape_quoted_quotes() {
        assert_eq!(
            quote_escape_truncate("foo:\"bar\"", MAX_FIELD_LENGTH),
            "\"foo:\\\"bar\\\"\""
        );
    }

    #[test]
    fn quote_and_escape_nested_1() {
        assert_eq!(
            quote_escape_truncate(r#"a "b" c"#, MAX_FIELD_LENGTH),
            r#""a \"b\" c""#
        );
    }

    #[test]
    fn quote_and_escape_nested_2() {
        assert_eq!(
            quote_escape_truncate(r#"a "0 \"1\" 2" c"#, MAX_FIELD_LENGTH),
            r#""a \"0 \\\"1\\\" 2\" c""#
        );
    }

    #[test]
    fn quote_not_printable() {
        assert_eq!(
            quote_escape_truncate("foo\nbar", MAX_FIELD_LENGTH),
            r#""foo\nbar""#
        );
        assert_eq!(
            quote_escape_truncate("foo\r\nbar", MAX_FIELD_LENGTH),
            r#""foo\r\nbar""#
        );
        assert_eq!(
            quote_escape_truncate("foo\0bar", MAX_FIELD_LENGTH),
            r#""foo\0bar""#
        );
    }

    #[test]
    fn not_quote_unicode_unnecessarily() {
        assert_eq!(
            quote_escape_truncate("mikuličić", MAX_FIELD_LENGTH),
            "mikuličić"
        );
    }

    #[test]
    // https://github.com/influxdata/influxdb_iox/issues/4352
    fn test_uri_quoted() {
        assert_eq!(
            quote_escape_truncate(
                "/api/v2/write?bucket=06fddb4f912a0d7f&org=9df0256628d1f506&orgID=9df0256628d1f506&precision=ns",
                MAX_FIELD_LENGTH
            ),
            r#""/api/v2/write?bucket=06fddb4f912a0d7f&org=9df0256628d1f506&orgID=9df0256628d1f506&precision=ns""#
        );
    }

    #[test]
    fn test_truncation() {
        // The fully escaped string is:
        // r#""a \"0 \\\"1\\\" 2\" c""#
        assert_eq!(
            quote_escape_truncate(r#"a "0 \"1\" 2" c"#, 13),
            r#""a \"0 \\…""#,
        );
        assert_eq!(quote_escape_truncate(r#"a "0 \"1\" 2" c"#, 8), r#""a …""#,);

        // Because č and ć are actually 2 bytes, this looks like 9 but gets truncated
        assert_eq!(quote_escape_truncate("mikuličić", 10), "mikuli…");

        assert_eq!(quote_escape_truncate("mikuličić", 9), "mikuli…");
    }
}
