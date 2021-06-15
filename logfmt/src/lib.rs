#![deny(broken_intra_doc_links, rust_2018_idioms)]

use observability_deps::{
    tracing::{
        self,
        field::{Field, Visit},
        subscriber::Interest,
        Id, Level, Subscriber,
    },
    tracing_subscriber::{fmt::MakeWriter, layer::Context, registry::LookupSpan, Layer},
};
use std::borrow::Cow;
use std::{io::Write, time::SystemTime};

/// Implements a `tracing_subscriber::Layer` which generates
/// [logfmt] formatted log entries, suitable for log ingestion
///
/// At time of writing, I could find no good existing crate
///
/// https://github.com/mcountryman/logfmt_logger from @mcountryman
/// looked very small and did not (obviously) work with the tracing subscriber
///
/// [logfmt]: https://brandur.org/logfmt
pub struct LogFmtLayer<W: MakeWriter> {
    writer: W,
    display_target: bool,
}

impl<W: MakeWriter> LogFmtLayer<W> {
    /// Create a new logfmt Layer to pass into tracing_subscriber
    ///
    /// Note this layer simply formats and writes to the specified writer. It
    /// does not do any filtering for levels itself. Filtering can be done
    /// using a EnvFilter
    ///
    /// For example:
    /// ```
    ///  use logfmt::LogFmtLayer;
    ///  use observability_deps::tracing_subscriber::{EnvFilter, prelude::*, self};
    ///
    ///  // setup debug logging level
    ///  std::env::set_var("RUST_LOG", "debug");
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
    W: MakeWriter + 'static,
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn register_callsite(
        &self,
        _metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        Interest::always()
    }

    fn new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
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

        write!(writer, r#"level={}"#, level_str).ok();

        Self {
            writer,
            display_target,
        }
    }

    fn write_span_name(&mut self, value: &str) {
        write!(self.writer, " span_name=\"{}\"", quote_and_escape(value)).ok();
    }

    fn write_source_info(&mut self, event: &tracing::Event<'_>) {
        if !self.display_target {
            return;
        }

        let metadata = event.metadata();
        write!(
            self.writer,
            " target=\"{}\"",
            quote_and_escape(metadata.target())
        )
        .ok();

        if let Some(module_path) = metadata.module_path() {
            if metadata.target() != module_path {
                write!(self.writer, " module_path=\"{}\"", module_path).ok();
            }
        }
        if let (Some(file), Some(line)) = (metadata.file(), metadata.line()) {
            write!(self.writer, " location=\"{}:{}\"", file, line).ok();
        }
    }

    fn write_span_id(&mut self, id: &Id) {
        write!(self.writer, " span={}", id.into_u64()).ok();
    }

    fn write_timestamp(&mut self) {
        let ns_since_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("System time should have been after the epoch")
            .as_nanos();

        write!(self.writer, " time={:?}", ns_since_epoch).ok();
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
            quote_and_escape(value)
        )
        .ok();
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        let field_name = translate_field_name(field.name());

        let debug_formatted = format!("{:?}", value);
        write!(
            self.writer,
            " {}={:?}",
            field_name,
            quote_and_escape(&debug_formatted)
        )
        .ok();

        let display_formatted = format!("{}", value);
        write!(
            self.writer,
            " {}.display={}",
            field_name,
            quote_and_escape(&display_formatted)
        )
        .ok();
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // Note this appears to be invoked via `debug!` and `info! macros
        let formatted_value = format!("{:?}", value);
        write!(
            self.writer,
            " {}={}",
            translate_field_name(field.name()),
            quote_and_escape(&formatted_value)
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

    value.contains(' ') && !pre_quoted
}

/// escape any characters in name as needed, otherwise return string as is
fn quote_and_escape(value: &'_ str) -> Cow<'_, str> {
    if needs_quotes_and_escaping(value) {
        Cow::Owned(format!(
            "\"{}\"",
            value.replace(r#"\"#, r#"\\"#).replace(r#"""#, r#"\""#)
        ))
    } else {
        Cow::Borrowed(value)
    }
}

// Translate the field name from tracing into the logfmt style
fn translate_field_name(name: &str) -> &str {
    if name == "message" {
        "msg"
    } else {
        name
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn quote_and_escape_len0() {
        assert_eq!(quote_and_escape(""), "");
    }

    #[test]
    fn quote_and_escape_len1() {
        assert_eq!(quote_and_escape("f"), "f");
    }

    #[test]
    fn quote_and_escape_len2() {
        assert_eq!(quote_and_escape("fo"), "fo");
    }

    #[test]
    fn quote_and_escape_len3() {
        assert_eq!(quote_and_escape("foo"), "foo");
    }

    #[test]
    fn quote_and_escape_len3_1quote_start() {
        assert_eq!(quote_and_escape("\"foo"), "\"\\\"foo\"");
    }

    #[test]
    fn quote_and_escape_len3_1quote_end() {
        assert_eq!(quote_and_escape("foo\""), "\"foo\\\"\"");
    }

    #[test]
    fn quote_and_escape_len3_2quote() {
        assert_eq!(quote_and_escape("\"foo\""), "\"foo\"");
    }

    #[test]
    fn quote_and_escape_space() {
        assert_eq!(quote_and_escape("foo bar"), "\"foo bar\"");
    }

    #[test]
    fn quote_and_escape_space_prequoted() {
        assert_eq!(quote_and_escape("\"foo bar\""), "\"foo bar\"");
    }

    #[test]
    fn quote_and_escape_space_prequoted_but_not_escaped() {
        assert_eq!(quote_and_escape("\"foo \"bar\""), "\"\\\"foo \\\"bar\\\"\"");
    }

    #[test]
    fn quote_and_escape_quoted_quotes() {
        assert_eq!(quote_and_escape("foo:\"bar\""), "\"foo:\\\"bar\\\"\"");
    }

    #[test]
    fn quote_and_escape_nested_1() {
        assert_eq!(quote_and_escape(r#"a "b" c"#), r#""a \"b\" c""#);
    }

    #[test]
    fn quote_and_escape_nested_2() {
        assert_eq!(
            quote_and_escape(r#"a "0 \"1\" 2" c"#),
            r#""a \"0 \\\"1\\\" 2\" c""#
        );
    }
}
