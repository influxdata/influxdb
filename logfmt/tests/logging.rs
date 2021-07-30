// Note that this needs to be an integration test because since the tracing
// structures are global, once you se a logging subscriber you can't undo
// that.... So punting on that for now

use logfmt::LogFmtLayer;
use observability_deps::tracing::{debug, error, info, span, trace, warn, Level};
use observability_deps::tracing_subscriber::{self, fmt::MakeWriter, prelude::*};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use regex::Regex;
use std::{
    error::Error,
    fmt,
    io::{self, Cursor},
};

/// Compares the captured messages with the expected messages,
/// normalizing for time and location
#[macro_export]
macro_rules! assert_logs {
    ($CAPTURE: expr, $EXPECTED_LINES: expr) => {
        let expected_lines: Vec<String> = $EXPECTED_LINES.iter().map(|&s| s.into()).collect();
        let actual_lines = $CAPTURE.to_strings();

        let normalized_expected = normalize(expected_lines.iter());
        let normalized_actual = normalize(actual_lines.iter());

        assert_eq!(
            normalized_expected, normalized_actual,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\nnormalized_expected:\n\n{:#?}\nnormalized_actual:\n\n{:#?}\n\n",
            expected_lines, actual_lines,
            normalized_expected, normalized_actual
        )
    };
}

#[test]
fn level() {
    let capture = CapturedWriter::new();

    info!("This is an info message");
    debug!("This is a debug message");
    trace!("This is a trace message");
    warn!("This is a warn message");
    error!("This is a error message");

    let expected = vec![
        "level=info msg=\"This is an info message\" target=\"logging\" location=\"logfmt/tests/logging.rs:36\" time=1612181556329599000",
        "level=debug msg=\"This is a debug message\" target=\"logging\" location=\"logfmt/tests/logging.rs:37\" time=1612181556329618000",
        "level=trace msg=\"This is a trace message\" target=\"logging\" location=\"logfmt/tests/logging.rs:38\" time=1612181556329634000",
        "level=warn msg=\"This is a warn message\" target=\"logging\" location=\"logfmt/tests/logging.rs:39\" time=1612181556329646000",
        "level=error msg=\"This is a error message\" target=\"logging\" location=\"logfmt/tests/logging.rs:40\" time=1612181556329661000",
    ];

    assert_logs!(capture, expected);
}

#[test]
fn event_fields_strings() {
    let capture = CapturedWriter::new();
    info!(
        event_name = "foo bar",
        other_event = "baz",
        "This is an info message"
    );

    let expected = vec![
            "level=info msg=\"This is an info message\" event_name=\"foo bar\" other_event=baz target=\"logging\" location=\"logfmt/tests/logging.rs:59\" time=1612187170712973000",
    ];

    assert_logs!(capture, expected);
}

#[test]
fn event_fields_strings_quoting() {
    let capture = CapturedWriter::new();
    info!(foo = r#"body: Body(Full(b"{\"error\": \"Internal error\"}"))"#,);

    let expected = vec![
        r#"level=info foo="body: Body(Full(b\"{\\\"error\\\": \\\"Internal error\\\"}\"))" target="logging" location="logfmt/tests/logging.rs:59" time=1612187170712973000"#,
    ];

    assert_logs!(capture, expected);
}

#[test]
fn test_without_normalization() {
    let capture = CapturedWriter::new();
    info!(
        event_name = "foo bar",
        other_event = "baz",
        "This is an info message"
    );

    // double assure that normalization isn't messing with things by
    // checking for presence of strings as well
    let log_string = normalize(capture.to_strings().iter()).join("\n");
    assert!(log_string.contains("This is an info message"));
    assert!(log_string.contains("event_name"));
    assert!(log_string.contains("other_event"));
    assert!(log_string.contains("baz"));
    assert!(log_string.contains("foo bar"));
}

#[test]
fn event_fields_numeric() {
    let capture = CapturedWriter::new();
    info!(bar = 1, frr = false, "This is an info message");

    let expected = vec![
        "level=info msg=\"This is an info message\" bar=1 frr=false target=\"logging\" location=\"logfmt/tests/logging.rs:72\" time=1612187170712947000",
    ];

    assert_logs!(capture, expected);
}

#[test]
fn event_fields_repeated() {
    let capture = CapturedWriter::new();
    info!(bar = 1, bar = 2, "This is an info message");

    let expected = vec![
        "level=info msg=\"This is an info message\" bar=1 bar=2 target=\"logging\" location=\"logfmt/tests/logging.rs:84\" time=1612187170712948000",
    ];

    assert_logs!(capture, expected);
}

#[test]
fn event_fields_errors() {
    let capture = CapturedWriter::new();

    let err: Box<dyn Error + 'static> =
        io::Error::new(io::ErrorKind::Other, "shaving yak failed!").into();

    error!(the_error = err.as_ref(), "This is an error message");
    let expected = vec![
        "level=error msg=\"This is an error message\" the_error=\"\\\"Custom { kind: Other, error: \\\\\\\"shaving yak failed!\\\\\\\" }\\\"\" the_error.display=\"shaving yak failed!\" target=\"logging\" location=\"logfmt/tests/logging.rs:99\" time=1612187170712947000",
    ];
    assert_logs!(capture, expected);
}

#[test]
fn event_fields_structs() {
    let capture = CapturedWriter::new();
    let my_struct = TestDebugStruct::new();

    info!(s = ?my_struct, "This is an info message");

    let expected = vec![
        "level=info msg=\"This is an info message\" s=\"TestDebugStruct { b: true, s: \\\"The String\\\" }\" target=\"logging\" location=\"logfmt/tests/logging.rs:111\" time=1612187170712937000",
    ];

    assert_logs!(capture, expected);
}

#[test]
fn event_spans() {
    // Demonstrate the inclusion of span_id (as `span`)
    let capture = CapturedWriter::new();
    let span = span!(Level::INFO, "my_span", foo = "bar");
    let enter = span.enter();
    info!(shave = "mo yak!", "info message in span");
    std::mem::drop(enter);

    let expected = vec![
        "level=info span_name=\"my_span\" foo=bar span=1 time=1612209178717290000",
        "level=info msg=\"info message in span\" shave=\"mo yak!\" span=1 target=\"logging\" location=\"logfmt/tests/logging.rs:132\" time=1612209178717329000",
    ];

    assert_logs!(capture, expected);
}

#[test]
fn event_multi_span() {
    // Demonstrate the inclusion of span_id (as `span`)
    let capture = CapturedWriter::new();

    let span1 = span!(Level::INFO, "my_span", foo = "bar");
    let _ = span1.enter();
    {
        let span2 = span!(Level::INFO, "my_second_span", foo = "baz");
        let _ = span2.enter();
        info!(shave = "yak!", "info message in span 2");
    }

    {
        let span3 = span!(Level::INFO, "my_second_span", foo = "brmp");
        let _ = span3.enter();
        info!(shave = "mo yak!", "info message in span 3");
    }

    let expected = vec![
        "level=info span_name=\"my_span\" foo=bar span=1 time=1612209327939714000",
        "level=info span_name=\"my_second_span\" foo=baz span=2 time=1612209327939743000",
        "level=info msg=\"info message in span 2\" shave=yak! target=\"logging\" location=\"logfmt/tests/logging.rs:154\" time=1612209327939774000",
        "level=info span_name=\"my_second_span\" foo=brmp span=3 time=1612209327939795000",
        "level=info msg=\"info message in span 3\" shave=\"mo yak!\" target=\"logging\" location=\"logfmt/tests/logging.rs:160\" time=1612209327939828000",
    ];

    assert_logs!(capture, expected);
}

// TODO: it might be nice to write some tests for time and location, but for now
// just punt

/// Test structure that has a debug representation
#[derive(Debug)]
struct TestDebugStruct {
    b: bool,
    s: String,
}
impl TestDebugStruct {
    fn new() -> Self {
        Self {
            b: true,
            s: "The String".into(),
        }
    }
}

impl fmt::Display for TestDebugStruct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Display for TestDebugStruct b:{} s:\"{}\"",
            self.b, self.s
        )
    }
}

/// Normalize lines for easy comparison
fn normalize<'a>(lines: impl Iterator<Item = &'a String>) -> Vec<String> {
    let lines = lines
        .map(|line| normalize_timestamp(line))
        .map(|line| normalize_location(&line))
        .collect();
    normalize_spans(lines)
}

/// s/time=1612187170712947000/time=NORMALIZED/g
fn normalize_timestamp(v: &str) -> String {
    let re = Regex::new(r#"time=\d+"#).unwrap();
    re.replace_all(v, "time=NORMALIZED").to_string()
}

/// s/location=\"logfmt/tests/logging.rs:128\"/location=NORMALIZED/g
fn normalize_location(v: &str) -> String {
    let re = Regex::new(r#"location=".*?""#).unwrap();
    re.replace_all(v, "location=NORMALIZED").to_string()
}

/// s/span=1/span=SPAN1/g
fn normalize_spans(lines: Vec<String>) -> Vec<String> {
    // since there can be multiple unique span values, need to normalize them
    // differently
    //
    // Note: we include leading and trailing spaces so that span=2
    // doesn't also match span=21423
    let re = Regex::new(r#" span=(\d+) "#).unwrap();

    // This collect isn't needless: the `fold` below moves `lines`, so this
    // iterator can't borrow `lines`, we need to collect into a `Vec` to
    // stop borrowing `lines`.
    // See https://github.com/rust-lang/rust-clippy/issues/7336
    #[allow(clippy::needless_collect)]
    let span_ids: Vec<String> = lines
        .iter()
        .map(|line| re.find_iter(line))
        .flatten()
        .map(|m| m.as_str().to_string())
        .collect();

    // map span ids to something uniform
    span_ids
        .into_iter()
        .enumerate()
        .fold(lines, |lines, (idx, orig_id)| {
            // replace old span
            let new_id = format!(" span=SPAN{} ", idx);
            let re = Regex::new(&orig_id).unwrap();
            lines
                .into_iter()
                .map(|line| re.replace_all(&line as &str, &new_id as &str).to_string())
                .collect()
        })
}

// Each thread has a local collection of lines that is captured to
// This is needed because the rust test framework runs the
// tests potentially using multiple threads but there is a single
// global logger.
thread_local! {
    static LOG_LINES: Mutex<Cursor<Vec<u8>>> = Mutex::new(Cursor::new(Vec::new()));
}

// Since we can only setup logging once, we need to have gloabl to
// use it among test cases
static GLOBAL_WRITER: Lazy<Mutex<CapturedWriter>> = Lazy::new(|| {
    let capture = CapturedWriter::default();
    tracing_subscriber::registry()
        .with(LogFmtLayer::new(capture.clone()))
        .init();
    Mutex::new(capture)
});

// This thing captures log lines
#[derive(Default, Clone)]
struct CapturedWriter {
    // all state is held in the LOG_LINES thread local variable
}

impl CapturedWriter {
    fn new() -> Self {
        let global_writer = GLOBAL_WRITER.lock();
        global_writer.clone().clear()
    }

    /// Clear all thread local state
    fn clear(self) -> Self {
        LOG_LINES.with(|lines| {
            let mut cursor = lines.lock();
            cursor.get_mut().clear()
        });
        self
    }

    fn to_strings(&self) -> Vec<String> {
        LOG_LINES.with(|lines| {
            let cursor = lines.lock();
            let bytes: Vec<u8> = cursor.get_ref().clone();
            String::from_utf8(bytes)
                .expect("valid utf8")
                .lines()
                .map(|s| s.to_string())
                .collect()
        })
    }
}

impl fmt::Display for CapturedWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for s in self.to_strings() {
            writeln!(f, "{}", s)?
        }
        Ok(())
    }
}

impl std::io::Write for CapturedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        LOG_LINES.with(|lines| {
            let mut cursor = lines.lock();
            cursor.write(buf)
        })
    }

    fn flush(&mut self) -> std::io::Result<()> {
        LOG_LINES.with(|lines| {
            let mut cursor = lines.lock();
            cursor.flush()
        })
    }
}

impl MakeWriter for CapturedWriter {
    type Writer = Self;

    fn make_writer(&self) -> Self::Writer {
        self.clone()
    }
}
