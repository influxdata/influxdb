use super::*;

/// Build a `WriteChunkStream` over a fixed sequence of input `Bytes`
/// chunks and collect every emitted chunk into a `Vec<String>` for easy
/// assertion.
async fn run_chunker(inputs: Vec<&str>, max_size: usize) -> Vec<String> {
    let inputs = inputs.into_iter().map(String::from).collect::<Vec<_>>();
    let stream = stream::iter(
        inputs
            .into_iter()
            .map(|s| Ok::<Bytes, Error>(Bytes::from(s))),
    )
    .boxed();
    let out: Vec<String> = WriteChunkStream::new(stream, max_size)
        .map(|chunk| {
            let chunk = chunk.expect("chunker should not emit errors");
            String::from_utf8(chunk.to_vec()).expect("chunker should emit valid UTF-8")
        })
        .collect()
        .await;

    // Verify that all chunks are below the max size
    for (idx, chunk) in out.iter().enumerate() {
        // If the chunk contains more than one line, it must be <= max_size.
        // A single-line chunk may be <= max_size, or may exceed it when emitting an oversize line.
        let num_newlines = chunk.chars().filter(|&c| c == '\n').count();
        if num_newlines > 1 {
            assert!(
                chunk.len() <= max_size,
                "chunk {chunk:?} exceeds max_size {max_size} but has {num_newlines} newlines"
            );
        }
        // all but last chunk must end with newline
        if idx < (out.len() - 1) {
            assert!(
                chunk.ends_with('\n'),
                "chunk {chunk:?} not newline-terminated"
            );
        }
    }
    out
}

#[test]
fn next_chunk_end_picks_largest_line_aligned_prefix_under_limit() {
    // "a\nb\nc\n" — last '\n' at index 5; with limit 6 we emit all six bytes.
    assert_eq!(next_chunk_end(b"a\nb\nc\n", 6, false), Some(6));
    // With a 4-byte window, only the first two lines (4 bytes) fit.
    assert_eq!(next_chunk_end(b"a\nb\nc\n", 4, false), Some(4));
}

#[test]
fn next_chunk_end_emits_oversize_line_when_window_has_no_newline() {
    // First line is 12 bytes incl. \n; limit 5 → look past window, find \n
    // at index 12, emit the whole first line as an oversize chunk.
    assert_eq!(next_chunk_end(b"verylongline\nshort\n", 5, false), Some(13));
}

#[test]
fn next_chunk_end_returns_none_when_no_newline_yet() {
    assert_eq!(next_chunk_end(b"partial", 16, false), None);
}

#[test]
fn next_chunk_end_emits_remainder_when_finished() {
    assert_eq!(next_chunk_end(b"no_newline", 16, true), Some(10));
}

#[tokio::test]
async fn empty_stream_yields_no_chunks() {
    assert!(run_chunker(vec![], 16).await.is_empty());
}

#[tokio::test]
async fn single_small_input_yields_single_chunk() {
    let chunks = run_chunker(vec!["cpu a=1\ncpu a=2\n"], 1024).await;
    assert_eq!(chunks, vec!["cpu a=1\ncpu a=2\n"]);
}

#[tokio::test]
async fn coalesces_inputs_split_mid_line() {
    // The newline arrives in the second input, must coalesce
    let chunks = run_chunker(vec!["cpu a=", "1\ncpu a=2\n"], 1024).await;
    assert_eq!(chunks, vec!["cpu a=1\ncpu a=2\n"]);
}

#[tokio::test]
async fn splits_over_max_size_at_newline_boundary() {
    // Each "cpu a=N\n" is 8 bytes; limit 16 means 2 lines per chunk.
    let chunks = run_chunker(vec!["cpu a=1\ncpu a=2\ncpu a=3\ncpu a=4\n"], 16).await;
    assert_eq!(chunks, vec!["cpu a=1\ncpu a=2\n", "cpu a=3\ncpu a=4\n"]);
}

#[tokio::test]
async fn splits_across_input_chunk_boundaries() {
    // Limit 16, but input dribbles in 5-byte pieces and lines straddle them.
    let chunks = run_chunker(
        vec![
            "cpu a", "=1\ncp", "u a=2", "\ncpu ", "a=3\nc", "pu a=", "4\n",
        ],
        16,
    )
    .await;

    assert_eq!(chunks, vec!["cpu a=1\ncpu a=2\n", "cpu a=3\ncpu a=4\n"]);
}

#[tokio::test]
async fn final_line_without_trailing_newline_is_emitted() {
    let chunks = run_chunker(vec!["cpu a=1\ncpu a=2"], 16).await;
    assert_eq!(chunks, vec!["cpu a=1\n", "cpu a=2"]);
}

#[tokio::test]
async fn line_larger_than_limit_emitted_as_oversize_chunk() {
    // First line is 21 bytes incl. \n; limit 8 → emitted whole-line oversize.
    let chunks = run_chunker(vec!["cpu a=12345678901234\ncpu b=1\n"], 8).await;
    assert_eq!(chunks, vec!["cpu a=12345678901234\n", "cpu b=1\n"]);
}

#[tokio::test]
async fn many_small_inputs_assemble_then_split() {
    // 8 lines × 8 bytes = 64 bytes, dribbled one byte at a time.
    let bytes = "cpu a=1\ncpu a=2\ncpu a=3\ncpu a=4\n\
                 cpu a=5\ncpu a=6\ncpu a=7\ncpu a=8\n";
    let inputs: Vec<&'static str> = bytes
        .as_bytes()
        .chunks(1)
        .map(|b| std::str::from_utf8(b).unwrap())
        .collect();

    let chunks = run_chunker(inputs, 16).await;
    assert_eq!(
        chunks,
        vec![
            "cpu a=1\ncpu a=2\n",
            "cpu a=3\ncpu a=4\n",
            "cpu a=5\ncpu a=6\n",
            "cpu a=7\ncpu a=8\n"
        ]
    );
}

#[tokio::test]
async fn propagates_input_errors() {
    // With max_size=1024, the chunker waits for more input before emitting
    // anything; when the error arrives it aborts, discards the buffered
    // "cpu a=1\n", and surfaces the error directly.
    let stream = stream::iter(vec![
        Ok::<Bytes, Error>(Bytes::from_static(b"cpu a=1\n")),
        Err(Error::NoInput),
    ])
    .boxed();
    let mut wcs = WriteChunkStream::new(stream, 1024);
    assert!(matches!(wcs.next().await, Some(Err(Error::NoInput))));
    // After an error the stream is done.
    assert!(wcs.next().await.is_none());
}

#[test]
fn count_lines_counts_lines_including_trailing_partial() {
    assert_eq!(count_lines(b""), 0);
    assert_eq!(count_lines(b"a\n"), 1);
    assert_eq!(count_lines(b"a\nb\nc\n"), 3);
    // No trailing newline → the final partial line still counts.
    assert_eq!(count_lines(b"a\nb"), 2);
    assert_eq!(count_lines(b"cpu a=1"), 1);
}

#[test]
fn metrics_accumulates() {
    let mut m = Metrics::default();
    m += Metrics::from_write(b"a\nb\n");
    m += Metrics::from_write(b"c\n");
    assert_eq!(m.chunk_count, 2);
    assert_eq!(m.lines_sent, 3);
    assert_eq!(m.bytes_sent, 6);
}

#[test]
fn metrics_report_empty() {
    let m = Metrics::default();
    assert_eq!(
        m.report(Duration::from_secs(42)).to_string(),
        "no data written"
    );
}

#[test]
fn metrics_report_uses_singular_for_one_request() {
    let m = Metrics {
        chunk_count: 1,
        bytes_sent: 100,
        lines_sent: 5,
    };
    let elapsed = Duration::from_secs(1);
    assert_eq!(
        m.report(elapsed).to_string(),
        "1s: 1 request (1.00 requests/sec), 5 lines (5 lines/s), 100B (100B/s)"
    );
}

#[test]
fn metrics_report_uses_plural_and_throughput() {
    let m = Metrics {
        chunk_count: 3,
        bytes_sent: 25_165_824,
        lines_sent: 100_000,
    };
    let elapsed = Duration::from_secs_f64(1.23);
    // 25165824 B = 24.00 MiB; rate ≈ 19.51 MiB/s
    // 100000 / 1.23 ≈ 81300.8 → 81301 lines/s
    // 3 / 1.23 ≈ 2.44 requests/sec
    // elapsed truncated to ms → "1s 230ms"
    assert_eq!(
        m.report(elapsed).to_string(),
        "1s 230ms: 3 requests (2.44 requests/sec), \
         100000 lines (81301 lines/s), \
         24.00MiB (19.51MiB/s)"
    );
}

#[test]
fn metrics_report_truncates_sub_ms_precision() {
    // 102 ms + 50 µs + 209 ns — sub-ms bits must NOT appear in output.
    let elapsed =
        Duration::from_millis(102) + Duration::from_micros(50) + Duration::from_nanos(209);
    let m = Metrics {
        chunk_count: 1,
        bytes_sent: 18,
        lines_sent: 1,
    };
    let line = m.report(elapsed).to_string();
    assert!(line.starts_with("102ms: "), "got: {line}");
    assert!(!line.contains("us"), "got: {line}");
    assert!(!line.contains("ns"), "got: {line}");
}

#[test]
fn metrics_report_same_value_at_different_elapsed_times() {
    // The same Metrics rendered against two elapsed times yields two
    // different reports (different throughputs). Shape needed by a
    // future per-chunk progress loop.
    let m = Metrics {
        chunk_count: 1,
        bytes_sent: 100,
        lines_sent: 10,
    };
    let one_sec = m.report(Duration::from_secs(1)).to_string();
    let two_sec = m.report(Duration::from_secs(2)).to_string();
    assert!(one_sec.contains("10 lines/s"));
    assert!(two_sec.contains("5 lines/s"));
}
