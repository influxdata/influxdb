use super::*;

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
