//! HTTP-wrapper test for [`pprof_heap_response`].
//!
//! Validates that the response status, content type, and body match what
//! downstream tooling expect, end-to-end with a real jemalloc dump.

#![cfg(not(target_env = "msvc"))]

use http_body_util::BodyExt;
use hyper::StatusCode;
use hyper::header::CONTENT_TYPE;
use jemalloc_pprof_http::pprof_heap_response;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

jemalloc_stats::install_default_malloc_conf_for_profiling!();

#[tokio::test(flavor = "multi_thread")]
async fn pprof_heap_response_returns_heap_v2_with_expected_headers() {
    // See jemalloc_stats::dump_heap_profile test for sizing rationale.
    let _big = vec![0u8; 64 * 1024 * 1024];

    let response = pprof_heap_response()
        .await
        .expect("response should build successfully");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/octet-stream"),
    );
    assert_eq!(
        response
            .headers()
            .get("Content-Disposition")
            .and_then(|v| v.to_str().ok()),
        Some("attachment; filename=\"heap.prof\""),
    );

    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("streaming body should drain")
        .to_bytes();
    assert!(
        bytes.starts_with(b"heap_v2/"),
        "body should be jemalloc heap_v2 text format; first bytes were: {:?}",
        String::from_utf8_lossy(&bytes[..bytes.len().min(32)]),
    );
    assert!(
        bytes.len() > 200,
        "profile should contain sample records (got {} bytes — likely header-only)",
        bytes.len(),
    );
}
