//! End-to-end test for `dump_heap_profile`.
//!
//! Validates the full malloc_conf plumbing: at process start jemalloc
//! reads `malloc_conf` (or `_rjem_malloc_conf` on prefixed platforms) for the
//! `prof:true` option; without that, `dump_heap_profile` returns
//! `ProfilingDisabled`.
//!
//! The test runs in its own integration-test binary so the global allocator
//! and `malloc_conf` static here don't collide with the lib's own
//! `#[cfg(test)] mod tests`.

#![cfg(not(target_env = "msvc"))]

use futures::StreamExt;
use jemalloc_stats::dump_heap_profile;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

jemalloc_stats::install_default_malloc_conf_for_profiling!();

#[tokio::test(flavor = "multi_thread")]
async fn dump_heap_profile_yields_heap_v2_bytes() {
    // Allocate well above the 1 MiB sample interval. jemalloc's sampler
    // decides per-allocation whether to record; for a single 64 MiB request
    // against a 1 MiB threshold the recording probability is effectively 1,
    // so the profile should always contain at least one stack.
    let _big = vec![0u8; 64 * 1024 * 1024];

    let mut stream = dump_heap_profile()
        .await
        .expect("dump should succeed with prof:true at startup");

    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.expect("chunk read"));
    }

    assert!(
        bytes.starts_with(b"heap_v2/"),
        "stream should yield jemalloc heap_v2 text format; first bytes were: {:?}",
        String::from_utf8_lossy(&bytes[..bytes.len().min(32)]),
    );
    const LARGER_THAN_HEADER_SIZE: usize = 200;
    assert!(
        bytes.len() > LARGER_THAN_HEADER_SIZE,
        "profile should contain sample records (got {} bytes — likely header-only)",
        bytes.len(),
    );
}
