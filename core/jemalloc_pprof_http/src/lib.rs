//! Shared HTTP handler for the jemalloc heap profile endpoint.
//!
//! Both the OSS and Enterprise `influxdb3_server` crates delegate to
//! [`pprof_heap_response`] so the wire format, headers, and error mapping stay
//! identical across binaries. Use this crate's [`API_DEBUG_PPROF_HEAP`]
//! constant for route registration.

use hyper::{StatusCode, header::CONTENT_TYPE};
use iox_http_util::{
    Response, ResponseBuilder, bytes_to_response_body, stream_results_to_response_body,
};
use jemalloc_stats::HeapDumpError;

/// Path under which the heap profile endpoint is registered.
pub const API_DEBUG_PPROF_HEAP: &str = "/debug/pprof/heap";

/// Errors that can occur while building a heap-profile HTTP response.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Dump(#[from] jemalloc_stats::HeapDumpError),

    #[error(transparent)]
    Http(#[from] hyper::http::Error),
}

/// Trigger a jemalloc heap profile dump and build a streaming Response
/// containing the resulting `heap_v2` bytes.
///
/// Profiling must be enabled at process start (binaries install a
/// `malloc_conf` static via [`jemalloc_stats::install_default_malloc_conf_for_profiling!`],
/// which jemalloc reads at init time; operators override via
/// [`jemalloc_stats::MALLOC_CONF_ENV`]). When profiling is disabled this
/// returns `Ok` with a 503 response and a body naming the env var to set.
pub async fn pprof_heap_response() -> Result<Response, Error> {
    match jemalloc_stats::dump_heap_profile().await {
        Ok(stream) => Ok(ResponseBuilder::new()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/octet-stream")
            .header("Content-Disposition", "attachment; filename=\"heap.prof\"")
            .body(stream_results_to_response_body(stream))?),
        Err(e @ HeapDumpError::ProfilingDisabled) => Ok(ResponseBuilder::new()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header(CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(bytes_to_response_body(e.to_string()))?),
        Err(e) => Err(e.into()),
    }
}
