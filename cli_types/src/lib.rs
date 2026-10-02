//! Shared CLI argument types used by both OSS and Enterprise `influxdb3` binaries.

use std::fmt;
use std::str::FromStr;

/// The effective upper bound for query concurrency — equal to `tokio::sync::Semaphore::MAX_PERMITS`.
/// Used as the validation ceiling; setting the limit to this value means effectively unlimited.
pub const QUERY_CONCURRENCY_LIMIT_MAX: usize = tokio::sync::Semaphore::MAX_PERMITS;

/// Advisory minimum for `--max-concurrent-queries` in real-world deployments:
/// the CLI warns for explicit values below it, and the Enterprise runtime
/// configure API refuses them unless the client passes `?force=true`. Limits
/// this low mostly serialize query execution and suit testing and
/// experimentation, not production.
pub const QUERY_CONCURRENCY_LIMIT_ADVISORY_MIN: usize = 16;

/// Floor of the computed `--max-concurrent-queries` default: small nodes still
/// admit this many queries.
pub const DEFAULT_MAX_CONCURRENT_QUERIES_FLOOR: usize = 50;

/// Queries admitted per unit of effective parallelism in the computed
/// `--max-concurrent-queries` default.
pub const DEFAULT_MAX_CONCURRENT_QUERIES_PER_CORE: usize = 4;

/// The default query concurrency limit for a node whose effective query
/// parallelism (the smaller of the core count and the DataFusion thread
/// count) is `effective_parallelism`: `max(50, 4 × effective_parallelism)`.
/// The limit is an admission gate: during a burst, queries beyond it queue
/// rather than execute.
pub fn default_max_concurrent_queries(effective_parallelism: usize) -> usize {
    DEFAULT_MAX_CONCURRENT_QUERIES_FLOOR
        .max(DEFAULT_MAX_CONCURRENT_QUERIES_PER_CORE.saturating_mul(effective_parallelism))
}

/// The query concurrency limit a server runs with: the configured value when
/// the operator set one, otherwise [`default_max_concurrent_queries`] of the
/// effective parallelism `min(cores, datafusion_threads)` (threads default to
/// the core count when unset).
pub fn resolve_max_concurrent_queries(
    configured: Option<MaxConcurrentQueries>,
    datafusion_threads: Option<usize>,
    cores: usize,
) -> usize {
    match configured {
        Some(limit) => limit.0,
        None => default_max_concurrent_queries(cores.min(datafusion_threads.unwrap_or(cores))),
    }
}

/// Upper bound on concurrently-executing queries, parsed from the `--max-concurrent-queries`
/// CLI flag. Valid range is `1..=QUERY_CONCURRENCY_LIMIT_MAX`.
#[derive(Debug, Clone, Copy)]
pub struct MaxConcurrentQueries(pub usize);

impl FromStr for MaxConcurrentQueries {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let n: usize = s.parse().map_err(|e| format!("invalid integer: {e}"))?;
        if n == 0 {
            return Err("must be a positive integer, got 0".into());
        }
        if n > QUERY_CONCURRENCY_LIMIT_MAX {
            return Err(format!("exceeds maximum of {QUERY_CONCURRENCY_LIMIT_MAX}"));
        }
        Ok(Self(n))
    }
}

impl fmt::Display for MaxConcurrentQueries {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod lib_tests;
