//! Shared CLI argument types used by both OSS and Enterprise `influxdb3` binaries.

use std::fmt;
use std::str::FromStr;

/// The effective upper bound for query concurrency — equal to `tokio::sync::Semaphore::MAX_PERMITS`.
/// Used as the default (effectively unlimited) and as the validation ceiling.
pub const QUERY_CONCURRENCY_LIMIT_MAX: usize = tokio::sync::Semaphore::MAX_PERMITS;

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
