//! Shared state for tracking object store health, used by the `/ready` endpoint.
//!
//! [`ObjectStoreHealth`] is intended to be shared between writers (an
//! `ObservedObjectStore` wrapper and a startup probe) and a reader (the
//! `/ready` HTTP handler). Reads must be lock-free since the handler may be
//! polled frequently.

use iox_time::Time;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Operator-friendly classification of an [`object_store::Error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorCategory {
    AuthFailure = 1,
    NotFound = 2,
    Timeout = 3,
    ConfigError = 4,
    Unknown = 5,
}

impl ErrorCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AuthFailure => "auth_failure",
            Self::NotFound => "not_found",
            Self::Timeout => "timeout",
            Self::ConfigError => "config_error",
            Self::Unknown => "unknown",
        }
    }

    pub fn hint(self) -> &'static str {
        match self {
            Self::AuthFailure => {
                "object store returned access denied; verify credentials and IAM policy"
            }
            Self::NotFound => {
                "object store target path was not found; verify bucket and prefix configuration"
            }
            Self::Timeout => {
                "object store request timed out; verify network connectivity to the endpoint"
            }
            Self::ConfigError => {
                "object store is misconfigured; check --object-store and related flags"
            }
            Self::Unknown => "object store request failed; check logs for details",
        }
    }

    /// Classify an [`object_store::Error`] into an operator-friendly category.
    ///
    /// Note: this does **not** return [`ErrorCategory::Timeout`]. The
    /// `object_store` crate has no dedicated timeout variant; timeouts surface
    /// as `Error::Generic` with the timeout wrapped inside an opaque source,
    /// and walking that chain to reliably detect timeouts is fragile across
    /// crate versions. Runtime operation timeouts are therefore categorized as
    /// [`ErrorCategory::Unknown`] by this function.
    ///
    /// [`ErrorCategory::Timeout`] is reserved for callers that know a timeout
    /// occurred because they applied an explicit `tokio::time::timeout`
    /// wrapper themselves (e.g. the startup probe in `ready_probe.rs`). Such
    /// callers should set the category directly via
    /// [`ObjectStoreHealth::record_error`] rather than relying on this
    /// function.
    pub fn categorize(err: &object_store::Error) -> Self {
        use object_store::Error;
        match err {
            Error::PermissionDenied { .. } | Error::Unauthenticated { .. } => Self::AuthFailure,
            Error::NotFound { .. } => Self::NotFound,
            Error::NotSupported { .. } | Error::NotImplemented | Error::InvalidPath { .. } => {
                Self::ConfigError
            }
            _ => Self::Unknown,
        }
    }
}

/// Lock-free shared state recording the last successful and last failed object
/// store operation.
///
/// `last_success_at` is nanoseconds since the Unix epoch in an [`AtomicI64`],
/// using `0` as the unset sentinel.
///
/// The last-error state (timestamp + category) is packed into a single
/// [`AtomicU64`] rather than two separate atomics. Packing ensures readers
/// always see a consistent `(timestamp, category)` pair: with two atomics and
/// `Relaxed` ordering, a reader could observe a new timestamp alongside an old
/// or zero category (or vice versa), which would render as a timestamp without
/// a category in `/ready` responses. Using a single atomic eliminates that
/// window without needing `Acquire`/`Release` ordering or a lock.
///
/// Packing layout (64 bits):
///   - bits 63..56: [`ErrorCategory`] discriminant (0 = unset)
///   - bits 55..0:  nanoseconds since the Unix epoch (truncated to 56 bits)
///
/// 56 bits of nanos covers through roughly year 2285, which is plenty for
/// practical server runtimes. We assume timestamps are non-negative; a
/// negative timestamp would alias into the category byte when masked. In
/// practice `TimeProvider::now()` returns non-negative values, so this does
/// not arise.
///
/// The packed value `0` is reserved for "no error recorded" (category 0 is
/// not a valid discriminant).
#[derive(Debug, Default)]
pub struct ObjectStoreHealth {
    last_success_at: AtomicI64,
    last_error: AtomicU64,
}

const ERROR_TIMESTAMP_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;

impl ObjectStoreHealth {
    /// Construct a fresh, shared [`ObjectStoreHealth`]. All fields start
    /// unset; [`is_ok`](Self::is_ok) returns `false` until the first success
    /// is recorded.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record that an object store operation succeeded at `at`. Overwrites
    /// any previously recorded success timestamp. Does not clear prior error
    /// state; [`last_error_at`](Self::last_error_at) keeps its value so
    /// operators retain historical context after recovery. Use
    /// [`is_ok`](Self::is_ok) or compare timestamps to determine current
    /// health.
    pub fn record_success(&self, at: Time) {
        self.last_success_at
            .store(at.timestamp_nanos(), Ordering::Relaxed);
    }

    /// Record that an object store operation failed at `at` with the given
    /// category. Writes atomically -- readers never observe a new timestamp
    /// with an old category (or vice versa). Overwrites any previously
    /// recorded error.
    pub fn record_error(&self, at: Time, category: ErrorCategory) {
        let nanos = at.timestamp_nanos() as u64 & ERROR_TIMESTAMP_MASK;
        let packed = ((category as u8 as u64) << 56) | nanos;
        self.last_error.store(packed, Ordering::Relaxed);
    }

    /// Return the timestamp of the most recent recorded success, or `None`
    /// if no success has ever been recorded.
    pub fn last_success_at(&self) -> Option<Time> {
        match self.last_success_at.load(Ordering::Relaxed) {
            0 => None,
            ns => Some(Time::from_timestamp_nanos(ns)),
        }
    }

    /// Return the timestamp of the most recent recorded error, or `None` if
    /// no error has ever been recorded. Paired with
    /// [`last_error_category`](Self::last_error_category) via a single
    /// atomic, so the two are always mutually consistent.
    pub fn last_error_at(&self) -> Option<Time> {
        let packed = self.last_error.load(Ordering::Relaxed);
        if packed == 0 {
            return None;
        }
        Some(Time::from_timestamp_nanos(
            (packed & ERROR_TIMESTAMP_MASK) as i64,
        ))
    }

    /// Return the category of the most recent recorded error, or `None` if
    /// no error has ever been recorded. Paired with
    /// [`last_error_at`](Self::last_error_at) via a single atomic.
    pub fn last_error_category(&self) -> Option<ErrorCategory> {
        let packed = self.last_error.load(Ordering::Relaxed);
        if packed == 0 {
            return None;
        }
        match (packed >> 56) as u8 {
            1 => Some(ErrorCategory::AuthFailure),
            2 => Some(ErrorCategory::NotFound),
            3 => Some(ErrorCategory::Timeout),
            4 => Some(ErrorCategory::ConfigError),
            5 => Some(ErrorCategory::Unknown),
            _ => None,
        }
    }

    /// True if the most recent known state is a success.
    ///
    /// Returns `false` when no success has ever been recorded, or when the most
    /// recent error is at or after the most recent success.
    pub fn is_ok(&self) -> bool {
        match (self.last_success_at(), self.last_error_at()) {
            (None, _) => false,
            (Some(_), None) => true,
            (Some(s), Some(e)) => s > e,
        }
    }
}

#[cfg(test)]
mod tests;
