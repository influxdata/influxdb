//! Logging support.

use std::time::Duration;

use object_store::{GetRange, path::Path};
use tracing::debug;

use crate::{OpResult, StoreType};

/// A giant structure with all possible log fields.
///
/// This is required because [`tracing`] does NOT support dynamic fields, see:
/// - [tokio-rs/tracing#341]
/// - [tokio-rs/tracing#2048]
///
///
/// [`tracing`]: https://tracing.rs/
/// [tokio-rs/tracing#341]: https://github.com/tokio-rs/tracing/issues/341
/// [tokio-rs/tracing#2048]: https://github.com/tokio-rs/tracing/pull/2048
#[derive(Debug)]
pub(crate) struct LogRecord {
    pub(crate) store_type: StoreType,
    pub(crate) op: &'static str,
    pub(crate) op_res: OpResult,
    pub(crate) context: LogContext,
    pub(crate) duration: Option<Duration>,
    pub(crate) bytes: Option<u64>,
    pub(crate) d_headers: Option<Duration>,
    pub(crate) d_first_byte: Option<Duration>,
    pub(crate) count: Option<u64>,
    pub(crate) bucket: Option<String>,
}

impl LogRecord {
    pub(crate) fn new(
        store_type: StoreType,
        op: &'static str,
        context: LogContext,
        bucket: &Option<String>,
    ) -> Self {
        Self {
            store_type,
            op,
            op_res: OpResult::Canceled,
            context,
            duration: None,
            bytes: None,
            d_headers: None,
            d_first_byte: None,
            count: None,
            bucket: bucket.clone(),
        }
    }

    fn emit(&self) {
        let Self {
            store_type,
            op,
            op_res,
            context,
            duration,
            bytes,
            d_headers,
            d_first_byte,
            count,
            bucket,
        } = self;

        let LogContext {
            location,
            prefix,
            offset,
            from,
            to,
            get_range,
        } = context;

        let result = match op_res {
            OpResult::Success => "success",
            OpResult::Error => "error",
            OpResult::Canceled => "canceled",
        };

        debug!(
            store_type = store_type.0.as_ref(),
            op,
            result,
            location = location.as_ref().map(|p| p.as_ref()),
            prefix = prefix.as_ref().map(|p| p.as_ref()),
            offset = offset.as_ref().map(|p| p.as_ref()),
            from = from.as_ref().map(|p| p.as_ref()),
            to = to.as_ref().map(|p| p.as_ref()),
            get_range = get_range.as_ref().map(tracing::field::display),
            duration_secs = duration.map(|d| d.as_secs_f64()),
            bytes,
            headers_secs = d_headers.map(|d| d.as_secs_f64()),
            first_byte_secs = d_first_byte.map(|d| d.as_secs_f64()),
            count,
            bucket = bucket.as_ref(),
            "object store operation"
        );
    }
}

impl Drop for LogRecord {
    fn drop(&mut self) {
        self.emit();
    }
}

/// Context information for a log record.
///
/// This is information that is known when the respective operation is triggered/called, in contrast to measured data
/// by the metric wrapper.
#[derive(Debug, Default)]
pub(crate) struct LogContext {
    pub(crate) location: Option<Path>,
    pub(crate) prefix: Option<Path>,
    pub(crate) offset: Option<Path>,
    pub(crate) from: Option<Path>,
    pub(crate) to: Option<Path>,
    pub(crate) get_range: Option<GetRange>,
}
