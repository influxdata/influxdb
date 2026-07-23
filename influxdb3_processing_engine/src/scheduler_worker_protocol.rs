//! Scheduler-worker protocol for processing-engine trigger execution.
//!
//! # Synchronous decisions and eventual outcomes
//!
//! All methods on [`TriggerScheduler`] and [`TriggerWorker`] are intentionally synchronous and
//! infallible: they represent decisions and events, not the eventual outcome of execution or
//! transport. Calling [`TriggerWorker::submit_work`] only makes a decision to submit work;
//! execution success/failure is reported later through [`TriggerScheduler`]. Likewise, calling
//! [`TriggerWorker::cancel_work`] only makes a decision to request cancellation, not confirmation
//! that work has stopped.
//!
//! Network-based implementations must buffer these protocol messages and retry their transport.
//! Transport failures and execution failures are both eventual: neither should make these trait
//! methods asynchronous or turn a synchronous call into a final outcome.

use crate::plugins::PluginError;
use crate::scheduler::TriggerKey;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use hashbrown::HashMap;
use influxdb3_py_api::wal::WalFlushElement;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub(crate) struct TriggerExecutionError {
    message: String,
    cancelled: bool,
}

impl TriggerExecutionError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            cancelled: false,
        }
    }

    pub(crate) fn cancelled() -> Self {
        Self {
            message: "trigger execution cancelled".to_string(),
            cancelled: true,
        }
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled
    }
}

impl Display for TriggerExecutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TriggerExecutionError {}

impl From<PluginError> for TriggerExecutionError {
    fn from(value: PluginError) -> Self {
        Self::new(value.to_string())
    }
}

impl From<anyhow::Error> for TriggerExecutionError {
    fn from(value: anyhow::Error) -> Self {
        Self::new(value.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TriggerWorkId(Uuid);

impl TriggerWorkId {
    pub(crate) fn next() -> Self {
        Self(Uuid::now_v7())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RequestTriggerWork {
    pub(crate) query_params: HashMap<String, String>,
    pub(crate) headers: HashMap<String, String>,
    pub(crate) body: Bytes,
}

#[derive(Debug, Clone)]
pub(crate) enum TriggerWorkPayload {
    Wal {
        database_name: Arc<str>,
        wal_contents: Arc<[WalFlushElement]>,
    },
    Schedule {
        scheduled_at: DateTime<Utc>,
    },
    Request(RequestTriggerWork),
}

#[derive(Debug, Clone)]
pub(crate) struct TriggerWork {
    pub(crate) id: TriggerWorkId,
    pub(crate) key: TriggerKey,
    pub(crate) payload: TriggerWorkPayload,
}

/// The HTTP response produced by a request trigger.
///
/// The scheduler owns the connection-local response sender. Workers return this wire-safe value
/// so local and remote workers use the same completion protocol.
#[derive(Debug, Clone)]
pub(crate) struct TriggerResponse {
    pub(crate) status_code: u16,
    pub(crate) headers: HashMap<String, String>,
    pub(crate) body: String,
}

#[derive(Debug, Clone)]
pub(crate) enum TriggerWorkOutput {
    Complete,
    RequestResponse(TriggerResponse),
}

#[derive(Debug, Clone)]
pub(crate) struct TriggerWorkResult {
    pub(crate) work_id: TriggerWorkId,
    pub(crate) result: Result<TriggerWorkOutput, TriggerExecutionError>,
}

/// Scheduler endpoint that receives callbacks from a trigger worker.
pub(crate) trait TriggerScheduler: Send + Sync + 'static {
    /// The runtime node that owns this scheduler.
    fn node_id(&self) -> Arc<str>;

    /// Notify this scheduler that `worker_node_id` has started processing work.
    fn work_progressed(&self, worker_node_id: Arc<str>, work_id: TriggerWorkId);

    /// Notify this scheduler that `worker_node_id` has finished work.
    fn work_finished(&self, worker_node_id: Arc<str>, result: TriggerWorkResult);
}

/// Worker endpoint that accepts trigger work and cancellation decisions.
pub(crate) trait TriggerWorker: Send + Sync + 'static {
    /// The runtime node that owns this worker.
    fn node_id(&self) -> Arc<str>;

    /// Submit work on behalf of `scheduler_node_id`.
    ///
    /// This method only represents a synchronous decision to submit work. The worker must return
    /// quickly and report progress/completion to the supplied scheduler endpoint.
    fn submit_work(self: Arc<Self>, scheduler_node_id: Arc<str>, work: TriggerWork);

    /// Request cancellation of work submitted by `scheduler_node_id`.
    ///
    /// This method only represents a synchronous decision to request cancellation. It does not
    /// report whether the work was found or has stopped.
    fn cancel_work(self: Arc<Self>, scheduler_node_id: Arc<str>, work_id: TriggerWorkId);
}

#[cfg(test)]
mod tests;
