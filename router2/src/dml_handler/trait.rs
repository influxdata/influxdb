use std::fmt::Debug;

use async_trait::async_trait;
use dml::DmlOperation;
use mutable_batch_lp::PayloadStatistics;
use thiserror::Error;

/// Errors emitted by a [`DmlHandler`] implementation during DML request
/// processing.
#[derive(Debug, Error)]
pub enum DmlError {
    /// The database specified by the caller does not exist.
    #[error("database {0} does not exist")]
    DatabaseNotFound(String),

    /// An unknown error occured while processing the DML request.
    #[error("internal dml handler error: {0}")]
    Internal(Box<dyn std::error::Error + Send + Sync>),
}

/// An abstract handler of [`DmlOperation`] requests.
#[async_trait]
pub trait DmlHandler: Debug + Send + Sync {
    /// Apply `op` to `db_name`.
    async fn dispatch<'a>(
        &'a self,
        db_name: impl Into<String> + Send + Sync + 'a,
        op: impl Into<DmlOperation> + Send + Sync + 'a,
        payload_stats: PayloadStatistics,
        body_len: usize,
    ) -> Result<(), DmlError>;
}
