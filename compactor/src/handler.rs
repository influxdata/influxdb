//! Compactor handler

use async_trait::async_trait;
use iox_catalog::interface::Catalog;
use object_store::ObjectStore;
use observability_deps::tracing::warn;
use std::{fmt::Formatter, sync::Arc};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {}

/// The [`CompactorHandler`] does nothing at this point
#[async_trait]
pub trait CompactorHandler: Send + Sync {
    /// Wait until the handler finished  to shutdown.
    ///
    /// Use [`shutdown`](Self::shutdown) to trigger a shutdown.
    async fn join(&self);

    /// Shut down background workers.
    fn shutdown(&self);
}

/// Implementation of the `CompactorHandler` trait (that currently does nothing)
pub struct CompactorHandlerImpl {
    /// The global catalog for schema, parquet files and tombstones
    catalog: Arc<dyn Catalog>,
    /// Object store for persistence of parquet files
    object_store: Arc<ObjectStore>,
    /// A token that is used to trigger shutdown of the background worker
    shutdown: CancellationToken,
}

// LB: copied this over from the ingester handler; doesn't seem to be needed there either
impl std::fmt::Debug for CompactorHandlerImpl {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl CompactorHandlerImpl {
    /// Initialize the Compactor
    pub fn new(
        catalog: Arc<dyn Catalog>,
        object_store: Arc<ObjectStore>,
        _registry: &metric::Registry,
    ) -> Self {
        let shutdown = CancellationToken::new();

        // TODO: initialise compactor threads here

        Self {
            catalog,
            object_store,
            shutdown,
        }
    }
}

#[async_trait]
impl CompactorHandler for CompactorHandlerImpl {
    async fn join(&self) {
        // join compactor threads here
        todo!();
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

impl Drop for CompactorHandlerImpl {
    fn drop(&mut self) {
        if !self.shutdown.is_cancelled() {
            warn!("CompactorHandlerImpl dropped without calling shutdown()");
            self.shutdown.cancel();
        }
    }
}
