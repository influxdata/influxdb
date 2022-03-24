//! Compactor handler

use async_trait::async_trait;
use backoff::BackoffConfig;
use data_types2::SequencerId;
use futures::{
    future::{BoxFuture, Shared},
    FutureExt, TryFutureExt,
};
use iox_catalog::interface::Catalog;
use object_store::DynObjectStore;
use observability_deps::tracing::warn;
use query::exec::Executor;
use std::sync::Arc;
use thiserror::Error;
use time::TimeProvider;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::compact::Compactor;

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

/// A [`JoinHandle`] that can be cloned
type SharedJoinHandle = Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>;

/// Convert a [`JoinHandle`] into a [`SharedJoinHandle`].
fn shared_handle(handle: JoinHandle<()>) -> SharedJoinHandle {
    handle.map_err(Arc::new).boxed().shared()
}

/// Implementation of the `CompactorHandler` trait (that currently does nothing)
#[derive(Debug)]
pub struct CompactorHandlerImpl {
    /// Data to compact
    compactor_data: Arc<Compactor>,

    /// A token that is used to trigger shutdown of the background worker
    shutdown: CancellationToken,
}

impl CompactorHandlerImpl {
    /// Initialize the Compactor
    pub fn new(
        sequencers: Vec<SequencerId>,
        catalog: Arc<dyn Catalog>,
        object_store: Arc<DynObjectStore>,
        exec: Arc<Executor>,
        time_provider: Arc<dyn TimeProvider>,
        _registry: &metric::Registry,
    ) -> Self {
        let shutdown = CancellationToken::new();

        let compactor_data = Arc::new(Compactor::new(
            sequencers,
            catalog,
            object_store,
            exec,
            time_provider,
            BackoffConfig::default(),
        ));

        Self {
            compactor_data,
            shutdown,
        }
    }
}

#[async_trait]
impl CompactorHandler for CompactorHandlerImpl {
    async fn join(&self) {
        // TODO: this should block on the compactor background loop exiting, not
        // this token being cancelled.
        //
        // This fires immediately when shutdown() is called.
        self.shutdown.cancelled().await
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
