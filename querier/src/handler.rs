//! Querier handler

use async_trait::async_trait;
use futures::{
    future::{BoxFuture, Shared},
    stream::FuturesUnordered,
    FutureExt, StreamExt, TryFutureExt,
};
use iox_catalog::interface::Catalog;
use object_store::ObjectStore;
use observability_deps::tracing::warn;
use std::sync::Arc;
use thiserror::Error;
use time::TimeProvider;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::database::{database_sync_loop, QuerierDatabase};

#[derive(Debug, Error)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {}

/// The [`QuerierHandler`] does nothing at this point
#[async_trait]
pub trait QuerierHandler: Send + Sync {
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

/// Implementation of the `QuerierHandler` trait (that currently does nothing)
#[derive(Debug)]
pub struct QuerierHandlerImpl {
    /// Database.
    database: Arc<QuerierDatabase>,

    /// Future that resolves when the background worker exits
    join_handles: Vec<(String, SharedJoinHandle)>,

    /// A token that is used to trigger shutdown of the background worker
    shutdown: CancellationToken,
}

impl QuerierHandlerImpl {
    /// Initialize the Querier
    pub fn new(
        catalog: Arc<dyn Catalog>,
        metric_registry: Arc<metric::Registry>,
        object_store: Arc<ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        let database = Arc::new(QuerierDatabase::new(
            catalog,
            metric_registry,
            object_store,
            time_provider,
        ));
        let shutdown = CancellationToken::new();

        let join_handles = vec![(
            String::from("database sync"),
            shared_handle(tokio::spawn(database_sync_loop(
                Arc::clone(&database),
                shutdown.clone(),
            ))),
        )];
        Self {
            database,
            join_handles,
            shutdown,
        }
    }
}

#[async_trait]
impl QuerierHandler for QuerierHandlerImpl {
    async fn join(&self) {
        // Need to poll handlers unordered to detect early exists of any worker in the list.
        let mut unordered: FuturesUnordered<_> = self
            .join_handles
            .iter()
            .cloned()
            .map(|(name, handle)| async move { handle.await.map(|_| name) })
            .collect();

        while let Some(e) = unordered.next().await {
            let name = e.unwrap();

            if !self.shutdown.is_cancelled() {
                panic!("Background worker '{name}' exited early!");
            }
        }
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

impl Drop for QuerierHandlerImpl {
    fn drop(&mut self) {
        if !self.shutdown.is_cancelled() {
            warn!("QuerierHandlerImpl dropped without calling shutdown()");
            self.shutdown.cancel();
        }
    }
}
