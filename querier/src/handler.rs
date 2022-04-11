//! Querier handler

use async_trait::async_trait;
use futures::{
    future::{BoxFuture, Shared},
    stream::FuturesUnordered,
    FutureExt, StreamExt, TryFutureExt,
};
use observability_deps::tracing::warn;
use std::sync::Arc;
use thiserror::Error;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{database::QuerierDatabase, poison::PoisonCabinet};

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

    /// Poison pills for testing.
    poison_cabinet: Arc<PoisonCabinet>,
}

impl QuerierHandlerImpl {
    /// Initialize the Querier
    pub fn new(database: Arc<QuerierDatabase>) -> Self {
        let shutdown = CancellationToken::new();
        let poison_cabinet = Arc::new(PoisonCabinet::new());

        let join_handles = vec![];
        Self {
            database,
            join_handles,
            shutdown,
            poison_cabinet,
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

        self.shutdown.cancelled().await;
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use iox_catalog::mem::MemCatalog;
    use object_store::ObjectStoreImpl;
    use query::exec::Executor;
    use time::{MockProvider, Time};

    use crate::create_ingester_connection_for_testing;

    use super::*;

    #[tokio::test]
    async fn test_shutdown() {
        let querier = TestQuerier::new().querier;

        // does not exit w/o shutdown
        tokio::select! {
            _ = querier.join() => panic!("querier finished w/o shutdown"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {},
        };

        querier.shutdown();

        tokio::time::timeout(Duration::from_millis(1000), querier.join())
            .await
            .unwrap();
    }

    struct TestQuerier {
        querier: QuerierHandlerImpl,
    }

    impl TestQuerier {
        fn new() -> Self {
            let metric_registry = Arc::new(metric::Registry::new());
            let catalog = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
            let object_store = Arc::new(ObjectStoreImpl::new_in_memory());
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
            let exec = Arc::new(Executor::new(1));
            let database = Arc::new(QuerierDatabase::new(
                catalog,
                metric_registry,
                object_store,
                time_provider,
                exec,
                create_ingester_connection_for_testing(),
            ));
            let querier = QuerierHandlerImpl::new(database);

            Self { querier }
        }
    }
}
