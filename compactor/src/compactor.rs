//! Main compactor entry point.
use std::sync::Arc;

use futures::{
    future::{BoxFuture, Shared},
    FutureExt, TryFutureExt,
};
use generated_types::influxdata::iox::gossip::{v1::CompactionEvent, Topic};
use gossip::{NopDispatcher, TopicInterests};
use observability_deps::tracing::{info, warn};
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;
use tracker::AsyncSemaphoreMetrics;

use crate::{
    components::{
        hardcoded::hardcoded_components,
        report::{log_components, log_config},
    },
    config::Config,
    driver::compact,
};

/// A [`JoinHandle`] that can be cloned
type SharedJoinHandle = Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>;

/// Convert a [`JoinHandle`] into a [`SharedJoinHandle`].
fn shared_handle(handle: JoinHandle<()>) -> SharedJoinHandle {
    handle.map_err(Arc::new).boxed().shared()
}

/// Main compactor driver.
#[derive(Debug)]
pub struct Compactor {
    shutdown: CancellationToken,
    worker: SharedJoinHandle,
}

impl Compactor {
    /// Start compactor.
    pub async fn start(config: Config) -> Self {
        info!("compactor starting");
        log_config(&config);

        let shutdown = CancellationToken::new();
        let shutdown_captured = shutdown.clone();

        let components = hardcoded_components(&config);
        log_components(&components);

        let semaphore_metrics = Arc::new(AsyncSemaphoreMetrics::new(
            &config.metric_registry,
            &[("semaphore", "job")],
        ));
        let df_semaphore = Arc::new(semaphore_metrics.new_semaphore(config.df_concurrency.get()));

        // Initialise the gossip subsystem, if configured.
        let gossip = match config.gossip_bind_address {
            Some(bind) => {
                // Initialise the gossip subsystem.
                let handle = gossip::Builder::<_, Topic>::new(
                    config.gossip_seeds,
                    NopDispatcher,
                    Arc::clone(&config.metric_registry),
                )
                // Configure the compactor to subscribe to no topics - it
                // currently only sends events.
                .with_topic_filter(TopicInterests::default())
                .bind(bind)
                .await
                .expect("failed to start gossip reactor");

                let event_tx =
                    gossip_compaction::tx::CompactionEventTx::<CompactionEvent>::new(handle);

                Some(Arc::new(event_tx))
            }
            None => None,
        };

        let worker = tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_captured.cancelled() => {}
                _ = async {
                    compact(
                        config.trace_collector,
                        config.partition_concurrency,
                        config.partition_timeout,
                        Arc::clone(&df_semaphore),
                        &components,
                        gossip,
                    ).await;

                    info!("compactor done");
                } => {}
            }
        });
        let worker = shared_handle(worker);

        Self { shutdown, worker }
    }

    /// Trigger shutdown. You should [join](Self::join) afterwards.
    pub fn shutdown(&self) {
        info!("compactor shutting down");
        self.shutdown.cancel();
    }

    /// Wait until the compactor finishes.
    pub async fn join(&self) -> Result<(), Arc<JoinError>> {
        self.worker.clone().await
    }
}

impl Drop for Compactor {
    fn drop(&mut self) {
        if self.worker.clone().now_or_never().is_none() {
            warn!("Compactor was not shut down properly");
        }
    }
}
