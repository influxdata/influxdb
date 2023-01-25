//! Tool to clean up old object store files that don't appear in the catalog.

#![deny(
    rustdoc::broken_intra_doc_links,
    rust_2018_idioms,
    missing_debug_implementations,
    unreachable_pub
)]
#![warn(
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send,
    clippy::todo,
    clippy::dbg_macro
)]
#![allow(clippy::missing_docs_in_private_items)]

use async_trait::async_trait;
use futures::{
    future::{BoxFuture, Shared},
    prelude::*,
};
use garbage_collector::GarbageCollector;
use hyper::{Body, Request, Response};
use ioxd_common::{
    http::error::{HttpApiError, HttpApiErrorCode, HttpApiErrorSource},
    rpc::RpcBuilderInput,
    serve_builder,
    server_type::{RpcError, ServerType},
    setup_builder,
};
use metric::Registry;
use snafu::prelude::*;
use std::{fmt::Debug, sync::Arc, time::Duration};
use tokio::{select, sync::broadcast, task::JoinError, time};
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;

pub use garbage_collector::Config;

/// The object store garbage collection server
pub struct Server {
    metric_registry: Arc<metric::Registry>,
    worker: SharedCloneError<(), JoinError>,
    shutdown_tx: broadcast::Sender<()>,
}

impl Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server(GarbageCollector)")
            .finish_non_exhaustive()
    }
}

impl Server {
    /// Construct and start the object store garbage collector
    pub fn start(metric_registry: Arc<metric::Registry>, config: Config) -> Self {
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let worker = tokio::spawn(Self::worker_task(config, shutdown_rx));
        let worker = shared_clone_error(worker);

        Self {
            metric_registry,
            worker,
            shutdown_tx,
        }
    }

    async fn worker_task(config: Config, mut shutdown_rx: broadcast::Receiver<()>) {
        const ONE_HOUR: u64 = 60 * 60;
        let mut minimum_next_start_time = time::interval(Duration::from_secs(ONE_HOUR));

        loop {
            // Avoid a hot infinite loop when there's nothing to delete
            select! {
                _ = shutdown_rx.recv() => return,
                _ = minimum_next_start_time.tick() => {},
            }

            let handle = GarbageCollector::start(config.clone())
                .context(StartGarbageCollectorSnafu)
                .unwrap_or_report();
            let shutdown_garbage_collector = handle.shutdown_handle();
            let mut complete = shared_clone_error(handle.join());

            loop {
                select! {
                    _ = shutdown_rx.recv() => {
                        shutdown_garbage_collector();
                        complete.await.context(JoinGarbageCollectorSnafu).unwrap_or_report();
                        return;
                    },
                    v = &mut complete => {
                        v.context(JoinGarbageCollectorSnafu).unwrap_or_report();
                        break;
                    },
                }
            }
        }
    }
}

#[async_trait]
impl ServerType for Server {
    fn metric_registry(&self) -> Arc<Registry> {
        Arc::clone(&self.metric_registry)
    }

    fn trace_collector(&self) -> Option<Arc<dyn TraceCollector>> {
        None
    }

    async fn route_http_request(
        &self,
        _req: Request<Body>,
    ) -> Result<Response<Body>, Box<dyn HttpApiErrorSource>> {
        Err(Box::new(HttpNotFound))
    }

    async fn server_grpc(self: Arc<Self>, builder_input: RpcBuilderInput) -> Result<(), RpcError> {
        let builder = setup_builder!(builder_input, self);
        serve_builder!(builder);

        Ok(())
    }

    async fn join(self: Arc<Self>) {
        self.worker
            .clone()
            .await
            .context(JoinWorkerSnafu)
            .unwrap_or_report();
    }

    fn shutdown(&self, frontend: CancellationToken) {
        frontend.cancel();
        self.shutdown_tx.send(()).ok();
    }
}

#[derive(Debug, Snafu)]
struct HttpNotFound;

impl HttpApiErrorSource for HttpNotFound {
    fn to_http_api_error(&self) -> HttpApiError {
        HttpApiError::new(HttpApiErrorCode::NotFound, self.to_string())
    }
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Could not start the garbage collector"))]
    StartGarbageCollector { source: garbage_collector::Error },

    #[snafu(display("Could not join the garbage collector"))]
    JoinGarbageCollector {
        source: Arc<garbage_collector::Error>,
    },

    #[snafu(display("Could not join the garbage collector worker task"))]
    JoinWorker { source: Arc<JoinError> },
}

#[allow(missing_docs)]
pub type Result<T, E = Error> = std::result::Result<T, E>;

trait UnwrapOrReportExt<T> {
    fn unwrap_or_report(self) -> T;
}

impl<T> UnwrapOrReportExt<T> for Result<T> {
    fn unwrap_or_report(self) -> T {
        match self {
            Ok(v) => v,
            Err(e) => {
                use snafu::ErrorCompat;
                use std::fmt::Write;

                let mut message = String::new();

                writeln!(message, "{}", e).unwrap();
                for cause in ErrorCompat::iter_chain(&e).skip(1) {
                    writeln!(message, "Caused by: {cause}").unwrap();
                }

                panic!("{message}");
            }
        }
    }
}

type SharedCloneError<T, E> = Shared<BoxFuture<'static, Result<T, Arc<E>>>>;

fn shared_clone_error<F>(handle: F) -> SharedCloneError<F::Ok, F::Error>
where
    F: TryFuture + Send + 'static,
    F::Ok: Clone,
{
    handle.map_err(Arc::new).boxed().shared()
}
