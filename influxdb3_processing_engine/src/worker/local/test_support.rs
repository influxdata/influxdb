//! Shared fixtures for the worker's tests.

use super::*;
use crate::environment::TestManager;
use crate::query::UnimplementedQueryEndpoint;
use influxdb3_py_api::write::WriteAccumulator;
use iox_time::{MockProvider, Time};
use object_store::memory::InMemory;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;

/// A [`TriggerScheduler`] that accepts every callback and records nothing.
///
/// The worker refuses work from a scheduler it cannot report back to, so tests
/// that expect a submission to be accepted have to register one of these.
pub(crate) struct NoopTriggerScheduler(pub(crate) Arc<str>);

impl TriggerScheduler for NoopTriggerScheduler {
    fn node_id(&self) -> Arc<str> {
        Arc::clone(&self.0)
    }

    fn work_progressed(&self, _worker_node_id: Arc<str>, _work_id: TriggerWorkId) {}

    fn work_finished(&self, _worker_node_id: Arc<str>, _result: TriggerWorkResult) {}
}

/// Build a worker backed by an in-memory catalog and object store.
///
/// `plugin_dir` is only needed by tests that resolve real plugin code.
pub(crate) async fn test_worker_with_plugin_repo(
    node_id: &str,
    plugin_dir: Option<PathBuf>,
    plugin_repo: Option<String>,
) -> (Arc<PythonTriggerWorker>, Arc<Catalog>, CancellationToken) {
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(1)));
    let cache = Arc::new(Mutex::new(CacheStore::new(
        Arc::clone(&time_provider),
        Duration::from_secs(10),
    )));
    let catalog = Catalog::new(
        "test_host",
        Arc::new(InMemory::new()),
        Arc::clone(&time_provider),
        Default::default(),
    )
    .await
    .unwrap();
    let plugin_shutdown = CancellationToken::new();

    let worker = make_trigger_worker(TriggerWorkerContext {
        environment_manager: ProcessingEngineEnvironmentManager {
            plugin_dir,
            virtual_env_location: None,
            package_manager: Arc::new(TestManager),
            plugin_dir_only: false,
            plugin_repo,
        },
        catalog: Arc::clone(&catalog),
        node_id: Arc::from(node_id),
        write_endpoint: Arc::new(WriteAccumulator::default()),
        query_endpoint: Arc::new(UnimplementedQueryEndpoint),
        time_provider,
        cache,
        plugin_shutdown: plugin_shutdown.clone(),
        plugin_trigger_invocation_registry: None,
    });

    (worker, catalog, plugin_shutdown)
}

/// An HTTP server serving one gated connection per body, sequentially, so a
/// plugin load can be suspended mid-fetch at an exact point.
///
/// Returns the URL to use as the worker's plugin repo, a [`Notify`] that fires
/// when a request has arrived, and a [`Notify`] the test signals to release
/// the response.
pub(crate) async fn gated_plugin_repo(
    bodies: &'static [&'static str],
) -> (String, Arc<Notify>, Arc<Notify>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let request_arrived = Arc::new(Notify::new());
    let release_response = Arc::new(Notify::new());
    let arrived = Arc::clone(&request_arrived);
    let release = Arc::clone(&release_response);
    tokio::spawn(async move {
        for body in bodies {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let n = stream.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..n]);
            }
            arrived.notify_one();
            release.notified().await;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len(),
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        }
    });
    (url, request_arrived, release_response)
}
