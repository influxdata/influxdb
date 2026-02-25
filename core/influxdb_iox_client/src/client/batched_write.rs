//! Batched write client for efficient bulk writing of line protocol data.
//!
//! This module provides a `BatchedWriteClient` that wraps the standard write client
//! and batches multiple write requests together before sending them. This is particularly
//! useful for high-throughput scenarios like query logging where many small writes
//! can be combined into fewer, larger requests.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::error;

use crate::{
    error::Error,
    write::{Client as WriteClient, DatabaseName},
};

/// A wrapper around either a regular or batched write client.
///
/// This enum allows code to work with either client type transparently,
/// which is particularly useful for optional batching functionality.
#[derive(Debug, Clone)]
pub enum MaybeBatchedWriteClient {
    /// Regular write client without batching
    Unbatched(WriteClient),
    /// Batched write client
    Batched(Arc<BatchedWriteClient>),
}

impl MaybeBatchedWriteClient {
    /// Write line protocol data to the specified database.
    pub async fn write_lp(
        &mut self,
        database: impl Into<DatabaseName> + Send,
        lp_data: impl Into<String> + Send,
    ) -> Result<(), Error> {
        match self {
            Self::Unbatched(client) => {
                client.write_lp(database, lp_data).await?;
                Ok(())
            }
            Self::Batched(client) => client.write_lp(database, lp_data).await,
        }
    }
}

/// Default maximum number of line protocol entries to batch before flushing
const DEFAULT_MAX_BATCH_SIZE: usize = 100;

/// Default flush interval for periodic flushing
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(3);

/// Configuration for the batched write client
#[derive(Debug, Clone, Copy)]
pub struct BatchedWriteClientConfig {
    /// Maximum number of line protocol entries to batch before flushing
    pub max_batch_size: usize,
    /// Interval at which to automatically flush pending writes, even if batch size hasn't been reached
    pub flush_interval: Duration,
}

impl Default for BatchedWriteClientConfig {
    fn default() -> Self {
        Self {
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
        }
    }
}

/// A batched write client that accumulates writes and flushes them in batches.
///
/// This client wraps a [`WriteClient`] and batches multiple write requests together
/// before sending them to reduce network overhead. Writes are flushed when:
/// - The batch reaches `max_batch_size` line protocol entries
/// - The `flush_interval` timer expires (default: 3 seconds)
/// - The client is dropped (graceful shutdown)
/// - `flush()` is explicitly called
///
/// # Example
///
/// ```no_run
/// # use influxdb_iox_client::{
/// #     connection::Builder,
/// #     write::Client as WriteClient,
/// #     batched_write::{BatchedWriteClient, BatchedWriteClientConfig},
/// # };
/// # #[tokio::main]
/// # async fn main() {
/// let connection = Builder::default()
///     .build("http://127.0.0.1:8080")
///     .await
///     .unwrap();
///
/// let write_client = WriteClient::new(connection);
/// let config = BatchedWriteClientConfig::default();
/// let batched_client = BatchedWriteClient::new(write_client, config);
///
/// // Writes are automatically batched
/// batched_client.write_lp("my_db", "cpu,host=a usage=0.5").await.unwrap();
/// batched_client.write_lp("my_db", "cpu,host=b usage=0.7").await.unwrap();
/// # }
/// ```
pub struct BatchedWriteClient {
    /// Internal state protected by a mutex
    inner: Arc<Mutex<BatchedWriteClientInner>>,

    /// Configuration
    config: BatchedWriteClientConfig,

    /// Shutdown flag for the background flush task
    shutdown: Arc<AtomicBool>,

    /// Handle to the background flush task
    _flush_task: JoinHandle<()>,
}

/// Internal state for the batched write client
#[derive(Debug)]
struct BatchedWriteClientInner {
    /// The underlying write client
    client: WriteClient,

    /// Buffer for accumulating writes per database
    buffer: Vec<(DatabaseName, String)>,
}

impl BatchedWriteClient {
    /// Creates a new batched write client with the given configuration.
    pub fn new(client: WriteClient, config: BatchedWriteClientConfig) -> Self {
        let inner = BatchedWriteClientInner {
            client,
            buffer: Vec::new(),
        };

        let inner = Arc::new(Mutex::new(inner));
        let shutdown = Arc::new(AtomicBool::new(false));

        // Spawn background task to periodically flush
        let flush_task = {
            let inner = Arc::clone(&inner);
            let shutdown = Arc::clone(&shutdown);
            let flush_interval = config.flush_interval;

            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(flush_interval).await;

                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }

                    let mut guard = inner.lock().await;
                    if let Err(e) = flush_buffer_internal(&mut guard).await {
                        error!("Failed to flush batched writes from timer: {}", e);
                    }
                }
            })
        };

        Self {
            inner,
            config,
            shutdown,
            _flush_task: flush_task,
        }
    }

    /// Creates a new batched write client with default configuration.
    pub fn new_with_defaults(client: WriteClient) -> Self {
        Self::new(client, BatchedWriteClientConfig::default())
    }

    /// Write line protocol data to the specified database.
    ///
    /// The write is buffered internally and will be flushed when the
    /// configured batch size is reached.
    pub async fn write_lp(
        &self,
        database: impl Into<DatabaseName> + Send,
        data: impl Into<String> + Send,
    ) -> Result<(), Error> {
        let database = database.into();
        let data = data.into();

        let mut inner = self.inner.lock().await;

        inner.buffer.push((database, data));

        if inner.buffer.len() >= self.config.max_batch_size {
            flush_buffer_internal(&mut inner).await?;
        }

        Ok(())
    }

    /// Explicitly flush all pending writes.
    ///
    /// This method blocks until all currently buffered writes have been sent.
    pub async fn flush(&self) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
        flush_buffer_internal(&mut inner).await
    }
}

impl std::fmt::Debug for BatchedWriteClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchedWriteClient")
            .field("inner", &self.inner)
            .field("config", &self.config)
            .field("shutdown", &self.shutdown)
            .field("_flush_task", &"<task>")
            .finish()
    }
}

impl Drop for BatchedWriteClient {
    fn drop(&mut self) {
        // Signal the background task to shut down
        self.shutdown.store(true, Ordering::Relaxed);

        // Try to flush remaining data on drop
        // We spawn a task since we can't use async in Drop
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            let mut guard = inner.lock().await;
            if !guard.buffer.is_empty()
                && let Err(e) = flush_buffer_internal(&mut guard).await
            {
                error!("Failed to flush remaining batched writes on drop: {}", e);
            }
        });
    }
}

/// Flush the buffer by grouping writes by database and sending them
async fn flush_buffer_internal(inner: &mut BatchedWriteClientInner) -> Result<(), Error> {
    if inner.buffer.is_empty() {
        return Ok(());
    }

    let mut by_database: std::collections::BTreeMap<DatabaseName, Vec<String>> =
        std::collections::BTreeMap::new();

    for (db, data) in inner.buffer.drain(..) {
        by_database.entry(db).or_default().push(data);
    }

    for (db_name, data_vec) in by_database {
        let combined = data_vec.join("\n");

        if let Err(e) = inner.client.write_lp(db_name.clone(), combined).await {
            error!(
                "Failed to write batched data for database {:?}: {}",
                db_name, e
            );
            return Err(e);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::RequestMaker;
    use futures_util::FutureExt;
    use futures_util::future::BoxFuture;
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct MockRequestMaker {
        requests: Mutex<Vec<String>>,
    }

    impl MockRequestMaker {
        fn new() -> Self {
            Self {
                requests: Mutex::new(vec![]),
            }
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl RequestMaker for MockRequestMaker {
        fn write_source(
            &self,
            _org_id: String,
            _bucket_id: String,
            body: String,
        ) -> BoxFuture<'_, Result<usize, Error>> {
            let sz = body.len();
            self.requests.lock().unwrap().push(body);
            async move { Ok(sz) }.boxed()
        }
    }

    #[tokio::test]
    async fn test_batching_by_size() {
        let mock = Arc::new(MockRequestMaker::new());
        let client = WriteClient::new_with_maker(Arc::clone(&mock) as _);

        let config = BatchedWriteClientConfig {
            max_batch_size: 3,
            flush_interval: Duration::from_secs(3600), // Long interval to not interfere with test
        };

        let batched = BatchedWriteClient::new(client, config);

        batched.write_lp("test_db", "m1 f=1").await.unwrap();
        batched.write_lp("test_db", "m2 f=2").await.unwrap();
        batched.write_lp("test_db", "m3 f=3").await.unwrap();

        let requests = mock.requests();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].contains("m1 f=1"));
        assert!(requests[0].contains("m2 f=2"));
        assert!(requests[0].contains("m3 f=3"));
    }

    #[tokio::test]
    async fn test_explicit_flush() {
        let mock = Arc::new(MockRequestMaker::new());
        let client = WriteClient::new_with_maker(Arc::clone(&mock) as _);

        let config = BatchedWriteClientConfig {
            max_batch_size: 100,
            flush_interval: Duration::from_secs(3600), // Long interval to not interfere with test
        };

        let batched = BatchedWriteClient::new(client, config);

        batched.write_lp("test_db", "m1 f=1").await.unwrap();
        batched.write_lp("test_db", "m2 f=2").await.unwrap();

        batched.flush().await.unwrap();

        let requests = mock.requests();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].contains("m1 f=1"));
        assert!(requests[0].contains("m2 f=2"));
    }

    #[tokio::test]
    async fn test_timer_flush() {
        let mock = Arc::new(MockRequestMaker::new());
        let client = WriteClient::new_with_maker(Arc::clone(&mock) as _);

        let config = BatchedWriteClientConfig {
            max_batch_size: 100,                        // High batch size so it won't trigger
            flush_interval: Duration::from_millis(100), // Short interval for testing
        };

        let batched = BatchedWriteClient::new(client, config);

        // Write some data that won't trigger batch size flush
        batched.write_lp("test_db", "m1 f=1").await.unwrap();
        batched.write_lp("test_db", "m2 f=2").await.unwrap();

        // Initially no flush should have happened
        assert_eq!(mock.requests().len(), 0);

        // Wait for the timer to trigger
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Now the timer should have flushed the data
        let requests = mock.requests();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].contains("m1 f=1"));
        assert!(requests[0].contains("m2 f=2"));
    }
}
