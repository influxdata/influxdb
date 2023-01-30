//! A lazy connector for Tonic gRPC [`Channel`] instances.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use generated_types::influxdata::iox::ingester::v1::{
    write_service_client::WriteServiceClient, WriteRequest,
};
use observability_deps::tracing::*;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint};

use super::{client::WriteClient, RpcWriteError};

const RETRY_INTERVAL: Duration = Duration::from_secs(1);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

/// How many consecutive errors must be observed before opening a new connection
/// (at most once per [`RETRY_INTERVAL]).
const RECONNECT_ERROR_COUNT: usize = 10;

/// Lazy [`Channel`] connector.
///
/// Connections are attempted in a background thread every [`RETRY_INTERVAL`].
/// once a connection has been established, the [`Channel`] internally handles
/// reconnections as needed.
///
/// Returns [`RpcWriteError::UpstreamNotConnected`] when no connection is
/// available.
#[derive(Debug)]
pub struct LazyConnector {
    addr: Endpoint,
    connection: Arc<Mutex<Option<Channel>>>,

    /// The number of request errors observed without a single success.
    consecutive_errors: Arc<AtomicUsize>,
    /// A task that periodically opens a new connection to `addr` when
    /// `consecutive_errors` is more than [`RECONNECT_ERROR_COUNT`].
    connection_task: JoinHandle<()>,
}

impl LazyConnector {
    /// Lazily connect to `addr`.
    pub fn new(addr: Endpoint, request_timeout: Duration) -> Self {
        let addr = addr
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(request_timeout);
        let connection = Default::default();

        // Drive first connection by setting it above the connection limit.
        let consecutive_errors = Arc::new(AtomicUsize::new(RECONNECT_ERROR_COUNT + 1));
        Self {
            addr: addr.clone(),
            connection: Arc::clone(&connection),
            connection_task: tokio::spawn(try_connect(
                addr,
                connection,
                Arc::clone(&consecutive_errors),
            )),
            consecutive_errors,
        }
    }

    /// Returns `true` if a connection was established at some point in the
    /// past.
    ///
    /// If true, the connection may be active and healthy, or currently
    /// unusable.
    pub fn did_connect(&self) -> bool {
        self.connection.lock().is_some()
    }
}

#[async_trait]
impl WriteClient for LazyConnector {
    async fn write(&self, op: WriteRequest) -> Result<(), RpcWriteError> {
        let conn = self.connection.lock().clone();
        let conn =
            conn.ok_or_else(|| RpcWriteError::UpstreamNotConnected(self.addr.uri().to_string()))?;

        match WriteServiceClient::new(conn).write(op).await {
            Err(e) => {
                self.consecutive_errors.fetch_add(1, Ordering::Relaxed);
                return Err(e);
            }
            Ok(_) => {
                self.consecutive_errors.store(0, Ordering::Relaxed);
                Ok(())
            }
        }
    }
}

impl Drop for LazyConnector {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}

async fn try_connect(
    addr: Endpoint,
    connection: Arc<Mutex<Option<Channel>>>,
    consecutive_errors: Arc<AtomicUsize>,
) {
    loop {
        if consecutive_errors.load(Ordering::Relaxed) > RECONNECT_ERROR_COUNT {
            match addr.connect().await {
                Ok(v) => {
                    info!(endpoint = %addr.uri(), "connected to upstream ingester");
                    *connection.lock() = Some(v);
                    consecutive_errors.store(0, Ordering::Relaxed);
                }
                Err(e) => warn!(
                    endpoint = %addr.uri(),
                    error=%e,
                    "failed to connect to upstream ingester"
                ),
            }
        }
        tokio::time::sleep(RETRY_INTERVAL).await;
    }
}
