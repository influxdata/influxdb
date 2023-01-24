//! A lazy connector for Tonic gRPC [`Channel`] instances.

use std::{sync::Arc, time::Duration};

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
    connection_task: JoinHandle<()>,
}

impl LazyConnector {
    /// Lazily connect to `addr`.
    pub fn new(addr: Endpoint) -> Self {
        let connection = Default::default();
        Self {
            addr: addr.clone(),
            connection: Arc::clone(&connection),
            connection_task: tokio::spawn(try_connect(addr, connection)),
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

        WriteServiceClient::new(conn).write(op).await?;
        Ok(())
    }
}

impl Drop for LazyConnector {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}

async fn try_connect(addr: Endpoint, connection: Arc<Mutex<Option<Channel>>>) {
    loop {
        match addr.connect().await {
            Ok(v) => {
                info!(endpoint = %addr.uri(), "connected to upstream ingester");
                *connection.lock() = Some(v);
                return;
            }
            Err(e) => warn!(
                endpoint = %addr.uri(),
                error=%e,
                "failed to connect to upstream ingester"
            ),
        }
        tokio::time::sleep(RETRY_INTERVAL).await;
    }
}
