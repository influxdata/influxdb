use std::sync::Arc;

use cache_loader_async::cache_api::LoadingCache;
use data_types::write_buffer::WriteBufferConnection;
use observability_deps::tracing::debug;
use trace::TraceCollector;
use write_buffer::{
    config::WriteBufferConfigFactory,
    core::{WriteBufferError, WriteBufferWriting},
};

use crate::grpc_client::GrpcClient;

type KeyWriteBufferProducer = (String, WriteBufferConnection);
pub type ConnectionError = Arc<dyn std::error::Error + Send + Sync + 'static>;

/// Stupid hack to fit the `Box<dyn ...>` in `WriteBufferError` into an `Arc`
struct EWrapper(WriteBufferError);

impl std::fmt::Debug for EWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for EWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for EWrapper {}

/// Connection pool for the entire routing server.
///
/// This avoids:
/// 1. That every [`Router`](crate::router::Router) uses their own connections
/// 2. That we open too many connections in total.
#[derive(Debug)]
pub struct ConnectionPool {
    grpc_clients: LoadingCache<String, Arc<dyn GrpcClient>, ConnectionError>,
    write_buffer_producers:
        LoadingCache<KeyWriteBufferProducer, Arc<dyn WriteBufferWriting>, ConnectionError>,
}

impl ConnectionPool {
    /// Create new connection pool.
    ///
    /// If `use_mock_grpc` is set only mock gRPC clients are created.
    pub async fn new(
        use_mock_grpc: bool,
        wb_factory: Arc<WriteBufferConfigFactory>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
    ) -> Self {
        // Note: this function is async even though it does not contain any `.await` calls because `LoadingCache::new`
        // requires tokio to be running and even if documented people will forget about this.

        let grpc_clients = if use_mock_grpc {
            LoadingCache::new(|_connection_string: String| async move {
                use crate::grpc_client::MockClient;

                Ok(Arc::new(MockClient::default()) as Arc<dyn GrpcClient>)
            })
        } else {
            LoadingCache::new(|connection_string: String| async move {
                use crate::grpc_client::RealClient;
                use influxdb_iox_client::connection::Builder;

                let connection = Builder::default()
                    .build(&connection_string)
                    .await
                    .map_err(|e| Arc::new(e) as ConnectionError)?;
                Ok(Arc::new(RealClient::new(connection)) as Arc<dyn GrpcClient>)
            })
        };

        let write_buffer_producers = LoadingCache::new(move |key: KeyWriteBufferProducer| {
            let wb_factory = Arc::clone(&wb_factory);
            let trace_collector = trace_collector.clone();
            async move {
                wb_factory
                    .new_config_write(&key.0, trace_collector.as_ref(), &key.1)
                    .await
                    .map_err(|e| Arc::new(EWrapper(e)) as ConnectionError)
            }
        });

        Self {
            grpc_clients,
            write_buffer_producers,
        }
    }

    /// Create new connection factory for testing purposes.
    #[cfg(test)]
    pub async fn new_testing() -> Self {
        use time::SystemProvider;

        let time_provider = Arc::new(SystemProvider::new());
        let metric_registry = Arc::new(metric::Registry::new());
        Self::new(
            true,
            Arc::new(WriteBufferConfigFactory::new(
                time_provider,
                metric_registry,
            )),
            None,
        )
        .await
    }

    /// Get gRPC client given a connection string.
    pub async fn grpc_client(
        &self,
        connection_string: &str,
    ) -> Result<Arc<dyn GrpcClient>, ConnectionError> {
        let res = self
            .grpc_clients
            .get_with_meta(connection_string.to_string())
            .await
            .map_err(|e| Arc::new(e) as ConnectionError)?;
        debug!(was_cached=%res.cached, %connection_string, "getting IOx client");
        Ok(res.result)
    }

    /// Get write buffer producer given a DB name and config.
    pub async fn write_buffer_producer(
        &self,
        db_name: &str,
        cfg: &WriteBufferConnection,
    ) -> Result<Arc<dyn WriteBufferWriting>, ConnectionError> {
        let res = self
            .write_buffer_producers
            .get_with_meta((db_name.to_string(), cfg.clone()))
            .await
            .map_err(|e| Arc::new(e) as ConnectionError)?;
        debug!(was_cached=%res.cached, %db_name, "getting write buffer");
        Ok(res.result)
    }
}

#[cfg(test)]
mod tests {
    use time::{SystemProvider, TimeProvider};

    use crate::grpc_client::MockClient;

    use super::*;

    #[tokio::test]
    async fn test_grpc_mocking() {
        let time_provider: Arc<dyn TimeProvider> = Arc::new(SystemProvider::new());
        let metric_registry = Arc::new(metric::Registry::new());

        let pool1 = ConnectionPool::new(
            false,
            Arc::new(WriteBufferConfigFactory::new(
                Arc::clone(&time_provider),
                Arc::clone(&metric_registry),
            )),
            None,
        )
        .await;
        // connection will fail
        pool1.grpc_client("foo").await.unwrap_err();

        let pool2 = ConnectionPool::new(
            true,
            Arc::new(WriteBufferConfigFactory::new(
                time_provider,
                metric_registry,
            )),
            None,
        )
        .await;
        let client2 = pool2.grpc_client("foo").await.unwrap();
        client2.as_any().downcast_ref::<MockClient>().unwrap();
    }
}
