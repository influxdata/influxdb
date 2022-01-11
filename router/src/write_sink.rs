use std::sync::Arc;

use data_types::{
    router::{
        WriteSink as WriteSinkConfig, WriteSinkSet as WriteSinkSetConfig,
        WriteSinkVariant as WriteSinkVariantConfig,
    },
    server_id::ServerId,
    write_buffer::WriteBufferConnection,
};
use dml::DmlOperation;
use snafu::{OptionExt, ResultExt, Snafu};

use crate::{
    connection_pool::{ConnectionError, ConnectionPool},
    resolver::Resolver,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No remote for server ID {}", server_id))]
    NoRemote { server_id: ServerId },

    #[snafu(display("Cannot connect: {}", source))]
    ConnectionFailure { source: ConnectionError },

    #[snafu(display("Cannot write: {}", source))]
    WriteFailure {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

#[derive(Debug)]
struct VariantGrpcRemote {
    db_name: String,
    server_id: ServerId,
    resolver: Arc<Resolver>,
    connection_pool: Arc<ConnectionPool>,
}

impl VariantGrpcRemote {
    fn new(
        db_name: String,
        server_id: ServerId,
        resolver: Arc<Resolver>,
        connection_pool: Arc<ConnectionPool>,
    ) -> Self {
        Self {
            db_name,
            server_id,
            resolver,
            connection_pool,
        }
    }

    async fn write(&self, write: &DmlOperation) -> Result<(), Error> {
        let connection_string =
            self.resolver
                .resolve_remote(self.server_id)
                .context(NoRemoteSnafu {
                    server_id: self.server_id,
                })?;
        let client = self
            .connection_pool
            .grpc_client(&connection_string)
            .await
            .context(ConnectionFailureSnafu)?;

        client
            .write(&self.db_name, write)
            .await
            .context(WriteFailureSnafu)
    }
}

#[derive(Debug)]
struct VariantWriteBuffer {
    db_name: String,
    write_buffer_cfg: WriteBufferConnection,
    connection_pool: Arc<ConnectionPool>,
}

impl VariantWriteBuffer {
    fn new(
        db_name: String,
        write_buffer_cfg: WriteBufferConnection,
        connection_pool: Arc<ConnectionPool>,
    ) -> Self {
        Self {
            db_name,
            write_buffer_cfg,
            connection_pool,
        }
    }

    async fn write(&self, operation: &DmlOperation) -> Result<(), Error> {
        let write_buffer = self
            .connection_pool
            .write_buffer_producer(&self.db_name, &self.write_buffer_cfg)
            .await
            .context(ConnectionFailureSnafu)?;

        // TODO(marco): use multiple sequencers
        write_buffer
            .store_operation(0, operation)
            .await
            .context(WriteFailureSnafu)?;

        Ok(())
    }
}

#[derive(Debug)]
enum WriteSinkVariant {
    /// Send write to a remote server via gRPC
    GrpcRemote(VariantGrpcRemote),

    /// Send write to a write buffer (which may be backed by kafka, local disk, etc)
    WriteBuffer(VariantWriteBuffer),
}

/// Write sink abstraction.
#[derive(Debug)]
pub struct WriteSink {
    ignore_errors: bool,
    variant: WriteSinkVariant,
}

impl WriteSink {
    pub fn new(
        db_name: &str,
        config: WriteSinkConfig,
        resolver: Arc<Resolver>,
        connection_pool: Arc<ConnectionPool>,
    ) -> Self {
        let variant = match config.sink {
            WriteSinkVariantConfig::GrpcRemote(server_id) => WriteSinkVariant::GrpcRemote(
                VariantGrpcRemote::new(db_name.to_string(), server_id, resolver, connection_pool),
            ),
            WriteSinkVariantConfig::WriteBuffer(write_buffer_cfg) => WriteSinkVariant::WriteBuffer(
                VariantWriteBuffer::new(db_name.to_string(), write_buffer_cfg, connection_pool),
            ),
        };

        Self {
            ignore_errors: config.ignore_errors,
            variant,
        }
    }

    pub async fn write(&self, write: &DmlOperation) -> Result<(), Error> {
        let res = match &self.variant {
            WriteSinkVariant::GrpcRemote(v) => v.write(write).await,
            WriteSinkVariant::WriteBuffer(v) => v.write(write).await,
        };

        match res {
            Ok(()) => Ok(()),
            Err(_) if self.ignore_errors => Ok(()),
            e => e,
        }
    }
}

/// A set of [`WriteSink`]s.
#[derive(Debug)]
pub struct WriteSinkSet {
    sinks: Vec<WriteSink>,
}

impl WriteSinkSet {
    /// Create new set from config.
    pub fn new(
        db_name: &str,
        config: WriteSinkSetConfig,
        resolver: Arc<Resolver>,
        connection_pool: Arc<ConnectionPool>,
    ) -> Self {
        Self {
            sinks: config
                .sinks
                .into_iter()
                .map(|sink_config| {
                    WriteSink::new(
                        db_name,
                        sink_config,
                        Arc::clone(&resolver),
                        Arc::clone(&connection_pool),
                    )
                })
                .collect(),
        }
    }

    /// Write to sinks. Fails on first error.
    pub async fn write(&self, operation: &DmlOperation) -> Result<(), Error> {
        for sink in &self.sinks {
            sink.write(operation).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dml::DmlWrite;
    use mutable_batch_lp::lines_to_batches;
    use time::SystemProvider;
    use write_buffer::config::WriteBufferConfigFactory;

    use crate::grpc_client::MockClient;

    use super::*;

    #[tokio::test]
    async fn test_write_sink_error_handling() {
        let server_id = ServerId::try_from(1).unwrap();

        let resolver = Arc::new(Resolver::new(None));
        resolver.update_remote(server_id, String::from("1.2.3.4"));

        let time_provider = Arc::new(SystemProvider::new());
        let metric_registry = Arc::new(metric::Registry::new());
        let wb_factory = Arc::new(WriteBufferConfigFactory::new(
            time_provider,
            metric_registry,
        ));
        wb_factory.register_always_fail_mock(String::from("failing_wb"));
        let connection_pool = Arc::new(ConnectionPool::new(true, wb_factory).await);

        let client_grpc = connection_pool.grpc_client("1.2.3.4").await.unwrap();
        let client_grpc = client_grpc.as_any().downcast_ref::<MockClient>().unwrap();
        client_grpc.poison();

        let write = DmlOperation::Write(DmlWrite::new(
            lines_to_batches("foo x=1 1", 0).unwrap(),
            Default::default(),
        ));

        // gRPC, do NOT ignore errors
        let config = WriteSinkConfig {
            sink: WriteSinkVariantConfig::GrpcRemote(server_id),
            ignore_errors: false,
        };
        let sink = WriteSink::new(
            "my_db",
            config,
            Arc::clone(&resolver),
            Arc::clone(&connection_pool),
        );
        sink.write(&write).await.unwrap_err();

        // gRPC, ignore errors
        let config = WriteSinkConfig {
            sink: WriteSinkVariantConfig::GrpcRemote(server_id),
            ignore_errors: true,
        };
        let sink = WriteSink::new(
            "my_db",
            config,
            Arc::clone(&resolver),
            Arc::clone(&connection_pool),
        );
        sink.write(&write).await.unwrap();

        // write buffer, do NOT ignore errors
        let write_buffer_cfg = WriteBufferConnection {
            type_: String::from("mock"),
            connection: String::from("failing_wb"),
            ..Default::default()
        };
        let config = WriteSinkConfig {
            sink: WriteSinkVariantConfig::WriteBuffer(write_buffer_cfg.clone()),
            ignore_errors: false,
        };
        let sink = WriteSink::new(
            "my_db",
            config,
            Arc::clone(&resolver),
            Arc::clone(&connection_pool),
        );
        sink.write(&write).await.unwrap_err();

        // write buffer, ignore errors
        let config = WriteSinkConfig {
            sink: WriteSinkVariantConfig::WriteBuffer(write_buffer_cfg),
            ignore_errors: true,
        };
        let sink = WriteSink::new(
            "my_db",
            config,
            Arc::clone(&resolver),
            Arc::clone(&connection_pool),
        );
        sink.write(&write).await.unwrap();
    }

    #[tokio::test]
    async fn test_write_sink_set() {
        let server_id_1 = ServerId::try_from(1).unwrap();
        let server_id_2 = ServerId::try_from(2).unwrap();
        let server_id_3 = ServerId::try_from(3).unwrap();

        let resolver = Arc::new(Resolver::new(None));
        resolver.update_remote(server_id_1, String::from("1"));
        resolver.update_remote(server_id_2, String::from("2"));
        resolver.update_remote(server_id_3, String::from("3"));

        let connection_pool = Arc::new(ConnectionPool::new_testing().await);

        let client_1 = connection_pool.grpc_client("1").await.unwrap();
        let client_2 = connection_pool.grpc_client("2").await.unwrap();
        let client_3 = connection_pool.grpc_client("3").await.unwrap();
        let client_1 = client_1.as_any().downcast_ref::<MockClient>().unwrap();
        let client_2 = client_2.as_any().downcast_ref::<MockClient>().unwrap();
        let client_3 = client_3.as_any().downcast_ref::<MockClient>().unwrap();

        let sink_set = WriteSinkSet::new(
            "my_db",
            WriteSinkSetConfig {
                sinks: vec![
                    WriteSinkConfig {
                        sink: WriteSinkVariantConfig::GrpcRemote(server_id_1),
                        ignore_errors: false,
                    },
                    WriteSinkConfig {
                        sink: WriteSinkVariantConfig::GrpcRemote(server_id_2),
                        ignore_errors: false,
                    },
                    WriteSinkConfig {
                        sink: WriteSinkVariantConfig::GrpcRemote(server_id_3),
                        ignore_errors: false,
                    },
                ],
            },
            resolver,
            connection_pool,
        );

        let write_1 = DmlOperation::Write(DmlWrite::new(
            lines_to_batches("foo x=1 1", 0).unwrap(),
            Default::default(),
        ));
        sink_set.write(&write_1).await.unwrap();

        let writes_1 = [(String::from("my_db"), write_1.clone())];
        client_1.assert_writes(&writes_1);
        client_2.assert_writes(&writes_1);
        client_3.assert_writes(&writes_1);

        client_2.poison();

        let write_2 = DmlOperation::Write(DmlWrite::new(
            lines_to_batches("foo x=2 2", 0).unwrap(),
            Default::default(),
        ));
        sink_set.write(&write_2).await.unwrap_err();

        // The sink set stops on first non-ignored error. So
        // - client 1 got the new data
        // - client 2 failed, but still has the data from the first write
        // - client 3 got skipped due to the failure, but still has the data from the first write
        let writes_2 = [
            (String::from("my_db"), write_1.clone()),
            (String::from("my_db"), write_2.clone()),
        ];
        client_1.assert_writes(&writes_2);
        client_2.assert_writes(&writes_1);
        client_3.assert_writes(&writes_1);
    }
}
