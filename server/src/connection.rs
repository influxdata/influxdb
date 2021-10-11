use std::sync::Arc;

use async_trait::async_trait;
use cache_loader_async::cache_api::LoadingCache;
use snafu::{ResultExt, Snafu};

use entry::Entry;
use influxdb_iox_client::{connection::Builder, write};
use observability_deps::tracing::debug;

type RemoteServerError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Snafu)]
pub enum ConnectionManagerError {
    #[snafu(display("cannot connect to remote: {}", source))]
    RemoteServerConnectError { source: RemoteServerError },
    #[snafu(display("cannot write to remote: {}", source))]
    RemoteServerWriteError { source: write::WriteError },
}

/// The `Server` will ask the `ConnectionManager` for connections to a specific
/// remote server. These connections can be used to communicate with other
/// servers. This is implemented as a trait for dependency injection in testing.
#[async_trait]
pub trait ConnectionManager {
    type RemoteServer: RemoteServer + Send + Sync + 'static;

    async fn remote_server(
        &self,
        connect: &str,
    ) -> Result<Arc<Self::RemoteServer>, ConnectionManagerError>;
}

/// The `RemoteServer` represents the API for replicating, subscribing, and
/// querying other servers.
#[async_trait]
pub trait RemoteServer {
    /// Sends an Entry to the remote server. An IOx server acting as a
    /// router/sharder will call this method to send entries to remotes.
    async fn write_entry(&self, db: &str, entry: Entry) -> Result<(), ConnectionManagerError>;
}

/// The connection manager maps a host identifier to a remote server.
#[derive(Debug)]
pub struct ConnectionManagerImpl {
    cache: LoadingCache<String, Arc<RemoteServerImpl>, CacheFillError>,
}

// Error must be Clone because LoadingCache requires so.
#[derive(Debug, Snafu, Clone)]
pub enum CacheFillError {
    #[snafu(display("gRPC error: {}", source))]
    GrpcError {
        source: Arc<dyn std::error::Error + Send + Sync + 'static>,
    },
}

impl ConnectionManagerImpl {
    pub fn new() -> Self {
        let cache = LoadingCache::new(Self::cached_remote_server);
        Self { cache }
    }

    async fn cached_remote_server(
        connect: String,
    ) -> Result<Arc<RemoteServerImpl>, CacheFillError> {
        let connection = Builder::default()
            .build(&connect)
            .await
            .map_err(|e| Arc::new(e) as _)
            .context(GrpcError)?;
        let client = write::Client::new(connection);
        Ok(Arc::new(RemoteServerImpl { client }))
    }
}

impl Default for ConnectionManagerImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ConnectionManager for ConnectionManagerImpl {
    type RemoteServer = RemoteServerImpl;

    async fn remote_server(
        &self,
        connect: &str,
    ) -> Result<Arc<Self::RemoteServer>, ConnectionManagerError> {
        let ret = self
            .cache
            .get_with_meta(connect.to_string())
            .await
            .map_err(|e| Box::new(e) as _)
            .context(RemoteServerConnectError)?;
        debug!(was_cached=%ret.cached, %connect, "getting remote connection");
        Ok(ret.result)
    }
}

/// An implementation for communicating with other IOx servers. This should
/// be moved into and implemented in an influxdb_iox_client create at a later
/// date.
#[derive(Debug)]
pub struct RemoteServerImpl {
    client: write::Client,
}

#[async_trait]
impl RemoteServer for RemoteServerImpl {
    /// Sends an Entry to the remote server. An IOx server acting as a
    /// router/sharder will call this method to send entries to remotes.
    async fn write_entry(&self, db_name: &str, entry: Entry) -> Result<(), ConnectionManagerError> {
        self.client
            .clone() // cheap, see https://docs.rs/tonic/0.4.2/tonic/client/index.html#concurrent-usage
            .write_entry(db_name, entry)
            .await
            .context(RemoteServerWriteError)
    }
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use std::collections::BTreeMap;

    #[derive(Debug)]
    pub struct TestConnectionManager {
        pub remotes: BTreeMap<String, Arc<TestRemoteServer>>,
    }

    impl TestConnectionManager {
        pub fn new() -> Self {
            Self {
                remotes: BTreeMap::new(),
            }
        }
    }

    #[async_trait]
    impl ConnectionManager for TestConnectionManager {
        type RemoteServer = TestRemoteServer;

        async fn remote_server(
            &self,
            id: &str,
        ) -> Result<Arc<TestRemoteServer>, ConnectionManagerError> {
            #[derive(Debug, Snafu)]
            enum TestRemoteError {
                #[snafu(display("remote not found"))]
                NotFound,
            }
            Ok(Arc::clone(self.remotes.get(id).ok_or_else(|| {
                ConnectionManagerError::RemoteServerConnectError {
                    source: Box::new(TestRemoteError::NotFound),
                }
            })?))
        }
    }

    #[derive(Debug)]
    pub struct TestRemoteServer {
        pub written: Arc<AtomicBool>,
    }

    #[async_trait]
    impl<'a> RemoteServer for TestRemoteServer {
        async fn write_entry(
            &self,
            _db: &str,
            _entry: Entry,
        ) -> Result<(), ConnectionManagerError> {
            self.written.store(true, Ordering::Relaxed);
            Ok(())
        }
    }
}
