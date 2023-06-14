use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use generated_types::i_ox_testing_client::IOxTestingClient;
use influxdb_storage_client::{
    connection::{Builder as ConnectionBuilder, Connection, GrpcConnection},
    Client as StorageClient,
};
use service_common::test_util::TestDatabaseStore;
use snafu::{ResultExt, Snafu};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;

use crate::service::ErrorLogger;

#[derive(Debug, Snafu)]
pub enum FixtureError {
    #[snafu(display("Error binding fixture server: {}", source))]
    Bind { source: std::io::Error },

    #[snafu(display("Error creating fixture: {}", source))]
    Tonic { source: tonic::transport::Error },
}

/// Wrapper around raw clients and test database
#[derive(Debug)]
pub struct Fixture {
    pub client_connection: Connection,
    pub iox_client: IOxTestingClient<GrpcConnection>,
    pub storage_client: StorageClient,
    pub test_storage: Arc<TestDatabaseStore>,
    pub join_handle: JoinHandle<()>,
}

impl Fixture {
    /// Start up a test storage server listening on `port`, returning
    /// a fixture with the test server and clients
    pub async fn new() -> Result<Self, FixtureError> {
        Self::new_with_semaphore_size(u16::MAX as usize).await
    }

    pub async fn new_with_semaphore_size(semaphore_size: usize) -> Result<Self, FixtureError> {
        let test_storage = Arc::new(TestDatabaseStore::new_with_semaphore_size(semaphore_size));

        // Get a random port from the kernel by asking for port 0.
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let socket = tokio::net::TcpListener::bind(bind_addr)
            .await
            .context(BindSnafu)?;

        // Pull the assigned port out of the socket
        let bind_addr = socket.local_addr().unwrap();

        println!("Starting InfluxDB IOx storage test server on {bind_addr:?}");

        let trace_header_parser = trace_http::ctx::TraceHeaderParser::new();

        let router = tonic::transport::Server::builder()
            .layer(trace_http::tower::TraceLayer::new(
                trace_header_parser,
                Arc::clone(&test_storage.metric_registry),
                None,
                true,
                "test server",
            ))
            .add_service(service_grpc_testing::make_server())
            .add_service(crate::make_server(Arc::clone(&test_storage)));

        let server = async move {
            let stream = TcpListenerStream::new(socket);

            router
                .serve_with_incoming(stream)
                .await
                .log_if_error("Running Tonic Server")
                .ok();
        };

        let join_handle = tokio::task::spawn(server);

        let client_connection = ConnectionBuilder::default()
            .connect_timeout(std::time::Duration::from_secs(30))
            .build(format!("http://{bind_addr}"))
            .await
            .unwrap();

        let iox_client = IOxTestingClient::new(client_connection.clone().into_grpc_connection());

        let storage_client = StorageClient::new(client_connection.clone());

        Ok(Self {
            client_connection,
            iox_client,
            storage_client,
            test_storage,
            join_handle,
        })
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.join_handle.abort();
    }
}
