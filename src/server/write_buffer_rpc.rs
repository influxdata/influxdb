//! This module contains gRPC service implementatations for the WriteBuffer
//! storage implementation

use std::{net::SocketAddr, sync::Arc};

use delorean::storage::DatabaseStore;
use snafu::{ResultExt, Snafu};

use delorean::generated_types::{
    delorean_server::{Delorean, DeloreanServer},
    CreateBucketRequest, CreateBucketResponse, DeleteBucketRequest, DeleteBucketResponse,
    GetBucketsResponse, Organization,
};

use tonic::Status;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("gRPC server error:  {}", source))]
    ServerError { source: tonic::transport::Error },

    #[snafu(display("Operation not yet implemented:  {}", operation))]
    NotYetImplemented { operation: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct GrpcService<T: DatabaseStore> {
    db_store: Arc<T>,
}

impl<T> GrpcService<T>
where
    T: DatabaseStore + 'static,
{
    /// Create a new GrpcServer connected to `db_store`
    pub fn new(db_store: Arc<T>) -> Self {
        Self { db_store }
    }
}

#[tonic::async_trait]
/// Implements the protobuf defined Delorean rpc service for a DatabaseStore
impl<T> Delorean for GrpcService<T>
where
    T: DatabaseStore + 'static,
{
    // TODO: Do we want to keep this gRPC request?
    async fn create_bucket(
        &self,
        _req: tonic::Request<CreateBucketRequest>,
    ) -> Result<tonic::Response<CreateBucketResponse>, Status> {
        Err(Status::unimplemented("create_bucket"))
    }

    async fn delete_bucket(
        &self,
        _req: tonic::Request<DeleteBucketRequest>,
    ) -> Result<tonic::Response<DeleteBucketResponse>, Status> {
        Err(Status::unimplemented("delete_bucket"))
    }

    async fn get_buckets(
        &self,
        _req: tonic::Request<Organization>,
    ) -> Result<tonic::Response<GetBucketsResponse>, Status> {
        Err(Status::unimplemented("get_buckets"))
    }
}

/// Instantiate a server listening on the specified address
/// implementing the Delorean and Storage gRPC interfaces, the
/// underlying hyper server instance. Resolves when the server has
/// shutdown.
pub async fn make_server<T>(bind_addr: SocketAddr, storage: Arc<T>) -> Result<()>
where
    T: DatabaseStore + 'static,
{
    tonic::transport::Server::builder()
        .add_service(DeloreanServer::new(GrpcService::new(storage.clone())))
        //.add_service(StorageServer::new(GrpcServer { app: state.clone() }))
        .serve(bind_addr)
        .await
        .context(ServerError {})
}

#[cfg(test)]
mod tests {
    use super::*;
    use delorean::storage::test_fixtures::TestDatabaseStore;
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::Duration,
    };
    use tonic::Code;

    use delorean_generated_types::delorean_client::DeloreanClient;

    #[tokio::test]
    async fn test_delorean_rpc_create() -> Result<()> {
        let mut delorean_client = make_test_server().await.expect("Connecting to test server");

        let org = Organization {
            id: 1337,
            name: "my non-existent-org".into(),
            buckets: Vec::new(),
        };

        // Test basic bucket listing
        let res = delorean_client.get_buckets(org).await;

        match res {
            Err(e) => {
                assert_eq!(e.code(), Code::Unimplemented);
                assert_eq!(e.message(), "get_buckets");
            }
            Ok(buckets) => {
                assert!(false, "Unexpected success: {:?}", buckets);
            }
        };

        Ok(())
    }

    // Start up a test server, returning a client suitable for communication with it.
    async fn make_test_server(
    ) -> Result<DeloreanClient<tonic::transport::Channel>, tonic::transport::Error> {
        let test_storage = Arc::new(TestDatabaseStore::new());
        // TODO: specify port 0 to let the OS pick the port (need to
        // figure out how to get access to the actual addr from tonic)
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 11807);

        println!("Starting delorean rpc test server on {:?}", bind_addr);

        let server = make_server(bind_addr, test_storage.clone());
        tokio::task::spawn(server);

        // Now, loop and try to make a client connection for 5 seconds
        const MAX_RETRIES: u32 = 10;
        let mut retry_count = 0;
        loop {
            let mut interval = tokio::time::interval(Duration::from_millis(500));

            match DeloreanClient::connect(format!("http://{}", bind_addr)).await {
                Ok(client) => {
                    return Ok(client);
                }
                Err(e) => {
                    retry_count += 1;

                    if retry_count > 10 {
                        println!("Server did not start in time: {}", e);
                        return Err(e);
                    } else {
                        println!(
                            "Server not yet up. Retrying ({}/{}): {}",
                            retry_count, MAX_RETRIES, e
                        );
                    }
                }
            };
            interval.tick().await;
        }
    }
}
