//! This module contains gRPC service implementatations for the WriteBuffer
//! storage implementation

// Something in instrument is causing lint warnings about unused braces
#![allow(unused_braces)]

// Something about how `tracing::instrument` works triggers a clippy
// warning about complex types
#![allow(clippy::type_complexity)]

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use delorean::storage::DatabaseStore;
use snafu::{ResultExt, Snafu};

use delorean::generated_types::{
    delorean_server::{Delorean, DeloreanServer},
    storage_server::{Storage, StorageServer},
    CapabilitiesResponse, CreateBucketRequest, CreateBucketResponse, DeleteBucketRequest,
    DeleteBucketResponse, GetBucketsResponse, MeasurementFieldsRequest, MeasurementFieldsResponse,
    MeasurementNamesRequest, MeasurementTagKeysRequest, MeasurementTagValuesRequest, Organization,
    ReadFilterRequest, ReadGroupRequest, ReadResponse, StringValuesResponse, TagKeysRequest,
    TagValuesRequest,
};

use tokio::sync::mpsc;
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
    /// Create a new GrpcService connected to `db_store`
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

/// Implementes the protobuf defined Storage service for a DatabaseStore
#[tonic::async_trait]
impl<T> Storage for GrpcService<T>
where
    T: DatabaseStore + 'static,
{
    type ReadFilterStream = mpsc::Receiver<Result<ReadResponse, Status>>;

    async fn read_filter(
        &self,
        _req: tonic::Request<ReadFilterRequest>,
    ) -> Result<tonic::Response<Self::ReadFilterStream>, Status> {
        Err(Status::unimplemented("read_filter"))
    }

    type ReadGroupStream = mpsc::Receiver<Result<ReadResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn read_group(
        &self,
        req: tonic::Request<ReadGroupRequest>,
    ) -> Result<tonic::Response<Self::ReadGroupStream>, Status> {
        Err(Status::unimplemented("read_group"))
    }

    type TagKeysStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn tag_keys(
        &self,
        req: tonic::Request<TagKeysRequest>,
    ) -> Result<tonic::Response<Self::TagKeysStream>, Status> {
        Err(Status::unimplemented("tag_keys"))
    }

    type TagValuesStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn tag_values(
        &self,
        req: tonic::Request<TagValuesRequest>,
    ) -> Result<tonic::Response<Self::TagValuesStream>, Status> {
        Err(Status::unimplemented("tag_values"))
    }

    #[tracing::instrument(level = "debug")]
    async fn capabilities(
        &self,
        req: tonic::Request<()>,
    ) -> Result<tonic::Response<CapabilitiesResponse>, Status> {
        // Full list of go capabilities in
        // idpe/storage/read/capabilities.go (aka window aggregate /
        // pushdown)
        //
        // For now, do not claim to support any capabilities
        let caps = CapabilitiesResponse {
            caps: HashMap::new(),
        };
        Ok(tonic::Response::new(caps))
    }

    type MeasurementNamesStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn measurement_names(
        &self,
        req: tonic::Request<MeasurementNamesRequest>,
    ) -> Result<tonic::Response<Self::MeasurementNamesStream>, Status> {
        Err(Status::unimplemented("measurement_names"))
    }

    type MeasurementTagKeysStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn measurement_tag_keys(
        &self,
        req: tonic::Request<MeasurementTagKeysRequest>,
    ) -> Result<tonic::Response<Self::MeasurementTagKeysStream>, Status> {
        Err(Status::unimplemented("measurement_tag_keys"))
    }

    type MeasurementTagValuesStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn measurement_tag_values(
        &self,
        req: tonic::Request<MeasurementTagValuesRequest>,
    ) -> Result<tonic::Response<Self::MeasurementTagValuesStream>, Status> {
        Err(Status::unimplemented("measurement_tag_values"))
    }

    type MeasurementFieldsStream = mpsc::Receiver<Result<MeasurementFieldsResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn measurement_fields(
        &self,
        req: tonic::Request<MeasurementFieldsRequest>,
    ) -> Result<tonic::Response<Self::MeasurementFieldsStream>, Status> {
        Err(Status::unimplemented("measurement_fields"))
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
        .add_service(StorageServer::new(GrpcService::new(storage.clone())))
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

    use delorean_generated_types::{delorean_client, storage_client};

    type DeloreanClient = delorean_client::DeloreanClient<tonic::transport::Channel>;
    type StorageClient = storage_client::StorageClient<tonic::transport::Channel>;

    struct Clients {
        delorean_client: DeloreanClient,
        storage_client: StorageClient,
    }

    #[tokio::test]
    async fn test_delorean_rpc_create() -> Result<()> {
        let mut clients = make_test_server().await.expect("Connecting to test server");

        let org = Organization {
            id: 1337,
            name: "my non-existent-org".into(),
            buckets: Vec::new(),
        };

        // Test response from delorean server
        let res = clients.delorean_client.get_buckets(org).await;

        match res {
            Err(e) => {
                assert_eq!(e.code(), Code::Unimplemented);
                assert_eq!(e.message(), "get_buckets");
            }
            Ok(buckets) => {
                assert!(false, "Unexpected delorean_client success: {:?}", buckets);
            }
        };

        // Test response from storage server
        let res = clients.storage_client.capabilities(()).await;
        match res {
            Err(e) => {
                assert!(false, "Unexpected storage_client error: {:?}", e);
                assert_eq!(e.message(), "get_buckets");
            }
            Ok(caps) => {
                let expected_caps = CapabilitiesResponse {
                    caps: HashMap::new(),
                };

                assert_eq!(*caps.get_ref(), expected_caps);
            }
        };

        Ok(())
    }

    // Start up a test rpc server, returning clients suitable for communication with it.
    async fn make_test_server() -> Result<Clients, tonic::transport::Error> {
        let test_storage = Arc::new(TestDatabaseStore::new());
        // TODO: specify port 0 to let the OS pick the port (need to
        // figure out how to get access to the actual addr from tonic)
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 11807);

        println!("Starting delorean rpc test server on {:?}", bind_addr);

        let server = make_server(bind_addr, test_storage.clone());
        tokio::task::spawn(server);

        let delorean_client = connect_to_server::<DeloreanClient>(bind_addr).await?;
        let storage_client = connect_to_server::<StorageClient>(bind_addr).await?;

        Ok(Clients {
            delorean_client,
            storage_client,
        })
    }

    /// Represents something that can make a connection to a server
    #[tonic::async_trait]
    trait NewClient: Sized + std::fmt::Debug {
        async fn connect(addr: String) -> Result<Self, tonic::transport::Error>;
    }

    #[tonic::async_trait]
    impl NewClient for DeloreanClient {
        async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
            Self::connect(addr).await
        }
    }

    #[tonic::async_trait]
    impl NewClient for StorageClient {
        async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
            Self::connect(addr).await
        }
    }

    /// loop and try to make a client connection for 5 seconds,
    /// returning the result of the connection
    async fn connect_to_server<T>(bind_addr: SocketAddr) -> Result<T, tonic::transport::Error>
    where
        T: NewClient,
    {
        const MAX_RETRIES: u32 = 10;
        let mut retry_count = 0;
        loop {
            let mut interval = tokio::time::interval(Duration::from_millis(500));

            match T::connect(format!("http://{}", bind_addr)).await {
                Ok(client) => {
                    println!("Sucessfully connected to server. Client: {:?}", client);
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
