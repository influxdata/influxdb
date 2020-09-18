//! This module contains gRPC service implementatations for the WriteBuffer
//! storage implementation

// Something in instrument is causing lint warnings about unused braces
#![allow(unused_braces)]

// Something about how `tracing::instrument` works triggers a clippy
// warning about complex types
#![allow(clippy::type_complexity)]

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use snafu::{ResultExt, Snafu};

use delorean::generated_types::{
    delorean_server::{Delorean, DeloreanServer},
    storage_server::{Storage, StorageServer},
    CapabilitiesResponse, CreateBucketRequest, CreateBucketResponse, DeleteBucketRequest,
    DeleteBucketResponse, GetBucketsResponse, MeasurementFieldsRequest, MeasurementFieldsResponse,
    MeasurementNamesRequest, MeasurementTagKeysRequest, MeasurementTagValuesRequest, Organization,
    ReadFilterRequest, ReadGroupRequest, ReadResponse, StringValuesResponse, TagKeysRequest,
    TagValuesRequest, TimestampRange,
};

use crate::server::rpc::input::GrpcInputs;
use delorean::storage;
use delorean::storage::{Database, DatabaseStore};

use tokio::sync::mpsc;
use tonic::Status;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("gRPC server error:  {}", source))]
    ServerError { source: tonic::transport::Error },

    #[snafu(display("Database not found: {}", db_name))]
    DatabaseNotFound { db_name: String },

    #[snafu(display("Can not retrieve table list for '{}': {}", db_name, source))]
    GettingTables {
        db_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Operation not yet implemented:  {}", operation))]
    NotYetImplemented { operation: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Converts a result from the business logic into the appropriate tonic status
    fn to_status(&self) -> tonic::Status {
        match &self {
            Self::ServerError { .. } => Status::internal(self.to_string()),
            Self::DatabaseNotFound { .. } => Status::not_found(self.to_string()),
            Self::GettingTables { .. } => Status::internal(self.to_string()),
            Self::NotYetImplemented { .. } => Status::internal(self.to_string()),
        }
    }
}

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
        let (mut tx, rx) = mpsc::channel(4);

        let measurement_names_request = req.into_inner();

        let db_name = storage::org_and_bucket_to_database(
            measurement_names_request.org_id()?,
            &measurement_names_request.bucket_name()?,
        );

        let range = convert_range(measurement_names_request.range);

        let response = measurement_name_impl(self.db_store.clone(), db_name, range)
            .await
            .map_err(|e| e.to_status());

        tx.send(response)
            .await
            .expect("sending measurement names response to server");

        Ok(tonic::Response::new(rx))
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

fn convert_range(range: Option<TimestampRange>) -> Option<storage::TimestampRange> {
    range.map(|TimestampRange { start, end }| storage::TimestampRange { start, end })
}

// The following code implements the business logic of the requests as
// methods that return Results with module specific Errors (and thus
// can use ?, etc). The trait implemententations then handle mapping
// to the appropriate tonic Status

/// Gathers all measurement names that have data in the specified
/// (optional) range
///
/// TODO: Do this work on a separate worker pool, not the main tokio
/// task poo
async fn measurement_name_impl<T>(
    db_store: Arc<T>,
    db_name: String,
    range: Option<storage::TimestampRange>,
) -> Result<StringValuesResponse>
where
    T: DatabaseStore,
{
    let table_names = db_store
        .db(&db_name)
        .await
        .ok_or_else(|| Error::DatabaseNotFound {
            db_name: db_name.clone(),
        })?
        .table_names(range)
        .await
        .map_err(|e| Error::GettingTables {
            db_name: db_name.clone(),
            source: Box::new(e),
        })?;

    // In theory this could be combined with the chain above, but
    // we break it into its own statement here for readability

    // Map the resulting collection of Strings into a Vec<Vec<u8>>for return
    let values = table_names
        .iter()
        .map(|name| name.bytes().collect())
        .collect::<Vec<_>>();

    Ok(StringValuesResponse { values })
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
    use delorean::id::Id;
    use delorean::test::storage::TestDatabaseStore;
    use std::{
        convert::TryFrom,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::Duration,
    };
    use tonic::Code;

    use futures::prelude::*;

    use delorean_generated_types::{delorean_client, storage_client, ReadSource};
    use prost::Message;

    type DeloreanClient = delorean_client::DeloreanClient<tonic::transport::Channel>;
    type StorageClient = storage_client::StorageClient<tonic::transport::Channel>;

    #[tokio::test]
    async fn test_delorean_rpc() -> Result<()> {
        let mut fixture = Fixture::new(11807)
            .await
            .expect("Connecting to test server");

        let org = Organization {
            id: 1337,
            name: "my non-existent-org".into(),
            buckets: Vec::new(),
        };

        // Test response from delorean server
        let res = fixture.delorean_client.get_buckets(org).await;

        match res {
            Err(e) => {
                assert_eq!(e.code(), Code::Unimplemented);
                assert_eq!(e.message(), "get_buckets");
            }
            Ok(buckets) => {
                assert!(false, "Unexpected delorean_client success: {:?}", buckets);
            }
        };

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_rpc_capabilities() -> Result<(), tonic::Status> {
        // Note we use a unique port. TODO: let the OS pick the port
        let mut fixture = Fixture::new(11808)
            .await
            .expect("Connecting to test server");

        // Test response from storage server
        assert_eq!(HashMap::new(), fixture.storage_client.capabilities().await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_rpc_measurement_names() -> Result<(), tonic::Status> {
        // Note we use a unique port. TODO: let the OS pick the port
        let mut fixture = Fixture::new(11809)
            .await
            .expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let lp_data = "h2o,state=CA temp=50.4 1568756160\no2,state=MA temp=50.4 1568756160";
        fixture
            .test_storage
            .add_lp_string(&db_info.db_name, lp_data)
            .await;

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));
        let request = MeasurementNamesRequest {
            source,
            range: None,
        };

        let actual_measurements = fixture.storage_client.measurement_names(request).await?;

        let expected_measurements = vec![String::from("h2o"), String::from("o2")];
        assert_eq!(actual_measurements, expected_measurements);

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_rpc_measurement_names_timestamp() -> Result<(), tonic::Status> {
        // Note we use a unique port. TODO: let the OS pick the port
        let mut fixture = Fixture::new(11810)
            .await
            .expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let lp_data = "h2o,state=CA temp=50.4 100\no2,state=MA temp=50.4 200";
        fixture
            .test_storage
            .add_lp_string(&db_info.db_name, lp_data)
            .await;

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));
        let request = MeasurementNamesRequest {
            source,
            range: Some(TimestampRange {
                start: 150,
                end: 200,
            }),
        };

        let actual_measurements = fixture.storage_client.measurement_names(request).await?;

        let expected_measurements = vec![String::from("o2")];
        assert_eq!(actual_measurements, expected_measurements);

        Ok(())
    }

    /// Delorean deals with database names. The gRPC interface deals
    /// with org_id and bucket_id represented as 16 digit hex
    /// values. This struct manages creating the org_id, bucket_id,
    /// and database names to be consistent with the implementation
    struct OrgAndBucket {
        org_id: u64,
        bucket_id: u64,
        /// The delorean database name corresponding to `org_id` and `bucket_id`
        db_name: String,
    }

    impl OrgAndBucket {
        fn new(org_id: u64, bucket_id: u64) -> Self {
            let org_id_str = Id::try_from(org_id).expect("org_id was valid").to_string();

            let bucket_id_str = Id::try_from(bucket_id)
                .expect("bucket_id was valid")
                .to_string();

            let db_name = storage::org_and_bucket_to_database(&org_id_str, &bucket_id_str);

            Self {
                org_id,
                bucket_id,
                db_name,
            }
        }
    }

    /// Wrapper around a StorageClient that does the various tonic /
    /// futures dance
    struct StorageClientWrapper {
        inner: StorageClient,
    }

    impl StorageClientWrapper {
        fn new(inner: StorageClient) -> Self {
            Self { inner }
        }

        /// Create a ReadSource suitable for constructing messages
        fn read_source(org_id: u64, bucket_id: u64, partition_id: u64) -> prost_types::Any {
            let read_source = ReadSource {
                org_id,
                bucket_id,
                partition_id,
            };
            let mut d = Vec::new();
            read_source
                .encode(&mut d)
                .expect("encoded read source appropriately");
            prost_types::Any {
                type_url: "/TODO".to_string(),
                value: d,
            }
        }

        /// return the capabilities of the server as a hash map
        async fn capabilities(&mut self) -> Result<HashMap<String, String>, tonic::Status> {
            let response = self.inner.capabilities(()).await?.into_inner();

            let CapabilitiesResponse { caps } = response;

            Ok(caps)
        }

        // Make a request to Storage::measurement_names and do the
        // required async dance to flatten the resulting stream to Strings
        async fn measurement_names(
            &mut self,
            request: MeasurementNamesRequest,
        ) -> Result<Vec<String>, tonic::Status> {
            let responses = self.inner.measurement_names(request).await?;

            // type annotations to help future readers
            let responses: Vec<StringValuesResponse> = responses.into_inner().try_collect().await?;

            let measurements = responses
                .into_iter()
                .map(|r| r.values.into_iter())
                .flatten()
                .map(|v| String::from_utf8(v).expect("measurement name was utf8"))
                .collect::<Vec<_>>();

            Ok(measurements)
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

    // Wrapper around raw clients and test database
    struct Fixture {
        delorean_client: DeloreanClient,
        storage_client: StorageClientWrapper,
        test_storage: Arc<TestDatabaseStore>,
    }

    impl Fixture {
        /// Start up a test rpc server listening on `port`, returning
        /// a fixture with the test server and clients
        async fn new(port: u16) -> Result<Self, tonic::transport::Error> {
            let test_storage = Arc::new(TestDatabaseStore::new());
            // TODO: specify port 0 to let the OS pick the port (need to
            // figure out how to get access to the actual addr from tonic)
            let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);

            println!("Starting delorean rpc test server on {:?}", bind_addr);

            let server = make_server(bind_addr, test_storage.clone());
            tokio::task::spawn(server);

            let delorean_client = connect_to_server::<DeloreanClient>(bind_addr).await?;
            let storage_client =
                StorageClientWrapper::new(connect_to_server::<StorageClient>(bind_addr).await?);

            Ok(Self {
                delorean_client,
                storage_client,
                test_storage,
            })
        }
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
}
