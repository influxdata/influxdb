use client_util::connection::GrpcConnection;
use futures::stream;
use futures_util::TryStreamExt;
use generated_types::Request as GrpcRequest;
use tonic_reflection::pb::v1alpha::{
    ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
    server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
};

use crate::connection::Connection;
use crate::error::Error;

/// A client for testing purposes
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{
///     reflection::Client,
///     connection::Builder,
/// };
///
/// let mut connection = Builder::default()
///     .build("http://127.0.0.1:8082")
///     .await
///     .unwrap();
///
/// let mut client = Client::new(connection);
///
/// // list all services registered with reflection server
/// let _services = client
///     .services()
///     .await
///     .expect("getting services from reflection service");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    inner: ServerReflectionClient<GrpcConnection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: ServerReflectionClient::new(connection.into_grpc_connection()),
        }
    }

    /// List services.
    pub async fn services(&mut self) -> Result<Vec<String>, Error> {
        let request = ServerReflectionRequest {
            host: "".to_string(),
            message_request: Some(MessageRequest::ListServices(String::new())),
        };
        let request = GrpcRequest::new(stream::iter(vec![request]));
        Ok(self
            .inner
            // the complexity here comes from the fact that server_reflection_info is (arguably
            // unnecessarily) a streaming endpoint where the intent is to be able to submit several
            // queries at once. this "services" method only cares about ListServices requests so we
            // have to filter out all hypothetical other possible response types for the sake of
            // keeping this method's signature simple (ie only return a vec of names) even though
            // we know there won't be any other types of MessageResponse.
            .server_reflection_info(request)
            .await?
            .into_inner()
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .filter_map(|resp| {
                resp.message_response.into_iter().find_map(|m| match m {
                    MessageResponse::ListServicesResponse(ls) => Some(ls.service),
                    _ => None,
                })
            })
            .flatten()
            .map(|s| s.name)
            .collect::<Vec<_>>())
    }
}
