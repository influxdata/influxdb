use crate::tower::{SetRequestHeadersLayer, SetRequestHeadersService};
use http::header::HeaderName;
use http::HeaderMap;
use http::{uri::InvalidUri, HeaderValue, Uri};
use std::convert::TryInto;
use std::time::Duration;
use thiserror::Error;
use tonic::transport::{Channel, Endpoint};
use tower::make::MakeConnection;

/// The connection type used for clients. Use [`Builder`] to create
/// instances of [`Connection`] objects
#[derive(Debug, Clone)]
pub struct Connection {
    grpc_connection: GrpcConnection,
    http_connection: HttpConnection,
}

impl Connection {
    /// Create a new Connection
    fn new(grpc_connection: GrpcConnection, http_connection: HttpConnection) -> Self {
        Self {
            grpc_connection,
            http_connection,
        }
    }

    /// Consume `self` and return a [`GrpcConnection`] (suitable for use in
    /// tonic clients)
    pub fn into_grpc_connection(self) -> GrpcConnection {
        self.grpc_connection
    }

    /// Consume `self` and return a [`HttpConnection`] (suitable for making
    /// calls to /api/v2 endpoints)
    pub fn into_http_connection(self) -> HttpConnection {
        self.http_connection
    }
}

/// The type used to make tonic (gRPC) requests
pub type GrpcConnection = SetRequestHeadersService<tonic::transport::Channel>;

/// The type used to make raw http request
#[derive(Debug, Clone)]
pub struct HttpConnection {
    /// The base uri of the IOx http API endpoint
    uri: Uri,
    /// http client connection
    http_client: reqwest::Client,
}

impl HttpConnection {
    fn new(uri: Uri, http_client: reqwest::Client) -> Self {
        Self { uri, http_client }
    }

    /// Return a reference to the underyling http client
    pub fn client(&self) -> &reqwest::Client {
        &self.http_client
    }

    /// Return a reference to the base uri of the IOx http API endpoint
    pub fn uri(&self) -> &Uri {
        &self.uri
    }
}

/// The default User-Agent header sent by the HTTP client.
pub const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));
/// The default connection timeout
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
/// The default request timeout
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Errors returned by the ConnectionBuilder
#[derive(Debug, Error)]
pub enum Error {
    /// Server returned an invalid argument error
    #[error("Connection error: {}{}", source, details)]
    TransportError {
        /// underlying [`tonic::transport::Error`]
        source: tonic::transport::Error,
        /// stringified version of the tonic error's source
        details: String,
    },

    /// Client received an unexpected error from the server
    #[error("Invalid URI: {}", .0)]
    InvalidUri(#[from] InvalidUri),
}

// Custom impl to include underlying source (not included in tonic
// transport error)
impl From<tonic::transport::Error> for Error {
    fn from(source: tonic::transport::Error) -> Self {
        use std::error::Error;
        let details = source
            .source()
            .map(|e| format!(" ({e})"))
            .unwrap_or_else(|| "".to_string());

        Self::TransportError { source, details }
    }
}

/// Result type for the ConnectionBuilder
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A builder that produces a connection that can be used with any of the gRPC
/// clients
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use client_util::connection::Builder;
/// use std::time::Duration;
///
/// let connection = Builder::new()
///     .timeout(Duration::from_secs(42))
///     .user_agent("my_awesome_client")
///     .build("http://127.0.0.1:8082/")
///     .await
///     .expect("connection must succeed");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Builder {
    user_agent: String,
    headers: Vec<(HeaderName, HeaderValue)>,
    connect_timeout: Duration,
    timeout: Duration,
}

impl std::default::Default for Builder {
    fn default() -> Self {
        Self {
            user_agent: USER_AGENT.into(),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            timeout: DEFAULT_TIMEOUT,
            headers: Default::default(),
        }
    }
}

impl Builder {
    /// Create a new default builder
    pub fn new() -> Self {
        Default::default()
    }

    /// Construct the [`Connection`] instance using the specified base URL.
    pub async fn build<D>(self, dst: D) -> Result<Connection>
    where
        D: TryInto<Uri, Error = InvalidUri> + Send,
    {
        let endpoint = self.create_endpoint(dst)?;
        let channel = endpoint.connect().await?;
        Ok(self.compose_middleware(channel, endpoint))
    }

    /// Construct the [`Connection`] instance using the specified base URL and custom connector.
    pub async fn build_with_connector<D, C>(self, dst: D, connector: C) -> Result<Connection>
    where
        D: TryInto<Uri, Error = InvalidUri> + Send,
        C: MakeConnection<Uri> + Send + 'static,
        C::Connection: Unpin + Send + 'static,
        C::Future: Send + 'static,
        Box<dyn std::error::Error + Send + Sync>: From<C::Error> + Send + 'static,
    {
        let endpoint = self.create_endpoint(dst)?;
        let channel = endpoint.connect_with_connector(connector).await?;
        Ok(self.compose_middleware(channel, endpoint))
    }

    fn create_endpoint<D>(&self, dst: D) -> Result<Endpoint>
    where
        D: TryInto<Uri, Error = InvalidUri> + Send,
    {
        let endpoint = Endpoint::from(dst.try_into()?)
            .user_agent(&self.user_agent)?
            .connect_timeout(self.connect_timeout)
            .timeout(self.timeout);
        Ok(endpoint)
    }

    fn compose_middleware(self, channel: Channel, endpoint: Endpoint) -> Connection {
        let headers_map: HeaderMap = self.headers.iter().cloned().collect();

        // Compose channel with new tower middleware stack
        let grpc_connection = tower::ServiceBuilder::new()
            .layer(SetRequestHeadersLayer::new(self.headers))
            .service(channel);

        let http_client = reqwest::Client::builder()
            .connection_verbose(true)
            .default_headers(headers_map)
            .build()
            .expect("reqwest::Client should have built");

        let http_connection = HttpConnection::new(endpoint.uri().clone(), http_client);

        Connection::new(grpc_connection, http_connection)
    }

    /// Set the `User-Agent` header sent by this client.
    pub fn user_agent(self, user_agent: impl Into<String>) -> Self {
        Self {
            user_agent: user_agent.into(),
            ..self
        }
    }

    /// Sets a header to be included on all requests
    pub fn header(self, header: impl Into<HeaderName>, value: impl Into<HeaderValue>) -> Self {
        let mut headers = self.headers;
        headers.push((header.into(), value.into()));
        Self { headers, ..self }
    }

    /// Sets the maximum duration of time the client will wait for the IOx
    /// server to accept the TCP connection before aborting the request.
    ///
    /// Note this does not bound the request duration - see
    /// [`timeout`][Self::timeout].
    pub fn connect_timeout(self, timeout: Duration) -> Self {
        Self {
            connect_timeout: timeout,
            ..self
        }
    }

    /// Bounds the total amount of time a single client HTTP request take before
    /// being aborted.
    ///
    /// This timeout includes:
    ///
    ///  - Establishing the TCP connection (see [`connect_timeout`])
    ///  - Sending the HTTP request
    ///  - Waiting for, and receiving the entire HTTP response
    ///
    /// [`connect_timeout`]: Self::connect_timeout
    pub fn timeout(self, timeout: Duration) -> Self {
        Self { timeout, ..self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Method;

    #[test]
    fn test_builder_cloneable() {
        // Clone is used by Conductor.
        fn assert_clone<T: Clone>(_t: T) {}
        assert_clone(Builder::default())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn headers_are_set() {
        let mut mock_server = mockito::Server::new_async().await;
        let url = mock_server.url();

        let http_connection = Builder::new()
            .header(
                HeaderName::from_static("foo"),
                HeaderValue::from_static("bar"),
            )
            .build(&url)
            .await
            .unwrap()
            .into_http_connection();

        let url = format!("{url}/the_api");
        println!("Sending to {url}");

        let m = mock_server
            .mock("POST", "/the_api")
            .with_status(201)
            .with_body("world")
            .match_header("FOO", "bar")
            .create_async()
            .await;

        http_connection
            .client()
            .request(Method::POST, &url)
            .send()
            .await
            .expect("Error making http request");

        m.assert_async().await;
    }
}
