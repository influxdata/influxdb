use reqwest::{Body, IntoUrl};
use secrecy::{ExposeSecret, Secret};
use serde::Serialize;
use url::Url;

/// Primary error type for the [`Client`]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("base URL error: {0}")]
    BaseUrl(#[source] reqwest::Error),

    #[error("request URL error: {0}")]
    RequestUrl(#[from] url::ParseError),

    #[error("failed to send /api/v3/write_lp request: {0}")]
    WriteLpSend(#[source] reqwest::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// The InfluxDB 3.0 Client
///
/// Convenience type for accessing the HTTP API of InfluxDB 3.0
#[derive(Debug, Clone)]
pub struct Client {
    /// The base URL for making requests to a running InfluxDB 3.0 server
    base_url: Url,
    /// The `Bearer` token to use for authenticating on each request to the server
    auth_header: Option<Secret<String>>,
    /// A [`reqwest::Client`] for handling HTTP requests
    http_client: reqwest::Client,
}

impl Client {
    /// Create a new [`Client`]
    ///
    /// A [`reqwest::Client`] is passed in order to allow for re-use of an existing
    /// `reqwest::Client`.
    pub fn new<U: IntoUrl>(base_url: U, http_client: reqwest::Client) -> Result<Self> {
        Ok(Self {
            base_url: base_url.into_url().map_err(Error::BaseUrl)?,
            auth_header: None,
            http_client,
        })
    }

    /// Set the `Bearer` token that will be sent with each request to the server
    pub fn with_auth_header<S: Into<String>>(mut self, auth_header: S) -> Self {
        self.auth_header = Some(Secret::new(auth_header.into()));
        self
    }

    /// Compose a request to the `/api/v3/write_lp` API
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let client = Client::new("localhost:8181", reqwest::Client::new())?;
    /// let _ = client
    ///     .api_v3_write_lp("db_name")
    ///     .precision_milli()
    ///     .accept_partial(true)
    ///     .body("cpu,host=s1 usage=0.5")
    ///     .send()
    ///     .await
    ///     .expect("send write_lp request");
    /// # Ok(())
    /// # }
    /// ```
    pub fn api_v3_write_lp<S: Into<String>>(&self, db: S) -> WriteRequestBuilder<NoBody> {
        WriteRequestBuilder {
            client: self.clone(),
            db: db.into(),
            precision: None,
            accept_partial: None,
            body: NoBody,
        }
    }
}

/// The body of the request to the `/api/v3/write_lp` API
// TODO - this should re-use the type defined in the server code.
#[derive(Debug, Serialize)]
struct WriteRequest<'a> {
    db: &'a str,
    precision: Option<Precision>,
    accept_partial: Option<bool>,
}

impl<'a, B> From<&'a WriteRequestBuilder<B>> for WriteRequest<'a> {
    fn from(builder: &'a WriteRequestBuilder<B>) -> Self {
        Self {
            db: &builder.db,
            precision: builder.precision,
            accept_partial: builder.accept_partial,
        }
    }
}

/// Time series precision
// TODO - this should re-use type from server code.
#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum Precision {
    Second,
    Milli,
    Micro,
    Nano,
}

/// Builder type for composing a request to `/api/v3/write_lp`
///
/// Produced by [`Client::api_v3_write_lp`]
#[derive(Debug)]
pub struct WriteRequestBuilder<B> {
    client: Client,
    db: String,
    precision: Option<Precision>,
    accept_partial: Option<bool>,
    body: B,
}

impl<B> WriteRequestBuilder<B> {
    /// Use seconds precision
    pub fn precision_seconds(mut self) -> Self {
        self.precision = Some(Precision::Second);
        self
    }

    /// Use milliseconds precision
    pub fn precision_milli(mut self) -> Self {
        self.precision = Some(Precision::Milli);
        self
    }

    /// Use microseconds precision
    pub fn precision_micro(mut self) -> Self {
        self.precision = Some(Precision::Micro);
        self
    }

    /// Use nanoseconds precision
    pub fn precision_nano(mut self) -> Self {
        self.precision = Some(Precision::Nano);
        self
    }

    /// Set the `accept_partial` parameter
    pub fn accept_partial(mut self, set_to: bool) -> Self {
        self.accept_partial = Some(set_to);
        self
    }
}

impl WriteRequestBuilder<NoBody> {
    /// Set the body of the request to the `/api/v3/write_lp` API
    ///
    /// This essentially wraps `reqwest`'s [`body`][reqwest::RequestBuilder::body]
    /// method, and puts the responsibility on the caller for now.
    pub fn body<T: Into<Body>>(self, body: T) -> WriteRequestBuilder<Body> {
        WriteRequestBuilder {
            client: self.client,
            db: self.db,
            precision: self.precision,
            accept_partial: self.accept_partial,
            body: body.into(),
        }
    }
}

impl WriteRequestBuilder<Body> {
    /// Send the request to the server
    pub async fn send(self) -> Result<()> {
        let url = self.client.base_url.join("/api/v3/write_lp")?;
        let req = WriteRequest::from(&self);
        let mut b = self.client.http_client.post(url).query(&req);
        if let Some(token) = self.client.auth_header {
            b = b.bearer_auth(token.expose_secret());
        }
        // TODO - handle the response, return to caller, etc.
        b.body(self.body).send().await.map_err(Error::WriteLpSend)?;
        Ok(())
    }
}

#[doc(hidden)]
/// Typestate type for [`WriteRequestBuilder`]
#[derive(Debug, Copy, Clone)]
pub struct NoBody;

#[cfg(test)]
mod tests {
    use mockito::{Matcher, Server};

    use crate::Client;

    #[tokio::test]
    async fn api_v3_write_lp() {
        let token = "super-secret-token";
        let db = "stats";
        let body = "\
cpu,host=s1 usage=0.5
cpu,host=s1,region=us-west usage=0.7";

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/write_lp")
            .match_header("Authorization", format!("Bearer {token}").as_str())
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("precision".into(), "milli".into()),
                Matcher::UrlEncoded("db".into(), db.into()),
                Matcher::UrlEncoded("accept_partial".into(), "true".into()),
            ]))
            .match_body(body)
            .create_async()
            .await;

        let client = Client::new(mock_server.url(), reqwest::Client::new())
            .expect("create client")
            .with_auth_header(token);

        let _ = client
            .api_v3_write_lp(db)
            .precision_milli()
            .accept_partial(true)
            .body(body)
            .send()
            .await
            .expect("send write_lp request");

        mock.assert_async().await;
    }
}
