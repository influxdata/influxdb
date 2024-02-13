use std::string::FromUtf8Error;

use bytes::Bytes;
use reqwest::{Body, IntoUrl, StatusCode};
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

    #[error("failed to read the API response bytes: {0}")]
    Bytes(#[source] reqwest::Error),

    #[error("failed to send /api/v3/query_sql request: {0}")]
    QuerySqlSend(#[source] reqwest::Error),

    #[error("invalid UTF8 in response: {0}")]
    InvalidUtf8(#[from] FromUtf8Error),

    #[error("server responded with error [{code}]: {message}")]
    ApiError { code: StatusCode, message: String },
}

pub type Result<T> = std::result::Result<T, Error>;

/// The InfluxDB 3.0 Client
///
/// For programmatic access to the HTTP API of InfluxDB 3.0
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
    /// # use influxdb3_client::Precision;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let client = Client::new("localhost:8181", reqwest::Client::new())?;
    /// client
    ///     .api_v3_write_lp("db_name")
    ///     .precision(Precision::Milli)
    ///     .accept_partial(true)
    ///     .body("cpu,host=s1 usage=0.5")
    ///     .send()
    ///     .await
    ///     .expect("send write_lp request");
    /// # Ok(())
    /// # }
    /// ```
    pub fn api_v3_write_lp<S: Into<String>>(&self, db: S) -> WriteRequestBuilder<'_, NoBody> {
        WriteRequestBuilder {
            client: self,
            db: db.into(),
            precision: None,
            accept_partial: None,
            body: NoBody,
        }
    }

    /// Compose a request to the `/api/v3/query_sql` API
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let client = Client::new("localhost:8181", reqwest::Client::new())?;
    /// let response_bytes = client
    ///     .api_v3_query_sql("db_name", "SELECT * FROM foo")
    ///     .send()
    ///     .await
    ///     .expect("send query_sql request");
    /// # Ok(())
    /// # }
    /// ```
    pub fn api_v3_query_sql<D: Into<String>, Q: Into<String>>(
        &self,
        db: D,
        query: Q,
    ) -> QueryRequestBuilder<'_> {
        QueryRequestBuilder {
            client: self,
            db: db.into(),
            query: query.into(),
            format: None,
        }
    }
}

/// The body of the request to the `/api/v3/write_lp` API
// TODO - this should re-use the type defined in the server code.
#[derive(Debug, Serialize)]
struct WriteParams<'a> {
    db: &'a str,
    precision: Option<Precision>,
    accept_partial: Option<bool>,
}

impl<'a, B> From<&'a WriteRequestBuilder<'a, B>> for WriteParams<'a> {
    fn from(builder: &'a WriteRequestBuilder<'a, B>) -> Self {
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
pub enum Precision {
    Second,
    Milli,
    Micro,
    Nano,
}

/// Builder type for composing a request to `/api/v3/write_lp`
///
/// Produced by [`Client::api_v3_write_lp`]
#[derive(Debug)]
pub struct WriteRequestBuilder<'c, B> {
    client: &'c Client,
    db: String,
    precision: Option<Precision>,
    accept_partial: Option<bool>,
    body: B,
}

impl<'c, B> WriteRequestBuilder<'c, B> {
    /// Set the precision
    pub fn precision(mut self, set_to: Precision) -> Self {
        self.precision = Some(set_to);
        self
    }

    /// Set the `accept_partial` parameter
    pub fn accept_partial(mut self, set_to: bool) -> Self {
        self.accept_partial = Some(set_to);
        self
    }
}

impl<'c> WriteRequestBuilder<'c, NoBody> {
    /// Set the body of the request to the `/api/v3/write_lp` API
    ///
    /// This essentially wraps `reqwest`'s [`body`][reqwest::RequestBuilder::body]
    /// method, and puts the responsibility on the caller for now.
    pub fn body<T: Into<Body>>(self, body: T) -> WriteRequestBuilder<'c, Body> {
        WriteRequestBuilder {
            client: self.client,
            db: self.db,
            precision: self.precision,
            accept_partial: self.accept_partial,
            body: body.into(),
        }
    }
}

impl<'c> WriteRequestBuilder<'c, Body> {
    /// Send the request to the server
    pub async fn send(self) -> Result<()> {
        let url = self.client.base_url.join("/api/v3/write_lp")?;
        let req = WriteParams::from(&self);
        let mut b = self.client.http_client.post(url).query(&req);
        if let Some(token) = &self.client.auth_header {
            b = b.bearer_auth(token.expose_secret());
        }
        let resp = b.body(self.body).send().await.map_err(Error::WriteLpSend)?;
        let status = resp.status();
        let content = resp.bytes().await.map_err(Error::Bytes)?;
        match status {
            // TODO - handle the OK response content, return to caller, etc.
            StatusCode::OK => Ok(()),
            code => Err(Error::ApiError {
                code,
                message: String::from_utf8(content.to_vec())?,
            }),
        }
    }
}

#[doc(hidden)]
/// Typestate type for [`WriteRequestBuilder`]
#[derive(Debug, Copy, Clone)]
pub struct NoBody;

/// Used to compose a request to the `/api/v3/query_sql` API
///
/// Produced by [`Client::api_v3_query_sql`] method.
#[derive(Debug)]
pub struct QueryRequestBuilder<'c> {
    client: &'c Client,
    db: String,
    query: String,
    format: Option<Format>,
}

// TODO - for now the send method just returns the bytes from the response.
//   It may be nicer to have the format parameter dictate how we return from
//   send, e.g., using types more specific to the format selected.
impl<'c> QueryRequestBuilder<'c> {
    /// Specify the format, `json`, `csv`, `pretty`, or `parquet`
    pub fn format(mut self, format: Format) -> Self {
        self.format = Some(format);
        self
    }

    /// Send the request to `/api/v3/query_sql`
    pub async fn send(self) -> Result<Bytes> {
        let url = self.client.base_url.join("/api/v3/query_sql")?;
        let req = QueryParams::from(&self);
        let mut b = self.client.http_client.get(url).query(&req);
        if let Some(token) = &self.client.auth_header {
            b = b.bearer_auth(token.expose_secret());
        }
        let resp = b.send().await.map_err(Error::QuerySqlSend)?;
        let status = resp.status();
        let content = resp.bytes().await.map_err(Error::Bytes)?;

        match status {
            StatusCode::OK => Ok(content),
            code => Err(Error::ApiError {
                code,
                message: String::from_utf8(content.to_vec()).map_err(Error::InvalidUtf8)?,
            }),
        }
    }
}

/// Query parameters for the `/api/v3/query_sql` API
#[derive(Debug, Serialize)]
pub struct QueryParams<'a> {
    db: &'a str,
    #[serde(rename = "q")]
    query: &'a str,
    format: Option<Format>,
}

impl<'a> From<&'a QueryRequestBuilder<'a>> for QueryParams<'a> {
    fn from(builder: &'a QueryRequestBuilder<'a>) -> Self {
        Self {
            db: &builder.db,
            query: &builder.query,
            format: builder.format,
        }
    }
}

/// Output format to request from the server in the `/api/v3/query_sql` API
#[derive(Debug, Serialize, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    Json,
    Csv,
    Parquet,
    Pretty,
}

#[cfg(test)]
mod tests {
    use mockito::{Matcher, Server};

    use crate::{Client, Format, Precision};

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
            .precision(Precision::Milli)
            .accept_partial(true)
            .body(body)
            .send()
            .await
            .expect("send write_lp request");

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn api_v3_query_sql() {
        let token = "super-secret-token";
        let db = "stats";
        let query = "SELECT * FROM foo";
        let body = r#"[{"host": "foo", "time": "1990-07-23T06:00:00:000", "val": 1}]"#;

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("GET", "/api/v3/query_sql")
            .match_header("Authorization", format!("Bearer {token}").as_str())
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("db".into(), db.into()),
                Matcher::UrlEncoded("q".into(), query.into()),
                Matcher::UrlEncoded("format".into(), "json".into()),
            ]))
            .with_status(200)
            // TODO - could add content-type header but that may be too brittle
            //        at the moment
            //      - this will be JSON Lines at some point
            .with_body(body)
            .create_async()
            .await;

        let client = Client::new(mock_server.url(), reqwest::Client::new())
            .expect("create client")
            .with_auth_header(token);

        let r = client
            .api_v3_query_sql(db, query)
            .format(Format::Json)
            .send()
            .await
            .expect("send request to server");

        assert_eq!(&r, body);

        mock.assert_async().await;
    }
}
